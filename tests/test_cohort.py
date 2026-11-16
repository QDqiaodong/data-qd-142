"""Tests for the entity cohort migration audit/reconciliation pipeline."""
import io
import json
import os
import random
import resource
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from distribution_drift.cohort import (  # noqa: E402
    CohortError, CohortStore, MAX_LINE_BYTES, decode_line, iter_lines,
    load_config, parse_observation, parse_ruling, run_audit, run_reconcile,
    validate_config, write_audit_jsonl, write_reconcile_report)
from distribution_drift.model import analyze  # noqa: E402

CONFIG = {'categories': ['a', 'b', 'c'], 'alpha': 1}
DOMAIN = frozenset(CONFIG['categories'])


def jsonl(rows):
    return io.BytesIO(''.join(json.dumps(r, ensure_ascii=False) + '\n' for r in rows).encode())


def write_jsonl(path, rows):
    Path(path).write_bytes(
        ''.join(json.dumps(r, ensure_ascii=False) + '\n' for r in rows).encode())


def reconcile(reference, current, config=CONFIG, rulings=(), workdir=None):
    if isinstance(rulings, dict):
        rulings = [rulings]
    store = run_reconcile(jsonl(reference), jsonl(current), config, jsonl(list(rulings)),
                          workdir=workdir)
    try:
        return store, _render(store, config)
    except Exception:
        store.close()
        raise


def _render(store, config):
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / 'report.json'
        write_reconcile_report(store, path, config['categories'], config['alpha'])
        return json.loads(path.read_text())


class StrictLineContract(unittest.TestCase):
    def parse(self, raw):
        for line in iter_lines(io.BytesIO(raw)):
            parse_observation(decode_line(line), DOMAIN)

    def bad(self, raw):
        with self.assertRaises(CohortError):
            self.parse(raw)

    def test_illegal_lines_rejected(self):
        self.bad(b'\n')
        self.bad(b'   \n')
        self.bad(b'{"entity":"x","entity":"y","category":"a"}\n')
        self.bad(b'{"entity":NaN,"category":"a"}\n')
        self.bad(b'{"entity":"x","category":-Infinity}\n')
        self.bad(b'{"entity":"x","category":"a","extra":1}\n')
        self.bad(b'{"entity":"x"}\n')
        self.bad(b'{"entity":"x","category":"z"}\n')
        self.bad(b'{"entity":"","category":"a"}\n')
        self.bad(b'{"entity":" x","category":"a"}\n')
        self.bad(b'{"entity":1,"category":"a"}\n')
        self.bad(b'{not json}\n')
        self.bad(b'{"entity":"\xff","category":"a"}\n')
        self.bad(b'{"entity":"x","category":true}\n')

    def test_empty_file_is_legal(self):
        store = run_audit(io.BytesIO(b''), io.BytesIO(b''), CONFIG)
        try:
            self.assertEqual(store.raw_rows, {'reference': 0, 'current': 0})
            self.assertEqual(list(store.iter_conflicts()), [])
        finally:
            store.close()

    def test_line_byte_ceiling(self):
        legal = b'{"entity":"x","category":"a"}\n'
        for line in iter_lines(io.BytesIO(legal)):
            parse_observation(decode_line(line), DOMAIN)
        exact = b'x' * (MAX_LINE_BYTES - 1) + b'\n'
        self.assertEqual(len(exact), MAX_LINE_BYTES)
        self.assertEqual(len(list(iter_lines(io.BytesIO(exact)))[0]), MAX_LINE_BYTES)
        over = b'x' * MAX_LINE_BYTES + b'\n'
        with self.assertRaises(CohortError):
            list(iter_lines(io.BytesIO(over)))
        # chunked reader still enforces the ceiling across read() boundaries
        with self.assertRaises(CohortError):
            list(iter_lines(io.BytesIO(b'x' * (MAX_LINE_BYTES + 100) + b'\n')))

    def test_one_shot_nonseekable_iterator_accepted(self):
        def generator():
            for raw in [b'{"entity":"x","category":"a"}\n',
                        b'{"entity":"y","category":"b"}\n']:
                yield raw
        with tempfile.TemporaryDirectory() as td:
            store = run_audit(generator(), io.BytesIO(b''), CONFIG, workdir=td)
            try:
                self.assertEqual(store.raw_rows['reference'], 2)
                self.assertEqual(len(list(Path(td).iterdir())), 1)
            finally:
                store.close()
            self.assertEqual(list(Path(td).iterdir()), [])

    def test_row_cap_and_rollback(self):
        import distribution_drift.cohort as mod
        with tempfile.TemporaryDirectory() as td:
            store = CohortStore(td)
            old = mod.MAX_ROWS_PER_SIDE
            mod.MAX_ROWS_PER_SIDE = 2
            try:
                with self.assertRaises(CohortError):
                    store.ingest_observations(
                        'reference',
                        (b'{"entity":"x","category":"a"}\n' for _ in range(3)), DOMAIN)
                self.assertEqual(
                    store.connection.execute('SELECT COUNT(*) FROM obs').fetchone()[0], 0)
            finally:
                mod.MAX_ROWS_PER_SIDE = old
                store.close()

    def test_config_contract(self):
        with self.assertRaises(CohortError):
            validate_config({'categories': [], 'alpha': 1})
        with self.assertRaises(CohortError):
            validate_config({'categories': ['a', 'a'], 'alpha': 1})
        with self.assertRaises(CohortError):
            validate_config({'categories': ['a'], 'alpha': 0})
        with self.assertRaises(CohortError):
            validate_config({'categories': ['a'], 'alpha': True})
        with self.assertRaises(CohortError):
            validate_config({'categories': ['a'], 'alpha': 1, 'extra': 1})
        domain, alpha, as_set = validate_config(CONFIG)
        self.assertEqual((domain, alpha), (['a', 'b', 'c'], 1))
        self.assertEqual(as_set, DOMAIN)


class Audit(unittest.TestCase):
    def test_distant_repeats_are_folded_with_original_row_counts(self):
        rows = [{'entity': 'x', 'category': 'a'},
                {'entity': 'y', 'category': 'a'},
                {'entity': 'x', 'category': 'a'},
                {'entity': 'x', 'category': 'a'},
                {'entity': 'x', 'category': 'b'}]
        store = run_audit(jsonl(rows), io.BytesIO(b''), CONFIG)
        try:
            conflicts = list(store.iter_conflicts())
        finally:
            store.close()
        self.assertEqual(conflicts, [{
            'side': 'reference', 'entity': 'x',
            'candidates': [{'category': 'a', 'rows': 3},
                           {'category': 'b', 'rows': 1}]}])
        # y is a clean single-category observation and must not be reported
        self.assertEqual(conflicts[0]['candidates'][0]['rows'], 3)

    def test_tie_and_uneven_conflicts_and_unicode_ordering(self):
        rows = [
            {'entity': 'tie', 'category': 'b'}, {'entity': 'tie', 'category': 'b'},
            {'entity': 'tie', 'category': 'a'}, {'entity': 'tie', 'category': 'a'},
            {'entity': 'unequal', 'category': 'c'},
            {'entity': 'unequal', 'category': 'a'},
            {'entity': 'unequal', 'category': 'a'},
            {'entity': 'é', 'category': 'a'}, {'entity': 'é', 'category': 'b'},
        ]
        current = [{'entity': 'tie', 'category': 'a'},
                   {'entity': 'tie', 'category': 'b'}]
        store = run_audit(jsonl(rows), jsonl(current), CONFIG)
        try:
            conflicts = list(store.iter_conflicts())
            audit = store.side_audit()
        finally:
            store.close()
        # sorted by side, then Unicode entity, then category: current before reference
        self.assertEqual([(c['side'], c['entity']) for c in conflicts],
                         [('current', 'tie'),
                          ('reference', 'tie'),
                          ('reference', 'unequal'),
                          ('reference', 'é')])
        self.assertEqual(conflicts[1]['candidates'],
                         [{'category': 'a', 'rows': 2}, {'category': 'b', 'rows': 2}])
        self.assertEqual(conflicts[2]['candidates'],
                         [{'category': 'a', 'rows': 2}, {'category': 'c', 'rows': 1}])
        # no last-row-wins, no frequency vote: both candidates survive regardless
        self.assertEqual(conflicts[0]['candidates'],
                         [{'category': 'a', 'rows': 1}, {'category': 'b', 'rows': 1}])
        self.assertEqual(audit['reference']['conflict_entities'], 3)
        # tie: b,b + a,a = 4 rows folded into 2 candidate rows;
        # unequal: c once + a twice = 3 rows folded into 2; é: 2 rows -> 2
        self.assertEqual(audit['reference']['folded_duplicate_rows'], 3)
        self.assertEqual(audit['reference']['raw_rows'], 9)
        self.assertEqual(audit['current']['conflict_entities'], 1)

    def test_audit_jsonl_output_is_sorted_and_atomic(self):
        rows = [{'entity': 'z', 'category': 'a'}, {'entity': 'z', 'category': 'b'},
                {'entity': 'a', 'category': 'a'}, {'entity': 'a', 'category': 'c'}]
        with tempfile.TemporaryDirectory() as td:
            store = run_audit(jsonl(rows), io.BytesIO(b''), CONFIG)
            try:
                out = Path(td) / 'conflicts.jsonl'
                count = write_audit_jsonl(store, out)
            finally:
                store.close()
            records = [json.loads(line) for line in out.read_text().splitlines()]
            self.assertEqual(count, 2)
            self.assertEqual([r['entity'] for r in records], ['a', 'z'])
            self.assertEqual(set(records[0]), {'side', 'entity', 'candidates'})
            self.assertEqual(set(records[0]['candidates'][0]), {'category', 'rows'})
            # empty conflict set still publishes an empty file
            store = run_audit(jsonl([{'entity': 'x', 'category': 'a'}]),
                              io.BytesIO(b''), CONFIG)
            try:
                empty = Path(td) / 'empty.jsonl'
                self.assertEqual(write_audit_jsonl(store, empty), 0)
                self.assertEqual(empty.read_text(), '')
            finally:
                store.close()


class Rulings(unittest.TestCase):
    def _store(self, reference, rulings, current=()):
        if isinstance(rulings, dict):
            rulings = [rulings]
        return run_reconcile(jsonl(reference), jsonl(list(current)), CONFIG,
                             jsonl(list(rulings)))

    def test_valid_rulings_select_and_null(self):
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'x', 'category': 'b'},
                     {'entity': 'y', 'category': 'b'}, {'entity': 'y', 'category': 'c'}]
        rulings = [
            {'side': 'reference', 'entity': 'x',
             'expected_categories': ['a', 'b'], 'selected': 'a'},
            {'side': 'reference', 'entity': 'y',
             'expected_categories': ['b', 'c'], 'selected': None},
        ]
        with tempfile.TemporaryDirectory() as td:
            store = self._store(reference, rulings)
            try:
                self.assertEqual(store.side_audit()['reference']['selected_conflicts'], 1)
            finally:
                store.close()

    def test_stale_duplicate_mismatched_rulings_rejected(self):
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'x', 'category': 'b'},
                     {'entity': 'clean', 'category': 'a'}]

        def expect(ruling):
            with self.assertRaises(CohortError):
                store = self._store(reference, [ruling])
                store.close()

        expect({'side': 'reference', 'entity': 'missing',
                'expected_categories': ['a', 'b'], 'selected': 'a'})
        expect({'side': 'current', 'entity': 'x',
                'expected_categories': ['a', 'b'], 'selected': 'a'})
        expect({'side': 'reference', 'entity': 'clean',
                'expected_categories': ['a'], 'selected': 'a'})
        expect({'side': 'reference', 'entity': 'x',
                'expected_categories': ['a'], 'selected': 'a'})
        expect({'side': 'reference', 'entity': 'x',
                'expected_categories': ['b', 'a'], 'selected': 'a'})
        expect({'side': 'reference', 'entity': 'x',
                'expected_categories': ['a', 'b'], 'selected': 'c'})
        expect({'side': 'reference', 'entity': 'x',
                'expected_categories': ['a', 'a'], 'selected': 'a'})
        expect({'side': 'reference', 'entity': 'x',
                'expected_categories': ['a', 'b', 'z'], 'selected': 'a'})
        # duplicate identical ruling is also rejected
        good = {'side': 'reference', 'entity': 'x',
                'expected_categories': ['a', 'b'], 'selected': 'a'}
        with self.assertRaises(CohortError):
            store = self._store(reference, [good, good])
            store.close()

    def test_malformed_ruling_lines_rejected(self):
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'x', 'category': 'b'}]
        for raw in [b'\n',
                    b'{"side":"reference","entity":"x","expected_categories":["a","b"]}\n',
                    b'{"side":"middle","entity":"x","expected_categories":["a","b"],"selected":null}\n',
                    b'{"side":"reference","entity":"x","expected_categories":[],"selected":null}\n',
                    b'{"side":"reference","entity":"x","expected_categories":["a","z"],"selected":null}\n']:
            with self.assertRaises(CohortError):
                store = CohortStore()
                try:
                    store.ingest_observations('reference', io.BytesIO(
                        b'{"entity":"x","category":"a"}\n'
                        b'{"entity":"x","category":"b"}\n'), DOMAIN)
                    store.ingest_rulings(io.BytesIO(raw), DOMAIN)
                finally:
                    store.close()

    def test_ruling_parser_unit(self):
        good = {'side': 'reference', 'entity': 'x',
                'expected_categories': ['a', 'b'], 'selected': None}
        self.assertEqual(parse_ruling(good, DOMAIN),
                         ('reference', 'x', ('a', 'b'), None))


class EligibilityAndMigration(unittest.TestCase):
    def test_linked_unresolved_conflict_isolates_both_sides(self):
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'x', 'category': 'b'}]
        current = [{'entity': 'x', 'category': 'c'}]
        _, report = reconcile(reference, current)
        self.assertEqual(report['cells'], [])
        self.assertEqual(report['movement'],
                         {'retained': 0, 'changed': 0, 'entered': 0, 'exited': 0})
        self.assertEqual(report['totals'], {'reference': 0, 'current': 0})
        self.assertEqual(report['excluded_entities'], ['x'])
        self.assertEqual(report['unresolved_entities'], ['x'])
        self.assertEqual(report['explicitly_excluded_entities'], [])
        self.assertEqual(report['distribution_status'], 'insufficient_population')
        self.assertIsNone(report['distribution'])

    def test_selecting_a_creates_exactly_one_retained_changed_cell(self):
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'x', 'category': 'b'}]
        current = [{'entity': 'x', 'category': 'c'}]
        ruling = {'side': 'reference', 'entity': 'x',
                  'expected_categories': ['a', 'b'], 'selected': 'a'}
        _, report = reconcile(reference, current, rulings=[ruling])
        self.assertEqual(report['cells'], [{'from': 'a', 'to': 'c', 'count': 1}])
        self.assertEqual(report['movement'],
                         {'retained': 1, 'changed': 1, 'entered': 0, 'exited': 0})
        self.assertEqual(report['totals'], {'reference': 1, 'current': 1})
        self.assertEqual(report['excluded_entities'], [])
        self.assertEqual(
            report['audit']['reference']['selected_conflicts'], 1)

    def test_null_selection_keeps_global_exclusion(self):
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'x', 'category': 'b'}]
        current = [{'entity': 'x', 'category': 'c'}]
        ruling = {'side': 'reference', 'entity': 'x',
                  'expected_categories': ['a', 'b'], 'selected': None}
        _, report = reconcile(reference, current, rulings=[ruling])
        self.assertEqual(report['cells'], [])
        self.assertEqual(report['excluded_entities'], ['x'])
        self.assertEqual(report['explicitly_excluded_entities'], ['x'])
        self.assertEqual(report['unresolved_entities'], [])
        self.assertEqual(report['totals'], {'reference': 0, 'current': 0})

    def test_mixed_reasons_deduplicate_the_isolation_set(self):
        # x: unresolved on reference, explicitly excluded on current
        # z: unresolved on reference only (current-side single observation must
        #    not become a fake entered cell)
        # w: clean on both sides -> retained
        reference = [
            {'entity': 'x', 'category': 'a'}, {'entity': 'x', 'category': 'b'},
            {'entity': 'z', 'category': 'a'}, {'entity': 'z', 'category': 'b'},
            {'entity': 'w', 'category': 'a'}]
        current = [
            {'entity': 'x', 'category': 'b'}, {'entity': 'x', 'category': 'c'},
            {'entity': 'z', 'category': 'c'},
            {'entity': 'w', 'category': 'a'}]
        rulings = [{'side': 'current', 'entity': 'x',
                    'expected_categories': ['b', 'c'], 'selected': None}]
        _, report = reconcile(reference, current, rulings=rulings)
        self.assertEqual(report['excluded_entities'], ['x', 'z'])
        self.assertEqual(report['unresolved_entities'], ['x', 'z'])
        self.assertEqual(report['explicitly_excluded_entities'], ['x'])
        self.assertEqual(report['cells'], [{'from': 'a', 'to': 'a', 'count': 1}])
        self.assertEqual(report['movement']['retained'], 1)

    def test_resolved_conflict_contributes_one_entity_not_raw_rows(self):
        reference = [{'entity': 'x', 'category': 'a'},
                     {'entity': 'x', 'category': 'a'},
                     {'entity': 'x', 'category': 'a'},
                     {'entity': 'x', 'category': 'b'}]
        current = [{'entity': 'x', 'category': 'a'}]
        ruling = {'side': 'reference', 'entity': 'x',
                  'expected_categories': ['a', 'b'], 'selected': 'a'}
        _, report = reconcile(reference, current, rulings=ruling)
        self.assertEqual(report['cells'], [{'from': 'a', 'to': 'a', 'count': 1}])
        self.assertEqual(report['totals'], {'reference': 1, 'current': 1})
        self.assertEqual(report['audit']['reference']['raw_rows'], 4)
        self.assertEqual(report['audit']['reference']['folded_duplicate_rows'], 2)


class MigrationCellsAndConservation(unittest.TestCase):
    def _conservation(self, report):
        reference_marginals = {}
        current_marginals = {}
        for cell in report['cells']:
            self.assertGreater(cell['count'], 0)
            self.assertFalse(cell['from'] is None and cell['to'] is None)
            if cell['from'] is not None:
                reference_marginals[cell['from']] = \
                    reference_marginals.get(cell['from'], 0) + cell['count']
            if cell['to'] is not None:
                current_marginals[cell['to']] = \
                    current_marginals.get(cell['to'], 0) + cell['count']
        self.assertEqual(reference_marginals, report['reference_marginals'])
        self.assertEqual(current_marginals, report['current_marginals'])
        movement = report['movement']
        self.assertEqual(
            sum(reference_marginals.values()),
            movement['retained'] + movement['exited'])
        self.assertEqual(
            sum(current_marginals.values()),
            movement['retained'] + movement['entered'])
        self.assertEqual(report['totals'],
                         {'reference': sum(reference_marginals.values()),
                          'current': sum(current_marginals.values())})
        self.assertLessEqual(movement['changed'], movement['retained'])

    def test_swap_same_marginals_is_detected_as_changed(self):
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'y', 'category': 'b'}]
        current = [{'entity': 'x', 'category': 'b'}, {'entity': 'y', 'category': 'a'}]
        _, report = reconcile(reference, current)
        self.assertEqual(report['cells'],
                         [{'from': 'a', 'to': 'b', 'count': 1},
                          {'from': 'b', 'to': 'a', 'count': 1}])
        self.assertEqual(report['movement'],
                         {'retained': 2, 'changed': 2, 'entered': 0, 'exited': 0})
        # marginals identical on both sides: the legacy PSI is 0 but the
        # cohort report exposes the full swap
        self.assertEqual(report['reference_marginals'], {'a': 1, 'b': 1})
        self.assertEqual(report['current_marginals'], {'a': 1, 'b': 1})
        self.assertEqual(report['distribution']['psi'], 0.0)
        self._conservation(report)

    def test_pure_entered_and_pure_exited(self):
        _, entered = reconcile(
            [{'entity': 'x', 'category': 'a'}],
            [{'entity': 'x', 'category': 'a'}, {'entity': 'y', 'category': 'b'}])
        self.assertEqual(entered['movement'],
                         {'retained': 1, 'changed': 0, 'entered': 1, 'exited': 0})
        # nulls-first on the from column, then Unicode category order
        self.assertEqual(entered['cells'],
                         [{'from': None, 'to': 'b', 'count': 1},
                          {'from': 'a', 'to': 'a', 'count': 1}])
        self._conservation(entered)
        _, exited = reconcile(
            [{'entity': 'x', 'category': 'a'}, {'entity': 'y', 'category': 'b'}],
            [{'entity': 'x', 'category': 'a'}])
        self.assertEqual(exited['movement'],
                         {'retained': 1, 'changed': 0, 'entered': 0, 'exited': 1})
        self.assertEqual(exited['cells'],
                         [{'from': 'a', 'to': 'a', 'count': 1},
                          {'from': 'b', 'to': None, 'count': 1}])
        self._conservation(exited)

    def test_empty_inputs_never_fabricate_counts(self):
        _, report = reconcile([], [])
        self.assertEqual(report['cells'], [])
        self.assertEqual(report['reference_marginals'], {})
        self.assertEqual(report['current_marginals'], {})
        self.assertEqual(report['totals'], {'reference': 0, 'current': 0})
        self.assertIsNone(report['distribution'])
        self.assertEqual(report['distribution_status'], 'insufficient_population')
        _, one_sided = reconcile([], [{'entity': 'x', 'category': 'a'}])
        self.assertIsNone(one_sided['distribution'])
        self.assertEqual(one_sided['distribution_status'], 'insufficient_population')
        self.assertEqual(one_sided['cells'], [{'from': None, 'to': 'a', 'count': 1}])

    def test_declared_double_zero_category_kept_in_smoothing_domain(self):
        config = {'categories': ['a', 'b', 'c', 'd'], 'alpha': 2}
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'y', 'category': 'b'}]
        current = [{'entity': 'x', 'category': 'b'}, {'entity': 'y', 'category': 'a'}]
        _, report = reconcile(reference, current, config=config)
        baseline = analyze({
            'categories': config['categories'], 'alpha': 2,
            'reference': [{'category': 'a', 'count': 1}, {'category': 'b', 'count': 1}],
            'current': [{'category': 'a', 'count': 1}, {'category': 'b', 'count': 1}]})
        self.assertEqual(report['distribution'], baseline)
        double_zero = [row for row in report['distribution']['categories']
                       if row['category'] == 'd'][0]
        self.assertEqual((double_zero['reference_count'], double_zero['current_count']),
                         (0, 0))
        self.assertEqual((double_zero['p'], double_zero['q']), ('1/5', '1/5'))
        self._conservation(report)

    def test_distribution_matches_analyze_on_general_case(self):
        reference = [{'entity': 'r1', 'category': 'a'},
                     {'entity': 'r2', 'category': 'a'},
                     {'entity': 'r3', 'category': 'b'}]
        current = [{'entity': 'r1', 'category': 'a'},
                   {'entity': 'r3', 'category': 'c'},
                   {'entity': 'n1', 'category': 'c'}]
        config = {'categories': ['a', 'b', 'c'], 'alpha': 3}
        _, report = reconcile(reference, current, config=config)
        baseline = analyze({
            'categories': config['categories'], 'alpha': 3,
            'reference': [{'category': 'a', 'count': 2}, {'category': 'b', 'count': 1}],
            'current': [{'category': 'a', 'count': 1}, {'category': 'c', 'count': 2}]})
        self.assertEqual(report['distribution'], baseline)
        self._conservation(report)

    def test_repeats_and_reordering_do_not_change_results(self):
        reference = [{'entity': 'x', 'category': 'a'}, {'entity': 'y', 'category': 'b'},
                     {'entity': 'x', 'category': 'a'}]
        current = [{'entity': 'y', 'category': 'a'}, {'entity': 'x', 'category': 'b'}]
        _, first = reconcile(reference, current)
        rng = random.Random(4242)
        shuffled_ref = reference[:]
        shuffled_cur = current[:]
        rng.shuffle(shuffled_ref)
        rng.shuffle(shuffled_cur)
        _, second = reconcile(shuffled_ref, shuffled_cur)
        for key in ('cells', 'movement', 'totals', 'reference_marginals',
                    'current_marginals', 'distribution'):
            self.assertEqual(first[key], second[key])
        self.assertEqual(first['audit']['reference']['folded_duplicate_rows'], 1)
        self.assertEqual(first['audit']['reference']['raw_rows'], 3)


class Oracle(unittest.TestCase):
    """Independent in-memory oracle on small randomized cohorts."""

    @staticmethod
    def oracle(reference, current, rulings, categories):
        folded = {'reference': {}, 'current': {}}
        raw = {'reference': 0, 'current': 0}
        for side, rows in (('reference', reference), ('current', current)):
            for row in rows:
                key = (row['entity'], row['category'])
                folded[side].setdefault(key, 0)
                folded[side][key] += 1
                raw[side] += 1
        candidates = {'reference': {}, 'current': {}}
        for side in ('reference', 'current'):
            for (entity, category), count in folded[side].items():
                candidates[side].setdefault(entity, {})[category] = count
        selection = {}
        for ruling in rulings:
            selection[(ruling['side'], ruling['entity'])] = ruling['selected']
        entities = set(candidates['reference']) | set(candidates['current'])
        excluded = set()
        unresolved = set()
        explicit = set()
        effective = {'reference': {}, 'current': {}}
        for entity in entities:
            reasons = False
            for side in ('reference', 'current'):
                cats = candidates[side].get(entity)
                if cats is not None and len(cats) > 1:
                    chosen = selection.get((side, entity), 'UNRESOLVED')
                    if chosen == 'UNRESOLVED':
                        unresolved.add(entity)
                        reasons = True
                    elif chosen is None:
                        explicit.add(entity)
                        reasons = True
            if reasons:
                excluded.add(entity)
                continue
            for side in ('reference', 'current'):
                cats = candidates[side].get(entity)
                if cats is None:
                    continue
                if len(cats) == 1:
                    effective[side][entity] = next(iter(cats))
                else:
                    effective[side][entity] = selection[(side, entity)]
        cells = {}
        for entity in entities - excluded:
            refcat = effective['reference'].get(entity)
            curcat = effective['current'].get(entity)
            cells[(refcat, curcat)] = cells.get((refcat, curcat), 0) + 1
        marginals = {'reference': {}, 'current': {}}
        for (refcat, curcat), count in cells.items():
            if refcat is not None:
                marginals['reference'][refcat] = \
                    marginals['reference'].get(refcat, 0) + count
            if curcat is not None:
                marginals['current'][curcat] = \
                    marginals['current'].get(curcat, 0) + count
        return raw, cells, marginals, excluded, unresolved, explicit

    def test_random_cohorts_match_oracle(self):
        rng = random.Random(20261116)
        categories = ['a', 'b', 'c']
        for seed in range(40):
            rng.seed(9000 + seed)
            entities = ['e%d' % i for i in range(rng.randint(1, 12))]
            reference = []
            current = []
            for entity in entities:
                for side, bucket in (('reference', reference), ('current', current)):
                    if rng.random() < 0.2:
                        continue  # absent on this side
                    cats = rng.sample(categories, rng.randint(1, 2))
                    for category in cats:
                        for _ in range(rng.randint(1, 3)):
                            bucket.append({'entity': entity, 'category': category})
            rulings = []
            for side, rows in (('reference', reference), ('current', current)):
                seen = {}
                for row in rows:
                    seen.setdefault(row['entity'], set()).add(row['category'])
                for entity, cats in seen.items():
                    if len(cats) > 1 and rng.random() < 0.7:
                        chosen = rng.choice(sorted(cats) + [None])
                        rulings.append({'side': side, 'entity': entity,
                                        'expected_categories': sorted(cats),
                                        'selected': chosen})
            raw, cells, marginals, excluded, unresolved, explicit = self.oracle(
                reference, current, rulings, categories)
            store = run_reconcile(jsonl(reference), jsonl(current), CONFIG, jsonl(rulings))
            try:
                report = _render(store, CONFIG)
                audit = store.side_audit()
            finally:
                store.close()
            expected_cells = [{'from': f, 'to': t, 'count': n}
                              for (f, t), n in sorted(
                                      cells.items(),
                                      key=lambda item: (item[0][0] is not None, item[0][0],
                                                        item[0][1] is not None, item[0][1]))]
            self.assertEqual(report['cells'], expected_cells)
            self.assertEqual(report['reference_marginals'], marginals['reference'])
            self.assertEqual(report['current_marginals'], marginals['current'])
            self.assertEqual(report['excluded_entities'], sorted(excluded))
            self.assertEqual(report['unresolved_entities'], sorted(unresolved))
            self.assertEqual(report['explicitly_excluded_entities'], sorted(explicit))
            self.assertEqual(audit['reference']['raw_rows'], raw['reference'])
            self.assertEqual(audit['current']['raw_rows'], raw['current'])
            totals = report['totals']
            movement = report['movement']
            self.assertEqual(totals['reference'],
                             movement['retained'] + movement['exited'])
            self.assertEqual(totals['current'],
                             movement['retained'] + movement['entered'])
            if totals['reference'] and totals['current']:
                baseline = analyze({
                    'categories': categories, 'alpha': 1,
                    'reference': [{'category': c, 'count': v}
                                  for c, v in marginals['reference'].items()],
                    'current': [{'category': c, 'count': v}
                                for c, v in marginals['current'].items()]})
                self.assertEqual(report['distribution'], baseline)
            else:
                self.assertIsNone(report['distribution'])
                self.assertEqual(report['distribution_status'],
                                 'insufficient_population')


class StreamingMemory(unittest.TestCase):
    ENTITIES = 200_000
    RSS_LIMIT = 128 * 1024 * 1024

    def test_multibatch_stream_keeps_rss_under_budget(self):
        driver = ROOT / 'tests' / 'cohort_stream_driver.py'
        with tempfile.TemporaryDirectory() as td:
            output = Path(td) / 'report.json'
            proc = subprocess.run(
                [sys.executable, '-I', str(driver),
                 '--entities', str(self.ENTITIES),
                 '--output', str(output), '--workdir', td],
                capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            payload = json.loads(proc.stdout)
            report = json.loads(output.read_text())
            # every reference entity was an unresolved conflict: fully isolated
            self.assertEqual(report['cells'], [])
            self.assertEqual(len(report['unresolved_entities']), self.ENTITIES)
            self.assertEqual(report['audit']['reference']['raw_rows'],
                             2 * self.ENTITIES)
            self.assertEqual(report['audit']['reference']['conflict_entities'],
                             self.ENTITIES)
            # peak RSS of the child (ru_maxrss is KiB on Linux)
            usage = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * 1024
            self.assertLess(usage, self.RSS_LIMIT,
                            'child peak RSS %d bytes exceeds budget' % usage)
            # the temporary database is removed by close()
            leftovers = [p for p in Path(td).iterdir()
                         if p.name.startswith('cohort-')]
            self.assertEqual(leftovers, [])
            self.assertEqual(payload['audit']['current']['raw_rows'], self.ENTITIES)


class CommandLine(unittest.TestCase):
    def call(self, *args):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(ROOT / 'src')
        return subprocess.run(
            [sys.executable, '-m', 'distribution_drift.cohort_cli', *map(str, args)],
            capture_output=True, text=True, cwd=ROOT, env=env)

    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.config = self.dir / 'config.json'
        self.config.write_text(json.dumps(CONFIG))
        self.reference = self.dir / 'reference.jsonl'
        self.current = self.dir / 'current.jsonl'
        self.rulings = self.dir / 'rulings.jsonl'
        write_jsonl(self.reference, [
            {'entity': 'x', 'category': 'a'}, {'entity': 'x', 'category': 'a'},
            {'entity': 'x', 'category': 'b'},
            {'entity': 'y', 'category': 'b'},
            {'entity': 'z', 'category': 'a'}])
        write_jsonl(self.current, [
            {'entity': 'x', 'category': 'c'},
            {'entity': 'y', 'category': 'b'},
            {'entity': 'q', 'category': 'c'}])
        write_jsonl(self.rulings, [
            {'side': 'reference', 'entity': 'x',
             'expected_categories': ['a', 'b'], 'selected': 'a'}])

    def tearDown(self):
        self.td.cleanup()

    def test_audit_and_reconcile_end_to_end(self):
        conflicts = self.dir / 'conflicts.jsonl'
        result = self.call('audit', '--reference', self.reference,
                           '--current', self.current, '--config', self.config,
                           '--output', conflicts)
        self.assertEqual(result.returncode, 0, result.stderr)
        records = [json.loads(line) for line in conflicts.read_text().splitlines()]
        self.assertEqual([(r['side'], r['entity']) for r in records],
                         [('reference', 'x')])
        self.assertEqual(records[0]['candidates'],
                         [{'category': 'a', 'rows': 2}, {'category': 'b', 'rows': 1}])
        summary = json.loads(result.stdout)
        self.assertEqual(summary['status'], 'audited')

        report_path = self.dir / 'report.json'
        result = self.call('reconcile', '--reference', self.reference,
                           '--current', self.current, '--config', self.config,
                           '--rulings', self.rulings, '--output', report_path)
        self.assertEqual(result.returncode, 0, result.stderr)
        report = json.loads(report_path.read_text())
        # x resolved a -> c (changed, one entity despite 3 raw rows);
        # y retained b->b; z (clean reference a) exited; q (current-only c) entered.
        # Cell order is nulls-first on each side, then Unicode category order.
        self.assertEqual(report['cells'], [
            {'from': None, 'to': 'c', 'count': 1},
            {'from': 'a', 'to': None, 'count': 1},
            {'from': 'a', 'to': 'c', 'count': 1},
            {'from': 'b', 'to': 'b', 'count': 1}])
        self.assertEqual(report['movement'],
                         {'retained': 2, 'changed': 1, 'entered': 1, 'exited': 1})
        self.assertEqual(report['totals'], {'reference': 3, 'current': 3})
        self.assertEqual(report['reference_marginals'], {'a': 2, 'b': 1})
        self.assertEqual(report['current_marginals'], {'b': 1, 'c': 2})
        self.assertEqual(report['distribution_status'], 'ok')
        self.assertEqual(json.loads(result.stdout)['status'], 'reconciled')

    def test_failure_preserves_target_and_help_documents_resources(self):
        target = self.dir / 'report.json'
        target.write_text('PRIOR')
        bad = self.dir / 'bad.jsonl'
        bad.write_text('{"entity":"x","category":"a"}\n\n')
        result = self.call('audit', '--reference', bad, '--current', self.current,
                           '--config', self.config, '--output', target)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(target.read_text(), 'PRIOR')
        # bad ruling fails the whole reconcile and preserves the target
        self.rulings.write_text(
            '{"side":"reference","entity":"ghost",'
            '"expected_categories":["a","b"],"selected":"a"}\n')
        result = self.call('reconcile', '--reference', self.reference,
                           '--current', self.current, '--config', self.config,
                           '--rulings', self.rulings, '--output', target)
        self.assertEqual(result.returncode, 2)
        self.assertIn('does not exist', result.stderr)
        self.assertEqual(target.read_text(), 'PRIOR')
        # same path and symlink boundaries
        result = self.call('audit', '--reference', self.reference,
                           '--current', self.current, '--config', self.config,
                           '--output', self.reference)
        self.assertEqual(result.returncode, 2)
        link = self.dir / 'link.jsonl'
        link.symlink_to(self.reference)
        result = self.call('audit', '--reference', self.reference,
                           '--current', self.current, '--config', self.config,
                           '--output', link)
        self.assertEqual(result.returncode, 2)
        help_text = self.call('--help').stdout + self.call('reconcile', '--help').stdout
        for token in ('--reference', '--current', '--config', '--rulings',
                      '65536', '10,000,000', '128 MiB', 'SQLite'):
            self.assertIn(token, help_text)

    def test_empty_ruling_file_and_replay_are_stable(self):
        self.rulings.write_text('')
        first = self.dir / 'report.json'
        result = self.call('reconcile', '--reference', self.reference,
                           '--current', self.current, '--config', self.config,
                           '--rulings', self.rulings, '--output', first)
        self.assertEqual(result.returncode, 0, result.stderr)
        payload = first.read_bytes()
        result = self.call('reconcile', '--reference', self.reference,
                           '--current', self.current, '--config', self.config,
                           '--rulings', self.rulings, '--output', first)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(first.read_bytes(), payload)
        # unresolved x is isolated both sides; its current c cell never appears
        report = json.loads(payload)
        # unresolved x is isolated on both sides: no x-derived cell exists;
        # the surviving null->c cell belongs to clean current-only entity q
        self.assertNotIn(
            {'from': 'a', 'to': 'c', 'count': 1}, report['cells'])
        self.assertEqual(report['unresolved_entities'], ['x'])
        self.assertEqual(report['excluded_entities'], ['x'])
        self.assertEqual(
            report['cells'],
            [{'count': 1, 'from': None, 'to': 'c'},
             {'count': 1, 'from': 'a', 'to': None},
             {'count': 1, 'from': 'b', 'to': 'b'}])
        self.assertEqual(report['totals'], {'reference': 2, 'current': 2})


if __name__ == '__main__':
    unittest.main()
