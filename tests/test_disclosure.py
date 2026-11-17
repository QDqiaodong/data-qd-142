"""Tests for the cross-batch safe aggregate publication workbench."""
import json
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from distribution_drift.disclosure import (  # noqa: E402
    Store, DisclosureError, ConflictError, init_db, plan,
    validate_spec, validate_request, parse_spec_bytes, parse_request_bytes,
    MAX_QUERIES)


def spec_obj(categories=('a', 'b', 'c'), counts=(100, 20, 7), protected=('a',)):
    return {'categories': list(categories),
            'counts': [{'category': c, 'count': v} for c, v in zip(categories, counts)],
            'protected': list(protected)}


def request_obj(request_id, queries):
    """queries: list of (id, members, utility)."""
    return {'request_id': request_id,
            'queries': [{'id': qid, 'members': list(members), 'utility': u}
                        for qid, members, u in queries]}


def normalized_spec(**kw):
    return validate_spec(spec_obj(**kw))


def normalized_request(spec, request_id, queries):
    return validate_request(request_obj(request_id, queries),
                            frozenset(spec['categories']))


# ---------------------------------------------------------------------------
# independent exhaustive oracle (separate exact rank implementation)
# ---------------------------------------------------------------------------

def oracle_rank(rows):
    """Exact rank over Q via Fraction Gauss-Jordan elimination."""
    if not rows:
        return 0
    mat = [list(map(Fraction, row)) for row in rows]
    ncols = len(mat[0])
    rank = 0
    for col in range(ncols):
        pivot = None
        for r in range(rank, len(mat)):
            if mat[r][col]:
                pivot = r
                break
        if pivot is None:
            continue
        mat[rank], mat[pivot] = mat[pivot], mat[rank]
        inv = Fraction(1) / mat[rank][col]
        mat[rank] = [x * inv for x in mat[rank]]
        for r in range(len(mat)):
            if r != rank and mat[r][col]:
                factor = mat[r][col]
                mat[r] = [a - factor * b for a, b in zip(mat[r], mat[rank])]
        rank += 1
    return rank


def oracle_safe(history, subset_rows, protected, n):
    base = [tuple(map(Fraction, r)) for r in history] + \
           [tuple(map(Fraction, r)) for r in subset_rows]
    rank = oracle_rank(base)
    for c in protected:
        unit = tuple(Fraction(1) if i == c else Fraction(0) for i in range(n))
        if oracle_rank(base + [unit]) == rank:
            return False
    return True


def oracle_plan(n, history, protected, candidates):
    """Brute-force optimum: max utility, then smallest sorted id tuple."""
    best_utility = -1
    best_ids = None
    m = len(candidates)
    for mask in range(1 << m):
        subset = [candidates[i] for i in range(m) if mask >> i & 1]
        if not oracle_safe(history, [vec for _, vec, _ in subset], protected, n):
            continue
        utility = sum(u for _, _, u in subset)
        ids = tuple(sorted(qid for qid, _, _ in subset))
        if utility > best_utility or \
                (utility == best_utility and (best_ids is None or ids < best_ids)):
            best_utility, best_ids = utility, ids
    return best_ids, best_utility


class SpecValidation(unittest.TestCase):
    def test_valid_spec_normalizes_order(self):
        a = validate_spec(spec_obj())
        b = validate_spec({'categories': ['c', 'a', 'b'],
                           'counts': [{'category': 'b', 'count': 20},
                                      {'category': 'c', 'count': 7},
                                      {'category': 'a', 'count': 100}],
                           'protected': ['a']})
        self.assertEqual(a, b)
        self.assertEqual(a['categories'], ['a', 'b', 'c'])
        self.assertEqual([r['category'] for r in a['counts']], ['a', 'b', 'c'])

    def test_rejects_bad_specs(self):
        def bad(spec):
            with self.assertRaises(DisclosureError):
                validate_spec(spec)
        bad({'categories': ['a', 'b'], 'counts': [], 'protected': ['a'], 'extra': 1})
        bad({'categories': ['a'], 'counts': [{'category': 'a', 'count': 1}],
             'protected': ['a']})                       # below 2 categories
        bad({'categories': ['c%d' % i for i in range(33)],
             'counts': [{'category': 'c%d' % i, 'count': 1} for i in range(33)],
             'protected': ['c0']})                      # above 32 categories
        bad({'categories': ['a', 'a'], 'counts': [{'category': 'a', 'count': 1}],
             'protected': ['a']})                       # duplicate category
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 1}],
             'protected': ['a']})                       # missing count
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 1},
                        {'category': 'b', 'count': 2},
                        {'category': 'x', 'count': 3}],
             'protected': ['a']})                       # undeclared count category
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 1},
                        {'category': 'a', 'count': 2}],
             'protected': ['a']})                       # duplicate count row
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': True},
                        {'category': 'b', 'count': 2}],
             'protected': ['a']})                       # boolean count
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': -1},
                        {'category': 'b', 'count': 2}],
             'protected': ['a']})                       # negative count
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 10 ** 9 + 1},
                        {'category': 'b', 'count': 2}],
             'protected': ['a']})                       # count above 10^9
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 1.5},
                        {'category': 'b', 'count': 2}],
             'protected': ['a']})                       # non-integer count
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 1},
                        {'category': 'b', 'count': 2}],
             'protected': []})                          # empty protected
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 1},
                        {'category': 'b', 'count': 2}],
             'protected': ['a', 'a']})                  # duplicate protected
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 1},
                        {'category': 'b', 'count': 2}],
             'protected': ['x']})                       # protected outside domain
        bad({'categories': ['a', 'b'],
             'counts': [{'category': 'a', 'count': 1},
                        {'category': 'b', 'count': 2}],
             'protected': [' a']})                      # ident whitespace rejected

    def test_strict_json_bytes(self):
        with self.assertRaises(DisclosureError):
            parse_spec_bytes(b'{"categories":["a","b"],"categories":["a","b"],'
                             b'"counts":[],"protected":[]}')
        with self.assertRaises(DisclosureError):
            parse_spec_bytes(b'{"categories":["a","b"],"counts":'
                             b'[{"category":"a","count":NaN},{"category":"b","count":1}],'
                             b'"protected":["a"]}')
        with self.assertRaises(DisclosureError):
            parse_spec_bytes(b'{' + b' ' * 8_000_001 + b'}')


class RequestValidation(unittest.TestCase):
    def setUp(self):
        self.declared = frozenset(['a', 'b', 'c'])

    def test_canonical_order_insensitive(self):
        x = validate_request(request_obj('r', [('q1', ['a', 'b'], 5),
                                               ('q2', ['c'], 1)]), self.declared)
        y = validate_request(request_obj('r', [('q2', ['c'], 1),
                                               ('q1', ['b', 'a'], 5)]), self.declared)
        self.assertEqual(x, y)

    def test_rejects_bad_requests(self):
        def bad(req):
            with self.assertRaises(DisclosureError):
                validate_request(req, self.declared)
        bad({'request_id': 'r'})                                            # missing queries
        bad({'request_id': 'r', 'queries': [], })                           # empty queries
        bad({'request_id': 'r',
             'queries': [{'id': 'q%d' % i, 'members': ['a'], 'utility': 1}
                         for i in range(MAX_QUERIES + 1)]})                 # too many
        bad({'request_id': 'r',
             'queries': [{'id': 'q', 'members': ['a'], 'utility': 1, 'x': 0}]})
        bad({'request_id': 'r',
             'queries': [{'id': 'q', 'members': ['a'], 'utility': 1},
                         {'id': 'q', 'members': ['b'], 'utility': 2}]})     # duplicate id
        bad({'request_id': 'r',
             'queries': [{'id': 'q', 'members': [], 'utility': 1}]})        # empty members
        bad({'request_id': 'r',
             'queries': [{'id': 'q', 'members': ['a', 'a'], 'utility': 1}]})
        bad({'request_id': 'r',
             'queries': [{'id': 'q', 'members': ['x'], 'utility': 1}]})     # outside domain
        bad({'request_id': 'r',
             'queries': [{'id': 'q', 'members': ['a'], 'utility': 0}]})     # utility low
        bad({'request_id': 'r',
             'queries': [{'id': 'q', 'members': ['a'], 'utility': 10 ** 6 + 1}]})
        bad({'request_id': 'r',
             'queries': [{'id': 'q', 'members': ['a'], 'utility': True}]})  # boolean
        bad({'request_id': ' r',
             'queries': [{'id': 'q', 'members': ['a'], 'utility': 1}]})     # ident space

    def test_duplicate_members_allowed_across_queries(self):
        req = validate_request(request_obj('r', [('q1', ['a', 'b'], 1),
                                                 ('q2', ['b', 'a'], 2)]), self.declared)
        self.assertEqual(req['queries'][0]['members'], ['a', 'b'])
        self.assertEqual(req['queries'][1]['members'], ['a', 'b'])


class Planner(unittest.TestCase):
    def test_exact_rational_dependence(self):
        # {a,b}+{a,c}-{b,c} = 2*e_a: only exact rational arithmetic sees it.
        selected, utility = plan(3, [], [0],
                                 [('q1', (1, 1, 0), 5), ('q2', (1, 0, 1), 5),
                                  ('q3', (0, 1, 1), 5)])
        self.assertEqual((selected, utility), (('q1', 'q2'), 10))

    def test_three_way_combination_leaks_but_pairs_safe(self):
        # {a,b}-{b,c}+{c} = e_a; every pair is safe, the triple is not.
        # A pairwise-only checker would wrongly publish all three (15).
        selected, utility = plan(4, [], [0],
                                 [('q1', (1, 1, 0, 0), 5), ('q2', (0, 1, 1, 0), 5),
                                  ('q3', (0, 0, 1, 0), 5)])
        self.assertEqual((selected, utility), (('q1', 'q2'), 10))

    def test_duplicate_members_both_selected(self):
        selected, utility = plan(3, [], [0],
                                 [('q1', (0, 1, 0), 3), ('q2', (0, 1, 0), 4),
                                  ('q3', (0, 0, 1), 2)])
        self.assertEqual((selected, utility), (('q1', 'q2', 'q3'), 9))

    def test_known_row_republish_counts_utility(self):
        # {a,b} already published: asking it again adds no knowledge and is
        # free utility; {b} would combine with it into e_a and is suppressed.
        selected, utility = plan(3, [(1, 1, 0)], [0],
                                 [('q1', (1, 1, 0), 7), ('q2', (0, 1, 0), 100)])
        self.assertEqual((selected, utility), (('q1',), 7))

    def test_candidate_in_historical_span_is_free(self):
        # {a,b,c} is the sum of published {a,b} and {c}: inside the span.
        selected, utility = plan(4, [(1, 1, 0, 0), (0, 0, 1, 0)], [0],
                                 [('q1', (1, 1, 1, 0), 4), ('q2', (0, 1, 0, 0), 9)])
        self.assertEqual((selected, utility), (('q1',), 4))

    def test_greedy_utility_counterexample(self):
        # protected {a,b}: q1={a,c} (10) conflicts with q2={c} via e_a and
        # with q3={a,b,c} via e_b; q2+q3 are jointly safe and worth 12 > 10.
        selected, utility = plan(4, [], [0, 1],
                                 [('q1', (1, 0, 1, 0), 10), ('q2', (0, 0, 1, 0), 6),
                                  ('q3', (1, 1, 1, 0), 6)])
        self.assertEqual((selected, utility), (('q2', 'q3'), 12))

    def test_tie_break_single_positions(self):
        # equal utility, mutually exclusive: the smaller id tuple wins,
        # regardless of input order.
        selected, utility = plan(3, [], [0],
                                 [('beta', (1, 1, 0), 5), ('alpha', (0, 1, 0), 5)])
        self.assertEqual((selected, utility), (('alpha',), 5))

    def test_tie_break_compares_element_wise(self):
        # ('x0','x9') < ('x1',): a longer tuple can win on the first element.
        selected, utility = plan(4, [], [0],
                                 [('x9', (1, 1, 0, 0), 2), ('x1', (0, 1, 0, 0), 5),
                                  ('x0', (1, 1, 0, 0), 3)])
        self.assertEqual((selected, utility), (('x0', 'x9'), 5))

    def test_all_suppressed_empty_selection(self):
        selected, utility = plan(2, [], [0],
                                 [('q1', (1, 0), 5), ('q2', (1, 0), 3)])
        self.assertEqual((selected, utility), ((), 0))

    def test_history_invariant_violation_detected(self):
        with self.assertRaises(DisclosureError):
            plan(2, [(1, 0)], [0], [('q1', (0, 1), 1)])


class PlannerOracle(unittest.TestCase):
    def test_random_against_exhaustive_oracle(self):
        rng = random.Random(20261117)
        for trial in range(400):
            n = rng.randint(2, 6)
            protected = rng.sample(range(n), rng.randint(1, min(2, n)))
            m = rng.randint(1, 10)
            candidates = []
            for j in range(m):
                members = sorted(rng.sample(range(n), rng.randint(1, n)))
                vec = tuple(1 if i in members else 0 for i in range(n))
                candidates.append(('q%d' % j, vec, rng.randint(1, 9)))
            history = []
            for _ in range(rng.randint(0, 3)):
                members = sorted(rng.sample(range(n), rng.randint(1, n)))
                vec = tuple(1 if i in members else 0 for i in range(n))
                if oracle_safe(history, [vec], protected, n):
                    history.append(vec)
            expected = oracle_plan(n, history, protected, candidates)
            got = plan(n, history, protected, candidates)
            self.assertEqual(got, expected,
                             'trial %d: n=%d protected=%s history=%s candidates=%s'
                             % (trial, n, protected, history, candidates))

    def test_random_with_duplicate_rows_and_zero_counts(self):
        rng = random.Random(7)
        for trial in range(200):
            n = rng.randint(2, 5)
            protected = rng.sample(range(n), rng.randint(1, n))
            m = rng.randint(1, 9)
            candidates = []
            for j in range(m):
                members = sorted(rng.sample(range(n), rng.randint(1, n)))
                vec = tuple(1 if i in members else 0 for i in range(n))
                candidates.append(('q%d' % j, vec, rng.randint(1, 4)))
            expected = oracle_plan(n, [], protected, candidates)
            got = plan(n, [], protected, candidates)
            self.assertEqual(got, expected, 'trial %d' % trial)

    def test_random_conflicting_pairs_uniform_utility(self):
        # tie-heavy instances: many equal-utility optimal sets exercise the
        # lexicographic tuple pruning against the exhaustive oracle.
        rng = random.Random(99)
        for trial in range(120):
            pairs = rng.randint(1, 5)
            n = pairs + 1
            protected = [0]
            candidates = []
            for i in range(pairs):
                a = [0] * n
                a[0] = 1
                a[i + 1] = 1
                b = [0] * n
                b[i + 1] = 1
                u = rng.randint(1, 3)
                candidates.append(('a%d' % i, tuple(a), u))
                candidates.append(('b%d' % i, tuple(b), u))
            rng.shuffle(candidates)
            expected = oracle_plan(n, [], protected, candidates)
            got = plan(n, [], protected, candidates)
            self.assertEqual(got, expected, 'trial %d' % trial)


class Ledger(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / 'w.db'
        self.spec = normalized_spec()
        self.store, created = init_db(self.db, self.spec)
        self.assertTrue(created)

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def equations(self):
        return self.store.connection.execute(
            'SELECT request_id, query_id, members, value FROM equation'
            ' ORDER BY query_id').fetchall()

    def generation(self):
        return self.store._generation()

    def test_init_idempotent_order_insensitive_any_difference_rejected(self):
        same = validate_spec({'categories': ['c', 'b', 'a'],
                              'counts': [{'category': 'c', 'count': 7},
                                         {'category': 'a', 'count': 100},
                                         {'category': 'b', 'count': 20}],
                              'protected': ['a']})
        self.assertFalse(self.store.configure(same))
        for changed in (normalized_spec(counts=(100, 21, 7)),
                        normalized_spec(protected=('b',)),
                        normalized_spec(categories=('a', 'b', 'c', 'd'),
                                        counts=(100, 20, 7, 1))):
            with self.assertRaises(DisclosureError):
                self.store.configure(changed)

    def test_release_receipt_roundtrip(self):
        req = normalized_request(self.spec, 'r1', [('q-ab', ['a', 'b'], 5)])
        receipt, text, idempotent = self.store.release(req)
        self.assertFalse(idempotent)
        self.assertEqual(receipt['generation'], 0)
        self.assertEqual(receipt['total_utility'], 5)
        self.assertEqual(receipt['selected'],
                         [{'id': 'q-ab', 'members': ['a', 'b'], 'value': 120}])
        self.assertEqual(self.generation(), 1)
        self.assertEqual(self.store.receipt('r1'), text)
        again, text2, idempotent = self.store.release(req)
        self.assertTrue(idempotent)
        self.assertEqual(text2, text)
        self.assertEqual(len(self.equations()), 1)

    def test_canonical_order_replay_byte_identical(self):
        req1 = normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 5),
                                                    ('q2', ['c'], 1)])
        _, text1, _ = self.store.release(req1)
        req2 = normalized_request(self.spec, 'r1', [('q2', ['c'], 1),
                                                    ('q1', ['b', 'a'], 5)])
        _, text2, idempotent = self.store.release(req2)
        self.assertTrue(idempotent)
        self.assertEqual(text1, text2)
        self.assertEqual(len(self.equations()), 2)

    def test_same_id_different_request_rejected(self):
        req = normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 5)])
        self.store.release(req)
        for changed in (normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 6)]),
                        normalized_request(self.spec, 'r1', [('q1', ['a', 'c'], 5)]),
                        normalized_request(self.spec, 'r1', [('q2', ['a', 'b'], 5)])):
            with self.assertRaises(ConflictError):
                self.store.release(changed)
        self.assertEqual(len(self.equations()), 1)
        self.assertEqual(self.generation(), 1)

    def test_history_is_permanent_across_releases(self):
        self.store.release(normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 5)]))
        self.store.release(normalized_request(self.spec, 'r2', [('q2', ['c'], 1)]))
        # many releases later the first equation still constrains:
        receipt, _, _ = self.store.release(
            normalized_request(self.spec, 'r3', [('q3', ['b'], 9)]))
        self.assertEqual(receipt['selected'], [])
        self.assertEqual(receipt['total_utility'], 0)

    def test_empty_set_receipt_recorded_and_replayed(self):
        self.store.release(normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 5)]))
        receipt, text, idempotent = self.store.release(
            normalized_request(self.spec, 'r2', [('q2', ['b'], 9)]))
        self.assertFalse(idempotent)
        self.assertEqual(receipt['selected'], [])
        self.assertEqual(receipt['generation'], 1)
        # empty release records the request but does not move the generation
        self.assertEqual(self.generation(), 1)
        again, text2, idempotent = self.store.release(
            normalized_request(self.spec, 'r2', [('q2', ['b'], 9)]))
        self.assertTrue(idempotent)
        self.assertEqual(text2, text)
        self.assertEqual(self.store.receipt('r2'), text)

    def test_receipt_has_no_private_fields(self):
        spec = validate_spec(spec_obj(counts=(987654321, 12345, 7)))
        store, _ = init_db(Path(self.temp.name) / 'private.db', spec)
        try:
            req = normalized_request(spec, 'req-1', [('q1', ['a', 'b'], 5),
                                                     ('q2', ['b'], 4)])
            receipt, text, _ = store.release(req)
            self.assertEqual(set(receipt), {'generation', 'request_id',
                                            'selected', 'total_utility'})
            self.assertEqual(set(receipt['selected'][0]), {'id', 'members', 'value'})
            self.assertNotIn('q2', text)               # suppressed query absent
            self.assertNotIn('987654321', text)        # private count of a absent
            self.assertNotIn('12345', text)            # private count of b absent
            self.assertEqual(receipt['selected'][0]['value'], 987654321 + 12345)
        finally:
            store.close()

    def test_replay_returns_original_bytes_even_after_knowledge_changed(self):
        req1 = normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 5)])
        _, text1, _ = self.store.release(req1)
        # later releases move the knowledge forward
        self.store.release(normalized_request(self.spec, 'r2', [('q2', ['c'], 1)]))
        self.assertEqual(self.generation(), 2)
        # replaying r1 returns the original receipt bytes, never re-planned
        _, text1b, idempotent = self.store.release(req1)
        self.assertTrue(idempotent)
        self.assertEqual(text1b, text1)
        self.assertEqual(json.loads(text1b)['generation'], 0)

    def test_preview_tracks_history_generation(self):
        req1 = normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 5)])
        before = self.store.preview(req1)
        self.store.release(req1)
        after = self.store.preview(normalized_request(self.spec, 'r3', [('q3', ['b'], 9)]))
        self.assertEqual((before['generation'], after['generation']), (0, 1))
        self.assertEqual(after['selected_ids'], [])  # {b} now unsafe

    def test_restart_recovers_committed_receipt(self):
        req = normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 5)])
        _, text, _ = self.store.release(req)
        self.store.close()
        self.store = Store(self.db)
        again, text2, idempotent = self.store.release(req)
        self.assertTrue(idempotent)
        self.assertEqual(text2, text)
        self.assertEqual(self.store.receipt('r1'), text)
        self.assertEqual(len(self.equations()), 1)

    def test_preview_registers_nothing_and_returns_no_values(self):
        req = normalized_request(self.spec, 'r1', [('q1', ['a', 'b'], 5),
                                                   ('q2', ['b'], 4)])
        preview = self.store.preview(req)
        self.assertEqual(preview, {'request_id': 'r1', 'generation': 0,
                                   'selected_ids': ['q1'], 'total_utility': 5})
        self.assertNotIn('value', json.dumps(preview))
        self.assertEqual(len(self.equations()), 0)
        self.assertEqual(self.store.connection.execute(
            'SELECT COUNT(*) AS n FROM request').fetchone()['n'], 0)
        # a preview does not grant release eligibility; release re-plans
        receipt, _, _ = self.store.release(req)
        self.assertEqual([r['id'] for r in receipt['selected']], ['q1'])
        self.assertEqual(receipt['generation'], preview['generation'])

    def test_invalid_request_leaves_ledger_untouched(self):
        # contract violations are rejected before any ledger write
        with self.assertRaises(DisclosureError):
            validate_request(request_obj('r1', [('q1', ['x'], 5)]),
                             frozenset(self.spec['categories']))
        with self.assertRaises(DisclosureError):
            self.store.release_bytes(b'{"request_id":"r1","queries":'
                                     b'[{"id":"q1","members":["a"],"utility":0}]}')
        self.assertEqual(len(self.equations()), 0)
        self.assertEqual(self.store.connection.execute(
            'SELECT COUNT(*) AS n FROM request').fetchone()['n'], 0)
        with self.assertRaises(DisclosureError):
            self.store.receipt('r1')

    def test_uninitialized_database_rejected(self):
        store = Store(Path(self.temp.name) / 'empty.db')
        try:
            with self.assertRaises(DisclosureError):
                store.preview(normalized_request(self.spec, 'r1', [('q1', ['a'], 1)]))
            with self.assertRaises(DisclosureError):
                store.receipt('r1')
        finally:
            store.close()


class Integration(unittest.TestCase):
    """categories a/b/c, protected=[a]: {a,b} then {b} must be suppressed."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.td = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def make_store(self, name):
        store, _ = init_db(self.td / name, normalized_spec())
        return store

    def test_ab_then_b_suppressed(self):
        with self.make_store('main.db') as store:
            r1, _, _ = store.release(normalized_request(
                store.spec, 'batch-1', [('q-ab', ['a', 'b'], 5)]))
            self.assertEqual([r['id'] for r in r1['selected']], ['q-ab'])
            r2, _, _ = store.release(normalized_request(
                store.spec, 'batch-2', [('q-b', ['b'], 9)]))
            self.assertEqual(r2['selected'], [])
            self.assertEqual(r2['total_utility'], 0)

    def test_only_ab_is_safe(self):
        with self.make_store('only-ab.db') as store:
            receipt, _, _ = store.release(normalized_request(
                store.spec, 'batch-1', [('q-ab', ['a', 'b'], 5)]))
            self.assertEqual([r['id'] for r in receipt['selected']], ['q-ab'])

    def test_only_b_is_safe(self):
        with self.make_store('only-b.db') as store:
            receipt, _, _ = store.release(normalized_request(
                store.spec, 'batch-1', [('q-b', ['b'], 9)]))
            self.assertEqual([r['id'] for r in receipt['selected']], ['q-b'])

    def test_three_releases_combine_into_leak(self):
        # {a,b} and {b,c} are each safe when committed; afterwards {c}
        # completes e_a = {a,b} - {b,c} + {c} and must be suppressed.
        with self.make_store('chain.db') as store:
            r1, _, _ = store.release(normalized_request(
                store.spec, 'r1', [('q1', ['a', 'b'], 5)]))
            r2, _, _ = store.release(normalized_request(
                store.spec, 'r2', [('q2', ['b', 'c'], 5)]))
            self.assertEqual(len(r1['selected']), 1)
            self.assertEqual(len(r2['selected']), 1)
            r3, _, _ = store.release(normalized_request(
                store.spec, 'r3', [('q3', ['c'], 5)]))
            self.assertEqual(r3['selected'], [])


class Concurrency(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.td = Path(self.temp.name)
        self.db = self.td / 'w.db'
        store, _ = init_db(self.db, normalized_spec())
        store.close()

    def tearDown(self):
        self.temp.cleanup()

    def test_stale_executor_recomputes_after_recovery(self):
        # Executor A reads the empty history and pauses; executor B commits
        # {a,b} in the meantime; A's stale plan must be discarded and
        # recomputed against the new history instead of overwriting it.
        store_a = Store(self.db)
        store_b = Store(self.db)
        try:
            released_b = []

            def pause(generation):
                if not released_b:
                    receipt, _, _ = store_b.release(normalized_request(
                        store_b.spec, 'B', [('q-ab', ['a', 'b'], 5)]))
                    released_b.append(receipt)

            receipt_a, _, _ = store_a.release(
                normalized_request(store_a.spec, 'A', [('q-b', ['b'], 9)]),
                pause=pause)
            self.assertEqual([r['id'] for r in released_b[0]['selected']], ['q-ab'])
            self.assertEqual(receipt_a['selected'], [])
            self.assertEqual(receipt_a['generation'], 1)  # recomputed, not stale
            rows = store_a.connection.execute('SELECT * FROM equation').fetchall()
            self.assertEqual(len(rows), 1)
        finally:
            store_a.close()
            store_b.close()

    def test_write_transaction_possible_during_solve_pause(self):
        # The exact solve holds no write transaction: a second connection
        # can BEGIN IMMEDIATE and roll back while the first is paused.
        store = Store(self.db)
        events = []

        def pause(generation):
            con = sqlite3.connect(str(self.db), timeout=5.0)
            con.isolation_level = None
            con.execute('BEGIN IMMEDIATE')
            con.execute('ROLLBACK')
            con.close()
            events.append('second-connection-write-tx-ok')

        try:
            receipt, _, _ = store.release(
                normalized_request(store.spec, 'r1', [('q-ab', ['a', 'b'], 5)]),
                pause=pause)
            self.assertEqual(events, ['second-connection-write-tx-ok'])
            self.assertEqual(len(receipt['selected']), 1)
        finally:
            store.close()

    def test_threaded_same_request_registers_once(self):
        barrier = threading.Barrier(2)
        paused = []
        results = []
        errors = []

        def pause(generation):
            if len(paused) < 2:
                paused.append(generation)
                barrier.wait(timeout=10)

        def worker():
            try:
                store = Store(self.db)
                try:
                    results.append(store.release(normalized_request(
                        store.spec, 'r1', [('q-ab', ['a', 'b'], 5)]), pause=pause))
                finally:
                    store.close()
            except Exception as exc:  # pragma: no cover - failure diagnostic
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0][1], results[1][1])  # identical receipt bytes
        self.assertEqual(sorted(r[2] for r in results), [False, True])
        with Store(self.db) as store:
            self.assertEqual(len(store.connection.execute(
                'SELECT * FROM equation').fetchall()), 1)

    def test_threaded_competing_requests_not_both_published(self):
        barrier = threading.Barrier(2)
        paused = []
        results = {}
        errors = []

        def pause(generation):
            if len(paused) < 2:
                paused.append(generation)
                barrier.wait(timeout=10)

        def worker(name, members):
            try:
                store = Store(self.db)
                try:
                    results[name] = store.release(normalized_request(
                        store.spec, name, [('q-' + name, members, 5)]), pause=pause)
                finally:
                    store.close()
            except Exception as exc:  # pragma: no cover - failure diagnostic
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=('A', ['a', 'b'])),
                   threading.Thread(target=worker, args=('B', ['b']))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        selected = {name: len(r[0]['selected']) for name, r in results.items()}
        self.assertEqual(sorted(selected.values()), [0, 1])
        with Store(self.db) as store:
            self.assertEqual(len(store.connection.execute(
                'SELECT * FROM equation').fetchall()), 1)


class TwoProcessRace(unittest.TestCase):
    """Two independent processes plan from the same empty history and race."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.td = Path(self.temp.name)
        self.db = self.td / 'w.db'
        store, _ = init_db(self.db, normalized_spec())
        store.close()
        (self.td / 'req-ab.json').write_text(json.dumps(
            request_obj('batch-ab', [('q-ab', ['a', 'b'], 5)])))
        (self.td / 'req-b.json').write_text(json.dumps(
            request_obj('batch-b', [('q-b', ['b'], 5)])))

    def tearDown(self):
        self.temp.cleanup()

    def driver(self, request, ready, gate, crash='none'):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(ROOT / 'src') + os.pathsep + env.get('PYTHONPATH', '')
        cmd = [sys.executable, str(ROOT / 'tests' / 'disclosure_release_driver.py'),
               '--db', str(self.db), '--request', str(request),
               '--crash', crash, '--ready', str(ready), '--gate', str(gate)]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, env=env)

    def test_two_processes_not_both_published(self):
        gate = self.td / 'gate'
        ready_a = self.td / 'ready-a'
        ready_b = self.td / 'ready-b'
        proc_a = self.driver(self.td / 'req-ab.json', ready_a, gate)
        proc_b = self.driver(self.td / 'req-b.json', ready_b, gate)
        # both processes read the empty history and park at the solve pause
        deadline = time.monotonic() + 30
        while not (ready_a.exists() and ready_b.exists()):
            self.assertLess(time.monotonic(), deadline, 'drivers never reached the pause')
            time.sleep(0.01)
        gate.touch()
        out_a, err_a = proc_a.communicate(timeout=60)
        out_b, err_b = proc_b.communicate(timeout=60)
        self.assertEqual(proc_a.returncode, 0, err_a)
        self.assertEqual(proc_b.returncode, 0, err_b)
        receipt_a = json.loads(out_a)
        receipt_b = json.loads(out_b)
        # exactly one of the two equations became public knowledge
        with Store(self.db) as store:
            rows = store.connection.execute(
                'SELECT query_id, members FROM equation').fetchall()
            self.assertEqual(len(rows), 1)
            published = rows[0]['query_id']
        loser = receipt_b if published == 'q-ab' else receipt_a
        winner = receipt_a if published == 'q-ab' else receipt_b
        self.assertEqual(len(winner['selected']), 1)
        self.assertEqual(loser['selected'], [])
        self.assertEqual(loser['total_utility'], 0)
        # the loser recomputed against the winner's history (generation 1)
        self.assertEqual(loser['generation'], 1)
        self.assertEqual(winner['generation'], 0)


class CrashRecovery(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.td = Path(self.temp.name)
        self.db = self.td / 'w.db'
        store, _ = init_db(self.db, normalized_spec())
        store.close()
        self.request = self.td / 'req.json'
        self.request.write_text(json.dumps(
            request_obj('batch-1', [('q-ab', ['a', 'b'], 5)])))

    def tearDown(self):
        self.temp.cleanup()

    def driver(self, crash):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(ROOT / 'src') + os.pathsep + env.get('PYTHONPATH', '')
        return subprocess.run(
            [sys.executable, str(ROOT / 'tests' / 'disclosure_release_driver.py'),
             '--db', str(self.db), '--request', str(self.request),
             '--crash', crash],
            capture_output=True, text=True, env=env)

    def test_exit_during_solve_neither_blocks_nor_registers(self):
        result = self.driver('during-solve')
        self.assertEqual(result.returncode, 3)
        with Store(self.db) as store:
            self.assertEqual(len(store.connection.execute(
                'SELECT * FROM equation').fetchall()), 0)
            self.assertEqual(len(store.connection.execute(
                'SELECT * FROM request').fetchall()), 0)
        # a later release proceeds normally: the killed solver left no lock
        result = self.driver('none')
        self.assertEqual(result.returncode, 0, result.stderr)
        receipt = json.loads(result.stdout)
        self.assertEqual([r['id'] for r in receipt['selected']], ['q-ab'])

    def test_exit_after_commit_before_output_recovers_original_receipt(self):
        result = self.driver('after-commit')
        self.assertEqual(result.returncode, 4)
        self.assertEqual(result.stdout, '')
        with Store(self.db) as store:
            equations = store.connection.execute('SELECT * FROM equation').fetchall()
            self.assertEqual(len(equations), 1)
            stored = store.receipt('batch-1')
            generation = store._generation()
        # restart with the identical request: original receipt, no re-register
        result = self.driver('none')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, stored)
        with Store(self.db) as store:
            self.assertEqual(len(store.connection.execute(
                'SELECT * FROM equation').fetchall()), 1)
            self.assertEqual(store._generation(), generation)


class Fixtures(unittest.TestCase):
    """The fixtures/disclosure walkthrough replays byte-identical receipts."""

    def test_fixture_workflow_golden_bytes(self):
        fx = ROOT / 'fixtures' / 'disclosure'
        with tempfile.TemporaryDirectory() as td:
            store, created = init_db(Path(td) / 'w.db',
                                     parse_spec_bytes((fx / 'spec.json').read_bytes()))
            self.assertTrue(created)
            declared = frozenset(store.spec['categories'])
            _, text1, _ = store.release(parse_request_bytes(
                (fx / 'request-ab.json').read_bytes(), declared))
            _, text2, _ = store.release(parse_request_bytes(
                (fx / 'request-b.json').read_bytes(), declared))
            store.close()
        self.assertEqual(text1, (fx / 'expected' / 'receipt-ab.json').read_text())
        self.assertEqual(text2, (fx / 'expected' / 'receipt-b.json').read_text())


class CommandLine(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.td = Path(self.temp.name)
        self.db = self.td / 'w.db'
        self.spec_path = self.td / 'spec.json'
        self.spec_path.write_text(json.dumps(spec_obj()))

    def tearDown(self):
        self.temp.cleanup()

    def call(self, *args):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(ROOT / 'src') + os.pathsep + env.get('PYTHONPATH', '')
        return subprocess.run(
            [sys.executable, '-m', 'distribution_drift.disclosure_cli', *map(str, args)],
            capture_output=True, text=True, cwd=ROOT, env=env)

    def write_request(self, name, obj):
        path = self.td / name
        path.write_text(json.dumps(obj))
        return path

    def test_full_cli_workflow(self):
        r = self.call('init', '--db', self.db, '--spec', self.spec_path)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout)['status'], 'initialized')
        r = self.call('init', '--db', self.db, '--spec', self.spec_path)
        self.assertEqual(json.loads(r.stdout)['status'], 'unchanged')
        # a different private vector is rejected
        other = self.td / 'other.json'
        other.write_text(json.dumps(spec_obj(counts=(100, 21, 7))))
        r = self.call('init', '--db', self.db, '--spec', other)
        self.assertEqual(r.returncode, 2)
        # preview: selection and generation, no values, nothing registered
        req_ab = self.write_request('ab.json', request_obj('batch-1', [('q-ab', ['a', 'b'], 5)]))
        r = self.call('preview', '--db', self.db, '--request', req_ab)
        self.assertEqual(r.returncode, 0, r.stderr)
        preview = json.loads(r.stdout)
        self.assertEqual(preview, {'request_id': 'batch-1', 'generation': 0,
                                   'selected_ids': ['q-ab'], 'total_utility': 5})
        # release prints the committed receipt; replay is byte-identical
        r = self.call('release', '--db', self.db, '--request', req_ab)
        self.assertEqual(r.returncode, 0, r.stderr)
        receipt = json.loads(r.stdout)
        self.assertEqual(receipt['selected'],
                         [{'id': 'q-ab', 'members': ['a', 'b'], 'value': 120}])
        r2 = self.call('release', '--db', self.db, '--request', req_ab)
        self.assertEqual(r2.stdout, r.stdout)
        # receipt command reads the stored bytes
        r3 = self.call('receipt', '--db', self.db, '--request-id', 'batch-1')
        self.assertEqual(r3.returncode, 0, r3.stderr)
        self.assertEqual(r3.stdout, r.stdout)
        # the follow-up {b} request is suppressed but still recorded
        req_b = self.write_request('b.json', request_obj('batch-2', [('q-b', ['b'], 9)]))
        r = self.call('release', '--db', self.db, '--request', req_b)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout)['selected'], [])
        r = self.call('receipt', '--db', self.db, '--request-id', 'batch-2')
        self.assertEqual(json.loads(r.stdout)['total_utility'], 0)
        # unknown request id: exit 2
        r = self.call('receipt', '--db', self.db, '--request-id', 'nope')
        self.assertEqual(r.returncode, 2)

    def test_cli_failures_preserve_ledger(self):
        r = self.call('init', '--db', self.db, '--spec', self.spec_path)
        self.assertEqual(r.returncode, 0, r.stderr)
        bad = self.td / 'bad.json'
        bad.write_text('{"request_id":"r","queries":[{"id":"q","members":["x"],"utility":1}]}')
        r = self.call('release', '--db', self.db, '--request', bad)
        self.assertEqual(r.returncode, 2)
        dup = self.td / 'dup.json'
        dup.write_text('{"request_id":"r","request_id":"s","queries":[]}')
        r = self.call('release', '--db', self.db, '--request', dup)
        self.assertEqual(r.returncode, 2)
        r = self.call('release', '--db', self.td / 'missing.db',
                      '--request', self.spec_path)
        self.assertEqual(r.returncode, 2)
        with Store(self.db) as store:
            self.assertEqual(len(store.connection.execute(
                'SELECT * FROM request').fetchall()), 0)


if __name__ == '__main__':
    unittest.main()
