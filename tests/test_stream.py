"""Tests for the restartable event-time drift monitoring pipeline."""
import io
import json
import math
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from fractions import Fraction
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from distribution_drift import stream  # noqa: E402
from distribution_drift.model import analyze  # noqa: E402
from distribution_drift.stream import (  # noqa: E402
    PayloadConflict, Store, StreamError, init_db, iter_lines, parse_record,
    validate_config)

CONFIG = {'sources': ['A', 'B'], 'categories': ['x', 'y'], 'alpha': 1, 'width': 10}
SOURCES = frozenset(CONFIG['sources'])
DOMAIN = frozenset(CONFIG['categories'])


def jsonl(rows):
    return io.BytesIO(''.join(json.dumps(r) + '\n' for r in rows).encode())


def ev(source, seq, ts, category='x'):
    return {'source': source, 'seq': seq, 'kind': 'event', 'ts': ts, 'category': category}


def wm(source, seq, time):
    return {'source': source, 'seq': seq, 'kind': 'watermark', 'time': time}


class PipelineCase(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / 'pipeline.db'
        self.store, created = init_db(self.db, CONFIG)
        assert created

    def tearDown(self):
        self.store.close()
        self.temp.cleanup()

    def apply(self, rows):
        return self.store.apply(jsonl(rows))

    def reinit(self, config):
        """Swap in a fresh database with a different config."""
        self.store.close()
        self.db = Path(self.temp.name) / ('pipeline-%d.db' % id(config))
        self.store, _ = init_db(self.db, config)

    def source(self, name):
        return next(s for s in self.store.status()['sources'] if s['name'] == name)

    def export(self):
        self.out = Path(self.temp.name) / 'out'
        result = self.store.export(self.out)
        return result

    def report(self, end):
        return json.loads((self.out / ('window-%d.json' % end)).read_text())


class ConfigValidation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.db = Path(self.temp.name) / 'p.db'

    def tearDown(self):
        self.temp.cleanup()

    def good(self, **override):
        config = dict(CONFIG)
        config.update(override)
        return config

    def test_valid_config_normalizes(self):
        config = validate_config(self.good())
        self.assertEqual(config, CONFIG)

    def test_exact_field_set_required(self):
        for bad in (self.good(extra=1), {k: v for k, v in CONFIG.items() if k != 'width'},
                    ['not', 'a', 'dict']):
            with self.assertRaises(StreamError):
                validate_config(bad)

    def test_source_bounds(self):
        with self.assertRaises(StreamError):
            validate_config(self.good(sources=[]))
        with self.assertRaises(StreamError):
            validate_config(self.good(sources=['s%d' % i for i in range(65)]))
        with self.assertRaises(StreamError):
            validate_config(self.good(sources=['A', 'A']))
        with self.assertRaises(StreamError):
            validate_config(self.good(sources=[' A']))
        validate_config(self.good(sources=['s%d' % i for i in range(64)]))

    def test_category_and_alpha_rules_match_analyze(self):
        with self.assertRaises(StreamError):
            validate_config(self.good(categories=[]))
        with self.assertRaises(StreamError):
            validate_config(self.good(categories=['x', 'x']))
        with self.assertRaises(StreamError):
            validate_config(self.good(alpha=0))
        with self.assertRaises(StreamError):
            validate_config(self.good(alpha=10 ** 6 + 1))
        with self.assertRaises(StreamError):
            validate_config(self.good(alpha=True))
        validate_config(self.good(alpha=10 ** 6))

    def test_width_bounds(self):
        for bad in (0, 10 ** 6 + 1, 1.5, '10', True, None):
            with self.assertRaises(StreamError):
                validate_config(self.good(width=bad))
        validate_config(self.good(width=1))
        validate_config(self.good(width=10 ** 6))

    def test_reinit_same_config_idempotent_different_rejected(self):
        store, created = init_db(self.db, CONFIG)
        store.close()
        store, created = init_db(self.db, json.loads(json.dumps(CONFIG)))
        self.assertFalse(created)
        store.close()
        with self.assertRaises(StreamError):
            init_db(self.db, self.good(width=20))
        with self.assertRaises(StreamError):
            init_db(self.db, self.good(sources=['A']))

    def test_uninitialized_database_rejected(self):
        store = Store(self.db)
        with self.assertRaises(StreamError):
            store.status()
        with self.assertRaises(StreamError):
            store.apply(jsonl([]))
        with self.assertRaises(StreamError):
            store.export(Path(self.temp.name) / 'out')
        store.close()


class StrictLineContract(PipelineCase):
    def bad(self, raw):
        with self.assertRaises(StreamError):
            self.store.apply(io.BytesIO(raw))
        self.assertEqual(self.source('A')['committed_seq'], 0)

    def test_blank_and_whitespace_lines(self):
        self.bad(b'\n')
        self.bad(b'   \n')
        self.bad(b'{"source":"A","seq":1,"kind":"watermark","time":1}\n\n')

    def test_duplicate_keys_and_nonfinite_constants(self):
        self.bad(b'{"source":"A","source":"A","seq":1,"kind":"watermark","time":1}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"watermark","time":NaN}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":Infinity,"category":"x"}\n')

    def test_unknown_and_missing_fields(self):
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":1,"category":"x","extra":0}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":1}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"watermark","time":1,"ts":1}\n')
        self.bad(b'{"source":"A","seq":1,"ts":1,"category":"x"}\n')

    def test_oversized_line_and_bad_utf8(self):
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":1,"category":"'
                 + b'x' * 65536 + b'"}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":1,"category":"\xff"}\n')

    def test_strict_integer_fields(self):
        self.bad(b'{"source":"A","seq":true,"kind":"watermark","time":1}\n')
        self.bad(b'{"source":"A","seq":1.0,"kind":"watermark","time":1}\n')
        self.bad(b'{"source":"A","seq":0,"kind":"watermark","time":1}\n')
        self.bad(b'{"source":"A","seq":1000000000001,"kind":"watermark","time":1}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":-1,"category":"x"}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":1000000000001,"category":"x"}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"watermark","time":-1}\n')

    def test_boundary_values_accepted(self):
        summary = self.apply([wm('A', 1, 10 ** 12), ev('B', 1, 0, 'x'),
                              wm('B', 2, 0)])
        self.assertEqual(summary['watermarks'], 2)
        self.assertEqual(self.source('A')['watermark'], 10 ** 12)
        # seq = 10^12 is the largest legal sequence number
        record = parse_record(wm('A', 10 ** 12, 10 ** 12), SOURCES, DOMAIN)
        self.assertEqual(record.seq, 10 ** 12)
        record = parse_record(ev('A', 1, 10 ** 12, 'x'), SOURCES, DOMAIN)
        self.assertEqual(record.time, 10 ** 12)

    def test_unknown_source_category_and_kind(self):
        self.bad(b'{"source":"C","seq":1,"kind":"watermark","time":1}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":1,"category":"zz"}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"snapshot","time":1}\n')
        self.bad(b'{"source":"A","seq":1,"kind":"event","ts":1,"category":7}\n')

    def test_parse_record_returns_canonical_fields(self):
        record = parse_record({'source': 'A', 'seq': 3, 'kind': 'event',
                               'ts': 9, 'category': 'x'}, SOURCES, DOMAIN)
        self.assertEqual(tuple(record), ('A', 3, 'event', 9, 'x'))
        record = parse_record({'kind': 'watermark', 'time': 4, 'seq': 2,
                               'source': 'B'}, SOURCES, DOMAIN)
        self.assertEqual(tuple(record), ('B', 2, 'watermark', 4, None))

    def test_iter_lines_requires_bytes_and_enforces_ceiling(self):
        with self.assertRaises(StreamError):
            list(iter_lines(['not-bytes']))
        with self.assertRaises(StreamError):
            list(iter_lines(io.BytesIO(b'x' * 65537)))
        # a chunked stream reassembles lines without ever holding the input
        chunks = [b'{"a":1}\n{"b":', b'2}\n', b'{"c":3}']
        self.assertEqual(list(iter_lines(io.BytesIO(b''.join(chunks)))),
                         [b'{"a":1}\n', b'{"b":2}\n', b'{"c":3}'])


class SourceAcceptance(PipelineCase):
    def test_cross_batch_replay_is_idempotent(self):
        self.apply([ev('A', 1, 1), ev('A', 2, 2), wm('A', 3, 5)])
        summary = self.apply([ev('A', 1, 1), ev('A', 2, 2), wm('A', 3, 5), ev('A', 4, 6)])
        self.assertEqual(summary['replayed'], 3)
        self.assertEqual(summary['accepted_events'], 1)
        a = self.source('A')
        self.assertEqual((a['committed_seq'], a['accepted_events'], a['late_events']), (4, 3, 0))

    def test_replay_ignores_whitespace_and_key_order(self):
        self.store.apply(io.BytesIO(b'{"source":"A","seq":1,"kind":"event","ts":5,"category":"x"}\n'))
        pretty = b'{ "category": "x",  "ts": 5, "kind": "event",   "seq": 1, "source": "A" }\n'
        summary = self.store.apply(io.BytesIO(pretty))
        self.assertEqual(summary['replayed'], 1)

    def test_same_seq_different_payload_rejected_and_rolled_back(self):
        self.apply([ev('A', 1, 5)])
        with self.assertRaises(PayloadConflict):
            self.apply([ev('A', 1, 6)])
        with self.assertRaises(PayloadConflict):
            self.apply([ev('A', 1, 5, 'y')])
        with self.assertRaises(PayloadConflict):
            self.apply([wm('A', 1, 5)])  # same seq, different kind
        self.assertEqual(self.source('A')['committed_seq'], 1)
        # the original payload still replays cleanly afterwards
        summary = self.apply([ev('A', 1, 5)])
        self.assertEqual(summary['replayed'], 1)

    def test_sequence_gap_fails_whole_apply(self):
        with self.assertRaises(StreamError):
            self.apply([ev('A', 1, 1), ev('A', 3, 2), ev('B', 1, 1)])
        self.assertEqual(self.source('A')['committed_seq'], 0)
        self.assertEqual(self.source('B')['committed_seq'], 0)
        count = self.store.connection.execute('SELECT COUNT(*) FROM record').fetchone()[0]
        self.assertEqual(count, 0)

    def test_watermark_cannot_decrease(self):
        self.apply([wm('A', 1, 10)])
        with self.assertRaises(StreamError):
            self.apply([wm('A', 2, 9)])
        with self.assertRaises(StreamError):
            self.apply([wm('B', 1, 10), wm('B', 2, 9)])
        self.assertEqual(self.source('A')['watermark'], 10)
        # an equal watermark is a legal no-op advance
        summary = self.apply([wm('A', 2, 10)])
        self.assertEqual(summary['watermarks'], 1)
        self.assertEqual(self.source('A')['watermark'], 10)

    def test_event_ts_equal_to_watermark_is_accepted(self):
        summary = self.apply([wm('A', 1, 10), ev('A', 2, 10)])
        self.assertEqual(summary['accepted_events'], 1)
        self.assertEqual(summary['late_events'], 0)

    def test_same_batch_watermark_then_late_event(self):
        summary = self.apply([wm('A', 1, 10), ev('A', 2, 5, 'y')])
        self.assertEqual(summary['late_events'], 1)
        a = self.source('A')
        self.assertEqual((a['committed_seq'], a['late_events']), (2, 1))
        late = list(self.store.iter_late())
        self.assertEqual(len(late), 1)
        self.assertEqual((late[0]['source'], late[0]['seq'], late[0]['ts'],
                          late[0]['category']), ('A', 2, 5, 'y'))
        self.assertIn('watermark 10', late[0]['reason'])

    def test_replay_is_not_rejudged_against_current_watermark(self):
        self.apply([ev('A', 1, 5), wm('A', 2, 10)])
        summary = self.apply([ev('A', 1, 5)])
        self.assertEqual(summary['replayed'], 1)
        a = self.source('A')
        self.assertEqual((a['accepted_events'], a['late_events']), (1, 0))

    def test_interleaved_sources_keep_independent_sequences(self):
        summary = self.apply([ev('A', 1, 1), ev('B', 1, 2), ev('A', 2, 3),
                              wm('B', 2, 4), ev('A', 3, 5)])
        self.assertEqual(summary['accepted_events'], 4)
        self.assertEqual(self.source('A')['committed_seq'], 3)
        self.assertEqual(self.source('B')['committed_seq'], 2)
        # a gap in one source rolls back the other's progress too
        with self.assertRaises(StreamError):
            self.apply([ev('B', 3, 1), ev('A', 5, 2)])
        self.assertEqual(self.source('B')['committed_seq'], 2)

    def test_same_batch_pending_replay_and_conflict(self):
        summary = self.apply([ev('A', 1, 1), ev('A', 1, 1)])
        self.assertEqual((summary['accepted_events'], summary['replayed']), (1, 1))
        with self.assertRaises(PayloadConflict):
            self.apply([ev('B', 1, 1), ev('B', 1, 2)])

    def test_replay_after_flush_reads_committed_rows(self):
        with mock.patch.object(stream, '_FLUSH_BATCH', 1):
            summary = self.apply([ev('A', 1, 1), ev('A', 2, 2), ev('A', 1, 1)])
        self.assertEqual((summary['accepted_events'], summary['replayed']), (2, 1))

    def test_rollback_covers_already_flushed_rows(self):
        with mock.patch.object(stream, '_FLUSH_BATCH', 2):
            with self.assertRaises(StreamError):
                self.apply([ev('A', 1, 1), ev('A', 2, 2), ev('A', 3, 3), ev('A', 3, 9)])
        self.assertEqual(self.source('A')['committed_seq'], 0)
        count = self.store.connection.execute('SELECT COUNT(*) FROM record').fetchone()[0]
        self.assertEqual(count, 0)

    def test_apply_line_limit(self):
        with mock.patch.object(stream, 'MAX_APPLY_LINES', 3):
            with self.assertRaises(StreamError):
                self.apply([ev('A', 1, 1), ev('A', 2, 2), ev('A', 3, 3), ev('A', 4, 4)])
        self.assertEqual(self.source('A')['committed_seq'], 0)

    def test_apply_accepts_one_shot_generators(self):
        def lines():
            yield json.dumps(ev('A', 1, 1)).encode() + b'\n'
            yield json.dumps(wm('A', 2, 5)).encode() + b'\n'
        summary = self.store.apply(lines())  # no read(), no seek, consumed once
        self.assertEqual(summary['lines'], 2)
        self.assertEqual(self.source('A')['committed_seq'], 2)


class WindowClosing(PipelineCase):
    def test_half_open_interval_boundaries(self):
        self.reinit({'sources': ['S'], 'categories': ['a', 'b'],
                     'alpha': 1, 'width': 10})
        self.apply([ev('S', 1, 0, 'a'), ev('S', 2, 9, 'b'), ev('S', 3, 10, 'a'),
                    ev('S', 4, 19, 'a'), ev('S', 5, 20, 'b'), ev('S', 6, 29, 'b'),
                    wm('S', 7, 40)])
        self.export()
        r20 = self.report(20)
        self.assertEqual((r20['reference']['counts'], r20['current']['counts']),
                         ({'a': 1, 'b': 1}, {'a': 2, 'b': 0}))
        r30 = self.report(30)
        self.assertEqual((r30['reference']['counts'], r30['current']['counts']),
                         ({'a': 2, 'b': 0}, {'a': 0, 'b': 2}))
        r40 = self.report(40)
        self.assertEqual((r40['reference']['counts'], r40['current']['counts']),
                         ({'a': 0, 'b': 2}, {'a': 0, 'b': 0}))
        self.assertEqual(r40['distribution_status'], 'insufficient_population')
        self.assertIsNone(r40['distribution'])
        self.assertEqual(r40['window'], {'end': 40, 'width': 10,
                                         'reference': {'start': 20, 'end': 30},
                                         'current': {'start': 30, 'end': 40}})

    def test_double_zero_categories_stay_in_smoothed_domain(self):
        self.reinit({'sources': ['S'], 'categories': ['a', 'b', 'c'],
                     'alpha': 3, 'width': 10})
        self.apply([ev('S', 1, 1, 'a'), ev('S', 2, 11, 'a'), wm('S', 3, 20)])
        self.export()
        distribution = self.report(20)['distribution']
        entry = next(c for c in distribution['categories'] if c['category'] == 'c')
        self.assertEqual((entry['reference_count'], entry['current_count']), (0, 0))
        self.assertEqual(entry['p'], entry['q'])
        self.assertEqual(entry['psi_contribution'], 0.0)
        self.assertEqual(distribution['appeared'], [])
        self.assertEqual(distribution['disappeared'], [])

    def test_one_side_empty_still_reports_full_frequencies(self):
        self.reinit({'sources': ['S'], 'categories': ['a', 'b'],
                     'alpha': 1, 'width': 10})
        self.apply([ev('S', 1, 2, 'a'), ev('S', 2, 3, 'a'), wm('S', 3, 20)])
        self.export()
        report = self.report(20)
        self.assertEqual(report['distribution_status'], 'insufficient_population')
        self.assertIsNone(report['distribution'])
        self.assertEqual(report['reference'], {'total': 2, 'counts': {'a': 2, 'b': 0}})
        self.assertEqual(report['current'], {'total': 0, 'counts': {'a': 0, 'b': 0}})

    def test_both_empty_windows_produce_no_file(self):
        self.reinit({'sources': ['S'], 'categories': ['a'], 'alpha': 1, 'width': 10})
        # data in [0,10) and [30,40): window 30 is empty on both sides
        self.apply([ev('S', 1, 0, 'a'), ev('S', 2, 35, 'a'), wm('S', 3, 45)])
        result = self.export()
        self.assertEqual(result['published'], 2)
        self.assertTrue((self.out / 'window-20.json').exists())
        self.assertFalse((self.out / 'window-30.json').exists())
        self.assertTrue((self.out / 'window-40.json').exists())
        self.assertEqual(self.store.status()['next_window_end'], 50)

    def test_huge_empty_span_is_not_enumerated(self):
        self.reinit({'sources': ['S'], 'categories': ['a'], 'alpha': 1, 'width': 10})
        summary = self.apply([ev('S', 1, 0, 'a'), wm('S', 2, 10 ** 9)])
        self.assertEqual(summary['windows_closed'], 10 ** 8 - 1)
        self.assertEqual(summary['reports_generated'], 1)  # only E=20 has data
        status = self.store.status()
        self.assertEqual(status['next_window_end'], 10 ** 9 + 10)
        self.assertEqual(status['reports'], {'pending': 1, 'delivered': 0})
        # fully consumed buckets are pruned; nothing proportional to the span remains
        remaining = self.store.connection.execute('SELECT COUNT(*) FROM bucket').fetchone()[0]
        self.assertEqual(remaining, 0)

    def test_windows_close_exactly_once_across_batches(self):
        self.reinit({'sources': ['S'], 'categories': ['a'], 'alpha': 1, 'width': 10})
        self.apply([ev('S', 1, 1, 'a'), ev('S', 2, 21, 'a'), wm('S', 3, 25)])
        self.assertEqual(self.store.status()['reports']['pending'], 1)
        summary = self.apply([wm('S', 4, 25)])  # no advance: nothing recloses
        self.assertEqual(summary['windows_closed'], 0)
        summary = self.apply([wm('S', 5, 35)])
        self.assertEqual(summary['windows_closed'], 1)  # only E=30
        ends = [row[0] for row in self.store.connection.execute(
            'SELECT window_end FROM report ORDER BY window_end')]
        self.assertEqual(ends, [20, 30])
        # replaying old watermarks can never reclose or duplicate a window
        summary = self.apply([wm('S', 3, 25), wm('S', 4, 25), wm('S', 5, 35)])
        self.assertEqual(summary['windows_closed'], 0)
        count = self.store.connection.execute('SELECT COUNT(*) FROM report').fetchone()[0]
        self.assertEqual(count, 2)

    def test_per_category_count_over_analyze_limit_fails_apply(self):
        self.reinit({'sources': ['S'], 'categories': ['a'], 'alpha': 1, 'width': 10})
        rows = [ev('S', i, 1, 'a') for i in range(1, 7)] + [wm('S', 7, 20)]
        with mock.patch.object(stream, 'MAX_COUNT', 5):
            with self.assertRaises(StreamError):
                self.apply(rows)
        # the whole batch rolled back: no source progress, no report
        self.assertEqual(self.source('S')['committed_seq'], 0)
        self.assertEqual(self.store.status()['reports'], {'pending': 0, 'delivered': 0})

    def test_closed_report_never_rewritten_by_later_batches(self):
        self.reinit({'sources': ['S'], 'categories': ['a'], 'alpha': 1, 'width': 10})
        self.apply([ev('S', 1, 1, 'a'), wm('S', 2, 20)])
        self.export()
        before = (self.out / 'window-20.json').read_bytes()
        self.apply([wm('S', 3, 100)])  # closes many more windows
        self.apply([ev('S', 4, 1, 'a')])  # late now; must not touch window 20
        self.export()
        self.assertEqual((self.out / 'window-20.json').read_bytes(), before)


def canonical(line):
    return (line['kind'], line.get('ts', line.get('time')), line.get('category'))


def simulate(batches, sources, width):
    """Independent in-test oracle for the acceptance and window state machine."""
    seq = {s: 0 for s in sources}
    watermark = {s: 0 for s in sources}
    committed = {}
    buckets = {}
    late = []
    accepted = {s: 0 for s in sources}
    frontier = 2 * width
    close_g = {}
    for lines in batches:
        for line in lines:
            source = line['source']
            q = line['seq']
            if q <= seq[source]:
                assert committed[(source, q)] == canonical(line)
                continue
            assert q == seq[source] + 1
            seq[source] = q
            committed[(source, q)] = canonical(line)
            if line['kind'] == 'watermark':
                assert line['time'] >= watermark[source]
                watermark[source] = line['time']
            elif line['ts'] < watermark[source]:
                late.append(line)
            else:
                accepted[source] += 1
                key = (line['ts'] // width, line['category'])
                buckets[key] = buckets.get(key, 0) + 1
        g = min(watermark.values())
        j = frontier // width
        while j <= g // width:
            close_g[j * width] = g
            j += 1
        if g // width >= frontier // width:
            frontier = (g // width + 1) * width
    return {'seq': seq, 'watermark': watermark, 'buckets': buckets, 'late': late,
            'accepted': accepted, 'close_g': close_g, 'g': min(watermark.values())}


def random_batches(rng, sources, categories, n_batches):
    seq = {s: 0 for s in sources}
    watermark = {s: 0 for s in sources}
    history = []
    batches = []
    horizon = 0
    for _ in range(n_batches):
        lines = []
        for _ in range(rng.randint(1, 25)):
            if history and rng.random() < 0.2:
                lines.append(dict(rng.choice(history)))  # exact replay of a committed line
                continue
            source = rng.choice(sources)
            seq[source] += 1
            if rng.random() < 0.55:
                horizon += rng.randint(0, 4)
                line = ev(source, seq[source], rng.randint(0, horizon + 3),
                          rng.choice(categories))
            else:
                watermark[source] += rng.randint(0, 6)
                line = wm(source, seq[source], watermark[source])
            history.append(line)
            lines.append(line)
        batches.append(lines)
    return batches


def independent_distribution(categories, reference, current, alpha):
    """Straight-from-contract Fraction oracle for PSI and Jensen-Shannon."""
    nr = sum(reference.values())
    nc = sum(current.values())
    k = len(categories)
    psi = js = 0.0
    for category in categories:
        p = Fraction(reference.get(category, 0) + alpha, nr + alpha * k)
        q = Fraction(current.get(category, 0) + alpha, nc + alpha * k)
        m = (p + q) / 2
        psi += float(q - p) * math.log(float(q / p))
        js += (float(p) * math.log(float(p / m)) + float(q) * math.log(float(q / m))) / 2
    return psi, js


class WindowOracle(unittest.TestCase):
    def test_random_streams_match_oracle_and_analyze(self):
        for seed in range(8):
            rng = random.Random(seed)
            width = rng.choice([1, 2, 5, 10])
            sources = ['s%d' % i for i in range(rng.randint(1, 3))]
            categories = ['a', 'b', 'c']
            alpha = rng.randint(1, 4)
            batches = random_batches(rng, sources, categories, rng.randint(2, 5))
            oracle = simulate(batches, sources, width)
            with tempfile.TemporaryDirectory() as td:
                store, _ = init_db(Path(td) / 'p.db',
                                   {'sources': sources, 'categories': categories,
                                    'alpha': alpha, 'width': width})
                for lines in batches:
                    store.apply(jsonl(lines))
                status = store.status()
                for entry in status['sources']:
                    name = entry['name']
                    self.assertEqual(entry['committed_seq'], oracle['seq'][name])
                    self.assertEqual(entry['watermark'], oracle['watermark'][name])
                    self.assertEqual(entry['accepted_events'], oracle['accepted'][name])
                late = list(store.iter_late())
                ordered = sorted(oracle['late'], key=lambda l: (l['source'], l['seq']))
                self.assertEqual([(r['source'], r['seq']) for r in late],
                                 [(l['source'], l['seq']) for l in ordered])
                out = Path(td) / 'out'
                store.export(out)
                files = {int(p.stem.split('-')[1]): p for p in out.glob('window-*.json')}
                g = oracle['g']
                expected = {}
                j = 2
                while j * width <= g:
                    ref = {c: oracle['buckets'].get((j - 2, c), 0) for c in categories}
                    cur = {c: oracle['buckets'].get((j - 1, c), 0) for c in categories}
                    if sum(ref.values()) or sum(cur.values()):
                        expected[j * width] = (ref, cur)
                    j += 1
                self.assertEqual(set(files), set(expected),
                                 'seed %d: exported windows mismatch' % seed)
                for end, (ref, cur) in expected.items():
                    report = json.loads(files[end].read_text())
                    self.assertEqual(report['window']['end'], end)
                    self.assertEqual(report['reference']['counts'], ref)
                    self.assertEqual(report['current']['counts'], cur)
                    self.assertEqual(report['closed_at']['min_watermark'],
                                     oracle['close_g'][end])
                    if sum(ref.values()) and sum(cur.values()):
                        self.assertEqual(report['distribution_status'], 'ok')
                        want = analyze({'categories': categories, 'alpha': alpha,
                                        'reference': [{'category': c, 'count': v}
                                                      for c, v in ref.items()],
                                        'current': [{'category': c, 'count': v}
                                                    for c, v in cur.items()]})
                        self.assertEqual(report['distribution'], want)
                        psi, js = independent_distribution(categories, ref, cur, alpha)
                        self.assertAlmostEqual(report['distribution']['psi'], psi)
                        self.assertAlmostEqual(
                            report['distribution']['jensen_shannon_nats'], js)
                    else:
                        self.assertEqual(report['distribution_status'],
                                         'insufficient_population')
                        self.assertIsNone(report['distribution'])
                store.close()


class LateAuditApi(PipelineCase):
    def test_iter_late_streams_in_source_seq_order_and_filters(self):
        self.apply([wm('A', 1, 50), wm('B', 1, 50),
                    ev('B', 2, 1, 'y'), ev('A', 2, 3, 'x'), ev('A', 3, 2, 'y'),
                    ev('B', 3, 40, 'x')])
        late = list(self.store.iter_late())
        self.assertEqual([(r['source'], r['seq']) for r in late],
                         [('A', 2), ('A', 3), ('B', 2), ('B', 3)])
        only_b = list(self.store.iter_late('B'))
        self.assertEqual([(r['source'], r['seq'], r['ts'], r['category']) for r in only_b],
                         [('B', 2, 1, 'y'), ('B', 3, 40, 'x')])
        for record in late:
            self.assertIn('below source watermark', record['reason'])
        with self.assertRaises(StreamError):
            list(self.store.iter_late('unknown'))
        # ts == watermark is not late
        self.apply([ev('A', 4, 50, 'x')])
        self.assertEqual(len(list(self.store.iter_late())), 4)


class StatusAndRestart(PipelineCase):
    def test_status_reports_sources_and_reports(self):
        self.apply([ev('A', 1, 1), wm('A', 2, 25), ev('B', 1, 2, 'y'), wm('B', 2, 30)])
        status = self.store.status()
        self.assertEqual(status['config'], CONFIG)
        self.assertEqual(status['min_watermark'], 25)
        self.assertEqual(status['next_window_end'], 30)
        self.assertEqual(status['reports'], {'pending': 1, 'delivered': 0})
        a = self.source('A')
        self.assertEqual((a['committed_seq'], a['watermark'], a['accepted_events'],
                          a['late_events']), (2, 25, 1, 0))
        self.export()
        status = self.store.status()
        self.assertEqual(status['reports'], {'pending': 0, 'delivered': 1})

    def test_restart_preserves_progress_and_config(self):
        self.apply([ev('A', 1, 1), wm('A', 2, 25), wm('B', 1, 30)])
        self.export()
        self.store.close()
        self.store = Store(self.db)  # reopen: config and progress survive
        status = self.store.status()
        self.assertEqual(status['config'], CONFIG)
        self.assertEqual(self.source('A')['committed_seq'], 2)
        self.assertEqual(status['reports']['delivered'], 1)
        # the pipeline continues exactly where it stopped
        summary = self.apply([ev('A', 3, 26, 'y'), wm('A', 4, 30)])
        self.assertEqual(summary['accepted_events'], 1)
        self.assertEqual(status['next_window_end'], 30)


class DeliveryRecovery(unittest.TestCase):
    """Crash the export in real subprocesses; recovery must be byte-exact."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.td = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def build_db(self, name):
        db = self.td / name / 'p.db'
        db.parent.mkdir(parents=True, exist_ok=True)
        store, _ = init_db(db, {'sources': ['A', 'B'], 'categories': ['x', 'y'],
                                'alpha': 1, 'width': 10})
        store.apply(jsonl([ev('A', 1, 1, 'x'), ev('A', 2, 11, 'y'),
                           wm('A', 3, 30), wm('B', 1, 30)]))
        store.close()
        return db

    def driver(self, db, outdir, crash):
        return subprocess.run(
            [sys.executable, str(ROOT / 'tests' / 'stream_delivery_driver.py'),
             '--db', str(db), '--output-dir', str(outdir), '--crash', crash],
            capture_output=True, text=True)

    def cli(self, *args):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(ROOT / 'src') + os.pathsep + env.get('PYTHONPATH', '')
        return subprocess.run(
            [sys.executable, '-m', 'distribution_drift.stream_cli', *map(str, args)],
            capture_output=True, text=True, cwd=ROOT, env=env)

    def test_uninterrupted_export_is_the_golden_reference(self):
        db = self.build_db('golden')
        out = self.td / 'golden' / 'out'
        result = self.driver(db, out, 'none')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'confirmed': 0, 'published': 2})
        self.assertTrue((out / 'window-20.json').exists())
        self.assertTrue((out / 'window-30.json').exists())

    def test_crash_after_commit_before_publish_recovers(self):
        golden = self.td / 'golden' / 'out'
        self.driver(self.build_db('golden'), golden, 'none')
        db = self.build_db('crash1')
        out = self.td / 'crash1' / 'out'
        result = self.driver(db, out, 'before-publish')
        self.assertEqual(result.returncode, 3)
        self.assertFalse((out / 'window-20.json').exists())
        # restart: export completes the committed deliveries
        result = self.driver(db, out, 'none')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual((out / 'window-20.json').read_bytes(),
                         (golden / 'window-20.json').read_bytes())
        self.assertEqual((out / 'window-30.json').read_bytes(),
                         (golden / 'window-30.json').read_bytes())
        with Store(db) as store:
            self.assertEqual(store.status()['reports'], {'pending': 0, 'delivered': 2})

    def test_crash_after_replace_before_confirm_recovers(self):
        golden = self.td / 'golden' / 'out'
        self.driver(self.build_db('golden'), golden, 'none')
        db = self.build_db('crash2')
        out = self.td / 'crash2' / 'out'
        result = self.driver(db, out, 'after-replace')
        self.assertEqual(result.returncode, 4)
        # the first report was replaced but never confirmed
        self.assertEqual((out / 'window-20.json').read_bytes(),
                         (golden / 'window-20.json').read_bytes())
        self.assertFalse((out / 'window-30.json').exists())
        with Store(db) as store:
            self.assertEqual(store.status()['reports'], {'pending': 2, 'delivered': 0})
        # restart: identical bytes confirm in place, the rest publishes
        result = self.driver(db, out, 'none')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout), {'confirmed': 1, 'published': 1})
        self.assertEqual((out / 'window-20.json').read_bytes(),
                         (golden / 'window-20.json').read_bytes())
        self.assertEqual((out / 'window-30.json').read_bytes(),
                         (golden / 'window-30.json').read_bytes())
        with Store(db) as store:
            self.assertEqual(store.status()['reports'], {'pending': 0, 'delivered': 2})

    def test_foreign_target_bytes_abort_and_are_preserved(self):
        db = self.build_db('foreign')
        out = self.td / 'foreign' / 'out'
        out.mkdir(parents=True)
        alien = out / 'window-20.json'
        alien.write_bytes(b'{"foreign": true}\n')
        result = self.cli('export', '--db', db, '--output-dir', out)
        self.assertEqual(result.returncode, 2)
        self.assertIn('different bytes', result.stderr)
        self.assertEqual(alien.read_bytes(), b'{"foreign": true}\n')
        self.assertFalse((out / 'window-30.json').exists())
        # removing the foreign object lets the retry deliver everything
        alien.unlink()
        result = self.cli('export', '--db', db, '--output-dir', out)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['published'], 2)

    def test_symlink_target_aborts_and_is_preserved(self):
        db = self.build_db('link')
        out = self.td / 'link' / 'out'
        out.mkdir(parents=True)
        target = self.td / 'link' / 'elsewhere.json'
        target.write_bytes(b'{}\n')
        link = out / 'window-20.json'
        link.symlink_to(target)
        result = self.cli('export', '--db', db, '--output-dir', out)
        self.assertEqual(result.returncode, 2)
        self.assertIn('symlink', result.stderr)
        self.assertTrue(link.is_symlink())
        self.assertEqual(target.read_bytes(), b'{}\n')

    def test_export_is_idempotent_and_confirms_identical_bytes(self):
        db = self.build_db('twice')
        out = self.td / 'twice' / 'out'
        self.assertEqual(self.driver(db, out, 'none').returncode, 0)
        again = json.loads(self.driver(db, out, 'none').stdout)
        self.assertEqual(again, {'confirmed': 0, 'published': 0})
        # simulate a lost confirmation: bytes already in place, flag cleared
        con = sqlite3.connect(db)
        con.execute('UPDATE report SET delivered = 0')
        con.commit()
        con.close()
        recovered = json.loads(self.driver(db, out, 'none').stdout)
        self.assertEqual(recovered, {'confirmed': 2, 'published': 0})


class Integration(PipelineCase):
    """The end-to-end acceptance scenario from the pipeline contract."""

    def test_laggard_source_gates_windows_and_replay_is_safe(self):
        # source A rushes to watermark 30 while B is stuck at 9: no end-20 report
        summary = self.apply([wm('A', 1, 30), wm('B', 1, 9)])
        self.assertEqual(summary['windows_closed'], 0)
        self.assertEqual(self.store.status()['reports'], {'pending': 0, 'delivered': 0})
        self.assertEqual(self.export()['published'], 0)
        self.assertFalse((self.out / 'window-20.json').exists())

        # B accepts ts=19 and advances to 20: end-20 closes with exactly that event
        summary = self.apply([ev('B', 2, 19, 'x'), wm('B', 3, 20)])
        self.assertEqual(summary['reports_generated'], 1)
        self.assertEqual(self.export()['published'], 1)
        report = self.report(20)
        self.assertEqual(report['current']['counts'], {'x': 1, 'y': 0})
        self.assertEqual(report['reference']['counts'], {'x': 0, 'y': 0})
        self.assertEqual(report['distribution_status'], 'insufficient_population')
        golden = (self.out / 'window-20.json').read_bytes()

        # replaying every batch changes nothing
        summary = self.apply([wm('A', 1, 30), wm('B', 1, 9),
                              ev('B', 2, 19, 'x'), wm('B', 3, 20)])
        self.assertEqual(summary['replayed'], 4)
        self.assertEqual(self.source('B')['accepted_events'], 1)
        self.assertEqual(self.export(), {'published': 0, 'confirmed': 0})
        self.assertEqual((self.out / 'window-20.json').read_bytes(), golden)

        # a new seq with ts=19 is late now; the old report is immutable
        summary = self.apply([ev('B', 4, 19, 'x')])
        self.assertEqual(summary['late_events'], 1)
        self.assertEqual(self.export(), {'published': 0, 'confirmed': 0})
        self.assertEqual((self.out / 'window-20.json').read_bytes(), golden)
        late = list(self.store.iter_late('B'))
        self.assertEqual([(r['seq'], r['ts']) for r in late], [(4, 19)])

        # an invalid last line rolls back the window this batch would have closed
        with self.assertRaises(StreamError):
            self.apply([wm('B', 5, 30), wm('A', 2, 40), ev('B', 99, 1, 'x')])
        status = self.store.status()
        b = self.source('B')
        self.assertEqual((b['committed_seq'], b['watermark']), (4, 20))
        self.assertEqual(status['reports'], {'pending': 0, 'delivered': 1})
        self.assertEqual(status['next_window_end'], 30)
        self.assertFalse((self.out / 'window-30.json').exists())


class FixedFixture(unittest.TestCase):
    def test_fixture_pipeline_matches_golden_bytes(self):
        fixture = ROOT / 'fixtures' / 'stream'
        config = json.loads((fixture / 'config.json').read_text())
        with tempfile.TemporaryDirectory() as td:
            store, _ = init_db(Path(td) / 'p.db', config)
            first = store.apply_file(fixture / 'events-1.jsonl')
            self.assertEqual(first['windows_closed'], 0)
            second = store.apply_file(fixture / 'events-2.jsonl')
            self.assertEqual(second['reports_generated'], 2)
            self.assertEqual(second['late_events'], 1)
            out = Path(td) / 'out'
            store.export(out)
            store.close()
            for end in (20, 30):
                self.assertEqual((out / ('window-%d.json' % end)).read_bytes(),
                                 (fixture / 'expected' / ('window-%d.json' % end)).read_bytes())


class CommandLine(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.td = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def call(self, *args):
        env = dict(os.environ)
        env['PYTHONPATH'] = str(ROOT / 'src') + os.pathsep + env.get('PYTHONPATH', '')
        return subprocess.run(
            [sys.executable, '-m', 'distribution_drift.stream_cli', *map(str, args)],
            capture_output=True, text=True, cwd=ROOT, env=env)

    def write(self, name, rows):
        path = self.td / name
        path.write_bytes(jsonl(rows).getvalue())
        return path

    def test_full_workflow_and_replayable_entries(self):
        db = self.td / 'p.db'
        config = self.td / 'config.json'
        config.write_text(json.dumps(CONFIG))
        result = self.call('init', '--db', db, '--config', config)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['status'], 'initialized')
        result = self.call('init', '--db', db, '--config', config)
        self.assertEqual(json.loads(result.stdout)['status'], 'unchanged')

        batch1 = self.write('b1.jsonl', [ev('A', 1, 1, 'x'), wm('A', 2, 20), wm('B', 1, 20)])
        result = self.call('apply', '--db', db, '--input', batch1)
        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary['status'], 'applied')
        self.assertEqual(summary['reports_generated'], 1)

        out = self.td / 'out'
        result = self.call('export', '--db', db, '--output-dir', out)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['published'], 1)
        self.assertTrue((out / 'window-20.json').exists())
        # export is safe to re-run
        result = self.call('export', '--db', db, '--output-dir', out)
        self.assertEqual(json.loads(result.stdout)['published'], 0)

        result = self.call('status', '--db', db)
        self.assertEqual(result.returncode, 0, result.stderr)
        status = json.loads(result.stdout)
        self.assertEqual(status['min_watermark'], 20)
        self.assertEqual(status['reports'], {'pending': 0, 'delivered': 1})

        # replaying the same batch is idempotent
        result = self.call('apply', '--db', db, '--input', batch1)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['replayed'], 3)

    def test_failures_exit_2_and_preserve_state(self):
        db = self.td / 'p.db'
        # apply before init
        batch = self.write('b.jsonl', [ev('A', 1, 1)])
        result = self.call('apply', '--db', db, '--input', batch)
        self.assertEqual(result.returncode, 2)
        # init, then a bad config and a bad batch
        config = self.td / 'config.json'
        config.write_text(json.dumps(CONFIG))
        self.assertEqual(self.call('init', '--db', db, '--config', config).returncode, 0)
        bad_config = self.td / 'bad.json'
        bad_config.write_text(json.dumps(dict(CONFIG, width=0)))
        self.assertEqual(self.call('init', '--db', db, '--config', bad_config).returncode, 2)
        bad_batch = self.write('bad.jsonl', [ev('A', 2, 1)])  # seq gap
        result = self.call('apply', '--db', db, '--input', bad_batch)
        self.assertEqual(result.returncode, 2)
        result = self.call('status', '--db', db)
        status = json.loads(result.stdout)
        self.assertEqual(status['sources'][0]['committed_seq'], 0)


class StreamingBounds(PipelineCase):
    def test_large_batch_from_one_shot_generator(self):
        n = 50_000

        def lines():
            yield json.dumps(wm('B', 1, 25)).encode() + b'\n'
            for i in range(1, n + 1):
                yield json.dumps(ev('A', i, i % 25, 'x')).encode() + b'\n'
            yield json.dumps(wm('A', n + 1, 25)).encode() + b'\n'
        summary = self.store.apply(lines())
        self.assertEqual(summary['lines'], n + 2)
        self.assertEqual(summary['accepted_events'], n)
        a = self.source('A')
        self.assertEqual((a['committed_seq'], a['accepted_events']), (n + 1, n))
        # watermark 25 closes window 20; each residue ts in 0..24 occurs 2000x
        self.export()
        report = self.report(20)
        self.assertEqual(report['reference']['counts']['x'], 20000)  # ts 0..9
        self.assertEqual(report['current']['counts']['x'], 20000)    # ts 10..19


if __name__ == '__main__':
    unittest.main()
