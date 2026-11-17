"""Restartable event-time drift monitoring pipeline on streaming JSONL + SQLite.

The legacy :func:`distribution_drift.model.analyze` contract compares two
offline frequency snapshots but cannot tell whether a time window is really
complete when several collection sources arrive out of step.  This module
adds a restartable monitoring pipeline: categorical events and watermarks
stream in per-source sequence order, completeness is tracked as the minimum
watermark across all configured sources, and final fixed-window drift
reports are committed to SQLite exactly once, then published to local files
by a crash-safe export step.  No partition revision chains, no entity
pairing, no manual adjudication.

Design summary (see docs/stream.md for the full discussion):

* One ``BEGIN IMMEDIATE`` transaction per ``apply`` batch commits the source
  prefix progress, the per-(source, seq) acceptance ledger, the late-event
  quarantine audit, the interval bucket counts and the generated pending
  reports together; any invalid line — even the last one — rolls the whole
  batch back and publishes nothing.
* A source watermark advance can only close windows when it moves the
  global minimum watermark G: reports are generated exactly for window ends
  E = k*width, k >= 2, E <= G, and each such window is closed at most once
  (a persisted frontier), so replays and late events never rewrite history.
* Window reports are serialized once at close time and stored as immutable
  text.  Export publishes those exact bytes (sibling temp file +
  ``os.replace``) and only then marks the report delivered, so a crash
  anywhere between the database commit and the delivery confirmation is
  recovered by re-running export: identical target bytes count as success,
  foreign bytes or a symlink abort without touching the target.
* Python memory stays bounded by the batch flush size plus the source and
  category domains; every event-, sequence- or report-proportional
  structure lives in SQLite.  Empty windows across huge time spans are
  skipped arithmetically via the persisted frontier, never enumerated.
"""
from collections import namedtuple
from contextlib import contextmanager
import json
import os
import sqlite3
import tempfile
from pathlib import Path

from .common import ident, parse_strict, read_bytes
from .model import analyze

MAX_SOURCES = 64
MAX_DOMAIN_CATEGORIES = 1_000
MAX_ALPHA = 1_000_000
MAX_WIDTH = 1_000_000
MAX_SEQ = 10 ** 12
MAX_TIME = 10 ** 12
MAX_LINE_BYTES = 65_536
MAX_APPLY_LINES = 10_000_000
# Per-category window counts must stay inside the model.analyze contract.
MAX_COUNT = 10 ** 9
_READ_CHUNK = 8_192
_FLUSH_BATCH = 8_192

KIND_EVENT = 'event'
KIND_WATERMARK = 'watermark'

# Canonical record: ``time`` is the event ts or the watermark time;
# ``category`` is None for watermarks.
Record = namedtuple('Record', ('source', 'seq', 'kind', 'time', 'category'))


class StreamError(ValueError):
    """Base class for rejected config, JSONL record or pipeline requests."""


class PayloadConflict(StreamError):
    """A replayed (source, seq) carries a payload different from the committed one."""


class DeliveryError(StreamError):
    """An export target holds foreign bytes or is a symlink; it is kept untouched."""


# ---------------------------------------------------------------------------
# strict validation
# ---------------------------------------------------------------------------

def _ident(value, field):
    try:
        return ident(value)
    except ValueError as exc:
        raise StreamError(field + ': ' + str(exc))


def _bounded_int(value, field, minimum, maximum):
    # bool is a subclass of int; the contract forbids booleans explicitly.
    if type(value) is not int:
        raise StreamError(field + ' must be a strict integer (booleans rejected)')
    if not minimum <= value <= maximum:
        raise StreamError('%s must be within %d..%d' % (field, minimum, maximum))
    return value


def validate_config(obj):
    """Validate ``{sources, categories, alpha, width}``; return a normalized copy.

    Category and alpha rules mirror :func:`distribution_drift.model.analyze`
    exactly: 1..1000 unique identifiers and a strict integer alpha in
    1..10^6.  ``sources`` holds 1..64 unique identifiers and ``width`` is a
    strict integer in 1..10^6.
    """
    if not isinstance(obj, dict) or set(obj) != {'sources', 'categories', 'alpha', 'width'}:
        raise StreamError('config must contain exactly sources, categories, alpha, width')
    raw_sources = obj['sources']
    if not isinstance(raw_sources, list) or not raw_sources or len(raw_sources) > MAX_SOURCES:
        raise StreamError('sources must be a nonempty array of at most %d identifiers' % MAX_SOURCES)
    sources = []
    seen = set()
    for value in raw_sources:
        name = _ident(value, 'source')
        if name in seen:
            raise StreamError('duplicate source: ' + name)
        seen.add(name)
        sources.append(name)
    raw_categories = obj['categories']
    if not isinstance(raw_categories, list) or not raw_categories or \
            len(raw_categories) > MAX_DOMAIN_CATEGORIES:
        raise StreamError('categories must be a nonempty array of at most %d identifiers'
                          % MAX_DOMAIN_CATEGORIES)
    categories = []
    seen = set()
    for value in raw_categories:
        name = _ident(value, 'category')
        if name in seen:
            raise StreamError('duplicate config category: ' + name)
        seen.add(name)
        categories.append(name)
    alpha = _bounded_int(obj['alpha'], 'alpha', 1, MAX_ALPHA)
    width = _bounded_int(obj['width'], 'width', 1, MAX_WIDTH)
    return {'sources': sources, 'categories': categories, 'alpha': alpha, 'width': width}


def _reject_duplicate_key(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise StreamError('duplicate JSON key: ' + str(key))
        result[key] = value
    return result


def _reject_constant(value):
    raise StreamError('non-finite JSON constant is not allowed: ' + value)


def _check_length(raw):
    if len(raw) > MAX_LINE_BYTES:
        raise StreamError('line exceeds %d bytes' % MAX_LINE_BYTES)
    return raw


def iter_lines(source):
    """Yield raw UTF-8 byte lines from a binary stream or any byte iterable.

    File-like objects (anything offering ``read``) are consumed in small
    chunks with an explicit line-length ceiling, so an oversized single line
    can never grow an unbounded Python buffer.  Plain iterables are accepted
    unchanged, which supports one-shot, non-seekable input generators.
    """
    if hasattr(source, 'read'):
        buffer = b''
        while True:
            chunk = source.read(_READ_CHUNK)
            if not chunk:
                if buffer:
                    yield _check_length(buffer)
                return
            buffer += chunk
            while True:
                newline = buffer.find(b'\n')
                if newline < 0:
                    _check_length(buffer)
                    break
                line = buffer[:newline + 1]
                buffer = buffer[newline + 1:]
                yield _check_length(line)
    else:
        for raw in source:
            if not isinstance(raw, (bytes, bytearray)):
                raise StreamError('input iterables must yield bytes lines')
            yield _check_length(bytes(raw))


def decode_line(raw):
    """Strictly parse one JSONL line into a Python object."""
    body = raw[:-1] if raw.endswith(b'\n') else raw
    if not body or not body.strip():
        raise StreamError('empty lines are not allowed')
    try:
        text = body.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise StreamError('line is not valid UTF-8: ' + str(exc))
    try:
        return json.loads(text, object_pairs_hook=_reject_duplicate_key,
                          parse_constant=_reject_constant)
    except StreamError:
        raise
    except ValueError as exc:
        raise StreamError('invalid JSON line: ' + str(exc))


def parse_record(obj, sources, domain):
    """Validate one JSONL object into a canonical :class:`Record`.

    ``sources`` is the configured source set and ``domain`` the declared
    category set.  Unknown fields, unknown sources, out-of-domain
    categories, non-strict integers and out-of-range values are rejected.
    """
    if not isinstance(obj, dict):
        raise StreamError('each line must be a JSON object')
    keys = set(obj)
    kind = obj.get('kind')
    if kind == KIND_EVENT:
        if keys != {'source', 'seq', 'kind', 'ts', 'category'}:
            raise StreamError('event records must contain exactly source, seq, kind, ts, category')
    elif kind == KIND_WATERMARK:
        if keys != {'source', 'seq', 'kind', 'time'}:
            raise StreamError('watermark records must contain exactly source, seq, kind, time')
    else:
        raise StreamError('kind must be "event" or "watermark"')
    source = _ident(obj['source'], 'source')
    if source not in sources:
        raise StreamError('source is not configured: ' + repr(source))
    seq = _bounded_int(obj['seq'], 'seq', 1, MAX_SEQ)
    if kind == KIND_EVENT:
        ts = _bounded_int(obj['ts'], 'ts', 0, MAX_TIME)
        category = obj['category']
        if not isinstance(category, str):
            raise StreamError('category must be a string')
        if category not in domain:
            raise StreamError('category outside configured domain: ' + repr(category))
        return Record(source, seq, kind, ts, category)
    time = _bounded_int(obj['time'], 'time', 0, MAX_TIME)
    return Record(source, seq, kind, time, None)


# ---------------------------------------------------------------------------
# persistent store
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS source(
  name TEXT PRIMARY KEY,
  committed_seq INTEGER NOT NULL,
  watermark INTEGER NOT NULL,
  accepted_events INTEGER NOT NULL,
  late_events INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS record(
  source TEXT NOT NULL,
  seq INTEGER NOT NULL,
  kind TEXT NOT NULL,
  ts INTEGER NOT NULL,
  category TEXT,
  disposition TEXT NOT NULL,
  reason TEXT,
  PRIMARY KEY(source, seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS bucket(
  k INTEGER NOT NULL,
  category TEXT NOT NULL,
  n INTEGER NOT NULL,
  PRIMARY KEY(k, category)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS report(
  window_end INTEGER PRIMARY KEY,
  report_json TEXT NOT NULL,
  delivered INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS report_pending ON report(delivered, window_end);
"""


@contextmanager
def _write_transaction(connection):
    connection.execute('BEGIN IMMEDIATE')
    try:
        yield
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _publish_file(path, data):
    """Atomically publish exactly ``data`` to ``path``; return the outcome.

    Identical existing bytes count as success (``confirmed``) — that is the
    crash-after-replace recovery path.  Foreign bytes or a symlink raise
    :class:`DeliveryError` and the target is left untouched, so an alien
    object can never be overwritten into a success.
    """
    path = Path(path)
    if path.is_symlink():
        raise DeliveryError('export target is a symlink: ' + str(path))
    if path.exists():
        with path.open('rb') as stream:
            existing = stream.read()
        if existing == data:
            return 'confirmed'
        raise DeliveryError('export target already exists with different bytes: ' + str(path))
    fd, tmp = tempfile.mkstemp(prefix='.' + path.name + '.', dir=str(path.parent))
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
    return 'published'


class Store:
    """SQLite-backed acceptance ledger, window frontier and report outbox."""

    def __init__(self, path):
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, timeout=30.0)
        # autocommit mode: BEGIN/COMMIT boundaries are managed explicitly.
        self.connection.isolation_level = None
        self.connection.execute('PRAGMA busy_timeout=30000')
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(_SCHEMA)
        row = self.connection.execute(
            "SELECT value FROM meta WHERE key = 'config'").fetchone()
        self._config = json.loads(row['value']) if row is not None else None

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def config(self):
        if self._config is None:
            raise StreamError('database is not initialized; run init with a config first')
        return self._config

    # -- init ---------------------------------------------------------------
    def configure(self, config):
        """Fix the validated config in this database; return True when created.

        Re-running with the identical config is an idempotent no-op
        (progress is preserved); a different config is rejected.
        """
        canonical = json.dumps(config, sort_keys=True, ensure_ascii=False, allow_nan=False)
        con = self.connection
        with _write_transaction(con):
            row = con.execute("SELECT value FROM meta WHERE key = 'config'").fetchone()
            if row is not None:
                if row['value'] != canonical:
                    raise StreamError('database already initialized with a different config')
                return False
            con.execute("INSERT INTO meta(key, value) VALUES ('config', ?)", (canonical,))
            con.execute("INSERT INTO meta(key, value) VALUES ('next_window_end', ?)",
                        (str(2 * config['width']),))
            for name in config['sources']:
                con.execute(
                    'INSERT INTO source(name, committed_seq, watermark,'
                    ' accepted_events, late_events) VALUES (?, 0, 0, 0, 0)', (name,))
        self._config = config
        return True

    # -- apply --------------------------------------------------------------
    def apply(self, source):
        """Consume one JSONL batch in a single atomic transaction.

        ``source`` is a binary file-like object or any one-shot iterable of
        byte lines; it is read exactly once, in order, and records are never
        reordered by event time.  Returns a bounded summary dict.  Any
        invalid line or contract violation rolls the whole batch back.
        """
        config = self.config
        domain = frozenset(config['categories'])
        configured = frozenset(config['sources'])
        width = config['width']
        con = self.connection
        summary = {'lines': 0, 'replayed': 0, 'accepted_events': 0,
                   'late_events': 0, 'watermarks': 0,
                   'windows_closed': 0, 'reports_generated': 0}
        with _write_transaction(con):
            states = {row['name']: {'seq': row['committed_seq'],
                                    'watermark': row['watermark'],
                                    'accepted': row['accepted_events'],
                                    'late': row['late_events']}
                      for row in con.execute(
                          'SELECT name, committed_seq, watermark, accepted_events, late_events'
                          ' FROM source')}
            records = []          # pending INSERT rows, bounded by _FLUSH_BATCH
            pending = {}          # (source, seq) -> canonical payload, same bound
            deltas = {}           # (bucket, category) -> count, same bound
            for raw in iter_lines(source):
                summary['lines'] += 1
                if summary['lines'] > MAX_APPLY_LINES:
                    raise StreamError('apply exceeds the %d line limit' % MAX_APPLY_LINES)
                record = parse_record(decode_line(raw), configured, domain)
                state = states[record.source]
                if record.seq <= state['seq']:
                    # Replay of a committed sequence number: only the exact
                    # same canonical payload is an idempotent no-op.  The
                    # historical disposition is never re-judged against the
                    # current watermark.
                    key = (record.source, record.seq)
                    if key in pending:
                        if pending[key] != (record.kind, record.time, record.category):
                            raise PayloadConflict(
                                'source %r seq %d conflicts with the payload accepted'
                                ' earlier in this batch' % (record.source, record.seq))
                    else:
                        self._check_replay(record)
                    summary['replayed'] += 1
                    continue
                if record.seq > state['seq'] + 1:
                    raise StreamError(
                        'sequence gap for source %r: expected seq %d, got %d'
                        % (record.source, state['seq'] + 1, record.seq))
                state['seq'] = record.seq
                if record.kind == KIND_WATERMARK:
                    if record.time < state['watermark']:
                        raise StreamError(
                            'watermark cannot decrease for source %r: %d < %d'
                            % (record.source, record.time, state['watermark']))
                    state['watermark'] = record.time
                    records.append((record.source, record.seq, KIND_WATERMARK,
                                    record.time, None, 'watermark', None))
                    summary['watermarks'] += 1
                elif record.time < state['watermark']:
                    state['late'] += 1
                    reason = 'event ts %d is below source watermark %d' % (
                        record.time, state['watermark'])
                    records.append((record.source, record.seq, KIND_EVENT,
                                    record.time, record.category, 'late', reason))
                    summary['late_events'] += 1
                else:
                    state['accepted'] += 1
                    records.append((record.source, record.seq, KIND_EVENT,
                                    record.time, record.category, 'accepted', None))
                    summary['accepted_events'] += 1
                    key = (record.time // width, record.category)
                    deltas[key] = deltas.get(key, 0) + 1
                pending[(record.source, record.seq)] = (record.kind, record.time,
                                                        record.category)
                if len(records) >= _FLUSH_BATCH:
                    self._flush(records, deltas)
                    records.clear()
                    pending.clear()
                    deltas.clear()
            if records:
                self._flush(records, deltas)
            for name, state in states.items():
                con.execute('UPDATE source SET committed_seq = ?, watermark = ?,'
                            ' accepted_events = ?, late_events = ? WHERE name = ?',
                            (state['seq'], state['watermark'], state['accepted'],
                             state['late'], name))
            closed, generated = self._close_windows(states)
            summary['windows_closed'] = closed
            summary['reports_generated'] = generated
        return summary

    def apply_file(self, path):
        with Path(path).open('rb') as stream:
            return self.apply(stream)

    def _check_replay(self, record):
        row = self.connection.execute(
            'SELECT kind, ts, category FROM record WHERE source = ? AND seq = ?',
            (record.source, record.seq)).fetchone()
        if row is None:
            # Committed sequence numbers are contiguous from 1, so a missing
            # row would mean the ledger itself is inconsistent.
            raise StreamError('committed record missing for source %r seq %d'
                              % (record.source, record.seq))
        if (row['kind'], row['ts'], row['category']) != \
                (record.kind, record.time, record.category):
            raise PayloadConflict(
                'source %r seq %d already committed with a different payload'
                % (record.source, record.seq))

    def _flush(self, records, deltas):
        con = self.connection
        con.executemany(
            'INSERT INTO record(source, seq, kind, ts, category, disposition, reason)'
            ' VALUES (?, ?, ?, ?, ?, ?, ?)', records)
        con.executemany(
            'INSERT INTO bucket(k, category, n) VALUES (?, ?, ?)'
            ' ON CONFLICT(k, category) DO UPDATE SET n = n + excluded.n',
            [(k, category, n) for (k, category), n in deltas.items()])

    # -- window closing -------------------------------------------------------
    def _bucket_counts(self, k):
        return {row['category']: row['n'] for row in self.connection.execute(
            'SELECT category, n FROM bucket WHERE k = ?', (k,))}

    def _close_windows(self, states):
        """Close every window E = k*width (k >= 2) with E <= min-watermark G.

        Only windows touched by at least one interval bucket are
        materialised; the empty span is crossed arithmetically by advancing
        the persisted frontier.  Fully consumed buckets are pruned.  Returns
        ``(windows_closed, reports_generated)``.
        """
        con = self.connection
        width = self._config['width']
        g = min(state['watermark'] for state in states.values())
        jmax = g // width
        row = con.execute("SELECT value FROM meta WHERE key = 'next_window_end'").fetchone()
        jmin = int(row['value']) // width
        if jmax < jmin:
            return 0, 0
        progress = {name: {'committed_seq': state['seq'], 'watermark': state['watermark']}
                    for name, state in sorted(states.items())}
        generated = 0
        # Window j*width reads buckets j-2 (reference) and j-1 (current); a
        # window is non-empty iff either bucket exists.  The UNION only ever
        # yields windows backed by stored events — empty windows are skipped
        # without enumeration.
        cursor = con.execute(
            'SELECT j FROM ('
            '  SELECT k + 1 AS j FROM bucket WHERE k BETWEEN ? AND ?'
            '  UNION'
            '  SELECT k + 2 AS j FROM bucket WHERE k BETWEEN ? AND ?'
            ') ORDER BY j',
            (jmin - 1, jmax - 1, jmin - 2, jmax - 2))
        for (j,) in cursor:
            text = self._render_report(j * width, self._bucket_counts(j - 2),
                                       self._bucket_counts(j - 1), g, progress)
            con.execute('INSERT INTO report(window_end, report_json, delivered)'
                        ' VALUES (?, ?, 0)', (j * width, text))
            generated += 1
        con.execute("UPDATE meta SET value = ? WHERE key = 'next_window_end'",
                    (str((jmax + 1) * width),))
        # Bucket k only feeds windows (k+1)*width and (k+2)*width; both are
        # closed once k <= jmax - 2, and no future event can land there
        # (accepted events have ts >= their source watermark >= G).
        con.execute('DELETE FROM bucket WHERE k <= ?', (jmax - 2,))
        return jmax - jmin + 1, generated

    def _render_report(self, end, reference, current, g, progress):
        config = self._config
        width = config['width']
        domain = config['categories']
        counts = {}
        for side, sparse in (('reference', reference), ('current', current)):
            full = {category: sparse.get(category, 0) for category in domain}
            for category, value in full.items():
                if value > MAX_COUNT:
                    raise StreamError(
                        'window %d %s count for category %r exceeds the analyze'
                        ' limit %d' % (end, side, category, MAX_COUNT))
            counts[side] = full
        totals = {side: sum(full.values()) for side, full in counts.items()}
        if totals['reference'] > 0 and totals['current'] > 0:
            distribution = analyze({
                'categories': list(domain),
                'alpha': config['alpha'],
                'reference': [{'category': c, 'count': v}
                              for c, v in counts['reference'].items()],
                'current': [{'category': c, 'count': v}
                            for c, v in counts['current'].items()]})
            status = 'ok'
        else:
            # No pseudo-counts are fabricated: the analyze contract itself
            # rejects zero-total snapshots, so distribution is explicitly null.
            distribution = None
            status = 'insufficient_population'
        report = {
            'schema_version': 1,
            'kind': 'window_report',
            'window': {'end': end, 'width': width,
                       'reference': {'start': end - 2 * width, 'end': end - width},
                       'current': {'start': end - width, 'end': end}},
            'alpha': config['alpha'],
            'categories': list(domain),
            'closed_at': {'min_watermark': g, 'sources': progress},
            'reference': {'total': totals['reference'], 'counts': counts['reference']},
            'current': {'total': totals['current'], 'counts': counts['current']},
            'distribution_status': status,
            'distribution': distribution,
        }
        # Serialized once, stored, and exported verbatim forever after:
        # recovery never recomputes a closed report from mutable counts.
        return json.dumps(report, sort_keys=True, ensure_ascii=False,
                          allow_nan=False, indent=2) + '\n'

    # -- status and audit -----------------------------------------------------
    def status(self):
        """Return source progress, the completeness boundary and report counts."""
        config = self.config
        con = self.connection
        sources = [{'name': row['name'], 'committed_seq': row['committed_seq'],
                    'watermark': row['watermark'],
                    'accepted_events': row['accepted_events'],
                    'late_events': row['late_events']}
                   for row in con.execute(
                       'SELECT name, committed_seq, watermark, accepted_events, late_events'
                       ' FROM source ORDER BY name')]
        g = min(entry['watermark'] for entry in sources)
        pending = con.execute('SELECT COUNT(*) FROM report WHERE delivered = 0').fetchone()[0]
        delivered = con.execute('SELECT COUNT(*) FROM report WHERE delivered = 1').fetchone()[0]
        next_end = int(con.execute(
            "SELECT value FROM meta WHERE key = 'next_window_end'").fetchone()['value'])
        return {'config': config, 'sources': sources, 'min_watermark': g,
                'next_window_end': next_end,
                'reports': {'pending': pending, 'delivered': delivered}}

    def iter_late(self, source=None):
        """Stream quarantined late events in (source, seq) order.

        Each yielded record carries source, seq, ts, category and the
        quarantine reason.  The cursor is streaming: nothing proportional to
        the late-event history is materialised in Python.
        """
        config = self.config
        if source is not None:
            if source not in frozenset(config['sources']):
                raise StreamError('source is not configured: ' + repr(source))
            cursor = self.connection.execute(
                "SELECT source, seq, ts, category, reason FROM record"
                " WHERE disposition = 'late' AND source = ? ORDER BY source, seq",
                (source,))
        else:
            cursor = self.connection.execute(
                "SELECT source, seq, ts, category, reason FROM record"
                " WHERE disposition = 'late' ORDER BY source, seq")
        for row in cursor:
            yield {'source': row['source'], 'seq': row['seq'], 'ts': row['ts'],
                   'category': row['category'], 'reason': row['reason']}

    # -- export ---------------------------------------------------------------
    def export(self, output_dir):
        """Publish every committed report as OUTPUT_DIR/window-END.json.

        Reports are published in ascending window-end order, each by writing
        the exact bytes stored at close time to a sibling temp file,
        atomically replacing the target, and only then marking the report
        delivered.  Re-running is safe: a target already holding the same
        bytes counts as success (crash between replace and confirmation),
        foreign bytes or a symlink abort the run with the target untouched.
        """
        self.config  # an uninitialized database has nothing to export
        dest = Path(output_dir)
        dest.mkdir(parents=True, exist_ok=True)
        published = 0
        confirmed = 0
        while True:
            row = self.connection.execute(
                'SELECT window_end, report_json FROM report WHERE delivered = 0'
                ' ORDER BY window_end LIMIT 1').fetchone()
            if row is None:
                break
            end = row['window_end']
            outcome = _publish_file(dest / ('window-%d.json' % end),
                                    row['report_json'].encode('utf-8'))
            if outcome == 'published':
                published += 1
            else:
                confirmed += 1
            self._confirm(end)
        return {'published': published, 'confirmed': confirmed}

    def _confirm(self, end):
        """Record the delivery confirmation in its own small transaction."""
        with _write_transaction(self.connection):
            self.connection.execute(
                'UPDATE report SET delivered = 1 WHERE window_end = ?', (end,))


def init_db(path, config):
    """Create or reopen a pipeline database and fix ``config`` in it.

    Returns ``(store, created)``.  Re-initializing with the identical config
    is an idempotent no-op (``created`` is False) that never loses progress;
    a different config is rejected.
    """
    normalized = validate_config(config)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    store = Store(path)
    try:
        created = store.configure(normalized)
    except BaseException:
        store.close()
        raise
    return store, created
