"""Entity cohort migration reconciliation on streaming JSONL + SQLite external storage.

The legacy :func:`distribution_drift.model.analyze` contract only compares two
aggregate marginal frequency snapshots: a PSI of 0 cannot tell retention from a
wholesale swap of categories between entities.  This module adds an
entity-level reconciliation pipeline.

Design summary (see docs/cohort.md for the full discussion):

* Inputs are UTF-8 JSONL observation streams ``{"entity", "category"}`` plus a
  config ``{"categories", "alpha"}`` and, for reconcile, ruling JSONL.  Every
  line is validated strictly (no blank lines, no duplicate JSON keys, no
  non-finite constants, no unknown fields, a 65536-byte line ceiling).
* All entity-proportional state lives in a *per-invocation* temporary SQLite
  database.  Python only ever holds scalar counters, the bounded category
  domain and single rows/cursors; nothing is ever ``fetchall``-ed and the
  category cartesian product is never materialised (only non-zero cells are
  stored).
* Same-side same-entity same-category repeats are folded in the clustered
  ``obs`` table while the original raw row count is retained.  Same-side
  entities observed under several distinct categories are *conflicts*, never
  resolved by last-row-wins or frequency voting.
* Rulings are validated against the audited candidates as they stream in and
  are themselves stored externally.  Eligibility is decided only afterwards,
  for both sides of every entity at once: an unresolved or explicitly excluded
  conflict isolates the whole entity on both sides.
* Migration cells are a SQL full outer join over the accepted entity set;
  ``NULL`` means "no observation on that side" and is kept distinct from
  category strings.  The accepted marginals are handed back to
  :func:`distribution_drift.model.analyze` with the full declared domain.
"""
from contextlib import contextmanager
import io
import json
import os
import sqlite3
import tempfile
from pathlib import Path

from .common import ident, number, parse_strict, read_bytes
from .model import analyze

SIDES = ('reference', 'current')
MAX_ROWS_PER_SIDE = 10_000_000
MAX_LINE_BYTES = 65_536
MAX_DOMAIN_CATEGORIES = 1_000
MAX_ALPHA = 1_000_000
_READ_CHUNK = 8_192
_INSERT_BATCH = 10_000
# Keep the SQLite page cache at roughly 8 MiB so RSS stays bounded regardless
# of how large the temporary database grows on disk.
_CACHE_SIZE_KIB = -8_192


class CohortError(ValueError):
    """A malformed line, invalid ruling, contract or output-safety violation."""


# ---------------------------------------------------------------------------
# strict validation
# ---------------------------------------------------------------------------

def _ident(value, field):
    try:
        return ident(value)
    except ValueError as exc:
        raise CohortError(field + ': ' + str(exc))


def validate_config(obj):
    """Validate ``{categories, alpha}``; return ``(domain, alpha, domain_set)``.

    The domain and alpha rules mirror :func:`distribution_drift.model.analyze`
    exactly: 1..1000 unique identifiers, strict positive integer alpha.
    """
    if not isinstance(obj, dict) or set(obj) != {'categories', 'alpha'}:
        raise CohortError('config must contain exactly categories and alpha')
    raw = obj['categories']
    if not isinstance(raw, list) or not raw or len(raw) > MAX_DOMAIN_CATEGORIES:
        raise CohortError(
            'categories must be a nonempty array of at most %d identifiers' % MAX_DOMAIN_CATEGORIES)
    domain = []
    seen = set()
    for category in raw:
        name = _ident(category, 'category')
        if name in seen:
            raise CohortError('duplicate config category: ' + name)
        seen.add(name)
        domain.append(name)
    try:
        alpha = number(obj['alpha'], 1, MAX_ALPHA)
    except ValueError as exc:
        raise CohortError(str(exc))
    return domain, alpha, frozenset(domain)


def load_config(path):
    """Read and validate a config file (strict JSON, 8 MB ceiling)."""
    return validate_config(parse_strict(read_bytes(path)))


def _reject_duplicate_key(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise CohortError('duplicate JSON key: ' + str(key))
        result[key] = value
    return result


def _reject_constant(value):
    raise CohortError('non-finite JSON constant is not allowed: ' + value)


def _check_length(raw):
    if len(raw) > MAX_LINE_BYTES:
        raise CohortError('line exceeds %d bytes' % MAX_LINE_BYTES)
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
                raise CohortError('input iterables must yield bytes lines')
            yield _check_length(bytes(raw))


def decode_line(raw):
    """Strictly parse one JSONL line into a Python object."""
    body = raw[:-1] if raw.endswith(b'\n') else raw
    if not body or not body.strip():
        raise CohortError('empty lines are not allowed')
    try:
        text = body.decode('utf-8')
    except UnicodeDecodeError as exc:
        raise CohortError('line is not valid UTF-8: ' + str(exc))
    try:
        return json.loads(text, object_pairs_hook=_reject_duplicate_key,
                          parse_constant=_reject_constant)
    except CohortError:
        raise
    except ValueError as exc:
        raise CohortError('invalid JSON line: ' + str(exc))


def parse_observation(obj, domain):
    """Validate one ``{"entity", "category"}`` record against the domain."""
    if not isinstance(obj, dict) or set(obj) != {'entity', 'category'}:
        raise CohortError('each observation must be exactly {"entity": ..., "category": ...}')
    entity = _ident(obj['entity'], 'entity')
    category = obj['category']
    if not isinstance(category, str):
        raise CohortError('category must be a string')
    if category not in domain:
        raise CohortError('category outside configured domain: ' + repr(category))
    return entity, category


def parse_ruling(obj, domain):
    """Validate one adjudication record.

    Returns ``(side, entity, expected_tuple, selected)`` where ``selected`` is
    either a domain category or ``None`` (explicit exclusion).
    """
    if not isinstance(obj, dict) or set(obj) != {
            'side', 'entity', 'expected_categories', 'selected'}:
        raise CohortError(
            'each ruling must be exactly {"side", "entity", "expected_categories", "selected"}')
    side = obj['side']
    if side not in SIDES:
        raise CohortError('ruling side must be reference or current')
    entity = _ident(obj['entity'], 'entity')
    expected = obj['expected_categories']
    if not isinstance(expected, list) or not expected:
        raise CohortError('expected_categories must be a nonempty array')
    candidates = []
    seen = set()
    for category in expected:
        if not isinstance(category, str):
            raise CohortError('expected_categories entries must be strings')
        if category in seen:
            raise CohortError('expected_categories must be unique')
        if category not in domain:
            raise CohortError('expected category outside configured domain: ' + repr(category))
        seen.add(category)
        candidates.append(category)
    if candidates != sorted(candidates):
        raise CohortError('expected_categories must be sorted in Unicode order')
    selected = obj['selected']
    if selected is not None and (not isinstance(selected, str) or selected not in seen):
        raise CohortError('selected must be null or one of expected_categories')
    return side, entity, tuple(candidates), selected


# ---------------------------------------------------------------------------
# external store
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE obs(
  side TEXT NOT NULL,
  entity TEXT NOT NULL,
  category TEXT NOT NULL,
  rows INTEGER NOT NULL,
  PRIMARY KEY(side, entity, category)
) WITHOUT ROWID;
CREATE TABLE ruling(
  side TEXT NOT NULL,
  entity TEXT NOT NULL,
  selected TEXT,
  PRIMARY KEY(side, entity)
) WITHOUT ROWID;
"""

_MIGRATION_SQL = """
CREATE TABLE entity_union(
  entity TEXT PRIMARY KEY
) WITHOUT ROWID;
INSERT INTO entity_union SELECT DISTINCT entity FROM obs;

CREATE TABLE side_entity(
  side TEXT NOT NULL,
  entity TEXT NOT NULL,
  ncat INTEGER NOT NULL,
  has_ruling INTEGER NOT NULL,
  selected TEXT,
  PRIMARY KEY(side, entity)
) WITHOUT ROWID;
INSERT INTO side_entity
SELECT o.side, o.entity, COUNT(*),
       EXISTS(SELECT 1 FROM ruling r WHERE r.side = o.side AND r.entity = o.entity),
       (SELECT r.selected FROM ruling r WHERE r.side = o.side AND r.entity = o.entity)
FROM obs o
GROUP BY o.side, o.entity;

-- Eligibility is decided for the whole entity across BOTH sides at once: any
-- unresolved conflict (no ruling) or explicit exclusion (selected IS NULL) on
-- either side isolates every observation of that entity on both sides.
-- EXCEPT (not a correlated NOT EXISTS) keeps this a single streaming set
-- difference: side_entity's leading index column is "side", so per-entity
-- NOT EXISTS lookups would degrade to quadratic scans.
CREATE TABLE eligible(
  entity TEXT PRIMARY KEY
) WITHOUT ROWID;
INSERT INTO eligible
SELECT entity FROM entity_union
EXCEPT
SELECT entity FROM side_entity
WHERE ncat > 1 AND (has_ruling = 0 OR selected IS NULL);

CREATE TABLE effective(
  side TEXT NOT NULL,
  entity TEXT NOT NULL,
  category TEXT NOT NULL,
  PRIMARY KEY(side, entity)
) WITHOUT ROWID;
INSERT INTO effective
SELECT o.side, o.entity, MIN(o.category)
FROM obs o
JOIN side_entity s ON s.side = o.side AND s.entity = o.entity
JOIN eligible g ON g.entity = o.entity
WHERE s.ncat = 1
GROUP BY o.side, o.entity;
INSERT INTO effective
SELECT s.side, s.entity, s.selected
FROM side_entity s
JOIN eligible g ON g.entity = s.entity
WHERE s.ncat > 1 AND s.has_ruling = 1 AND s.selected IS NOT NULL;
-- The cell join below looks effective up by entity alone; the WITHOUT ROWID
-- primary key leads with side, so an entity-leading index is required to
-- keep the full outer join O(n log n) instead of quadratic.
CREATE INDEX effective_entity_idx ON effective(entity);

CREATE TABLE cell(
  refcat TEXT,
  curcat TEXT,
  n INTEGER NOT NULL
);
INSERT INTO cell
SELECT e0.category, e1.category, COUNT(*)
FROM eligible g
LEFT JOIN effective e0 ON e0.entity = g.entity AND e0.side = 'reference'
LEFT JOIN effective e1 ON e1.entity = g.entity AND e1.side = 'current'
WHERE e0.entity IS NOT NULL OR e1.entity IS NOT NULL
GROUP BY e0.category, e1.category;
"""


class CohortStore:
    """Temporary SQLite external storage for one audit/reconcile run.

    The database file is created per instance and deleted on :meth:`close`;
    it is never a cross-run authority.  All scans use streaming cursors.
    """

    def __init__(self, workdir=None):
        fd, self.path = tempfile.mkstemp(prefix='cohort-', suffix='.sqlite', dir=workdir)
        os.close(fd)
        self.connection = sqlite3.connect(self.path)
        con = self.connection
        con.execute('PRAGMA journal_mode=OFF')
        con.execute('PRAGMA synchronous=OFF')
        con.execute('PRAGMA temp_store=FILE')
        con.execute('PRAGMA cache_size=%d' % _CACHE_SIZE_KIB)
        con.execute('PRAGMA mmap_size=0')
        con.executescript(_SCHEMA)
        self.raw_rows = {side: 0 for side in SIDES}
        self._migration_built = False

    # -- lifecycle ----------------------------------------------------------
    def close(self):
        try:
            self.connection.close()
        finally:
            for suffix in ('', '-journal', '-wal', '-shm'):
                try:
                    os.unlink(self.path + suffix)
                except OSError:
                    pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- ingestion ----------------------------------------------------------
    def ingest_observations(self, side, source, domain):
        """Stream one side's JSONL observations, folding repeats as we go."""
        if side not in SIDES:
            raise CohortError('side must be reference or current')
        con = self.connection
        count = 0
        batch = []
        con.execute('BEGIN')
        try:
            for raw in iter_lines(source):
                entity, category = parse_observation(decode_line(raw), domain)
                batch.append((side, entity, category))
                count += 1
                if count > MAX_ROWS_PER_SIDE:
                    raise CohortError(
                        '%s exceeds the %d row limit' % (side, MAX_ROWS_PER_SIDE))
                if len(batch) >= _INSERT_BATCH:
                    self._flush_observations(batch)
                    batch.clear()
            if batch:
                self._flush_observations(batch)
            con.commit()
        except BaseException:
            con.rollback()
            raise
        self.raw_rows[side] += count
        return count

    def _flush_observations(self, batch):
        self.connection.executemany(
            'INSERT INTO obs(side, entity, category, rows) VALUES(?, ?, ?, 1) '
            'ON CONFLICT(side, entity, category) DO UPDATE SET rows = rows + 1',
            batch)

    def ingest_rulings(self, source, domain):
        """Stream and validate rulings against the audited candidates."""
        con = self.connection
        count = 0
        con.execute('BEGIN')
        try:
            for raw in iter_lines(source):
                side, entity, expected, selected = parse_ruling(decode_line(raw), domain)
                cursor = con.execute(
                    'SELECT category FROM obs WHERE side = ? AND entity = ? ORDER BY category',
                    (side, entity))
                candidates = tuple(row[0] for row in cursor)  # bounded by domain size
                if not candidates:
                    raise CohortError(
                        'ruling targets entity %r that does not exist on side %s' % (entity, side))
                if len(candidates) == 1:
                    raise CohortError(
                        'ruling targets non-conflict entity %r on side %s' % (entity, side))
                if candidates != expected:
                    raise CohortError(
                        'ruling candidate set mismatch for %s/%r: expected %r, audited %r'
                        % (side, entity, list(expected), list(candidates)))
                try:
                    con.execute(
                        'INSERT INTO ruling(side, entity, selected) VALUES(?, ?, ?)',
                        (side, entity, selected))
                except sqlite3.IntegrityError:
                    raise CohortError(
                        'duplicate ruling for %s/%r' % (side, entity))
                count += 1
            con.commit()
        except BaseException:
            con.rollback()
            raise
        return count

    # -- audit --------------------------------------------------------------
    def iter_conflicts(self):
        """Yield conflict records in side/entity/category Unicode order.

        Only entities with several *distinct* categories on the same side are
        emitted; per-entity state is bounded by the declared domain.
        """
        cursor = self.connection.execute(
            'SELECT o.side, o.entity, o.category, o.rows '
            'FROM obs o '
            'JOIN (SELECT side, entity FROM obs GROUP BY side, entity HAVING COUNT(*) > 1) c '
            'ON c.side = o.side AND c.entity = o.entity '
            'ORDER BY o.side, o.entity, o.category')
        current_key = None
        candidates = None
        for side, entity, category, rows in cursor:
            key = (side, entity)
            if key != current_key:
                if current_key is not None:
                    yield {'side': current_key[0], 'entity': current_key[1],
                           'candidates': candidates}
                current_key = key
                candidates = [{'category': category, 'rows': rows}]
            else:
                candidates.append({'category': category, 'rows': rows})
        if current_key is not None:
            yield {'side': current_key[0], 'entity': current_key[1],
                   'candidates': candidates}

    def conflict_count(self, side):
        return self.connection.execute(
            'SELECT COUNT(*) FROM ('
            'SELECT entity FROM obs WHERE side = ? GROUP BY entity HAVING COUNT(*) > 1)',
            (side,)).fetchone()[0]

    def folded_duplicate_rows(self, side):
        return self.connection.execute(
            'SELECT COALESCE(SUM(rows), 0) - COUNT(*) FROM obs WHERE side = ?',
            (side,)).fetchone()[0]

    # -- reconciliation -----------------------------------------------------
    def build_migration(self):
        """Materialise eligibility, effective categories and migration cells."""
        if self._migration_built:
            return
        self.connection.executescript(_MIGRATION_SQL)
        self._migration_built = True

    def side_audit(self):
        """Per-side raw/fold/conflict/selected counts (available pre-migration)."""
        out = {}
        for side in SIDES:
            # ingest_rulings guarantees rulings can only reference real
            # conflict entities, so selected rows equal selected conflicts.
            selected = self.connection.execute(
                'SELECT COUNT(*) FROM ruling WHERE side = ? AND selected IS NOT NULL',
                (side,)).fetchone()[0]
            out[side] = {
                'raw_rows': self.raw_rows[side],
                'folded_duplicate_rows': self.folded_duplicate_rows(side),
                'conflict_entities': self.conflict_count(side),
                'selected_conflicts': selected,
            }
        return out

    def movement(self):
        def scalar(where):
            return self.connection.execute(
                'SELECT COALESCE(SUM(n), 0) FROM cell WHERE ' + where).fetchone()[0]
        return {
            'retained': scalar("refcat IS NOT NULL AND curcat IS NOT NULL"),
            'changed': scalar("refcat IS NOT NULL AND curcat IS NOT NULL AND refcat != curcat"),
            'entered': scalar('refcat IS NULL'),
            'exited': scalar('curcat IS NULL'),
        }

    def iter_cells(self):
        # Default SQLite ordering: NULL sorts before any text value, matching
        # Python's own tuple ordering, so cells come out in a fully
        # deterministic (from, to) sequence with no null/string ambiguity.
        return self.connection.execute(
            'SELECT refcat, curcat, n FROM cell ORDER BY refcat, curcat')

    def iter_marginals(self, side):
        column = 'refcat' if side == 'reference' else 'curcat'
        return self.connection.execute(
            'SELECT %s, SUM(n) FROM cell WHERE %s IS NOT NULL GROUP BY %s ORDER BY %s'
            % (column, column, column, column))

    def accepted_total(self, side):
        return self.connection.execute(
            'SELECT COUNT(*) FROM effective WHERE side = ?', (side,)).fetchone()[0]

    def iter_unresolved_entities(self):
        return self.connection.execute(
            'SELECT DISTINCT entity FROM side_entity '
            'WHERE ncat > 1 AND has_ruling = 0 ORDER BY entity')

    def iter_explicitly_excluded_entities(self):
        return self.connection.execute(
            'SELECT DISTINCT entity FROM side_entity '
            'WHERE ncat > 1 AND has_ruling = 1 AND selected IS NULL ORDER BY entity')

    def iter_excluded_entities(self):
        # Union of the two isolation reasons, deduplicated by GROUP BY.
        return self.connection.execute(
            'SELECT entity FROM side_entity '
            'WHERE ncat > 1 AND (has_ruling = 0 OR selected IS NULL) '
            'GROUP BY entity ORDER BY entity')


# ---------------------------------------------------------------------------
# high level API
# ---------------------------------------------------------------------------

def run_audit(reference, current, config, *, workdir=None):
    """Ingest both sides and return an open :class:`CohortStore` for auditing."""
    domain, _, domain_set = validate_config(config)
    store = CohortStore(workdir)
    try:
        store.ingest_observations('reference', reference, domain_set)
        store.ingest_observations('current', current, domain_set)
    except BaseException:
        store.close()
        raise
    return store


def run_reconcile(reference, current, config, rulings, *, workdir=None):
    """Ingest observations + rulings and build the migration structures."""
    domain, _, domain_set = validate_config(config)
    store = CohortStore(workdir)
    try:
        store.ingest_observations('reference', reference, domain_set)
        store.ingest_observations('current', current, domain_set)
        store.ingest_rulings(rulings, domain_set)
        store.build_migration()
    except BaseException:
        store.close()
        raise
    return store


@contextmanager
def atomic_text_output(path):
    """Open a temp sibling file and atomically replace ``path`` on success."""
    dest = Path(path)
    if dest.is_symlink():
        raise CohortError('output cannot be a symlink')
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.' + dest.name + '.', dir=str(dest.parent))
    stream = io.open(fd, 'w', encoding='utf-8', newline='\n')
    published = False
    try:
        yield stream
        stream.flush()
        os.fsync(stream.fileno())
        published = True
    finally:
        stream.close()
        if published:
            os.replace(tmp, dest)
        else:
            try:
                os.unlink(tmp)
            except OSError:
                pass


def _json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, sort_keys=True)


def write_audit_jsonl(store, output):
    """Stream conflict records as JSONL and publish the file atomically."""
    conflicts = 0
    with atomic_text_output(output) as stream:
        for record in store.iter_conflicts():
            stream.write(_json(record))
            stream.write('\n')
            conflicts += 1
    return conflicts


def _write_entity_array(stream, cursor):
    stream.write('[')
    first = True
    for (entity,) in cursor:
        if not first:
            stream.write(',')
        first = False
        stream.write(json.dumps(entity, ensure_ascii=False))
    stream.write(']')


def write_reconcile_report(store, output, domain, alpha):
    """Stream the full reconciliation report and publish it atomically."""
    store.build_migration()
    con = store.connection
    totals = {side: store.accepted_total(side) for side in SIDES}
    marginals = {}
    for side in SIDES:
        marginals[side] = {category: count for category, count in store.iter_marginals(side)}
    if totals['reference'] > 0 and totals['current'] > 0:
        distribution = analyze({
            'categories': list(domain),
            'alpha': alpha,
            'reference': [{'category': c, 'count': v} for c, v in marginals['reference'].items()],
            'current': [{'category': c, 'count': v} for c, v in marginals['current'].items()],
        })
        distribution_status = 'ok'
    else:
        # No pseudo-counts are fabricated: the legacy contract itself rejects
        # zero-total snapshots, so distribution is explicitly absent.
        distribution = None
        distribution_status = 'insufficient_population'

    with atomic_text_output(output) as stream:
        w = stream.write
        w('{')
        w('"schema_version":1,')
        w('"alpha":' + str(alpha) + ',')
        w('"categories":' + _json(list(domain)) + ',')
        w('"audit":' + _json(store.side_audit()) + ',')
        w('"movement":' + _json(store.movement()) + ',')

        w('"cells":[')
        first = True
        for refcat, curcat, count in store.iter_cells():
            if not first:
                w(',')
            first = False
            w(_json({'from': refcat, 'to': curcat, 'count': count}))
        w('],')

        for side, key in (('reference', 'reference_marginals'),
                          ('current', 'current_marginals')):
            w('"%s":{' % key)
            first = True
            for category, count in store.iter_marginals(side):
                if not first:
                    w(',')
                first = False
                w(json.dumps(category, ensure_ascii=False) + ':' + str(count))
            w('},')

        w('"totals":' + _json({'reference': totals['reference'],
                               'current': totals['current']}) + ',')
        w('"excluded_entities":')
        _write_entity_array(stream, store.iter_excluded_entities())
        w(',"unresolved_entities":')
        _write_entity_array(stream, store.iter_unresolved_entities())
        w(',"explicitly_excluded_entities":')
        _write_entity_array(stream, store.iter_explicitly_excluded_entities())
        w(',"distribution_status":' + json.dumps(distribution_status))
        w(',"distribution":')
        if distribution is None:
            w('null')
        else:
            w(json.dumps(distribution, ensure_ascii=False, allow_nan=False, sort_keys=True))
        w('}')
    return {
        'totals': totals,
        'movement': store.movement(),
        'distribution_status': distribution_status,
        'excluded_entities': con.execute(
            'SELECT COUNT(*) FROM (SELECT 1 FROM side_entity WHERE ncat > 1 '
            'AND (has_ruling = 0 OR selected IS NULL) GROUP BY entity)').fetchone()[0],
        'unresolved_entities': con.execute(
            'SELECT COUNT(DISTINCT entity) FROM side_entity WHERE ncat > 1 AND has_ruling = 0'
        ).fetchone()[0],
        'explicitly_excluded_entities': con.execute(
            'SELECT COUNT(DISTINCT entity) FROM side_entity '
            'WHERE ncat > 1 AND has_ruling = 1 AND selected IS NULL').fetchone()[0],
    }
