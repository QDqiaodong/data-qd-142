"""Cross-batch safe aggregate publication workbench (SQLite, stdlib only).

The data team publishes category-sum aggregates of one fixed private count
vector ``x`` to outside partners.  Any single published sum may look harmless,
yet two individually safe sums can be combined by exact subtraction to expose
a protected category count.  This module keeps a persistent public-knowledge
ledger of every equation ever published and only releases a new batch when
the optimal selection is provably safe against the *complete* published
history at commit time.  No new PSI/drift computation, no entity pairing, no
revision chains and no time windows.

Design summary (see docs/disclosure.md for the full discussion):

* A query with member set ``M`` is the 0/1 row vector ``a`` over the declared
  categories and publishing it adds the equation ``a.x = value`` to the public
  record.  Safety is defined over exact rational linear inference only: no
  protected unit vector ``e_c`` may lie in the rational row space spanned by
  all historically published rows plus the rows selected now.  There is no
  default public total and no other prior equation.  Integrality,
  non-negativity, external knowledge and selection side channels are out of
  scope; no differential privacy is claimed.
* The planner maximises total utility over all safe subsets of the batch
  (ties: the lexicographically smallest tuple of Unicode-sorted selected
  ids).  It works in exact rational arithmetic (``fractions.Fraction``) with
  a reduced row echelon basis of the published row space; floating point
  ranks, single-query checks, batch-only checks or pairwise-difference
  checks would all be unsound and are not used.
* The exact combinatorial solve never runs inside a SQLite write
  transaction.  ``release`` reads a consistent knowledge snapshot in a short
  read transaction, solves outside any transaction, then commits in a short
  ``BEGIN IMMEDIATE`` transaction that re-checks the history generation:
  if another release moved the generation forward, the stale plan is
  discarded, the transaction rolls back and the whole attempt retries
  against the new history.  A killed solver therefore never blocks later
  releases and a stale executor can never overwrite newer decisions.
* Every committed release atomically stores the new public equations, the
  monotonic history generation, the canonical request identity and the
  deterministic public receipt.  Replaying the same request id with the
  canonically identical request returns the stored receipt bytes verbatim
  (even after the knowledge moved on); the same id with a different request
  is rejected.  Empty selections are recorded with their generation too.
"""
from contextlib import contextmanager
from fractions import Fraction
import hashlib
import json
import sqlite3
from pathlib import Path

from .common import ident, parse_strict

MIN_CATEGORIES = 2
MAX_CATEGORIES = 32
MAX_QUERIES = 22
MAX_UTILITY = 10 ** 6
MAX_COUNT = 10 ** 9
# Each retry means another release committed a knowledge-changing batch in
# between; the bound only guards against pathological livelock.
MAX_ATTEMPTS = 1000


class DisclosureError(ValueError):
    """Base class for rejected specs, requests and ledger operations."""


class ConflictError(DisclosureError):
    """Same request_id (or database) already registered with different content."""


# ---------------------------------------------------------------------------
# strict validation
# ---------------------------------------------------------------------------

def _ident(value, field):
    try:
        return ident(value)
    except ValueError as exc:
        raise DisclosureError(field + ': ' + str(exc))


def _strict_int(value, field, minimum, maximum):
    # bool is a subclass of int; the contract forbids booleans explicitly.
    if type(value) is not int:
        raise DisclosureError(field + ' must be a strict integer (booleans rejected)')
    if not minimum <= value <= maximum:
        raise DisclosureError('%s must be within %d..%d' % (field, minimum, maximum))
    return value


def validate_spec(obj):
    """Validate ``{categories, counts, protected}``; return a normalized copy.

    The normalized form sorts every array, so the private vector identity is
    order-insensitive while identifiers still match exactly.  ``categories``
    holds 2..32 unique identifiers; ``counts`` declares every category
    exactly once with a strict 0..10^9 integer; ``protected`` is a nonempty
    unique subset of the declared categories.
    """
    if not isinstance(obj, dict) or set(obj) != {'categories', 'counts', 'protected'}:
        raise DisclosureError('spec must contain exactly categories, counts, protected')
    raw_categories = obj['categories']
    if not isinstance(raw_categories, list) or \
            not MIN_CATEGORIES <= len(raw_categories) <= MAX_CATEGORIES:
        raise DisclosureError('categories must be an array of %d..%d unique identifiers'
                              % (MIN_CATEGORIES, MAX_CATEGORIES))
    categories = []
    seen = set()
    for value in raw_categories:
        name = _ident(value, 'category')
        if name in seen:
            raise DisclosureError('duplicate category: ' + name)
        seen.add(name)
        categories.append(name)
    raw_counts = obj['counts']
    if not isinstance(raw_counts, list):
        raise DisclosureError('counts must be an array of {category, count} rows')
    entries = []
    counted = set()
    for row in raw_counts:
        if not isinstance(row, dict) or set(row) != {'category', 'count'}:
            raise DisclosureError('each count row must contain exactly category and count')
        category = _ident(row['category'], 'count category')
        if category in counted:
            raise DisclosureError('duplicate count category: ' + category)
        counted.add(category)
        entries.append((category, _strict_int(row['count'], 'count', 0, MAX_COUNT)))
    if counted != seen:
        raise DisclosureError('counts must declare every declared category exactly once')
    raw_protected = obj['protected']
    if not isinstance(raw_protected, list) or not raw_protected:
        raise DisclosureError('protected must be a nonempty array of declared categories')
    protected = []
    pseen = set()
    for value in raw_protected:
        name = _ident(value, 'protected')
        if name in pseen:
            raise DisclosureError('duplicate protected category: ' + name)
        if name not in seen:
            raise DisclosureError('protected category outside declared domain: ' + repr(name))
        pseen.add(name)
        protected.append(name)
    return {'categories': sorted(categories),
            'counts': [{'category': c, 'count': v} for c, v in sorted(entries)],
            'protected': sorted(protected)}


def validate_request(obj, declared):
    """Validate ``{request_id, queries}`` against the declared domain.

    Returns the canonical request: queries sorted by id, members sorted.
    Member and query array order is irrelevant for identity; utility and any
    set content are part of it.  ``declared`` is the set of declared
    categories from the fixed spec.
    """
    if not isinstance(obj, dict) or set(obj) != {'request_id', 'queries'}:
        raise DisclosureError('request must contain exactly request_id and queries')
    request_id = _ident(obj['request_id'], 'request_id')
    raw_queries = obj['queries']
    if not isinstance(raw_queries, list) or not 1 <= len(raw_queries) <= MAX_QUERIES:
        raise DisclosureError('queries must be a nonempty array of at most %d entries'
                              % MAX_QUERIES)
    queries = []
    seen = set()
    for row in raw_queries:
        if not isinstance(row, dict) or set(row) != {'id', 'members', 'utility'}:
            raise DisclosureError('each query must contain exactly id, members, utility')
        qid = _ident(row['id'], 'query id')
        if qid in seen:
            raise DisclosureError('duplicate query id in request: ' + qid)
        seen.add(qid)
        raw_members = row['members']
        if not isinstance(raw_members, list) or not raw_members:
            raise DisclosureError('members must be a nonempty array of declared categories')
        members = set()
        for value in raw_members:
            name = _ident(value, 'member')
            if name in members:
                raise DisclosureError('duplicate member in query ' + qid + ': ' + name)
            if name not in declared:
                raise DisclosureError('member outside declared domain: ' + repr(name))
            members.add(name)
        utility = _strict_int(row['utility'], 'utility', 1, MAX_UTILITY)
        queries.append({'id': qid, 'members': sorted(members), 'utility': utility})
    queries.sort(key=lambda q: q['id'])
    return {'request_id': request_id, 'queries': queries}


def _canonical_text(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(',', ':'))


def parse_spec_bytes(raw):
    try:
        return validate_spec(parse_strict(raw))
    except (ValueError, TypeError) as exc:
        if isinstance(exc, DisclosureError):
            raise
        raise DisclosureError(str(exc))


def parse_request_bytes(raw, declared):
    try:
        return validate_request(parse_strict(raw), declared)
    except (ValueError, TypeError) as exc:
        if isinstance(exc, DisclosureError):
            raise
        raise DisclosureError(str(exc))


# ---------------------------------------------------------------------------
# exact rational linear algebra over the published row space
# ---------------------------------------------------------------------------

def _reduce(vec, basis):
    """Subtract basis pivot multiples from ``vec``; return the remainder."""
    r = list(vec)
    for pivot, row in basis.items():
        coeff = r[pivot]
        if coeff:
            r = [a - coeff * b for a, b in zip(r, row)]
    return tuple(r)


def _pivot(vec):
    for i, a in enumerate(vec):
        if a:
            return i
    return None


def _extend(basis, rems, w):
    """Extend the RREF basis with reduced nonzero vector ``w``.

    ``rems`` holds the protected unit vectors reduced by the current basis.
    Returns ``(new_basis, new_rems)`` or ``None`` when a protected vector
    enters the enlarged span (its remainder becomes zero).
    """
    p = _pivot(w)
    inv = Fraction(1) / w[p]
    wn = tuple(a * inv for a in w)
    new_basis = {}
    for q, row in basis.items():
        coeff = row[p]
        new_basis[q] = tuple(a - coeff * b for a, b in zip(row, wn)) if coeff else row
    new_basis[p] = wn
    new_rems = []
    for g in rems:
        coeff = g[p]
        g2 = tuple(a - coeff * b for a, b in zip(g, wn)) if coeff else g
        if not any(g2):
            return None
        new_rems.append(g2)
    return new_basis, new_rems


def _unit(i, n):
    v = [Fraction(0)] * n
    v[i] = Fraction(1)
    return tuple(v)


def plan(n, history_rows, protected, candidates):
    """Choose the optimal safely publishable subset of ``candidates``.

    ``history_rows`` are the 0/1 rows of every equation published so far,
    ``protected`` the protected category indices and ``candidates`` a list of
    ``(qid, vector, utility)``.  Returns ``(selected_ids, total_utility)``
    where ``selected_ids`` is the id tuple sorted in Unicode order.  Safety
    of a set S means: no protected unit vector lies in the rational row
    space of history + S.  The empty set is always allowed.
    """
    basis0 = {}
    for row in history_rows:
        w = _reduce(tuple(Fraction(x) for x in row), basis0)
        if _pivot(w) is not None:
            basis0, _ = _extend(basis0, [], w)
    rems0 = []
    for c in protected:
        g = _reduce(_unit(c, n), basis0)
        if _pivot(g) is None:
            raise DisclosureError('public history already determines a protected category')
        rems0.append(g)
    items = [(qid, tuple(Fraction(x) for x in vec), utility)
             for (qid, vec, utility) in candidates]
    m = len(items)

    def include(basis, rems, vec):
        """(basis, rems) with ``vec`` added, or None when unsafe."""
        w = _reduce(vec, basis)
        if _pivot(w) is None:
            return basis, rems  # already inside the span: adds no knowledge
        return _extend(basis, rems, w)

    # Single DFS over candidates in ascending id order, optimising
    # (total utility, then lexicographically smallest sorted id tuple).
    # Prune rules:
    #   * safety: ``include`` returns None once a protected vector enters
    #     the span; unsafe sets are upward closed, so the branch dies;
    #   * utility: current + remaining-suffix < best utility — the subtree
    #     cannot reach the best value;
    #   * tuple: current + remaining-suffix == best utility and the decided
    #     id prefix already forces a lexicographically worse tuple than the
    #     incumbent — for two sets S, T the smaller sorted id tuple is the
    #     one holding min(S symmetric-difference T), and all undecided ids
    #     are larger than every decided one, so a decided prefix whose
    #     first difference with the incumbent belongs to the incumbent can
    #     never improve the tuple.
    order = sorted(range(m), key=lambda i: items[i][0])
    suffix = [0] * (m + 1)
    for k in range(m - 1, -1, -1):
        suffix[k] = suffix[k + 1] + items[order[k]][2]
    best_utility = 0
    best_tuple = ()

    def tuple_prefix_worse(chosen, incumbent, next_id):
        """True when every completion of ``chosen`` loses to ``incumbent``.

        ``chosen`` holds the included decided ids (all < next_id); incumbent
        ids >= next_id are undecided and ignored.  Scans for the first
        decided id that is in exactly one of the two tuples.
        """
        i = j = 0
        while True:
            a = chosen[i] if i < len(chosen) else None
            b = incumbent[j] if j < len(incumbent) else None
            if b is not None and next_id is not None and b >= next_id:
                b = None
            if a is None and b is None:
                return False
            if a is None:
                return True
            if b is None:
                return False
            if a < b:
                return False
            if b < a:
                return True
            i += 1
            j += 1

    def dfs(k, utility, basis, rems, chosen):
        nonlocal best_utility, best_tuple
        if utility + suffix[k] < best_utility:
            return
        if utility + suffix[k] == best_utility and \
                tuple_prefix_worse(chosen, best_tuple,
                                   items[order[k]][0] if k < m else None):
            return
        if k == m:
            if utility > best_utility or \
                    (utility == best_utility and chosen < best_tuple):
                best_utility = utility
                best_tuple = chosen
            return
        i = order[k]
        nxt = include(basis, rems, items[i][1])
        if nxt is not None:
            dfs(k + 1, utility + items[i][2], nxt[0], nxt[1],
                chosen + (items[i][0],))
        dfs(k + 1, utility, basis, rems, chosen)

    dfs(0, 0, basis0, rems0, ())
    return best_tuple, best_utility


# ---------------------------------------------------------------------------
# persistent public-knowledge ledger
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS request(
  request_id TEXT PRIMARY KEY,
  request_sha256 TEXT NOT NULL,
  canonical TEXT NOT NULL,
  generation INTEGER NOT NULL,
  receipt TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS equation(
  request_id TEXT NOT NULL,
  query_id TEXT NOT NULL,
  members TEXT NOT NULL,
  value INTEGER NOT NULL,
  PRIMARY KEY(request_id, query_id)
) WITHOUT ROWID;
"""


@contextmanager
def _write_transaction(connection):
    connection.execute('BEGIN IMMEDIATE')
    try:
        yield
    except BaseException:
        if connection.in_transaction:
            connection.execute('ROLLBACK')
        raise
    else:
        connection.execute('COMMIT')


class Store:
    """SQLite-backed ledger of the private vector and the public knowledge."""

    def __init__(self, path):
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, timeout=30.0)
        # autocommit mode: BEGIN/COMMIT boundaries are managed explicitly.
        self.connection.isolation_level = None
        self.connection.execute('PRAGMA busy_timeout=30000')
        self.connection.row_factory = sqlite3.Row
        self.connection.executescript(SCHEMA)
        row = self.connection.execute(
            "SELECT value FROM meta WHERE key = 'config'").fetchone()
        self._spec = json.loads(row['value']) if row is not None else None

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def spec(self):
        if self._spec is None:
            raise DisclosureError('database is not initialized; run init with a spec first')
        return self._spec

    # -- init ---------------------------------------------------------------
    def configure(self, spec):
        """Fix the validated private vector in this database.

        Re-running with the canonically identical spec is an idempotent
        no-op; any difference in categories, counts or protected is rejected.
        Returns True when the database was freshly initialized.
        """
        canonical = _canonical_text(spec)
        con = self.connection
        with _write_transaction(con):
            row = con.execute("SELECT value FROM meta WHERE key = 'config'").fetchone()
            if row is not None:
                if row['value'] != canonical:
                    raise DisclosureError(
                        'database already initialized with a different private vector')
                return False
            con.execute("INSERT INTO meta(key, value) VALUES ('config', ?)", (canonical,))
            con.execute("INSERT INTO meta(key, value) VALUES ('generation', '0')")
        self._spec = spec
        return True

    # -- knowledge snapshot ---------------------------------------------------
    def _generation(self):
        row = self.connection.execute(
            "SELECT value FROM meta WHERE key = 'generation'").fetchone()
        return int(row['value']) if row is not None else 0

    def _knowledge(self):
        """Consistent ``(generation, published_rows)`` snapshot.

        Both SELECTs run inside one short read transaction, so the row set
        and the generation can never be observed from different commits.
        """
        spec = self.spec
        index = {c: i for i, c in enumerate(spec['categories'])}
        n = len(spec['categories'])
        con = self.connection
        con.execute('BEGIN')
        try:
            generation = self._generation()
            rows = []
            for r in con.execute('SELECT members FROM equation'):
                vec = [0] * n
                for name in json.loads(r['members']):
                    vec[index[name]] = 1
                rows.append(tuple(vec))
            con.execute('COMMIT')
        except BaseException:
            if con.in_transaction:
                con.execute('ROLLBACK')
            raise
        return generation, rows

    def _vectors(self, request):
        spec = self.spec
        index = {c: i for i, c in enumerate(spec['categories'])}
        n = len(spec['categories'])
        candidates = []
        for q in request['queries']:
            vec = [0] * n
            for name in q['members']:
                vec[index[name]] = 1
            candidates.append((q['id'], tuple(vec), q['utility']))
        protected = [index[c] for c in spec['protected']]
        return n, protected, candidates

    # -- preview --------------------------------------------------------------
    def preview(self, request):
        """Plan against the current history; register nothing.

        Returns selected ids, total utility and the history generation the
        plan is based on.  No counts, no values, no public facts.
        """
        n, protected, candidates = self._vectors(request)
        generation, rows = self._knowledge()
        selected, total = plan(n, rows, protected, candidates)
        return {'request_id': request['request_id'],
                'generation': generation,
                'selected_ids': list(selected),
                'total_utility': total}

    def preview_bytes(self, raw):
        return self.preview(parse_request_bytes(raw, frozenset(self.spec['categories'])))

    # -- release --------------------------------------------------------------
    def release(self, request, pause=None):
        """Commit the optimal safe selection as public equations.

        Returns ``(receipt, receipt_text, idempotent)``.  Replays of the
        canonically identical request return the stored receipt bytes
        without re-planning; the same id with a different request raises
        :class:`ConflictError`.  ``pause`` (test hook) is called with the
        snapshot generation after each knowledge read and before solving;
        no write transaction is held at that point.
        """
        spec = self.spec
        n, protected, candidates = self._vectors(request)
        counts = {row['category']: row['count'] for row in spec['counts']}
        by_id = {q['id']: q for q in request['queries']}
        request_id = request['request_id']
        canonical = _canonical_text(request)
        digest = hashlib.sha256(canonical.encode('utf-8')).hexdigest()
        for _ in range(MAX_ATTEMPTS):
            row = self.connection.execute(
                'SELECT request_sha256, receipt FROM request WHERE request_id = ?',
                (request_id,)).fetchone()
            if row is not None:
                if row['request_sha256'] != digest:
                    raise ConflictError(
                        'request_id %r already registered with a different request'
                        % request_id)
                return json.loads(row['receipt']), row['receipt'], True
            generation, rows = self._knowledge()
            if pause is not None:
                pause(generation)
            selected, total = plan(n, rows, protected, candidates)
            receipt = {'request_id': request_id,
                       'generation': generation,
                       'total_utility': total,
                       'selected': [{'id': qid,
                                     'members': by_id[qid]['members'],
                                     'value': sum(counts[c] for c in by_id[qid]['members'])}
                                    for qid in selected]}
            text = json.dumps(receipt, sort_keys=True, ensure_ascii=False,
                              allow_nan=False, indent=2) + '\n'
            outcome = self._commit(request_id, digest, canonical, generation,
                                   by_id, counts, selected, receipt, text)
            if outcome is not None:
                return outcome
        raise DisclosureError('release could not reach a stable history after %d attempts'
                              % MAX_ATTEMPTS)

    def _commit(self, request_id, digest, canonical, generation,
                by_id, counts, selected, receipt, text):
        """Atomically register the release if the generation is still current.

        Returns ``None`` when the history moved on (caller retries); the
        short ``BEGIN IMMEDIATE`` transaction covers the replay re-check, the
        generation check and every write, so no stale plan can be committed
        and no committed state is half-written.
        """
        con = self.connection
        con.execute('BEGIN IMMEDIATE')
        try:
            row = con.execute(
                'SELECT request_sha256, receipt FROM request WHERE request_id = ?',
                (request_id,)).fetchone()
            if row is not None:
                con.execute('COMMIT')
                if row['request_sha256'] != digest:
                    raise ConflictError(
                        'request_id %r already registered with a different request'
                        % request_id)
                return json.loads(row['receipt']), row['receipt'], True
            if self._generation() != generation:
                con.execute('ROLLBACK')
                return None
            for qid in selected:
                q = by_id[qid]
                con.execute(
                    'INSERT INTO equation(request_id, query_id, members, value)'
                    ' VALUES (?, ?, ?, ?)',
                    (request_id, qid, _canonical_text(q['members']),
                     sum(counts[c] for c in q['members'])))
            if selected:
                con.execute("UPDATE meta SET value = ? WHERE key = 'generation'",
                            (str(generation + 1),))
            con.execute(
                'INSERT INTO request(request_id, request_sha256, canonical,'
                ' generation, receipt) VALUES (?, ?, ?, ?, ?)',
                (request_id, digest, canonical, generation, text))
            con.execute('COMMIT')
            return receipt, text, False
        except BaseException:
            if con.in_transaction:
                con.execute('ROLLBACK')
            raise

    def release_bytes(self, raw, pause=None):
        return self.release(parse_request_bytes(raw, frozenset(self.spec['categories'])),
                            pause=pause)

    # -- receipt ----------------------------------------------------------------
    def receipt(self, request_id):
        """Return the stored receipt bytes for ``request_id`` (read-only)."""
        self.spec  # an uninitialized database holds no receipts
        request_id = _ident(request_id, 'request_id')
        row = self.connection.execute(
            'SELECT receipt FROM request WHERE request_id = ?', (request_id,)).fetchone()
        if row is None:
            raise DisclosureError('no receipt registered for request_id %r' % request_id)
        return row['receipt']


def init_db(path, spec):
    """Create or reopen a workbench database and fix ``spec`` in it.

    Returns ``(store, created)``.  Re-initializing with the canonically
    identical spec is an idempotent no-op; any difference is rejected.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    store = Store(path)
    try:
        created = store.configure(spec)
    except BaseException:
        store.close()
        raise
    return store, created
