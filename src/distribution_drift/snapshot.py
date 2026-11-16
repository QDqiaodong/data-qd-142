"""Immutable revision chains and sealed snapshot reports backed by SQLite.

Design (see docs/snapshot.md for the full discussion):

* Ingest only validates format/content identity and appends immutable rows.
  A revision whose parent has not arrived yet is still kept, so late parents
  can later unlock already stored descendants. No chain qualification or
  cumulative state is materialised at ingest time.
* Seal traces the referenced parent prefixes on demand inside one
  ``BEGIN IMMEDIATE`` transaction. Revision bodies are streamed one row at a
  time and counts are kept as sparse ``dict``; zero rows are never
  materialised and never all revision bodies are loaded at once.
* Every read the seal decision depends on (revision chain, manifest identity,
  previously sealed snapshots) and every write the seal produces happen in the
  same transaction, hence chain validity, snapshot identity and the merged
  result share one consistency boundary.
"""
from contextlib import contextmanager
import hashlib, json, sqlite3
from .common import ident, parse_strict, read_bytes, save_bytes
from .divergence import compare

SIDES=('reference','current')
MAX_CHAINS_PER_DB=100_000
MAX_REVISIONS_PER_CHAIN=1_000
MAX_CATEGORIES_PER_REVISION=10_000
MAX_DOMAIN_CATEGORIES=1_000
MAX_PARTITIONS_PER_SIDE=1_000
MAX_ALPHA=1_000_000
MAX_COUNT=1_000_000_000
MAX_DELTA=1_000_000_000
MIN_DELTA=-1_000_000_000
HEX64=frozenset('0123456789abcdef')


class SnapshotError(ValueError):
    """Base class for rejected snapshot ledger or seal requests."""


class ConflictError(SnapshotError):
    """Same (side, partition, revision) already stored with different bytes."""


class SealRejected(SnapshotError):
    """The manifest cannot be sealed against the current ledger state."""


class _Shard:
    """A validated ingest shard plus its original raw bytes and digest."""
    __slots__=('side','partition','revision','parent_sha256','counts','raw','sha256')

    def __init__(self,values,raw,sha256):
        self.side=values['side'];self.partition=values['partition']
        self.revision=values['revision'];self.parent_sha256=values['parent_sha256']
        self.counts=values['counts'];self.raw=raw;self.sha256=sha256


def _strict_int(value,field):
    # bool is a subclass of int; the contract forbids booleans explicitly.
    if type(value) is not int:
        raise SnapshotError(field+' must be a strict integer (booleans rejected)')
    return value


def _is_hex64(value):
    return isinstance(value,str) and len(value)==64 and all(ch in HEX64 for ch in value)


def _ident(value,field):
    try:
        return ident(value)
    except ValueError as exc:
        raise SnapshotError(field+': '+str(exc))


def parse_shard(raw):
    """Validate one shard's bytes and return a :class:`_Shard`.

    The digest is over the exact input bytes; no JSON re-ordering or
    whitespace normalisation is ever performed.
    """
    try:
        data=parse_strict(raw)
    except (ValueError,TypeError) as exc:
        raise SnapshotError(str(exc))
    if not isinstance(data,dict) or set(data)!={'side','partition','revision','parent_sha256','counts'}:
        raise SnapshotError('shard must contain exactly side, partition, revision, parent_sha256, counts')
    side=data['side']
    if side not in SIDES:
        raise SnapshotError('side must be reference or current')
    partition=_ident(data['partition'],'partition')
    revision=_strict_int(data['revision'],'revision')
    if revision<1:
        raise SnapshotError('revision starts at 1')
    parent=data['parent_sha256']
    if revision==1:
        if parent is not None:
            raise SnapshotError('first revision parent_sha256 must be null')
    elif not _is_hex64(parent):
        raise SnapshotError('parent_sha256 must be null for revision 1 or a 64-char lowercase hex sha256')
    counts=data['counts']
    if not isinstance(counts,list) or len(counts)>MAX_CATEGORIES_PER_REVISION:
        raise SnapshotError('counts must be an array of at most %d entries'%MAX_CATEGORIES_PER_REVISION)
    checked=[];seen=set()
    for row in counts:
        if not isinstance(row,dict) or set(row)!={'category','delta'}:
            raise SnapshotError('each count row must contain exactly category and delta')
        category=_ident(row['category'],'category')
        if category in seen:
            raise SnapshotError('duplicate category inside revision: '+category)
        seen.add(category)
        delta=_strict_int(row['delta'],'delta')
        if not MIN_DELTA<=delta<=MAX_DELTA or delta==0:
            raise SnapshotError('delta absolute value must be within 1..%d'%MAX_DELTA)
        checked.append((category,delta))
    values=dict(data);values['counts']=checked
    return _Shard(values,bytes(raw),hashlib.sha256(bytes(raw)).hexdigest())


def parse_manifest(raw):
    """Validate manifest bytes; return ``(manifest, manifest_sha256, domain)``."""
    try:
        data=parse_strict(raw)
    except (ValueError,TypeError) as exc:
        raise SnapshotError(str(exc))
    if not isinstance(data,dict) or set(data)!={'snapshot_id','categories','alpha','reference','current'}:
        raise SnapshotError('manifest must contain exactly snapshot_id, categories, alpha, reference, current')
    snapshot_id=_ident(data['snapshot_id'],'snapshot_id')
    categories=data['categories']
    if not isinstance(categories,list) or not categories or len(categories)>MAX_DOMAIN_CATEGORIES:
        raise SnapshotError('categories must be a nonempty array of at most %d'%MAX_DOMAIN_CATEGORIES)
    domain=[];seen=set()
    for category in categories:
        name=_ident(category,'category')
        if name in seen:
            raise SnapshotError('duplicate manifest category: '+name)
        seen.add(name);domain.append(name)
    alpha=_strict_int(data['alpha'],'alpha')
    if not 1<=alpha<=MAX_ALPHA:
        raise SnapshotError('alpha must be within 1..%d'%MAX_ALPHA)
    for side in SIDES:
        entries=data[side]
        if not isinstance(entries,list) or not entries or len(entries)>MAX_PARTITIONS_PER_SIDE:
            raise SnapshotError('%s must reference 1..%d unique partitions'%(side,MAX_PARTITIONS_PER_SIDE))
        side_seen=set()
        for entry in entries:
            if not isinstance(entry,dict) or set(entry)!={'partition','revision'}:
                raise SnapshotError(side+' entries must contain exactly partition and revision')
            partition=_ident(entry['partition'],'partition')
            if partition in side_seen:
                raise SnapshotError('duplicate %s partition in manifest: %s'%(side,partition))
            side_seen.add(partition)
            revision=_strict_int(entry['revision'],'revision')
            if revision<1 or revision>MAX_REVISIONS_PER_CHAIN:
                raise SnapshotError('%s revision outside 1..%d'%(side,MAX_REVISIONS_PER_CHAIN))
    return data,hashlib.sha256(bytes(raw)).hexdigest(),domain


SCHEMA="""
CREATE TABLE IF NOT EXISTS chain(
  side TEXT NOT NULL, partition TEXT NOT NULL,
  PRIMARY KEY(side,partition)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS revision(
  side TEXT NOT NULL, partition TEXT NOT NULL, revision INTEGER NOT NULL,
  parent_sha256 TEXT, content_sha256 TEXT NOT NULL, raw BLOB NOT NULL,
  PRIMARY KEY(side,partition,revision)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS snapshot(
  snapshot_id TEXT PRIMARY KEY, manifest_sha256 TEXT NOT NULL,
  manifest_bytes BLOB NOT NULL, report TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS sealed_revision(
  snapshot_id TEXT NOT NULL, side TEXT NOT NULL, partition TEXT NOT NULL,
  revision INTEGER NOT NULL, content_sha256 TEXT NOT NULL,
  PRIMARY KEY(snapshot_id,side,partition,revision)) WITHOUT ROWID;
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


class Store:
    """SQLite-backed ledger of immutable revision chains and snapshots."""

    def __init__(self,path):
        self.path=str(path)
        self.connection=sqlite3.connect(self.path,timeout=30.0)
        # autocommit mode: we manage BEGIN/COMMIT boundaries explicitly.
        self.connection.isolation_level=None
        self.connection.execute('PRAGMA busy_timeout=30000')
        self.connection.execute('PRAGMA foreign_keys=ON')
        self.connection.row_factory=sqlite3.Row
        self.connection.executescript(SCHEMA)

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self,*exc):
        self.close()

    # -- ingest -------------------------------------------------------------
    def ingest_bytes(self,raw):
        """Validate and append one shard's bytes.

        Returns ``(content_sha256, idempotent)``. The whole check-and-insert
        runs in one immediate transaction; any rejection rolls back and the
        ledger is untouched.
        """
        shard=parse_shard(raw)  # format errors raise before any DB access
        con=self.connection
        with _write_transaction(con):
            row=con.execute(
                'SELECT content_sha256 FROM revision WHERE side=? AND partition=? AND revision=?',
                (shard.side,shard.partition,shard.revision)).fetchone()
            if row is not None:
                if row['content_sha256']==shard.sha256:
                    return shard.sha256,True  # identical bytes: idempotent
                raise ConflictError(
                    'revision %d of %s/%s already exists with different bytes'
                    %(shard.revision,shard.side,shard.partition))
            chain=con.execute(
                'SELECT 1 FROM chain WHERE side=? AND partition=?',
                (shard.side,shard.partition)).fetchone()
            if chain is None:
                count=con.execute('SELECT COUNT(*) AS n FROM chain').fetchone()['n']
                if count>=MAX_CHAINS_PER_DB:
                    raise SnapshotError('database already holds %d (side,partition) chains'%MAX_CHAINS_PER_DB)
                con.execute('INSERT INTO chain(side,partition) VALUES(?,?)',
                            (shard.side,shard.partition))
            stored_count=con.execute(
                'SELECT COUNT(*) AS n FROM revision WHERE side=? AND partition=?',
                (shard.side,shard.partition)).fetchone()['n']
            if stored_count>=MAX_REVISIONS_PER_CHAIN:
                raise SnapshotError('a chain holds at most %d revisions'%MAX_REVISIONS_PER_CHAIN)
            con.execute(
                'INSERT INTO revision(side,partition,revision,parent_sha256,content_sha256,raw)'
                ' VALUES(?,?,?,?,?,?)',
                (shard.side,shard.partition,shard.revision,shard.parent_sha256,
                 shard.sha256,shard.raw))
        return shard.sha256,False

    def ingest_file(self,path):
        return self.ingest_bytes(read_bytes(path))

    # -- seal ---------------------------------------------------------------
    def _trace_prefix(self,side,partition,tip,domain):
        """Stream revisions 1..tip for one chain inside the seal transaction.

        Returns ``(chain_rows, merged)`` where ``merged`` is a sparse dict of
        final per-category counts. Raises :class:`SealRejected` when the
        parent chain has a gap, a parent hash mismatches, a delta leaves the
        declared domain, any prefix cumulative count goes negative, or a
        cumulative count exceeds the contract ceiling.
        """
        con=self.connection
        cursor=con.execute(
            'SELECT revision,parent_sha256,content_sha256,raw FROM revision'
            ' WHERE side=? AND partition=? AND revision<=? ORDER BY revision',
            (side,partition,tip))
        seen=0;merged={};chain_rows=[];expected_parent=None
        for row in cursor:  # one BLOB resident at a time; never fetchall()
            seen+=1
            if row['parent_sha256']!=expected_parent:
                raise SealRejected(
                    'chain %s/%s revision %d parent_sha256 does not match the previous revision bytes'
                    %(side,partition,row['revision']))
            digest=hashlib.sha256(row['raw']).hexdigest()
            if digest!=row['content_sha256']:
                raise SealRejected('stored content hash mismatch for %s/%s revision %d'
                                   %(side,partition,row['revision']))
            shard=parse_shard(row['raw'])  # stored rows always parse; re-validated
            for category,delta in shard.counts:
                if category not in domain:
                    raise SealRejected(
                        'delta category %r of %s/%s revision %d is outside the manifest domain'
                        %(category,side,partition,row['revision']))
                value=merged.get(category,0)+delta
                if value<0:
                    raise SealRejected(
                        'negative cumulative count for %r at %s/%s revision %d'
                        %(category,side,partition,row['revision']))
                if value>MAX_COUNT:
                    raise SealRejected(
                        'cumulative count for %r exceeds %d at %s/%s revision %d'
                        %(category,MAX_COUNT,side,partition,row['revision']))
                merged[category]=value
            chain_rows.append({'revision':row['revision'],'sha256':digest})
            expected_parent=digest
        if seen!=tip:
            raise SealRejected(
                'chain %s/%s has a gap: manifest references revision %d but %d revision(s) are stored'
                %(side,partition,tip,seen))
        return chain_rows,merged

    def seal_manifest_bytes(self,raw):
        """Seal a manifest; returns ``(report, report_text, idempotent)``.

        All validation, the replay check, the merge and the report insert are
        one serialised immediate transaction, so a concurrent late-parent
        ingest can never be half-observed.
        """
        manifest,manifest_sha,domain=parse_manifest(raw)  # format errors first
        con=self.connection
        with _write_transaction(con):
            stored=con.execute(
                'SELECT manifest_sha256,report FROM snapshot WHERE snapshot_id=?',
                (manifest['snapshot_id'],)).fetchone()
            if stored is not None:
                if stored['manifest_sha256']!=manifest_sha:
                    raise ConflictError(
                        'snapshot_id %r already sealed with different manifest bytes'%manifest['snapshot_id'])
                return json.loads(stored['report']),stored['report'],True

            totals={};chains_out={};side_maps={};sealed_rows=[]
            for side in SIDES:
                side_merged={};side_chains=[]
                for entry in manifest[side]:
                    partition=entry['partition'];tip=entry['revision']
                    chain_rows,merged=self._trace_prefix(side,partition,tip,domain)
                    side_chains.append({'partition':partition,'tip_revision':tip,
                                        'chain':chain_rows})
                    sealed_rows.extend((manifest['snapshot_id'],side,partition,
                                        r['revision'],r['sha256']) for r in chain_rows)
                    for category,value in merged.items():
                        side_merged[category]=side_merged.get(category,0)+value
                        if side_merged[category]>MAX_COUNT:
                            raise SealRejected(
                                'merged %s count for %r exceeds %d'%(side,category,MAX_COUNT))
                side_total=sum(side_merged.values())
                if side_total<=0:
                    raise SealRejected('%s total must be greater than zero'%side)
                totals[side]=side_total
                chains_out[side]=side_chains
                side_maps[side]=dict(sorted(side_merged.items()))
            result=compare(domain,
                           {c:side_maps['reference'].get(c,0) for c in domain},
                           {c:side_maps['current'].get(c,0) for c in domain},
                           manifest['alpha'])
            report={
                'schema_version':1,
                'snapshot_id':manifest['snapshot_id'],
                'manifest_sha256':manifest_sha,
                'alpha':manifest['alpha'],
                'categories':domain,
                'reference':{'total':totals['reference'],'partitions':chains_out['reference']},
                'current':{'total':totals['current'],'partitions':chains_out['current']},
                'merged_reference_counts':side_maps['reference'],
                'merged_current_counts':side_maps['current'],
                'result':result,
            }
            text=json.dumps(report,sort_keys=True,ensure_ascii=False,allow_nan=False,indent=2)+'\n'
            con.execute(
                'INSERT INTO snapshot(snapshot_id,manifest_sha256,manifest_bytes,report)'
                ' VALUES(?,?,?,?)',
                (manifest['snapshot_id'],manifest_sha,bytes(raw),text))
            con.executemany(
                'INSERT INTO sealed_revision(snapshot_id,side,partition,revision,content_sha256)'
                ' VALUES(?,?,?,?,?)',sealed_rows)
        return report,text,False

    def seal_manifest_file(self,manifest_path,output_path=None):
        raw=read_bytes(manifest_path)
        report,text,idempotent=self.seal_manifest_bytes(raw)
        if output_path is not None:
            save_bytes(output_path,text.encode('utf-8'))
        return report,text,idempotent


def init_db(path):
    """Create a ledger database if absent; opening an existing one is a no-op."""
    return Store(path)
