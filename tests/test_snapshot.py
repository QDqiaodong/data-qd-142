"""Tests for immutable revision chains, sealing and the snapshot CLI."""
import hashlib, json, math, random, sqlite3, subprocess, sys, tempfile, threading, unittest
from unittest import mock
from fractions import Fraction
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
from distribution_drift.model import analyze
from distribution_drift.snapshot import (Store, SnapshotError, ConflictError, SealRejected,
                                         parse_shard, parse_manifest, MAX_CHAINS_PER_DB,
                                         MAX_REVISIONS_PER_CHAIN, MAX_PARTITIONS_PER_SIDE,
                                         MAX_DOMAIN_CATEGORIES)


def shard_bytes(side,partition,revision,parent,counts):
    return (json.dumps({'side':side,'partition':partition,'revision':revision,
                        'parent_sha256':parent,'counts':counts},separators=(',',':'))
            .encode('utf-8'))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def build_chain(side,partition,deltas_by_revision):
    """Return (list_of_raw_shards, list_of_digests) for cumulative deltas."""
    raws=[];digests=[];parent=None
    for revision,counts in enumerate(deltas_by_revision,start=1):
        raw=shard_bytes(side,partition,revision,parent,counts)
        raws.append(raw);digests.append(sha(raw));parent=digests[-1]
    return raws,digests


def manifest_bytes(snapshot_id,categories,alpha,reference,current):
    return json.dumps({'snapshot_id':snapshot_id,'categories':categories,'alpha':alpha,
                       'reference':reference,'current':current},
                      separators=(',',':')).encode('utf-8')


class Ledger(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.db=Path(self.temp.name)/'ledger.db'
        self.store=Store(self.db)

    def tearDown(self):
        self.store.close();self.temp.cleanup()

    def test_replay_is_idempotent_and_conflict_rejected(self):
        raw,_=build_chain('reference','p',[[{'category':'a','delta':4}]])
        digest,first=self.store.ingest_bytes(raw[0])
        self.assertFalse(first)
        again,idempotent=self.store.ingest_bytes(raw[0])
        self.assertTrue(idempotent);self.assertEqual(again,digest)
        # any byte difference on the same version is a conflict
        different=shard_bytes('reference','p',1,None,[{'category':'a','delta':5}])
        with self.assertRaises(ConflictError):
            self.store.ingest_bytes(different)
        # ledger untouched by the rejected request
        row=self.store.connection.execute(
            'SELECT content_sha256 FROM revision').fetchone()
        self.assertEqual(row['content_sha256'],digest)

    def test_identity_is_exact_input_bytes_no_normalisation(self):
        compact=shard_bytes('reference','p',1,None,[{'category':'a','delta':1}])
        pretty=json.dumps(json.loads(compact),indent=2).encode()
        self.assertNotEqual(compact,pretty)
        self.store.ingest_bytes(compact)
        with self.assertRaises(ConflictError):
            self.store.ingest_bytes(pretty)

    def test_same_bytes_under_other_revision_are_distinct_revisions(self):
        raw=shard_bytes('reference','p',1,None,[{'category':'a','delta':1}])
        self.store.ingest_bytes(raw)
        # identical content bytes filed under another revision number are a
        # distinct revision identity; the seal-time parent hash governs chains
        clone=dict(json.loads(raw));clone['revision']=2;clone['parent_sha256']=sha(raw)
        clone_raw=json.dumps(clone,separators=(',',':')).encode()
        digest,_=self.store.ingest_bytes(clone_raw)
        self.assertEqual(digest,sha(clone_raw))
        # but re-submitting revision 1 with the exact original bytes stays idempotent
        again,idempotent=self.store.ingest_bytes(raw)
        self.assertTrue(idempotent);self.assertEqual(again,sha(raw))

    def test_dangling_revisions_are_kept_when_parent_missing(self):
        r1,_=build_chain('reference','p',[[{'category':'a','delta':5}],
                                          [{'category':'a','delta':3}],
                                          [{'category':'a','delta':2}]])
        # ingest rev 3 then rev 2 before rev 1 exists
        self.store.ingest_bytes(r1[2]);self.store.ingest_bytes(r1[1])
        count=self.store.connection.execute('SELECT COUNT(*) AS n FROM revision').fetchone()['n']
        self.assertEqual(count,2)

    def test_parent_hash_is_chain_content_sha(self):
        raws,digests=build_chain('reference','p',[[{'category':'a','delta':5}],
                                                  [{'category':'a','delta':3}]])
        self.store.ingest_bytes(raws[0])
        wrong_parent=shard_bytes('reference','p',2,'0'*64,[{'category':'a','delta':3}])
        # format-legal, stored as a dangling revision; seal later proves the mismatch
        self.store.ingest_bytes(wrong_parent)
        manifest=manifest_bytes('s',['a'],1,[{'partition':'p','revision':2}],
                                [{'partition':'q','revision':1}])
        q,_=build_chain('current','q',[[{'category':'a','delta':1}]])
        self.store.ingest_bytes(q[0])
        with self.assertRaises(SealRejected):
            self.store.seal_manifest_bytes(manifest)

    def test_negative_cumulative_chain_is_still_stored(self):
        raws,_=build_chain('reference','p',[[{'category':'a','delta':5}],
                                            [{'category':'a','delta':-9}]])
        for raw in raws:
            self.store.ingest_bytes(raw)
        rows=self.store.connection.execute('SELECT revision FROM revision ORDER BY revision').fetchall()
        self.assertEqual([r['revision'] for r in rows],[1,2])

    def test_wrong_parent_hash_kept_dangling_stays_unsealable_after_parent_arrives(self):
        r1,_=build_chain('reference','p',[[{'category':'a','delta':5}]])
        wrong=shard_bytes('reference','p',2,'0'*64,[{'category':'a','delta':3}])
        self.store.ingest_bytes(wrong)   # dangling but format-legal, retained
        self.store.ingest_bytes(r1[0])   # real parent arrives later
        c,_=build_chain('current','q',[[{'category':'a','delta':1}]])
        self.store.ingest_bytes(c[0])
        manifest=manifest_bytes('s',['a'],1,[{'partition':'p','revision':2}],
                                [{'partition':'q','revision':1}])
        with self.assertRaises(SealRejected):
            self.store.seal_manifest_bytes(manifest)
        # nothing was sealed
        self.assertEqual(self.store.connection.execute(
            'SELECT COUNT(*) AS n FROM snapshot').fetchone()['n'],0)

    def test_revision_category_cap_enforced(self):
        import distribution_drift.snapshot as mod
        many=[{'category':'c%04d'%i,'delta':1} for i in range(11)]
        with mock.patch.object(mod,'MAX_CATEGORIES_PER_REVISION',10):
            with self.assertRaises(SnapshotError):
                self.store.ingest_bytes(shard_bytes('reference','p',1,None,many))
        self.assertEqual(self.store.connection.execute(
            'SELECT COUNT(*) AS n FROM revision').fetchone()['n'],0)

    def test_strict_validation_rules(self):
        def bad(raw):
            with self.assertRaises(SnapshotError):
                parse_shard(raw)
        bad(b'{"side":"left","partition":"p","revision":1,"parent_sha256":null,"counts":[]}')
        bad(shard_bytes('reference','p',0,None,[]))
        bad(shard_bytes('reference','p',1,'deadbeef',[]))
        bad(shard_bytes('reference','p',1,None,[{'category':'a','delta':True}]))
        bad(shard_bytes('reference','p',1,None,[{'category':'a','delta':0}]))
        bad(shard_bytes('reference','p',1,None,[{'category':'a','delta':10**9+1}]))
        bad(shard_bytes('reference','p',1,None,[{'category':'a','delta':-10**9-1}]))
        bad(shard_bytes('reference','p',1,None,[{'category':'a','delta':1.5}]))
        bad(shard_bytes('reference','p',1,None,[{'category':'','delta':1}]))
        bad(shard_bytes('reference','p',1,None,[{'category':'a','delta':1},
                                                {'category':'a','delta':2}]))
        bad(b'{"side":"reference","partition":"p","revision":1,"parent_sha256":null,"counts":[],"x":1}')
        # duplicate JSON keys and non-finite constants rejected by the strict parser
        bad(b'{"side":"reference","partition":"p","revision":1,"revision":2,"parent_sha256":null,"counts":[]}')
        bad(b'{"side":"reference","partition":"p","revision":NaN,"parent_sha256":null,"counts":[]}')

    def test_oversized_json_rejected_before_db_touch(self):
        padding='x'*8_000_001
        big=json.dumps({'side':'reference','partition':'big','revision':1,
                        'parent_sha256':None,
                        'counts':[{'category':'a','delta':1,'note':padding}]}).encode()
        self.assertGreater(len(big),8_000_000)
        with self.assertRaises(SnapshotError):
            self.store.ingest_bytes(big)
        self.assertEqual(self.store.connection.execute(
            'SELECT COUNT(*) AS n FROM revision').fetchone()['n'],0)

    def test_chain_and_revision_limits_enforced(self):
        import distribution_drift.snapshot as mod
        raws,_=build_chain('reference','p',[[{'category':'a','delta':1}]])
        self.store.ingest_bytes(raws[0])
        second=shard_bytes('reference','p',2,sha(raws[0]),[{'category':'b','delta':1}])
        with self.assertRaises(SnapshotError):
            with mock.patch.object(mod,'MAX_REVISIONS_PER_CHAIN',1):
                self.store.ingest_bytes(second)
        other=shard_bytes('reference','other',1,None,[{'category':'a','delta':1}])
        with self.assertRaises(SnapshotError):
            with mock.patch.object(mod,'MAX_CHAINS_PER_DB',1):
                self.store.ingest_bytes(other)


class Sealing(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.db=Path(self.temp.name)/'ledger.db'
        self.store=Store(self.db)

    def tearDown(self):
        self.store.close();self.temp.cleanup()

    def _seed_simple(self):
        r,_=build_chain('reference','p',[[{'category':'a','delta':8},{'category':'b','delta':2}],
                                         [{'category':'a','delta':-1},{'category':'b','delta':1}]])
        c,_=build_chain('current','p',[[{'category':'a','delta':2},{'category':'b','delta':8}]])
        for raw in r+c:self.store.ingest_bytes(raw)

    def test_gap_blocks_seal_and_late_parent_unlocks_descendants(self):
        r,_=build_chain('reference','p',[[{'category':'a','delta':5}],
                                         [{'category':'a','delta':3}],
                                         [{'category':'a','delta':2}]])
        c,_=build_chain('current','q',[[{'category':'a','delta':9}]])
        # linked acceptance: revisions 3 and 2 first, then the manifest is refused
        self.store.ingest_bytes(r[2]);self.store.ingest_bytes(r[1])
        self.store.ingest_bytes(c[0])
        manifest=manifest_bytes('s',['a'],1,[{'partition':'p','revision':3}],
                                [{'partition':'q','revision':1}])
        with self.assertRaises(SealRejected):
            self.store.seal_manifest_bytes(manifest)
        self.assertIsNone(self.store.connection.execute(
            'SELECT 1 FROM snapshot WHERE snapshot_id=?',('s',)).fetchone())
        # late parent unlocks the whole stored descendant chain at once
        self.store.ingest_bytes(r[0])
        report,text,idempotent=self.store.seal_manifest_bytes(manifest)
        self.assertFalse(idempotent)
        self.assertEqual(report['merged_reference_counts'],{'a':10})
        self.assertEqual([x['revision'] for x in report['reference']['partitions'][0]['chain']],
                         [1,2,3])

    def test_negative_prefix_chain_format_legal_but_unsealable(self):
        r,_=build_chain('reference','p',[[{'category':'a','delta':5}],
                                         [{'category':'a','delta':-9}],
                                         [{'category':'a','delta':6}]])
        c,_=build_chain('current','q',[[{'category':'a','delta':1}]])
        for raw in r+c:self.store.ingest_bytes(raw)
        manifest=manifest_bytes('s',['a'],1,[{'partition':'p','revision':3}],
                                [{'partition':'q','revision':1}])
        with self.assertRaises(SealRejected):
            self.store.seal_manifest_bytes(manifest)
        # chain remains in the ledger even though sealing is refused
        self.assertEqual(self.store.connection.execute(
            'SELECT COUNT(*) AS n FROM revision WHERE side=?',('reference',)).fetchone()['n'],3)

    def test_out_of_domain_delta_rejected_only_at_seal(self):
        r,_=build_chain('reference','p',[[{'category':'z','delta':1}]])
        self.store.ingest_bytes(r[0])
        c,_=build_chain('current','q',[[{'category':'a','delta':1}]])
        self.store.ingest_bytes(c[0])
        manifest=manifest_bytes('s',['a'],1,[{'partition':'p','revision':1}],
                                [{'partition':'q','revision':1}])
        with self.assertRaises(SealRejected):
            self.store.seal_manifest_bytes(manifest)

    def test_zero_total_rejected_and_ceiling_enforced(self):
        r,_=build_chain('reference','p',[[{'category':'a','delta':1}],
                                         [{'category':'a','delta':-1}]])
        c,_=build_chain('current','q',[[{'category':'a','delta':2}]])
        for raw in r+c:self.store.ingest_bytes(raw)
        manifest=manifest_bytes('s',['a'],1,[{'partition':'p','revision':2}],
                                [{'partition':'q','revision':1}])
        with self.assertRaises(SealRejected):
            self.store.seal_manifest_bytes(manifest)
        huge,_=build_chain('reference','h',[[{'category':'a','delta':10**9}]])
        self.store.ingest_bytes(huge[0])
        more=shard_bytes('reference','h',2,sha(huge[0]),[{'category':'a','delta':1}])
        self.store.ingest_bytes(more)
        manifest2=manifest_bytes('s2',['a'],1,[{'partition':'h','revision':2}],
                                 [{'partition':'q','revision':1}])
        with self.assertRaises(SealRejected):
            self.store.seal_manifest_bytes(manifest2)

    def test_snapshot_identity_and_replay_returns_identical_report(self):
        self._seed_simple()
        manifest=manifest_bytes('snap',['a','b','c'],1,[{'partition':'p','revision':2}],
                                [{'partition':'p','revision':1}])
        report,text,_=self.store.seal_manifest_bytes(manifest)
        again,text2,idempotent=self.store.seal_manifest_bytes(manifest)
        self.assertTrue(idempotent);self.assertEqual(text,text2);self.assertEqual(again,report)
        # same id, different manifest bytes -> conflict
        other=manifest_bytes('snap',['a','b','c'],2,[{'partition':'p','revision':2}],
                             [{'partition':'p','revision':1}])
        with self.assertRaises(ConflictError):
            self.store.seal_manifest_bytes(other)

    def test_sealed_report_immune_to_later_descendants(self):
        self._seed_simple()
        manifest=manifest_bytes('snap',['a','b','c'],1,[{'partition':'p','revision':2}],
                                [{'partition':'p','revision':1}])
        _,before,_=self.store.seal_manifest_bytes(manifest)
        r3=shard_bytes('reference','p',3,
                       self.store.connection.execute(
                           "SELECT content_sha256 FROM revision WHERE side='reference' AND revision=2"
                       ).fetchone()['content_sha256'],
                       [{'category':'a','delta':40}])
        self.store.ingest_bytes(r3)
        _,after,idempotent=self.store.seal_manifest_bytes(manifest)
        self.assertTrue(idempotent);self.assertEqual(before,after)

    def test_sparse_storage_no_zero_materialisation(self):
        r,_=build_chain('reference','p',[[{'category':'a','delta':1}]])
        self.store.ingest_bytes(r[0])
        c,_=build_chain('current','q',[[{'category':'b','delta':1}]])
        self.store.ingest_bytes(c[0])
        manifest=manifest_bytes('s',['a','b','c'],1,[{'partition':'p','revision':1}],
                                [{'partition':'q','revision':1}])
        report,_,_=self.store.seal_manifest_bytes(manifest)
        self.assertEqual(set(report['merged_reference_counts']),{'a'})
        self.assertEqual(set(report['merged_current_counts']),{'b'})
        self.assertEqual(report['result']['reference_total'],1)
        # the fully projected per-category table still carries smoothed zero rows
        cats={row['category']:row for row in report['result']['categories']}
        self.assertEqual(cats['c']['reference_count'],0)
        self.assertEqual(cats['c']['current_count'],0)

    def test_manifest_validation(self):
        def bad(raw):
            with self.assertRaises(SnapshotError):
                parse_manifest(raw)
        good=manifest_bytes('s',['a'],1,[{'partition':'p','revision':1}],
                            [{'partition':'q','revision':1}])
        parse_manifest(good)
        bad(manifest_bytes('s',[],1,[{'partition':'p','revision':1}],
                           [{'partition':'q','revision':1}]))
        bad(manifest_bytes('s',['a','a'],1,[{'partition':'p','revision':1}],
                           [{'partition':'q','revision':1}]))
        bad(manifest_bytes('s',['a'],0,[{'partition':'p','revision':1}],
                           [{'partition':'q','revision':1}]))
        bad(manifest_bytes('s',['a'],True,[{'partition':'p','revision':1}],
                           [{'partition':'q','revision':1}]))
        bad(manifest_bytes('s',['a'],1,[],[{'partition':'q','revision':1}]))
        bad(manifest_bytes('s',['a'],1,
                           [{'partition':'p','revision':1},{'partition':'p','revision':2}],
                           [{'partition':'q','revision':1}]))
        bad(b'{"snapshot_id":"s","categories":["a"],"alpha":1,'
            b'"reference":[{"partition":"p","revision":1}],"current":[{"partition":"q","revision":1}],"x":1}')

    def test_manifest_caps_enforced(self):
        import distribution_drift.snapshot as mod
        with mock.patch.object(mod,'MAX_DOMAIN_CATEGORIES',2):
            with self.assertRaises(SnapshotError):
                parse_manifest(manifest_bytes('s',['a','b','c'],1,
                                              [{'partition':'p','revision':1}],
                                              [{'partition':'q','revision':1}]))
        many=[{'partition':'p%d'%i,'revision':1} for i in range(4)]
        with mock.patch.object(mod,'MAX_PARTITIONS_PER_SIDE',3):
            with self.assertRaises(SnapshotError):
                parse_manifest(manifest_bytes('s',['a'],1,many,
                                              [{'partition':'q','revision':1}]))


class Restart(unittest.TestCase):
    def test_reopen_preserves_ledger_and_sealed_report(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'ledger.db'
            r,digests=build_chain('reference','p',[[{'category':'a','delta':4}],
                                                   [{'category':'a','delta':6}]])
            c,_=build_chain('current','q',[[{'category':'a','delta':5}]])
            with Store(db) as store:
                for raw in r+c:store.ingest_bytes(raw)
                manifest=manifest_bytes('s',['a'],1,[{'partition':'p','revision':2}],
                                        [{'partition':'q','revision':1}])
                _,text,_=store.seal_manifest_bytes(manifest)
            # fresh connection: replay returns the exact stored report
            with Store(db) as store:
                report,text2,idempotent=store.seal_manifest_bytes(manifest)
                self.assertTrue(idempotent);self.assertEqual(text,text2)
                # re-ingesting after restart stays idempotent
                _,again=store.ingest_bytes(r[0]);self.assertTrue(again)
                sealed=store.connection.execute(
                    'SELECT side,partition,revision,content_sha256 FROM sealed_revision ORDER BY side,revision'
                ).fetchall()
                self.assertEqual([(x['side'],x['revision']) for x in sealed],
                                 [('current',1),('reference',1),('reference',2)])
                self.assertEqual(sealed[1]['content_sha256'],digests[0])


class Oracle(unittest.TestCase):
    """Independent Fraction-based oracle for totals, PSI, JS and appear/disappear."""

    @staticmethod
    def oracle(reference,current,alpha):
        cats=sorted(set(reference)|set(current))
        nr=sum(reference.values());nc=sum(current.values());k=len(cats)
        psi=js=0.0;appeared=[];disappeared=[]
        for cat in cats:
            p=Fraction(reference.get(cat,0)+alpha,nr+alpha*k)
            q=Fraction(current.get(cat,0)+alpha,nc+alpha*k)
            m=(p+q)/2
            psi+=float(q-p)*math.log(float(q/p))
            js+=(float(p)*math.log(float(p/m))+float(q)*math.log(float(q/m)))/2
            if reference.get(cat,0)==0 and current.get(cat,0)>0:appeared.append(cat)
            if reference.get(cat,0)>0 and current.get(cat,0)==0:disappeared.append(cat)
        return nr,nc,psi,js,appeared,disappeared

    def test_random_multi_partition_chains_match_oracle_and_analyze(self):
        rng=random.Random(20261116)
        for seed in range(25):
            rng.seed(1000+seed)
            with tempfile.TemporaryDirectory() as td:
                store=Store(Path(td)/'l.db')
                domain=['cat%d'%i for i in range(6)]
                expected={'reference':{c:0 for c in domain},'current':{c:0 for c in domain}}
                manifest_refs={'reference':[],'current':[]}
                for side in ('reference','current'):
                    nparts=rng.randint(1,3)
                    for p in range(nparts):
                        partition='%s-%d'%(side,p)
                        nrev=rng.randint(1,4);cum={};raws=[]
                        for revision in range(1,nrev+1):
                            parent=None if revision==1 else sha(raws[-1])
                            counts=[]
                            for cat in domain:
                                if rng.random()<0.45:
                                    # keep every prefix cumulative non-negative
                                    lower=-cum.get(cat,0)
                                    delta=rng.randint(max(lower,-50) if lower<0 else 1,40)
                                    if delta!=0:
                                        counts.append({'category':cat,'delta':delta})
                                        cum[cat]=cum.get(cat,0)+delta
                            raw=shard_bytes(side,partition,revision,parent,counts)
                            raws.append(raw);store.ingest_bytes(raw)
                        for cat,value in cum.items():
                            expected[side][cat]=expected[side][cat]+value
                        manifest_refs[side].append({'partition':partition,'revision':nrev})
                alpha=rng.randint(1,5)
                manifest=manifest_bytes('oracle-%d'%seed,domain,alpha,
                                        manifest_refs['reference'],manifest_refs['current'])
                report,_,_=store.seal_manifest_bytes(manifest)
                nr,nc,psi,js,appeared,disappeared=self.oracle(
                    expected['reference'],expected['current'],alpha)
                result=report['result']
                self.assertEqual(result['reference_total'],nr)
                self.assertEqual(result['current_total'],nc)
                self.assertAlmostEqual(result['psi'],psi,places=12)
                self.assertAlmostEqual(result['jensen_shannon_nats'],js,places=12)
                self.assertEqual(result['appeared'],appeared)
                self.assertEqual(result['disappeared'],disappeared)
                self.assertEqual(report['merged_reference_counts'],
                                 {k:v for k,v in expected['reference'].items() if v})
                # cross-check against the shipped analyzer on the merged snapshot
                merged={'categories':domain,'alpha':alpha,
                        'reference':[{'category':k,'count':v} for k,v in expected['reference'].items() if v],
                        'current':[{'category':k,'count':v} for k,v in expected['current'].items() if v]}
                baseline=analyze(merged)
                self.assertEqual(baseline['psi'],result['psi'])
                self.assertEqual(baseline['jensen_shannon_nats'],result['jensen_shannon_nats'])
                self.assertEqual([(x['category'],x['p'],x['q'],x['delta']) for x in baseline['categories']],
                                 [(x['category'],x['p'],x['q'],x['delta']) for x in result['categories']])
                store.close()


class FixedFixture(unittest.TestCase):
    def test_fixed_manifest_matches_analyze_exactly(self):
        fx=ROOT/'fixtures'/'snapshot'
        with tempfile.TemporaryDirectory() as td:
            with Store(Path(td)/'l.db') as store:
                for path in sorted(fx.glob('*rev*.json')):
                    store.ingest_file(path)
                report,_,_=store.seal_manifest_file(fx/'manifest.json')
        baseline=analyze(json.loads((ROOT/'fixtures'/'input.json').read_text()))
        result=report['result']
        self.assertEqual(result['reference_total'],baseline['reference_total'])
        self.assertEqual(result['current_total'],baseline['current_total'])
        self.assertEqual(result['psi'],baseline['psi'])
        self.assertEqual(result['jensen_shannon_nats'],baseline['jensen_shannon_nats'])
        self.assertEqual(result['categories'],baseline['categories'])
        self.assertEqual(result['appeared'],baseline['appeared'])
        self.assertEqual(result['disappeared'],baseline['disappeared'])
        self.assertEqual(report['manifest_sha256'],sha((fx/'manifest.json').read_bytes()))
        # chain identity section carries every revision's exact byte sha
        ref=report['reference']['partitions'][0]
        self.assertEqual([x['revision'] for x in ref['chain']],[1,2])
        self.assertEqual([x['sha256'] for x in ref['chain']],
                         [sha((fx/'reference_shard-0_rev1.json').read_bytes()),
                          sha((fx/'reference_shard-0_rev2.json').read_bytes())])


class Concurrency(unittest.TestCase):
    def test_interleaved_late_parent_and_seal_share_one_chain_state(self):
        with tempfile.TemporaryDirectory() as td:
            db=Path(td)/'l.db'
            raws,digests=build_chain('reference','p',[[{'category':'a','delta':5}],
                                                      [{'category':'a','delta':3}],
                                                      [{'category':'a','delta':2}]])
            c,_=build_chain('current','q',[[{'category':'a','delta':9}]])
            with Store(db) as setup:
                setup.ingest_bytes(raws[2]);setup.ingest_bytes(raws[1])
                setup.ingest_bytes(c[0])
            manifest=manifest_bytes('conc',['a'],1,[{'partition':'p','revision':3}],
                                    [{'partition':'q','revision':1}])
            errors=[];reports=[];barrier=threading.Barrier(2)

            import time
            def sealer():
                try:
                    with Store(db) as store:
                        barrier.wait()
                        # The parent ingest runs concurrently. Each attempt is a
                        # full serialised transaction, so it either sees the gap
                        # (reject, retry) or the complete valid chain (seal).
                        deadline=time.monotonic()+10
                        while True:
                            try:
                                report,_,_=store.seal_manifest_bytes(manifest)
                                reports.append(report);break
                            except SealRejected:
                                if time.monotonic()>deadline:
                                    raise
                                time.sleep(0.01)
                except Exception as exc:  # pragma: no cover - failure diagnostic
                    errors.append(exc)

            def parent_ingest():
                with Store(db) as store:
                    barrier.wait()
                    store.ingest_bytes(raws[0])

            threads=[threading.Thread(target=sealer),threading.Thread(target=parent_ingest)]
            for t in threads:t.start()
            for t in threads:t.join()
            self.assertEqual(errors,[])
            self.assertEqual(len(reports),1)
            report=reports[0]
            shas=[x['sha256'] for x in report['reference']['partitions'][0]['chain']]
            self.assertEqual(shas,digests)
            self.assertEqual(report['merged_reference_counts'],{'a':10})
            self.assertEqual(report['merged_current_counts'],{'a':9})
            # two more seals from independent connections observe the same immutable set
            for _ in range(2):
                with Store(db) as store:
                    again,text,idempotent=store.seal_manifest_bytes(manifest)
                    self.assertTrue(idempotent)
                    self.assertEqual(
                        [x['sha256'] for x in again['reference']['partitions'][0]['chain']],shas)


class CommandLine(unittest.TestCase):
    def call(self,*args):
        return subprocess.run([sys.executable,'-m','distribution_drift.snapshot_cli',
                               *map(str,args)],capture_output=True,text=True,cwd=ROOT)

    def test_full_cli_workflow_and_failure_preservation(self):
        with tempfile.TemporaryDirectory() as td:
            td=Path(td);db=td/'l.db';out=td/'report.json'
            raws,_=build_chain('reference','p',[[{'category':'a','delta':7}],
                                                [{'category':'a','delta':3}]])
            c,_=build_chain('current','q',[[{'category':'a','delta':4}]])
            sources=[]
            for i,raw in enumerate(raws+c):
                path=td/('shard%d.json'%i);path.write_bytes(raw);sources.append(path)
            manifest=manifest_bytes('cli',['a'],1,[{'partition':'p','revision':2}],
                                    [{'partition':'q','revision':1}])
            mp=td/'manifest.json';mp.write_bytes(manifest)
            r=self.call('init','--db',db);self.assertEqual(r.returncode==0,True,r.stderr)
            self.assertEqual(json.loads(r.stdout)['status'],'initialized')
            r=self.call('ingest','--db',db,'--source',sources[1]);self.assertEqual(r.returncode,0,r.stderr)
            r=self.call('ingest','--db',db,'--source',sources[0]);self.assertEqual(r.returncode,0,r.stderr)
            r=self.call('ingest','--db',db,'--source',sources[2]);self.assertEqual(r.returncode,0,r.stderr)
            # replay ingest reports idempotent
            r=self.call('ingest','--db',db,'--source',sources[0])
            self.assertEqual(json.loads(r.stdout)['status'],'idempotent')
            r=self.call('seal','--db',db,'--manifest',mp,'--output',out)
            self.assertEqual(r.returncode,0,r.stderr)
            first=out.read_bytes()
            r=self.call('seal','--db',db,'--manifest',mp,'--output',out)
            self.assertEqual(r.returncode,0);self.assertEqual(out.read_bytes(),first)
            self.assertEqual(json.loads(r.stdout)['status'],'replayed')
            # invalid shard: exit 2, ledger unchanged
            bad=td/'bad.json';bad.write_bytes(b'{"side":"nope","partition":"p","revision":1,'
                                              b'"parent_sha256":null,"counts":[]}')
            before=self._row_count(db)
            r=self.call('ingest','--db',db,'--source',bad)
            self.assertEqual(r.returncode,2);self.assertEqual(self._row_count(db),before)
            # failed seal leaves no output behind
            gap_manifest=manifest_bytes('cli2',['a'],1,[{'partition':'p','revision':9}],
                                        [{'partition':'q','revision':1}])
            gp=td/'gap.json';gp.write_bytes(gap_manifest);missing=td/'missing.json'
            r=self.call('seal','--db',db,'--manifest',gp,'--output',missing)
            self.assertEqual(r.returncode,2);self.assertFalse(missing.exists())
            # same input/output path rejected
            r=self.call('seal','--db',db,'--manifest',mp,'--output',mp)
            self.assertEqual(r.returncode,2)

    @staticmethod
    def _row_count(db):
        con=sqlite3.connect(db)
        try:
            return con.execute('SELECT COUNT(*) FROM revision').fetchone()[0]
        finally:
            con.close()


if __name__=='__main__':
    unittest.main()
