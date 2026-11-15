
import copy,json,unittest,tempfile,subprocess,sys,hashlib,random,itertools
from pathlib import Path
from fractions import Fraction
from distribution_drift.model import analyze
ROOT=Path(__file__).resolve().parents[1]
def sample():return json.loads((ROOT/'fixtures/input.json').read_text())
class Business(unittest.TestCase):
    def bad(self,d):
        with self.assertRaises((ValueError,TypeError)):analyze(d)
    def test_unknown_fields(self):
        d=sample();d['unknown']=1;self.bad(d)

    def test_exact_probabilities(self):
        r=analyze(sample());self.assertEqual((r['categories'][0]['p'],r['categories'][0]['q']),('9/13','3/13'));self.assertAlmostEqual(r['psi'],12/13*__import__('math').log(3));self.assertGreater(r['jensen_shannon_nats'],0)
    def test_identity(self):
        d=sample();d['current']=copy.deepcopy(d['reference']);r=analyze(d);self.assertEqual(r['psi'],0);self.assertEqual(r['jensen_shannon_nats'],0)
    def test_symmetry(self):
        d=sample();d['reference'],d['current']=d['current'],d['reference'];r=analyze(d);base=analyze(sample());self.assertAlmostEqual(r['psi'],base['psi']);self.assertAlmostEqual(r['jensen_shannon_nats'],base['jensen_shannon_nats'])
    def test_appear_disappear(self):
        d=sample();d['reference']=[{'category':'a','count':1}];d['current']=[{'category':'c','count':1}];r=analyze(d);self.assertEqual(r['appeared'],['c']);self.assertEqual(r['disappeared'],['a'])
    def test_zero_total(self):
        d=sample();d['current']=[];self.bad(d)
    def test_duplicate(self):
        d=sample();d['reference'].append(d['reference'][0]);self.bad(d)
    def test_unknown_category(self):
        d=sample();d['current'][0]['category']='x';self.bad(d)
    def test_order(self):
        d=sample();d['categories'].reverse();d['reference'].reverse();self.assertEqual(analyze(d),analyze(sample()))

class Command(unittest.TestCase):
    def call(self,source,output):
        return subprocess.run([sys.executable,'-m','distribution_drift.cli','--source',str(source),'--output',str(output)],capture_output=True,text=True)
    def test_cli_replay(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'out.json';src=ROOT/'fixtures/input.json';r=self.call(src,out);self.assertEqual(r.returncode,0,r.stderr);first=out.read_bytes();self.assertEqual(json.loads(first)['source_sha256'],hashlib.sha256(src.read_bytes()).hexdigest());self.assertEqual(self.call(src,out).returncode,0);self.assertEqual(first,out.read_bytes())
    def test_failure_preserves(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'bad.json';out=Path(td)/'out.json';out.write_text('prior')
            for body in ['{}','{"a":1,"a":2}','NaN']:
                src.write_text(body);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(out.read_text(),'prior')
    def test_same_path_and_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            src=Path(td)/'in.json';src.write_bytes((ROOT/'fixtures/input.json').read_bytes());before=src.read_bytes();self.assertEqual(self.call(src,src).returncode,2);self.assertEqual(src.read_bytes(),before);out=Path(td)/'link.json';out.symlink_to(src);self.assertEqual(self.call(src,out).returncode,2);self.assertEqual(src.read_bytes(),before)
