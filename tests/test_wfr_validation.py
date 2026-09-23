"""Read-only archived WFR v2 checks and tamper-rejection regression tests."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('wfr_validation', ROOT/'scripts/validate-wfr-run.py')
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)


class WfrValidationTests(unittest.TestCase):
    @unittest.skipUnless(
        (ROOT/'results/raw/c-wfr-v2-secondary-stop-smoke-0921').exists(),
        'archived WFR fixture bundle is not included in the compact submission',
    )
    def test_archived_smokes(self):
        paths = sorted((ROOT/'results/raw').glob('c-wfr-v2-*-smoke-0921'))
        self.assertEqual(len(paths), 4)
        self.assertEqual(sum(validator.validate(p)['trials'] for p in paths), 8)

    def fixture(self, tmp):
        original = ROOT/'results/raw/c-wfr-v2-secondary-stop-smoke-0921'
        target = Path(tmp)/original.name
        shutil.copytree(original, target)
        shutil.copy2(ROOT/'results/fault-control'/original.name/'host-recovery.json', target/'host-recovery.json')
        return target

    @unittest.skipUnless(
        (ROOT/'results/raw/c-wfr-v2-secondary-stop-smoke-0921').exists(),
        'archived WFR fixture bundle is not included in the compact submission',
    )
    def test_changed_raw_data_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = self.fixture(tmp)
            (target/'operations.jsonl').write_text('{}\n')
            with self.assertRaisesRegex(ValueError, 'hash operations.jsonl'):
                validator.validate(target)

    @unittest.skipUnless(
        (ROOT/'results/raw/c-wfr-v2-secondary-stop-smoke-0921').exists(),
        'archived WFR fixture bundle is not included in the compact submission',
    )
    def test_same_clock_order_checked_even_with_matching_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = self.fixture(tmp)
            path = target/'events.jsonl'
            rows = [json.loads(s) for s in path.read_text().splitlines()]
            for row in rows:
                if row['event'] == 'controller_response' and row.get('action') == 'crash':
                    row['monotonic_ns'] = 0
            path.write_text('\n'.join(json.dumps(row) for row in rows)+'\n')
            manifest = json.loads((target/'manifest.json').read_text())
            manifest['artifact_sha256']['events.jsonl'] = hashlib.sha256(path.read_bytes()).hexdigest()
            (target/'manifest.json').write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, 'R fault W ordering'):
                validator.validate(target)
