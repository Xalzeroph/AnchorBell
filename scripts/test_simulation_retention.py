import json
import os
from pathlib import Path
import tempfile
import unittest

from simulation_retention import maintain


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.run = self.root / "simulation-test-run-001"
        self.run.mkdir()
        (self.run / "run-manifest.json").write_text('{"run_id":"test"}')
        self.now = 2_000_000_000

    def archive(self, number, age=172800, name="shared-market", verified=True):
        path = self.run / f"{name}.jsonl.segment-{number:06}.zst"
        path.write_bytes(b"x" * 100)
        os.utime(path, (self.now - age, self.now - age))
        if verified:
            Path(str(path) + ".meta.json").write_text(json.dumps({
                "schema_version": 1, "compression": "zstd",
                "compressed_bytes": 100, "sha256": "a" * 64,
            }))
        return path

    def clean(self, **kwargs):
        return maintain(self.root, max_bytes=100, free_floor=0,
                        now=self.now, **kwargs)

    def test_budget_removes_oldest_sealed_archive_and_preserves_results(self):
        oldest = self.archive(0, age=200000)
        latest = self.archive(1)
        records = self.archive(2, name="records")
        summary = self.run / "metrics.json"
        summary.write_text('{"fill_count":3}')
        result = self.clean(apply=True)
        self.assertFalse(oldest.exists())
        self.assertTrue(latest.exists())
        self.assertTrue(records.exists())
        self.assertEqual(json.loads(summary.read_text())["fill_count"], 3)
        self.assertEqual(len(result["removed"]), 1)
        self.assertTrue(Path(str(oldest) + ".meta.json").exists())
        self.assertIn("deleted", (self.root / "retention-audit.jsonl").read_text())
        self.assertTrue((self.run / "retention-status.json").exists())

    def test_dry_run_does_not_delete_or_write_audit(self):
        paths = [self.archive(0), self.archive(1)]
        result = self.clean()
        self.assertEqual(len(result["planned"]), 1)
        self.assertTrue(all(p.exists() for p in paths))
        self.assertFalse((self.root / "retention-audit.jsonl").exists())

    def test_active_and_unverified_files_are_never_deleted(self):
        recent = self.archive(0, age=3600)
        unverified = self.archive(1, verified=False)
        active = self.run / "shared-market.jsonl"
        active.write_bytes(b"x" * 1000)
        result = self.clean(apply=True)
        self.assertTrue(all(p.exists() for p in [recent, unverified, active]))
        self.assertEqual(result["removed"], [])

    def test_max_age_expires_evidence_even_below_budget(self):
        old = self.archive(0, age=8 * 86400, name="evidence-opportunities")
        self.clean(apply=True)
        self.assertFalse(old.exists())

    def test_invalid_metadata_and_nested_files_are_preserved(self):
        bad = self.archive(0)
        Path(str(bad) + ".meta.json").write_text('{"compressed_bytes":999}')
        nested = self.run / "CORE_V1"
        nested.mkdir()
        protected = nested / "shared-market.jsonl.segment-000001.zst"
        protected.write_bytes(b"x" * 1000)
        self.clean(apply=True)
        self.assertTrue(bad.exists())
        self.assertTrue(protected.exists())

    def test_disk_pressure_never_overrides_minimum_age_or_protected_records(self):
        old = self.archive(0)
        young = self.archive(1, age=3600)
        records = self.archive(2, name="records")
        result = maintain(self.root, apply=True, max_bytes=10_000,
                          free_floor=2**63, now=self.now)
        self.assertFalse(old.exists())
        self.assertTrue(young.exists())
        self.assertTrue(records.exists())
        self.assertEqual(result["status"], "pressure")

    def test_symlinked_run_is_never_followed(self):
        with tempfile.TemporaryDirectory() as outside:
            external = Path(outside)
            (external / "run-manifest.json").write_text('{}')
            target = external / "shared-market.jsonl.segment-000000.zst"
            target.write_bytes(b"x" * 1000)
            link = self.root / "simulation-external-run-002"
            try:
                link.symlink_to(external, target_is_directory=True)
            except OSError:
                self.skipTest("symlink creation is unavailable")
            result = self.clean(apply=True)
            self.assertTrue(target.exists())
            self.assertEqual(result["archive_bytes_before"], 0)


if __name__ == "__main__":
    unittest.main()
