import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from barrier_checkpoint.cli import main
from barrier_checkpoint.demo import scenario


class CLITests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.manifest = self.root / "source.json"
        self.manifest.write_text(json.dumps(scenario().document()), encoding="utf-8")

    def invoke(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = main(list(map(str, args)))
        return code, out.getvalue(), err.getvalue()

    def test_run_export_and_restore_in_new_process(self):
        for mode in ("aligned", "recording"):
            path = self.root / f"{mode}.json"
            code, out, err = self.invoke("run", self.manifest, "--mode", mode, "--checkpoint", path)
            self.assertEqual(code, 0, err)
            self.assertEqual(json.loads(out)["checkpoint"], json.loads(path.read_text()))
            child = subprocess.run([sys.executable, "-m", "barrier_checkpoint", "restore",
                                    str(self.manifest), str(path)], capture_output=True, text=True, timeout=10)
            self.assertEqual(child.returncode, 0, child.stderr)
            self.assertEqual(json.loads(child.stdout)["state"], {"requests": {"count": 20, "sum": 158}})

    def test_export_never_overwrites_existing_file(self):
        path = self.root / "existing.json"
        path.write_text("keep me", encoding="utf-8")
        self.assertEqual(self.invoke("run", self.manifest, "--checkpoint", path)[0], 2)
        self.assertEqual(path.read_text(), "keep me")

    def test_incomplete_checkpoint_cannot_be_exported(self):
        raw = scenario().document()
        raw["max_ticks"] = 1
        self.manifest.write_text(json.dumps(raw), encoding="utf-8")
        path = self.root / "incomplete.json"
        self.assertEqual(self.invoke("run", self.manifest)[0], 1)
        self.assertEqual(self.invoke("run", self.manifest, "--checkpoint", path)[0], 2)
        self.assertFalse(path.exists())

    def test_aborted_checkpoint_is_nonzero(self):
        raw = scenario().document()
        raw["capture_limit"] = 0
        self.manifest.write_text(json.dumps(raw), encoding="utf-8")
        code, out, _ = self.invoke("run", self.manifest, "--mode", "recording")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["checkpoint_status"], "aborted")
        self.assertEqual(self.invoke("compare", self.manifest)[0], 1)

    def test_bounded_and_strict_json(self):
        for content in [b'{"capacity":1,"capacity":2}', b'{"value":NaN}', b"\xff", b"[" * 2000,
                        b" " * 1048577, b"{}"]:
            self.manifest.write_bytes(content)
            code, out, err = self.invoke("run", self.manifest)
            self.assertEqual(code, 2)
            self.assertEqual(out, "")
            self.assertIn("error", json.loads(err))

    def test_missing_input(self):
        self.assertEqual(self.invoke("compare", self.root / "missing.json")[0], 2)

    def test_truncated_checkpoint_rejected(self):
        path = self.root / "truncated.json"
        path.write_text('{"format_version":', encoding="utf-8")
        self.assertEqual(self.invoke("restore", self.manifest, path)[0], 2)

    def test_demo_and_manifest_agree(self):
        demo = self.invoke("demo")
        comparison = self.invoke("compare", self.manifest)
        self.assertEqual(demo[0], 0)
        self.assertEqual(demo, comparison)
