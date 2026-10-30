"""Check the storage constraint before downloading or compiling dependencies."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


DEV = Path(__file__).resolve().parents[1] / "scripts" / "dev"


class ExternalBuildTests(unittest.TestCase):
    def invoke(self, *args, volume=None, build_root=None, unset_volume=False):
        env = os.environ.copy()
        if unset_volume:
            env.pop("SQLX_DORIS_EXTERNAL_VOLUME", None)
        elif volume is not None:
            env["SQLX_DORIS_EXTERNAL_VOLUME"] = str(volume)
        if build_root is not None:
            env["SQLX_DORIS_BUILD_ROOT"] = str(build_root)
        return subprocess.run(
            [sys.executable, str(DEV), *args], env=env, capture_output=True, text=True
        )

    def test_all_directories_are_on_external_volume(self):
        result = self.invoke("--print-env")
        self.assertEqual(result.returncode, 0, result.stderr)
        paths = json.loads(result.stdout)
        volume = Path(os.environ["SQLX_DORIS_EXTERNAL_VOLUME"]).resolve()
        for value in paths.values():
            path = Path(value).resolve()
            self.assertTrue(path.is_relative_to(volume), path)
            self.assertNotEqual(path.stat().st_dev, Path("/").stat().st_dev)

    def test_child_receives_external_temp_and_cargo_paths(self):
        result = self.invoke(
            sys.executable,
            "-B",
            "-c",
            "import os,tempfile,json; print(json.dumps([tempfile.gettempdir(),os.environ['CARGO_HOME'],os.environ['CARGO_TARGET_DIR']]))",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        root = Path(os.environ["SQLX_DORIS_BUILD_ROOT"])
        self.assertEqual(
            json.loads(result.stdout),
            [
                str(root / "tmp-clean"),
                str(root / "cargo-home"),
                str(root / "target-clean"),
            ],
        )

    def test_volume_must_be_explicit_even_when_build_root_is_inherited(self):
        result = self.invoke("--print-env", unset_volume=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("Set SQLX_DORIS_EXTERNAL_VOLUME", result.stderr)

    def test_root_setting_alone_cannot_bypass_a_script_storage_check(self):
        env = os.environ.copy()
        env.pop("SQLX_DORIS_EXTERNAL_VOLUME", None)
        env.pop("SQLX_DORIS_DEV_ACTIVE", None)
        result = subprocess.run(
            [sys.executable, str(DEV.parent / "verify-delivery"), "--help"],
            env=env,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("Set SQLX_DORIS_EXTERNAL_VOLUME", result.stderr)

    def test_build_root_is_configurable_and_can_contain_spaces(self):
        with tempfile.TemporaryDirectory() as parent:
            build_root = Path(parent) / "custom build root"
            result = self.invoke("--print-env", build_root=build_root)
            self.assertEqual(result.returncode, 0, result.stderr)
            paths = json.loads(result.stdout)
            self.assertEqual(Path(paths["SQLX_DORIS_BUILD_ROOT"]), build_root)
            for value in paths.values():
                self.assertTrue(Path(value).resolve().is_relative_to(build_root))

    def test_symlinked_build_root_cannot_escape_the_volume(self):
        with tempfile.TemporaryDirectory() as parent:
            build_root = Path(parent) / "escape"
            build_root.symlink_to("/", target_is_directory=True)
            result = self.invoke("--print-env", build_root=build_root)
            self.assertEqual(result.returncode, 2)
            self.assertIn("escapes external volume", result.stderr)

    def test_missing_volume_is_rejected_without_creation(self):
        with tempfile.TemporaryDirectory() as parent:
            missing = Path(parent) / "unmounted"
            result = self.invoke("--print-env", volume=missing)
            self.assertEqual(result.returncode, 2)
            self.assertFalse(missing.exists())

    def test_ordinary_directory_is_rejected(self):
        with tempfile.TemporaryDirectory() as parent:
            result = self.invoke("--print-env", volume=parent)
            self.assertEqual(result.returncode, 2)
            self.assertFalse((Path(parent) / "sqlx-doris").exists())

    def test_internal_root_is_rejected(self):
        self.assertEqual(self.invoke("--print-env", volume="/").returncode, 2)

    def test_child_exit_status_is_preserved(self):
        self.assertEqual(
            self.invoke(sys.executable, "-B", "-c", "raise SystemExit(7)").returncode, 7
        )


if __name__ == "__main__":
    unittest.main()
