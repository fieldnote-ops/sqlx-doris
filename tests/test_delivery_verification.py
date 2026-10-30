"""Delivery identity must reject stale sources even when checksums are regenerated."""

import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tarfile
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "delivery", "tools/delivery_verification.py"
)
delivery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(delivery)


def write_tar(path, prefix, files, extra=None):
    with tarfile.open(path, "w:gz") as out:
        for name, data in files.items():
            info = tarfile.TarInfo(prefix + name)
            info.size = len(data)
            out.addfile(info, io.BytesIO(data))
        if extra:
            out.addfile(extra)


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(
            prefix="delivery-policy-", dir=os.environ["SQLX_DORIS_ARTIFACTS_DIR"]
        )
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.project = self.root / "project"
        self.sdk = self.project / "crates/sqlx-doris"
        (self.sdk / "src").mkdir(parents=True)
        (self.sdk / "tests").mkdir()
        for name, data in {
            "src/lib.rs": b"pub fn value() {}",
            "tests/check.rs": b"#[test] fn ok() {}",
            "README.md": b"SDK",
            "README.en.md": b"SDK in English",
            "LICENSE": b"Apache",
            "Cargo.toml": b"[package]",
        }.items():
            (self.sdk / name).write_bytes(data)
        self.files = {
            str(p.relative_to(self.sdk)): p.read_bytes()
            for p in self.sdk.rglob("*")
            if p.is_file() and p.name != "Cargo.toml"
        }
        self.files.update(
            {
                "Cargo.toml": b"normalized",
                "Cargo.toml.orig": b"[package]",
                "Cargo.lock": b"version = 4",
            }
        )
        self.archive = self.root / "sdk.crate"
        self.make_sdk()
        (self.project / "crates/sqlx-mysql/src").mkdir(parents=True)
        (self.project / "crates/sqlx-mysql/src/lib.rs").write_bytes(b"driver")
        (self.project / "patches").mkdir(parents=True)
        for name in [
            "README.md",
            "README.en.md",
            "sqlx-mysql-provenance.json",
            "prepare-ok.patch",
            "native-transactions.patch",
            "safe-bindings.patch",
        ]:
            (self.project / "patches" / name).write_bytes(name.encode())
        self.bundle = self.root / "bundle.tar.gz"
        self.payload = {
            "crates/sqlx-doris/" + str(p.relative_to(self.sdk)): p.read_bytes()
            for p in self.sdk.rglob("*")
            if p.is_file()
            and (
                p.suffix in (".rs", ".py")
                or p.name in ("README.md", "README.en.md", "LICENSE")
            )
        }
        driver = self.project / "crates/sqlx-mysql"
        for p in driver.rglob("*"):
            if p.is_file():
                self.payload["crates/sqlx-mysql/" + str(p.relative_to(driver))] = (
                    p.read_bytes()
                )
        for p in (self.project / "patches").rglob("*"):
            if p.is_file():
                self.payload[
                    "patches/" + str(p.relative_to(self.project / "patches"))
                ] = p.read_bytes()
        self.payload["README.md"] = (
            "查询使用 sqlx_doris::sqlx，驱动由 SDK 的 path 依赖带入。"
        ).encode()
        self.payload["README.en.md"] = (
            b"Use sqlx_doris::sqlx; the SDK path dependency carries the driver."
        )

    def make_sdk(self):
        write_tar(self.archive, "sqlx-doris-0.1.0/", self.files)

    def make_bundle(self):
        payload = dict(self.payload)
        payload["SHA256SUMS.json"] = json.dumps(
            {n: hashlib.sha256(b).hexdigest() for n, b in self.payload.items()}
        ).encode()
        write_tar(self.bundle, "sqlx-doris-compat-0.1.0/", payload)

    def test_exact_source_pair_and_generated_vcs_metadata(self):
        _, result = delivery.verify_sdk(self.project, self.archive, "0.1.0")
        self.assertEqual(result["current_source_test_readme_license_files"], 5)
        self.make_bundle()
        self.assertFalse(
            delivery.verify_bundle(self.project, self.bundle, self.files, "0.1.0")[
                "root_cargo_patch_required"
            ]
        )
        self.files[".cargo_vcs_info.json"] = json.dumps(
            {"git": {"sha1": "a" * 40}, "path_in_vcs": "crates/sqlx-doris"}
        ).encode()
        self.make_sdk()
        self.assertEqual(
            delivery.verify_sdk(self.project, self.archive, "0.1.0")[1]["vcs"]["git"][
                "sha1"
            ],
            "a" * 40,
        )

    def test_sdk_stale_source_missing_test_and_manifest_are_rejected(self):
        for name in ["src/lib.rs", "tests/check.rs", "README.en.md", "Cargo.toml.orig"]:
            original = self.files[name]
            self.files[name] = b"stale"
            self.make_sdk()
            with self.assertRaises(ValueError):
                delivery.verify_sdk(self.project, self.archive, "0.1.0")
            self.files[name] = original
        del self.files["tests/check.rs"]
        self.make_sdk()
        with self.assertRaises(ValueError):
            delivery.verify_sdk(self.project, self.archive, "0.1.0")

    def test_manifest_rehash_cannot_hide_a_different_sdk_or_companion(self):
        for name in [
            "crates/sqlx-doris/src/lib.rs",
            "crates/sqlx-mysql/src/lib.rs",
            "patches/safe-bindings.patch",
        ]:
            original = self.payload[name]
            self.payload[name] = b"modified with a valid new manifest hash"
            self.make_bundle()
            with self.assertRaises(ValueError):
                delivery.verify_bundle(self.project, self.bundle, self.files, "0.1.0")
            self.payload[name] = original

    def test_extra_server_source_or_binary_is_rejected_even_with_valid_hash(self):
        for name in ["server/FePatch.java", "server/doris-fe.jar", "unlisted.txt"]:
            self.payload[name] = b"not part of the SQLx-only delivery"
            self.make_bundle()
            with self.assertRaises(ValueError):
                delivery.verify_bundle(self.project, self.bundle, self.files, "0.1.0")
            del self.payload[name]

    def test_manifest_missing_payload_or_entry_is_rejected(self):
        self.make_bundle()
        files = delivery.archive_files(self.bundle, "sqlx-doris-compat-0.1.0/")
        del files["README.md"]
        write_tar(self.bundle, "sqlx-doris-compat-0.1.0/", files)
        with self.assertRaises(ValueError):
            delivery.verify_bundle(self.project, self.bundle, self.files, "0.1.0")

    def test_links_path_traversal_and_duplicate_members_are_rejected(self):
        for name, kind in [("../escape", tarfile.REGTYPE), ("link", tarfile.SYMTYPE)]:
            extra = tarfile.TarInfo("sqlx-doris-0.1.0/" + name)
            extra.type = kind
            extra.linkname = "/etc/passwd"
            write_tar(self.archive, "sqlx-doris-0.1.0/", self.files, extra)
            with self.assertRaises(ValueError):
                delivery.archive_files(self.archive, "sqlx-doris-0.1.0/")
        duplicate = tarfile.TarInfo("sqlx-doris-0.1.0/src/lib.rs")
        write_tar(self.archive, "sqlx-doris-0.1.0/", self.files, duplicate)
        with self.assertRaises(ValueError):
            delivery.archive_files(self.archive, "sqlx-doris-0.1.0/")
