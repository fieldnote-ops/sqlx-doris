"""Read-only checks for source reconstruction and one exact SDK/companion bundle."""

import hashlib
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile


def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def archive_files(path, prefix):
    """Reject duplicate, linked or escaping members before inspecting payloads."""
    files = {}
    with tarfile.open(path) as archive:
        for member in archive:
            name = PurePosixPath(member.name)
            if (
                name.is_absolute()
                or ".." in name.parts
                or str(name) != member.name.rstrip("/")
            ):
                raise ValueError(f"noncanonical archive member: {member.name}")
            if member.name.rstrip("/") == prefix.rstrip("/") and member.isdir():
                continue
            if not member.name.startswith(prefix):
                raise ValueError(f"unexpected archive root: {member.name}")
            if member.isdir():
                continue
            if not member.isfile() or member.size > 64 * 1024 * 1024:
                raise ValueError(f"unsupported archive member: {member.name}")
            relative = member.name[len(prefix) :]
            if relative in files:
                raise ValueError(f"duplicate archive member: {member.name}")
            files[relative] = archive.extractfile(member).read()
    return files


def reconstruct(project, build_root, output):
    provenance = json.loads(
        (project / "patches/sqlx-mysql-provenance.json").read_text()
    )
    version = provenance["version"]
    matches = list(
        (build_root / "cargo-home/registry/cache").glob(f"*/sqlx-mysql-{version}.crate")
    )
    archive = next(
        (p for p in matches if sha(p) == provenance["registry_checksum"]), None
    )
    if archive is None:
        raise ValueError(
            "exact original SQLx crate missing; run ./scripts/dev cargo fetch --locked --manifest-path tests/mysql-upstream/Cargo.toml"
        )
    original = archive_files(archive, f"sqlx-mysql-{version}/")
    expected = {**provenance["upstream_files"], **provenance["modified_files"]}
    vendor = project / "patches/sqlx-mysql"
    for name, digest in {**expected, **provenance["license_files"]}.items():
        if sha(vendor / name) != digest:
            raise ValueError(f"vendored source/provenance mismatch: {name}")
    patches = ["prepare-ok.patch", "native-transactions.patch", "safe-bindings.patch"]
    with tempfile.TemporaryDirectory(prefix="reconstruct-sqlx-") as directory:
        destination = Path(directory)
        for name, contents in original.items():
            p = destination / name
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(contents)
        with (output / "patch-chain.log").open("w") as log:
            for name in patches:
                result = subprocess.run(
                    [
                        "patch",
                        "--batch",
                        "--fuzz=0",
                        "-p2",
                        "-i",
                        str(project / "patches" / name),
                    ],
                    cwd=destination,
                    capture_output=True,
                    text=True,
                )
                log.write(name + "\n" + result.stdout + result.stderr)
                log.flush()
                if result.returncode:
                    raise ValueError(f"patch chain failed at {name}")
        for name, digest in expected.items():
            p = destination / name
            if not p.is_file() or sha(p) != digest:
                raise ValueError(f"reconstructed source differs: {name}")
        # An unlisted new source cannot hide outside the provenance map.
        if {str(p.relative_to(destination)) for p in destination.rglob("*.rs")} != {
            name for name in expected if name.endswith(".rs")
        }:
            raise ValueError(
                "patch chain source-file set differs from reviewed provenance"
            )
    return dict(
        version=version,
        registry_checksum=sha(archive),
        reconstructed_files=len(expected),
        reviewed_files=len(provenance["modified_files"]),
        license_files=len(provenance["license_files"]),
        patches={name: sha(project / "patches" / name) for name in patches},
    )


def verify_sdk(project, sdk_archive, version):
    source = project / "crates/sqlx-doris"
    files = archive_files(sdk_archive, f"sqlx-doris-{version}/")
    expected = {
        str(p.relative_to(source)): p.read_bytes()
        for p in source.rglob("*")
        if p.is_file()
        and (p.suffix in (".rs", ".py") or p.name in ("README.md", "LICENSE"))
    }
    allowed = set(expected) | {
        "Cargo.toml",
        "Cargo.toml.orig",
        "Cargo.lock",
        ".cargo_vcs_info.json",
    }
    if set(files) - allowed or set(expected) - set(files):
        raise ValueError(
            "SDK archive file set differs from current source and Cargo-generated metadata"
        )
    for name, contents in expected.items():
        if files.get(name) != contents:
            raise ValueError(f"stale SDK source: {name}")
    if files.get("Cargo.toml.orig") != (source / "Cargo.toml").read_bytes():
        raise ValueError("stale SDK Cargo.toml.orig")
    if not all(name in files for name in ("Cargo.toml", "Cargo.lock")):
        raise ValueError("Cargo-generated SDK manifest/lock missing")
    if ".cargo_vcs_info.json" in files:
        vcs = json.loads(files[".cargo_vcs_info.json"])
        digest = vcs.get("git", {}).get("sha1", "")
        if len(digest) != 40 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("invalid Cargo VCS source revision")
        if vcs.get("path_in_vcs") != "crates/sqlx-doris":
            raise ValueError("unexpected SDK path in Cargo VCS metadata")
    return files, dict(
        files=len(files),
        current_source_test_readme_license_files=len(expected),
        sha256=sha(sdk_archive),
        vcs=(
            json.loads(files[".cargo_vcs_info.json"])
            if ".cargo_vcs_info.json" in files
            else None
        ),
    )


def verify_bundle(project, archive, sdk_files, version):
    files = archive_files(archive, f"sqlx-doris-compat-{version}/")
    if "SHA256SUMS.json" not in files:
        raise ValueError("bundle manifest missing")
    manifest = json.loads(files["SHA256SUMS.json"])
    if set(manifest) != set(files) - {"SHA256SUMS.json"}:
        raise ValueError("bundle manifest does not cover every payload exactly")
    for name, digest in manifest.items():
        if hashlib.sha256(files[name]).hexdigest() != digest:
            raise ValueError(f"bundle checksum mismatch: {name}")
    expected = {f"sdk/sqlx-doris-{version}/{n}": b for n, b in sdk_files.items()}
    for path in (project / "patches/sqlx-mysql").rglob("*"):
        if path.is_file():
            expected[
                "patches/sqlx-mysql/"
                + str(path.relative_to(project / "patches/sqlx-mysql"))
            ] = path.read_bytes()
    for name in [
        "README.md",
        "sqlx-mysql-provenance.json",
        "prepare-ok.patch",
        "native-transactions.patch",
        "safe-bindings.patch",
    ]:
        expected["patches/" + name] = (project / "patches" / name).read_bytes()
    if set(files) != set(expected) | {"README.md", "SHA256SUMS.json"}:
        raise ValueError(
            "bundle payload differs from the explicit SDK/SQLx-only delivery set"
        )
    for name, contents in expected.items():
        if files[name] != contents:
            raise ValueError(f"stale bundle source or mismatched SDK archive: {name}")
    readme = files["README.md"].decode()
    if (
        "[patch.crates-io]" not in readme
        or "sqlx-mysql" not in readme
        or "workspace-root" not in readme
    ):
        raise ValueError(
            "required application-root companion installation instructions missing"
        )
    return dict(
        files=len(files),
        manifest_entries=len(manifest),
        current_payload_files_matched=len(expected),
        sha256=sha(archive),
        root_cargo_patch_required=True,
        server_sources_or_binaries_in_delivery=False,
    )
