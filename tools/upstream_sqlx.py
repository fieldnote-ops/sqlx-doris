"""Verify the upstream registry graph and its archived SQLx sources."""

import hashlib
from pathlib import Path
import tarfile
import tomllib

SQLX_PACKAGES = {"sqlx", "sqlx-core", "sqlx-mysql", "sqlx-macros", "sqlx-macros-core"}


def verify(metadata, cargo_home, lock_file, expected_mysql_checksum):
    if any(p["name"] == "sqlx-doris" for p in metadata["packages"]):
        raise ValueError("official baseline graph contains the SDK")
    locked = {
        (p["name"], p["version"]): p
        for p in tomllib.loads(Path(lock_file).read_text())["package"]
        if p.get("source", "").startswith("registry+")
    }
    result = []
    for name in sorted(SQLX_PACKAGES):
        matches = [p for p in metadata["packages"] if p["name"] == name]
        if len(matches) != 1:
            raise ValueError(f"official graph must contain one {name}")
        package = matches[0]
        source = Path(package["manifest_path"]).parent.resolve()
        if package["version"] != "0.9.0" or not (
            package.get("source") or ""
        ).startswith("registry+"):
            raise ValueError(f"official {name} is not registry SQLx 0.9.0")
        if not source.is_relative_to(Path(cargo_home).resolve() / "registry/src"):
            raise ValueError(f"official {name} source escapes the external registry")
        digest = locked[(name, package["version"])]["checksum"]
        if name == "sqlx-mysql" and digest != expected_mysql_checksum:
            raise ValueError(
                "official MySQL checksum differs from the reviewed upstream"
            )
        archives = list(
            (Path(cargo_home) / "registry/cache").glob(
                f"*/{name}-{package['version']}.crate"
            )
        )
        archive = next(
            (
                p
                for p in archives
                if hashlib.sha256(p.read_bytes()).hexdigest() == digest
            ),
            None,
        )
        if archive is None:
            raise ValueError(f"original registry archive missing or altered: {name}")
        files = 0
        with tarfile.open(archive) as tar:
            prefix = f"{name}-{package['version']}/"
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                if not member.name.startswith(prefix):
                    raise ValueError("registry archive has an unexpected member prefix")
                relative = member.name.removeprefix(prefix)
                if (source / relative).read_bytes() != tar.extractfile(member).read():
                    raise ValueError(
                        f"expanded official source changed: {name}/{relative}"
                    )
                files += 1
        result.append(
            {
                "name": name,
                "version": package["version"],
                "source": package["source"],
                "directory": str(source),
                "archive": str(archive),
                "sha256": digest,
                "expanded_files_byte_identical": files,
            }
        )
    return result


def sqlx_features(metadata):
    names = {p["id"]: p["name"] for p in metadata["packages"]}
    return {
        names[n["id"]]: sorted(n["features"])
        for n in metadata["resolve"]["nodes"]
        if names[n["id"]] in SQLX_PACKAGES
    }


def sqlx_dependencies(metadata):
    packages = {p["id"]: p for p in metadata["packages"]}
    return {
        packages[n["id"]]["name"]: sorted(
            (d["name"], packages[d["pkg"]]["name"], packages[d["pkg"]]["version"])
            for d in n["deps"]
        )
        for n in metadata["resolve"]["nodes"]
        if packages[n["id"]]["name"] in SQLX_PACKAGES
    }
