import os
from pathlib import Path
import subprocess
import sys

project = Path(__file__).resolve().parents[1]
if os.environ.get("SQLX_DORIS_DEV_ACTIVE") != "1":
    os.execv(
        str(project / "scripts/dev"),
        [str(project / "scripts/dev"), sys.executable, "-B", *sys.argv],
    )
os.chdir(project)
artifacts = Path(os.environ["SQLX_DORIS_ARTIFACTS_DIR"])
artifacts.mkdir(parents=True, exist_ok=True)


def run(commands, name, env=None, cwd=None):
    log = artifacts / name
    with log.open("w") as output:
        for command in commands:
            print("Running:", " ".join(map(str, command)), flush=True)
            output.write("$ " + " ".join(map(str, command)) + "\n")
            output.flush()
            directory = cwd
            # Cargo discovers .cargo configuration from the current directory,
            # not from --manifest-path. Isolated consumers must use their own
            # configuration; never inherit the repository's ignored config.
            if directory is None and str(command[0]) == "cargo":
                arguments = list(map(str, command))
                if "--manifest-path" in arguments:
                    manifest = Path(arguments[arguments.index("--manifest-path") + 1])
                    directory = manifest.resolve().parent
            code = subprocess.run(
                command, stdout=output, stderr=subprocess.STDOUT, env=env, cwd=directory
            ).returncode
            if code:
                raise SystemExit(f"Failed ({code}): {log}")
    print(f"Passed: {log}")


def workspace_packages():
    """Read the aligned package set once, in dependency order."""
    import tomllib

    manifest = tomllib.loads((project / "Cargo.toml").read_text())
    roles = (
        "sqlx-doris-core",
        "sqlx-doris-macros",
        "sqlx-doris-ops",
        "sqlx-doris-cli",
        "sqlx-doris",
    )
    members = manifest["workspace"]["members"]
    if (
        "patch" in manifest
        or set(members) != {f"crates/{name}" for name in roles}
        or len(members) != len(roles)
    ):
        raise SystemExit(
            "Workspace must contain exactly the five aligned Doris packages"
        )
    result = {}
    for name in roles:
        data = tomllib.loads((project / "crates" / name / "Cargo.toml").read_text())
        if data["package"]["name"] != name:
            raise SystemExit(f"Unexpected package identity: {name}")
        result[name] = data
    versions = {data["package"]["version"] for data in result.values()}
    if len(versions) != 1:
        raise SystemExit("All Doris package versions must be aligned")
    for name, data in result.items():
        for dependency, spec in data.get("dependencies", {}).items():
            if dependency == "sqlx-doris" and name != "sqlx-doris":
                raise SystemExit(f"Internal package depends on facade: {name}")
            if (
                dependency in result
                and spec.get("version")
                != "=" + result[dependency]["package"]["version"]
            ):
                raise SystemExit(
                    f"Internal dependency must use the exact aligned version: {name}/{dependency}"
                )
    return result
