//! Cargo integration, external cache publication and advisory locking.
use super::{Context, output};
use sqlx_doris_core::DorisConnectOptions;
use sqlx_doris_ops::{
    failure::Result,
    storage::{external, external_volume},
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

fn cargo_diagnostics(directory: &Path) -> Result<(std::fs::File, PathBuf)> {
    let path = directory.join(format!("prepare-cargo-{}.log", uuid::Uuid::new_v4()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(&path)?;
    Ok((file, path))
}
fn child(command: &mut Command, log: Option<&std::fs::File>) -> Result<()> {
    if let Some(file) = log {
        command
            .stdout(Stdio::from(file.try_clone()?))
            .stderr(Stdio::from(file.try_clone()?));
    }
    let status = command.status()?;
    if let Some(log) = log {
        log.sync_all()?;
    }
    if !status.success() {
        return Err(output::fault(
            3,
            "Cargo command failed; see its diagnostics",
        ));
    }
    Ok(())
}
struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct PrepareLock(std::fs::File);
impl Drop for PrepareLock {
    fn drop(&mut self) {
        // A concurrent fork can briefly inherit a CLOEXEC descriptor. Closing
        // only our descriptor leaves that shared lock alive until its exec.
        // Explicitly release this file description before closing it.
        let _ = self.0.unlock();
    }
}
fn prepare_lock(output: &Path) -> Result<PrepareLock> {
    let path = output.join(".sqlx-doris-prepare.lock");
    match std::fs::symlink_metadata(&path) {
        Ok(meta) if !meta.file_type().is_file() => {
            return Err("prepare lock path must be a regular file".into());
        }
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
        _ => {}
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)?;
    file.try_lock().map_err(|e| {
        format!(
            "cannot acquire prepare lock {}: {e}; another prepare may be running",
            path.display()
        )
    })?;
    // Keep the inode: removing an advisory lock file would let a new process
    // bypass a waiter on the old inode. Drop unlocks explicitly; process exit
    // still closes the file if the guard cannot run.
    Ok(PrepareLock(file))
}
fn query_names(path: &Path) -> Result<Vec<String>> {
    let mut result = Vec::new();
    for file in std::fs::read_dir(path)? {
        let file = file?;
        let name = file.file_name().to_string_lossy().into_owned();
        if name
            .strip_prefix("query-")
            .and_then(|n| n.strip_suffix(".json"))
            .is_some_and(|hash| hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            if !file.file_type()?.is_file() {
                return Err(format!(
                    "offline metadata must be a regular file: {}",
                    file.path().display()
                )
                .into());
            }
            result.push(name);
        }
    }
    Ok(result)
}
fn query_files(path: &Path) -> Result<BTreeMap<String, serde_json::Value>> {
    use sha2::{Digest, Sha256};
    let mut result = BTreeMap::new();
    for name in query_names(path)? {
        let value: serde_json::Value = serde_json::from_slice(&std::fs::read(path.join(&name))?)
            .map_err(|e| format!("invalid offline metadata {name}: {e}"))?;
        let query = value["query"]
            .as_str()
            .ok_or("offline metadata has no query")?;
        let hash = format!("{:x}", Sha256::digest(query.as_bytes()));
        if value["hash"] != hash || name != format!("query-{hash}.json") {
            return Err(format!("offline metadata query/hash mismatch: {name}").into());
        }
        result.insert(name, value);
    }
    Ok(result)
}
fn publish(generated: &Path, output: &Path, names: impl Iterator<Item = String>) -> Result<()> {
    let old = query_names(output)?;
    let stage_path = output.join(format!(".sqlx-doris-stage-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&stage_path)?;
    let stage = Directory(stage_path);
    let names = names.collect::<Vec<_>>();
    // Copy everything before touching a previously usable cache. Publish whole
    // JSON files with same-filesystem renames, then remove obsolete query files.
    for name in &names {
        std::fs::copy(generated.join(name), stage.0.join(name))?;
    }
    for name in &names {
        std::fs::rename(stage.0.join(name), output.join(name))?;
    }
    for name in old {
        if !names.contains(&name) {
            std::fs::remove_file(output.join(name))?;
        }
    }
    Ok(())
}

pub(super) fn run(
    args: &Context,
    manifest: &Path,
    output: Option<&Path>,
    check: bool,
    offline: bool,
    cargo_args: &[std::ffi::OsString],
) -> Result<()> {
    use sha2::{Digest, Sha256};
    for arg in cargo_args {
        let arg = arg.to_string_lossy();
        if ["--target-dir", "--config", "--manifest-path"]
            .iter()
            .any(|key| arg == *key || arg.starts_with(&format!("{key}=")))
        {
            return Err("prepare owns target/cache paths and the manifest; pass feature/package options only".into());
        }
    }
    let volume = external_volume()?;
    let root = external(
        &std::env::var_os("SQLX_DORIS_BUILD_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| volume.join("sqlx-doris")),
        &volume,
    )?;
    let manifest = manifest
        .canonicalize()
        .map_err(|_| output::fault(2, "manifest does not exist or cannot be read"))?;
    if !manifest.is_file() {
        return Err(output::fault(2, "manifest must be a regular file"));
    }
    let project_directory = manifest
        .parent()
        .ok_or_else(|| output::fault(2, "manifest directory is unavailable"))?;
    let key = format!(
        "{:x}",
        Sha256::digest(manifest.to_string_lossy().as_bytes())
    );
    let output = external(
        &output
            .map(PathBuf::from)
            .or_else(|| std::env::var_os("SQLX_OFFLINE_DIR").map(PathBuf::from))
            .unwrap_or_else(|| root.join("offline").join(&key[..16])),
        &volume,
    )?;
    let _lock = prepare_lock(&output)?;
    let target = external(
        &std::env::var_os("CARGO_TARGET_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| root.join("target-clean")),
        &volume,
    )?;
    let home = external(&root.join("cargo-home"), &volume)?;
    let tmp = external(&root.join("tmp-clean"), &volume)?;
    let generated = tmp.join(format!("prepare-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&generated)?;
    let _generated = Directory(generated.clone());
    let url = args.url()?;
    url.parse::<DorisConnectOptions>()?;
    let diagnostics = if args.json {
        let (file, path) = cargo_diagnostics(&external(&root.join("artifacts/clean"), &volume)?)?;
        args.observation
            .data("cargo_diagnostics_file", serde_json::json!(path));
        Some(file)
    } else {
        None
    };
    let configure = |command: &mut Command| {
        command
            .current_dir(project_directory)
            .env("CARGO_TARGET_DIR", &target)
            .env("CARGO_BUILD_BUILD_DIR", &target)
            .env("CARGO_HOME", &home)
            .env("TMPDIR", &tmp)
            .env("TMP", &tmp)
            .env("TEMP", &tmp)
            .env("DATABASE_URL", &url)
            .env("SQLX_OFFLINE", "false")
            .env("SQLX_OFFLINE_DIR", &generated);
        for key in [
            "XDG_CACHE_HOME",
            "SCCACHE_DIR",
            "PIP_CACHE_DIR",
            "UV_CACHE_DIR",
        ] {
            command.env(key, root.join("cache").join(key.to_ascii_lowercase()));
        }
    };
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let mut metadata = Command::new(&cargo);
    metadata
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
        ])
        .arg(&manifest);
    if offline {
        metadata.arg("--offline");
    }
    configure(&mut metadata);
    let answer = metadata.output()?;
    if !answer.status.success() {
        if args.json {
            use std::io::Write;
            let mut log = diagnostics.as_ref().unwrap();
            log.write_all(&answer.stderr)?;
            log.sync_all()?;
            return Err(output::fault(
                2,
                "Cargo metadata failed; inspect external diagnostics",
            ));
        }
        return Err(String::from_utf8_lossy(&answer.stderr).into_owned().into());
    }
    let metadata: serde_json::Value = serde_json::from_slice(&answer.stdout)?;
    let workspace = metadata["workspace_root"]
        .as_str()
        .ok_or("Cargo workspace root missing")?;
    let workspace_key = format!("{:x}", Sha256::digest(workspace.as_bytes()));
    let workspace_lock_dir =
        external(&tmp.join("prepare-workspaces").join(workspace_key), &volume)?;
    let _workspace_lock = prepare_lock(&workspace_lock_dir)?;
    // Recompile application packages, preserving dependency builds. This makes
    // --check observe schema changes even when Rust sources are unchanged.
    for package in metadata["packages"]
        .as_array()
        .ok_or("invalid Cargo metadata")?
    {
        let mut clean = Command::new(&cargo);
        clean
            .arg("clean")
            .arg("--manifest-path")
            .arg(&manifest)
            .arg("-p")
            .arg(package["name"].as_str().ok_or("package name missing")?);
        configure(&mut clean);
        if offline {
            clean.arg("--offline");
        }
        child(&mut clean, diagnostics.as_ref())?;
    }
    let mut compile = Command::new(&cargo);
    compile
        .args(["check", "--all-targets", "--manifest-path"])
        .arg(&manifest)
        .args(cargo_args);
    if offline {
        compile.arg("--offline");
    }
    configure(&mut compile);
    child(&mut compile, diagnostics.as_ref())?;
    let found = query_files(&generated)?;
    if found.is_empty() {
        return Err("no Doris query macros were compiled; metadata was not replaced".into());
    }
    if found.values().any(|v| v["db_name"] != "Doris") {
        return Err("offline metadata contains a database other than Doris".into());
    }
    args.observation.data(
        "prepare",
        serde_json::json!({
            "query_count": found.len(),
            "output": output,
            "check": check
        }),
    );
    if check {
        if query_files(&output)? != found {
            return Err(output::fault(
                3,
                "offline metadata differs; run prepare against the current schema",
            ));
        }
        args.line(format_args!("Verified {} queries", found.len()));
    } else {
        args.observation
            .phase(output::Phase::Local, "offline_cache_publish");
        publish(&generated, &output, found.keys().cloned())?;
        args.line(format_args!(
            "Prepared {} queries in {}",
            found.len(),
            output.display()
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::migrate::create_migration_files;

    fn fixture() -> Directory {
        let dir = Directory(
            std::env::temp_dir().join(format!("doris-cache-test-{}", uuid::Uuid::new_v4())),
        );
        std::fs::create_dir(&dir.0).unwrap();
        dir
    }
    #[test]
    fn reversible_pair_failure_preserves_existing_sources() {
        let output = fixture();
        let down = output.0.join("1_initial.down.sql");
        std::fs::write(&down, "existing source").unwrap();
        assert!(create_migration_files(&output.0, "initial", 1, true).is_err());
        assert!(!output.0.join("1_initial.up.sql").exists());
        assert_eq!(std::fs::read_to_string(down).unwrap(), "existing source");
    }
    #[test]
    fn prepare_excludes_other_writers_and_releases_on_drop() {
        let output = fixture();
        let first = prepare_lock(&output.0).unwrap();
        assert!(prepare_lock(&output.0).is_err());
        drop(first);
        let next = prepare_lock(&output.0).unwrap();
        drop(next);
    }
    #[test]
    fn prepare_unlock_releases_inherited_descriptors_without_unlocking_a_successor() {
        let output = fixture();
        let first = prepare_lock(&output.0).unwrap();
        let inherited = first.0.try_clone().unwrap();
        assert!(prepare_lock(&output.0).is_err());
        drop(first);
        let successor = prepare_lock(&output.0).unwrap();
        drop(inherited);
        assert!(prepare_lock(&output.0).is_err());
        drop(successor);
        drop(prepare_lock(&output.0).unwrap());
    }
    #[test]
    fn failed_staging_keeps_the_previous_cache_usable() {
        let generated = fixture();
        let output = fixture();
        std::fs::write(
            output.0.join(
                "query-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
            ),
            "previous",
        )
        .unwrap();
        std::fs::write(
            generated.0.join(
                "query-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json",
            ),
            "complete",
        )
        .unwrap();
        assert!(
            publish(
                &generated.0,
                &output.0,
                [
                    "query-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
                        .into(),
                    "query-cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc.json"
                        .into()
                ]
                .into_iter()
            )
            .is_err()
        );
        assert_eq!(
            std::fs::read_to_string(output.0.join(
                "query-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json"
            ))
            .unwrap(),
            "previous"
        );
        assert!(
            !output
                .0
                .join("query-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json")
                .exists()
        );
        assert_eq!(std::fs::read_dir(&output.0).unwrap().count(), 1);
    }
    #[test]
    fn publication_replaces_corrupt_old_metadata_and_preserves_other_files() {
        let generated = fixture();
        let output = fixture();
        std::fs::write(
            output.0.join(
                "query-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json",
            ),
            "broken JSON",
        )
        .unwrap();
        std::fs::write(output.0.join("notes.txt"), "keep").unwrap();
        std::fs::write(output.0.join("query-notes.json"), "keep this too").unwrap();
        std::fs::write(
            generated.0.join(
                "query-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json",
            ),
            "new",
        )
        .unwrap();
        publish(
            &generated.0,
            &output.0,
            [
                "query-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
                    .into(),
            ]
            .into_iter(),
        )
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(output.0.join(
                "query-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
            ))
            .unwrap(),
            "new"
        );
        assert!(
            !output
                .0
                .join("query-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.json")
                .exists()
        );
        assert_eq!(
            std::fs::read_to_string(output.0.join("query-notes.json")).unwrap(),
            "keep this too"
        );
        assert_eq!(
            std::fs::read_to_string(output.0.join("notes.txt")).unwrap(),
            "keep"
        );
    }
    #[cfg(unix)]
    #[test]
    fn cache_file_symlinks_are_rejected() {
        let target = fixture();
        let output = fixture();
        std::fs::write(target.0.join("outside.json"), "do not touch").unwrap();
        std::os::unix::fs::symlink(
            target.0.join("outside.json"),
            output.0.join(
                "query-dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd.json",
            ),
        )
        .unwrap();
        assert!(query_names(&output.0).is_err());
        assert_eq!(
            std::fs::read_to_string(target.0.join("outside.json")).unwrap(),
            "do not touch"
        );
    }
}
