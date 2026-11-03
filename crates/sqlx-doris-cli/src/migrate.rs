//! Migration file commands and SQLx runner integration.
#[cfg(feature = "admin")]
use super::args::Repair;
use super::{Context, args::MigrationAction, output, render};
use sqlx_core::{
    connection::Connection,
    migrate::{AppliedMigration, Migrate, Migrator},
};
use sqlx_doris_core::DorisConnection;
use sqlx_doris_ops::{self as ops, failure::Result};
use std::path::{Path, PathBuf};

pub(super) async fn run(
    args: &Context,
    source: &Path,
    table: &str,
    no_lock: bool,
    command: &MigrationAction,
) -> Result<()> {
    if let MigrationAction::Add { name, reversible } = command {
        args.observation
            .phase(output::Phase::Local, "migration_files");
        let created = add(source, name, *reversible)?;
        args.observation
            .data("created_files", serde_json::json!(created));
        for path in created {
            args.line(format_args!("Created {}", path.display()));
        }
        return Ok(());
    }
    let m = load_source(source, table, !no_lock).await?;
    let mut c = args.connect().await?;
    let result: Result<()> = async {
        match command {
            MigrationAction::Run { target } => {
                let applied = prepare_runner(args, &mut c, table, !no_lock).await?;
                if !args.json {
                    render::runner(&args.observation.snapshot().details);
                }

                ops::migration_history::validate_applied(&m, &applied)?;
                if let Some(target) = target {
                    if !m
                        .iter()
                        .any(|v| v.version == *target && !v.migration_type.is_down_migration())
                    {
                        return Err(format!(
                            "target version {target} not found in migration up source"
                        )
                        .into());
                    }
                    let latest = applied.iter().map(|v| v.version).max().unwrap_or(0);
                    if *target < latest {
                        return Err(format!(
                            "target {target} is older than applied version {latest}"
                        )
                        .into());
                    }
                    args.observation
                        .phase(output::Phase::Submitted, "migration_steps");
                    m.run_to(*target, &mut c).await?;
                } else {
                    args.observation
                        .phase(output::Phase::Submitted, "migration_steps");
                    m.run(&mut c).await?;
                }
                args.line(format_args!("Migrations applied and verified"));
            }
            MigrationAction::Revert { target } => {
                let applied = prepare_runner(args, &mut c, table, !no_lock).await?;
                if !args.json {
                    render::runner(&args.observation.snapshot().details);
                }

                let cutoff = if let Some(target) = target {
                    if *target != 0 && !m.version_exists(*target) {
                        return Err(format!(
                            "target version {target} not found in migration source"
                        )
                        .into());
                    }
                    let latest = applied.iter().map(|v| v.version).max().unwrap_or(0);
                    if *target > latest {
                        return Err(format!(
                            "target {target} is newer than applied version {latest}"
                        )
                        .into());
                    }
                    *target
                } else {
                    m.iter()
                        .filter(|v| {
                            v.migration_type.is_down_migration()
                                && applied.iter().any(|a| a.version == v.version)
                        })
                        .map(|v| v.version)
                        .max()
                        .map(|v| v.checked_sub(1).ok_or("migration version underflow"))
                        .transpose()?
                        .unwrap_or(i64::MAX)
                };
                ops::migration_history::validate_applied(&m, &applied)?;
                args.observation
                    .phase(output::Phase::Submitted, "migration_revert_steps");
                m.undo(&mut c, cutoff).await?;
                args.line(format_args!("Revert complete"));
            }
            MigrationAction::Info => {
                use ops::migration_history::{self as history, SourceState};
                args.observation
                    .phase(output::Phase::Read, "migration_history");
                let history = history::observe(&mut c, table).await?;
                args.observation.data(
                    "migrations",
                    serde_json::json!({
                        "table_present": history.present,
                        "rows": history.rows
                    }),
                );
                let comparisons = history::compare(&history, Some(&m))?;
                let mut entries = Vec::new();
                for migration in m
                    .iter()
                    .filter(|migration| !migration.migration_type.is_down_migration())
                {
                    let previous = comparisons
                        .iter()
                        .find(|row| row.record.version == migration.version);
                    let state = match previous {
                        Some(row) if !row.record.success => "dirty",
                        Some(row) if row.state == SourceState::ChecksumMismatch => {
                            "checksum-mismatch"
                        }
                        Some(_) => "applied",
                        None => "pending",
                    };
                    entries.push(serde_json::json!({
                        "version": migration.version,
                        "state": state,
                        "description": migration.description.as_ref()
                    }));
                    args.line(format_args!(
                        "{} {state} {}",
                        migration.version, migration.description
                    ));
                }
                for row in comparisons
                    .iter()
                    .filter(|row| row.state == SourceState::Missing)
                {
                    let state = if row.record.success {
                        "applied"
                    } else {
                        "dirty"
                    };
                    entries.push(serde_json::json!({
                        "version": row.record.version,
                        "state": state,
                        "source": "missing_up_source"
                    }));
                    args.line(format_args!(
                        "{} {state} missing-up-source",
                        row.record.version
                    ));
                }
                args.observation
                    .data("migration_status", serde_json::json!(entries));
                args.observation
                    .phase(output::Phase::Read, "migration_lock_owners");
                let main_owner = c.migration_lock_owner().await?;
                let transition_owner = c.migration_transition_owner().await?;
                args.observation.data(
                    "locks",
                    serde_json::json!({
                        "main_owner": main_owner,
                        "transition_owner": transition_owner,
                    }),
                );
                args.line(format_args!(
                    "main-owner {}; transition-owner {}",
                    main_owner.as_deref().unwrap_or("unlocked"),
                    transition_owner.as_deref().unwrap_or("unlocked"),
                ));
            }
            MigrationAction::Add { .. } => unreachable!(),
        }
        Ok(())
    }
    .await;
    if result.is_ok() && !matches!(command, MigrationAction::Info) {
        args.observation
            .phase(output::Phase::Verified, "migration_completed");
    }
    let close = c.close().await;
    result?;
    close?;
    Ok(())
}

async fn load_source(source: &Path, table: &str, locking: bool) -> Result<Migrator> {
    let mut m = Migrator::new(source)
        .await
        .map_err(|_| output::fault(2, "invalid migration source"))?;
    ops::migration_history::sources(&m)?;
    m.dangerous_set_table_name(table.to_owned());
    m.set_locking(locking);
    Ok(m)
}

#[cfg(feature = "admin")]
pub(super) async fn repair(
    args: &Context,
    source: &Path,
    table: &str,
    version: i64,
    action: &Repair,
) -> Result<()> {
    let m = load_source(source, table, true).await?;
    let mut c = args.connect().await?;
    let result: Result<()> = async {
        args.observation
            .phase(output::Phase::Submitted, "migration_repair");
        c.lock().await?;
        args.observation.phase(
            output::Phase::Acknowledged,
            "migration_repair_lock_acquired",
        );
        if c.dirty_version(table).await? != Some(version) {
            return Err("requested version is not the current dirty migration".into());
        }
        match action {
            Repair::MarkApplied => {
                let migration = m
                    .iter()
                    .find(|v| v.version == version && !v.migration_type.is_down_migration())
                    .ok_or("version not found in migration source")?;
                args.observation
                    .phase(output::Phase::Submitted, "migration_repair_state");
                c.skip(table, migration).await?;
            }
            Repair::Remove => {
                args.observation
                    .phase(output::Phase::Submitted, "migration_repair_state");
                c.remove_dirty_migration(table, version).await?;
            }
        }
        c.unlock().await?;
        args.line(format_args!(
            "Reconciled migration {version}; no migration SQL was replayed"
        ));
        Ok(())
    }
    .await;
    if result.is_ok() {
        args.observation
            .phase(output::Phase::Verified, "migration_completed");
    }
    let close = c.close().await;
    result?;
    close?;
    Ok(())
}

fn add(source: &Path, name: &str, reversible: bool) -> Result<Vec<PathBuf>> {
    let name = name.replace(' ', "_");
    if name.is_empty()
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
    {
        return Err(output::fault(
            2,
            "migration name must contain letters, digits, underscores or hyphens",
        ));
    }
    std::fs::create_dir_all(source)?;
    let version = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let version = i64::try_from(version)?;
    create_migration_files(source, &name, version, reversible)
}
pub(super) fn create_migration_files(
    source: &Path,
    name: &str,
    version: i64,
    reversible: bool,
) -> Result<Vec<PathBuf>> {
    use std::io::Write;
    let mut created = Vec::new();
    let result = (|| -> Result<()> {
        for suffix in if reversible {
            vec!["up.sql", "down.sql"]
        } else {
            vec!["sql"]
        } {
            let path = source.join(format!("{version}_{name}.{suffix}"));
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            created.push(path);
            file.write_all(
                b"-- Doris migrations use non-atomic DDL. Review recovery before deployment.\n",
            )?;
        }
        Ok(())
    })();
    if let Err(error) = result {
        for path in &created {
            let _ = std::fs::remove_file(path);
        }
        return Err(error);
    }
    Ok(created)
}

/// Prepare one migration workflow; SQLx's later lock/owner checks remain intact.
async fn prepare_runner(
    context: &Context,
    connection: &mut DorisConnection,
    table: &str,
    locking: bool,
) -> Result<Vec<AppliedMigration>> {
    if locking {
        context
            .observation
            .phase(output::Phase::Submitted, "migration_coordination");
        connection.lock().await?;
        context
            .observation
            .phase(output::Phase::Acknowledged, "migration_lock_acquired");
        #[cfg(feature = "admin")]
        if let Some(path) = &context.identity_file {
            let name = ops::maintenance::identity::credential_name(path)?;
            let main_owner = connection
                .migration_lock_owner()
                .await?
                .ok_or("managed runner lock missing")?;
            context.observation.data(
                "runner_identity",
                serde_json::json!({
                    "name": name,
                    "main_owner": main_owner,
                }),
            );
        }
    }
    context
        .observation
        .phase(output::Phase::Submitted, "migration_state_and_steps");
    connection.ensure_migrations_table(table).await?;
    context
        .observation
        .phase(output::Phase::Acknowledged, "migration_metadata_ready");
    Ok(connection.list_applied_migrations(table).await?)
}
