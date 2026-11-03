//! User command grammar; domain requests live in the operations library.
#[cfg(feature = "kafka-admin")]
use crate::commands::kafka;
#[cfg(feature = "admin")]
use crate::commands::{diagnose, drain, identity, routine};
#[cfg(feature = "admin")]
use clap::ValueEnum;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(
    name = "cargo sqlx-doris",
    bin_name = "cargo sqlx-doris",
    version,
    about = "SQLx query preparation and migrations for Apache Doris"
)]
pub(crate) struct Args {
    /// Doris URL. Can also use DORIS_TEST_URL for dedicated source tests.
    #[arg(long, env = "DATABASE_URL", hide_env_values = true, global = true)]
    pub(crate) database_url: Option<String>,
    /// Emit one versioned JSON result, including controlled errors.
    #[arg(long, global = true)]
    pub(crate) json: bool,
    #[cfg(feature = "admin")]
    /// Private external runner credentials. Unset DATABASE_URL/DORIS_TEST_URL;
    /// never expose administrator credentials to the runner process.
    #[arg(long, global = true)]
    pub(crate) identity_file: Option<PathBuf>,
    /// Replicas for migration metadata (default 3; use 1 for a single-BE fixture).
    #[arg(long, default_value_t = 3, global = true, value_parser = clap::value_parser!(u16).range(1..))]
    pub(crate) replication_num: u16,
    /// Maximum time to wait for another migration runner.
    #[arg(long, default_value_t = 30, global = true, value_parser = clap::value_parser!(u64).range(0..=3600))]
    pub(crate) lock_timeout: u64,
    /// Maximum time for schema-change completion or drain metadata/cancellation.
    /// drain wait uses its own --timeout instead.
    #[arg(long, default_value_t = 300, global = true, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub(crate) ddl_timeout: u64,
    #[command(subcommand)]
    pub(crate) command: Action,
}
#[derive(Subcommand)]
pub(crate) enum Action {
    /// Generate or verify SQLx-compatible offline query metadata.
    Prepare {
        #[arg(long, default_value = "Cargo.toml")]
        manifest_path: PathBuf,
        /// Existing metadata to check, or output directory. Must be external.
        #[arg(long)]
        output: Option<PathBuf>,
        #[arg(long)]
        check: bool,
        /// Pass --offline to Cargo when all Rust dependencies are cached.
        #[arg(long)]
        cargo_offline: bool,
        /// Extra cargo check arguments, for example -- --workspace --all-features.
        #[arg(last = true)]
        cargo_args: Vec<std::ffi::OsString>,
    },
    /// Add, apply, inspect or revert SQLx migration files.
    Migrate {
        #[arg(long, default_value = "migrations")]
        source: PathBuf,
        #[arg(long, default_value = "_sqlx_migrations")]
        table: String,
        /// Explicitly select externally serialized execution without a DB lock.
        #[arg(long)]
        no_lock: bool,
        #[command(subcommand)]
        command: MigrationAction,
    },
    /// Create/check/drop a Doris database.
    Database {
        #[command(subcommand)]
        command: DatabaseAction,
    },
    /// Explicit specialist administration; not required for queries or ingestion.
    #[cfg(feature = "admin")]
    Admin {
        #[command(subcommand)]
        command: AdminAction,
    },
}
#[derive(Subcommand)]
pub(crate) enum MigrationAction {
    Add {
        name: String,
        #[arg(long)]
        reversible: bool,
    },
    Run {
        #[arg(long)]
        target: Option<i64>,
    },
    Info,
    Revert {
        /// Stop at this version. Without --target, revert one applied pair.
        #[arg(long)]
        target: Option<i64>,
    },
}
#[cfg(feature = "admin")]
#[derive(Clone, Copy, ValueEnum)]
pub(crate) enum Repair {
    MarkApplied,
    Remove,
}
#[cfg(feature = "admin")]
#[derive(Subcommand)]
pub(crate) enum AdminAction {
    /// Observe recovery prerequisites without changing identities, jobs, locks or offsets.
    Diagnose {
        #[command(flatten)]
        options: diagnose::Options,
    },
    /// Read-only Kafka identity, range and retained-epoch observations; no offset reset or recovery.
    #[cfg(feature = "kafka-admin")]
    Kafka {
        #[command(subcommand)]
        command: kafka::Action,
    },
    /// Create/inspect/explicitly stop Routine Load under a permanent UUID no-reuse protocol.
    Routine {
        #[command(subcommand)]
        command: routine::Action,
    },
    /// Inspect/wait for retired-runner sessions, database transactions and async work.
    /// Uncovered job classes and admitted commands require external reconciliation.
    Drain {
        #[command(subcommand)]
        command: drain::Action,
    },
    /// Manage a dedicated, non-reusable migration identity (verified single-FE built-in RBAC).
    Identity {
        #[command(subcommand)]
        command: identity::Action,
    },
    /// Inspect a durable lock, or recover it after stopping its previous runner.
    Lock {
        #[command(subcommand)]
        command: LockAction,
    },
    /// Repair reconciled migration history without replaying its SQL.
    Migrate {
        #[arg(long, default_value = "migrations")]
        source: PathBuf,
        #[arg(long, default_value = "_sqlx_migrations")]
        table: String,
        #[command(subcommand)]
        command: AdminMigrationAction,
    },
}
#[cfg(feature = "admin")]
#[derive(Subcommand)]
pub(crate) enum AdminMigrationAction {
    /// Record a reconciled dirty migration without replaying its SQL.
    Repair {
        #[arg(long)]
        version: i64,
        #[arg(long, value_enum)]
        action: Repair,
        #[arg(long, required = true)]
        force: bool,
    },
}
#[derive(Subcommand)]
pub(crate) enum DatabaseAction {
    Create,
    Exists,
    Drop {
        #[arg(long, required = true)]
        force: bool,
    },
}
#[cfg(feature = "admin")]
#[derive(Subcommand)]
pub(crate) enum LockAction {
    Status,
    /// Release the exact main owner partition after stopping its runner and commands.
    /// Does not cancel sessions, block reconnects or revoke write privileges.
    Recover {
        #[arg(long, value_parser = recovery_owner)]
        owner: String,
        #[arg(long, required = true)]
        force: bool,
        /// Additionally verify retired-name absence and old sessions.
        #[arg(long, requires = "maintenance")]
        retired_identity: Option<String>,
        /// Confirm stopped old actors/admission, serialized administration and
        /// reconciliation of work outside the documented drain scope.
        #[arg(long)]
        maintenance: bool,
    },
    /// Preview release of an interrupted owner partition. Never takes over automatically.
    RecoverTransition {
        /// Exact transition-owner UUID from lock status.
        #[arg(long, value_parser = recovery_owner)]
        owner: String,
        /// Exact main owner from lock status, or "unlocked" if the main lock is absent.
        #[arg(long, value_name = "UUID|unlocked", value_parser = recovery_lock_owner)]
        lock_owner: String,
        /// Confirm all migration/unlock/recovery processes are stopped, no old
        /// commands can commit, and this is the sole administrator across machines.
        #[arg(long)]
        maintenance: bool,
        /// Remove only this owner's partition after checks; retain the gate table.
        /// Without this flag, only preview the operation.
        #[arg(long, requires = "maintenance")]
        force: bool,
        /// Additionally verify retired-name absence and old sessions.
        #[arg(long, requires = "maintenance")]
        retired_identity: Option<String>,
    },
}
#[cfg(feature = "admin")]
fn recovery_owner(value: &str) -> std::result::Result<String, String> {
    uuid::Uuid::parse_str(value)
        .map_err(|_| "expected the exact owner UUID from lock status".to_owned())?;
    Ok(value.to_owned())
}
#[cfg(feature = "admin")]
fn recovery_lock_owner(value: &str) -> std::result::Result<String, String> {
    if value == "unlocked" {
        Ok(value.to_owned())
    } else {
        recovery_owner(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn command_surface_matches_the_installed_profile_without_old_aliases() {
        let command = Args::command();
        for name in ["prepare", "database", "migrate"] {
            assert!(command.find_subcommand(name).is_some());
        }
        for name in ["lock", "identity", "drain", "routine", "kafka", "diagnose"] {
            assert!(command.find_subcommand(name).is_none());
            assert!(Args::try_parse_from(["cli", name]).is_err());
        }
        let migrate = command.find_subcommand("migrate").unwrap();
        assert!(migrate.find_subcommand("repair").is_none());
        assert_eq!(
            command.find_subcommand("admin").is_some(),
            cfg!(feature = "admin")
        );
        let has_identity = command
            .get_arguments()
            .any(|arg| arg.get_id() == "identity_file");
        assert_eq!(has_identity, cfg!(feature = "admin"));
        #[cfg(feature = "admin")]
        {
            let admin = command.find_subcommand("admin").unwrap();
            for name in [
                "lock", "identity", "drain", "routine", "diagnose", "migrate",
            ] {
                assert!(admin.find_subcommand(name).is_some());
            }
            assert_eq!(
                admin.find_subcommand("kafka").is_some(),
                cfg!(feature = "kafka-admin")
            );
            let diagnose = admin.find_subcommand("diagnose").unwrap();
            let kafka_arguments = diagnose
                .get_arguments()
                .any(|arg| arg.get_id() == "connection_file");
            assert_eq!(kafka_arguments, cfg!(feature = "kafka-admin"));
            let repair = admin
                .find_subcommand("migrate")
                .unwrap()
                .find_subcommand("repair");
            assert!(repair.is_some());
        }
    }

    #[test]
    fn basic_migration_configuration_and_global_json_parse_at_the_leaf() {
        let parsed = Args::try_parse_from([
            "cli",
            "migrate",
            "info",
            "--replication-num",
            "1",
            "--lock-timeout",
            "0",
            "--ddl-timeout",
            "7",
            "--json",
        ])
        .unwrap();
        assert!(parsed.json);
        assert_eq!(parsed.replication_num, 1);
        assert_eq!(parsed.lock_timeout, 0);
        assert_eq!(parsed.ddl_timeout, 7);
        assert!(matches!(
            parsed.command,
            Action::Migrate {
                command: MigrationAction::Info,
                ..
            }
        ));
    }
}
