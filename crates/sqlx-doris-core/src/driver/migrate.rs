use super::*;
use crate::sql::migration::{Step, quote, split};
use sqlx_core::{
    migrate::{AppliedMigration, Migrate, MigrateDatabase, MigrateError, Migration},
    row::Row,
    sql_str::AssertSqlSafe,
};
use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

/// Client-observed DROP stage of main-lock release or explicit lock recovery.
/// This observes client progress, not a server fence or permission to retry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationRecoveryPhase {
    /// No owner-partition DROP submitted. Coordination metadata may still change.
    NotSubmitted,
    /// DROP may have reached the server; its outcome is uncertain.
    Submitted,
    /// DROP was acknowledged; later verification or coordination may fail.
    Acknowledged,
    /// Requested removal and its required postconditions completed.
    Verified,
}
const LOCK_TABLE: &str = "_sqlx_migrations_lock";
const LOCK_FORMAT: &str = "sqlx-doris migration lock v2";
const GATE_TABLE: &str = "_sqlx_migrations_gate";
const GATE_FORMAT: &str = "sqlx-doris transition gate v2";
const LOCK_ANCHOR: &str = "sqlx_anchor";
const LOCK_OWNER_PREFIX: &str = "sqlx_owner_";
#[derive(Debug)]
pub(super) struct Lock {
    table: String,
    token: String,
}
struct Running<'a>(&'a mut DorisConnection);
impl Drop for Running<'_> {
    fn drop(&mut self) {
        if self.0.migration_running {
            self.0.retired = true;
        }
    }
}
// Maintenance recovery must never return its connection to business work,
// including when DROP is cancelled or its response is lost.
struct MaintenanceRemoval<'a>(&'a mut DorisConnection);
impl Drop for MaintenanceRemoval<'_> {
    fn drop(&mut self) {
        self.0.retired = true;
    }
}
// Serialize ownership-check + DROP across normal release and forced recovery.
// A cancelled transition retires its connection and may retain its owner
// partition. Never take it over automatically; late cleanup names only that UUID.
struct Transition<'a> {
    conn: &'a mut DorisConnection,
    database: String,
    table: String,
    token: String,
    armed: bool,
}
impl Drop for Transition<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.conn.retired = true;
        }
    }
}
impl Transition<'_> {
    async fn remove_main(&mut self, table: &str, owner: &str) -> Result<(), Error> {
        // Cancellation is covered by the armed transition. On an error, retain
        // the gate and retire this connection instead of attempting cleanup.
        let sql = owner_drop(table, owner)?;
        self.conn.recovery_drop_phase = MigrationRecoveryPhase::Submitted;
        if let Err(e) = self.conn.execute(sql).await {
            self.conn.retired = true;
            return Err(error(format!(
                "main lock DROP outcome is uncertain: {e}; inspect lock status on a new connection; do not replay migrations"
            )));
        }
        self.conn.recovery_drop_phase = MigrationRecoveryPhase::Acknowledged;
        Ok(())
    }
    async fn finish<T>(mut self, result: Result<T, Error>) -> Result<T, Error> {
        let cleanup = async {
            self.conn.ready()?;
            if self.conn.gate_owner(&self.database).await?.as_deref() != Some(&self.token) {
                return Err(error(
                    "transition owner changed; refusing to remove its gate",
                ));
            }
            self.conn
                .execute(owner_drop(&self.table, &self.token)?)
                .await?;
            self.armed = false;
            Ok(())
        }
        .await;
        match (result, cleanup) {
            (Err(e), _) => Err(e),
            (Ok(value), Ok(())) => {
                if self.conn.recovery_drop_phase == MigrationRecoveryPhase::Acknowledged {
                    self.conn.recovery_drop_phase = MigrationRecoveryPhase::Verified;
                }
                Ok(value)
            }
            (Ok(_), Err(e)) => Err(e),
        }
    }
}
fn error(message: impl Into<String>) -> Error {
    Error::Protocol(format!("Doris migration: {}", message.into()))
}

fn lock_deadline(timeout: Duration) -> Result<Instant, Error> {
    Instant::now().checked_add(timeout).ok_or_else(|| {
        Error::Configuration(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "migration lock timeout exceeds the clock range",
        )))
    })
}

fn trusted(s: String) -> SqlStr {
    AssertSqlSafe(s).into_sql_str()
}
fn owner_partition(owner: &str) -> Result<String, Error> {
    let token = uuid::Uuid::parse_str(owner).map_err(|_| error("invalid lock owner UUID"))?;
    Ok(format!("{LOCK_OWNER_PREFIX}{}", token.simple()))
}
fn owner_drop(table: &str, owner: &str) -> Result<SqlStr, Error> {
    // Never address a reusable parent name for deletion. A successor has a
    // different partition name, so even a delayed statement cannot delete it.
    Ok(trusted(format!(
        "ALTER TABLE {table} DROP PARTITION {}",
        quote(&owner_partition(owner)?)?
    )))
}
fn owner_add(table: &str, owner: &str, replicas: u16) -> Result<SqlStr, Error> {
    Ok(trusted(format!(
        "ALTER TABLE {table} ADD PARTITION {} VALUES [('0'),('1')) ('replication_num'='{replicas}')",
        quote(&owner_partition(owner)?)?
    )))
}
fn owner_range(value: &str, lower: i8, upper: i8) -> bool {
    let compact: String = value.chars().filter(|c| !c.is_ascii_whitespace()).collect();
    compact.eq_ignore_ascii_case(&format!(
        "[types:[TINYINT];keys:[{lower}];..types:[TINYINT];keys:[{upper}];)"
    ))
}
fn metadata_conflict(error: &Error) -> bool {
    error.as_database_error().is_some_and(|e| {
        let message = e.message().to_ascii_lowercase();
        message.contains("already exists") || message.contains("is intersected with range")
    })
}
fn checksum_bytes(hex: &str) -> Result<Vec<u8>, Error> {
    if hex.len() != 96 || !hex.is_ascii() || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(error("invalid SHA-384 checksum in migration table"));
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| error("invalid checksum hex")))
        .collect()
}
fn checksum_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

mod adapter;
mod history;
mod partition_lock;
mod recovery;
mod schema;
