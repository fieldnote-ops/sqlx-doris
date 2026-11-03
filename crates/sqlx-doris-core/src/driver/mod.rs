mod bindings;
pub(crate) mod codec;
#[cfg(feature = "macro-support")]
mod macro_support;
#[cfg(feature = "migrate")]
mod migrate;
#[cfg(feature = "migrate")]
pub use migrate::MigrationRecoveryPhase;
mod connection;
mod executor;
mod model;
mod options;
mod transaction;
mod type_checking;
mod wire;

use codec::Cursor;
pub use codec::DorisArguments;
use futures_core::{future::BoxFuture, stream::BoxStream};
use futures_util::TryStreamExt;
pub use model::*;
pub use options::*;
use sqlx_core::Either;
use sqlx_core::error::Error;
use sqlx_core::{
    common::StatementCache,
    connection::{Connection, LogSettings},
    executor::{Execute, Executor},
    row::Row,
    sql_str::{SqlSafeStr, SqlStr},
    transaction::{Transaction, TransactionManager},
};
use std::sync::Arc;
pub use wire::DorisDatabaseError;
use wire::{Wire, database_error};

pub(crate) fn protocol(s: &str) -> Error {
    Error::Protocol(s.into())
}
#[cfg(test)]
mod randomized;
/// SQLx's native pool specialized for Doris.
pub type DorisPool = sqlx_core::pool::Pool<Doris>;
/// SQLx pool sizing, timeout and connection lifecycle configuration.
pub type DorisPoolOptions = sqlx_core::pool::PoolOptions<Doris>;
/// SQLx transaction guard specialized for Doris's write transactions.
pub type DorisTransaction<'c> = Transaction<'c, Doris>;

#[derive(Debug, Clone)]
struct Prepared {
    id: u32,
    statement: DorisStatement,
    binary_safe: bool,
}
#[derive(Debug)]
struct OkReply {
    result: DorisQueryResult,
    status: u16,
    info: Vec<u8>,
}
fn ok_reply(b: &[u8]) -> Result<OkReply, Error> {
    let mut c = Cursor(b);
    let tag = c.byte()?;
    if !matches!(tag, 0 | 254) {
        return Err(protocol("expected OK packet"));
    }
    let affected = c.len()?;
    c.len()?;
    let status = c.u16()?;
    c.u16()?;
    Ok(OkReply {
        result: DorisQueryResult { affected },
        status,
        info: c.0.to_vec(),
    })
}
/// An asynchronous Doris FE connection. An interrupted command retires the
/// connection; writes are never retried. Use a pool for automatic replacement.
#[derive(Debug)]
pub struct DorisConnection {
    wire: Wire,
    cache: StatementCache<Prepared>,
    dirty: bool,
    retired: bool,
    transaction: bool,
    no_backslash: bool,
    rejected_begin: bool,
    log: LogSettings,
    #[cfg(feature = "migrate")]
    migration_lock: Option<migrate::Lock>,
    #[cfg(feature = "migrate")]
    migration_running: bool,
    #[cfg(feature = "migrate")]
    recovery_drop_phase: MigrationRecoveryPhase,
    #[cfg(feature = "migrate")]
    migration_replication: u16,
    #[cfg(feature = "migrate")]
    migration_timeout: std::time::Duration,
    #[cfg(feature = "migrate")]
    migration_lock_timeout: std::time::Duration,
}

/// Doris's top-level write transaction implementation. Nested transactions and
/// custom BEGIN statements are rejected before any command is sent.
#[derive(Debug)]
pub struct DorisTransactionManager;
