//! Shared SQLx MySQL/Doris connections, explicit Doris types and operations.
//!
//! ```no_run
//! # async fn run() -> Result<(), sqlx_doris::Error> {
//! use sqlx_doris::{Backend, PoolBuilder, sqlx};
//! let db = PoolBuilder::new(Backend::Doris)
//!     .connect("mysql://root@localhost:9030/app").await?;
//! let answer: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
//!     .bind(42_i64).fetch_one(db.pool()).await?;
//! assert_eq!(answer, 42);
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]

mod client;
mod error;
pub mod operations;
#[cfg(feature = "stream-load")]
pub mod stream_load;
pub mod types;

pub use client::{Backend, Capabilities, Client, ConnectFuture, GroupCommit, PoolBuilder};
pub use error::{Error, Result};
pub mod sqlx;
pub use crate::sqlx::{MySql, MySqlConnection, MySqlPool};
