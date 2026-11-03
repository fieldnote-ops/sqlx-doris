//! Standalone Apache Doris driver implementing SQLx's native traits.
//!
//! ```no_run
//! use sqlx_doris_core::DorisPool;
//! # async fn run() -> Result<(), sqlx_core::Error> {
//! let pool = DorisPool::connect("doris://root@localhost:9030/app").await?;
//! let answer: i64 = sqlx_core::query_scalar::query_scalar("SELECT CAST(? AS BIGINT)")
//!     .bind(42_i64)
//!     .fetch_one(&pool)
//!     .await?;
//! assert_eq!(answer, 42);
//! pool.close().await;
//! # Ok(()) }
//! ```
#![forbid(unsafe_code)]

mod driver;
pub mod types;

pub use driver::*;

#[cfg(any(feature = "migrate", feature = "macro-support"))]
mod sql;

/// Shared value encoders for the aligned operations package.
#[doc(hidden)]
#[cfg(feature = "value-support")]
pub mod value_support {
    pub use crate::types::json::to_vec as json_vec;
    pub use crate::types::text::{Value, to_doris, to_value};
}
