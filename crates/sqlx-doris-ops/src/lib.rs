//! Doris application helpers and explicitly requested maintenance operations.
#![forbid(unsafe_code)]

#[cfg(feature = "helpers")]
mod error;
#[cfg(feature = "helpers")]
pub use error::{Error, Result};

#[cfg(feature = "helpers")]
pub mod operations;
#[cfg(feature = "stream-load")]
pub mod stream_load;

#[cfg(feature = "helpers")]
use sqlx_doris_core::{Doris, DorisPool};

#[doc(hidden)]
#[cfg(any(feature = "cli-support", feature = "kafka"))]
pub mod context;
#[doc(hidden)]
#[cfg(any(feature = "cli-support", feature = "kafka"))]
pub mod failure;
#[doc(hidden)]
#[cfg(feature = "kafka")]
pub mod kafka;
#[doc(hidden)]
#[cfg(feature = "maintenance")]
pub mod maintenance;
#[doc(hidden)]
#[cfg(any(feature = "cli-support", feature = "kafka"))]
pub mod observation;
#[doc(hidden)]
#[cfg(any(feature = "cli-support", feature = "kafka"))]
pub mod storage;

#[doc(hidden)]
#[cfg(feature = "cli-support")]
pub mod migration_history;
