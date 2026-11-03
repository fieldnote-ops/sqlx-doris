//! Apache Doris driver types and checked query macros. Use upstream `sqlx`
//! directly for generic queries, traits, FromRow and migrations.
#![forbid(unsafe_code)]

#[cfg(feature = "migrate")]
pub use sqlx_doris_core::MigrationRecoveryPhase;
pub use sqlx_doris_core::{
    Doris, DorisArguments, DorisColumn, DorisConnectOptions, DorisConnection, DorisDatabaseError,
    DorisPool, DorisPoolOptions, DorisQueryResult, DorisRow, DorisSslMode, DorisStatement,
    DorisTransaction, DorisTransactionManager, DorisTypeInfo, DorisValue, DorisValueFormat,
    DorisValueRef, GroupCommit, types,
};
#[cfg(feature = "macros")]
pub use sqlx_doris_macros::{
    query, query_as, query_file, query_file_as, query_file_scalar, query_scalar,
};
#[cfg(feature = "operations")]
pub use sqlx_doris_ops::operations;
#[cfg(feature = "stream-load")]
pub use sqlx_doris_ops::stream_load;
