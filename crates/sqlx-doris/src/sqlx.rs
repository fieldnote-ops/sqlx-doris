//! SQLx API for applications that use this crate's bundled MySQL driver.
//!
//! The type machinery (`Pool`, `Row`, `FromRow`, `Executor`, ...) is upstream
//! `sqlx-core`; the `MySql` database, connections and options come from the
//! `sqlx-mysql` build shipped with this repository. Applications therefore need no
//! `[patch.crates-io]` entry: use `sqlx_doris::sqlx::{query_as, ...}` with
//! `sqlx_doris::MySqlPool` instead of the upstream facade.
//!
//! Compile-time macros (`query!`) still expand to `::sqlx::` paths, so they need
//! the upstream `sqlx` crate and the documented patch variant.

pub use sqlx_core::acquire::Acquire;
pub use sqlx_core::arguments::{Arguments, IntoArguments};
pub use sqlx_core::column::{Column, ColumnIndex};
pub use sqlx_core::connection::{ConnectOptions, Connection};
pub use sqlx_core::database::{self, Database};
pub use sqlx_core::decode::Decode;
pub use sqlx_core::describe::Describe;
pub use sqlx_core::encode::{self, Encode, IsNull};
pub use sqlx_core::error::{Error, Result};
pub use sqlx_core::executor::{Execute, Executor};
pub use sqlx_core::from_row::FromRow;
pub use sqlx_core::pool::{self, Pool};
pub use sqlx_core::query::{query, query_with};
pub use sqlx_core::query_as::{query_as, query_as_with};
pub use sqlx_core::query_builder::{self, QueryBuilder};
pub use sqlx_core::query_scalar::{query_scalar, query_scalar_with};
pub use sqlx_core::raw_sql::{raw_sql, RawSql};
pub use sqlx_core::row::Row;
pub use sqlx_core::sql_str::{AssertSqlSafe, SqlSafeStr, SqlStr};
pub use sqlx_core::statement::Statement;
pub use sqlx_core::transaction::Transaction;
pub use sqlx_core::type_info::TypeInfo;
pub use sqlx_core::types::Type;
pub use sqlx_core::value::{Value, ValueRef};
pub use sqlx_core::Either;

pub use sqlx_mysql::{
    self as mysql, MySql, MySqlConnection, MySqlExecutor, MySqlPool, MySqlTransaction,
};

/// SQLx query module (`sqlx::query`).
pub mod query {
    pub use sqlx_core::query::*;
}

/// SQLx error module (`sqlx::error`).
pub mod error {
    pub use sqlx_core::error::*;
}

/// SQLx decode module (`sqlx::decode`).
pub mod decode {
    pub use sqlx_core::decode::*;
}

/// SQLx types module (`sqlx::types`).
pub mod types {
    pub use sqlx_core::types::*;
}
