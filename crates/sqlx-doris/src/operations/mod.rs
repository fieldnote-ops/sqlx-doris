//! Doris schema, transactions, and visibility operations.
use crate::sqlx;

mod materialized_view;
mod polling;
mod schema;
mod transaction;
mod value;
mod variant;

use crate::{Backend, Client, Error, Result};
use std::{future::Future, time::Duration};

pub use materialized_view::{MaterializedViewTask, RefreshMode, TaskStatus};
pub use schema::{
    Aggregate, AggregateArgument, CatalogTableName, Column, ColumnInfo, DataType, TableDefinition,
    TableModel, TableName, describe_table, project,
};
pub use transaction::DorisTransaction;
pub use value::push_complex_value;
pub use variant::push_variant_path;

/// Poll an application read predicate until visible. Errors are returned directly;
/// this helper never resubmits the original write.
pub async fn wait_until<F, Fut>(timeout: Duration, interval: Duration, mut visible: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<bool, sqlx::Error>>,
{
    polling::poll_until(
        timeout,
        interval,
        || Error::VisibilityTimeout,
        || {
            let result = visible();
            async move { Ok(result.await?.then_some(())) }
        },
    )
    .await
}

impl Client {
    pub(crate) fn require_doris(&self) -> Result<()> {
        if self.backend() == Backend::Doris {
            Ok(())
        } else {
            Err(Error::Unsupported(
                "this operation requires the Doris profile",
            ))
        }
    }
    /// Create an OLAP table with an explicitly selected Doris table model.
    pub async fn create_table(&self, definition: &TableDefinition) -> Result<()> {
        use crate::sqlx::Executor;
        self.require_doris()?;
        self.pool()
            .execute(sqlx::AssertSqlSafe(definition.to_sql()?))
            .await?;
        Ok(())
    }
    /// Read DESC metadata, preserving the server's logical type descriptions.
    pub async fn describe_table(&self, table: &TableName) -> Result<Vec<ColumnInfo>> {
        describe_table(self.pool(), table).await
    }
    /// Read logical metadata for a three-part Doris catalog relation.
    pub async fn describe_catalog_table(
        &self,
        table: &CatalogTableName,
    ) -> Result<Vec<ColumnInfo>> {
        self.require_doris()?;
        schema::describe_catalog_table(self.pool(), table).await
    }
    /// Begin a raw SQL Doris write transaction. Prepared statements with parameters
    /// are not supported by the target server, so this guard exposes execute only.
    pub async fn doris_transaction(&self) -> Result<DorisTransaction> {
        self.require_doris()?;
        DorisTransaction::begin(self.pool()).await
    }
}
