//! Optional schema/MV helpers and application visibility predicates.

mod materialized_view;
mod polling;
mod schema;
mod value;
mod variant;

use crate::DorisPool;
pub use crate::{Error, Result};
use std::{future::Future, time::Duration};

pub use materialized_view::{
    MaterializedViewTask, RefreshMode, TaskStatus, cancel_materialized_view_task,
    materialized_view_tasks, refresh_materialized_view, wait_materialized_view_task,
};
pub use schema::{
    Aggregate, AggregateArgument, CatalogTableName, Column, ColumnInfo, DataType, TableDefinition,
    TableModel, TableName, describe_catalog_table, describe_table, project,
};
pub use value::push_complex_value;
pub use variant::push_variant_path;

/// Poll an application read predicate until visible. Errors are returned directly;
/// this helper never resubmits the original write.
///
/// A timeout beyond the clock range returns `Error::InvalidInput` before the
/// predicate is called. Intervals are clipped to the deadline; no subsequent
/// observation starts after it. Zero timeout permits one immediately ready result.
pub async fn wait_until<F, Fut>(timeout: Duration, interval: Duration, mut visible: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<bool, sqlx_core::Error>>,
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

/// Create an OLAP table with an explicitly selected Doris model.
pub async fn create_table(pool: &DorisPool, definition: &TableDefinition) -> Result<()> {
    use sqlx_core::executor::Executor;
    pool.execute(sqlx_core::sql_str::AssertSqlSafe(definition.to_sql()?))
        .await?;
    Ok(())
}
