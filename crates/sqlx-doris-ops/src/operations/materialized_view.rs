use super::TableName;

use crate::{DorisPool, Error, Result};
use sqlx_core::row::Row;
use std::time::Duration;

/// Refresh strategy for an asynchronous Doris materialized view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RefreshMode {
    /// Let Doris choose the partitions that need refresh.
    Auto,
    /// Refresh the full materialized view.
    Complete,
}

/// Task status, retaining future server status names instead of guessing success.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskStatus {
    /// Submitted but not running.
    Pending,
    /// Currently executing.
    Running,
    /// Finished successfully; validate application freshness separately.
    Success,
    /// Failed; inspect the task's error message.
    Failed,
    /// Canceled by the server or an explicit request.
    Canceled,
    /// An unrecognized server value.
    Other(String),
}
impl TaskStatus {
    /// Whether Doris has reported a known terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Success | Self::Failed | Self::Canceled)
    }
    fn from_server(value: String) -> Self {
        match value.as_str() {
            "PENDING" => Self::Pending,
            "RUNNING" => Self::Running,
            "SUCCESS" => Self::Success,
            "FAILED" => Self::Failed,
            "CANCELED" => Self::Canceled,
            _ => Self::Other(value),
        }
    }
}

/// One task from `tasks('type'='mv')`; submission and completion are distinct.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedViewTask {
    /// Server task ID, unique independently of the view name; not chronological.
    pub id: i64,
    /// Actual server status, including failure/cancellation.
    pub status: TaskStatus,
    /// Original server diagnostic, empty on successful tasks.
    pub error_message: String,
}

/// Submit a refresh once. A successful return acknowledges submission only.
/// Use task IDs to observe completion; this method does not retry the write.
pub async fn refresh_materialized_view(
    pool: &DorisPool,
    view: &TableName,
    mode: RefreshMode,
) -> Result<()> {
    view.validate()?;
    let mode = match mode {
        RefreshMode::Auto => "AUTO",
        RefreshMode::Complete => "COMPLETE",
    };
    sqlx_core::raw_sql::raw_sql(sqlx_core::sql_str::AssertSqlSafe(format!(
        "REFRESH MATERIALIZED VIEW {view} {mode}"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Read this view's tasks by creation time, with unspecified order for ties.
/// IDs are not monotonic: identify new tasks by comparison with an earlier
/// ID set, not by numeric range or the first row. Concurrent submissions may
/// create multiple new tasks. Use a database-qualified name to separate views.
pub async fn materialized_view_tasks(
    pool: &DorisPool,
    view: &TableName,
) -> Result<Vec<MaterializedViewTask>> {
    view.validate()?;
    let rows = sqlx_core::query::query(concat!(
        "SELECT CAST(TaskId AS BIGINT) AS task_id,Status,ErrorMsg ",
        "FROM tasks('type'='mv') ",
        "WHERE MvName=? AND MvDatabaseName=COALESCE(?,DATABASE()) ",
        "ORDER BY CreateTime DESC"
    ))
    .bind(&view.table)
    .bind(&view.database)
    .fetch_all(pool)
    .await?;
    rows.into_iter()
        .map(|row| {
            Ok(MaterializedViewTask {
                id: row.try_get("task_id")?,
                status: TaskStatus::from_server(row.try_get("Status")?),
                error_message: row.try_get("ErrorMsg")?,
            })
        })
        .collect()
}

/// Request cancellation once. The task may race to completion; inspect its
/// eventual status rather than treating a successful command as Canceled.
/// The task ID must be positive; invalid IDs are rejected before pool acquisition.
pub async fn cancel_materialized_view_task(
    pool: &DorisPool,
    view: &TableName,
    task_id: i64,
) -> Result<()> {
    view.validate()?;
    validate_task_id(task_id)?;
    sqlx_core::raw_sql::raw_sql(sqlx_core::sql_str::AssertSqlSafe(format!(
        "CANCEL MATERIALIZED VIEW TASK {task_id} ON {view}"
    )))
    .execute(pool)
    .await?;
    Ok(())
}

/// Poll a specific submitted task until Success, Failed or Canceled. Failed
/// and Canceled are returned as task results, never converted into success.
/// A timeout does not cancel the server task or resubmit the refresh.
/// The task ID must be positive and the timeout must fit the clock range.
/// Invalid inputs are rejected before pool acquisition; intervals are clipped
/// to the deadline.
pub async fn wait_materialized_view_task(
    pool: &DorisPool,
    view: &TableName,
    task_id: i64,
    timeout: Duration,
    interval: Duration,
) -> Result<MaterializedViewTask> {
    validate_task_id(task_id)?;
    super::polling::poll_until(
        timeout,
        interval,
        || Error::TaskTimeout,
        || async {
            Ok(materialized_view_tasks(pool, view)
                .await?
                .into_iter()
                .find(|task| task.id == task_id && task.status.is_terminal()))
        },
    )
    .await
}

fn validate_task_id(task_id: i64) -> Result<()> {
    if task_id <= 0 {
        return Err(Error::InvalidInput("task ID must be positive".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancel_and_wait_reject_invalid_task_ids_without_acquiring_a_connection() {
        let pool = sqlx_doris_core::DorisPoolOptions::new()
            .acquire_timeout(Duration::from_millis(20))
            .connect_lazy("doris://root@127.0.0.1:1/test")
            .unwrap();
        let view = TableName::unqualified("example");
        for task_id in [0, -1] {
            assert!(matches!(
                cancel_materialized_view_task(&pool, &view, task_id).await,
                Err(Error::InvalidInput(_))
            ));
            assert!(matches!(
                wait_materialized_view_task(
                    &pool,
                    &view,
                    task_id,
                    Duration::from_secs(1),
                    Duration::from_millis(10),
                )
                .await,
                Err(Error::InvalidInput(_))
            ));
        }
        pool.close().await;
    }
}
