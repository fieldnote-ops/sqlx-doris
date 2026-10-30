mod common;

use sqlx_doris::{
    Backend, Client, Error,
    operations::{MaterializedViewTask, RefreshMode, TableName, TaskStatus},
    sqlx,
};
use std::{collections::HashSet, time::Duration};

async fn execute(db: &Client, sql: String) {
    sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
        .execute(db.pool())
        .await
        .unwrap();
}
async fn new_task(db: &Client, view: &TableName, known: &HashSet<i64>) -> MaterializedViewTask {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(t) = db
            .materialized_view_tasks(view)
            .await
            .unwrap()
            .into_iter()
            .find(|t| !known.contains(&t.id))
        {
            return t;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no new task for {view}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
async fn create_base(db: &Client) -> TableName {
    let t = common::name();
    execute(
        db,
        format!(
            "CREATE TABLE {t}(id INT,v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) \
        BUCKETS 1 PROPERTIES('replication_num'='1')"
        ),
    )
    .await;
    execute(db, format!("INSERT INTO {t} VALUES(1,2),(1,3)")).await;
    t
}
async fn create_view(db: &Client, base: &TableName, slow: bool) -> TableName {
    let mv = common::name();
    let value = if slow {
        "SUM(v)+SUM(SLEEP(IF(id>0,20,0)))"
    } else {
        "SUM(v)"
    };
    execute(
        db,
        format!(
            "CREATE MATERIALIZED VIEW {mv} BUILD DEFERRED REFRESH COMPLETE ON MANUAL \
        DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1') AS \
        SELECT id,{value} AS total FROM {base} GROUP BY id"
        ),
    )
    .await;
    mv
}
async fn wait(db: &Client, mv: &TableName, id: i64) -> MaterializedViewTask {
    let result = db
        .wait_materialized_view_task(mv, id, Duration::from_secs(45), Duration::from_millis(100))
        .await
        .unwrap();
    println!("{mv}: {result:?}");
    result
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn deferred_view_submission_completion_and_stale_data_are_separate_contracts() {
    let db = common::client(Backend::Doris).await;
    let base = create_base(&db).await;
    let mv = create_view(&db, &base, false).await;
    assert!(db.materialized_view_tasks(&mv).await.unwrap().is_empty());
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {mv}")))
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    db.refresh_materialized_view(&mv, RefreshMode::Complete)
        .await
        .unwrap();
    let first = new_task(&db, &mv, &HashSet::new()).await;
    assert_eq!(wait(&db, &mv, first.id).await.status, TaskStatus::Success);
    let value: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT total FROM {mv} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(value, 5);
    execute(&db, format!("INSERT INTO {base} VALUES(1,7)")).await;
    let stale: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT total FROM {mv} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(stale, 5);
    // AbstractJob.generateTaskId adds randomness; a later task can have a
    // smaller ID. Compare all IDs observed before this submission instead.
    let known = db
        .materialized_view_tasks(&mv)
        .await
        .unwrap()
        .into_iter()
        .map(|task| task.id)
        .collect();
    db.refresh_materialized_view(&mv, RefreshMode::Auto)
        .await
        .unwrap();
    let next = new_task(&db, &mv, &known).await;
    assert_eq!(wait(&db, &mv, next.id).await.status, TaskStatus::Success);
    let fresh: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT total FROM {mv} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(fresh, 12);
    execute(&db, format!("DROP MATERIALIZED VIEW {mv}")).await;
    common::drop_table(&db, &base).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn refresh_of_a_view_with_a_missing_base_is_an_observable_failed_task() {
    let db = common::client(Backend::Doris).await;
    let base = create_base(&db).await;
    let mv = create_view(&db, &base, false).await;
    common::drop_table(&db, &base).await;
    db.refresh_materialized_view(&mv, RefreshMode::Complete)
        .await
        .unwrap();
    let task = new_task(&db, &mv, &HashSet::new()).await;
    let result = wait(&db, &mv, task.id).await;
    assert_eq!(result.status, TaskStatus::Failed);
    assert!(!result.error_message.is_empty());
    execute(&db, format!("DROP MATERIALIZED VIEW {mv}")).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn client_wait_timeout_does_not_cancel_task_and_explicit_cancel_is_observed() {
    let db = common::client(Backend::Doris).await;
    let base = create_base(&db).await;
    let mv = create_view(&db, &base, true).await;
    db.refresh_materialized_view(&mv, RefreshMode::Complete)
        .await
        .unwrap();
    let task = new_task(&db, &mv, &HashSet::new()).await;
    let timeout = db
        .wait_materialized_view_task(
            &mv,
            task.id,
            Duration::from_millis(20),
            Duration::from_millis(5),
        )
        .await
        .unwrap_err();
    assert!(matches!(timeout, Error::TaskTimeout));
    let still = db.materialized_view_tasks(&mv).await.unwrap();
    assert!(
        still
            .iter()
            .any(|t| t.id == task.id && !t.status.is_terminal())
    );
    db.cancel_materialized_view_task(&mv, task.id)
        .await
        .unwrap();
    assert_eq!(wait(&db, &mv, task.id).await.status, TaskStatus::Canceled);
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {mv}")))
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(n, 0);
    execute(&db, format!("DROP MATERIALIZED VIEW {mv}")).await;
    common::drop_table(&db, &base).await;
    db.close().await;
}
