mod common;

use sqlx_doris::{
    Backend, Error, GroupCommit, PoolBuilder,
    operations::{
        Aggregate as A, AggregateArgument, Column, DataType as T, TableDefinition, TableModel,
        TableName,
    },
    sqlx::{self, Executor, Row, ValueRef},
};
use std::{env, time::Duration};

fn definition(name: TableName, model: TableModel) -> TableDefinition {
    TableDefinition::new(name, model, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("value", T::BigInt))
        .replication(1)
}

async fn count(db: &sqlx_doris::Client, table: &TableName) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(db.pool())
        .await
        .unwrap()
}

#[derive(Debug, ::sqlx::FromRow, PartialEq)]
struct AggregateRow {
    id: i64,
    total: Option<i64>,
    low: Option<i64>,
    high: Option<i64>,
    latest: Option<String>,
    remembered: Option<String>,
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn aggregate_model_merges_batches_and_retains_declared_null_semantics_and_metadata() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let schema = TableDefinition::new(table.clone(), TableModel::Aggregate, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("total", T::BigInt).aggregate(A::Sum))
        .column(Column::new("low", T::BigInt).aggregate(A::Min))
        .column(Column::new("high", T::BigInt).aggregate(A::Max))
        .column(Column::new("latest", T::String).aggregate(A::Replace))
        .column(Column::new("remembered", T::String).aggregate(A::ReplaceIfNotNull))
        .replication(1);
    db.create_table(&schema).await.unwrap();
    // Separate committed batches make REPLACE ordering deterministic.
    for (total, latest, remembered) in [
        (Some(10_i64), Some("first"), Some("keep")),
        (Some(3), Some("second"), None),
        (None, None, Some("last")),
    ] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?,?,?,?,?)"
        )))
        .bind(1_i64)
        .bind(total)
        .bind(total)
        .bind(total)
        .bind(latest)
        .bind(remembered)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let actual: AggregateRow = schema
        .select_builder()
        .unwrap()
        .build_query_as()
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        actual,
        AggregateRow {
            id: 1,
            total: Some(13),
            low: Some(3),
            high: Some(10),
            latest: None,
            remembered: Some("last".into()),
        }
    );
    let metadata = db.describe_table(&table).await.unwrap();
    assert_eq!(metadata.len(), 6);
    assert!(metadata[0].is_key);
    assert!(!metadata[0].nullable);
    for (column, expected) in
        metadata[1..]
            .iter()
            .zip(["SUM", "MIN", "MAX", "REPLACE", "REPLACE_IF_NOT_NULL"])
    {
        assert!(!column.is_key);
        assert!(column.nullable);
        assert!(
            column.extra.as_deref().unwrap_or("").contains(expected),
            "{column:?}"
        );
    }
    // The builder leaves final model/type constraints to the real server.
    let invalid = common::name();
    let error = db
        .create_table(
            &TableDefinition::new(invalid, TableModel::Aggregate, ["id"])
                .column(Column::new("id", T::BigInt).not_null())
                .column(Column::new("invalid_sum", T::String).aggregate(A::Sum))
                .replication(1),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Database(sqlx::Error::Database(_))),
        "{error:?}"
    );
    assert_eq!(count(&db, &table).await, 1);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn opaque_aggregation_columns_are_constructed_merged_and_read_via_scalar_functions() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Aggregate, ["id"])
            .column(Column::new("id", T::BigInt).not_null())
            .column(
                Column::new("bits", T::Bitmap)
                    .not_null()
                    .aggregate(A::BitmapUnion),
            )
            .column(
                Column::new("users", T::Hll)
                    .not_null()
                    .aggregate(A::HllUnion),
            )
            .column(
                Column::new("latencies", T::QuantileState)
                    .not_null()
                    .aggregate(A::QuantileUnion),
            )
            .replication(1),
    )
    .await
    .unwrap();
    for (bit, user, latency) in [(1_i64, "alice", 10_f64), (2, "bob", 20.), (1, "alice", 30.)] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,TO_BITMAP(?),HLL_HASH(?),TO_QUANTILE_STATE(?,2048))"
        )))
        .bind(1_i64)
        .bind(bit)
        .bind(user)
        .bind(latency)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let actual: (i64, i64, f64, f64, f64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT BITMAP_COUNT(bits),HLL_CARDINALITY(users),QUANTILE_PERCENT(latencies,\
            0.0),QUANTILE_PERCENT(latencies,0.5),QUANTILE_PERCENT(latencies,1.0) FROM \
            {table}"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!((actual.0, actual.1), (2, 2));
    for (value, expected) in [(actual.2, 10.), (actual.3, 20.), (actual.4, 30.)] {
        assert!((value - expected).abs() < 1e-9, "{actual:?}");
    }
    let raw = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "SELECT bits,users,latencies FROM {table}"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    // Default result serialization deliberately hides opaque state objects.
    for index in 0..3 {
        assert!(
            raw.try_get_raw(index).unwrap().is_null(),
            "state column {index}"
        );
    }
    let binary = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT bits,users,latencies FROM {table} WHERE id=?"
    )))
    .bind(1_i64)
    .fetch_one(db.pool())
    .await
    .unwrap();
    for index in 0..3 {
        assert!(binary.try_get_raw(index).unwrap().is_null());
    }
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn aggregate_state_signatures_support_bound_construction_merge_union_and_reject_other_functions()
 {
    let db = PoolBuilder::new(Backend::Doris)
        .max_connections(2)
        .after_connect(|connection| {
            Box::pin(async move {
                connection.execute("SET enable_agg_state=true").await?;
                Ok(())
            })
        })
        .connect(&env::var("DORIS_TEST_URL").unwrap())
        .await
        .unwrap();
    let table = common::name();
    let state = T::AggState {
        function: "max_by".into(),
        arguments: vec![
            AggregateArgument::new(T::Int).not_null(),
            AggregateArgument::new(T::Int),
        ],
    };
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Aggregate, ["id"])
            .column(Column::new("id", T::BigInt).not_null())
            .column(Column::new("state_value", state).aggregate(A::Generic))
            .replication(1),
    )
    .await
    .unwrap();
    for (key, value, order) in [(1_i64, 3_i32, 1_i32), (1, 2, 2), (2, 1, 3)] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,MAX_BY_STATE(CAST(? AS INT),CAST(? AS INT)))"
        )))
        .bind(key)
        .bind(value)
        .bind(order)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let rows: Vec<(i64, i32)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,MAX_BY_MERGE(state_value) FROM {table} GROUP BY id ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(rows, [(1, 2), (2, 1)]);
    let combined: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT MAX_BY_MERGE(merged) FROM (SELECT MAX_BY_UNION(state_value) AS \
            merged FROM {table} GROUP BY id) t"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(combined, 1);
    // The planner can coerce an input width within the same state function.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?,MAX_BY_STATE(CAST(? AS BIGINT),CAST(? AS INT)))"
    )))
    .bind(3_i64)
    .bind(5_i64)
    .bind(4_i32)
    .execute(db.pool())
    .await
    .unwrap();
    let coerced: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT MAX_BY_MERGE(state_value) FROM {table} WHERE id=3"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(coerced, 5);
    let mismatch = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?,MIN_STATE(CAST(? AS INT)))"
    )))
    .bind(4_i64)
    .bind(9_i32)
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(matches!(mismatch, sqlx::Error::Database(_)), "{mismatch:?}");
    assert_eq!(count(&db, &table).await, 3);
    let metadata = db.describe_table(&table).await.unwrap();
    assert!(
        metadata[1]
            .data_type
            .to_ascii_lowercase()
            .contains("agg_state"),
        "{metadata:?}"
    );
    assert!(
        metadata[1]
            .data_type
            .to_ascii_lowercase()
            .contains("max_by"),
        "{metadata:?}"
    );
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn synchronous_group_commit_sql_is_visible_on_return_and_failure_does_not_replay_batches() {
    let db = PoolBuilder::new(Backend::Doris)
        .max_connections(2)
        .group_commit(GroupCommit::Sync)
        .connect(&env::var("DORIS_TEST_URL").unwrap())
        .await
        .unwrap();
    let table = common::name();
    db.create_table(
        &definition(table.clone(), TableModel::Duplicate)
            .property("group_commit_interval_ms", "1000"),
    )
    .await
    .unwrap();
    let statement = format!("INSERT INTO {table} VALUES (?,?)");
    let (left, right) = tokio::join!(
        sqlx::query(sqlx::AssertSqlSafe(statement.clone()))
            .bind(1_i64)
            .bind(10_i64)
            .execute(db.pool()),
        sqlx::query(sqlx::AssertSqlSafe(statement.clone()))
            .bind(2_i64)
            .bind(20_i64)
            .execute(db.pool()),
    );
    left.unwrap();
    right.unwrap();
    // This is intentionally an immediate read, without visibility polling.
    assert_eq!(count(&db, &table).await, 2);
    let error = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?,CAST(? AS BIGINT))"
    )))
    .bind(3_i64)
    .bind("not-an-integer")
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(matches!(error, sqlx::Error::Database(_)), "{error:?}");
    assert_eq!(count(&db, &table).await, 2);
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .bind(4_i64)
        .bind(40_i64)
        .execute(db.pool())
        .await
        .unwrap();
    assert_eq!(count(&db, &table).await, 3);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn values_transaction_rejects_queries_and_ddl_and_preserves_successes_after_statement_errors()
{
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(&definition(table.clone(), TableModel::Duplicate))
        .await
        .unwrap();
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (1,10)"
    )))
    .await
    .unwrap();
    for sql in [
        "SELECT 42".to_owned(),
        format!("CREATE TABLE {} LIKE {table}", common::name()),
        format!("INSERT INTO {table} (missing) VALUES (1)"),
    ] {
        let error = tx.execute(sqlx::AssertSqlSafe(sql)).await.unwrap_err();
        assert!(
            matches!(error, Error::Database(sqlx::Error::Database(_))),
            "{error:?}"
        );
    }
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (2,20)"
    )))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(count(&db, &table).await, 2);
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (3,30)"
    )))
    .await
    .unwrap();
    // Repeated BEGIN is a server no-op, not a savepoint or nested transaction.
    tx.execute("BEGIN").await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (4,40)"
    )))
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(count(&db, &table).await, 2);
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (5,50)"
    )))
    .await
    .unwrap();
    tx.execute("this is invalid SQL").await.unwrap_err();
    tx.rollback().await.unwrap();
    assert_eq!(count(&db, &table).await, 2);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn insert_select_transactions_span_tables_without_reading_their_own_uncommitted_rows() {
    let db = common::client(Backend::Doris).await;
    let source = common::name();
    let first = common::name();
    let second = common::name();
    for name in [&source, &first, &second] {
        db.create_table(&definition(name.clone(), TableModel::Duplicate))
            .await
            .unwrap();
    }
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {source} VALUES (1,10),(2,20)"
        )))
        .await
        .unwrap();
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {first} SELECT * FROM {source}"
    )))
    .await
    .unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {second} SELECT * FROM {first}"
    )))
    .await
    .unwrap();
    let error = tx
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {first} VALUES (3,30)"
        )))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("insert into values and insert into select"),
        "{error:?}"
    );
    tx.commit().await.unwrap();
    assert_eq!(count(&db, &first).await, 2);
    assert_eq!(
        count(&db, &second).await,
        0,
        "uncommitted writes must not be read"
    );
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {first} SELECT * FROM {source}"
    )))
    .await
    .unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {second} SELECT * FROM {source}"
    )))
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(count(&db, &first).await, 2);
    assert_eq!(count(&db, &second).await, 0);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT 42")
            .fetch_one(db.pool())
            .await
            .unwrap(),
        42
    );
    for table in [&source, &first, &second] {
        common::drop_table(&db, table).await;
    }
    tokio::time::timeout(Duration::from_secs(5), db.close())
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn filtered_insert_select_rolls_back_only_the_failed_statement_before_explicit_commit() {
    let db = common::client(Backend::Doris).await;
    let source = common::name();
    let target = common::name();
    for (table, length) in [(&source, 64), (&target, 3)] {
        db.create_table(
            &TableDefinition::new(table.clone(), TableModel::Duplicate, ["id"])
                .column(Column::new("id", T::BigInt).not_null())
                .column(Column::new("payload", T::VarChar(length)))
                .replication(1),
        )
        .await
        .unwrap();
    }
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {source} VALUES (0,'a'),(1,'bb'),(2,'overlong')"
        )))
        .await
        .unwrap();
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {target} SELECT * FROM {source} WHERE id=0"
    )))
    .await
    .unwrap();
    let error = tx
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {target} SELECT * FROM {source} WHERE id>0"
        )))
        .await
        .unwrap_err();
    assert!(
        matches!(error, Error::Database(sqlx::Error::Database(_))),
        "{error:?}"
    );
    assert!(
        error.to_string().to_ascii_lowercase().contains("filter"),
        "{error:?}"
    );
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {target} SELECT * FROM {source} WHERE id=1"
    )))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {target} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        ids,
        [0, 1],
        "the valid row in the failed batch must not leak through"
    );
    for table in [&source, &target] {
        common::drop_table(&db, table).await;
    }
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn update_delete_transactions_follow_mow_light_delete_settings_and_rollback_across_tables() {
    let db = common::client(Backend::Doris).await;
    for light_delete in [false, true] {
        let first = common::name();
        let second = common::name();
        for table in [&first, &second] {
            db.create_table(
                &definition(table.clone(), TableModel::Unique)
                    .property("enable_unique_key_merge_on_write", "true")
                    .property("enable_mow_light_delete", light_delete.to_string()),
            )
            .await
            .unwrap();
        }
        for (table, multiplier) in [(&first, 10), (&second, 100)] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?),(?,?)"
            )))
            .bind(1_i64)
            .bind(i64::from(multiplier))
            .bind(2_i64)
            .bind(i64::from(2 * multiplier))
            .execute(db.pool())
            .await
            .unwrap();
        }
        let mut tx = db.doris_transaction().await.unwrap();
        tx.execute(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {first} WHERE id=1"
        )))
        .await
        .unwrap();
        tx.execute(sqlx::AssertSqlSafe(format!(
            "UPDATE {second} SET value=value+1 WHERE id=2"
        )))
        .await
        .unwrap();
        tx.execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {first} SELECT * FROM {second} WHERE id=1"
        )))
        .await
        .unwrap();
        tx.commit().await.unwrap();
        for (table, expected) in [
            (&first, vec![(1_i64, 100_i64), (2, 20)]),
            (&second, vec![(1, 100), (2, 201)]),
        ] {
            let actual: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id,value FROM {table} ORDER BY id"
            )))
            .fetch_all(db.pool())
            .await
            .unwrap();
            assert_eq!(actual, expected);
        }
        let mut tx = db.doris_transaction().await.unwrap();
        tx.execute(sqlx::AssertSqlSafe(format!(
            "UPDATE {first} SET value=999 WHERE id=2"
        )))
        .await
        .unwrap();
        tx.execute(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {second} WHERE id=1"
        )))
        .await
        .unwrap();
        tx.rollback().await.unwrap();
        let mut tx = db.doris_transaction().await.unwrap();
        tx.execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {first} SELECT * FROM {second}"
        )))
        .await
        .unwrap();
        let result = tx
            .execute(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {first} WHERE id=1"
            )))
            .await;
        if light_delete {
            let error = result.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("Can not delete because there is a insert operation"),
                "{error:?}"
            );
        } else {
            result.unwrap();
        }
        tx.rollback().await.unwrap();
        for (table, expected) in [
            (&first, vec![(1_i64, 100_i64), (2, 20)]),
            (&second, vec![(1, 100), (2, 201)]),
        ] {
            let actual: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id,value FROM {table} ORDER BY id"
            )))
            .fetch_all(db.pool())
            .await
            .unwrap();
            assert_eq!(actual, expected, "rollback must undo both table changes");
            common::drop_table(&db, table).await;
        }
    }
    db.close().await;
}
