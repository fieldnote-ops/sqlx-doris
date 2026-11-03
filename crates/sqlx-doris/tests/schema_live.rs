mod common;

use chrono::NaiveDate;
use sqlx::{Executor, Row};
use sqlx_doris::operations::{Column, DataType as T, TableDefinition, TableModel};

#[tokio::test]
#[ignore = "requires Doris"]
async fn sequence_values_control_out_of_order_writes_and_partial_update_inheritance() {
    let db = common::client().await;
    let table = common::name();
    let definition = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("seq", T::BigInt).not_null())
        .column(Column::new("value", T::String))
        .column(Column::new("other", T::BigInt))
        .property("enable_unique_key_merge_on_write", "true")
        .property("function_column.sequence_col", "seq")
        .replication(1);
    sqlx_doris::operations::create_table(&db, &definition)
        .await
        .unwrap();
    let read = format!("SELECT seq,value,other FROM {table} WHERE id=?");
    for (seq, value, other) in [(20_i64, "new", 1_i64), (10, "late", 2), (30, "newest", 3)] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?,?,?)"
        )))
        .bind(1_i64)
        .bind(seq)
        .bind(value)
        .bind(other)
        .execute(&db)
        .await
        .unwrap();
        let actual: (i64, String, i64) = sqlx::query_as(sqlx::AssertSqlSafe(read.clone()))
            .bind(1_i64)
            .fetch_one(&db)
            .await
            .unwrap();
        let expected = if seq == 10 {
            (20, "new".into(), 1)
        } else {
            (seq, value.into(), other)
        };
        assert_eq!(actual, expected, "after full write with sequence {seq}");
    }
    let row: (i64, String, i64) = sqlx::query_as(sqlx::AssertSqlSafe(read.clone()))
        .bind(1_i64)
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(row, (30, "newest".into(), 3));
    let missing = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table}(id,value,other) VALUES (?,?,?)"
    )))
    .bind(1_i64)
    .bind("missing sequence")
    .bind(99_i64)
    .execute(&db)
    .await
    .unwrap_err();
    assert!(
        missing.to_string().to_lowercase().contains("sequence"),
        "{missing}"
    );
    let mut connection = db.acquire().await.unwrap();
    sqlx::raw_sql("SET show_hidden_columns=true")
        .execute(&mut *connection)
        .await
        .unwrap();
    let columns = sqlx::raw_sql(sqlx::AssertSqlSafe(format!("DESC {table}")))
        .fetch_all(&mut *connection)
        .await
        .unwrap();
    assert!(
        columns
            .iter()
            .any(|row| row.get::<String, _>("Field") == "__DORIS_SEQUENCE_COL__")
    );
    sqlx::raw_sql("SET show_hidden_columns=false")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::raw_sql("SET enable_unique_key_partial_update=true")
        .execute(&mut *connection)
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table}(id,value) VALUES (?,?)"
    )))
    .bind(1_i64)
    .bind("partial")
    .execute(&mut *connection)
    .await
    .unwrap();
    let inherited: (i64, String, i64) = sqlx::query_as(sqlx::AssertSqlSafe(read.clone()))
        .bind(1_i64)
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert_eq!(inherited, (30, "partial".into(), 3));
    for (seq, value) in [(25_i64, "stale partial"), (40, "fresh partial")] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table}(id,seq,value) VALUES (?,?,?)"
        )))
        .bind(1_i64)
        .bind(seq)
        .bind(value)
        .execute(&mut *connection)
        .await
        .unwrap();
        let actual: (i64, String, i64) = sqlx::query_as(sqlx::AssertSqlSafe(read.clone()))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        let expected = if seq == 25 {
            (30, "partial".into(), 3)
        } else {
            (40, "fresh partial".into(), 3)
        };
        assert_eq!(actual, expected, "after partial write with sequence {seq}");
    }
    let row: (i64, String, i64) = sqlx::query_as(sqlx::AssertSqlSafe(read))
        .bind(1_i64)
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    assert_eq!(row, (40, "fresh partial".into(), 3));
    sqlx::raw_sql("SET enable_unique_key_partial_update=false")
        .execute(&mut *connection)
        .await
        .unwrap();
    drop(connection);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn range_partition_ddl_routing_filter_errors_and_metadata_are_consistent() {
    let db = common::client().await;
    let table = common::name();
    db.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table} (d DATE NOT NULL,id BIGINT NOT NULL,value STRING) \
        DUPLICATE KEY(d,id) PARTITION BY RANGE(d) (PARTITION p202601 VALUES \
        [('2026-01-01'),('2026-02-01')),PARTITION p202602 VALUES [('2026-02-01'),\
        ('2026-03-01'))) DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES \
        ('replication_num'='1')"
    )))
    .await
    .unwrap();
    let insert = format!("INSERT INTO {table} VALUES (?,?,?)");
    for (id, month) in [(1_i64, 1), (2, 2)] {
        sqlx::query(sqlx::AssertSqlSafe(insert.clone()))
            .bind(NaiveDate::from_ymd_opt(2026, month, 15).unwrap())
            .bind(id)
            .bind("bound")
            .execute(&db)
            .await
            .unwrap();
    }
    let partitions = sqlx::raw_sql(sqlx::AssertSqlSafe(format!("SHOW PARTITIONS FROM {table}")))
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(partitions.len(), 2);
    for partition in partitions {
        assert_eq!(partition.get::<String, _>("State"), "NORMAL");
        assert_eq!(partition.get::<String, _>("Buckets"), "2");
    }
    let mismatched = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} PARTITION(p202601) VALUES (?,?,?)"
    )))
    .bind(NaiveDate::from_ymd_opt(2026, 2, 20).unwrap())
    .bind(3_i64)
    .bind("wrong target")
    .execute(&db)
    .await
    .unwrap_err();
    assert!(
        mismatched.to_string().to_lowercase().contains("filter"),
        "{mismatched}"
    );
    let filtered: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {table} WHERE id=?"
    )))
    .bind(3_i64)
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(filtered, 0, "a failed targeted insert must not leave a row");
    db.execute(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {table} ADD PARTITION p202603 VALUES [('2026-03-01'),('2026-04-01'))"
    )))
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(insert))
        .bind(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap())
        .bind(4_i64)
        .bind("new partition")
        .execute(&db)
        .await
        .unwrap();
    let march: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {table} PARTITION(p202603)"
    )))
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(march, 1);
    db.execute(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {table} DROP PARTITION p202602 FORCE"
    )))
    .await
    .unwrap();
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(count, 2, "the failed targeted insert did not leave a row");
    common::drop_table(&db, &table).await;
    db.close().await;
}
