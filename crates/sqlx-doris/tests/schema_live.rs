mod common;

use chrono::NaiveDate;
use sqlx_doris::{
    Backend,
    operations::{self, Column, DataType as T, TableDefinition, TableModel, TableName},
    sqlx::{self, Connection, Executor, Row},
};
use std::time::Duration;

async fn finish_alter(db: &sqlx_doris::Client, table: &TableName) {
    let sql = format!(
        "SHOW ALTER TABLE COLUMN FROM `sqlx_doris_test` WHERE TableName='{}'",
        table.table
    );
    operations::wait_until(
        Duration::from_secs(120),
        Duration::from_millis(100),
        || async {
            let rows = sqlx::raw_sql(sqlx::AssertSqlSafe(sql.clone()))
                .fetch_all(db.pool())
                .await?;
            let mut finished = !rows.is_empty();
            for row in rows {
                let state: String = row.try_get("State")?;
                if state == "CANCELLED" {
                    return Err(sqlx::Error::Protocol(format!(
                        "schema job cancelled: {}",
                        row.try_get::<String, _>("Msg")?
                    )));
                }
                finished &= state == "FINISHED";
            }
            Ok(finished)
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn sequence_values_control_out_of_order_writes_and_partial_update_inheritance() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let definition = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("seq", T::BigInt).not_null())
        .column(Column::new("value", T::String))
        .column(Column::new("other", T::BigInt))
        .property("enable_unique_key_merge_on_write", "true")
        .property("function_column.sequence_col", "seq")
        .replication(1);
    db.create_table(&definition).await.unwrap();
    let read = format!("SELECT seq,value,other FROM {table} WHERE id=?");
    for (seq, value, other) in [(20_i64, "new", 1_i64), (10, "late", 2), (30, "newest", 3)] {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?,?,?)"
        )))
        .bind(1_i64)
        .bind(seq)
        .bind(value)
        .bind(other)
        .execute(db.pool())
        .await
        .unwrap();
        let actual: (i64, String, i64) = sqlx::query_as(sqlx::AssertSqlSafe(read.clone()))
            .bind(1_i64)
            .fetch_one(db.pool())
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
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(row, (30, "newest".into(), 3));
    let missing = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table}(id,value,other) VALUES (?,?,?)"
    )))
    .bind(1_i64)
    .bind("missing sequence")
    .bind(99_i64)
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        missing.to_string().to_lowercase().contains("sequence"),
        "{missing}"
    );
    let mut connection = db.pool().acquire().await.unwrap();
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
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.pool().execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table} (d DATE NOT NULL,id BIGINT NOT NULL,value STRING) DUPLICATE KEY(d,id) PARTITION BY RANGE(d) (PARTITION p202601 VALUES [('2026-01-01'),('2026-02-01')),PARTITION p202602 VALUES [('2026-02-01'),('2026-03-01'))) DISTRIBUTED BY HASH(id) BUCKETS 2 PROPERTIES ('replication_num'='1')"))).await.unwrap();
    let insert = format!("INSERT INTO {table} VALUES (?,?,?)");
    for (id, month) in [(1_i64, 1), (2, 2)] {
        sqlx::query(sqlx::AssertSqlSafe(insert.clone()))
            .bind(NaiveDate::from_ymd_opt(2026, month, 15).unwrap())
            .bind(id)
            .bind("bound")
            .execute(db.pool())
            .await
            .unwrap();
    }
    let partitions = sqlx::raw_sql(sqlx::AssertSqlSafe(format!("SHOW PARTITIONS FROM {table}")))
        .fetch_all(db.pool())
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
    .execute(db.pool())
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
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(filtered, 0, "a failed targeted insert must not leave a row");
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {table} ADD PARTITION p202603 VALUES [('2026-03-01'),('2026-04-01'))"
        )))
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(insert))
        .bind(NaiveDate::from_ymd_opt(2026, 3, 1).unwrap())
        .bind(4_i64)
        .bind("new partition")
        .execute(db.pool())
        .await
        .unwrap();
    let march: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {table} PARTITION(p202603)"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(march, 1);
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {table} DROP PARTITION p202602 FORCE"
        )))
        .await
        .unwrap();
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 2, "the failed targeted insert did not leave a row");
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn cached_prepared_results_refresh_after_type_width_and_column_count_changes() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        let ddl = if backend == Backend::MySql {
            format!("CREATE TABLE {table}(id BIGINT PRIMARY KEY,value INT NOT NULL)")
        } else {
            format!(
                "CREATE TABLE {table}(id BIGINT NOT NULL,value INT NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1','light_schema_change'='false')"
            )
        };
        db.pool().execute(sqlx::AssertSqlSafe(ddl)).await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(1_i64)
        .bind(7_i32)
        .execute(db.pool())
        .await
        .unwrap();
        let select = format!("SELECT * FROM {table} WHERE id=?");
        let mut connection = db.pool().acquire().await.unwrap();
        let first = sqlx::query(sqlx::AssertSqlSafe(select.clone()))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(first.get::<i64, _>("value"), 7);
        assert_eq!(first.len(), 2);
        assert!(connection.cached_statements_size() > 0);
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "ALTER TABLE {table} MODIFY COLUMN value BIGINT NOT NULL"
            )))
            .await
            .unwrap();
        if backend == Backend::Doris {
            finish_alter(&db, &table).await;
        }
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {table} SET value=? WHERE id=?"
        )))
        .bind(5_000_000_000_i64)
        .bind(1_i64)
        .execute(db.pool())
        .await
        .unwrap();
        let row = sqlx::query(sqlx::AssertSqlSafe(select.clone()))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(
            row.get::<i64, _>("value"),
            5_000_000_000,
            "{backend:?}: cached result must use BIGINT bytes"
        );
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "ALTER TABLE {table} ADD COLUMN note VARCHAR(24) DEFAULT 'fresh'"
            )))
            .await
            .unwrap();
        if backend == Backend::Doris {
            finish_alter(&db, &table).await;
        }
        let row = sqlx::query(sqlx::AssertSqlSafe(select.clone()))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(row.len(), 3, "{backend:?}");
        assert_eq!(row.get::<String, _>("note"), "fresh");
        let metadata = db.describe_table(&table).await.unwrap();
        assert_eq!(metadata.len(), 3);
        assert!(
            metadata[1]
                .data_type
                .to_ascii_uppercase()
                .starts_with("BIGINT")
        );
        assert_eq!(metadata[2].name, "note");
        assert!(metadata[2].nullable);
        connection.clear_cached_statements().await.unwrap();
        assert_eq!(connection.cached_statements_size(), 0);
        let row = sqlx::query(sqlx::AssertSqlSafe(select.clone()))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(row.get::<i64, _>("value"), 5_000_000_000);
        assert_eq!(row.get::<String, _>("note"), "fresh");
        // Keep the same connection and SQL text through DROP and recreation.
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
            .await
            .unwrap();
        let missing = sqlx::query(sqlx::AssertSqlSafe(select.clone()))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap_err();
        assert!(matches!(missing, sqlx::Error::Database(_)), "{missing}");
        let recreate = if backend == Backend::MySql {
            format!("CREATE TABLE {table}(id BIGINT PRIMARY KEY,value VARCHAR(64),note INT)")
        } else {
            format!(
                "CREATE TABLE {table}(id BIGINT NOT NULL,value VARCHAR(64),note INT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')"
            )
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(recreate))
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?,?)"
        )))
        .bind(1_i64)
        .bind("recreated")
        .bind(9_i32)
        .execute(db.pool())
        .await
        .unwrap();
        let row = sqlx::query(sqlx::AssertSqlSafe(select))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("value"), "recreated", "{backend:?}");
        assert_eq!(row.get::<i32, _>("note"), 9);
        drop(connection);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn explicit_reprepare_or_cache_clear_reads_the_recreated_table() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        let suffix = if backend == Backend::MySql {
            String::new()
        } else {
            " UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1')"
                .into()
        };
        let id = if backend == Backend::MySql {
            "id BIGINT PRIMARY KEY"
        } else {
            "id BIGINT NOT NULL"
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {table}({id},value BIGINT){suffix}"
            )))
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(1_i64)
        .bind(123_i64)
        .execute(db.pool())
        .await
        .unwrap();
        let select = format!("SELECT value FROM {table} WHERE id=?");
        let mut connection = db.pool().acquire().await.unwrap();
        let old: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(select.clone()))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(old, 123);
        assert!(connection.cached_statements_size() > 0);
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
            .await
            .unwrap();
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {table}({id},value VARCHAR(64)){suffix}"
            )))
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(1_i64)
        .bind("fresh incarnation")
        // The fixture write must also prepare against the new incarnation.
        .persistent(false)
        .execute(db.pool())
        .await
        .unwrap();
        let fresh: String = sqlx::query_scalar(sqlx::AssertSqlSafe(select.clone()))
            .bind(1_i64)
            .persistent(false)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(fresh, "fresh incarnation");
        assert!(
            connection.cached_statements_size() > 0,
            "the old cached entry was not used by persistent(false)"
        );
        connection.clear_cached_statements().await.unwrap();
        assert_eq!(connection.cached_statements_size(), 0);
        let fresh: String = sqlx::query_scalar(sqlx::AssertSqlSafe(select))
            .bind(1_i64)
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(fresh, "fresh incarnation");
        drop(connection);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}
