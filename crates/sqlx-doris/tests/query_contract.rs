mod common;

use futures_util::TryStreamExt;
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::{Column, DataType as T, TableDefinition, TableModel, TableName},
    sqlx::{
        self, Connection, Executor, MySql, QueryBuilder, Row, mysql::MySqlConnectOptions,
    },
};
use std::{env, str::FromStr, time::Duration};

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris with the shared SQLx patch"]
async fn zero_parameter_prepared_writes_and_cache_reuse_work_with_native_query_on_both_backends() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        if backend == Backend::Doris {
            db.create_table(
                &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                    .column(Column::new("id", T::BigInt).not_null())
                    .column(Column::new("value", T::BigInt))
                    .property("enable_unique_key_merge_on_write", "true")
                    .replication(1),
            )
            .await
            .unwrap();
        } else {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {table}(id BIGINT PRIMARY KEY,value BIGINT)"
            )))
            .execute(db.pool())
            .await
            .unwrap();
        }
        let mut conn = db.pool().acquire().await.unwrap();
        let inserted = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (1,3)"
        )))
        .execute(&mut *conn)
        .await
        .unwrap();
        assert_eq!(inserted.rows_affected(), 1);
        let update = format!("UPDATE {table} SET value=value+1 WHERE id=1");
        for persistent in [true, true, false] {
            let result = sqlx::query(sqlx::AssertSqlSafe(update.clone()))
                .persistent(persistent)
                .execute(&mut *conn)
                .await
                .unwrap();
            assert_eq!(result.rows_affected(), 1);
        }
        // The normal bound path and the no-parameter path share the same native connection.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(2_i64)
        .bind(6_i64)
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (3,9)"
        )))
        .execute(&mut *conn)
        .await
        .unwrap();
        let deleted = sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE id=3"
        )))
        .execute(&mut *conn)
        .await
        .unwrap();
        assert_eq!(deleted.rows_affected(), 1);
        let error = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (4,CAST('not-an-integer' AS BIGINT))"
        )))
        .execute(&mut *conn)
        .await
        .unwrap_err();
        assert!(matches!(error, sqlx::Error::Database(_)), "{error:?}");
        let rows: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id,value FROM {table} ORDER BY id"
        )))
        .fetch_all(&mut *conn)
        .await
        .unwrap();
        assert_eq!(
            rows,
            [(1, 6), (2, 6)],
            "{backend:?}: no lost, duplicated, or partially rejected writes"
        );
        drop(conn);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

async fn single_connection_client(backend: Backend) -> Client {
    // Give initial TLS setup its own timeout, separate from the pool-wait assertions.
    single_connection_client_with_timeout(backend, Duration::from_secs(5)).await
}

async fn single_connection_client_with_timeout(backend: Backend, timeout: Duration) -> Client {
    let options = MySqlConnectOptions::from_str(
        &env::var(match backend {
            Backend::MySql => "MYSQL_TEST_URL",
            Backend::Doris => "DORIS_TEST_URL",
        })
        .unwrap(),
    )
    .unwrap()
    .statement_cache_capacity(2);
    PoolBuilder::new(backend)
        .max_connections(1)
        .acquire_timeout(timeout)
        .connect_with(options)
        .await
        .unwrap()
}

async fn create(db: &Client, table: &TableName) {
    if db.backend() == Backend::MySql {
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY,group_id BIGINT NULL,payload \
                MEDIUMTEXT NULL)"
            )))
            .await
            .unwrap();
    } else {
        db.create_table(
            &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                .column(Column::new("id", T::BigInt).not_null())
                .column(Column::new("group_id", T::BigInt))
                .column(Column::new("payload", T::String))
                .replication(1),
        )
        .await
        .unwrap();
    }
}

#[derive(Debug, ::sqlx::FromRow, PartialEq)]
struct Record {
    id: i64,
    group_id: Option<i64>,
    payload: Option<String>,
}

async fn insert(db: &Client, table: &TableName, rows: &[Record]) {
    let mut query: QueryBuilder<MySql> = QueryBuilder::new(format!("INSERT INTO {table} "));
    query.push_values(rows, |mut bindings, row| {
        bindings
            .push_bind(row.id)
            .push_bind(row.group_id)
            .push_bind(&row.payload);
    });
    query.build().execute(db.pool()).await.unwrap();
}

#[derive(Debug, ::sqlx::FromRow, PartialEq)]
struct Summary {
    group_id: i64,
    label: String,
    row_count: i64,
    total: i64,
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn join_aggregate_subquery_aliases_and_optional_rows_use_the_same_queries_and_dto() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = single_connection_client(backend).await;
        let facts = common::name();
        let labels = common::name();
        create(&db, &facts).await;
        create(&db, &labels).await;
        let rows = (0_i64..20)
            .map(|id| Record {
                id,
                group_id: Some(id % 3),
                payload: None,
            })
            .collect::<Vec<_>>();
        insert(&db, &facts, &rows).await;
        let rows = ["zero", "one", "two"]
            .iter()
            .enumerate()
            .map(|(id, label)| Record {
                id: id as i64,
                group_id: None,
                payload: Some((*label).into()),
            })
            .collect::<Vec<_>>();
        insert(&db, &labels, &rows).await;
        let query = format!(
            "SELECT b.id AS group_id,b.payload AS label,CAST(COUNT(*) AS SIGNED) AS \
                row_count,CAST(SUM(a.id) AS SIGNED) AS total FROM {facts} a JOIN {labels} b \
                ON a.group_id=b.id WHERE a.id IN (SELECT id FROM {facts} WHERE id>=?) GROUP \
                BY b.id,b.payload HAVING COUNT(*)>=? ORDER BY b.id"
        );
        let actual: Vec<Summary> = sqlx::query_as(sqlx::AssertSqlSafe(query))
            .bind(10_i64)
            .bind(3_i64)
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(
            actual,
            vec![
                Summary {
                    group_id: 0,
                    label: "zero".into(),
                    row_count: 3,
                    total: 45
                },
                Summary {
                    group_id: 1,
                    label: "one".into(),
                    row_count: 4,
                    total: 58
                },
                Summary {
                    group_id: 2,
                    label: "two".into(),
                    row_count: 3,
                    total: 42
                },
            ],
            "{backend:?}"
        );
        let query = format!("SELECT id,group_id,payload FROM {labels} WHERE id=?");
        let optional: Option<Record> = sqlx::query_as(sqlx::AssertSqlSafe(query.clone()))
            .bind(1_i64)
            .fetch_optional(db.pool())
            .await
            .unwrap();
        assert_eq!(
            optional,
            Some(Record {
                id: 1,
                group_id: None,
                payload: Some("one".into())
            })
        );
        let absent: Option<Record> = sqlx::query_as(sqlx::AssertSqlSafe(query.clone()))
            .bind(999_i64)
            .fetch_optional(db.pool())
            .await
            .unwrap();
        assert!(absent.is_none());
        let error = sqlx::query_as::<_, Record>(sqlx::AssertSqlSafe(query))
            .bind(999_i64)
            .fetch_one(db.pool())
            .await
            .unwrap_err();
        assert!(matches!(error, sqlx::Error::RowNotFound));
        let raw = sqlx::raw_sql("SELECT 42 AS answer,NULL AS absent")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(raw.len(), 2);
        assert_eq!(
            raw.try_get::<i32, _>(0).unwrap(),
            raw.try_get::<i32, _>("answer").unwrap()
        );
        assert_eq!(raw.try_get::<Option<String>, _>("absent").unwrap(), None);
        for table in [&facts, &labels] {
            common::drop_table(&db, table).await;
        }
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn batches_and_complete_or_dropped_row_streams_preserve_order_and_reuse_the_single_connection()
 {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = single_connection_client(backend).await;
        let table = common::name();
        create(&db, &table).await;
        let expected = (0_i64..2048)
            .map(|id| Record {
                id,
                group_id: if id % 7 == 0 { None } else { Some(id % 3) },
                payload: if id % 11 == 0 {
                    None
                } else {
                    Some(format!("{id}:中文🙂 ' \\ ?\0 {}", "x".repeat(256)))
                },
            })
            .collect::<Vec<_>>();
        for batch in expected.chunks(256) {
            insert(&db, &table, batch).await;
        }
        let query = format!("SELECT id,group_id,payload FROM {table} ORDER BY id");
        let mut stream =
            sqlx::query_as::<_, Record>(sqlx::AssertSqlSafe(query.clone())).fetch(db.pool());
        let mut count = 0;
        while let Some(row) = stream.try_next().await.unwrap() {
            assert_eq!(row, expected[count], "{backend:?}, row {count}");
            count += 1;
        }
        drop(stream);
        assert_eq!(count, expected.len());
        let mut stream = sqlx::query_as::<_, Record>(sqlx::AssertSqlSafe(query)).fetch(db.pool());
        for row in expected.iter().take(37) {
            assert_eq!(stream.try_next().await.unwrap().as_ref(), Some(row));
        }
        drop(stream);
        let count: i64 = tokio::time::timeout(
            Duration::from_secs(10),
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(db.pool()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count, 2048);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn cache_eviction_and_explicit_clear_do_not_change_binding() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = single_connection_client(backend).await;
        let mut conn = db.pool().acquire().await.unwrap();
        for persistent in [true, false] {
            for index in 0_i64..9 {
                let query = format!(
                    "SELECT CAST(? AS SIGNED)+CAST(? AS SIGNED) /* {persistent}_{index} */"
                );
                let value: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(query))
                    .persistent(persistent)
                    .bind(index)
                    .bind(100_i64)
                    .fetch_one(&mut *conn)
                    .await
                    .unwrap();
                assert_eq!(value, index + 100);
                assert!(conn.cached_statements_size() <= 2);
            }
        }
        assert_eq!(conn.cached_statements_size(), 2);
        conn.clear_cached_statements().await.unwrap();
        assert_eq!(conn.cached_statements_size(), 0);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT CAST(? AS SIGNED)")
                .bind(42_i64)
                .fetch_one(&mut *conn)
                .await
                .unwrap(),
            42
        );
        drop(conn);
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn upstream_parameter_count_gap_is_reproduced_and_profile_retires_the_connection_on_release()
{
    for backend in [Backend::MySql, Backend::Doris] {
        let db = single_connection_client(backend).await;
        for persistent in [true, false] {
            for count in [1, 3] {
                let mut conn = db.pool().acquire().await.unwrap();
                let mut query = sqlx::query("SELECT CAST(? AS SIGNED)+CAST(? AS SIGNED)")
                    .persistent(persistent);
                for value in 0..count {
                    query = query.bind(i64::from(value));
                }
                let error = tokio::time::timeout(Duration::from_secs(2), query.execute(&mut *conn))
                    .await
                    .unwrap()
                    .unwrap_err();
                assert!(
                    matches!(error, sqlx::Error::Protocol(_)),
                    "{backend:?}: {error:?}"
                );
                assert!(
                    error.to_string().contains("expected 2 parameters"),
                    "{error:?}"
                );
                // The retained raw connection still has the upstream phantom
                // result. This is intentionally bounded rather than hanging CI.
                assert!(
                    tokio::time::timeout(
                        Duration::from_millis(250),
                        sqlx::query("SELECT 42").execute(&mut *conn)
                    )
                    .await
                    .is_err()
                );
                drop(conn);
                // The profile hook must recover max_connections=1 without the
                // caller setting close_on_drop or retrying the original query.
                let answer: i64 = tokio::time::timeout(
                    Duration::from_secs(2),
                    sqlx::query_scalar("SELECT 42").fetch_one(db.pool()),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(answer, 42);
            }
        }
        // Ordinary pool-based error handling also releases the broken connection.
        sqlx::query("SELECT ?+?")
            .bind(1_i64)
            .execute(db.pool())
            .await
            .unwrap_err();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT 42")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            42
        );
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn long_strings_control_bytes_trailing_spaces_and_sql_syntax_remain_bound_data() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = single_connection_client(backend).await;
        let table = common::name();
        create(&db, &table).await;
        let values = [
            None,
            Some(String::new()),
            Some("tail  ".into()),
            Some("'); DROP TABLE x; -- ? /* comment */ \\ \" 中文🙂\0\r\n\t".into()),
            Some("中文🙂 '\\?\0\r\n".repeat(16_384)),
        ];
        let rows = values
            .into_iter()
            .enumerate()
            .map(|(id, payload)| Record {
                id: id as i64,
                group_id: None,
                payload,
            })
            .collect::<Vec<_>>();
        assert!(rows[4].payload.as_ref().unwrap().len() > 250_000);
        insert(&db, &table, &rows).await;
        let query = format!("SELECT id,group_id,payload FROM {table} ORDER BY id");
        let binary: Vec<Record> = sqlx::query_as(sqlx::AssertSqlSafe(query.clone()))
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(binary, rows, "{backend:?}");
        let text = sqlx::raw_sql(sqlx::AssertSqlSafe(query))
            .fetch_all(db.pool())
            .await
            .unwrap();
        for (actual, expected) in text.iter().zip(&rows) {
            assert_eq!(
                actual.try_get::<Option<String>, _>("payload").unwrap(),
                expected.payload
            );
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn pool_acquisition_timeout_connection_replacement_and_close_wait_are_bounded() {
    for backend in [Backend::MySql, Backend::Doris] {
        let warmup = single_connection_client(backend).await;
        warmup.close().await;
        // Keep the intentionally exhausted pool's actual 200ms timeout after
        // cold cryptographic initialization has completed in this process.
        let db = single_connection_client_with_timeout(backend, Duration::from_millis(200)).await;
        let mut held = db.pool().acquire().await.unwrap();
        let first: i64 = sqlx::query_scalar("SELECT CAST(CONNECTION_ID() AS SIGNED)")
            .fetch_one(&mut *held)
            .await
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(2), db.pool().acquire())
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, sqlx::Error::PoolTimedOut), "{error:?}");
        held.close().await.unwrap();
        let mut held = db.pool().acquire().await.unwrap();
        let next: i64 = sqlx::query_scalar("SELECT CAST(CONNECTION_ID() AS SIGNED)")
            .fetch_one(&mut *held)
            .await
            .unwrap();
        assert_ne!(first, next);
        if backend == Backend::Doris {
            let (strict, decimal): (i64, i64) = sqlx::query_as(
                "SELECT CAST(@@enable_strict_cast AS SIGNED),CAST(@@enable_decimal256 AS SIGNED)",
            )
            .fetch_one(&mut *held)
            .await
            .unwrap();
            assert_eq!((strict, decimal), (1, 1));
        }
        let pool = db.pool().clone();
        let closing = tokio::spawn(async move { pool.close().await });
        tokio::time::timeout(Duration::from_secs(2), async {
            while !db.pool().is_closed() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !closing.is_finished(),
            "close must wait for checked-out connections"
        );
        drop(held);
        tokio::time::timeout(Duration::from_secs(2), closing)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            db.pool().acquire().await.unwrap_err(),
            sqlx::Error::PoolClosed
        ));
    }
}

#[tokio::test]
#[ignore = "requires MySQL"]
async fn mysql_native_nested_savepoints_and_statement_errors_preserve_outer_transaction() {
    let db = single_connection_client(Backend::MySql).await;
    let table = common::name();
    create(&db, &table).await;
    insert(
        &db,
        &table,
        &[Record {
            id: 1,
            group_id: Some(10),
            payload: None,
        }],
    )
    .await;
    let mut outer = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?, ?, ?)"
    )))
    .bind(2_i64)
    .bind(20_i64)
    .bind(None::<String>)
    .execute(&mut *outer)
    .await
    .unwrap();
    let mut nested = (*outer).begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET group_id=? WHERE id=?"
    )))
    .bind(99_i64)
    .bind(1_i64)
    .execute(&mut *nested)
    .await
    .unwrap();
    nested.rollback().await.unwrap();
    sqlx::query("this is invalid SQL")
        .execute(&mut *outer)
        .await
        .unwrap_err();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET group_id=? WHERE id=?"
    )))
    .bind(21_i64)
    .bind(2_i64)
    .execute(&mut *outer)
    .await
    .unwrap();
    outer.commit().await.unwrap();
    let actual: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,group_id FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(actual, [(1, 10), (2, 21)]);
    let mut interrupted = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?, ?, ?)"
    )))
    .bind(3_i64)
    .bind(30_i64)
    .bind(None::<String>)
    .execute(&mut *interrupted)
    .await
    .unwrap();
    sqlx::query("SELECT ?+?")
        .bind(1_i64)
        .execute(&mut *interrupted)
        .await
        .unwrap_err();
    drop(interrupted);
    let count: i64 = tokio::time::timeout(
        Duration::from_secs(2),
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(db.pool()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        count, 2,
        "dropping a protocol-damaged MySQL transaction must roll back"
    );
    common::drop_table(&db, &table).await;
    db.close().await;
}
