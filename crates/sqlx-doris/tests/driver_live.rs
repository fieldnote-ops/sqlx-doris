mod common;
use futures_util::TryStreamExt;
use sqlx::SqlSafeStr;
use sqlx::{Connection, Executor, Row};
use sqlx_doris::{
    DorisConnectOptions, DorisConnection, DorisPoolOptions,
    types::{BigDecimal, LargeInt, Time},
};
use std::{str::FromStr, time::Duration};

fn url() -> String {
    std::env::var("DORIS_TEST_URL").expect("DORIS_TEST_URL is required")
}

#[tokio::test]
#[ignore = "requires dedicated Doris; randomized payloads are read-only session tests"]
async fn seeded_unicode_parameters_round_trip_binary_and_text_in_both_sql_modes() {
    let mut c = DorisConnection::connect(&url()).await.unwrap();
    let mut seed = std::env::var("DORIS_RANDOM_SEED")
        .map_or(Ok(0xa9d6_417b_028c_e35fu64), |v| v.parse())
        .unwrap();
    let alphabet = [
        '中', '🙂', '\0', '\n', '\r', '\t', '\u{1a}', '\'', '"', '`', '?', '\\', ';', '/', '*',
        '-', ' ', 'a',
    ];
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for mode in ["SET sql_mode=''", "SET sql_mode='NO_BACKSLASH_ESCAPES'"] {
        c.execute(mode).await.unwrap();
        for case in 0..128 {
            let n = (next() % 128) as usize;
            let value: String = (0..n)
                .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                .collect();
            let binary: String = sqlx::query_scalar("SELECT CAST(? AS STRING)")
                .bind(&value)
                .fetch_one(&mut c)
                .await
                .unwrap();
            let text: String = sqlx::query_scalar("SELECT CAST(? AS STRING) LIMIT ?")
                .bind(&value)
                .bind(1_i64)
                .fetch_one(&mut c)
                .await
                .unwrap();
            assert_eq!(binary, value, "binary case {case}, {mode}");
            assert_eq!(text, value, "text case {case}, {mode}");
        }
    }
    c.close().await.unwrap();
}

#[derive(Debug, PartialEq, ::sqlx::FromRow)]
struct Record {
    id: i64,
    name: String,
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn sqlx_query_derive_builder_decimals_datetime_and_time_boundaries() {
    let mut c = DorisConnection::connect(&url()).await.unwrap();
    for (id, name) in [(i64::MIN, "中文🙂'\\\0?"), (i64::MAX, "")] {
        let v: Record = sqlx::query_as("SELECT CAST(? AS BIGINT) AS id,CAST(? AS STRING) AS name")
            .bind(id)
            .bind(name)
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(
            v,
            Record {
                id,
                name: name.into()
            }
        );
    }
    let decimal = BigDecimal::from_str("12345678901234567890.123456789012345678").unwrap();
    let v: BigDecimal = sqlx::query_scalar("SELECT CAST(? AS DECIMAL(38,18))")
        .bind(&decimal)
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(v, decimal);
    let large: LargeInt = sqlx::query_scalar("SELECT CAST(? AS LARGEINT)")
        .bind(LargeInt(i128::MIN))
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(large.0, i128::MIN);
    let date = chrono::NaiveDate::from_ymd_opt(2026, 10, 31).unwrap();
    let stamp = date.and_hms_micro_opt(12, 34, 56, 123456).unwrap();
    let v: (chrono::NaiveDate, chrono::NaiveDateTime) =
        sqlx::query_as("SELECT CAST(? AS DATE),CAST(? AS DATETIME(6))")
            .bind(date)
            .bind(stamp)
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert_eq!(v, (date, stamp));
    for n in [Time::MAX_MICROSECONDS, -Time::MAX_MICROSECONDS, -1, 0, 1] {
        let expected = Time::from_microseconds(n).unwrap();
        let t: Time = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
            .bind(expected)
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(t, expected);
    }
    let mut q = sqlx::QueryBuilder::new("SELECT CAST(");
    q.push_bind(42_i64).push(" AS BIGINT)");
    let n: i64 = q.build_query_scalar().fetch_one(&mut c).await.unwrap();
    assert_eq!(n, 42);
    let rows: Vec<i32> = sqlx::query_scalar("SELECT 1 UNION ALL SELECT 2 LIMIT ? OFFSET ?")
        .bind(1_i64)
        .bind(1_i64)
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(rows, vec![2]);
    assert!(c.cached_statements_size() > 0);
    c.clear_cached_statements().await.unwrap();
    assert_eq!(c.cached_statements_size(), 0);
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn native_transactions_bind_safely_commit_rollback_and_close_on_drop() {
    let pool = DorisPoolOptions::new()
        .max_connections(1)
        .connect(&url())
        .await
        .unwrap();
    let table = common::qualified_table();
    pool.execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}(id BIGINT, value STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"))).await.unwrap();
    let insert = sqlx::AssertSqlSafe(format!("INSERT INTO {table} VALUES (?,?)")).into_sql_str();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(insert.clone())
        .bind(1_i64)
        .bind("中'\\\0?; DROP TABLE t")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(insert.clone())
        .bind(2_i64)
        .bind("rollback")
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(insert)
        .bind(3_i64)
        .bind("drop")
        .execute(&mut *tx)
        .await
        .unwrap();
    drop(tx);
    let rows: Vec<(i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,value FROM {table} ORDER BY id"
    )))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows, vec![(1, "中'\\\0?; DROP TABLE t".into())]);
    pool.execute(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
        .await
        .unwrap();
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn stream_and_cancel_release_pool_slots_and_never_reuse_unread_protocol() {
    let pool = DorisPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url())
        .await
        .unwrap();
    let mut rows = sqlx::query("SELECT number FROM numbers('number'='10000')").fetch(&pool);
    let row = rows.try_next().await.unwrap().unwrap();
    assert_eq!(row.try_get::<i64, _>(0).unwrap(), 0);
    drop(rows);
    let v: i32 = sqlx::query_scalar("SELECT 42")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 42);
    let cancelled = tokio::time::timeout(
        Duration::from_millis(50),
        sqlx::query("SELECT SLEEP(2)").fetch_one(&pool),
    )
    .await;
    assert!(cancelled.is_err());
    let v: i32 = sqlx::query_scalar("SELECT 43")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v, 43);
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn byte_predicates_modes_and_nonpersistent_cache_remain_correct() {
    let o = DorisConnectOptions::from_str(&url()).unwrap();
    let mut c = DorisConnection::connect_with(&o).await.unwrap();
    for mode in ["SET sql_mode=''", "SET sql_mode='NO_BACKSLASH_ESCAPES'"] {
        c.execute(mode).await.unwrap();
        let bytes = vec![0, 39, 92, 128, 255, 63];
        let actual: Vec<u8> = sqlx::query_scalar("SELECT ?")
            .bind(bytes.clone())
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(actual, bytes);
        let name = "x'\\\0; DROP TABLE t -- ?";
        let actual: String = sqlx::query_scalar("SELECT CAST(? AS STRING) LIMIT ?")
            .bind(name)
            .bind(1_i64)
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(actual, name);
    }
    for p in [true, false, true] {
        let n: i64 = sqlx::query_scalar("SELECT CAST(? AS BIGINT)")
            .bind(7_i64)
            .persistent(p)
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(n, 7);
    }
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris; only per-session settings are changed"]
async fn sqlx_after_connect_initializes_each_pool_connection() {
    let pool = DorisPoolOptions::new()
        .max_connections(2)
        .min_connections(2)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET time_zone = '+00:00'")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET query_timeout = 30")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET workload_group = 'normal'")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&url())
        .await
        .unwrap();
    let mut connections = [pool.acquire().await.unwrap(), pool.acquire().await.unwrap()];
    let mut ids = std::collections::BTreeSet::new();
    for connection in &mut connections {
        let row: (i64, String, i64, String) = sqlx::query_as(
            "SELECT CAST(CONNECTION_ID() AS BIGINT), @@time_zone, CAST(@@query_timeout AS BIGINT), @@workload_group",
        ).fetch_one(&mut **connection).await.unwrap();
        assert!(ids.insert(row.0));
        assert_eq!(row.1, "+00:00");
        assert_eq!(row.2, 30);
        assert_eq!(row.3, "normal");
    }
    drop(connections);
    pool.close().await;
}
