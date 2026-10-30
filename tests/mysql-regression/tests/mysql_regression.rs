//! One MySQL contract source, built against registry SQLx and the companion.
//! Expected upstream TIME defects are explicit controls, not equivalent results.
use futures_util::TryStreamExt;
use sqlx::{
    ConnectOptions, Connection, Executor, FromRow, MySql, MySqlPool, QueryBuilder, Row, SqlSafeStr,
    Statement,
    mysql::{
        MySqlConnectOptions, MySqlPoolOptions, MySqlSslMode,
        types::{MySqlTime, MySqlTimeSign},
    },
    types::{
        BigDecimal, Json,
        chrono::{NaiveDate, NaiveDateTime},
    },
};
use std::{
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn options() -> MySqlConnectOptions {
    let options = MySqlConnectOptions::from_str(
        &std::env::var("MYSQL_TEST_URL").expect("dedicated MYSQL_TEST_URL required"),
    )
    .unwrap();
    assert_eq!(options.get_database(), Some("sqlx_doris_test"));
    options
        .ssl_mode(MySqlSslMode::Required)
        .statement_cache_capacity(2)
        .disable_statement_logging()
}
async fn pool() -> MySqlPool {
    let pool = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options())
        .await
        .unwrap();
    let version: String = sqlx::query_scalar("SELECT VERSION()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        version.split('-').next() == Some("8.4.13"),
        "fixed MySQL 8.4.13 fixture required"
    );
    pool
}
fn table() -> String {
    format!(
        "`_mysql_regression_{}_{}`",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}
async fn drop_table(pool: &MySqlPool, table: &str) {
    pool.execute(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn identity_tls_and_session() {
    println!(
        "MYSQL_REGRESSION_IDENTITY official={}",
        cfg!(feature = "official-baseline")
    );
    let pool = pool().await;
    let row = sqlx::query("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!row.get::<String, _>(1).is_empty());
    let db: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(db, "sqlx_doris_test");
    pool.execute("SET SESSION time_zone='+00:00'")
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT @@time_zone")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "+00:00"
    );
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn bound_scalars_sql_modes_and_null() {
    let pool = pool().await;
    let payload = concat!(r#"'); DROP TABLE x; -- ? /* */ \ ""#, " 中文🙂\0\r\n\t");
    for mode in [
        "STRICT_TRANS_TABLES",
        "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES",
        "STRICT_TRANS_TABLES,ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
    ] {
        sqlx::query("SET SESSION sql_mode=?")
            .bind(mode)
            .execute(&pool)
            .await
            .unwrap();
        for persistent in [true, true, false] {
            let row: (i64, u64, String, Option<i64>) =
                sqlx::query_as("SELECT CAST(? AS SIGNED),CAST(? AS UNSIGNED),?,CAST(? AS SIGNED)")
                    .bind(i64::MIN)
                    .bind(u64::MAX)
                    .bind(payload)
                    .bind(None::<i64>)
                    .persistent(persistent)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(row, (i64::MIN, u64::MAX, payload.into(), None));
        }
    }
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn cache_explicit_prepare_eviction_and_clear() {
    let pool = pool().await;
    let mut conn = pool.acquire().await.unwrap();
    for n in 0..8 {
        let sql = format!("SELECT CAST(? AS SIGNED)+{n}");
        for persistent in [true, true, false] {
            assert_eq!(
                sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql.clone()))
                    .bind(100_i64)
                    .persistent(persistent)
                    .fetch_one(&mut *conn)
                    .await
                    .unwrap(),
                100 + n
            );
            assert!(conn.cached_statements_size() <= 2);
        }
    }
    let prepared = conn
        .prepare("SELECT CAST(? AS SIGNED) AS n".into_sql_str())
        .await
        .unwrap();
    assert_eq!(
        prepared
            .query_scalar::<i64>()
            .bind(42_i64)
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        42
    );
    conn.clear_cached_statements().await.unwrap();
    assert_eq!(conn.cached_statements_size(), 0);
    drop(conn);
    pool.close().await;
}

#[derive(Debug, FromRow, PartialEq)]
struct Record {
    id: i64,
    n: Option<i64>,
    payload: Option<Vec<u8>>,
}
#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn crud_query_builder_defaults_and_dto() {
    let pool = pool().await;
    let table = table();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT PRIMARY KEY,n BIGINT DEFAULT 7,payload LONGBLOB NULL)"
    )))
    .await
    .unwrap();
    let records = (0..17)
        .map(|id| Record {
            id,
            n: if id % 3 == 0 { None } else { Some(id * 10) },
            payload: if id % 4 == 0 {
                None
            } else {
                Some(
                    (0..=255)
                        .cycle()
                        .take(if id == 15 { 65536 } else { 257 })
                        .collect(),
                )
            },
        })
        .collect::<Vec<_>>();
    let mut builder: QueryBuilder<MySql> =
        QueryBuilder::new(format!("INSERT INTO {table}(id,n,payload) "));
    builder.push_values(&records, |mut row, v| {
        row.push_bind(v.id).push_bind(v.n).push_bind(&v.payload);
    });
    assert_eq!(
        builder
            .build()
            .execute(&pool)
            .await
            .unwrap()
            .rows_affected(),
        17
    );
    assert_eq!(
        sqlx::query_as::<_, Record>(sqlx::AssertSqlSafe(format!(
            "SELECT id,n,payload FROM {table} ORDER BY id"
        )))
        .fetch_all(&pool)
        .await
        .unwrap(),
        records
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table}(id,n) VALUES (?,DEFAULT)"
    )))
    .bind(20_i64)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {table} SET n=n+? WHERE id=?"
        )))
        .bind(2_i64)
        .bind(20_i64)
        .execute(&pool)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    let last: Record = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,n,payload FROM {table} WHERE id=?"
    )))
    .bind(20_i64)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        last,
        Record {
            id: 20,
            n: Some(9),
            payload: None
        }
    );
    assert_eq!(
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE id=?"
        )))
        .bind(20_i64)
        .execute(&pool)
        .await
        .unwrap()
        .rows_affected(),
        1
    );
    assert!(
        sqlx::query_as::<_, Record>(sqlx::AssertSqlSafe(format!(
            "SELECT id,n,payload FROM {table} WHERE id=20"
        )))
        .fetch_optional(&pool)
        .await
        .unwrap()
        .is_none()
    );
    drop_table(&pool, &table).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn exact_decimal_json_and_datetime() {
    let pool = pool().await;
    let table = table();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT PRIMARY KEY,d DECIMAL(65,30),j JSON,day DATE,\
        dt DATETIME(6))"
    )))
    .await
    .unwrap();
    let decimal =
        BigDecimal::from_str("12345678901234567890123456789012345.123456789012345678901234567890")
            .unwrap();
    let json = serde_json::json!({
        "quote": "\u{0}中文🙂",
        "null": null,
        "n": 123456789012345678901234567890_u128.to_string()
    });
    let day = NaiveDate::from_ymd_opt(1000, 1, 2).unwrap();
    let dt = day.and_hms_micro_opt(3, 4, 5, 999999).unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (1,?,?,?,?),(2,NULL,NULL,NULL,NULL)"
    )))
    .bind(&decimal)
    .bind(Json(&json))
    .bind(day)
    .bind(dt)
    .execute(&pool)
    .await
    .unwrap();
    type StoredValues = (
        i64,
        Option<BigDecimal>,
        Option<Json<serde_json::Value>>,
        Option<NaiveDate>,
        Option<NaiveDateTime>,
    );
    let rows: Vec<StoredValues> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT * FROM {table} ORDER BY id"
    )))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, Some(decimal), Some(Json(json)), Some(day), Some(dt)),
            (2, None, None, None, None)
        ]
    );
    drop_table(&pool, &table).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn streaming_early_drop_and_next_query() {
    let pool = pool().await;
    let table = table();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT PRIMARY KEY,payload TEXT)"
    )))
    .await
    .unwrap();
    let mut builder: QueryBuilder<MySql> = QueryBuilder::new(format!("INSERT INTO {table} "));
    builder.push_values(0_i64..2048, |mut row, id| {
        row.push_bind(id).push_bind("中".repeat(300));
    });
    builder.build().execute(&pool).await.unwrap();
    let mut conn = pool.acquire().await.unwrap();
    {
        let mut rows = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT id,payload FROM {table} ORDER BY id"
        )))
        .fetch(&mut *conn);
        for id in 0_i64..3 {
            assert_eq!(rows.try_next().await.unwrap().unwrap().get::<i64, _>(0), id);
        }
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT 42")
            .fetch_one(&mut *conn)
            .await
            .unwrap(),
        42
    );
    let ids = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {table} ORDER BY id"
    )))
    .fetch(&mut *conn)
    .try_collect::<Vec<_>>()
    .await
    .unwrap();
    assert_eq!(ids, (0..2048).collect::<Vec<_>>());
    drop(conn);
    drop_table(&pool, &table).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn transaction_commit_rollback_drop_and_savepoint() {
    let pool = pool().await;
    let table = table();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT PRIMARY KEY,n BIGINT)"
    )))
    .await
    .unwrap();
    for outcome in 0..3 {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(1_i64)
        .bind(10_i64)
        .execute(&mut *tx)
        .await
        .unwrap();
        {
            let mut nested = tx.begin().await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {table} SET n=? WHERE id=1"
            )))
            .bind(99_i64)
            .execute(&mut *nested)
            .await
            .unwrap();
            nested.rollback().await.unwrap();
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
                "SELECT n FROM {table} WHERE id=1"
            )))
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
            10
        );
        match outcome {
            0 => tx.rollback().await.unwrap(),
            1 => drop(tx),
            _ => tx.commit().await.unwrap(),
        }
        let rows =
            sqlx::query_as::<_, (i64, i64)>(sqlx::AssertSqlSafe(format!("SELECT * FROM {table}")))
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows, if outcome == 2 { vec![(1, 10)] } else { vec![] });
    }
    drop_table(&pool, &table).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn duplicate_key_and_server_error_recovery() {
    let pool = pool().await;
    let table = table();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT PRIMARY KEY,n BIGINT NOT NULL)"
    )))
    .await
    .unwrap();
    let mut tx = pool.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (1,10)"
    )))
    .execute(&mut *tx)
    .await
    .unwrap();
    let error = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?,20),(?,30)"
    )))
    .bind(2_i64)
    .bind(1_i64)
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("23000")
    );
    tx.commit().await.unwrap();
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(sqlx::AssertSqlSafe(format!(
            "SELECT * FROM {table} ORDER BY id"
        )))
        .fetch_all(&pool)
        .await
        .unwrap(),
        [(1, 10)]
    );
    let error = sqlx::query("SELECT * FROM __mysql_regression_absent_9e72")
        .fetch_all(&pool)
        .await
        .unwrap_err();
    assert_eq!(
        error.as_database_error().unwrap().code().as_deref(),
        Some("42S02")
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT 42")
            .fetch_one(&pool)
            .await
            .unwrap(),
        42
    );
    drop_table(&pool, &table).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn schema_changed_prepared_metadata() {
    let pool = pool().await;
    let table = table();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT PRIMARY KEY,n INT)"
    )))
    .await
    .unwrap();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES(1,7)"
    )))
    .await
    .unwrap();
    let sql = format!("SELECT n FROM {table} WHERE id=?");
    assert_eq!(
        sqlx::query_scalar::<_, i32>(sqlx::AssertSqlSafe(sql.clone()))
            .bind(1_i64)
            .fetch_one(&pool)
            .await
            .unwrap(),
        7
    );
    pool.execute(sqlx::AssertSqlSafe(format!(
        "ALTER TABLE {table} MODIFY n BIGINT"
    )))
    .await
    .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET n=? WHERE id=1"
    )))
    .bind(i64::MAX)
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
            .bind(1_i64)
            .fetch_one(&pool)
            .await
            .unwrap(),
        i64::MAX
    );
    drop_table(&pool, &table).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn cancelled_read_preserves_pool_capacity() {
    let pool = pool().await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            sqlx::query("SELECT SLEEP(0.4)").fetch_one(&pool)
        )
        .await
        .is_err()
    );
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            sqlx::query_scalar::<_, i64>("SELECT 42").fetch_one(&pool)
        )
        .await
        .unwrap()
        .unwrap(),
        42
    );
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn time_binary_and_explicit_upstream_fix_controls() {
    let pool = pool().await;
    let table = table();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT PRIMARY KEY,t TIME(6))"
    )))
    .await
    .unwrap();
    let values = [
        MySqlTime::ZERO,
        MySqlTime::new(MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap(),
        MySqlTime::new(MySqlTimeSign::Positive, 25, 2, 3, 999999).unwrap(),
        MySqlTime::MIN,
        MySqlTime::MAX,
    ];
    let mut builder: QueryBuilder<MySql> = QueryBuilder::new(format!("INSERT INTO {table} "));
    builder.push_values(values.iter().enumerate(), |mut row, (id, t)| {
        row.push_bind(id as i64).push_bind(*t);
    });
    builder.build().execute(&pool).await.unwrap();
    let binary = sqlx::query_scalar::<_, MySqlTime>(sqlx::AssertSqlSafe(format!(
        "SELECT t FROM {table} ORDER BY id"
    )))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(binary, values);
    let text = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "SELECT t FROM {table} WHERE id=1"
    )))
    .fetch_one(&pool)
    .await
    .unwrap()
    .get::<MySqlTime, _>(0);
    assert_eq!(
        text,
        MySqlTime::new(
            if cfg!(feature = "official-baseline") {
                MySqlTimeSign::Positive
            } else {
                MySqlTimeSign::Negative
            },
            0,
            0,
            0,
            1
        )
        .unwrap()
    );
    assert_eq!(
        MySqlTime::ZERO.is_negative(),
        cfg!(feature = "official-baseline")
    );
    drop_table(&pool, &table).await;
    pool.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn native_time_membership_cache_coercion_is_preserved() {
    let pool = pool().await;
    let table = table();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT PRIMARY KEY,t TIME(6))"
    )))
    .await
    .unwrap();
    pool.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES(0,NULL),(1,'-00:00:00.000001'),(2,'00:00:00'),(3,\
            '25:00:00')"
    )))
    .await
    .unwrap();
    let sql = format!("SELECT id FROM {table} WHERE t IN(?,?) ORDER BY id");
    for persistent in [true, true, false] {
        let native = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql.clone()))
            .bind(MySqlTime::new(MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap())
            .bind(MySqlTime::ZERO)
            .persistent(persistent)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(native, [1]);
        let mixed = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql.clone()))
            .bind(MySqlTime::ZERO)
            .bind("25:00:00")
            .persistent(persistent)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(mixed, if persistent { vec![] } else { vec![3] });
        let numeric = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql.clone()))
            .bind(MySqlTime::ZERO)
            .bind(0_i64)
            .persistent(persistent)
            .fetch_all(&pool)
            .await
            .unwrap();
        assert_eq!(numeric, if persistent { vec![] } else { vec![2] });
    }
    drop_table(&pool, &table).await;
    pool.close().await;
}

#[path = "support/mysql_reply_loss.rs"]
mod reply_loss;
async fn lost_reply(commit: bool) {
    use std::sync::atomic::Ordering;
    let admin = pool().await;
    let table = table();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {table}(id BIGINT,n BIGINT)"
        )))
        .await
        .unwrap();
    let proxy = reply_loss::ReplyLoss::start(&options(), commit).await;
    let client = MySqlPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(3))
        .connect_with(proxy.options(options()))
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        if commit {
            let mut tx = client.begin().await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(?,?)"
            )))
            .bind(1_i64)
            .bind(42_i64)
            .execute(&mut *tx)
            .await
            .unwrap();
            tx.commit().await
        } else {
            client
                .execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES(1,42) /* MYSQL_REGRESSION_LOST_REPLY */"
                )))
                .await
                .map(|_| ())
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        result.unwrap_err(),
        sqlx::Error::Io(_) | sqlx::Error::Protocol(_)
    ));
    assert_eq!(
        sqlx::query_as::<_, (i64, i64)>(sqlx::AssertSqlSafe(format!("SELECT * FROM {table}")))
            .fetch_all(&admin)
            .await
            .unwrap(),
        [(1, 42)]
    );
    assert_eq!(
        proxy.hits.load(Ordering::SeqCst),
        1,
        "uncertain operation must be sent only once"
    );
    assert_eq!(
        proxy.ok_replies.load(Ordering::SeqCst),
        1,
        "drop a real server OK, not a pre-send failure"
    );
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            sqlx::query_scalar::<_, i64>("SELECT 42").fetch_one(&client)
        )
        .await
        .unwrap()
        .unwrap(),
        42
    );
    client.close().await;
    drop(proxy);
    drop_table(&admin, &table).await;
    admin.close().await;
}
#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn acknowledged_write_reply_loss_never_replays() {
    lost_reply(false).await;
}
#[tokio::test]
#[ignore = "requires dedicated MySQL 8.4.13"]
async fn acknowledged_commit_reply_loss_never_replays() {
    lost_reply(true).await;
}
