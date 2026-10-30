//! Shared MySQL/Doris contracts. Explicit live runs require both databases.

use std::{env, time::Duration};

use sqlx::{Connection, Executor, MySqlConnection, Row, mysql::MySqlPoolOptions};

fn endpoints() -> [(&'static str, String); 2] {
    [
        (
            "mysql",
            env::var("MYSQL_TEST_URL").expect("MYSQL_TEST_URL is required"),
        ),
        (
            "doris",
            env::var("DORIS_TEST_URL").expect("DORIS_TEST_URL is required"),
        ),
    ]
}

async fn connect(url: &str) -> MySqlConnection {
    tokio::time::timeout(Duration::from_secs(15), MySqlConnection::connect(url))
        .await
        .expect("connection timed out")
        .expect("upstream SQLx default connection failed")
}

#[tokio::test]
#[ignore = "requires MYSQL_TEST_URL and DORIS_TEST_URL"]
async fn server_identity() {
    for (backend, url) in endpoints() {
        let mut conn = connect(&url).await;
        let row = (&mut conn)
            .fetch_one("SELECT VERSION() AS version")
            .await
            .unwrap();
        let version: String = row.try_get("version").unwrap();
        assert!(!version.is_empty(), "{backend}: empty reported version");
        // The MySQL-compatible version string alone does not certify a Doris release.
        println!("backend={backend} mysql_protocol_version_string={version}");
        conn.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires MYSQL_TEST_URL and DORIS_TEST_URL"]
async fn shared_text_query() {
    for (backend, url) in endpoints() {
        let mut conn = connect(&url).await;
        let row = (&mut conn)
            .fetch_one("SELECT CAST(42 AS SIGNED) AS id, '中文🙂' AS message, CAST(NULL AS SIGNED) AS missing")
            .await
            .unwrap();
        assert_eq!(row.try_get::<i64, _>("id").unwrap(), 42, "{backend}");
        assert_eq!(
            row.try_get::<String, _>("message").unwrap(),
            "中文🙂",
            "{backend}"
        );
        assert_eq!(
            row.try_get::<Option<i64>, _>("missing").unwrap(),
            None,
            "{backend}"
        );
        conn.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires MYSQL_TEST_URL and DORIS_TEST_URL"]
async fn bound_values_and_from_row() {
    for (backend, url) in endpoints() {
        let mut conn = connect(&url).await;
        let value: (i64, String, Option<i64>) =
            sqlx::query_as("SELECT CAST(? AS SIGNED), ?, CAST(? AS SIGNED)")
                .bind(i64::MAX)
                .bind("中文🙂 ' \\ ? -- /* literal */")
                .bind(None::<i64>)
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert_eq!(
            value,
            (i64::MAX, "中文🙂 ' \\ ? -- /* literal */".into(), None),
            "{backend}"
        );
        conn.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires MYSQL_TEST_URL and DORIS_TEST_URL"]
async fn prepared_statement_cache_modes() {
    for (backend, url) in endpoints() {
        let mut conn = connect(&url).await;
        for persistent in [true, false] {
            for expected in [1_i64, 2, -3] {
                let actual: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
                    .bind(expected)
                    .persistent(persistent)
                    .fetch_one(&mut conn)
                    .await
                    .unwrap();
                assert_eq!(actual, expected, "{backend}: persistent={persistent}");
            }
        }
        conn.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires MYSQL_TEST_URL and DORIS_TEST_URL"]
async fn connection_usable_after_server_error() {
    for (backend, url) in endpoints() {
        let mut conn = connect(&url).await;
        let result = conn
            .execute("SELECT * FROM __sqlx_doris_missing_table_74c1b6")
            .await;
        let error = result.expect_err("the missing table must be rejected");
        assert!(
            error.as_database_error().is_some(),
            "{backend}: expected a server error"
        );
        let row = (&mut conn).fetch_one("SELECT 42 AS id").await.unwrap();
        assert_eq!(row.try_get::<i64, _>("id").unwrap(), 42, "{backend}");
        conn.close().await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires MYSQL_TEST_URL and DORIS_TEST_URL"]
async fn pool_concurrent_queries_and_reuse() {
    for (backend, url) in endpoints() {
        let pool = MySqlPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(15))
            .connect(&url)
            .await
            .unwrap();
        for _ in 0..3 {
            let first = sqlx::query_scalar::<_, i64>("SELECT CAST(? AS SIGNED)")
                .bind(41_i64)
                .fetch_one(&pool);
            let second = sqlx::query_scalar::<_, i64>("SELECT CAST(? AS SIGNED)")
                .bind(42_i64)
                .fetch_one(&pool);
            assert_eq!(
                tokio::try_join!(first, second).unwrap(),
                (41, 42),
                "{backend}"
            );
        }
        pool.close().await;
    }
}
