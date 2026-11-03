mod common;
use sqlx::Executor;
use sqlx_doris::{DorisConnectOptions, DorisPoolOptions, DorisSslMode};
use std::{path::PathBuf, time::Duration};

fn options() -> DorisConnectOptions {
    std::env::var("DORIS_TEST_URL")
        .expect("DORIS_TEST_URL required")
        .parse()
        .unwrap()
}
fn cert(name: &str) -> PathBuf {
    PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").expect("scripts/dev required"))
        .join("fixtures/tls")
        .join(name)
}

#[tokio::test]
#[ignore = "requires dedicated TLS-enabled Doris"]
async fn sql_tls_requires_encryption_and_checks_ca_and_hostname() {
    for mode in [
        DorisSslMode::Required,
        DorisSslMode::VerifyCa,
        DorisSslMode::VerifyIdentity,
    ] {
        let pool = DorisPoolOptions::new()
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(
                options()
                    .host("localhost")
                    .ssl_mode(mode)
                    .ssl_ca(cert("ca.pem")),
            )
            .await
            .unwrap();
        let n: i64 = sqlx::query_scalar("SELECT CAST(? AS BIGINT)")
            .bind(42_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(n, 42);
        pool.close().await;
    }
    for o in [
        options()
            .host("localhost")
            .ssl_mode(DorisSslMode::VerifyCa)
            .ssl_ca(cert("wrong-ca.pem")),
        options()
            .host("127.0.0.1")
            .ssl_mode(DorisSslMode::VerifyIdentity)
            .ssl_ca(cert("ca.pem")),
    ] {
        let error = DorisPoolOptions::new()
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(o)
            .await
            .unwrap_err();
        assert!(
            matches!(error, sqlx::Error::Tls(_) | sqlx::Error::Io(_)),
            "{error:?}"
        );
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris administrative account"]
async fn percent_encoded_passwords_authentication_and_permissions_are_preserved() {
    let admin = common::client().await;
    let table = common::name();
    admin.execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}(id BIGINT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"))).await.unwrap();
    let user = format!(
        "sdk_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let password = "Fixture:@/#% +?中文";
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE USER '{user}'@'%' IDENTIFIED BY '{password}'"
        )))
        .await
        .unwrap();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "GRANT SELECT_PRIV ON sqlx_doris_test.* TO '{user}'@'%'"
        )))
        .await
        .unwrap();
    let o = options().username(&user).password(password);
    use sqlx::ConnectOptions;
    let url = o.to_url_lossy().to_string();
    let pool = DorisPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .unwrap();
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
    assert!(
        pool.execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(1)"
        )))
        .await
        .unwrap_err()
        .as_database_error()
        .is_some()
    );
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
    let wrong = DorisPoolOptions::new()
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(o.password("wrong password"))
        .await
        .unwrap_err();
    assert!(wrong.as_database_error().is_some());
    let missing = DorisPoolOptions::new()
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options().database("_sdk_missing_database"))
        .await
        .unwrap_err();
    assert!(missing.as_database_error().is_some());
    pool.close().await;
    admin
        .execute(sqlx::AssertSqlSafe(format!("DROP USER '{user}'@'%'")))
        .await
        .unwrap();
    common::drop_table(&admin, &table).await;
    admin.close().await;
}
