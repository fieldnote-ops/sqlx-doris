mod common;
use sqlx_doris::{
    Backend, Error, PoolBuilder,
    operations::{Column, DataType as T, TableDefinition, TableModel},
    sqlx::{
        self, Executor,
        mysql::{MySqlConnectOptions, MySqlSslMode},
    },
};
use std::{
    env,
    path::PathBuf,
    str::FromStr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn options(backend: Backend) -> MySqlConnectOptions {
    MySqlConnectOptions::from_str(
        &env::var(if backend == Backend::MySql {
            "MYSQL_TEST_URL"
        } else {
            "DORIS_TEST_URL"
        })
        .expect("dedicated URL required"),
    )
    .unwrap()
}
fn cert(name: &str) -> PathBuf {
    let file = PathBuf::from(env::var_os("SQLX_DORIS_BUILD_ROOT").expect("scripts/dev required"))
        .join("fixtures/tls")
        .join(name);
    assert!(file.is_file(), "run scripts/prepare-test-certificates");
    file
}
fn encoded(value: &str) -> String {
    value
        .bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

#[tokio::test]
#[ignore = "requires dedicated TLS-enabled MySQL and Doris fixtures"]
async fn required_verified_ca_and_verified_identity_work_and_reject_wrong_ca_or_name() {
    for backend in [Backend::MySql, Backend::Doris] {
        for mode in [
            MySqlSslMode::Required,
            MySqlSslMode::VerifyCa,
            MySqlSslMode::VerifyIdentity,
        ] {
            let settings = options(backend)
                .host("localhost")
                .ssl_mode(mode)
                .ssl_ca(cert("ca.pem"));
            let db = PoolBuilder::new(backend)
                .acquire_timeout(Duration::from_secs(5))
                .connect_with(settings)
                .await
                .unwrap();
            let answer: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
                .bind(42_i64)
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(answer, 42);
            db.close().await;
        }
        for (settings, reason) in [
            (
                options(backend)
                    .host("localhost")
                    .ssl_mode(MySqlSslMode::VerifyCa)
                    .ssl_ca(cert("wrong-ca.pem")),
                "UnknownIssuer",
            ),
            (
                options(backend)
                    .host("127.0.0.1")
                    .ssl_mode(MySqlSslMode::VerifyIdentity)
                    .ssl_ca(cert("ca.pem")),
                "NotValidForName",
            ),
        ] {
            let error = PoolBuilder::new(backend)
                .acquire_timeout(Duration::from_secs(5))
                .connect_with(settings)
                .await
                .unwrap_err();
            let description = format!("{error:?}");
            assert!(
                matches!(
                    error,
                    Error::Database(sqlx::Error::Tls(_) | sqlx::Error::Io(_))
                ),
                "{backend:?}: {description}"
            );
            assert!(description.contains(reason), "{backend:?}: {description}");
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris fixtures with administrative setup access"]
async fn password_url_encoding_permissions_and_authentication_errors_preserve_pool_behavior() {
    const PASSWORD: &str = "Fixture:@/#% +?中文";
    for backend in [Backend::MySql, Backend::Doris] {
        let admin = common::client(backend).await;
        let table = common::name();
        let user = format!(
            "sdk_{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        if backend == Backend::MySql {
            admin
                .pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {table} (id BIGINT PRIMARY KEY)"
                )))
                .await
                .unwrap();
        } else {
            admin
                .create_table(
                    &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                        .column(Column::new("id", T::BigInt).not_null())
                        .replication(1),
                )
                .await
                .unwrap();
        }
        // Both identifiers and this synthetic password are owned fixture values.
        admin
            .pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE USER '{user}'@'%' IDENTIFIED BY '{PASSWORD}'"
            )))
            .await
            .unwrap();
        let privilege = if backend == Backend::MySql {
            "SELECT"
        } else {
            "SELECT_PRIV"
        };
        admin
            .pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "GRANT {privilege} ON sqlx_doris_test.* TO '{user}'@'%'"
            )))
            .await
            .unwrap();
        let base = options(backend);
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            encoded(&user),
            encoded(PASSWORD),
            base.get_host(),
            base.get_port(),
            encoded(base.get_database().unwrap())
        );
        let app = PoolBuilder::new(backend)
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&url)
            .await
            .unwrap();
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(app.pool())
                .await
                .unwrap();
        assert_eq!(count, 0);
        let denied = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?)"
        )))
        .bind(1_i64)
        .execute(app.pool())
        .await
        .unwrap_err();
        assert!(denied.as_database_error().is_some());
        let again: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(app.pool())
                .await
                .unwrap();
        assert_eq!(again, 0);
        let denied = PoolBuilder::new(backend)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(
                base.clone()
                    .username(&user)
                    .password("wrong fixture password"),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(denied, Error::Database(sqlx::Error::Database(_))),
            "{backend:?}: {denied:?}"
        );
        let missing = PoolBuilder::new(backend)
            .acquire_timeout(Duration::from_secs(5))
            .connect_with(base.database("_sqlx_doris_missing_database"))
            .await
            .unwrap_err();
        assert!(
            matches!(missing, Error::Database(sqlx::Error::Database(_))),
            "{backend:?}: {missing:?}"
        );
        app.close().await;
        admin
            .pool()
            .execute(sqlx::AssertSqlSafe(format!("DROP USER '{user}'@'%'")))
            .await
            .unwrap();
        common::drop_table(&admin, &table).await;
        admin.close().await;
    }
}
