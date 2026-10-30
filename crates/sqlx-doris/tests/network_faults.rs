mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;
use proxy::{MARKER, Mode, Proxy};
use sqlx_doris::{
    Backend, Error, PoolBuilder,
    operations::{Column, DataType, TableDefinition, TableModel},
    sqlx::{
        self, Executor,
        mysql::{MySqlConnectOptions, MySqlSslMode},
    },
};
use std::{env, str::FromStr, time::Duration};
fn options(backend: Backend) -> MySqlConnectOptions {
    MySqlConnectOptions::from_str(
        &env::var(if backend == Backend::MySql {
            "MYSQL_TEST_URL"
        } else {
            "DORIS_TEST_URL"
        })
        .unwrap(),
    )
    .unwrap()
}
async fn proxied(backend: Backend, proxy: &Proxy) -> sqlx_doris::Client {
    PoolBuilder::new(backend)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(3))
        .connect_with(proxy.options(options(backend)))
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris fixtures"]
async fn truncated_handshake_fails_with_a_deadline_and_required_tls_never_falls_back() {
    for backend in [Backend::MySql, Backend::Doris] {
        for mode in [Mode::HandshakeCut, Mode::NoTls] {
            let proxy = Proxy::start(&options(backend), mode).await;
            let settings =
                proxy
                    .options(options(backend))
                    .ssl_mode(if matches!(mode, Mode::NoTls) {
                        MySqlSslMode::Required
                    } else {
                        MySqlSslMode::Disabled
                    });
            let error = tokio::time::timeout(
                Duration::from_secs(3),
                PoolBuilder::new(backend)
                    .acquire_timeout(Duration::from_millis(300))
                    .connect_with(settings),
            )
            .await
            .expect("connect must terminate")
            .unwrap_err();
            assert!(matches!(error, Error::Database(_)));
            assert_eq!(
                proxy.auth_packets(),
                0,
                "no plaintext authentication may follow a required-TLS failure"
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris fixtures"]
async fn truncated_result_row_evicts_the_bad_connection_and_restores_pool_capacity() {
    for backend in [Backend::MySql, Backend::Doris] {
        let proxy = Proxy::start(&options(backend), Mode::RowCut).await;
        let db = proxied(backend, &proxy).await;
        let sql = format!("SELECT REPEAT('x',4096) AS value /* {MARKER} */");
        let error = sqlx::raw_sql(sqlx::AssertSqlSafe(sql))
            .fetch_all(db.pool())
            .await
            .unwrap_err();
        assert!(
            matches!(error, sqlx::Error::Io(_) | sqlx::Error::Protocol(_)),
            "{error:?}"
        );
        assert!(
            proxy.cut_size() >= 4096,
            "the fault must truncate row data, not just metadata"
        );
        let answer: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
            .bind(42_i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(answer, 42);
        assert_eq!(proxy.hits(), 1);
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris fixtures"]
async fn response_lost_after_committed_sql_write_is_not_replayed() {
    for backend in [Backend::MySql, Backend::Doris] {
        let admin = common::client(backend).await;
        let table = common::name();
        if backend == Backend::MySql {
            admin
                .pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {table} (id BIGINT)"
                )))
                .await
                .unwrap();
        } else {
            admin
                .create_table(
                    &TableDefinition::new(table.clone(), TableModel::Duplicate, ["id"])
                        .column(Column::new("id", DataType::BigInt).not_null())
                        .replication(1),
                )
                .await
                .unwrap();
        }
        let proxy = Proxy::start(&options(backend), Mode::LostWriteReply).await;
        let db = proxied(backend, &proxy).await;
        let error = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (1) /* {MARKER} */"
        )))
        .execute(db.pool())
        .await
        .unwrap_err();
        assert!(
            matches!(error, sqlx::Error::Io(_) | sqlx::Error::Protocol(_)),
            "{error:?}"
        );
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(count, 1);
        assert_eq!(
            proxy.hits(),
            1,
            "the uncertain write must not be submitted twice"
        );
        db.close().await;
        common::drop_table(&admin, &table).await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris fixtures"]
async fn query_future_cancellation_does_not_mix_the_late_result_with_the_next_query() {
    for backend in [Backend::MySql, Backend::Doris] {
        let proxy = Proxy::start(&options(backend), Mode::PauseReply).await;
        let db = proxied(backend, &proxy).await;
        {
            let query = sqlx::raw_sql(sqlx::AssertSqlSafe(format!("SELECT 99 /* {MARKER} */")))
                .fetch_all(db.pool());
            tokio::pin!(query);
            tokio::select! {
                result = &mut query => panic!("query finished before release: {result:?}"),
                _ = proxy.reached() => {}
            }
        }
        proxy.release();
        let value: i64 = tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
                .bind(42_i64)
                .fetch_one(db.pool()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(value, 42);
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn cancelling_doris_begin_or_execute_closes_the_owned_transaction_connection() {
    let admin = common::client(Backend::Doris).await;
    let table = common::name();
    admin
        .create_table(
            &TableDefinition::new(table.clone(), TableModel::Duplicate, ["id"])
                .column(Column::new("id", DataType::BigInt).not_null())
                .replication(1),
        )
        .await
        .unwrap();
    for mode in [Mode::PauseBegin, Mode::PauseReply] {
        let proxy = Proxy::start(&options(Backend::Doris), mode).await;
        let db = proxied(Backend::Doris, &proxy).await;
        if matches!(mode, Mode::PauseBegin) {
            let begin = db.doris_transaction();
            tokio::pin!(begin);
            tokio::select! {
                result = &mut begin => panic!("BEGIN completed before release: {result:?}"),
                _ = proxy.reached() => {}
            }
        } else {
            let mut tx = db.doris_transaction().await.unwrap();
            {
                let execute = tx.execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES (1) /* {MARKER} */"
                )));
                tokio::pin!(execute);
                tokio::select! {
                    result = &mut execute => panic!("execute completed before release: {result:?}"),
                    _ = proxy.reached() => {}
                }
            }
            assert!(matches!(
                tx.commit().await.unwrap_err(),
                Error::Unsupported(_)
            ));
        }
        let count: i64 = tokio::time::timeout(
            Duration::from_secs(5),
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(db.pool()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(count, 0);
        db.close().await;
    }
    common::drop_table(&admin, &table).await;
    admin.close().await;
}
