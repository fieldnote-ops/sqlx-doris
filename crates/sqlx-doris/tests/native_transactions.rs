mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;

use sqlx_doris::{
    Backend, Client, PoolBuilder,
    sqlx::{self, Acquire, Connection, Executor, SqlSafeStr},
};
use std::{str::FromStr, time::Duration};

fn options(backend: Backend) -> sqlx::mysql::MySqlConnectOptions {
    sqlx::mysql::MySqlConnectOptions::from_str(
        &std::env::var(if backend == Backend::Doris {
            "DORIS_TEST_URL"
        } else {
            "MYSQL_TEST_URL"
        })
        .unwrap(),
    )
    .unwrap()
}

async fn via_proxy(backend: Backend, proxy: &proxy::Proxy) -> Client {
    PoolBuilder::new(backend)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(3))
        .connect_with(proxy.options(options(backend)))
        .await
        .unwrap()
}

async fn count(db: &Client, table: &sqlx_doris::operations::TableName) -> i64 {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
        .fetch_one(db.pool())
        .await
        .unwrap()
}

async fn create(db: &Client) -> sqlx_doris::operations::TableName {
    let table = common::name();
    let suffix = if db.backend() == Backend::Doris {
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        ""
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {table}(id BIGINT,value BIGINT) {suffix}"
        )))
        .await
        .unwrap();
    table
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn native_top_level_transactions_commit_rollback_and_drop_on_both_backends() {
    for backend in [Backend::MySql, Backend::Doris] {
        println!("native transaction lifecycle: {backend:?}");
        let db = PoolBuilder::new(backend)
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(3))
            .connect(
                &std::env::var(if backend == Backend::Doris {
                    "DORIS_TEST_URL"
                } else {
                    "MYSQL_TEST_URL"
                })
                .unwrap(),
            )
            .await
            .unwrap();
        assert!(db.capabilities().sqlx_transactions);
        let table = create(&db).await;
        let mut tx: sqlx::Transaction<'_, sqlx::MySql> = db.pool().begin().await.unwrap();
        (&mut *tx)
            .execute(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(1,10)"
            )))
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        (&mut *tx)
            .execute(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(2,20)"
            )))
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        (&mut *tx)
            .execute(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(3,30)"
            )))
            .await
            .unwrap();
        drop(tx);
        let rows: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id,value FROM {table} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(rows, [(1, 10)]);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn safe_binding_preserves_prior_and_later_text_writes() {
    let db = common::client(Backend::Doris).await;
    let table = create(&db).await;
    let mut tx = db.pool().begin().await.unwrap();
    (&mut *tx)
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(1,10)"
        )))
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES(?,?)"
    )))
    .bind(2_i64)
    .bind(20_i64)
    .execute(&mut *tx)
    .await
    .unwrap();
    (&mut *tx)
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(3,30)"
        )))
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let rows: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,value FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(rows, [(1, 10), (2, 20), (3, 30)]);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn unsupported_native_nested_begin_retires_outer_transaction_without_false_commit() {
    let db = common::client(Backend::Doris).await;
    let table = create(&db).await;
    let mut tx = db.pool().begin().await.unwrap();
    (&mut *tx)
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(1,10)"
        )))
        .await
        .unwrap();
    let error = tx.begin().await.unwrap_err();
    assert!(error.to_string().contains("savepoints"));
    assert!(
        tx.commit().await.is_err(),
        "an unsupported nested begin must not produce false commit success"
    );
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn cancelled_begin_and_cancelled_write_cannot_commit_or_strand_the_pool() {
    let admin = common::client(Backend::Doris).await;
    let table = create(&admin).await;
    for during_begin in [true, false] {
        let proxy = proxy::Proxy::start(
            &options(Backend::Doris),
            if during_begin {
                proxy::Mode::PauseBegin
            } else {
                proxy::Mode::PauseReply
            },
        )
        .await;
        let db = via_proxy(Backend::Doris, &proxy).await;
        if during_begin {
            let future = db.pool().begin();
            tokio::pin!(future);
            tokio::select! { result=&mut future => panic!("unexpected completion: {result:?}"), _=proxy.reached()=>{} }
        } else {
            let mut tx = db.pool().begin().await.unwrap();
            (&mut *tx)
                .execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES(1,10)"
                )))
                .await
                .unwrap();
            {
                let future = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES(2,20) /* {} */",
                    proxy::MARKER
                )))
                .execute(&mut *tx);
                tokio::pin!(future);
                tokio::select! {result=&mut future=>panic!("unexpected completion: {result:?}"),_=proxy.reached()=>{}}
            }
            let error = tx.commit().await.unwrap_err();
            assert!(error.to_string().contains("unfinished query"), "{error:?}");
        }
        proxy.release();
        let recovered = tokio::time::timeout(
            Duration::from_secs(3),
            sqlx::query_scalar::<_, i64>("SELECT 42").fetch_one(db.pool()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recovered, 42);
        assert_eq!(count(&admin, &table).await, 0);
        assert_eq!(proxy.hits(), 1);
        db.close().await;
    }
    common::drop_table(&admin, &table).await;
    admin.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn lost_or_cancelled_commit_reply_never_replays_a_committed_transaction() {
    for backend in [Backend::MySql, Backend::Doris] {
        let admin = common::client(backend).await;
        for cancelled in [false, true] {
            let table = create(&admin).await;
            let proxy = proxy::Proxy::start(
                &options(backend),
                if cancelled {
                    proxy::Mode::PauseCommit
                } else {
                    proxy::Mode::LostCommitReply
                },
            )
            .await;
            let db = via_proxy(backend, &proxy).await;
            let mut tx = db.pool().begin().await.unwrap();
            (&mut *tx)
                .execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES(1,10)"
                )))
                .await
                .unwrap();
            if cancelled {
                let future = tx.commit();
                tokio::pin!(future);
                tokio::select! {result=&mut future=>panic!("unexpected completion: {result:?}"),_=proxy.reached()=>{}}
            } else {
                let error = tx.commit().await.unwrap_err();
                assert!(
                    matches!(error, sqlx::Error::Io(_) | sqlx::Error::Protocol(_)),
                    "{error:?}"
                );
            }
            proxy.release();
            assert_eq!(
                count(&admin, &table).await,
                1,
                "server committed before proxy withheld its reply"
            );
            let answer = tokio::time::timeout(
                Duration::from_secs(3),
                sqlx::query_scalar::<_, i64>("SELECT 42").fetch_one(db.pool()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(answer, 42);
            assert_eq!(proxy.hits(), 1, "COMMIT must never be retried");
            assert_eq!(count(&admin, &table).await, 1);
            db.close().await;
            common::drop_table(&admin, &table).await;
        }
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn direct_native_sqlx_pool_requires_explicit_option_and_custom_begin_is_rejected() {
    let plain = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(options(Backend::Doris))
        .await
        .unwrap();
    let plain = Client::from_pool(Backend::Doris, plain);
    assert!(!plain.capabilities().sqlx_transactions);
    plain.close().await;
    let pool: sqlx::MySqlPool = sqlx::mysql::MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(options(Backend::Doris).doris_transaction_compatibility(true))
        .await
        .unwrap();
    let db = Client::from_pool(Backend::Doris, pool);
    assert!(db.capabilities().sqlx_transactions);
    let table = create(&db).await;
    let mut conn = db.pool().acquire().await.unwrap();
    let error = sqlx::Connection::begin_with(&mut *conn, "BEGIN")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("standard BEGIN"));
    let mut tx = conn.begin().await.unwrap();
    (&mut *tx)
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(1,10)"
        )))
        .await
        .unwrap();
    drop(tx);
    assert!(
        conn.ping().await.is_err(),
        "borrowed dropped transaction connection must be closed"
    );
    conn.close().await.unwrap();
    assert_eq!(count(&db, &table).await, 0);
    db.pool().begin().await.unwrap().rollback().await.unwrap();
    let mut owned = db.pool().begin().await.unwrap();
    (&mut *owned)
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(2,20)"
        )))
        .await
        .unwrap();
    drop(owned);
    assert_eq!(
        count(&db, &table).await,
        0,
        "plain SQLx pools must retire dropped Doris transactions without SDK hooks"
    );
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn cached_parameter_count_error_is_preflight_and_preserves_prior_write() {
    let db = common::client(Backend::Doris).await;
    let table = create(&db).await;
    let mut conn = db.pool().acquire().await.unwrap();
    let sql = format!("INSERT INTO {table} VALUES(?,?)");
    conn.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    (&mut *tx)
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(1,10)"
        )))
        .await
        .unwrap();
    let error = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(2_i64)
        .execute(&mut *tx)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("parameter count"), "{error:?}");
    tx.commit().await.unwrap();
    drop(conn);
    assert_eq!(count(&db, &table).await, 1);
    common::drop_table(&db, &table).await;
    db.close().await;
}
