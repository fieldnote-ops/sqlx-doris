mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;
use futures_util::StreamExt;
use sqlx_doris::{
    Backend, PoolBuilder,
    sqlx::{self, Connection, Executor},
};
use std::time::Duration;
use std::{
    future::{Future, poll_fn},
    str::FromStr,
    task::Poll,
};
async fn table(backend: Backend) -> (sqlx_doris::Client, sqlx_doris::operations::TableName) {
    let db = common::client(backend).await;
    let name = common::name();
    let suffix = if backend == Backend::Doris {
        "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        ""
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {name}(id BIGINT,value BIGINT) {suffix}"
        )))
        .await
        .unwrap();
    (db, name)
}
#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn single_row_fetch_then_begin_keeps_the_native_transaction_api() {
    for backend in [Backend::MySql, Backend::Doris] {
        let (db, t) = table(backend).await;
        println!("read then begin {backend:?}: {t}");
        let mut c = db.pool().acquire().await.unwrap();
        let value: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
            .bind(42_i64)
            .fetch_one(&mut *c)
            .await
            .unwrap();
        assert_eq!(value, 42);
        let mut tx: sqlx::Transaction<'_, sqlx::MySql> = c.begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO {t} VALUES(?,?)")))
            .bind(1_i64)
            .bind(value)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        drop(c);
        let rows: Vec<(i64, i64)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT id,value FROM {t}")))
                .fetch_all(db.pool())
                .await
                .unwrap();
        assert_eq!(rows, [(1, 42)]);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn fresh_connection_hook_fetch_one_can_be_followed_by_begin() {
    for backend in [Backend::MySql, Backend::Doris] {
        let (admin, t) = table(backend).await;
        let url = std::env::var(if backend == Backend::MySql {
            "MYSQL_TEST_URL"
        } else {
            "DORIS_TEST_URL"
        })
        .unwrap();
        let db = PoolBuilder::new(backend)
            .max_connections(2)
            .min_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .after_connect(|conn| {
                Box::pin(async move {
                    let n: i64 = sqlx::query_scalar("SELECT CAST(42 AS SIGNED)")
                        .fetch_one(conn)
                        .await?;
                    assert_eq!(n, 42);
                    Ok(())
                })
            })
            .connect(&url)
            .await
            .unwrap();
        let blocker = db.pool().acquire().await.unwrap();
        println!("fresh hook then begin {backend:?}: {t}");
        let mut tx: sqlx::Transaction<'_, sqlx::MySql> = db.pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO {t} VALUES(?,?)")))
            .bind(1_i64)
            .bind(42_i64)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        drop(blocker);
        let rows: Vec<(i64, i64)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT id,value FROM {t}")))
                .fetch_all(admin.pool())
                .await
                .unwrap();
        assert_eq!(rows, [(1, 42)]);
        db.close().await;
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}

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

async fn via_proxy(backend: Backend, proxy: &proxy::Proxy) -> sqlx_doris::Client {
    PoolBuilder::new(backend)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(proxy.options(options(backend)))
        .await
        .unwrap()
}

fn begins(proxy: &proxy::Proxy) -> usize {
    proxy
        .trace()
        .iter()
        .filter(|p| p.first() == Some(&3) && &p[1..] == b"BEGIN")
        .count()
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn partial_reads_cache_and_time_keep_commit_rollback_and_drop_semantics() {
    for backend in [Backend::MySql, Backend::Doris] {
        let (db, t) = table(backend).await;
        let mut committed = Vec::new();
        let mut id = 0_i64;
        for persistent in [false, true] {
            for read in 0..4 {
                for action in 0..3 {
                    id += 1;
                    let mut conn = db.pool().acquire().await.unwrap();
                    match read {
                        0 => {
                            let row: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
                                .bind(42_i64)
                                .persistent(persistent)
                                .fetch_one(&mut *conn)
                                .await
                                .unwrap();
                            assert_eq!(row, 42);
                        }
                        1 => {
                            let row: Option<i64> = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
                                .bind(42_i64)
                                .persistent(persistent)
                                .fetch_optional(&mut *conn)
                                .await
                                .unwrap();
                            assert_eq!(row, Some(42));
                        }
                        2 => {
                            let mut rows = sqlx::query_scalar::<_, i64>(
                                "SELECT CAST(? AS SIGNED) UNION ALL SELECT CAST(? AS SIGNED)",
                            )
                            .bind(42_i64)
                            .bind(43_i64)
                            .persistent(persistent)
                            .fetch(&mut *conn);
                            assert_eq!(rows.next().await.unwrap().unwrap(), 42);
                            // Leave both an unread second row and the terminating packet.
                        }
                        _ => {
                            let value = sqlx::mysql::types::MySqlTime::new(
                                sqlx::mysql::types::MySqlTimeSign::Negative,
                                0,
                                0,
                                0,
                                1,
                            )
                            .unwrap();
                            let row: sqlx::mysql::types::MySqlTime =
                                sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                                    .bind(value)
                                    .persistent(persistent)
                                    .fetch_one(&mut *conn)
                                    .await
                                    .unwrap();
                            assert_eq!(row, value);
                        }
                    }
                    let mut tx: sqlx::Transaction<'_, sqlx::MySql> = conn.begin().await.unwrap();
                    sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO {t} VALUES(?,?)")))
                        .bind(id)
                        .bind(42_i64)
                        .persistent(persistent)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    match action {
                        0 => {
                            tx.commit().await.unwrap();
                            committed.push((id, 42_i64));
                        }
                        1 => tx.rollback().await.unwrap(),
                        _ => drop(tx),
                    }
                }
            }
        }
        let actual: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id,value FROM {t} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(actual, committed);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn cancelled_or_lost_read_tail_never_sends_begin_and_doris_retires() {
    for backend in [Backend::MySql, Backend::Doris] {
        for cancelled in [true, false] {
            let proxy = proxy::Proxy::start(
                &options(backend),
                if cancelled {
                    proxy::Mode::PauseRowEnd
                } else {
                    proxy::Mode::LostRowEnd
                },
            )
            .await;
            let db = via_proxy(backend, &proxy).await;
            let mut conn = db.pool().acquire().await.unwrap();
            let row = (&mut *conn)
                .fetch_one(sqlx::AssertSqlSafe(format!(
                    "SELECT CAST(42 AS SIGNED) /* {} */",
                    proxy::MARKER
                )))
                .await
                .unwrap();
            use sqlx::Row;
            assert_eq!(row.get::<i64, _>(0), 42);
            proxy.reached().await;
            if cancelled {
                {
                    let mut begin = Box::pin(conn.begin());
                    poll_fn(|cx| {
                        assert!(
                            begin.as_mut().poll(cx).is_pending(),
                            "BEGIN must await the held row terminator"
                        );
                        Poll::Ready(())
                    })
                    .await;
                }
                assert_eq!(begins(&proxy), 0);
                proxy.release();
                if backend == Backend::Doris {
                    let error = conn.begin().await.unwrap_err();
                    assert!(error.to_string().contains("interrupted"), "{error:?}");
                    assert!(conn.ping().await.is_err());
                } else {
                    conn.begin().await.unwrap().rollback().await.unwrap();
                    assert_eq!(begins(&proxy), 1);
                }
            } else {
                assert!(
                    tokio::time::timeout(Duration::from_secs(5), conn.begin())
                        .await
                        .unwrap()
                        .is_err()
                );
                assert_eq!(begins(&proxy), 0);
                if backend == Backend::Doris {
                    assert!(
                        conn.begin()
                            .await
                            .unwrap_err()
                            .to_string()
                            .contains("interrupted")
                    );
                    assert!(conn.ping().await.is_err());
                }
            }
            drop(conn);
            let n: i64 = tokio::time::timeout(
                Duration::from_secs(5),
                sqlx::query_scalar("SELECT CAST(42 AS SIGNED)").fetch_one(db.pool()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(n, 42);
            assert_eq!(proxy.hits(), 1);
            db.close().await;
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn pending_write_reply_is_still_rejected_without_begin_or_replay() {
    let (admin, t) = table(Backend::Doris).await;
    let proxy = proxy::Proxy::start(&options(Backend::Doris), proxy::Mode::PauseReply).await;
    let db = via_proxy(Backend::Doris, &proxy).await;
    let mut conn = db.pool().acquire().await.unwrap();
    {
        let mut write = Box::pin((&mut *conn).execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t} VALUES(1,42) /* {} */",
            proxy::MARKER
        ))));
        tokio::select! {r=&mut write=>panic!("unexpected write result: {r:?}"),_=proxy.reached()=>{}}
    }
    assert_eq!(proxy.fault_reply(), 0, "the server accepted this one write");
    let error = conn.begin().await.unwrap_err();
    assert!(error.to_string().contains("unfinished query"), "{error:?}");
    assert_eq!(begins(&proxy), 0);
    proxy.release();
    drop(conn);
    let n: i64 = sqlx::query_scalar("SELECT CAST(42 AS SIGNED)")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(n, 42);
    let writes = proxy
        .trace()
        .into_iter()
        .filter(|p| p.first() == Some(&3) && p[1..].starts_with(b"INSERT INTO"))
        .count();
    assert_eq!(writes, 1);
    let rows: Vec<(i64, i64)> =
        sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT id,value FROM {t}")))
            .fetch_all(admin.pool())
            .await
            .unwrap();
    assert_eq!(rows, [(1, 42)]);
    db.close().await;
    common::drop_table(&admin, &t).await;
    admin.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn cancelled_metadata_or_next_row_is_not_an_acknowledged_row_boundary() {
    for metadata in [true, false] {
        println!("read cancellation metadata={metadata}: connect");
        let proxy = proxy::Proxy::start(
            &options(Backend::Doris),
            if metadata {
                proxy::Mode::PauseRowMetadata
            } else {
                proxy::Mode::PauseSecondRow
            },
        )
        .await;
        let db = via_proxy(Backend::Doris, &proxy).await;
        let mut conn = db.pool().acquire().await.unwrap();
        {
            let sql = sqlx::AssertSqlSafe(format!(
                "SELECT CAST(42 AS SIGNED) UNION ALL SELECT CAST(43 AS SIGNED) /* {} */",
                proxy::MARKER
            ));
            let mut rows = (&mut *conn).fetch(sql);
            if !metadata {
                use sqlx::Row;
                assert_eq!(rows.next().await.unwrap().unwrap().get::<i64, _>(0), 42);
            }
            let mut next = Box::pin(rows.next());
            // Poll the read before accepting an already delivered proxy notification.
            // Otherwise cancellation might precede any attempt to read the next row.
            tokio::select! {biased; r=&mut next=>panic!("unexpected row: {r:?}"),_=proxy.reached()=>{}}
        }
        println!("read cancellation metadata={metadata}: reject BEGIN");
        let error = tokio::time::timeout(Duration::from_secs(10), conn.begin())
            .await
            .expect("cancelled read BEGIN timed out")
            .unwrap_err();
        assert!(error.to_string().contains("unfinished query"), "{error:?}");
        assert_eq!(begins(&proxy), 0);
        proxy.release();
        println!("read cancellation metadata={metadata}: retired ping");
        assert!(
            tokio::time::timeout(Duration::from_secs(10), conn.ping())
                .await
                .expect("retired ping timed out")
                .is_err()
        );
        drop(conn);
        println!("read cancellation metadata={metadata}: recover pool");
        let n: i64 = tokio::time::timeout(
            Duration::from_secs(10),
            sqlx::query_scalar("SELECT CAST(42 AS SIGNED)").fetch_one(db.pool()),
        )
        .await
        .expect("pool recovery timed out")
        .unwrap();
        assert_eq!(n, 42);
        println!("read cancellation metadata={metadata}: close pool");
        tokio::time::timeout(Duration::from_secs(10), db.close())
            .await
            .expect("pool close timed out");
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris; mutates only a proxy EOF status flag"]
async fn unexpected_additional_result_is_rejected_before_begin() {
    let proxy = proxy::Proxy::start(&options(Backend::Doris), proxy::Mode::MoreRowEnd).await;
    let db = via_proxy(Backend::Doris, &proxy).await;
    let mut conn = db.pool().acquire().await.unwrap();
    (&mut *conn)
        .fetch_one(sqlx::AssertSqlSafe(format!(
            "SELECT 42 /* {} */",
            proxy::MARKER
        )))
        .await
        .unwrap();
    proxy.reached().await;
    let error = conn.begin().await.unwrap_err();
    assert!(error.to_string().contains("multiple results"), "{error:?}");
    assert_eq!(begins(&proxy), 0);
    assert!(conn.ping().await.is_err());
    drop(conn);
    let n: i64 = sqlx::query_scalar("SELECT CAST(42 AS SIGNED)")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(n, 42);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris; truncates only a proxy EOF packet"]
async fn short_read_terminator_is_an_error_and_retires_without_begin() {
    let proxy = proxy::Proxy::start(&options(Backend::Doris), proxy::Mode::ShortRowEnd).await;
    let db = via_proxy(Backend::Doris, &proxy).await;
    let mut conn = db.pool().acquire().await.unwrap();
    (&mut *conn)
        .fetch_one(sqlx::AssertSqlSafe(format!(
            "SELECT 42 /* {} */",
            proxy::MARKER
        )))
        .await
        .unwrap();
    proxy.reached().await;
    let error = conn.begin().await.unwrap_err();
    assert!(error.to_string().contains("truncated EOF"), "{error:?}");
    assert_eq!(begins(&proxy), 0);
    assert!(conn.ping().await.is_err());
    drop(conn);
    let n: i64 = sqlx::query_scalar("SELECT CAST(42 AS SIGNED)")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(n, 42);
    db.close().await;
}
