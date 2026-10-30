//! Per-request encoding equivalence must not cache a mutable session mode.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    sqlx::{
        self, Executor,
        mysql::{
            MySqlConnectOptions,
            types::{MySqlTime, MySqlTimeSign},
        },
    },
};
use std::{str::FromStr, time::Duration};

fn options(b: Backend) -> MySqlConnectOptions {
    MySqlConnectOptions::from_str(
        &std::env::var(if b == Backend::Doris {
            "DORIS_TEST_URL"
        } else {
            "MYSQL_TEST_URL"
        })
        .expect("dedicated URL required"),
    )
    .unwrap()
}
async fn proxied(b: Backend, mode: proxy::Mode) -> (Client, proxy::Proxy) {
    let p = proxy::Proxy::start(&options(b), mode).await;
    let c = PoolBuilder::new(b)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(4))
        .connect_with(p.options(options(b)))
        .await
        .unwrap();
    (c, p)
}
fn mode_reads(trace: &[Vec<u8>]) -> usize {
    trace
        .iter()
        .filter(|p| p[0] == 3 && p[1..] == *b"SELECT @@sql_mode")
        .count()
}
fn time(i: usize) -> MySqlTime {
    match i % 4 {
        0 => MySqlTime::ZERO,
        1 => MySqlTime::new(MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap(),
        2 => MySqlTime::new(MySqlTimeSign::Positive, 27, 2, 3, 123456).unwrap(),
        _ => MySqlTime::new(MySqlTimeSign::Negative, 123, 45, 6, 654321).unwrap(),
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn independent_time_pagination_bytes_and_metadata_fallback_skip_mode_reads() {
    for b in [Backend::MySql, Backend::Doris] {
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for i in 0..4 {
                let before = p.trace().len();
                let got: (MySqlTime, String, Option<i64>, String) = sqlx::query_as(
                    "SELECT CAST(? AS TIME(6)),? AS v,? AS n,'? literal' AS q LIMIT ? OFFSET ? \
                    /* MODE_FREE */",
                )
                .bind(time(i))
                .bind("O'Reilly 中文🙂")
                .bind(None::<i64>)
                .bind(1_i64)
                .bind(0_i64)
                .persistent(i % 2 == 0)
                .fetch_one(&mut *c)
                .await
                .unwrap();
                assert_eq!(
                    got,
                    (time(i), "O'Reilly 中文🙂".into(), None, "? literal".into())
                );
                assert_eq!(mode_reads(&p.trace()[before..]), 0, "{b:?}/{mode}");
                let before = p.trace().len();
                let fallback: (MySqlTime, String) = sqlx::query_as(
                    "SELECT CAST('-00:00:00.000001' AS TIME(6)),? AS v /* MODE_FREE_METADATA */",
                )
                .bind("ordinary ' text")
                .persistent(i % 2 == 0)
                .fetch_one(&mut *c)
                .await
                .unwrap();
                assert_eq!(fallback, (time(1), "ordinary ' text".into()));
                assert_eq!(mode_reads(&p.trace()[before..]), 0, "metadata {b:?}/{mode}");
            }
            let before = p.trace().len();
            let bytes: Vec<u8> = (0..=255).collect();
            let got: (MySqlTime, Vec<u8>) =
                sqlx::query_as("SELECT CAST(? AS TIME(6)),? AS b LIMIT ?")
                    .bind(time(1))
                    .bind(bytes.clone())
                    .bind(1_i64)
                    .fetch_one(&mut *c)
                    .await
                    .unwrap();
            assert_eq!(got, (time(1), bytes));
            assert_eq!(mode_reads(&p.trace()[before..]), 0, "bytes {b:?}/{mode}");
            if b == Backend::Doris {
                let before = p.trace().len();
                let error = sqlx::query("SELECT CAST(? AS TIME(6)) LIMIT ?")
                    .bind(time(1))
                    .bind(-1_i64)
                    .fetch_one(&mut *c)
                    .await
                    .unwrap_err();
                assert!(matches!(error, sqlx::Error::Protocol(_)));
                assert_eq!(p.trace().len(), before, "invalid pagination sent a command");
            }
        }
        if b == Backend::MySql {
            assert!(p.trace().iter().any(|p| p[0] == 23));
        }
        drop(c);
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn sensitive_values_follow_actual_mode_after_switches_on_the_same_sql() {
    let sensitive = format!(
        "{}\\'; SELECT ? -- 中文🙂",
        (0_u8..=31).map(char::from).collect::<String>()
    );
    for b in [Backend::MySql, Backend::Doris] {
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
            "",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for value in [
                "simple ' 中文",
                sensitive.as_str(),
                "simple ' 中文",
                sensitive.as_str(),
            ] {
                for fallback in [false, true] {
                    let before = p.trace().len();
                    let got: (MySqlTime, String) = if fallback {
                        sqlx::query_as(
                            "SELECT CAST('-00:00:00.000001' AS TIME(6)),? AS v /* MODE_SENSITIVE_METADATA */")
                            .bind(value).persistent(true).fetch_one(&mut *c).await.unwrap()
                    } else {
                        sqlx::query_as(
                            "SELECT CAST(? AS TIME(6)),? AS v LIMIT ? /* MODE_SENSITIVE */",
                        )
                        .bind(time(1))
                        .bind(value)
                        .bind(1_i64)
                        .persistent(true)
                        .fetch_one(&mut *c)
                        .await
                        .unwrap()
                    };
                    assert_eq!(got, (time(1), value.into()), "{b:?}/{mode}");
                    let expected = usize::from(b == Backend::Doris && value == sensitive);
                    assert_eq!(
                        mode_reads(&p.trace()[before..]),
                        expected,
                        "{b:?}/{mode}/{fallback}"
                    );
                }
            }
        }
        drop(c);
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn independent_writes_are_not_replayed_after_cancel_or_lost_reply() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = common::client(b).await;
        let t = common::name();
        let schema = if b == Backend::Doris {
            "(id BIGINT,n BIGINT,t VARCHAR(64)) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) \
                BUCKETS 1 PROPERTIES('replication_num'='1')"
        } else {
            "(id BIGINT PRIMARY KEY,n BIGINT,t TIME(6))"
        };
        admin
            .pool()
            .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t}{schema}")))
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t} VALUES(1,0,?)"
        )))
        .bind(time(0))
        .execute(admin.pool())
        .await
        .unwrap();
        for persistent in [false, true] {
            for lost in [false, true] {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {t} SET n=0,t=? WHERE id=1"
                )))
                .bind(time(0))
                .execute(admin.pool())
                .await
                .unwrap();
                let mode = match (b, lost) {
                    (Backend::Doris, false) => proxy::Mode::PauseReply,
                    (Backend::Doris, true) => proxy::Mode::LostWriteReply,
                    (Backend::MySql, false) => proxy::Mode::PauseExecute,
                    (Backend::MySql, true) => proxy::Mode::LostExecuteReply,
                };
                let (db, p) = proxied(b, mode).await;
                let before = p.trace().len();
                {
                    let future = sqlx::query(sqlx::AssertSqlSafe(format!(
                        "UPDATE {t} SET t=?,n=n+1 WHERE id=1 /* {} */",
                        proxy::MARKER
                    )))
                    .bind(time(1))
                    .persistent(persistent)
                    .execute(db.pool());
                    tokio::pin!(future);
                    if lost {
                        assert!(future.await.is_err());
                    } else {
                        tokio::select! {
                            _ = &mut future => panic!("write did not pause"),
                            _ = p.reached() => {}
                        }
                    }
                }
                assert_eq!(p.fault_reply(), 0);
                p.release();
                let healthy: i64 = tokio::time::timeout(
                    Duration::from_secs(5),
                    sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
                        .bind(42_i64)
                        .fetch_one(db.pool()),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(healthy, 42);
                let got: (MySqlTime, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT t,n FROM {t} WHERE id=1"
                )))
                .fetch_one(admin.pool())
                .await
                .unwrap();
                assert_eq!(got, (time(1), 1));
                let trace = p.trace();
                let trace = &trace[before..];
                assert_eq!(mode_reads(trace), 0, "mode-free write {b:?}");
                assert_eq!(
                    trace
                        .iter()
                        .filter(|v| v[0] == if b == Backend::Doris { 3 } else { 22 }
                            && String::from_utf8_lossy(&v[1..]).starts_with("UPDATE"))
                        .count(),
                    1
                );
                assert_eq!(p.hits(), 1);
                db.close().await;
            }
        }
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}
