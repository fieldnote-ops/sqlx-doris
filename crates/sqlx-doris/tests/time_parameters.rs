//! TIME parameters and lossless results through unmodified official servers.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    sqlx::{
        self, Connection, Executor, Row, SqlSafeStr,
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
async fn client(b: Backend) -> Client {
    PoolBuilder::new(b)
        .max_connections(1)
        .connect_with(options(b))
        .await
        .unwrap()
}
async fn proxied(b: Backend, m: proxy::Mode) -> (Client, proxy::Proxy) {
    let o = options(b);
    let p = proxy::Proxy::start(&o, m).await;
    let db = PoolBuilder::new(b)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(4))
        .connect_with(p.options(o))
        .await
        .unwrap();
    (db, p)
}
fn negative() -> MySqlTime {
    MySqlTime::new(MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap()
}
fn values() -> Vec<MySqlTime> {
    vec![
        MySqlTime::ZERO,
        negative(),
        MySqlTime::new(MySqlTimeSign::Negative, 0, 2, 3, 123456).unwrap(),
        MySqlTime::new(MySqlTimeSign::Positive, 25, 2, 3, 123456).unwrap(),
        MySqlTime::new(MySqlTimeSign::Negative, 123, 45, 6, 123456).unwrap(),
        MySqlTime::new(MySqlTimeSign::Positive, 838, 59, 58, 999999).unwrap(),
        MySqlTime::MIN,
        MySqlTime::MAX,
    ]
}
async fn table(db: &Client) -> sqlx_doris::operations::TableName {
    let t = common::name();
    let schema = if db.backend() == Backend::Doris {
        "(id BIGINT,t VARCHAR(64),b STRING,n BIGINT) UNIQUE KEY(id) DISTRIBUTED BY \
            HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        "(id BIGINT PRIMARY KEY,t TIME(6),b LONGBLOB,n BIGINT)"
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t}{schema}")))
        .await
        .unwrap();
    t
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn native_time_bind_roundtrips_sign_days_micros_null_and_modes() {
    for b in [Backend::MySql, Backend::Doris] {
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        c.prepare("SELECT CAST(? AS TIME(6))".into_sql_str())
            .await
            .unwrap();
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for persistent in [true, true, false] {
                for expected in values() {
                    let got: MySqlTime = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                        .bind(expected)
                        .persistent(persistent)
                        .fetch_one(&mut *c)
                        .await
                        .unwrap();
                    assert_eq!(got, expected, "{b:?}/{mode}/{persistent}");
                    let delta: chrono::TimeDelta = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                        .bind(expected)
                        .fetch_one(&mut *c)
                        .await
                        .unwrap();
                    let micros = format!("{expected:.6}")
                        .parse::<sqlx_doris::types::Time>()
                        .unwrap()
                        .as_microseconds();
                    assert_eq!(delta.num_microseconds(), Some(micros));
                }
                let null: Option<MySqlTime> = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                    .bind(None::<MySqlTime>)
                    .persistent(persistent)
                    .fetch_one(&mut *c)
                    .await
                    .unwrap();
                assert_eq!(null, None);
            }
        }
        let expected = chrono::NaiveTime::from_hms_micro_opt(12, 34, 56, 123456).unwrap();
        let got: chrono::NaiveTime = sqlx::query_scalar("SELECT ?")
            .bind(expected)
            .fetch_one(&mut *c)
            .await
            .unwrap();
        assert_eq!(got, expected);
        let text = "中😀'\\\0?; -- literal";
        let got: (MySqlTime, String, Option<MySqlTime>) = sqlx::query_as("SELECT ?,?,?")
            .bind(negative())
            .bind(text)
            .bind(None::<MySqlTime>)
            .fetch_one(&mut *c)
            .await
            .unwrap();
        assert_eq!(got, (negative(), text.to_owned(), None));
        let trace = p.trace();
        if b == Backend::Doris {
            assert!(
                !trace.iter().any(|v| v[0] == 23),
                "TIME arguments never enter unsupported EXECUTE"
            );
            assert!(
                trace
                    .iter()
                    .any(|v| v[0] == 3 && String::from_utf8_lossy(v).contains("-0:00:00.000001"))
            );
        } else {
            assert!(trace.iter().any(|v| v[0] == 23));
        }
        drop(c);
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn original_dml_and_dto_support_time_and_blob_parameters() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = table(&db).await;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            let mut c = db.pool().acquire().await.unwrap();
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            c.execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                .await
                .unwrap();
            let payload = vec![255_u8, 0, 39, 92, 128];
            for (id, v) in values().into_iter().enumerate() {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} VALUES(?,?,?,0)"
                )))
                .bind(id as i64)
                .bind(v)
                .bind(&payload)
                .execute(&mut *c)
                .await
                .unwrap();
            }
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {t} VALUES(99,?,?,0)"
            )))
            .bind(None::<MySqlTime>)
            .bind(None::<Vec<u8>>)
            .execute(&mut *c)
            .await
            .unwrap();
            let rows: Vec<(i64, Option<MySqlTime>, Option<Vec<u8>>)> = sqlx::query_as(
                sqlx::AssertSqlSafe(format!("SELECT id,t,b FROM {t} ORDER BY id")),
            )
            .fetch_all(&mut *c)
            .await
            .unwrap();
            for ((id, v, bytes), expected) in rows[..8].iter().zip(values()) {
                assert_eq!(v, &Some(expected), "{b:?}/{mode}/{id}");
                assert_eq!(bytes, &Some(payload.clone()));
            }
            assert_eq!(rows[8], (99, None, None));
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {t} WHERE t < ? ORDER BY id"
            )))
            .bind(MySqlTime::ZERO)
            .fetch_all(&mut *c)
            .await
            .unwrap();
            assert_eq!(ids, vec![1, 2, 4, 6]);
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {t} WHERE t IN(?,NULL,?) ORDER BY id"
            )))
            .bind(negative())
            .bind(values()[3])
            .fetch_all(&mut *c)
            .await
            .unwrap();
            assert_eq!(ids, vec![1, 3]);
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {t} WHERE t <=> ?"
            )))
            .bind(None::<MySqlTime>)
            .fetch_all(&mut *c)
            .await
            .unwrap();
            assert_eq!(ids, vec![99]);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {t} SET t=?,b=? WHERE id=?"
            )))
            .bind(values()[3])
            .bind(&payload)
            .bind(1_i64)
            .execute(&mut *c)
            .await
            .unwrap();
            let got: MySqlTime =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT t FROM {t} WHERE id=?")))
                    .bind(1_i64)
                    .fetch_one(&mut *c)
                    .await
                    .unwrap();
            assert_eq!(got, values()[3]);
            let mut q = sqlx::QueryBuilder::<sqlx::MySql>::new(format!("DELETE FROM {t} WHERE t="));
            q.push_bind(values()[3]);
            q.build().execute(&mut *c).await.unwrap();
            let count: i64 =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {t}")))
                    .fetch_one(&mut *c)
                    .await
                    .unwrap();
            assert_eq!(count, 7);
        }
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn time_result_metadata_routes_before_execute_and_cache_remains_usable() {
    for b in [Backend::MySql, Backend::Doris] {
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        for persistent in [true, true, false] {
            for text in ["-00:00:00.000001", "25:02:03.123456"] {
                let got: MySqlTime = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                    .bind(text)
                    .persistent(persistent)
                    .fetch_one(&mut *c)
                    .await
                    .unwrap();
                assert_eq!(
                    format!("{got:.6}"),
                    if text.starts_with("-00") {
                        "-0:00:00.000001"
                    } else {
                        text
                    }
                );
            }
            let null: Option<MySqlTime> = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                .bind(None::<String>)
                .persistent(persistent)
                .fetch_one(&mut *c)
                .await
                .unwrap();
            assert!(null.is_none());
        }
        if b == Backend::Doris {
            assert!(!p.trace().iter().any(|v| v[0] == 23));
            assert!(
                p.trace().iter().any(|v| v[0] == 25),
                "uncached TIME metadata statement closed"
            );
        }
        for _ in 0..2 {
            let n: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
                .bind(42_i64)
                .fetch_one(&mut *c)
                .await
                .unwrap();
            assert_eq!(n, 42);
        }
        assert!(
            p.trace().iter().any(|v| v[0] == 23),
            "ordinary results retain binary EXECUTE"
        );
        let invalid = sqlx::query_scalar::<_, chrono::NaiveTime>("SELECT CAST(? AS TIME(6))")
            .bind("-00:00:00.000001")
            .fetch_one(&mut *c)
            .await
            .unwrap_err();
        assert!(matches!(invalid, sqlx::Error::ColumnDecode { .. }));
        drop(c);
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn cancelled_and_lost_time_writes_are_never_replayed() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        for lost in [false, true] {
            let mode = match (b, lost) {
                (Backend::Doris, false) => proxy::Mode::PauseReply,
                (Backend::Doris, true) => proxy::Mode::LostWriteReply,
                (_, false) => proxy::Mode::PauseExecute,
                (_, true) => proxy::Mode::LostExecuteReply,
            };
            let (db, p) = proxied(b, mode).await;
            let t = table(&admin).await;
            admin
                .pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} VALUES(1,NULL,NULL,0)"
                )))
                .await
                .unwrap();
            let sql = format!(
                "UPDATE {t} SET t=?,n=n+1 WHERE id=1 /* {} */",
                proxy::MARKER
            );
            {
                let future = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(negative())
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
            let healthy: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
                .bind(42_i64)
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(healthy, 42);
            let actual: (MySqlTime, i64) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT t,n FROM {t} WHERE id=1"
            )))
            .fetch_one(admin.pool())
            .await
            .unwrap();
            assert_eq!(actual, (negative(), 1));
            let trace = p.trace();
            let writes = trace
                .iter()
                .filter(|v| {
                    v[0] == if b == Backend::Doris { 3 } else { 22 }
                        && String::from_utf8_lossy(&v[1..]).starts_with("UPDATE")
                })
                .count();
            assert_eq!(writes, 1);
            assert_eq!(p.hits(), 1);
            common::drop_table(&admin, &t).await;
            db.close().await;
        }
        admin.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn submicrosecond_time_binding_fails_before_any_business_request() {
    for b in [Backend::MySql, Backend::Doris] {
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        for nanos in [1, 999, 1_000_000_000] {
            let t = chrono::NaiveTime::from_hms_nano_opt(
                0,
                0,
                if nanos == 1_000_000_000 { 59 } else { 0 },
                nanos,
            )
            .unwrap();
            let error = sqlx::query("SELECT ? /* TIME_INVALID_PRECISION */")
                .bind(t)
                .fetch_one(db.pool())
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx::Error::Encode(_)), "{error:?}");
        }
        assert!(
            !p.trace()
                .iter()
                .any(|v| String::from_utf8_lossy(v).contains("TIME_INVALID_PRECISION"))
        );
        let healthy: i32 = sqlx::query_scalar("SELECT 1")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(healthy, 1);
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn unconfigured_server_control_rejects_time_and_clamps_binary_boundary() {
    let mut c = sqlx::MySqlConnection::connect_with(
        &options(Backend::Doris).doris_transaction_compatibility(false),
    )
    .await
    .unwrap();
    let error = sqlx::query_scalar::<_, MySqlTime>("SELECT CAST(? AS TIME(6))")
        .bind(negative())
        .fetch_one(&mut c)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Unsupported MySQL type"));
    let got: MySqlTime = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
        .bind("838:59:59.999999")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(got, MySqlTime::MAX);
    let row = sqlx::raw_sql("SELECT CAST('838:59:59.999999' AS TIME(6))")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(
        row.try_get::<sqlx_doris::types::Time, _>(0)
            .unwrap()
            .as_microseconds(),
        sqlx_doris::types::Time::MAX_MICROSECONDS
    );
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn cancelled_time_results_drain_safely_without_replay() {
    for b in [Backend::MySql, Backend::Doris] {
        for native in [true, false] {
            let (db, p) = proxied(
                b,
                if b == Backend::Doris {
                    proxy::Mode::PauseReply
                } else {
                    proxy::Mode::PauseExecute
                },
            )
            .await;
            {
                let q = sqlx::query_scalar::<_, MySqlTime>(
                    "SELECT CAST(? AS TIME(6)) /* SQLX_DORIS_FAULT */",
                );
                let q = if native {
                    q.bind(negative())
                } else {
                    q.bind("-00:00:00.000001")
                };
                let future = q.fetch_one(db.pool());
                tokio::pin!(future);
                tokio::select! {
                    _ = &mut future => panic!("result did not pause"),
                    _ = p.reached() => {}
                }
            }
            p.release();
            let got: MySqlTime = sqlx::query_scalar("SELECT CAST(? AS TIME(6))")
                .bind(negative())
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(got, negative());
            assert_eq!(p.hits(), 1);
            assert_eq!(
                p.trace()
                    .iter()
                    .filter(|v| v[0] == if b == Backend::Doris { 3 } else { 22 }
                        && String::from_utf8_lossy(v).contains(proxy::MARKER))
                    .count(),
                1
            );
            db.close().await;
        }
    }
}
