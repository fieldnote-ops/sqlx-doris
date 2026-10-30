mod common;
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{
        self, Executor,
        mysql::{
            MySqlConnectOptions,
            types::{MySqlTime, MySqlTimeSign},
        },
    },
};
use std::{str::FromStr, time::Duration};
#[path = "support/sql_proxy.rs"]
mod proxy;
use sqlx_doris::sqlx::{Row, SqlSafeStr};
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
fn clock(us: i64) -> MySqlTime {
    let n = us.unsigned_abs();
    MySqlTime::new(
        if us < 0 {
            MySqlTimeSign::Negative
        } else {
            MySqlTimeSign::Positive
        },
        (n / 3_600_000_000) as u32,
        ((n / 60_000_000) % 60) as u8,
        ((n / 1_000_000) % 60) as u8,
        (n % 1_000_000) as u32,
    )
    .unwrap()
}
async fn table(db: &Client, precision: u8) -> TableName {
    let t = common::name();
    let schema = if db.backend() == Backend::Doris {
        "(id BIGINT,t VARCHAR(64),n BIGINT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')".to_owned()
    } else {
        format!("(id BIGINT PRIMARY KEY,t TIME({precision}),n BIGINT)")
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t}{schema}")))
        .await
        .unwrap();
    t
}
async fn seed(db: &Client, t: &TableName) {
    for (id, v) in [
        None,
        Some(-1),
        Some(0),
        Some(90_000_123_456),
        Some(-90_000_123_456),
        Some(90_000_000_000),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t} VALUES(?,?,0)"
        )))
        .bind(id as i64)
        .bind(v.map(clock))
        .execute(db.pool())
        .await
        .unwrap();
    }
}
#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn native_membership_and_public_normalization_source_controls() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = table(&db, 6).await;
        seed(&db, &t).await;
        let original: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {t} WHERE t IN(?,?) ORDER BY id"
        )))
        .bind(clock(-1))
        .bind(MySqlTime::ZERO)
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(original, vec![1, 2]);
        let normalized:Vec<i64>=sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT id FROM {t} WHERE CAST(t AS TIME(6)) IN(CAST(? AS TIME(6)),CAST(? AS TIME(6))) ORDER BY id"))).bind("-00:00:00.000001").bind("00:00:00").fetch_all(db.pool()).await.unwrap();
        assert_eq!(normalized, [1, 2]);
        let mixed:Vec<i64>=sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT id FROM {t} WHERE CAST(t AS TIME(6)) IN(CAST(? AS TIME(6)),CAST('00:00:00' AS TIME(6))) ORDER BY id"))).bind("-00:00:00.000001").fetch_all(db.pool()).await.unwrap();
        assert_eq!(mixed, [1, 2]);
        if b == Backend::MySql {
            let actual: (String, String) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT CAST(t AS CHAR),CAST(? AS CHAR) FROM {t} WHERE id=2"
            )))
            .bind(MySqlTime::ZERO)
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(actual, ("00:00:00.000000".into(), "00:00:00".into()));
            eprintln!("MySQL source formatting control: {actual:?}");
        }
        eprintln!(
            "{b:?} current native IN={original:?}; public normalized IN={normalized:?}; mixed={mixed:?}"
        );
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

#[derive(Clone, Debug)]
enum Candidate {
    Native(Option<i64>),
    Text(Option<&'static str>),
    Literal(&'static str),
    Null,
}
fn micros(c: &Candidate) -> Option<i64> {
    match c {
        Candidate::Native(n) => *n,
        Candidate::Text(s) => s.map(|s| {
            s.strip_prefix('+')
                .unwrap_or(s)
                .parse::<sqlx_doris::types::Time>()
                .unwrap()
                .as_microseconds()
        }),
        Candidate::Literal(s) => Some(
            s.strip_prefix('+')
                .unwrap_or(s)
                .parse::<sqlx_doris::types::Time>()
                .unwrap()
                .as_microseconds(),
        ),
        Candidate::Null => None,
    }
}
fn render(candidates: &[Candidate], style: usize) -> (String, Vec<Candidate>) {
    let mut args = Vec::new();
    let mut parts = Vec::new();
    for c in candidates {
        parts.push(match c {
            Candidate::Literal(s) => format!("'{s}'"),
            Candidate::Null => "NULL".into(),
            _ => {
                args.push(c.clone());
                if style == 0 { "?".into() } else { "(?)".into() }
            }
        });
    }
    (parts.join(", /* ? */ "), args)
}
fn query(
    sql: String,
    args: &[Candidate],
) -> sqlx::query::Query<'static, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
    for c in args {
        q = match c {
            Candidate::Native(t) => q.bind(t.map(clock)),
            Candidate::Text(t) => q.bind(*t),
            _ => panic!("literal cannot bind"),
        };
    }
    q
}
fn truth(value: Option<i64>, candidates: &[Candidate], not: bool) -> Option<i64> {
    let value = value?;
    if candidates.iter().any(|c| micros(c) == Some(value)) {
        Some(i64::from(!not))
    } else if candidates.iter().any(|c| micros(c).is_none()) {
        None
    } else {
        Some(i64::from(not))
    }
}
fn cases() -> Vec<Vec<Candidate>> {
    use Candidate::*;
    vec![
        vec![Native(Some(-1)), Native(Some(0))],
        vec![Native(Some(0)), Text(Some("0:00:00"))],
        vec![Native(None), Text(None)],
        vec![
            Native(Some(-1)),
            Text(Some("00:00:00")),
            Literal("25:00:00"),
        ],
        vec![
            Native(Some(90_000_000_000)),
            Text(Some("+25:00:00.000000")),
            Text(Some("-25:00:00.123456")),
        ],
        vec![Native(Some(0)), Text(None), Null],
        vec![
            Native(Some(0)),
            Text(Some("+00:00:00")),
            Text(Some("-00:00:00")),
        ],
        vec![Native(Some(-1)), Text(Some("838:59:59"))],
        vec![Native(None), Text(Some("-00:00:00.000000"))],
    ]
}
async fn proxied(b: Backend, mode: proxy::Mode) -> (Client, proxy::Proxy) {
    let o = options(b);
    let p = proxy::Proxy::start(&o, mode).await;
    let db = PoolBuilder::new(b)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(p.options(o))
        .await
        .unwrap();
    (db, p)
}
#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn time_in_not_in_zero_mixed_strings_null_modes_and_cache_match_the_oracle() {
    let data = [
        None,
        Some(-1),
        Some(0),
        Some(90_000_123_456),
        Some(-90_000_123_456),
        Some(90_000_000_000),
    ];
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = table(&admin, 6).await;
        seed(&admin, &t).await;
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        let original = format!("SELECT id FROM {t} WHERE t IN(?,?) ORDER BY id");
        c.prepare(sqlx::AssertSqlSafe(original).into_sql_str())
            .await
            .unwrap();
        let mut executions = 0;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for (i, values) in cases().iter().enumerate() {
                for not in [false, true] {
                    let style = i % 3;
                    let (list, args) = render(values, style);
                    let column = if style == 0 { "r.t" } else { "((r.`t`))" };
                    let predicate = format!("{column} {}IN({list})", if not { "NOT " } else { "" });
                    let expected: Vec<(i64, Option<i64>)> = data
                        .iter()
                        .enumerate()
                        .map(|(id, &v)| (id as i64, truth(v, values, not)))
                        .collect();
                    let ids: Vec<i64> = expected
                        .iter()
                        .filter(|v| v.1 == Some(1))
                        .map(|v| v.0)
                        .collect();
                    for persistent in [true, true, false] {
                        let rows = query(
                            format!(
                                "SELECT r.id,CAST(({predicate}) AS SIGNED) FROM {t} r ORDER BY r.id"
                            ),
                            &args,
                        )
                        .persistent(persistent)
                        .fetch_all(&mut *c)
                        .await
                        .unwrap_or_else(|e| panic!("{b:?}/{mode}/case{i}/{not}/{persistent}: {e}"));
                        let actual: Vec<(i64, Option<i64>)> =
                            rows.iter().map(|r| (r.get(0), r.get(1))).collect();
                        assert_eq!(actual, expected, "{b:?}/{mode}/case{i}/{not}");
                        let rows = query(
                            format!("SELECT r.id FROM {t} r WHERE {predicate} ORDER BY r.id"),
                            &args,
                        )
                        .persistent(persistent)
                        .fetch_all(&mut *c)
                        .await
                        .unwrap();
                        assert_eq!(
                            rows.iter().map(|r| r.get::<i64, _>(0)).collect::<Vec<_>>(),
                            ids
                        );
                        executions += 2;
                    }
                }
            }
        }
        assert_eq!(executions, 324);
        let trace = p.trace();
        if b == Backend::MySql {
            assert!(trace.iter().any(|v| v[0] == 23));
            assert!(
                trace
                    .iter()
                    .filter(|v| v[0] == 22 && String::from_utf8_lossy(v).contains("CAST(("))
                    .any(|v| String::from_utf8_lossy(v).contains("AS TIME(6)"))
            );
            assert!(trace.iter().any(|v| v[0] == 25));
        } else {
            assert!(!trace.iter().any(|v| v[0] == 23));
        }
        eprintln!(
            "time membership {b:?}: {executions} oracle SELECTs, three modes, cached/uncached"
        );
        drop(c);
        db.close().await;
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn time_precision_zero_whole_seconds_and_large_querybuilder_null_lists_work() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        for precision in [0, 3, 6] {
            let t = table(&db, precision).await;
            for (id, value) in [None, Some(0), Some(-90_000_000_000), Some(90_000_000_000)]
                .into_iter()
                .enumerate()
            {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} VALUES(?,?,0)"
                )))
                .bind(id as i64)
                .bind(value.map(clock))
                .execute(db.pool())
                .await
                .unwrap();
            }
            for not in [false, true] {
                for persistent in [true, true, false] {
                    let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                        "SELECT id FROM {t} WHERE t {}IN(?,?) ORDER BY id",
                        if not { "NOT " } else { "" }
                    )))
                    .bind(MySqlTime::ZERO)
                    .bind("-25:00:00")
                    .persistent(persistent)
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                    assert_eq!(ids, if not { vec![3] } else { vec![1, 2] });
                }
            }
            for fold in [false, true] {
                if b == Backend::Doris {
                    db.pool()
                        .execute(sqlx::AssertSqlSafe(format!(
                            "SET enable_fold_constant_by_be={fold}"
                        )))
                        .await
                        .unwrap();
                }
                for n in [1, 8, 9, 17, 65] {
                    for not in [false, true] {
                        let mut q = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                            "SELECT id FROM {t} WHERE t {}IN(",
                            if not { "NOT " } else { "" }
                        ));
                        for i in 0..n {
                            if i > 0 {
                                q.push(",");
                            }
                            match i % 3 {
                                0 => {
                                    q.push_bind(MySqlTime::ZERO);
                                }
                                1 => {
                                    q.push_bind(None::<String>);
                                }
                                _ => {
                                    q.push_bind("-25:00:00");
                                }
                            }
                        }
                        q.push(") ORDER BY id");
                        let ids: Vec<i64> =
                            q.build_query_scalar().fetch_all(db.pool()).await.unwrap();
                        let expected = if n == 1 {
                            if not { vec![2, 3] } else { vec![1] }
                        } else if not {
                            vec![]
                        } else if n == 2 {
                            vec![1]
                        } else {
                            vec![1, 2]
                        };
                        assert_eq!(ids, expected, "{b:?}/{precision}/{fold}/{n}/{not}");
                    }
                }
            }
            common::drop_table(&db, &t).await;
        }
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn time_membership_update_delete_commit_rollback_and_drop_keep_null_semantics() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = table(&db, 6).await;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for outcome in [0, 1, 2] {
                db.pool()
                    .execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                    .await
                    .unwrap();
                seed(&db, &t).await;
                let mut tx = db.pool().begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {t} SET n=n+1 WHERE t IN(?,?)"
                )))
                .bind(MySqlTime::ZERO)
                .bind("00:00:00")
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {t} WHERE t NOT IN(?,?,NULL)"
                )))
                .bind(MySqlTime::ZERO)
                .bind("00:00:00")
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {t} WHERE t NOT IN(?,?)"
                )))
                .bind(MySqlTime::ZERO)
                .bind("-00:00:00.000001")
                .execute(&mut *tx)
                .await
                .unwrap();
                match outcome {
                    0 => tx.commit().await.unwrap(),
                    1 => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
                let rows: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT id,n FROM {t} ORDER BY id"
                )))
                .fetch_all(db.pool())
                .await
                .unwrap();
                assert_eq!(
                    rows,
                    if outcome == 0 {
                        vec![(0, 0), (1, 0), (2, 1)]
                    } else {
                        (0..6).map(|id| (id, 0)).collect()
                    }
                );
            }
        }
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn bad_time_membership_strings_fail_before_send_and_preserve_prior_writes() {
    for b in [Backend::MySql, Backend::Doris] {
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let t = table(&db, 6).await;
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t} VALUES(2,?,0)"
        )))
        .bind(MySqlTime::ZERO)
        .execute(&mut *tx)
        .await
        .unwrap();
        let before = p.trace().len();
        for text in [
            "",
            "bad",
            "00:00:00.1234567",
            "838:59:59.000001",
            "839:00:00",
            "00:60:00",
            "00:00:00\0",
            "0' OR 1=1 --",
            " 00:00:00",
            "00:00:00:00",
        ] {
            let error = sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {t} SET n=n+1 WHERE t IN(?,?)"
            )))
            .bind(MySqlTime::ZERO)
            .bind(text)
            .execute(&mut *tx)
            .await
            .unwrap_err();
            assert!(
                matches!(error, sqlx::Error::Protocol(_)),
                "{b:?}/{text:?}/{error:?}"
            );
            assert_eq!(p.trace().len(), before);
        }
        for literal in [
            "'bad'",
            "'00:00:00.1234567'",
            "'838:59:59.000001'",
            "7",
            "ABS(1)",
        ] {
            let error = sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {t} WHERE t NOT IN(?,{literal})"
            )))
            .bind(MySqlTime::ZERO)
            .execute(&mut *tx)
            .await
            .unwrap_err();
            assert!(matches!(error, sqlx::Error::Protocol(_)));
            assert_eq!(p.trace().len(), before);
        }
        tx.commit().await.unwrap();
        let rows: Vec<(i64, i64)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT id,n FROM {t}")))
                .fetch_all(db.pool())
                .await
                .unwrap();
        assert_eq!(rows, [(2, 0)]);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris"]
async fn zero_time_membership_cached_writes_cancel_or_lose_ack_without_replay() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = table(&admin, 6).await;
        seed(&admin, &t).await;
        for native_tx in [false, true] {
            for persistent in [true, false] {
                for lost in [false, true] {
                    let mode = match (b, lost) {
                        (Backend::Doris, false) => proxy::Mode::PauseReply,
                        (Backend::Doris, true) => proxy::Mode::LostWriteReply,
                        (_, false) => proxy::Mode::PauseExecute,
                        (_, true) => proxy::Mode::LostExecuteReply,
                    };
                    let (db, p) = proxied(b, mode).await;
                    let sql = format!(
                        "UPDATE {t} SET n=n+1 WHERE t IN(?,?,NULL) /* {} */",
                        proxy::MARKER
                    );
                    p.suspend_faults(true);
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(MySqlTime::ZERO)
                        .bind("00:00:00")
                        .persistent(persistent)
                        .execute(db.pool())
                        .await
                        .unwrap();
                    admin
                        .pool()
                        .execute(sqlx::AssertSqlSafe(format!("UPDATE {t} SET n=0")))
                        .await
                        .unwrap();
                    let before = p.trace().len();
                    p.suspend_faults(false);
                    if native_tx {
                        let mut tx = db.pool().begin().await.unwrap();
                        {
                            let future = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                                .bind(MySqlTime::ZERO)
                                .bind("00:00:00")
                                .persistent(persistent)
                                .execute(&mut *tx);
                            tokio::pin!(future);
                            if lost {
                                assert!(future.await.is_err());
                            } else {
                                tokio::select! {_=&mut future=>panic!("write did not pause"),_=p.reached()=>{}}
                            }
                        }
                        drop(tx);
                    } else {
                        {
                            let future = sqlx::query(sqlx::AssertSqlSafe(sql))
                                .bind(MySqlTime::ZERO)
                                .bind("00:00:00")
                                .persistent(persistent)
                                .execute(db.pool());
                            tokio::pin!(future);
                            if lost {
                                assert!(future.await.is_err());
                            } else {
                                tokio::select! {_=&mut future=>panic!("write did not pause"),_=p.reached()=>{}}
                            }
                        }
                    }
                    assert_eq!(p.fault_reply(), 0);
                    p.release();
                    let n: i32 = sqlx::query_scalar("SELECT 42")
                        .fetch_one(db.pool())
                        .await
                        .unwrap();
                    assert_eq!(n, 42);
                    let rows: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                        "SELECT id,n FROM {t} ORDER BY id"
                    )))
                    .fetch_all(admin.pool())
                    .await
                    .unwrap();
                    assert_eq!(
                        rows,
                        (0..6)
                            .map(|id| (id, if id == 2 && !native_tx { 1 } else { 0 }))
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(p.hits(), 1);
                    let trace = p.trace();
                    if b == Backend::Doris {
                        assert_eq!(
                            trace[before..]
                                .iter()
                                .filter(|v| v[0] == 3
                                    && String::from_utf8_lossy(v).contains(proxy::MARKER))
                                .count(),
                            1
                        );
                    } else {
                        assert_eq!(
                            trace[before..].iter().filter(|v| v[0] == 23).count(),
                            2,
                            "one uncertain write plus health query"
                        );
                    }
                    db.close().await;
                }
            }
        }
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}
