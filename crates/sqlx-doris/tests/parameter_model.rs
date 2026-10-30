mod common;
#[path = "support/parameter_grammar.rs"]
mod grammar;
#[path = "support/sql_proxy.rs"]
mod proxy;
use grammar::{Data, Domain, Expr, Value};
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{
        self, Executor, Row,
        mysql::{MySqlConnectOptions, types::MySqlTime},
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
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(p.options(o))
        .await
        .unwrap();
    (db, p)
}
async fn table(db: &Client) -> TableName {
    let t = common::name();
    let schema = if db.backend() == Backend::Doris {
        "(id BIGINT,b STRING,t VARCHAR(64),n BIGINT) UNIQUE KEY(id) DISTRIBUTED BY \
            HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        "(id BIGINT PRIMARY KEY,b LONGBLOB,t TIME(6),n BIGINT)"
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t}{schema}")))
        .await
        .unwrap();
    t
}
async fn seed(db: &Client, t: &TableName) {
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
        .await
        .unwrap();
    for row in grammar::fixture() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t} VALUES(?,?,?,0)"
        )))
        .bind(row.id)
        .bind(row.bytes)
        .bind(row.time.map(grammar::native_time))
        .execute(db.pool())
        .await
        .unwrap();
    }
}
type StoredRow = (i64, Option<Vec<u8>>, Option<MySqlTime>, i64);
async fn stored(db: &Client, t: &TableName) -> Vec<StoredRow> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,b,t,n FROM {t} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}
fn matches(rows: &[StoredRow], expected: &[Data]) {
    assert_eq!(rows.len(), expected.len());
    for (actual, expected) in rows.iter().zip(expected) {
        assert_eq!(
            actual,
            &(
                expected.id,
                expected.bytes.clone(),
                expected.time.map(grammar::native_time),
                expected.n
            )
        );
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn generated_select_predicates_match_an_independent_three_valued_oracle() {
    let cases = grammar::corpus();
    let data = grammar::fixture();
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = table(&db).await;
        seed(&db, &t).await;
        let mut count = 0;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            let mut c = db.pool().acquire().await.unwrap();
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for (i, (expr, style)) in cases.iter().enumerate() {
                let mut args = Vec::new();
                let predicate = expr.render(*style, &mut args);
                let expected: Vec<(i64, Option<i64>)> = data
                    .iter()
                    .map(|r| (r.id, expr.eval(r).map(i64::from)))
                    .collect();
                let ids: Vec<_> = expected
                    .iter()
                    .filter(|r| r.1 == Some(1))
                    .map(|r| r.0)
                    .collect();
                for persistent in [true, true, false] {
                    let rows = grammar::query(
                        format!(
                            "SELECT r.id,CAST(({predicate}) AS SIGNED) FROM {t} r ORDER BY r.id"
                        ),
                        &args,
                    )
                    .persistent(persistent)
                    .fetch_all(&mut *c)
                    .await
                    .unwrap_or_else(|e| panic!("{b:?}/{mode}/case{i}: {predicate}: {e}"));
                    let actual: Vec<(i64, Option<i64>)> =
                        rows.iter().map(|r| (r.get(0), r.get(1))).collect();
                    assert_eq!(actual, expected, "{b:?}/{mode}/case{i}");
                    let rows = grammar::query(
                        format!("SELECT r.id FROM {t} r WHERE {predicate} ORDER BY r.id"),
                        &args,
                    )
                    .persistent(persistent)
                    .fetch_all(&mut *c)
                    .await
                    .unwrap();
                    assert_eq!(
                        rows.iter().map(|r| r.get::<i64, _>(0)).collect::<Vec<_>>(),
                        ids,
                        "WHERE {b:?}/{mode}/case{i}"
                    );
                    count += 2;
                }
            }
        }
        eprintln!(
            "generated live {b:?}: {} templates, {count} SELECT executions, {} oracle rows each",
            cases.len(),
            data.len()
        );
        assert_eq!(count, 2592);
        matches(&stored(&db, &t).await, &data);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn generated_mutation_predicates_keep_commit_rollback_and_drop_outcomes() {
    let cases = grammar::corpus();
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = table(&db).await;
        for index in [9, 18, 33, 75, 80, 85] {
            let (expr, style) = &cases[index];
            for outcome in [0, 1, 2] {
                seed(&db, &t).await;
                let mut expected = grammar::fixture();
                let mut values = vec![
                    Value::Bytes(Some(vec![128, 255, 0, 32])),
                    Value::Time(Some(-90_000_123_456)),
                ];
                let predicate = expr.render(*style, &mut values);
                let mut tx = db.pool().begin().await.unwrap();
                grammar::query(
                    format!("UPDATE {t} r SET b=?,t=?,n=n+1 WHERE {predicate}"),
                    &values,
                )
                .execute(&mut *tx)
                .await
                .unwrap();
                match outcome {
                    0 => {
                        tx.commit().await.unwrap();
                        for row in &mut expected {
                            if expr.eval(row) == Some(true) {
                                row.bytes = Some(vec![128, 255, 0, 32]);
                                row.time = Some(-90_000_123_456);
                                row.n += 1;
                            }
                        }
                    }
                    1 => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
                matches(&stored(&db, &t).await, &expected);
            }
        }
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn invalid_mutations_send_zero_packets_and_leave_prior_writes_committable() {
    let (db, p) = proxied(Backend::Doris, proxy::Mode::Trace).await;
    let t = table(&db).await;
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {t} VALUES(1,?,?,0)"
    )))
    .bind(vec![255_u8])
    .bind(grammar::native_time(-1))
    .execute(&mut *tx)
    .await
    .unwrap();
    let before = p.trace().len();
    let mut invalid = Vec::new();
    for op in [
        "<<",
        "<==",
        ">>",
        ">==",
        "==",
        "!==",
        "<==>",
        "= =",
        "< /* ? */ =>",
        "! =",
    ] {
        invalid.push(format!("UPDATE {t} SET n=n+1 WHERE b{op}?"));
    }
    for operand in ["b+1", "CONCAT(b,'x')", "(b,b)"] {
        invalid.push(format!("DELETE FROM {t} WHERE {operand}=?"));
    }
    for suffix in [
        "; DELETE FROM x",
        " /*",
        " '",
        " )",
        " /*!80000 ? */",
        " /*+ X */",
        "\0",
        "--?",
        "--x",
        "-- comment\\\n?",
        " = x",
        " + 1",
    ] {
        invalid.push(format!("UPDATE {t} SET n=n+1 WHERE b=?{suffix}"));
    }
    for list in [
        "?+1", "?,7", "?,b", "?,ABS(1)", "(?,1)", "?,", ",?", "SELECT ?",
    ] {
        invalid.push(format!("DELETE FROM {t} WHERE b IN({list})"));
    }
    for sql in &invalid {
        let error = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
            .bind(vec![255_u8])
            .execute(&mut *tx)
            .await
            .unwrap_err();
        assert!(
            matches!(error, sqlx::Error::Protocol(_)),
            "{sql:?}: {error:?}"
        );
        assert_eq!(p.trace().len(), before, "mutation reached server: {sql:?}");
    }
    tx.commit().await.unwrap();
    let actual = stored(&db, &t).await;
    assert_eq!(
        actual,
        vec![(1, Some(vec![255]), Some(grammar::native_time(-1)), 0)]
    );
    eprintln!(
        "invalid live mutations: {} rejected with zero packets",
        invalid.len()
    );
    common::drop_table(&db, &t).await;
    db.close().await;
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn generated_mixed_time_byte_writes_cancel_or_lose_ack_without_replay() {
    let expr = Expr::And(
        Box::new(Expr::Compare(
            Domain::Bytes,
            "<=>",
            Value::Bytes(Some(vec![255, 0, 128])),
            false,
        )),
        Box::new(Expr::In(
            Domain::Time,
            vec![Value::Time(Some(-1)), Value::Null],
            false,
        )),
    );
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = table(&admin).await;
        for native_tx in [false, true] {
            for lost in [false, true] {
                seed(&admin, &t).await;
                let mode = match (b, lost) {
                    (Backend::Doris, false) => proxy::Mode::PauseReply,
                    (Backend::Doris, true) => proxy::Mode::LostWriteReply,
                    (_, false) => proxy::Mode::PauseExecute,
                    (_, true) => proxy::Mode::LostExecuteReply,
                };
                let (db, p) = proxied(b, mode).await;
                let mut values = vec![Value::Time(Some(-123_456_789))];
                let predicate = expr.render(1, &mut values);
                let sql = format!(
                    "UPDATE {t} r SET t=?,n=n+1 WHERE {predicate} /* {} */",
                    proxy::MARKER
                );
                if native_tx {
                    let mut tx = db.pool().begin().await.unwrap();
                    {
                        let future = grammar::query(sql, &values).execute(&mut *tx);
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
                    drop(tx);
                } else {
                    {
                        let future = grammar::query(sql, &values).execute(db.pool());
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
                }
                assert_eq!(p.fault_reply(), 0);
                p.release();
                let healthy: i32 = sqlx::query_scalar("SELECT 42")
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
                assert_eq!(healthy, 42);
                let mut expected = grammar::fixture();
                if !native_tx {
                    expected[1].time = Some(-123_456_789);
                    expected[1].n = 1;
                }
                matches(&stored(&admin, &t).await, &expected);
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
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn raw_controls_separate_comment_and_mixed_time_coercion_differences() {
    for b in [Backend::MySql, Backend::Doris] {
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        for sql in ["SELECT 7--1", "SELECT 7 -- comment\\\n+1"] {
            let row = sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_owned()))
                .fetch_one(db.pool())
                .await
                .unwrap();
            let got: i64 = row.get(0);
            assert_eq!(
                got,
                if b == Backend::MySql { 8 } else { 7 },
                "{b:?}/{sql:?}"
            );
            eprintln!("raw comment control {b:?}: {sql:?} -> {got}");
        }
        if b == Backend::Doris {
            let before = p.trace().len();
            for sql in ["SELECT ?--1", "SELECT ? -- comment\\\n+1"] {
                assert!(
                    sqlx::query(sqlx::AssertSqlSafe(sql.to_owned()))
                        .bind(grammar::native_time(-1))
                        .fetch_one(db.pool())
                        .await
                        .is_err()
                );
                assert_eq!(p.trace().len(), before);
            }
        }
        let t = table(&db).await;
        seed(&db, &t).await;
        let expr = Expr::In(
            Domain::Time,
            vec![Value::Time(Some(-1)), Value::Literal("00:00:00")],
            false,
        );
        let mut args = Vec::new();
        let predicate = expr.render(0, &mut args);
        let rows = grammar::query(
            format!("SELECT r.id FROM {t} r WHERE {predicate} ORDER BY r.id"),
            &args,
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
        assert_eq!(ids, vec![1, 2, 6]);
        eprintln!(
            "mixed native TIME/string coercion control {b:?}: {ids:?}; normalized \
                TIME(6) domain on both backends"
        );
        let expr = Expr::In(
            Domain::Time,
            vec![Value::Time(Some(-1)), Value::Time(Some(0))],
            false,
        );
        let mut args = Vec::new();
        let predicate = expr.render(0, &mut args);
        let rows = grammar::query(
            format!("SELECT r.id FROM {t} r WHERE {predicate} ORDER BY r.id"),
            &args,
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        let ids: Vec<i64> = rows.iter().map(|r| r.get(0)).collect();
        assert_eq!(ids, vec![1, 2, 6]);
        eprintln!(
            "native zero TIME IN coercion control {b:?}: {ids:?}; native types retained; \
                precision normalized"
        );
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
