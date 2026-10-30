//! Unsigned byte ordering, independently checked against Rust slices.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{self, Acquire, Executor, SqlSafeStr, mysql::MySqlConnectOptions},
};
use std::{str::FromStr, time::Duration};

fn options(b: Backend) -> MySqlConnectOptions {
    MySqlConnectOptions::from_str(
        &std::env::var(if b == Backend::Doris {
            "DORIS_TEST_URL"
        } else {
            "MYSQL_TEST_URL"
        })
        .expect("dedicated test URL required"),
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
fn values() -> Vec<Option<Vec<u8>>> {
    let all: Vec<u8> = (0..=255).collect();
    let mut zero = all.clone();
    zero.push(0);
    let mut space = all.clone();
    space.push(32);
    let mut result = vec![None, Some(vec![]), Some(all), Some(zero), Some(space)];
    result.extend(
        [
            vec![0],
            vec![0, 0],
            vec![0, 32],
            vec![31],
            vec![32],
            vec![65],
            vec![65, 0],
            vec![65, 32],
            vec![97],
            vec![127],
            vec![128],
            vec![254],
            vec![255],
            vec![255, 0],
            vec![255, 32],
            vec![255, 255],
            "中😀'\\\0?".as_bytes().to_vec(),
        ]
        .into_iter()
        .map(Some),
    );
    result
}
fn compare(a: &Option<Vec<u8>>, b: &Option<Vec<u8>>, op: &str) -> Option<i64> {
    let (a, b) = (a.as_ref()?, b.as_ref()?);
    Some(i64::from(match op {
        "<" => a < b,
        "<=" => a <= b,
        ">" => a > b,
        ">=" => a >= b,
        _ => panic!("operator"),
    }))
}
async fn seed(db: &Client) -> TableName {
    let t = common::name();
    let schema = if db.backend() == Backend::Doris {
        "(id BIGINT,b STRING,n BIGINT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        "(id BIGINT PRIMARY KEY,b LONGBLOB,n BIGINT)"
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t}{schema}")))
        .await
        .unwrap();
    let mut tx = db.pool().begin().await.unwrap();
    for (id, b) in values().into_iter().enumerate() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t} VALUES(?,?,0)"
        )))
        .bind(id as i64)
        .bind(b)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    t
}
async fn stored(db: &Client, t: &TableName) -> Vec<(i64, Option<Vec<u8>>, i64)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,b,n FROM {t} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn byte_ranges_match_unsigned_oracle_with_null_modes_cache_and_ordering() {
    let values = values();
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = seed(&db).await;
        let mut c = db.pool().acquire().await.unwrap();
        let mut queries = 0;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for persistent in [true, true, false] {
                for op in ["<", "<=", ">", ">="] {
                    for reverse in [false, true] {
                        let predicate = if reverse {
                            format!("((?)) {op} ((a.`b`))")
                        } else {
                            format!("((a.`b`)) {op} ((?))")
                        };
                        for v in &values {
                            let expected: Vec<_> = values
                                .iter()
                                .enumerate()
                                .map(|(id, x)| {
                                    (
                                        id as i64,
                                        if reverse {
                                            compare(v, x, op)
                                        } else {
                                            compare(x, v, op)
                                        },
                                    )
                                })
                                .collect();
                            let actual: Vec<(i64, Option<i64>)> =
                                sqlx::query_as(sqlx::AssertSqlSafe(format!(
                                    "SELECT a.id,({predicate}) FROM {t} a ORDER BY a.id"
                                )))
                                .bind(v)
                                .persistent(persistent)
                                .fetch_all(&mut *c)
                                .await
                                .unwrap();
                            assert_eq!(
                                actual, expected,
                                "{b:?}/{mode}/{persistent}/{predicate}/{v:?}"
                            );
                            let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(
                                format!("SELECT a.id FROM {t} a WHERE {predicate} ORDER BY a.id"),
                            ))
                            .bind(v)
                            .persistent(persistent)
                            .fetch_all(&mut *c)
                            .await
                            .unwrap();
                            assert_eq!(
                                actual,
                                expected
                                    .iter()
                                    .filter(|v| v.1 == Some(1))
                                    .map(|v| v.0)
                                    .collect::<Vec<_>>()
                            );
                            queries += 2;
                        }
                    }
                }
            }
        }
        for desc in [false, true] {
            let sql = format!(
                "SELECT id,b FROM {t} ORDER BY b {},id",
                if desc { "DESC" } else { "ASC" }
            );
            let actual: Vec<(i64, Option<Vec<u8>>)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
                .fetch_all(&mut *c)
                .await
                .unwrap();
            let mut expected: Vec<_> = values
                .iter()
                .cloned()
                .enumerate()
                .map(|(i, v)| (i as i64, v))
                .collect();
            expected.sort_by(|a, b| {
                (if desc { b.1.cmp(&a.1) } else { a.1.cmp(&b.1) }).then(a.0.cmp(&b.0))
            });
            assert_eq!(actual, expected, "{b:?}/ORDER BY desc={desc}");
        }
        eprintln!(
            "{b:?}: {queries} range SELECTs checked independently, plus ASC/DESC storage-order controls"
        );
        drop(c);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn byte_range_shapes_querybuilder_large_null_bitmaps_and_native_wire_types() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = seed(&admin).await;
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        let sql = format!("SELECT id FROM {t} WHERE b>=? ORDER BY id");
        c.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
            .await
            .unwrap();
        for persistent in [true, true, false] {
            let rows: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                .bind(vec![255_u8])
                .persistent(persistent)
                .fetch_all(&mut *c)
                .await
                .unwrap();
            assert_eq!(rows, [17, 18, 19, 20]);
        }
        let trace = p.trace();
        assert!(trace.iter().any(|v| v[0] == 23));
        assert!(
            trace.iter().any(|v| v[0] == 25),
            "nonpersistent statement closes"
        );
        assert!(
            trace
                .iter()
                .filter(|v| v[0] == 23)
                .all(|v| v[12] == if b == Backend::Doris { 253 } else { 252 }),
            "native MySQL BLOB stays BLOB; Doris adapter uses VARCHAR"
        );
        if b == Backend::Doris {
            assert!(
                trace
                    .iter()
                    .any(|v| v[0] == 22 && String::from_utf8_lossy(v).contains("HEX(b)>=?"))
            );
        } else {
            assert!(
                !trace
                    .iter()
                    .any(|v| v[0] == 22 && String::from_utf8_lossy(v).contains("HEX(b)"))
            );
        }
        for fold in [false, true] {
            if b == Backend::Doris {
                c.execute(sqlx::AssertSqlSafe(format!(
                    "SET enable_fold_constant_by_be={fold}"
                )))
                .await
                .unwrap();
            }
            let rows: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT a.id FROM {t} a WHERE NOT(? > (a.b)) ORDER BY a.id"
            )))
            .bind(vec![255_u8])
            .fetch_all(&mut *c)
            .await
            .unwrap();
            assert_eq!(rows, [17, 18, 19, 20]);
            for count in [1, 8, 9, 17, 65] {
                let mut q =
                    sqlx::QueryBuilder::<sqlx::MySql>::new(format!("SELECT id FROM {t} WHERE ("));
                for i in 0..count {
                    if i > 0 {
                        q.push(" OR ");
                    }
                    q.push("b>=").push_bind(if i == count - 1 {
                        Some(vec![255_u8])
                    } else {
                        None::<Vec<u8>>
                    });
                }
                q.push(") AND b<")
                    .push_bind(vec![255_u8, 32])
                    .push(" ORDER BY id");
                let rows: Vec<i64> = q.build_query_scalar().fetch_all(&mut *c).await.unwrap();
                assert_eq!(rows, [17, 18], "{b:?}/{fold}/{count}");
            }
            for n in [0, 250, 251, 65535, 65536] {
                let v = Some(vec![255_u8; n]);
                for op in ["<", "<=", ">", ">="] {
                    let rows: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                        "SELECT id FROM {t} WHERE b{op}? ORDER BY id"
                    )))
                    .bind(&v)
                    .fetch_all(&mut *c)
                    .await
                    .unwrap();
                    assert_eq!(
                        rows,
                        values()
                            .iter()
                            .enumerate()
                            .filter(|(_, x)| compare(x, &v, op) == Some(1))
                            .map(|(i, _)| i as i64)
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
        drop(c);
        db.close().await;
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn byte_range_update_delete_commit_rollback_drop_preserve_null_and_bytes() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            for action in ["commit", "rollback", "drop"] {
                let t = seed(&db).await;
                let initial = stored(&db, &t).await;
                let mut c = db.pool().acquire().await.unwrap();
                c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                    .await
                    .unwrap();
                let mut tx = c.begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {t} SET n=n+1 WHERE b>=? AND b<?"
                )))
                .bind(vec![255_u8])
                .bind(vec![255_u8, 32])
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {t} WHERE ?>b")))
                    .bind(vec![0_u8])
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {t} WHERE b<=? OR b>?"
                )))
                .bind(None::<Vec<u8>>)
                .bind(None::<Vec<u8>>)
                .execute(&mut *tx)
                .await
                .unwrap();
                match action {
                    "commit" => tx.commit().await.unwrap(),
                    "rollback" => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
                drop(c);
                let expected = if action == "commit" {
                    initial
                        .into_iter()
                        .filter(|(_, v, _)| v.as_ref().is_none_or(|v| !v.is_empty()))
                        .map(|(id, v, n)| (id, v, n + if [17, 18].contains(&id) { 1 } else { 0 }))
                        .collect()
                } else {
                    initial
                };
                assert_eq!(stored(&db, &t).await, expected, "{b:?}/{mode}/{action}");
                common::drop_table(&db, &t).await;
            }
        }
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn cached_range_writes_cancel_or_lose_reply_without_replay() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = seed(&admin).await;
        for tx in [false, true] {
            for persistent in [true, false] {
                for lost in [false, true] {
                    let m = if b == Backend::Doris && tx {
                        if lost {
                            proxy::Mode::LostWriteReply
                        } else {
                            proxy::Mode::PauseReply
                        }
                    } else if lost {
                        proxy::Mode::LostExecuteReply
                    } else {
                        proxy::Mode::PauseExecute
                    };
                    let (db, p) = proxied(b, m).await;
                    let sql = format!(
                        "UPDATE {t} SET n=n+1 WHERE b>=? AND b<? /* {} */",
                        proxy::MARKER
                    );
                    p.suspend_faults(true);
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(vec![255_u8])
                        .bind(vec![255_u8, 32])
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
                    if tx {
                        let mut tx = db.pool().begin().await.unwrap();
                        {
                            let future = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                                .bind(vec![255_u8])
                                .bind(vec![255_u8, 32])
                                .persistent(persistent)
                                .execute(&mut *tx);
                            tokio::pin!(future);
                            if lost {
                                assert!(future.await.is_err())
                            } else {
                                tokio::select! {_=&mut future=>panic!("write did not pause"),_=p.reached()=>{}}
                            }
                        }
                        drop(tx);
                    } else {
                        let future = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                            .bind(vec![255_u8])
                            .bind(vec![255_u8, 32])
                            .persistent(persistent)
                            .execute(db.pool());
                        tokio::pin!(future);
                        if lost {
                            assert!(future.await.is_err())
                        } else {
                            tokio::select! {_=&mut future=>panic!("write did not pause"),_=p.reached()=>{}}
                        }
                    }
                    assert_eq!(p.fault_reply(), 0);
                    p.release();
                    let health: i32 = sqlx::query_scalar("SELECT 42")
                        .fetch_one(db.pool())
                        .await
                        .unwrap();
                    assert_eq!(health, 42);
                    assert_eq!(p.hits(), 1);
                    for (id, _, n) in stored(&admin, &t).await {
                        assert_eq!(
                            n,
                            if !tx && [17, 18].contains(&id) { 1 } else { 0 },
                            "{b:?}/{tx}/{persistent}/{lost}/{id}"
                        );
                    }
                    let trace = p.trace();
                    let trace = &trace[before..];
                    if b == Backend::Doris && tx {
                        assert_eq!(
                            trace
                                .iter()
                                .filter(|v| v[0] == 3
                                    && String::from_utf8_lossy(v).contains(proxy::MARKER))
                                .count(),
                            1
                        );
                    } else {
                        assert_eq!(
                            trace.iter().filter(|v| v[0] == 23).count(),
                            2,
                            "one write plus health query"
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

#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn complex_range_operands_fail_before_send_and_preserve_prior_writes() {
    let (db, p) = proxied(Backend::Doris, proxy::Mode::Trace).await;
    let t = seed(&db).await;
    for expression in [
        "b>(?+1)",
        "ABS(?)<b",
        "ABS(?+1)<b",
        "b+id<?",
        "?<=concat(b,'x')",
        "b>?=id",
        "b < = ?",
        "b<<=?",
    ] {
        for sql in [
            format!("SELECT ({expression}) FROM {t}"),
            format!("SELECT id FROM {t} WHERE {expression}"),
        ] {
            let before = p.trace().len();
            let e = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                .bind(vec![255_u8, 0])
                .fetch_all(db.pool())
                .await
                .unwrap_err();
            assert!(e.to_string().contains("Doris client binding"), "{sql}/{e}");
            assert_eq!(before, p.trace().len(), "{sql}");
        }
    }
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {t} SET n=n+1 WHERE b>=? AND b<?"
    )))
    .bind(vec![255_u8])
    .bind(vec![255_u8, 32])
    .execute(&mut *tx)
    .await
    .unwrap();
    let before = p.trace().len();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {t} WHERE b>(?+1)"
    )))
    .bind(vec![255_u8])
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert_eq!(before, p.trace().len());
    tx.commit().await.unwrap();
    for (id, _, n) in stored(&db, &t).await {
        assert_eq!(n, if [17, 18].contains(&id) { 1 } else { 0 });
    }
    common::drop_table(&db, &t).await;
    db.close().await;
}
