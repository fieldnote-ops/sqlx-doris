//! Native byte equality/membership through official protocols and transactions.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;

use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{
        self, Executor, SqlSafeStr,
        mysql::types::{MySqlTime, MySqlTimeSign},
    },
};
use std::{str::FromStr, time::Duration};

fn options(backend: Backend) -> sqlx::mysql::MySqlConnectOptions {
    sqlx::mysql::MySqlConnectOptions::from_str(
        &std::env::var(if backend == Backend::Doris {
            "DORIS_TEST_URL"
        } else {
            "MYSQL_TEST_URL"
        })
        .expect("dedicated test URL required"),
    )
    .unwrap()
}
async fn client(backend: Backend) -> Client {
    PoolBuilder::new(backend)
        .max_connections(1)
        .connect_with(options(backend))
        .await
        .unwrap()
}
async fn proxied(backend: Backend, mode: proxy::Mode) -> (Client, proxy::Proxy) {
    let o = options(backend);
    let p = proxy::Proxy::start(&o, mode).await;
    let db = PoolBuilder::new(backend)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(4))
        .connect_with(p.options(o))
        .await
        .unwrap();
    (db, p)
}
fn clock() -> MySqlTime {
    MySqlTime::new(MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap()
}
fn payloads() -> Vec<Option<Vec<u8>>> {
    let all: Vec<u8> = (0..=255).collect();
    let mut zero = all.clone();
    zero.push(0);
    let mut space = all.clone();
    space.push(32);
    vec![
        Some(all),
        Some(zero),
        Some(space),
        Some(vec![]),
        None,
        Some("中😀'\\\0".as_bytes().to_vec()),
        Some(b"'quoted'".to_vec()),
        Some(b"A".to_vec()),
        Some(b"a".to_vec()),
    ]
}
async fn seed(db: &Client) -> TableName {
    let table = common::name();
    let schema = if db.backend() == Backend::Doris {
        "(id BIGINT,b STRING,t VARCHAR(64),n BIGINT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        "(id BIGINT PRIMARY KEY,b LONGBLOB,t TIME(6),n BIGINT)"
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}{schema}")))
        .await
        .unwrap();
    let mut tx = db.pool().begin().await.unwrap();
    for (id, b) in payloads().into_iter().enumerate() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(?,?,?,0)"
        )))
        .bind(id as i64 + 1)
        .bind(b)
        .bind(clock())
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
    table
}
async fn stored(db: &Client, table: &TableName) -> Vec<(i64, Option<Vec<u8>>, i64)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,b,n FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn native_blob_where_equals_preserves_bytes_and_time_in_all_modes() {
    for backend in [Backend::MySql, Backend::Doris] {
        let admin = client(backend).await;
        let table = seed(&admin).await;
        let (db, p) = proxied(backend, proxy::Mode::Trace).await;
        assert!(db.capabilities().blob_parameters);
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            let mut c = db.pool().acquire().await.unwrap();
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for persistent in [true, true, false] {
                for (i, b) in payloads().into_iter().enumerate() {
                    let actual: Vec<(i64, Vec<u8>, MySqlTime)> =
                        sqlx::query_as(sqlx::AssertSqlSafe(format!(
                            "SELECT id,b,CAST(t AS TIME(6)) FROM {table} WHERE b = ? ORDER BY id"
                        )))
                        .bind(b.clone())
                        .persistent(persistent)
                        .fetch_all(&mut *c)
                        .await
                        .unwrap();
                    if let Some(b) = b {
                        assert_eq!(
                            actual,
                            [(i as i64 + 1, b, clock())],
                            "{backend:?}/{mode}/{persistent}"
                        );
                    } else {
                        assert!(actual.is_empty());
                    }
                }
            }
        }
        let _: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {table} WHERE b = ?"
        )))
        .bind(payloads()[0].clone().unwrap())
        .fetch_all(db.pool())
        .await
        .unwrap();
        let trace = p.trace();
        let statements: Vec<_> = trace
            .iter()
            .filter(|v| v[0] == 22 && String::from_utf8_lossy(&v[1..]).contains("WHERE"))
            .collect();
        assert!(!statements.is_empty());
        if backend == Backend::Doris {
            assert!(
                statements
                    .iter()
                    .all(|v| String::from_utf8_lossy(v).contains("HEX(b) = ?"))
            );
            assert!(
                trace.iter().any(|v| v[0] == 23)
                    && trace.iter().filter(|v| v[0] == 23).all(|v| v[12] == 253),
                "BLOB becomes native VARCHAR on the wire"
            );
            assert!(
                !trace.iter().any(|v| v[0] == 3
                    && String::from_utf8_lossy(v).contains("WHERE")
                    && !String::from_utf8_lossy(v).contains("CAST(t AS TIME(6))")),
                "non-TIME SELECT retains prepared binary rows"
            );
        }
        common::drop_table(&admin, &table).await;
        db.close().await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn byte_predicate_aliases_reverse_parentheses_compound_conditions_and_querybuilder() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = seed(&db).await;
        let bytes = payloads()[0].clone().unwrap();
        for fold in [false, true] {
            let mut c = db.pool().acquire().await.unwrap();
            if backend == Backend::Doris {
                c.execute(sqlx::AssertSqlSafe(format!(
                    "SET enable_fold_constant_by_be={fold}"
                )))
                .await
                .unwrap();
            }
            for condition in ["? = (a.`b`)", "((a.b)) = ((?))", "NOT (a.b = ?)"] {
                let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT a.id FROM {table} a WHERE {condition} ORDER BY a.id"
                )))
                .bind(&bytes)
                .fetch_all(&mut *c)
                .await
                .unwrap();
                assert_eq!(
                    actual,
                    if condition.starts_with("NOT") {
                        vec![2, 3, 4, 6, 7, 8, 9]
                    } else {
                        vec![1]
                    },
                    "{backend:?}/{fold}/{condition}"
                );
            }
            let actual:Vec<i64>=sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT a.id FROM {table} a JOIN {table} c ON a.b=? AND c.id=a.id WHERE c.b=? ORDER BY a.id"))).bind(&bytes).bind(&bytes).fetch_all(&mut *c).await.unwrap();
            assert_eq!(actual, [1]);
            let mut q =
                sqlx::QueryBuilder::<sqlx::MySql>::new(format!("SELECT id FROM {table} WHERE (b="));
            q.push_bind(&bytes)
                .push(" OR ")
                .push_bind(b"A".as_slice())
                .push("=b) AND id>=")
                .push_bind(1_i64)
                .push(" ORDER BY id");
            let actual: Vec<i64> = q.build_query_scalar().fetch_all(&mut *c).await.unwrap();
            assert_eq!(actual, [1, 8]);
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn prepared_cache_separates_byte_and_string_shapes_and_preserves_other_arguments() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = seed(&db).await;
        let sql = format!("SELECT id FROM {table} WHERE b=?");
        let mut c = db.pool().acquire().await.unwrap();
        c.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
            .await
            .unwrap();
        for _ in 0..3 {
            let id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                .bind("A")
                .fetch_one(&mut *c)
                .await
                .unwrap();
            assert_eq!(id, 8);
            let id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                .bind(b"a".as_slice())
                .fetch_one(&mut *c)
                .await
                .unwrap();
            assert_eq!(id, 9);
            let id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                .bind(payloads()[0].clone().unwrap())
                .fetch_one(&mut *c)
                .await
                .unwrap();
            assert_eq!(id, 1);
        }
        let time = chrono::NaiveDate::from_ymd_opt(2026, 10, 28)
            .unwrap()
            .and_hms_micro_opt(12, 34, 56, 123456)
            .unwrap();
        let actual:Vec<i64>=sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT id FROM {table} WHERE b=? AND ?>=0 AND ?='name' AND CAST(? AS DATETIME(6))=CAST('2026-10-28 12:34:56.123456' AS DATETIME(6)) AND CAST(? AS DECIMAL(38,6))=123.456789")))
            .bind(payloads()[0].clone().unwrap()).bind(7_i64).bind("name").bind(time).bind(bigdecimal::BigDecimal::from_str("123.456789").unwrap()).fetch_all(&mut *c).await.unwrap();
        assert_eq!(actual, [1]);
        for fold in [false, true] {
            if backend == Backend::Doris {
                c.execute(sqlx::AssertSqlSafe(format!(
                    "SET enable_fold_constant_by_be={fold}"
                )))
                .await
                .unwrap();
            }
            let a = vec![255_u8, 129, 0, 39, 92];
            let b = vec![128_u8, 254, 0, 39, 92];
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(10,?,'00:00:00',0)"
            )))
            .bind(&a)
            .execute(&mut *c)
            .await
            .unwrap();
            let id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {table} WHERE b=?"
            )))
            .bind(&a)
            .fetch_one(&mut *c)
            .await
            .unwrap();
            assert_eq!(id, 10);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {table} SET b=? WHERE b=?"
            )))
            .bind(&b)
            .bind(&a)
            .execute(&mut *c)
            .await
            .unwrap();
            let id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {table} WHERE b=?"
            )))
            .bind(&b)
            .fetch_one(&mut *c)
            .await
            .unwrap();
            assert_eq!(id, 10);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE b=?"
            )))
            .bind(&b)
            .execute(&mut *c)
            .await
            .unwrap();
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {table} WHERE b=?"
            )))
            .bind(&b)
            .fetch_all(&mut *c)
            .await
            .unwrap();
            assert!(ids.is_empty());
        }
        drop(c);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn transactional_update_delete_byte_conditions_commit_and_rollback_without_manual_hex() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = seed(&db).await;
        let old = payloads()[0].clone().unwrap();
        let new = vec![255_u8, 128, 0, 39, 92, 26];
        for commit in [false, true] {
            let mut tx = db.pool().begin().await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {table} SET b=?,n=n+1 WHERE b=?"
            )))
            .bind(&new)
            .bind(&old)
            .execute(&mut *tx)
            .await
            .unwrap();
            if commit {
                tx.commit().await.unwrap();
            } else {
                tx.rollback().await.unwrap();
            }
            let actual = stored(&db, &table).await;
            assert_eq!(
                actual[0],
                (
                    1,
                    Some(if commit { new.clone() } else { old.clone() }),
                    i64::from(commit)
                )
            );
            assert_eq!(actual.len(), 9);
        }
        for commit in [false, true] {
            let mut tx = db.pool().begin().await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE (b=? OR ?=b) AND id>=?"
            )))
            .bind(&new)
            .bind(payloads()[5].clone().unwrap())
            .bind(1_i64)
            .execute(&mut *tx)
            .await
            .unwrap();
            if commit {
                tx.commit().await.unwrap();
            } else {
                tx.rollback().await.unwrap();
            }
            let ids: Vec<i64> = stored(&db, &table).await.into_iter().map(|v| v.0).collect();
            assert_eq!(
                ids,
                if commit {
                    vec![2, 3, 4, 5, 7, 8, 9]
                } else {
                    (1..=9).collect()
                }
            );
        }
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE b=?"
        )))
        .bind(None::<Vec<u8>>)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        assert_eq!(stored(&db, &table).await.len(), 7);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn unsupported_byte_predicates_send_no_packets_and_leave_transactions_committable() {
    let (db, p) = proxied(Backend::Doris, proxy::Mode::Trace).await;
    let table = seed(&db).await;
    for condition in [
        "b<=(?+1)",
        "b IN(?+1)",
        "concat(b,'x')=?",
        "b+id=?",
        "b LIKE ?",
    ] {
        let before = p.trace().len();
        let e = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {table} WHERE {condition}"
        )))
        .bind(vec![255_u8, 0])
        .fetch_all(db.pool())
        .await
        .unwrap_err();
        assert!(e.to_string().contains("Doris client binding"));
        assert_eq!(p.trace().len(), before);
    }
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET n=1 WHERE b=?"
    )))
    .bind(payloads()[0].clone().unwrap())
    .execute(&mut *tx)
    .await
    .unwrap();
    let before = p.trace().len();
    let e = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {table} WHERE b IN(?+1)"
    )))
    .bind(vec![255_u8])
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert!(e.to_string().contains("Doris client binding"));
    assert_eq!(p.trace().len(), before);
    tx.commit().await.unwrap();
    assert_eq!(stored(&db, &table).await[0].2, 1);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn cancelled_and_lost_byte_filtered_writes_are_not_replayed() {
    byte_write_faults(&["b=?"]).await;
}

async fn byte_write_faults(conditions: &[&str]) {
    for backend in [Backend::MySql, Backend::Doris] {
        let admin = client(backend).await;
        for condition in conditions {
            for native_tx in [false, true] {
                for lost in [false, true] {
                    let mode = if native_tx && backend == Backend::Doris {
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
                    let (db, p) = proxied(backend, mode).await;
                    let table = seed(&admin).await;
                    let sql = format!(
                        "UPDATE {table} SET n=n+1 WHERE {condition} /* {} */",
                        proxy::MARKER
                    );
                    let null_target = condition.contains("id=5");
                    let parameter = if null_target {
                        None
                    } else if condition.contains("NOT IN")
                        || condition.contains("<>")
                        || condition.contains("!=")
                    {
                        Some(vec![255_u8])
                    } else {
                        payloads()[0].clone()
                    };
                    if native_tx {
                        let mut tx = db.pool().begin().await.unwrap();
                        {
                            let future = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                                .bind(&parameter)
                                .execute(&mut *tx);
                            tokio::pin!(future);
                            if lost {
                                assert!(future.await.is_err());
                            } else {
                                tokio::select! {_= &mut future=>panic!("write did not pause"),_=p.reached()=>{}}
                            }
                        }
                        drop(tx);
                    } else {
                        let future = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                            .bind(&parameter)
                            .execute(db.pool());
                        tokio::pin!(future);
                        if lost {
                            assert!(future.await.is_err());
                        } else {
                            tokio::select! {_= &mut future=>panic!("write did not pause"),_=p.reached()=>{}}
                        }
                    }
                    assert_eq!(p.fault_reply(), 0);
                    p.release();
                    let healthy: i32 = sqlx::query_scalar("SELECT 42")
                        .fetch_one(db.pool())
                        .await
                        .unwrap();
                    assert_eq!(healthy, 42);
                    assert_eq!(
                        stored(&admin, &table).await[if null_target { 4 } else { 0 }].2,
                        if native_tx { 0 } else { 1 },
                        "{backend:?}/{native_tx}/{lost}"
                    );
                    let trace = p.trace();
                    let writes = trace
                        .iter()
                        .filter(|v| {
                            if native_tx && backend == Backend::Doris {
                                v[0] == 3 && String::from_utf8_lossy(&v[1..]).starts_with("UPDATE")
                            } else {
                                v[0] == 22 && String::from_utf8_lossy(&v[1..]).starts_with("UPDATE")
                            }
                        })
                        .count();
                    assert_eq!(writes, 1);
                    if !(native_tx && backend == Backend::Doris) {
                        assert_eq!(
                            trace.iter().filter(|v| v[0] == 23).count(),
                            2,
                            "write plus health query, no extra EXECUTE"
                        );
                    }
                    common::drop_table(&admin, &table).await;
                    db.close().await;
                }
            }
        }
        admin.close().await;
    }
}

fn membership_result(value: &Option<Vec<u8>>, candidates: &[Option<Vec<u8>>]) -> Option<i64> {
    let value = value.as_ref()?;
    if candidates.iter().any(|c| c.as_ref() == Some(value)) {
        Some(1)
    } else if candidates.iter().any(Option::is_none) {
        None
    } else {
        Some(0)
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn native_blob_in_and_not_in_preserve_three_valued_null_semantics() {
    let values = payloads();
    let absent = Some(vec![255_u8, 128, 255]);
    let lists = [
        [values[0].clone(), values[3].clone(), absent.clone()],
        [None, values[0].clone(), None],
        [None, None, None],
        [None, values[3].clone(), absent.clone()],
        [values[0].clone(), values[0].clone(), None],
        [values[7].clone(), values[8].clone(), absent],
    ];
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = seed(&db).await;
        let mut c = db.pool().acquire().await.unwrap();
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for fold in [false, true] {
                if backend == Backend::Doris {
                    c.execute(sqlx::AssertSqlSafe(format!(
                        "SET enable_fold_constant_by_be={fold}"
                    )))
                    .await
                    .unwrap();
                }
                for list in &lists {
                    let sql = format!(
                        "SELECT id,CAST(b IN(?,?,?) AS SIGNED),CAST(b NOT IN(?,?,?) AS SIGNED) FROM {table} ORDER BY id"
                    );
                    let mut q = sqlx::query_as(sqlx::AssertSqlSafe(sql));
                    for _ in 0..2 {
                        for item in list {
                            q = q.bind(item);
                        }
                    }
                    let actual: Vec<(i64, Option<i64>, Option<i64>)> =
                        q.fetch_all(&mut *c).await.unwrap();
                    let expected: Vec<_> = values
                        .iter()
                        .enumerate()
                        .map(|(i, v)| {
                            let p = membership_result(v, list);
                            (i as i64 + 1, p, p.map(|p| 1 - p))
                        })
                        .collect();
                    assert_eq!(actual, expected, "{backend:?}/{mode}/{fold}/{list:?}");
                    for (op, desired) in [("IN", Some(1)), ("NOT IN", Some(0)), ("IN", None)] {
                        let suffix = if desired.is_none() { " IS NULL" } else { "" };
                        let sql = format!(
                            "SELECT id FROM {table} WHERE b {op}(?,?,?){suffix} ORDER BY id"
                        );
                        let mut q = sqlx::query_scalar(sqlx::AssertSqlSafe(sql));
                        for item in list {
                            q = q.bind(item);
                        }
                        let actual: Vec<i64> = q.fetch_all(&mut *c).await.unwrap();
                        let expected: Vec<_> = values
                            .iter()
                            .enumerate()
                            .filter(|(_, v)| membership_result(v, list) == desired)
                            .map(|(i, _)| i as i64 + 1)
                            .collect();
                        assert_eq!(
                            actual, expected,
                            "{backend:?}/{mode}/{fold}/{op}/{desired:?}"
                        );
                    }
                }
            }
        }
        drop(c);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn membership_mixed_strings_null_literals_and_native_prepared_cache_work() {
    for backend in [Backend::MySql, Backend::Doris] {
        let admin = client(backend).await;
        let table = seed(&admin).await;
        let (db, p) = proxied(backend, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        let template = format!("SELECT id FROM {table} WHERE b IN(?,?) ORDER BY id");
        c.prepare(sqlx::AssertSqlSafe(template.clone()).into_sql_str())
            .await
            .unwrap();
        for persistent in [true, true, false] {
            let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(template.clone()))
                .bind("A")
                .bind("a")
                .persistent(persistent)
                .fetch_all(&mut *c)
                .await
                .unwrap();
            assert_eq!(actual, [8, 9]);
            let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(template.clone()))
                .bind(payloads()[0].clone())
                .bind(Vec::<u8>::new())
                .persistent(persistent)
                .fetch_all(&mut *c)
                .await
                .unwrap();
            assert_eq!(actual, [1, 4]);
            let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(template.clone()))
                .bind("a")
                .bind(payloads()[0].clone())
                .persistent(persistent)
                .fetch_all(&mut *c)
                .await
                .unwrap();
            assert_eq!(actual, [1, 9]);
        }
        let actual:Vec<(i64,Option<i64>)>=sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT a.id,CAST((a.`b`) NOT IN(((?)),(?),NULL,'A','',?) AS SIGNED) FROM {table} a ORDER BY a.id"))).bind(payloads()[0].clone()).bind("a").bind(None::<i64>).fetch_all(&mut *c).await.unwrap();
        assert_eq!(
            actual,
            [
                (1, Some(0)),
                (2, None),
                (3, None),
                (4, Some(0)),
                (5, None),
                (6, None),
                (7, None),
                (8, Some(0)),
                (9, Some(0))
            ]
        );
        let mut q = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
            "SELECT a.id FROM {table} a JOIN {table} c ON a.id=c.id AND a.b IN("
        ));
        q.push_bind(payloads()[0].clone())
            .push(",")
            .push_bind(b"A".as_slice())
            .push(") WHERE NOT (c.b NOT IN(")
            .push_bind(payloads()[0].clone())
            .push(",")
            .push_bind(b"A".as_slice())
            .push(")) ORDER BY a.id");
        let actual: Vec<i64> = q.build_query_scalar().fetch_all(&mut *c).await.unwrap();
        assert_eq!(actual, [1, 8]);
        let selected: Vec<(i64, Vec<u8>, MySqlTime)> = sqlx::query_as(sqlx::AssertSqlSafe(
            format!("SELECT id,b,CAST(t AS TIME(6)) FROM {table} WHERE b IN(?,?) ORDER BY id"),
        ))
        .bind(payloads()[0].clone())
        .bind(None::<Vec<u8>>)
        .fetch_all(&mut *c)
        .await
        .unwrap();
        assert_eq!(selected, [(1, payloads()[0].clone().unwrap(), clock())]);
        let trace = p.trace();
        if backend == Backend::Doris {
            assert!(
                trace
                    .iter()
                    .any(|v| v[0] == 22
                        && String::from_utf8_lossy(&v[1..]).contains("HEX(b) IN(?,?)"))
            );
            assert!(
                trace.iter().any(|v| v[0] == 22
                    && String::from_utf8_lossy(&v[1..]).contains("HEX(b) IN(HEX(?),?)"))
            );
            assert!(
                !trace.iter().any(|v| v[0] == 3
                    && String::from_utf8_lossy(&v[1..]).contains("SELECT id")
                    && !String::from_utf8_lossy(v).contains("CAST(t AS TIME(6))")),
                "prepared SELECT never falls back to text"
            );
        } else {
            assert!(
                !trace
                    .iter()
                    .any(|v| v[0] == 22 && String::from_utf8_lossy(&v[1..]).contains("HEX(b)"))
            );
        }
        drop(c);
        common::drop_table(&admin, &table).await;
        db.close().await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn membership_transaction_writes_keep_nulls_commit_rollback_and_drop() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        for action in ["rollback", "drop", "commit"] {
            let table = seed(&db).await;
            let initial = stored(&db, &table).await;
            let new = vec![255_u8, 0, 128, 39, 92, 26];
            let mut tx = db.pool().begin().await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE b IN(?,NULL)"
            )))
            .bind(None::<Vec<u8>>)
            .execute(&mut *tx)
            .await
            .unwrap();
            let values = payloads();
            let mut q = sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE b NOT IN(?,?,?,?,?,?,?)"
            )));
            for i in [0, 2, 3, 5, 6, 7, 8] {
                q = q.bind(&values[i]);
            }
            q.execute(&mut *tx).await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {table} SET b=?,n=n+1 WHERE b IN(?,?,NULL)"
            )))
            .bind(&new)
            .bind(&values[0])
            .bind(&values[3])
            .execute(&mut *tx)
            .await
            .unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {table} SET n=999 WHERE b NOT IN(?,?)"
            )))
            .bind(&new)
            .bind(None::<Vec<u8>>)
            .execute(&mut *tx)
            .await
            .unwrap();
            match action {
                "commit" => tx.commit().await.unwrap(),
                "rollback" => tx.rollback().await.unwrap(),
                _ => drop(tx),
            }
            let actual = stored(&db, &table).await;
            let expected = if action == "commit" {
                initial
                    .into_iter()
                    .filter(|v| v.0 != 2)
                    .map(|(id, b, n)| {
                        if [1, 4].contains(&id) {
                            (id, Some(new.clone()), n + 1)
                        } else {
                            (id, b, n)
                        }
                    })
                    .collect()
            } else {
                initial
            };
            assert_eq!(actual, expected, "{backend:?}/{action}");
            common::drop_table(&db, &table).await;
        }
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn unsupported_membership_lists_send_no_packets_or_partial_writes() {
    let (db, p) = proxied(Backend::Doris, proxy::Mode::Trace).await;
    let table = seed(&db).await;
    for condition in [
        "b IN(?+1)",
        "b IN(?,1)",
        "b IN(?,id)",
        "b NOT IN(?,concat('a','b'))",
        "concat(b,'x') IN(?)",
        "(b,id) IN((?,1))",
        "b IN(? COLLATE utf8mb4_bin)",
        "b IN(?)=1",
        "b IN(?,)",
        "b IN(SELECT ?)",
        "b NOT IN(SELECT ?)",
    ] {
        let before = p.trace().len();
        let e = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {table} WHERE {condition}"
        )))
        .bind(vec![255_u8])
        .fetch_all(db.pool())
        .await
        .unwrap_err();
        assert!(
            e.to_string().contains("Doris client binding"),
            "{condition}/{e}"
        );
        assert_eq!(p.trace().len(), before, "{condition}");
    }
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET n=1 WHERE b IN(?)"
    )))
    .bind(payloads()[0].clone())
    .execute(&mut *tx)
    .await
    .unwrap();
    let before = p.trace().len();
    let e = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {table} WHERE b NOT IN(?,?)"
    )))
    .bind(vec![255_u8])
    .bind(1_i64)
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert!(e.to_string().contains("Doris client binding"));
    assert_eq!(p.trace().len(), before);
    tx.commit().await.unwrap();
    assert_eq!(stored(&db, &table).await[0].2, 1);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn cancelled_and_lost_membership_writes_are_not_replayed() {
    byte_write_faults(&["b IN(?,NULL)", "b NOT IN(?) AND id=1"]).await;
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn large_membership_lists_preserve_parameter_order_null_bitmap_and_byte_lengths() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = seed(&db).await;
        let bytes = vec![255_u8; 65536];
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(10,?,?,0)"
        )))
        .bind(&bytes)
        .bind(clock())
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        for count in [1, 8, 9, 250, 251, 257] {
            for has_null in [false, true] {
                for op in ["IN", "NOT IN"] {
                    let mut q = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                        "SELECT id FROM {table} WHERE b {op}("
                    ));
                    {
                        let mut separated = q.separated(",");
                        for i in 0..count {
                            let candidate = if i == count - 1 {
                                Some(bytes.clone())
                            } else if has_null && i % 3 == 0 {
                                None
                            } else {
                                Some(vec![254_u8])
                            };
                            separated.push_bind(candidate);
                        }
                    }
                    q.push(") ORDER BY id");
                    let actual: Vec<i64> =
                        q.build_query_scalar().fetch_all(db.pool()).await.unwrap();
                    let expected = if op == "IN" {
                        vec![10]
                    } else if has_null && count > 1 {
                        vec![]
                    } else {
                        vec![1, 2, 3, 4, 6, 7, 8, 9]
                    };
                    assert_eq!(actual, expected, "{backend:?}/{count}/{has_null}/{op}");
                }
            }
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

fn byte_comparison_result(
    left: &Option<Vec<u8>>,
    right: &Option<Vec<u8>>,
    op: &str,
) -> Option<i64> {
    if op == "<=>" {
        Some(i64::from(left == right))
    } else {
        Some(i64::from(left.as_ref()? != right.as_ref()?))
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn byte_not_equal_and_null_safe_comparisons_keep_projection_and_where_null_truth() {
    let values = payloads();
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = seed(&db).await;
        let mut c = db.pool().acquire().await.unwrap();
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for fold in [false, true] {
                if backend == Backend::Doris {
                    c.execute(sqlx::AssertSqlSafe(format!(
                        "SET enable_fold_constant_by_be={fold}"
                    )))
                    .await
                    .unwrap();
                }
                for op in ["<>", "!=", "<=>"] {
                    for candidate in &values {
                        let actual:Vec<(i64,Option<i64>,Option<i64>)>=sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT id,CAST(b {op} ? AS SIGNED),CAST(? {op} b AS SIGNED) FROM {table} ORDER BY id"))).bind(candidate).bind(candidate).fetch_all(&mut *c).await.unwrap();
                        let expected: Vec<_> = values
                            .iter()
                            .enumerate()
                            .map(|(i, v)| {
                                let p = byte_comparison_result(v, candidate, op);
                                (i as i64 + 1, p, p)
                            })
                            .collect();
                        assert_eq!(
                            actual, expected,
                            "{backend:?}/{mode}/{fold}/{op}/{candidate:?}"
                        );
                        for (condition, desired) in [
                            (format!("b{op}?"), 1),
                            (format!("?{op}b"), 1),
                            (format!("NOT (b{op}?)"), 0),
                        ] {
                            let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(
                                format!("SELECT id FROM {table} WHERE {condition} ORDER BY id"),
                            ))
                            .bind(candidate)
                            .fetch_all(&mut *c)
                            .await
                            .unwrap();
                            let expected: Vec<_> = values
                                .iter()
                                .enumerate()
                                .filter(|(_, v)| {
                                    byte_comparison_result(v, candidate, op) == Some(desired)
                                })
                                .map(|(i, _)| i as i64 + 1)
                                .collect();
                            assert_eq!(
                                actual, expected,
                                "{backend:?}/{mode}/{fold}/{condition}/{candidate:?}"
                            );
                        }
                    }
                }
            }
        }
        drop(c);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn comparison_original_templates_cache_aliases_joins_and_binary_results_work() {
    for backend in [Backend::MySql, Backend::Doris] {
        let admin = client(backend).await;
        let table = seed(&admin).await;
        let (db, p) = proxied(backend, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        for op in ["<>", "!=", "<=>"] {
            let sql = format!("SELECT id FROM {table} WHERE b{op}? ORDER BY id");
            c.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
                .await
                .unwrap();
            for persistent in [true, true, false] {
                let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                    .bind("A")
                    .persistent(persistent)
                    .fetch_all(&mut *c)
                    .await
                    .unwrap();
                assert_eq!(
                    actual,
                    if op == "<=>" {
                        vec![8]
                    } else {
                        vec![1, 2, 3, 4, 6, 7, 9]
                    }
                );
                for candidate in [payloads()[0].clone(), None] {
                    let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(&candidate)
                        .persistent(persistent)
                        .fetch_all(&mut *c)
                        .await
                        .unwrap();
                    let expected: Vec<_> = payloads()
                        .iter()
                        .enumerate()
                        .filter(|(_, v)| byte_comparison_result(v, &candidate, op) == Some(1))
                        .map(|(i, _)| i as i64 + 1)
                        .collect();
                    assert_eq!(actual, expected, "{backend:?}/{op}/{persistent}");
                }
            }
        }
        let actual:Vec<i64>=sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT a.id FROM {table} a JOIN {table} c ON ((a.`b`))<=>((?)) AND a.id=c.id WHERE c.b!=? AND NOT (?<>a.b) ORDER BY a.id"))).bind(payloads()[0].clone()).bind(vec![255_u8]).bind(payloads()[0].clone()).fetch_all(&mut *c).await.unwrap();
        assert_eq!(actual, [1]);
        let mut q =
            sqlx::QueryBuilder::<sqlx::MySql>::new(format!("SELECT id FROM {table} WHERE (b<=>"));
        q.push_bind(None::<Vec<u8>>)
            .push(" OR (b!=")
            .push_bind(vec![255_u8])
            .push(" AND id=1)) ORDER BY id");
        let actual: Vec<i64> = q.build_query_scalar().fetch_all(&mut *c).await.unwrap();
        assert_eq!(actual, [1, 5]);
        let row: (i64, Vec<u8>, MySqlTime) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id,b,CAST(t AS TIME(6)) FROM {table} WHERE b<=>? AND id>=?"
        )))
        .bind(payloads()[0].clone())
        .bind(1_i64)
        .fetch_one(&mut *c)
        .await
        .unwrap();
        assert_eq!(row, (1, payloads()[0].clone().unwrap(), clock()));
        let null_safe: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT CAST(b<=>? AS SIGNED) FROM {table} WHERE id=5"
        )))
        .bind(None::<Vec<u8>>)
        .fetch_one(&mut *c)
        .await
        .unwrap();
        assert_eq!(null_safe, 1);
        let trace = p.trace();
        if backend == Backend::Doris {
            for op in ["<>", "!=", "<=>"] {
                assert!(trace.iter().any(|v| v[0] == 22
                    && String::from_utf8_lossy(&v[1..]).contains(&format!("HEX(b){op}?"))));
            }
            assert!(
                trace
                    .iter()
                    .filter(|v| v[0] == 23 && v.len() > 13)
                    .all(|v| v[12] == 253),
                "native bytes become ASCII VARCHAR parameters"
            );
            assert!(
                !trace.iter().any(|v| v[0] == 3
                    && String::from_utf8_lossy(&v[1..]).starts_with("SELECT")
                    && !String::from_utf8_lossy(v).contains("CAST(t AS TIME(6))")
                    && !String::from_utf8_lossy(v).contains("@@sql_mode")),
                "non-TIME SELECT retains prepared binary rows"
            );
        } else {
            assert!(
                !trace
                    .iter()
                    .any(|v| v[0] == 22 && String::from_utf8_lossy(&v[1..]).contains("HEX(b)"))
            );
            assert!(
                trace
                    .iter()
                    .any(|v| v[0] == 23 && v.len() > 13 && v[12] == 252),
                "MySQL's original BLOB encoding retained"
            );
        }
        drop(c);
        common::drop_table(&admin, &table).await;
        db.close().await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn comparison_transaction_writes_preserve_null_semantics_commit_rollback_and_drop() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        for fold in [false, true] {
            for action in ["rollback", "drop", "commit"] {
                let table = seed(&db).await;
                let mut setup = db.pool().begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES(10,?,?,0)"
                )))
                .bind(None::<Vec<u8>>)
                .bind(clock())
                .execute(&mut *setup)
                .await
                .unwrap();
                setup.commit().await.unwrap();
                let initial = stored(&db, &table).await;
                if backend == Backend::Doris {
                    db.pool()
                        .execute(sqlx::AssertSqlSafe(format!(
                            "SET enable_fold_constant_by_be={fold}"
                        )))
                        .await
                        .unwrap();
                }
                let new = vec![255_u8, 128, 0, 39, 92, 26];
                let original = payloads()[0].clone();
                let mut tx = db.pool().begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE b<=>? AND id=10"
                )))
                .bind(None::<Vec<u8>>)
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {table} SET b=?,n=n+1 WHERE b<=>? AND id=5"
                )))
                .bind(&new)
                .bind(None::<Vec<u8>>)
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {table} SET n=n+1 WHERE b!=? AND id=2"
                )))
                .bind(&original)
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE b<>? AND id=3"
                )))
                .bind(&original)
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {table} SET n=99 WHERE b<>?"
                )))
                .bind(None::<Vec<u8>>)
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE b!=?"
                )))
                .bind(None::<Vec<u8>>)
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE ?<=>b AND id=1"
                )))
                .bind(&original)
                .execute(&mut *tx)
                .await
                .unwrap();
                match action {
                    "commit" => tx.commit().await.unwrap(),
                    "rollback" => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
                let actual = stored(&db, &table).await;
                let expected = if action == "commit" {
                    initial
                        .into_iter()
                        .filter(|v| ![1, 3, 10].contains(&v.0))
                        .map(|(id, b, n)| {
                            if id == 5 {
                                (id, Some(new.clone()), n + 1)
                            } else if id == 2 {
                                (id, b, n + 1)
                            } else {
                                (id, b, n)
                            }
                        })
                        .collect()
                } else {
                    initial
                };
                assert_eq!(actual, expected, "{backend:?}/{fold}/{action}");
                common::drop_table(&db, &table).await;
            }
        }
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn comparison_large_bytes_and_many_null_parameters_keep_lengths_and_order() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = seed(&db).await;
        let large = vec![255_u8; 65536];
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(10,?,?,0)"
        )))
        .bind(&large)
        .bind(clock())
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
        let mut values = payloads();
        values.push(Some(large.clone()));
        for n in [0, 250, 251, 65535, 65536] {
            let candidate = Some(vec![255_u8; n]);
            for op in ["<>", "!=", "<=>"] {
                let actual: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT id FROM {table} WHERE b{op}? ORDER BY id"
                )))
                .bind(&candidate)
                .fetch_all(db.pool())
                .await
                .unwrap();
                let expected: Vec<_> = values
                    .iter()
                    .enumerate()
                    .filter(|(_, v)| byte_comparison_result(v, &candidate, op) == Some(1))
                    .map(|(i, _)| i as i64 + 1)
                    .collect();
                assert_eq!(actual, expected, "{backend:?}/{n}/{op}");
            }
        }
        for count in [1, 8, 9, 17, 257] {
            let mut q =
                sqlx::QueryBuilder::<sqlx::MySql>::new(format!("SELECT id FROM {table} WHERE ("));
            for i in 0..count {
                if i > 0 {
                    q.push(" OR ");
                }
                q.push("b<=>").push_bind(if i == count - 1 {
                    Some(large.clone())
                } else if i % 3 == 0 {
                    None
                } else {
                    Some(vec![254_u8])
                });
            }
            q.push(") ORDER BY id");
            let actual: Vec<i64> = q.build_query_scalar().fetch_all(db.pool()).await.unwrap();
            assert_eq!(
                actual,
                if count == 1 { vec![10] } else { vec![5, 10] },
                "{backend:?}/{count}"
            );
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn unsupported_comparison_shapes_send_no_packets_and_preserve_prior_writes() {
    let (db, p) = proxied(Backend::Doris, proxy::Mode::Trace).await;
    let table = seed(&db).await;
    for condition in [
        "b<=(?+1)",
        "b>=(?+1)",
        "concat(b,'x')<>?",
        "b<=>?=id",
        "id=b!=?",
        "b ! = ?",
        "b < = > ?",
        "b<==>?",
        "b<=>=?",
        "b==?",
        "?!=b+id",
    ] {
        let before = p.trace().len();
        let e = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {table} WHERE {condition}"
        )))
        .bind(vec![255_u8])
        .fetch_all(db.pool())
        .await
        .unwrap_err();
        assert!(
            e.to_string().contains("Doris client binding"),
            "{condition}/{e}"
        );
        assert_eq!(p.trace().len(), before, "{condition}");
    }
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table} SET n=1 WHERE b<=>?"
    )))
    .bind(payloads()[0].clone())
    .execute(&mut *tx)
    .await
    .unwrap();
    let before = p.trace().len();
    let e = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {table} WHERE concat(b,'x')<>?"
    )))
    .bind(vec![255_u8])
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert!(e.to_string().contains("Doris client binding"));
    assert_eq!(p.trace().len(), before);
    tx.commit().await.unwrap();
    assert_eq!(stored(&db, &table).await[0].2, 1);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn cancelled_and_lost_not_equal_and_null_safe_writes_are_not_replayed() {
    byte_write_faults(&["b<>? AND id=1", "b!=? AND id=1", "b<=>?", "b<=>? AND id=5"]).await;
}
