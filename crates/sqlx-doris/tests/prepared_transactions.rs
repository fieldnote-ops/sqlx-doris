//! Safe binding and transaction boundaries against unmodified official servers.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;

use bigdecimal::BigDecimal;
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{self, Connection, Executor, SqlSafeStr},
};
use std::{str::FromStr, time::Duration};

fn official_rejection(error: sqlx::Error) {
    let sqlx::Error::Database(error) = error else {
        panic!("expected complete official FE ERR packet: {error:?}")
    };
    assert_eq!(error.code().as_deref(), Some("HY000"));
    assert!(
        error.message().contains("This is in a transaction"),
        "{error}"
    );
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
async fn proxied(backend: Backend, mode: proxy::Mode) -> (Client, proxy::Proxy) {
    let proxy = proxy::Proxy::start(&options(backend), mode).await;
    let db = PoolBuilder::new(backend)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(4))
        .connect_with(proxy.options(options(backend)))
        .await
        .unwrap();
    (db, proxy)
}
async fn create(db: &Client, rich: bool) -> TableName {
    let table = common::name();
    let fields = if rich {
        "id BIGINT NOT NULL, message VARCHAR(1024), n BIGINT, amount DECIMAL(27,9), \
            active BOOLEAN, event DATETIME(6)"
    } else {
        "id BIGINT NOT NULL, value BIGINT"
    };
    let suffix = if db.backend() == Backend::Doris {
        "UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        ""
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {table}({fields}) {suffix}"
        )))
        .await
        .unwrap();
    table
}
async fn rows(db: &Client, table: &TableName) -> Vec<(i64, i64)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,value FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires unmodified dedicated MySQL and Doris"]
async fn bound_values_cached_before_or_prepared_after_begin_commit_rollback_and_drop() {
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let (db, proxy) = proxied(backend, proxy::Mode::Trace).await;
        let table = create(&db, true).await;
        let sql = format!("INSERT INTO {table} VALUES(?,?,?,?,?,?)");
        let message = "引号'\"; -- ? \\ \0\n\r\t😀";
        let amount = BigDecimal::from_str("123456789012345678.123456789").unwrap();
        let event = chrono::NaiveDateTime::parse_from_str(
            "2026-10-27 12:34:56.123456",
            "%Y-%m-%d %H:%M:%S%.f",
        )
        .unwrap();
        for cached in [false, true] {
            // A distinct SQL string proves a true prepare after BEGIN even if the
            // pool has a cached copy from the previous iteration.
            let sql = format!("{sql} /* cached={cached} */");
            if cached {
                let mut c = db.pool().acquire().await.unwrap();
                c.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
                    .await
                    .unwrap();
            }
            for action in ["commit", "rollback", "drop"] {
                let mut tx: sqlx::Transaction<'_, sqlx::MySql> = db.pool().begin().await.unwrap();
                for (id, n) in [(1_i64, Some(i64::MIN)), (2, None)] {
                    let result = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(id)
                        .bind(message)
                        .bind(n)
                        .bind(&amount)
                        .bind(true)
                        .bind(event)
                        .execute(&mut *tx)
                        .await;
                    {
                        result.unwrap();
                    }
                }
                match action {
                    "commit" => tx.commit().await.unwrap(),
                    "rollback" => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
                let actual: Vec<(
                    i64,
                    String,
                    Option<i64>,
                    BigDecimal,
                    bool,
                    chrono::NaiveDateTime,
                )> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT * FROM {table} ORDER BY id"
                )))
                .fetch_all(db.pool())
                .await
                .unwrap();
                if action == "commit" {
                    assert_eq!(
                        actual,
                        [
                            (
                                1,
                                message.to_owned(),
                                Some(i64::MIN),
                                amount.clone(),
                                true,
                                event
                            ),
                            (2, message.to_owned(), None, amount.clone(), true, event)
                        ]
                    );
                } else {
                    assert!(actual.is_empty(), "{backend:?}/{action}: {actual:?}");
                }
                db.pool()
                    .execute(sqlx::AssertSqlSafe(format!("DELETE FROM {table}")))
                    .await
                    .unwrap();
            }
        }
        let trace = proxy.trace();
        let inserts: Vec<_> = trace
            .iter()
            .filter(|p| p[0] == 22 && String::from_utf8_lossy(&p[1..]).starts_with("INSERT"))
            .collect();
        assert!(inserts.iter().all(|p| p.windows(6).any(|x| x == b"?,?,?,")));
        assert!(!inserts.is_empty());
        let text_inserts: Vec<_> = trace
            .iter()
            .filter(|p| p[0] == 3 && String::from_utf8_lossy(&p[1..]).starts_with("INSERT"))
            .collect();
        assert_eq!(
            text_inserts.len(),
            if backend == Backend::Doris { 12 } else { 0 }
        );
        if backend == Backend::Doris {
            assert!(text_inserts.iter().all(|p| {
                let sql = String::from_utf8_lossy(p);
                sql.contains("VALUES(")
                    && sql.contains("IF(@@sql_mode IS NULL,")
                    && !sql.contains("UNION ALL")
            }));
        }
        let executes: Vec<_> = trace.iter().filter(|p| p[0] == 23).collect();
        if backend == Backend::MySql {
            assert!(
                executes
                    .iter()
                    .any(|p| p.windows(message.len()).any(|w| w == message.as_bytes()))
            );
            assert!(
                executes.iter().any(|p| p.len() > 11 && p[10] & 4 != 0),
                "NULL bitmap for third parameter"
            );
        }
        println!(
            "binding trace {backend:?} safe_client_binding={}: prepare={} execute={}",
            backend == Backend::Doris,
            inserts.len(),
            executes.len()
        );
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated dual databases"]
async fn prepared_update_delete_and_insert_select_obey_commit_and_rollback() {
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let db = common::client(backend).await;
        for commit in [false, true] {
            let source = create(&db, false).await;
            let dest = create(&db, false).await;
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {source} VALUES(1,10),(2,20),(3,30)"
                )))
                .await
                .unwrap();
            let mut tx = db.pool().begin().await.unwrap();
            let queries = [
                (
                    format!("UPDATE {source} SET value=value+? WHERE id=?"),
                    vec![100_i64, 1],
                ),
                (format!("DELETE FROM {source} WHERE id=?"), vec![3]),
                (
                    format!("INSERT INTO {dest} SELECT id+?,value+? FROM {source} WHERE id=?"),
                    vec![10, 200, 2],
                ),
            ];
            for (sql, values) in queries {
                let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
                for value in values {
                    q = q.bind(value);
                }
                let result = q.execute(&mut *tx).await;
                {
                    result.unwrap();
                }
            }
            if commit {
                tx.commit().await.unwrap();
            } else {
                tx.rollback().await.unwrap();
            }
            if commit {
                assert_eq!(rows(&db, &source).await, [(1, 110), (2, 20)]);
                assert_eq!(rows(&db, &dest).await, [(12, 220)]);
            } else {
                assert_eq!(rows(&db, &source).await, [(1, 10), (2, 20), (3, 30)]);
                assert!(rows(&db, &dest).await.is_empty());
            }
            common::drop_table(&db, &source).await;
            common::drop_table(&db, &dest).await;
        }
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn cached_prepared_select_and_ddl_cannot_bypass_transaction_restrictions() {
    let db = common::client(Backend::Doris).await;
    let table = create(&db, false).await;
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(1,10)"
        )))
        .await
        .unwrap();
    let select = format!("SELECT value FROM {table} WHERE id=?");
    let mut c = db.pool().acquire().await.unwrap();
    c.execute("SET enable_short_circuit_query=true")
        .await
        .unwrap();
    // Execute twice before BEGIN to populate FE's point-query fast-path cache.
    for _ in 0..2 {
        let n: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(select.clone()))
            .bind(1_i64)
            .fetch_all(&mut *c)
            .await
            .unwrap();
        assert_eq!(n, [10]);
    }
    for sql in [&select, &format!("DROP TABLE {table}")] {
        let mut tx = c.begin().await.unwrap();
        let result = if sql == &select {
            sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                .bind(1_i64)
                .execute(&mut *tx)
                .await
        } else {
            sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                .execute(&mut *tx)
                .await
        };
        official_rejection(result.unwrap_err());
        tx.rollback().await.unwrap();
    }
    drop(c);
    assert_eq!(rows(&db, &table).await, [(1, 10)]);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated dual databases"]
async fn cancelled_prepare_or_execute_retires_connection_and_rolls_back_without_replay() {
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let admin = common::client(backend).await;
        let modes = if backend == Backend::Doris {
            vec![proxy::Mode::PauseReply, proxy::Mode::LostWriteReply]
        } else {
            vec![
                proxy::Mode::PausePrepare,
                proxy::Mode::PauseExecute,
                proxy::Mode::LostExecuteReply,
            ]
        };
        for mode in modes {
            let table = create(&admin, false).await;
            let (db, proxy) = proxied(backend, mode).await;
            let sql = format!("INSERT INTO {table} VALUES(?,?) /* {} */", proxy::MARKER);
            // EXECUTE must be reached on the official FE as well: prepare before BEGIN.
            if backend == Backend::MySql && !matches!(mode, proxy::Mode::PausePrepare) {
                let mut c = db.pool().acquire().await.unwrap();
                c.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
                    .await
                    .unwrap();
            }
            let mut tx = db.pool().begin().await.unwrap();
            if matches!(
                mode,
                proxy::Mode::LostExecuteReply | proxy::Mode::LostWriteReply
            ) {
                assert!(
                    sqlx::query(sqlx::AssertSqlSafe(sql))
                        .bind(1_i64)
                        .bind(10_i64)
                        .execute(&mut *tx)
                        .await
                        .is_err()
                );
                drop(tx);
            } else {
                {
                    let future = sqlx::query(sqlx::AssertSqlSafe(sql))
                        .bind(1_i64)
                        .bind(10_i64)
                        .execute(&mut *tx);
                    tokio::pin!(future);
                    tokio::select! {
                        r = &mut future => panic!("fault completed early: {r:?}"),
                        _ = proxy.reached() => {}
                    }
                }
                if backend == Backend::Doris {
                    assert!(tx.commit().await.is_err());
                } else {
                    drop(tx);
                }
                proxy.release();
            }
            assert_eq!(proxy.fault_reply(), { 0 });
            let writes = proxy.trace().iter().filter(|p| p[0] == 23).count();
            assert_eq!(
                writes,
                usize::from(
                    backend == Backend::MySql && !matches!(mode, proxy::Mode::PausePrepare)
                )
            );
            let n: i64 = tokio::time::timeout(
                Duration::from_secs(5),
                sqlx::query_scalar("SELECT 42").fetch_one(db.pool()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(n, 42);
            assert!(rows(&admin, &table).await.is_empty());
            assert_eq!(proxy.hits(), 1);
            let trace = proxy.trace();
            // The only additional execute is the recovery SELECT; the INSERT
            // was never prepared or executed a second time on another socket.
            assert_eq!(trace.iter().filter(|p| p[0] == 23).count(), writes + 1);
            assert_eq!(
                trace
                    .iter()
                    .filter(|p| p[0] == 22
                        && String::from_utf8_lossy(&p[1..]).starts_with("INSERT"))
                    .count(),
                usize::from(backend == Backend::MySql)
            );
            println!(
                "cancel {backend:?} prepare={} reply={} execute_count={writes} rows=0 recovered=42",
                matches!(mode, proxy::Mode::PausePrepare),
                proxy.fault_reply()
            );
            common::drop_table(&admin, &table).await;
            db.close().await;
        }
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated dual databases"]
async fn cancelled_or_lost_commit_reply_after_bound_write_is_never_replayed() {
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let admin = common::client(backend).await;
        for cancelled in [false, true] {
            let table = create(&admin, false).await;
            let (db, proxy) = proxied(
                backend,
                if cancelled {
                    proxy::Mode::PauseCommit
                } else {
                    proxy::Mode::LostCommitReply
                },
            )
            .await;
            let mut tx = db.pool().begin().await.unwrap();
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(?,?)"
            )))
            .bind(1_i64)
            .bind(10_i64)
            .execute(&mut *tx)
            .await;
            {
                result.unwrap();
            }
            if cancelled {
                {
                    let future = tx.commit();
                    tokio::pin!(future);
                    tokio::select! {
                        r = &mut future => panic!("commit completed early: {r:?}"),
                        _ = proxy.reached() => {}
                    }
                }
                proxy.release();
            } else {
                assert!(tx.commit().await.is_err());
            }
            let n: i64 = tokio::time::timeout(
                Duration::from_secs(5),
                sqlx::query_scalar("SELECT 42").fetch_one(db.pool()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(n, 42);
            assert_eq!(rows(&admin, &table).await, { vec![(1, 10)] });
            assert_eq!(proxy.hits(), 1);
            assert_eq!(
                proxy
                    .trace()
                    .iter()
                    .filter(|p| p[0] == 3 && &p[1..] == b"COMMIT")
                    .count(),
                1
            );
            common::drop_table(&admin, &table).await;
            db.close().await;
        }
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated dual databases"]
async fn complete_prepared_statement_error_preserves_prior_and_later_bound_writes() {
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let db = common::client(backend).await;
        let table = create(&db, false).await;
        let missing = common::name();
        let mut tx = db.pool().begin().await.unwrap();
        for id in [1_i64, 3] {
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(?,?)"
            )))
            .bind(id)
            .bind(id * 10)
            .execute(&mut *tx)
            .await;
            {
                result.unwrap();
            }
            if id == 1 {
                let error = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {missing} VALUES(?)"
                )))
                .bind(2_i64)
                .execute(&mut *tx)
                .await
                .unwrap_err();
                {
                    assert!(matches!(error, sqlx::Error::Database(_)), "{error:?}");
                    assert!(error.to_string().contains(&missing.table), "{error}");
                }
            }
        }
        tx.commit().await.unwrap();
        assert_eq!(rows(&db, &table).await, { vec![(1, 10), (3, 30)] });
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn cached_group_commit_planner_cannot_autocommit_a_prepared_transaction() {
    let db = PoolBuilder::new(Backend::Doris)
        .max_connections(1)
        .connect_with(options(Backend::Doris))
        .await
        .unwrap();
    let table = create(&db, false).await;
    let sql = format!("INSERT INTO {table} VALUES(?,?)");
    let mut c = db.pool().acquire().await.unwrap();
    c.execute("SET group_commit='sync_mode'").await.unwrap();
    c.execute("SET enable_group_commit_full_prepare=true")
        .await
        .unwrap();
    for id in [1_i64, 2] {
        sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
            .bind(id)
            .bind(id * 10)
            .execute(&mut *c)
            .await
            .unwrap();
    }
    let mut tx = c.begin().await.unwrap();
    let result = sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(3_i64)
        .bind(30_i64)
        .execute(&mut *tx)
        .await;
    result.unwrap();
    tx.rollback().await.unwrap();
    drop(c);
    assert_eq!(rows(&db, &table).await, [(1, 10), (2, 20)]);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated dual databases"]
async fn mixed_text_and_bound_values_commit_rollback_and_drop_in_both_orders() {
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let (db, proxy) = proxied(backend, proxy::Mode::Trace).await;
        let table = create(&db, false).await;
        for cached in [false, true] {
            for bound_first in [false, true] {
                for action in ["commit", "rollback", "drop"] {
                    let sql = format!(
                        "INSERT INTO {table} VALUES(?,?),(?,?) /* {cached}/{bound_first}/{action} */"
                    );
                    if cached {
                        let mut c = db.pool().acquire().await.unwrap();
                        c.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
                            .await
                            .unwrap();
                    }
                    let mut tx: sqlx::Transaction<'_, sqlx::MySql> =
                        db.pool().begin().await.unwrap();
                    for bound in [bound_first, !bound_first] {
                        if bound {
                            let result = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                                .bind(2_i64)
                                .bind(-20_i64)
                                .bind(3_i64)
                                .bind(30_i64)
                                .execute(&mut *tx)
                                .await;
                            {
                                result.unwrap();
                            }
                        } else {
                            (&mut *tx)
                                .execute(sqlx::AssertSqlSafe(format!(
                                    "INSERT INTO {table} VALUES(1,10)"
                                )))
                                .await
                                .unwrap();
                        }
                    }
                    // After the two statement styles have mixed, reuse the same
                    // prepared ID with new values. Old bindings must not leak.
                    let result = sqlx::query(sqlx::AssertSqlSafe(sql))
                        .bind(4_i64)
                        .bind(40_i64)
                        .bind(5_i64)
                        .bind(50_i64)
                        .execute(&mut *tx)
                        .await;
                    {
                        result.unwrap();
                    }
                    match action {
                        "commit" => tx.commit().await.unwrap(),
                        "rollback" => tx.rollback().await.unwrap(),
                        _ => drop(tx),
                    }
                    let expected = if action == "commit" {
                        vec![(1, 10), (2, -20), (3, 30), (4, 40), (5, 50)]
                    } else {
                        vec![]
                    };
                    assert_eq!(
                        rows(&db, &table).await,
                        expected,
                        "{backend:?}/{cached}/{bound_first}/{action}"
                    );
                    db.pool()
                        .execute(sqlx::AssertSqlSafe(format!("DELETE FROM {table}")))
                        .await
                        .unwrap();
                }
            }
        }
        let trace = proxy.trace();
        let text_inserts: Vec<_> = trace
            .iter()
            .filter(|p| p[0] == 3 && String::from_utf8_lossy(&p[1..]).starts_with("INSERT"))
            .collect();
        assert_eq!(
            text_inserts.len(),
            if backend == Backend::Doris { 36 } else { 12 }
        );
        assert!(text_inserts.iter().all(|p| backend == Backend::Doris
            || String::from_utf8_lossy(&p[1..]).ends_with("VALUES(1,10)")));
        println!(
            "mixed VALUES {backend:?}: both orders/cached/new/commit/rollback/drop, \
                shared bind API verified"
        );
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated dual databases"]
async fn mixed_values_cancel_prepare_execute_text_commit_or_rollback_without_replay() {
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let admin = common::client(backend).await;
        for (case, mode) in [
            ("prepare", proxy::Mode::PausePrepare),
            ("execute", proxy::Mode::PauseExecute),
            ("text", proxy::Mode::PauseReply),
            ("execute-lost", proxy::Mode::LostExecuteReply),
            ("commit", proxy::Mode::PauseCommit),
            ("commit-lost", proxy::Mode::LostCommitReply),
            ("rollback", proxy::Mode::PauseRollback),
            ("rollback-lost", proxy::Mode::LostRollbackReply),
        ] {
            if backend == Backend::Doris && case == "prepare" {
                println!(
                    "Doris binding preflight has no PREPARE network stage; validated in driver unit tests"
                );
                continue;
            }
            let mode = if backend == Backend::Doris {
                match case {
                    "execute" => proxy::Mode::PauseReply,
                    "execute-lost" => proxy::Mode::LostWriteReply,
                    _ => mode,
                }
            } else {
                mode
            };
            println!("mixed cancellation {backend:?}/{case}");
            let table = create(&admin, false).await;
            let (db, proxy) = proxied(backend, mode).await;
            proxy.suspend_faults(true);
            let sql = format!("INSERT INTO {table} VALUES(?,?) /* {} */", proxy::MARKER);
            let mut tx = db.pool().begin().await.unwrap();
            (&mut *tx)
                .execute(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES(1,10)"
                )))
                .await
                .unwrap();
            let result = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                .bind(2_i64)
                .bind(20_i64)
                .execute(&mut *tx)
                .await;
            {
                result.unwrap();
            }
            // Official PREPARE was rejected; prepare the target before BEGIN is
            // covered separately. Here EXECUTE faults only run for supported FE.
            let binary = case.starts_with("execute");
            {}
            proxy.suspend_faults(false);
            let executes_before = proxy.trace().iter().filter(|p| p[0] == 23).count();
            if case.starts_with("commit") {
                if case.ends_with("lost") {
                    assert!(tx.commit().await.is_err());
                } else {
                    {
                        let future = tx.commit();
                        tokio::pin!(future);
                        tokio::select! {
                            r = &mut future => panic!("commit completed before fault: {r:?}"),
                            _ = proxy.reached() => {}
                        }
                    }
                    proxy.release();
                }
            } else if case.starts_with("rollback") {
                if case.ends_with("lost") {
                    assert!(tx.rollback().await.is_err());
                } else {
                    {
                        let future = tx.rollback();
                        tokio::pin!(future);
                        tokio::select! {
                            r = &mut future => panic!("rollback completed before fault: {r:?}"),
                            _ = proxy.reached() => {}
                        }
                    }
                    proxy.release();
                }
            } else if case == "execute-lost" {
                assert!(
                    sqlx::query(sqlx::AssertSqlSafe(sql))
                        .bind(3_i64)
                        .bind(30_i64)
                        .execute(&mut *tx)
                        .await
                        .is_err()
                );
                drop(tx);
            } else {
                {
                    let sql = if case == "prepare" {
                        format!("{sql} /* new preparation */")
                    } else {
                        sql
                    };
                    let future = async {
                        if case == "text" {
                            (&mut *tx)
                                .execute(sqlx::AssertSqlSafe(format!(
                                    "INSERT INTO {table} VALUES(3,30) /* {} */",
                                    proxy::MARKER
                                )))
                                .await
                        } else {
                            sqlx::query(sqlx::AssertSqlSafe(sql))
                                .bind(3_i64)
                                .bind(30_i64)
                                .execute(&mut *tx)
                                .await
                        }
                    };
                    tokio::pin!(future);
                    tokio::select! {
                        r = &mut future => panic!("write completed before fault: {r:?}"),
                        _ = proxy.reached() => {}
                    }
                }
                if backend == Backend::Doris {
                    assert!(tx.commit().await.is_err());
                } else {
                    drop(tx);
                }
                proxy.release();
            }
            assert_eq!(proxy.hits(), 1);
            assert_eq!(proxy.fault_reply(), { 0 });
            let executes_at_fault = proxy.trace().iter().filter(|p| p[0] == 23).count();
            assert_eq!(
                executes_at_fault,
                executes_before + usize::from(binary && backend == Backend::MySql)
            );
            let recovered: i64 = tokio::time::timeout(
                Duration::from_secs(5),
                sqlx::query_scalar("SELECT 42").fetch_one(db.pool()),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(recovered, 42);
            let expected = if case.starts_with("commit") {
                vec![(1, 10), (2, 20)]
            } else {
                vec![]
            };
            assert_eq!(rows(&admin, &table).await, expected, "{backend:?}/{case}");
            let trace = proxy.trace();
            assert_eq!(
                trace.iter().filter(|p| p[0] == 23).count(),
                executes_at_fault + 1
            );
            for command in ["COMMIT", "ROLLBACK"] {
                if case.starts_with(&command.to_lowercase()) {
                    let count = trace
                        .iter()
                        .filter(|p| p[0] == 3 && &p[1..] == command.as_bytes())
                        .count();
                    if backend == Backend::MySql && case == "rollback" {
                        // Upstream SQLx Drop may queue another harmless ROLLBACK
                        // while the cancelled rollback future still owns an open
                        // transaction. Keep native MySQL cleanup behavior.
                        assert!((1..=2).contains(&count));
                    } else {
                        assert_eq!(count, 1);
                    }
                    println!("{backend:?}/{case}: {command} packets={count}");
                }
            }
            // A new connection may prepare SELECT 42, but never the INSERT.
            let preparations = trace
                .iter()
                .filter(|p| p[0] == 22 && String::from_utf8_lossy(&p[1..]).starts_with("INSERT"))
                .count();
            assert_eq!(
                preparations,
                if backend == Backend::Doris {
                    0
                } else if case == "prepare" {
                    2
                } else {
                    1
                }
            );
            common::drop_table(&admin, &table).await;
            db.close().await;
        }
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris"]
async fn normalized_values_share_the_execution_engine_with_cross_table_and_select_writes() {
    let db = common::client(Backend::Doris).await;
    let table = create(&db, false).await;
    let other = create(&db, false).await;
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {other} VALUES(9,90)"
        )))
        .await
        .unwrap();
    for (insert_select, commit) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut tx = db.pool().begin().await.unwrap();
        (&mut *tx)
            .execute(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(1,10)"
            )))
            .await
            .unwrap();
        let sql = if insert_select {
            format!("INSERT INTO {table} SELECT id+?,value FROM {other}")
        } else {
            format!("INSERT INTO {other} VALUES(?,20)")
        };
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(2_i64)
            .execute(&mut *tx)
            .await
            .unwrap();
        if commit {
            tx.commit().await.unwrap();
        } else {
            tx.rollback().await.unwrap();
        }
        assert_eq!(
            rows(&db, &table).await,
            if !commit {
                vec![]
            } else if insert_select {
                vec![(1, 10), (11, 90)]
            } else {
                vec![(1, 10)]
            }
        );
        assert_eq!(
            rows(&db, &other).await,
            if commit && !insert_select {
                vec![(2, 20), (9, 90)]
            } else {
                vec![(9, 90)]
            }
        );
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE id>=0"
            )))
            .await
            .unwrap();
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {other} WHERE id=2"
            )))
            .await
            .unwrap();
    }
    common::drop_table(&db, &table).await;
    common::drop_table(&db, &other).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires unmodified dedicated MySQL and Doris"]
async fn client_binding_sql_modes_injection_payloads_and_emulated_dml_prepare() {
    use sqlx::Statement;
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let db = PoolBuilder::new(backend)
            .max_connections(1)
            .connect_with(options(backend))
            .await
            .unwrap();
        let table = common::name();
        let suffix = if backend == Backend::Doris {
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
        } else {
            ""
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {table}(id BIGINT,v TEXT) {suffix}"
            )))
            .await
            .unwrap();
        let mut payloads = vec![
            "'; DROP TABLE t; -- ?",
            "中😀\\\0'\"\n\r\t\u{8}\u{1a}&",
            "\\\0",
            "a\\b",
            "",
            "/* ? */ # ; \\\\",
        ];
        let ascii_payload: String = (0_u8..=127).map(char::from).collect();
        payloads.push(&ascii_payload);
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            let mut c = db.pool().acquire().await.unwrap();
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            let mut tx = c.begin().await.unwrap();
            let sql = format!("INSERT INTO {table}(id,v) VALUES(?,?) /* '?' */");
            let statement = (&mut *tx)
                .prepare(sqlx::AssertSqlSafe(sql).into_sql_str())
                .await
                .unwrap();
            for (i, payload) in payloads.iter().enumerate() {
                statement
                    .query()
                    .bind(i as i64)
                    .bind(*payload)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
            }
            tx.commit().await.unwrap();
            drop(c);
            let actual: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT v FROM {table} ORDER BY id"
            )))
            .fetch_all(db.pool())
            .await
            .unwrap();
            assert_eq!(actual, payloads, "{backend:?}/{mode}");
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE id>=0"
                )))
                .await
                .unwrap();
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires unmodified dedicated Doris"]
async fn unsafe_binding_forms_fail_before_wire_and_keep_prior_write_committable() {
    let (db, proxy) = proxied(Backend::Doris, proxy::Mode::Trace).await;
    assert!(db.capabilities().bound_parameters_in_explicit_transactions);
    assert!(!db.capabilities().prepared_in_explicit_transactions);
    let table = create(&db, false).await;
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES(?,?)"
    )))
    .bind(1_i64)
    .bind(10_i64)
    .execute(&mut *tx)
    .await
    .unwrap();
    for sql in [
        format!("INSERT INTO {table} VALUES(?,?)"),
        format!("INSERT INTO {table} VALUES(?,1); DROP TABLE {table}"),
        format!("INSERT INTO {table} VALUES(?, 'a\\' ?')"),
    ] {
        let before = proxy.trace().len();
        let error = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(2_i64)
            .execute(&mut *tx)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("Doris client binding"),
            "{error}"
        );
        assert_eq!(
            proxy.trace().len(),
            before,
            "preflight must send no request"
        );
    }
    let before = proxy.trace().len();
    let error = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES(2,?)"
    )))
    .bind(f64::NAN)
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert!(error.to_string().contains("non-finite"));
    assert_eq!(proxy.trace().len(), before);
    tx.commit().await.unwrap();
    assert_eq!(rows(&db, &table).await, [(1, 10)]);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires unmodified dedicated MySQL and Doris"]
async fn finite_float_and_scientific_decimal_bindings_in_native_transactions() {
    for backend in [Backend::MySql, Backend::Doris] {
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        assert!(
            backend
                .capabilities()
                .bound_parameters_in_explicit_transactions
        );
        let db = common::client(backend).await;
        let table = common::name();
        let suffix = if backend == Backend::Doris {
            "DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
        } else {
            ""
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {table}(id BIGINT,d DOUBLE,f FLOAT,n DECIMAL(30,10)) {suffix}"
            )))
            .await
            .unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        let decimal = BigDecimal::from_str("1E-10").unwrap();
        for (id, d, f) in [
            (1_i64, 1.25_f64, 1.25_f32),
            (2, 1.0e50, f32::MAX),
            (3, -0.0, f32::MIN_POSITIVE),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(?,?,?,?)"
            )))
            .bind(id)
            .bind(d)
            .bind(f)
            .bind(&decimal)
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        let actual: Vec<(i64, f64, f32, BigDecimal)> = sqlx::query_as(sqlx::AssertSqlSafe(
            format!("SELECT * FROM {table} ORDER BY id"),
        ))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(
            actual,
            vec![
                (1, 1.25, 1.25, decimal.clone()),
                (2, 1.0e50, f32::MAX, decimal.clone()),
                (3, -0.0, f32::MIN_POSITIVE, decimal)
            ]
        );
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}
