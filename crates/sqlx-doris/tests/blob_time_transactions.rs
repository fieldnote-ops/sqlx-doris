//! Native SQLx binding through official MySQL and Doris transaction protocols.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;

use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{
        self, Connection, Executor, SqlSafeStr, Statement,
        mysql::types::{MySqlTime, MySqlTimeSign},
    },
};
use std::{str::FromStr, time::Duration};

fn options(backend: Backend) -> sqlx::mysql::MySqlConnectOptions {
    sqlx::mysql::MySqlConnectOptions::from_str(
        &std::env::var(match backend {
            Backend::MySql => "MYSQL_TEST_URL",
            Backend::Doris => "DORIS_TEST_URL",
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
async fn create(db: &Client) -> TableName {
    let table = common::name();
    let definition = if db.backend() == Backend::MySql {
        "(id BIGINT PRIMARY KEY,b LONGBLOB,t TIME(6))"
    } else {
        // Doris cannot store native VARBINARY or TIME OLAP columns. This is
        // fixture DDL, not an automatic schema/type or application-data rewrite.
        "(id BIGINT,b STRING,t VARCHAR(64)) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) \
            BUCKETS 1 PROPERTIES('replication_num'='1')"
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {table}{definition}"
        )))
        .await
        .unwrap();
    table
}
type Stored = (i64, Option<Vec<u8>>, Option<MySqlTime>);
async fn rows(db: &Client, table: &TableName) -> Vec<Stored> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,b,CAST(t AS TIME(6)) FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}
async fn clear(db: &Client, table: &TableName) {
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE id>=0"
        )))
        .await
        .unwrap();
}
fn signed_time() -> MySqlTime {
    MySqlTime::new(MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn blob_all_bytes_and_native_time_values_modes_cache_commit_rollback_and_drop() {
    let mut payload = "中😀'\"; DROP TABLE t; -- /* ? */ \\".as_bytes().to_vec();
    payload.extend(0..=255);
    let values = [
        MySqlTime::ZERO,
        signed_time(),
        MySqlTime::MIN,
        MySqlTime::MAX,
        MySqlTime::new(MySqlTimeSign::Positive, 25, 2, 3, 123456).unwrap(),
        MySqlTime::new(MySqlTimeSign::Negative, 123, 45, 6, 999999).unwrap(),
    ];
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = create(&db).await;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            let mut c = db.pool().acquire().await.unwrap();
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for cached in [false, true] {
                for action in ["commit", "rollback", "drop"] {
                    let sql =
                        format!("INSERT INTO {table} VALUES(?,?,?) /* {mode}/{cached}/{action} */");
                    if cached {
                        c.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
                            .await
                            .unwrap();
                    }
                    let mut tx: sqlx::Transaction<'_, sqlx::MySql> = c.begin().await.unwrap();
                    let statement = (&mut *tx)
                        .prepare(sqlx::AssertSqlSafe(sql).into_sql_str())
                        .await
                        .unwrap();
                    for (id, &time) in values.iter().enumerate() {
                        statement
                            .query()
                            .bind(id as i64)
                            .bind(payload.as_slice())
                            .bind(time)
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                    }
                    statement
                        .query()
                        .bind(6_i64)
                        .bind(Vec::<u8>::new())
                        .bind(None::<MySqlTime>)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    statement
                        .query()
                        .bind(7_i64)
                        .bind(None::<Vec<u8>>)
                        .bind(MySqlTime::ZERO)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    statement
                        .query()
                        .bind(8_i64)
                        .bind(b"clock".as_slice())
                        .bind(chrono::NaiveTime::from_hms_micro_opt(12, 34, 56, 123456).unwrap())
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    statement
                        .query()
                        .bind(9_i64)
                        .bind(b"duration".as_slice())
                        .bind(
                            MySqlTime::try_from(
                                Duration::from_secs(25 * 3600 + 123)
                                    + Duration::from_micros(987654),
                            )
                            .unwrap(),
                        )
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    // Mix original textual VALUES with native bindings in one transaction.
                    (&mut *tx)
                        .execute(sqlx::AssertSqlSafe(format!(
                            "INSERT INTO {table} VALUES(10,'text','12:00:00')"
                        )))
                        .await
                        .unwrap();
                    match action {
                        "commit" => tx.commit().await.unwrap(),
                        "rollback" => tx.rollback().await.unwrap(),
                        _ => drop(tx),
                    }
                    // Reacquire only after releasing this one-connection pool lease.
                    drop(c);
                    let actual = rows(&db, &table).await;
                    if action == "commit" {
                        let mut expected: Vec<Stored> = values
                            .iter()
                            .enumerate()
                            .map(|(id, t)| (id as i64, Some(payload.clone()), Some(*t)))
                            .collect();
                        expected.extend([
                            (6, Some(vec![]), None),
                            (7, None, Some(MySqlTime::ZERO)),
                            (
                                8,
                                Some(b"clock".to_vec()),
                                Some(
                                    MySqlTime::new(MySqlTimeSign::Positive, 12, 34, 56, 123456)
                                        .unwrap(),
                                ),
                            ),
                            (
                                9,
                                Some(b"duration".to_vec()),
                                Some(
                                    MySqlTime::new(MySqlTimeSign::Positive, 25, 2, 3, 987654)
                                        .unwrap(),
                                ),
                            ),
                            (
                                10,
                                Some(b"text".to_vec()),
                                Some(MySqlTime::new(MySqlTimeSign::Positive, 12, 0, 0, 0).unwrap()),
                            ),
                        ]);
                        assert_eq!(actual, expected, "{backend:?}/{mode}/{cached}");
                    } else {
                        assert!(
                            actual.is_empty(),
                            "{backend:?}/{mode}/{cached}/{action}: {actual:?}"
                        );
                    }
                    clear(&db, &table).await;
                    c = db.pool().acquire().await.unwrap();
                    c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                        .await
                        .unwrap();
                }
            }
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn arbitrary_binary_insert_select_update_delete_keep_all_octets_and_time() {
    let mut payload: Vec<u8> = (0..=255).collect();
    payload.extend_from_slice(b"'; DROP TABLE t; -- \\\0");
    let replacement = vec![255, 0, 128, 39, 92, 26, 254];
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = create(&db).await;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            let mut c = db.pool().acquire().await.unwrap();
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for action in ["commit", "rollback", "drop"] {
                let mut tx = c.begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} SELECT ?,?,? UNION ALL SELECT ?,?,?"
                )))
                .bind(1_i64)
                .bind(&payload)
                .bind(signed_time())
                .bind(2_i64)
                .bind(None::<Vec<u8>>)
                .bind(None::<MySqlTime>)
                .execute(&mut *tx)
                .await
                .unwrap();
                match action {
                    "commit" => tx.commit().await.unwrap(),
                    "rollback" => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
                drop(c);
                let actual = rows(&db, &table).await;
                if action == "commit" {
                    assert_eq!(
                        actual,
                        [
                            (1, Some(payload.clone()), Some(signed_time())),
                            (2, None, None)
                        ]
                    );
                } else {
                    assert!(actual.is_empty());
                }
                clear(&db, &table).await;
                c = db.pool().acquire().await.unwrap();
                c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                    .await
                    .unwrap();
            }
            // Execute-engine statements allow binary expressions and typed TIME.
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} SELECT 1,UNHEX('00'),CAST('01:00:00' AS TIME(6))"
            )))
            .execute(&mut *c)
            .await
            .unwrap();
            for commit in [false, true] {
                let mut tx = c.begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {table} SET b=?,t=? WHERE id=1"
                )))
                .bind(&replacement)
                .bind(MySqlTime::MIN)
                .execute(&mut *tx)
                .await
                .unwrap();
                if commit {
                    tx.commit().await.unwrap();
                } else {
                    tx.rollback().await.unwrap();
                }
                drop(c);
                assert_eq!(
                    rows(&db, &table).await,
                    [(
                        1,
                        Some(if commit { replacement.clone() } else { vec![0] }),
                        Some(if commit {
                            MySqlTime::MIN
                        } else {
                            MySqlTime::new(MySqlTimeSign::Positive, 1, 0, 0, 0).unwrap()
                        })
                    )]
                );
                c = db.pool().acquire().await.unwrap();
                c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                    .await
                    .unwrap();
            }
            for commit in [false, true] {
                let mut tx = c.begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE HEX(b)=HEX(?) AND CAST(t AS TIME(6))=?"
                )))
                .bind(&replacement)
                .bind(MySqlTime::MIN)
                .execute(&mut *tx)
                .await
                .unwrap();
                if commit {
                    tx.commit().await.unwrap();
                } else {
                    tx.rollback().await.unwrap();
                }
                drop(c);
                assert_eq!(
                    rows(&db, &table).await.is_empty(),
                    commit,
                    "{backend:?}/{mode}/delete/commit={commit}"
                );
                c = db.pool().acquire().await.unwrap();
                c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                    .await
                    .unwrap();
            }
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn arbitrary_binary_values_mix_with_text_without_corruption_or_replay() {
    for backend in [Backend::MySql, Backend::Doris] {
        let opt = options(backend);
        let p = proxy::Proxy::start(&opt, proxy::Mode::Trace).await;
        let db = PoolBuilder::new(backend)
            .max_connections(1)
            .connect_with(p.options(opt))
            .await
            .unwrap();
        let table = create(&db).await;
        let mut tx = db.pool().begin().await.unwrap();
        let sql = format!("INSERT INTO {table} VALUES(?,?,?)");
        sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
            .bind(1_i64)
            .bind(b"prior".as_slice())
            .bind(signed_time())
            .execute(&mut *tx)
            .await
            .unwrap();
        let before = p.trace().len();
        let result = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
            .bind(2_i64)
            .bind(vec![0_u8, 255, 128])
            .bind(MySqlTime::MAX)
            .execute(&mut *tx)
            .await;
        result.unwrap();
        if backend == Backend::Doris {
            let trace = p.trace();
            assert_eq!(
                trace.len(),
                before + 1,
                "no failed-write retry or engine switch"
            );
            let sent = String::from_utf8_lossy(&trace[before]);
            assert!(sent.contains("UNHEX(") && sent.contains("00FF80"));
        }
        sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(3_i64)
            .bind(b"later".as_slice())
            .bind(MySqlTime::ZERO)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let mut expected = vec![(1, Some(b"prior".to_vec()), Some(signed_time()))];
        expected.push((2, Some(vec![0, 255, 128]), Some(MySqlTime::MAX)));
        expected.push((3, Some(b"later".to_vec()), Some(MySqlTime::ZERO)));
        assert_eq!(rows(&db, &table).await, expected);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn blob_time_cancelled_write_and_uncertain_commit_are_not_replayed() {
    for backend in [Backend::MySql, Backend::Doris] {
        let admin = client(backend).await;
        for arbitrary in [false, true] {
            for commit_fault in [false, true] {
                let opt = options(backend);
                let mode = if commit_fault {
                    proxy::Mode::PauseCommit
                } else if backend == Backend::MySql {
                    proxy::Mode::PauseExecute
                } else {
                    proxy::Mode::PauseReply
                };
                let p = proxy::Proxy::start(&opt, mode).await;
                let db = PoolBuilder::new(backend)
                    .max_connections(1)
                    .acquire_timeout(Duration::from_secs(4))
                    .connect_with(p.options(opt))
                    .await
                    .unwrap();
                let table = create(&admin).await;
                // PauseCommit must arm only on COMMIT, not the earlier write.
                let marker = if commit_fault {
                    "SQLX_DORIS_BINDING_WRITE"
                } else {
                    proxy::MARKER
                };
                let sql = format!("INSERT INTO {table} VALUES(?,?,?) /* {marker} */");
                let bytes = if arbitrary {
                    vec![0, 255, 128, 39, 92]
                } else {
                    b"safe'\\\0".to_vec()
                };
                let mut tx = db.pool().begin().await.unwrap();
                if commit_fault {
                    sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                        .bind(1_i64)
                        .bind(&bytes)
                        .bind(signed_time())
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    let future = tx.commit();
                    tokio::pin!(future);
                    tokio::select! {
                        _ = &mut future => panic!("commit completed before fault"),
                        _ = p.reached() => {}
                    }
                } else {
                    {
                        let future = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                            .bind(1_i64)
                            .bind(&bytes)
                            .bind(signed_time())
                            .execute(&mut *tx);
                        tokio::pin!(future);
                        tokio::select! {
                            _ = &mut future => panic!("write completed before fault"),
                            _ = p.reached() => {}
                        }
                    }
                    drop(tx);
                }
                assert_eq!(p.fault_reply(), 0, "real server accepted the operation");
                p.release();
                let healthy: i32 = sqlx::query_scalar("SELECT 42")
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
                assert_eq!(healthy, 42);
                let actual = rows(&admin, &table).await;
                assert_eq!(
                    actual,
                    if commit_fault {
                        vec![(1, Some(bytes), Some(signed_time()))]
                    } else {
                        vec![]
                    }
                );
                let trace = p.trace();
                let writes = trace
                    .iter()
                    .filter(|v| {
                        if backend == Backend::MySql {
                            v[0] == 23
                        } else {
                            v[0] == 3 && String::from_utf8_lossy(&v[1..]).starts_with("INSERT")
                        }
                    })
                    .count();
                // MySQL also EXECUTEs the subsequent SELECT 42 health query.
                assert_eq!(
                    writes,
                    1 + usize::from(backend == Backend::MySql),
                    "{backend:?}/{arbitrary}/{commit_fault}: business write must never be replayed"
                );
                if backend == Backend::MySql {
                    assert_eq!(
                        trace
                            .iter()
                            .filter(|v| v[0] == 22
                                && String::from_utf8_lossy(&v[1..]).starts_with("INSERT"))
                            .count(),
                        1
                    );
                }
                assert_eq!(
                    trace
                        .iter()
                        .filter(|v| v[0] == 3 && &v[1..] == b"COMMIT")
                        .count(),
                    usize::from(commit_fault)
                );
                common::drop_table(&admin, &table).await;
                db.close().await;
            }
        }
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn binary_values_querybuilder_length_boundaries_text_both_orders_and_fold_modes() {
    let mut payloads: Vec<Option<Vec<u8>>> = [0, 250, 251, 65535, 65536]
        .into_iter()
        .map(|len| Some((0..len).map(|i| (i % 256) as u8).collect()))
        .collect();
    payloads.push(Some(b"'quoted'\\\0".to_vec()));
    payloads.push(None);
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = create(&db).await;
        for fold in [false, true] {
            let mut c = db.pool().acquire().await.unwrap();
            if backend == Backend::Doris {
                c.execute(sqlx::AssertSqlSafe(format!(
                    "SET enable_fold_constant_by_be={fold}"
                )))
                .await
                .unwrap();
            }
            for bound_first in [false, true] {
                for action in ["commit", "rollback", "drop"] {
                    let mut tx = c.begin().await.unwrap();
                    let prefix = format!("INSERT INTO {table} VALUES(0,'prefix','12:00:00')");
                    if !bound_first {
                        (&mut *tx)
                            .execute(sqlx::AssertSqlSafe(prefix.clone()))
                            .await
                            .unwrap();
                    }
                    let mut query = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                        "INSERT INTO {table}(id,b,t) "
                    ));
                    query.push_values(payloads.iter().enumerate(), |mut row, (id, b)| {
                        row.push_bind(id as i64 + 1)
                            .push_bind(b.as_deref())
                            .push_bind(signed_time());
                    });
                    query.build().execute(&mut *tx).await.unwrap();
                    if bound_first {
                        (&mut *tx)
                            .execute(sqlx::AssertSqlSafe(prefix))
                            .await
                            .unwrap();
                    }
                    (&mut *tx)
                        .execute(sqlx::AssertSqlSafe(format!(
                            "INSERT INTO {table} VALUES(99,'suffix','12:00:00')"
                        )))
                        .await
                        .unwrap();
                    match action {
                        "commit" => tx.commit().await.unwrap(),
                        "rollback" => tx.rollback().await.unwrap(),
                        _ => drop(tx),
                    }
                    drop(c);
                    let actual = rows(&db, &table).await;
                    if action == "commit" {
                        let clock = MySqlTime::new(MySqlTimeSign::Positive, 12, 0, 0, 0).unwrap();
                        let mut expected = vec![(0, Some(b"prefix".to_vec()), Some(clock))];
                        expected.extend(
                            payloads
                                .iter()
                                .enumerate()
                                .map(|(id, b)| (id as i64 + 1, b.clone(), Some(signed_time()))),
                        );
                        expected.push((99, Some(b"suffix".to_vec()), Some(clock)));
                        assert_eq!(actual, expected, "{backend:?}/{fold}/{bound_first}");
                    } else {
                        assert!(
                            actual.is_empty(),
                            "{backend:?}/{fold}/{bound_first}/{action}"
                        );
                    }
                    clear(&db, &table).await;
                    c = db.pool().acquire().await.unwrap();
                    if backend == Backend::Doris {
                        c.execute(sqlx::AssertSqlSafe(format!(
                            "SET enable_fold_constant_by_be={fold}"
                        )))
                        .await
                        .unwrap();
                    }
                }
            }
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn binary_multirow_values_preserve_precision_atomicity_and_native_duplicate_key_semantics() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = client(backend).await;
        let table = common::name();
        let schema = if backend == Backend::Doris {
            "(id BIGINT NOT NULL,b STRING,n DECIMAL(38,0) NOT NULL) UNIQUE KEY(id) \
                DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
        } else {
            "(id BIGINT PRIMARY KEY,b LONGBLOB,n DECIMAL(38,0) NOT NULL)"
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}{schema}")))
            .await
            .unwrap();
        let mut tx = db.pool().begin().await.unwrap();
        (&mut *tx)
            .execute(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(0,'prior',0)"
            )))
            .await
            .unwrap();
        let multi = format!("INSERT INTO {table} VALUES(?,?,?),(?,?,?)");
        sqlx::query(sqlx::AssertSqlSafe(multi.clone()))
            .bind(1_i64)
            .bind(vec![0_u8, 255, 128])
            .bind(9007199254740993_i64)
            .bind(2_i64)
            .bind(vec![255_u8, 0, 128])
            .bind(1.25_f64)
            .execute(&mut *tx)
            .await
            .unwrap();
        let error = sqlx::query(sqlx::AssertSqlSafe(multi))
            .bind(3_i64)
            .bind(vec![0_u8, 255])
            .bind(3_i64)
            .bind(4_i64)
            .bind(vec![255_u8, 0])
            .bind(None::<i64>)
            .execute(&mut *tx)
            .await
            .unwrap_err();
        assert!(
            matches!(error, sqlx::Error::Database(_)),
            "{backend:?}: {error}"
        );
        for (bytes, n) in [(vec![0_u8, 255], 5_i64), (vec![255_u8, 0], 6)] {
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(5,?,?)"
            )))
            .bind(bytes)
            .bind(n)
            .execute(&mut *tx)
            .await;
            if backend == Backend::MySql && n == 6 {
                assert!(result.unwrap_err().to_string().contains("Duplicate entry"));
            } else {
                result.unwrap();
            }
        }
        tx.commit().await.unwrap();
        let actual: Vec<(i64, Vec<u8>, bigdecimal::BigDecimal)> = sqlx::query_as(
            sqlx::AssertSqlSafe(format!("SELECT id,b,n FROM {table} ORDER BY id")),
        )
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(
            actual,
            [
                (0, b"prior".to_vec(), 0.into()),
                (1, vec![0, 255, 128], 9007199254740993_i64.into()),
                (2, vec![255, 0, 128], 1.into()),
                (
                    5,
                    if backend == Backend::Doris {
                        vec![255, 0]
                    } else {
                        vec![0, 255]
                    },
                    if backend == Backend::Doris {
                        6.into()
                    } else {
                        5.into()
                    }
                )
            ],
            "{backend:?}"
        );
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn unsupported_values_shapes_fail_preflight_without_disturbing_binary_transaction() {
    let opt = options(Backend::Doris);
    let p = proxy::Proxy::start(&opt, proxy::Mode::Trace).await;
    let db = PoolBuilder::new(Backend::Doris)
        .max_connections(1)
        .connect_with(p.options(opt))
        .await
        .unwrap();
    let table = create(&db).await;
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES(1,?,?)"
    )))
    .bind(vec![0_u8, 255])
    .bind(signed_time())
    .execute(&mut *tx)
    .await
    .unwrap();
    for tail in [
        "VALUES()",
        "VALUES(DEFAULT,DEFAULT),(DEFAULT)",
        "VALUES(2,'x'),(3,'y','12:00:00')",
        "VALUES(2,'x','12:00:00');DELETE FROM t",
    ] {
        let before = p.trace().len();
        let error = (&mut *tx)
            .execute(sqlx::AssertSqlSafe(format!("INSERT INTO {table} {tail}")))
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("Doris client binding"),
            "{error}"
        );
        assert_eq!(p.trace().len(), before);
    }
    tx.commit().await.unwrap();
    assert_eq!(
        rows(&db, &table).await,
        [(1, Some(vec![0, 255]), Some(signed_time()))]
    );
    common::drop_table(&db, &table).await;
    db.close().await;
}
