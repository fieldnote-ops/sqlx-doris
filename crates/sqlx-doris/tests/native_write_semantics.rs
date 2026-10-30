//! Native conflict/table-model semantics and receipts, through the same SQLx API.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;

use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{self, Acquire, Executor},
};
use std::{str::FromStr, time::Duration};

fn options(b: Backend) -> sqlx::mysql::MySqlConnectOptions {
    sqlx::mysql::MySqlConnectOptions::from_str(
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
async fn proxied(b: Backend, mode: proxy::Mode) -> (Client, proxy::Proxy) {
    let o = options(b);
    let p = proxy::Proxy::start(&o, mode).await;
    let db = PoolBuilder::new(b)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(4))
        .connect_with(p.options(o))
        .await
        .unwrap();
    (db, p)
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum Model {
    Primary,
    Unique,
    Duplicate,
    Aggregate,
    Sequence,
}
async fn create(db: &Client, model: Model) -> TableName {
    let t = common::name();
    let schema = if db.backend() == Backend::MySql {
        let key = if model == Model::Primary {
            " PRIMARY KEY"
        } else {
            ""
        };
        format!("(id BIGINT NOT NULL{key},revision BIGINT NOT NULL,n BIGINT NULL)")
    } else {
        let (cols, keys, props) = match model {
            Model::Aggregate => (
                "id BIGINT NOT NULL,revision BIGINT MAX NOT NULL,n BIGINT SUM NULL",
                "AGGREGATE",
                "",
            ),
            Model::Duplicate => (
                "id BIGINT NOT NULL,revision BIGINT NOT NULL,n BIGINT NULL",
                "DUPLICATE",
                "",
            ),
            Model::Sequence => (
                "id BIGINT NOT NULL,revision BIGINT NOT NULL,n BIGINT NULL",
                "UNIQUE",
                ",'function_column.sequence_col'='revision','enable_unique_key_merge_on_write'='true'",
            ),
            _ => (
                "id BIGINT NOT NULL,revision BIGINT NOT NULL,n BIGINT NULL",
                "UNIQUE",
                ",'enable_unique_key_merge_on_write'='true'",
            ),
        };
        format!(
            "({cols}) {keys} KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1'{props})"
        )
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t}{schema}")))
        .await
        .unwrap();
    t
}
fn record(name: &str, rows: &[serde_json::Value]) {
    let p = std::path::PathBuf::from(std::env::var_os("SQLX_DORIS_ARTIFACTS_DIR").unwrap())
        .join("native-write-semantics/observations");
    std::fs::create_dir_all(&p).unwrap();
    std::fs::write(
        p.join(format!("{name}-{}.json", std::process::id())),
        serde_json::to_vec_pretty(rows).unwrap(),
    )
    .unwrap();
}
fn receipt(label: &str, r: &sqlx::mysql::MySqlQueryResult) -> serde_json::Value {
    serde_json::json!({"label":label,"rows_affected":r.rows_affected(),"last_insert_id":r.last_insert_id()})
}
async fn stored(db: &Client, t: &TableName) -> Vec<(i64, i64, Option<i64>)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,revision,n FROM {t} ORDER BY id,revision,n"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}
fn batch(sql: String) -> sqlx::query::Query<'static, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(1_i64)
        .bind(10_i64)
        .bind(Some(10_i64))
        .bind(1_i64)
        .bind(20_i64)
        .bind(Some(20_i64))
        .bind(2_i64)
        .bind(30_i64)
        .bind(None::<i64>)
}

#[tokio::test]
#[ignore = "requires dedicated official MySQL and Doris"]
async fn original_batch_duplicate_keys_preserve_native_errors_and_accepted_row_receipts() {
    let mut observations = Vec::new();
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = create(&db, Model::Primary).await;
        let mut c = db.pool().acquire().await.unwrap();
        for mode in [
            "STRICT_ALL_TABLES",
            "STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES",
            "STRICT_ALL_TABLES,ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for persistent in [true, true, false] {
                c.execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                    .await
                    .unwrap();
                let sql = format!("INSERT INTO {t}(id,revision,n) VALUES(?,?,?),(?,?,?),(?,?,?)");
                let result = batch(sql).persistent(persistent).execute(&mut *c).await;
                if b == Backend::MySql {
                    let error = result.unwrap_err();
                    let native = error.as_database_error().unwrap();
                    assert_eq!(native.code().as_deref(), Some("23000"));
                    assert_eq!(
                        native
                            .downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                            .number(),
                        1062
                    );
                    observations.push(serde_json::json!({"backend":"mysql","mode":mode,"persistent":persistent,"duplicate_error":1062,"message":native.message()}));
                } else {
                    let r = result.unwrap();
                    assert_eq!(r.rows_affected(), 3);
                    assert_eq!(r.last_insert_id(), 0);
                    observations.push(receipt("doris_native_unique_batch", &r));
                }
                let rows: Vec<(i64, i64, Option<i64>)> = sqlx::query_as(sqlx::AssertSqlSafe(
                    format!("SELECT id,revision,n FROM {t} ORDER BY id"),
                ))
                .fetch_all(&mut *c)
                .await
                .unwrap();
                if b == Backend::MySql {
                    assert!(rows.is_empty(), "failed InnoDB statement must be atomic");
                } else {
                    assert_eq!(rows.len(), 2);
                    assert!(
                        rows[0] == (1, 10, Some(10)) || rows[0] == (1, 20, Some(20)),
                        "no global order promise without Sequence"
                    );
                    assert_eq!(rows[1], (2, 30, None));
                }
                observations.push(serde_json::json!({"backend":format!("{b:?}"),"stored":rows}));
            }
        }
        drop(c);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
    record("same-sql-native-conflict", &observations);
}

#[tokio::test]
#[ignore = "requires dedicated official Doris"]
async fn doris_same_batch_models_and_sequence_keep_native_merge_results_and_input_receipts() {
    let db = client(Backend::Doris).await;
    let mut observations = Vec::new();
    for model in [
        Model::Unique,
        Model::Duplicate,
        Model::Aggregate,
        Model::Sequence,
    ] {
        let t = create(&db, model).await;
        let sql = format!("INSERT INTO {t}(id,revision,n) VALUES(?,?,?),(?,?,?),(?,?,?)");
        for (round, persistent) in [true, true, false].into_iter().enumerate() {
            let r = batch(sql.clone())
                .persistent(persistent)
                .execute(db.pool())
                .await
                .unwrap();
            assert_eq!(r.rows_affected(), 3);
            assert_eq!(r.last_insert_id(), 0);
            let rows = stored(&db, &t).await;
            match model {
                Model::Duplicate => {
                    let mut expected = Vec::new();
                    for _ in 0..=round {
                        expected.extend([(1, 10, Some(10)), (1, 20, Some(20)), (2, 30, None)]);
                    }
                    expected.sort();
                    assert_eq!(rows, expected);
                }
                Model::Aggregate => assert_eq!(
                    rows,
                    [(1, 20, Some(30 * (round as i64 + 1))), (2, 30, None)]
                ),
                Model::Sequence => assert_eq!(rows, [(1, 20, Some(20)), (2, 30, None)]),
                _ => {
                    assert_eq!(rows.len(), 2);
                    assert!([Some(10), Some(20)].contains(&rows[0].2));
                    assert_eq!(rows[1], (2, 30, None));
                }
            }
            observations.push(serde_json::json!({"model":format!("{model:?}"),"round":round,"receipt":receipt("repeat-batch",&r),"stored":rows}));
        }
        if model == Model::Sequence {
            let r = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {t} VALUES(?,?,?),(?,?,?)"
            )))
            .bind(1_i64)
            .bind(5_i64)
            .bind(999_i64)
            .bind(2_i64)
            .bind(10_i64)
            .bind(888_i64)
            .execute(db.pool())
            .await
            .unwrap();
            assert_eq!(
                r.rows_affected(),
                2,
                "accepted rows are not rows changed by Sequence"
            );
            assert_eq!(stored(&db, &t).await, [(1, 20, Some(20)), (2, 30, None)]);
            observations.push(receipt("stale-sequence-accepted-but-not-visible", &r));
        }
        if matches!(model, Model::Duplicate | Model::Aggregate) {
            let before = stored(&db, &t).await;
            let error = sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                .execute(db.pool())
                .await
                .unwrap_err();
            assert!(
                error
                    .as_database_error()
                    .is_some_and(|e| e.message().contains("must contain filter")),
                "{error:?}"
            );
            assert_eq!(stored(&db, &t).await, before);
            observations.push(serde_json::json!({"model":format!("{model:?}"),"delete_without_filter":error.to_string(),"stored_unchanged":true}));
        }
        common::drop_table(&db, &t).await;
    }
    record("native-models-and-sequence", &observations);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated official MySQL and Doris"]
async fn conflict_syntax_keeps_native_meaning_and_prior_transaction_success_can_commit() {
    let mut observations = Vec::new();
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = create(&db, Model::Primary).await;
        for inside in [false, true] {
            for persistent in [true, true, false] {
                for form in ["ignore", "replace", "on_duplicate"] {
                    db.pool()
                        .execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                        .await
                        .unwrap();
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "INSERT INTO {t} VALUES(1,10,10)"
                    )))
                    .execute(db.pool())
                    .await
                    .unwrap();
                    let sql = match form {
                        "ignore" => format!("INSERT IGNORE INTO {t}(id,revision,n) VALUES(?,?,?)"),
                        "replace" => format!("REPLACE INTO {t}(id,revision,n) VALUES(?,?,?)"),
                        _ => format!(
                            "INSERT INTO {t}(id,revision,n) VALUES(?,?,?) ON DUPLICATE KEY UPDATE revision=VALUES(revision),n=VALUES(n)"
                        ),
                    };
                    let mut c = db.pool().acquire().await.unwrap();
                    let result = if inside {
                        let mut tx = c.begin().await.unwrap();
                        sqlx::query(sqlx::AssertSqlSafe(format!(
                            "INSERT INTO {t} VALUES(2,30,NULL)"
                        )))
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                        let r = sqlx::query(sqlx::AssertSqlSafe(sql))
                            .bind(1_i64)
                            .bind(20_i64)
                            .bind(20_i64)
                            .persistent(persistent)
                            .execute(&mut *tx)
                            .await;
                        tx.commit().await.unwrap();
                        r
                    } else {
                        sqlx::query(sqlx::AssertSqlSafe(sql))
                            .bind(1_i64)
                            .bind(20_i64)
                            .bind(20_i64)
                            .persistent(persistent)
                            .execute(&mut *c)
                            .await
                    };
                    if b == Backend::MySql {
                        let r = result.unwrap();
                        assert_eq!(r.rows_affected(), if form == "ignore" { 0 } else { 2 });
                        observations.push(receipt(form, &r));
                    } else {
                        let error = result.unwrap_err();
                        if inside && form == "on_duplicate" {
                            assert!(matches!(error, sqlx::Error::Protocol(_)), "{error:?}");
                        } else {
                            assert_eq!(
                                error
                                    .as_database_error()
                                    .unwrap()
                                    .downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
                                    .number(),
                                1105
                            );
                        }
                        observations.push(serde_json::json!({"backend":"doris","form":form,"inside":inside,"error":error.to_string()}));
                    }
                    drop(c);
                    let mut expected = vec![(
                        1,
                        if b == Backend::MySql && form != "ignore" {
                            20
                        } else {
                            10
                        },
                        Some(if b == Backend::MySql && form != "ignore" {
                            20
                        } else {
                            10
                        }),
                    )];
                    if inside {
                        expected.push((2, 30, None));
                    }
                    assert_eq!(
                        stored(&db, &t).await,
                        expected,
                        "{b:?}/{inside}/{persistent}/{form}"
                    );
                }
            }
        }
        common::drop_table(&db, &t).await;
        db.close().await;
    }
    record("native-conflict-syntax", &observations);
}

#[tokio::test]
#[ignore = "requires dedicated official MySQL and Doris"]
async fn positive_statement_receipts_do_not_claim_commit_after_rollback_or_drop() {
    let mut observations = Vec::new();
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        for action in ["commit", "rollback", "drop"] {
            for persistent in [true, false] {
                let t = create(&db, Model::Primary).await;
                let mut tx = db.pool().begin().await.unwrap();
                let r = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} VALUES(?,?,?),(?,?,?),(?,?,?)"
                )))
                .bind(1_i64)
                .bind(10_i64)
                .bind(10_i64)
                .bind(2_i64)
                .bind(20_i64)
                .bind(None::<i64>)
                .bind(3_i64)
                .bind(30_i64)
                .bind(30_i64)
                .persistent(persistent)
                .execute(&mut *tx)
                .await
                .unwrap();
                assert_eq!(r.rows_affected(), 3);
                observations.push(receipt("uncommitted-three-accepted", &r));
                let r = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {t} SET n=? WHERE id=?"
                )))
                .bind(11_i64)
                .bind(1_i64)
                .execute(&mut *tx)
                .await
                .unwrap();
                // MySQL reads its uncommitted insert; Doris deliberately does
                // not. Preserve each server's UPDATE match count and data.
                assert_eq!(r.rows_affected(), if b == Backend::MySql { 1 } else { 0 });
                observations.push(receipt("update-pending-insert", &r));
                match action {
                    "commit" => tx.commit().await.unwrap(),
                    "rollback" => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
                let rows = stored(&db, &t).await;
                if action == "commit" {
                    assert_eq!(
                        rows,
                        [
                            (1, 10, Some(if b == Backend::MySql { 11 } else { 10 })),
                            (2, 20, None),
                            (3, 30, Some(30))
                        ]
                    );
                } else {
                    assert!(rows.is_empty());
                }
                observations.push(serde_json::json!({"backend":format!("{b:?}"),"action":action,"persistent":persistent,"stored":rows}));
                common::drop_table(&db, &t).await;
            }
        }
        db.close().await;
    }
    record("statement-not-commit-receipt", &observations);
}

#[tokio::test]
#[ignore = "requires dedicated official Doris"]
async fn duplicate_and_aggregate_batches_cancel_or_lose_commit_reply_without_replay() {
    let admin = client(Backend::Doris).await;
    for model in [Model::Duplicate, Model::Aggregate] {
        let t = create(&admin, model).await;
        for persistent in [true, false] {
            for committed in [false, true] {
                let (db, p) = proxied(
                    Backend::Doris,
                    if committed {
                        proxy::Mode::LostCommitReply
                    } else {
                        proxy::Mode::PauseReply
                    },
                )
                .await;
                let mut tx = db.pool().begin().await.unwrap();
                let sql = format!(
                    "INSERT INTO {t} VALUES(?,?,?),(?,?,?),(?,?,?) /* {} */",
                    proxy::MARKER
                );
                if committed {
                    // The generic marker fault must not consume the INSERT
                    // reply; this case targets COMMIT's accepted OK only.
                    p.suspend_faults(true);
                    batch(sql)
                        .persistent(persistent)
                        .execute(&mut *tx)
                        .await
                        .unwrap();
                    p.suspend_faults(false);
                    assert!(tx.commit().await.is_err());
                } else {
                    {
                        let future = batch(sql).persistent(persistent).execute(&mut *tx);
                        tokio::pin!(future);
                        tokio::select! {_=&mut future=>panic!("batch result did not pause"),_=p.reached()=>{}}
                    }
                    drop(tx);
                    p.release();
                }
                assert_eq!(p.hits(), 1);
                assert_eq!(p.fault_reply(), 0);
                let health: i64 = tokio::time::timeout(
                    Duration::from_secs(5),
                    sqlx::query_scalar("SELECT 42").fetch_one(db.pool()),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(health, 42);
                let rows = stored(&admin, &t).await;
                if !committed {
                    assert!(rows.is_empty());
                } else if model == Model::Duplicate {
                    assert_eq!(rows, [(1, 10, Some(10)), (1, 20, Some(20)), (2, 30, None)]);
                } else {
                    assert_eq!(rows, [(1, 20, Some(30)), (2, 30, None)]);
                }
                let trace = p.trace();
                assert_eq!(
                    trace
                        .iter()
                        .filter(|v| v[0] == 3 && String::from_utf8_lossy(v).contains(proxy::MARKER))
                        .count(),
                    1
                );
                assert_eq!(
                    trace
                        .iter()
                        .filter(|v| v[0] == 3 && &v[1..] == b"COMMIT")
                        .count(),
                    usize::from(committed)
                );
                db.close().await;
                admin
                    .pool()
                    .execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t} WHERE id>=0")))
                    .await
                    .unwrap();
            }
        }
        common::drop_table(&admin, &t).await;
    }
    admin.close().await;
}
