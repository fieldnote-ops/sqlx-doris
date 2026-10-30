//! Same application INSERT templates across modern MySQL and official Doris.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{self, Acquire, Executor, SqlSafeStr},
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
async fn create(db: &Client, auto: bool) -> TableName {
    let t = common::name();
    let id = if auto {
        if db.backend() == Backend::Doris {
            "id BIGINT NOT NULL AUTO_INCREMENT(1)"
        } else {
            "id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY"
        }
    } else if db.backend() == Backend::Doris {
        "id BIGINT NOT NULL DEFAULT '42'"
    } else {
        "id BIGINT NOT NULL PRIMARY KEY DEFAULT '42'"
    };
    let binary = if db.backend() == Backend::Doris {
        "STRING NULL DEFAULT 'seed'"
    } else {
        "LONGBLOB NULL DEFAULT ('seed')"
    };
    let suffix = if db.backend() == Backend::Doris {
        " UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        ""
    };
    db.pool().execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t}({id},title VARCHAR(128) NULL DEFAULT 'fresh',priority BIGINT NULL DEFAULT '7',optional BIGINT NULL DEFAULT NULL,payload {binary},created DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)){suffix}"))).await.unwrap();
    t
}
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
struct BusinessRow {
    id: i64,
    title: Option<String>,
    priority: Option<i64>,
    optional: Option<i64>,
    payload: Option<Vec<u8>>,
    created: chrono::NaiveDateTime,
}
async fn rows(db: &Client, t: &TableName) -> Vec<BusinessRow> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,title,priority,optional,payload,created FROM {t} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}
fn defaults(row: &BusinessRow) {
    assert_eq!(row.title.as_deref(), Some("fresh"));
    assert_eq!(row.priority, Some(7));
    assert_eq!(row.optional, None);
    assert_eq!(row.payload.as_deref(), Some(b"seed".as_slice()));
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn all_default_transaction_and_value_alias_source_controls() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = create(&db, false).await;
        let mut tx = db.pool().begin().await.unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t}(id,title) VALUES(?,?)"
        )))
        .bind(1_i64)
        .bind("bound")
        .execute(&mut *tx)
        .await
        .unwrap();
        let result = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t} VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT)"
        )))
        .execute(&mut *tx)
        .await;
        result.unwrap();
        tx.commit().await.unwrap();
        let result = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {t}(id,title) VALUE(?,DEFAULT)"
        )))
        .bind(3_i64)
        .execute(db.pool())
        .await;
        result.unwrap();
        let stored = rows(&db, &t).await;
        assert_eq!(stored.len(), 3);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

fn expect(
    row: &BusinessRow,
    id: i64,
    title: Option<&str>,
    priority: Option<i64>,
    optional: Option<i64>,
    payload: Option<&[u8]>,
) {
    assert_eq!(row.id, id);
    assert_eq!(row.title.as_deref(), title);
    assert_eq!(row.priority, priority);
    assert_eq!(row.optional, optional);
    assert_eq!(row.payload.as_deref(), payload);
}
async fn now(c: &mut sqlx::MySqlConnection) -> chrono::NaiveDateTime {
    sqlx::query_scalar("SELECT NOW(6)")
        .fetch_one(c)
        .await
        .unwrap()
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn column_lists_default_null_and_bulk_share_one_business_dto_in_three_modes() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = create(&db, false).await;
        let mut c = db.pool().acquire().await.unwrap();
        let mut base = 100_i64;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for persistent in [true, true, false] {
                let before = now(&mut c).await;
                let bytes: Vec<u8> = (0..=255).collect();
                sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO {t}(priority,id,title,payload,optional,created) VALUES(DEFAULT,?,DEFAULT,DEFAULT,DEFAULT,DEFAULT),(NULL,?,NULL,?,NULL,DEFAULT),(?,?,'DEFAULT',?,DEFAULT,DEFAULT)")))
     .bind(base+1).bind(base+2).bind(None::<Vec<u8>>).bind(9_i64).bind(base+3).bind(&bytes).persistent(persistent).execute(&mut *c).await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t}(payload,id) VALUE(?,?)"
                )))
                .bind(Vec::<u8>::new())
                .bind(base + 4)
                .persistent(persistent)
                .execute(&mut *c)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} VALUES({},'literal',DEFAULT,DEFAULT,DEFAULT,DEFAULT)",
                    base + 5
                )))
                .persistent(persistent)
                .execute(&mut *c)
                .await
                .unwrap();
                let after = now(&mut c).await;
                let actual:Vec<BusinessRow>=sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT id,title,priority,optional,payload,created FROM {t} WHERE id>=? AND id<=? ORDER BY id"))).bind(base+1).bind(base+5).fetch_all(&mut *c).await.unwrap();
                assert_eq!(actual.len(), 5);
                defaults(&actual[0]);
                expect(
                    &actual[0],
                    base + 1,
                    Some("fresh"),
                    Some(7),
                    None,
                    Some(b"seed"),
                );
                expect(&actual[1], base + 2, None, None, None, None);
                expect(
                    &actual[2],
                    base + 3,
                    Some("DEFAULT"),
                    Some(9),
                    None,
                    Some(&bytes),
                );
                expect(
                    &actual[3],
                    base + 4,
                    Some("fresh"),
                    Some(7),
                    None,
                    Some(&[]),
                );
                expect(
                    &actual[4],
                    base + 5,
                    Some("literal"),
                    Some(7),
                    None,
                    Some(b"seed"),
                );
                assert!(
                    actual
                        .iter()
                        .all(|r| r.created >= before && r.created <= after),
                    "{b:?}/{mode}: default datetime outside server NOW interval"
                );
                if b == Backend::MySql {
                    assert_eq!(actual[0].created, actual[1].created);
                    assert_eq!(actual[1].created, actual[2].created);
                }
                base += 10;
            }
        }
        eprintln!(
            "{b:?}: 45 business rows, three sql modes and cached/uncached column/default/NULL templates verified"
        );
        drop(c);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn columns_defaults_batches_and_literal_writes_mix_in_commit_rollback_and_drop() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            for action in ["commit", "rollback", "drop"] {
                let t = create(&db, false).await;
                let mut c = db.pool().acquire().await.unwrap();
                c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                    .await
                    .unwrap();
                let mut tx = c.begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} VALUES(1,'literal',9,NULL,'seed','2026-10-29 01:02:03.123456')"
                )))
                .execute(&mut *tx)
                .await
                .unwrap();
                let message = "业务'\\\0😀?;--";
                let bytes = vec![255_u8, 0, 128, 39, 92, 63];
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t}(title,id,payload) VALUES(?,?,?),(DEFAULT,?,DEFAULT)"
                )))
                .bind(message)
                .bind(2_i64)
                .bind(&bytes)
                .bind(3_i64)
                .execute(&mut *tx)
                .await
                .unwrap();
                let alias = format!("INSERT INTO {t}(id,optional) VALUE(?,?)");
                let statement = (&mut *tx)
                    .prepare(sqlx::AssertSqlSafe(alias.clone()).into_sql_str())
                    .await
                    .unwrap();
                assert_eq!(
                    sqlx::Statement::parameters(&statement),
                    Some(sqlx::Either::Right(2))
                );
                sqlx::query(sqlx::AssertSqlSafe(alias))
                    .bind(4_i64)
                    .bind(None::<i64>)
                    .persistent(false)
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {t} VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT)"
                )))
                .execute(&mut *tx)
                .await
                .unwrap();
                match action {
                    "commit" => tx.commit().await.unwrap(),
                    "rollback" => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
                drop(c);
                let stored = rows(&db, &t).await;
                if action == "commit" {
                    assert_eq!(
                        stored.iter().map(|r| r.id).collect::<Vec<_>>(),
                        [1, 2, 3, 4, 42]
                    );
                    expect(&stored[0], 1, Some("literal"), Some(9), None, Some(b"seed"));
                    expect(&stored[1], 2, Some(message), Some(7), None, Some(&bytes));
                    for r in &stored[2..] {
                        defaults(r);
                    }
                } else {
                    assert!(stored.is_empty(), "{b:?}/{mode}/{action}");
                }
                common::drop_table(&db, &t).await;
            }
        }
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn value_alias_prepare_cache_uses_server_defaults_and_preserves_schema_rejection() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = create(&admin, false).await;
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        let sql = format!(
            "INSERT INTO {t}(id,title,priority) VALUE(?,?,DEFAULT) /* business_cache_key */"
        );
        let statement = c
            .prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
            .await
            .unwrap();
        assert_eq!(sqlx::Statement::sql(&statement).as_str(), sql);
        assert_eq!(
            sqlx::Statement::parameters(&statement),
            Some(sqlx::Either::Right(2))
        );
        for (id, title) in [(10_i64, Some("cached")), (11, None), (12, Some("DEFAULT"))] {
            sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                .bind(id)
                .bind(title)
                .execute(&mut *c)
                .await
                .unwrap();
        }
        let trace = p.trace();
        let prepares: Vec<_> = trace
            .iter()
            .filter(|v| v[0] == 22 && String::from_utf8_lossy(v).contains("business_cache_key"))
            .collect();
        assert_eq!(
            prepares.len(),
            1,
            "alias prepare and run must share a cache key"
        );
        let wire = String::from_utf8_lossy(prepares[0]);
        assert!(wire.contains(if b == Backend::Doris {
            " VALUES("
        } else {
            " VALUE("
        }));
        let change = c
            .execute(sqlx::AssertSqlSafe(format!(
                "ALTER TABLE {t} MODIFY COLUMN priority BIGINT NULL DEFAULT '19'"
            )))
            .await;
        if b == Backend::Doris {
            let e = change.unwrap_err();
            assert!(matches!(e, sqlx::Error::Database(_)));
            assert!(e.to_string().contains("Can not change default value"));
            eprintln!("Doris ALTER DEFAULT native rejection retained: {e}");
        } else {
            change.unwrap();
        }
        sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
            .bind(13_i64)
            .bind("new default")
            .execute(&mut *c)
            .await
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
            .bind(14_i64)
            .bind(None::<String>)
            .persistent(false)
            .execute(&mut *c)
            .await
            .unwrap();
        drop(c);
        let stored = rows(&admin, &t).await;
        assert_eq!(stored.len(), 5);
        assert_eq!(
            stored.iter().map(|r| r.priority).collect::<Vec<_>>(),
            if b == Backend::Doris {
                [Some(7), Some(7), Some(7), Some(7), Some(7)]
            } else {
                [Some(7), Some(7), Some(7), Some(19), Some(19)]
            }
        );
        assert!(stored[1].title.is_none() && stored[4].title.is_none());
        db.close().await;
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn all_default_bulk_and_omitted_columns_keep_server_auto_ids_and_defaults() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        for action in ["autocommit", "commit", "rollback", "drop"] {
            let t = create(&db, true).await;
            let sql = format!(
                "INSERT INTO {t} VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT),(DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT)"
            );
            if action == "autocommit" {
                sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else {
                let mut tx = db.pool().begin().await.unwrap();
                sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                let mut q = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                    "INSERT INTO {t}(title,priority) "
                ));
                q.push_values(0..3, |mut row, _| {
                    row.push("DEFAULT").push("DEFAULT");
                });
                q.build().execute(&mut *tx).await.unwrap();
                match action {
                    "commit" => tx.commit().await.unwrap(),
                    "rollback" => tx.rollback().await.unwrap(),
                    _ => drop(tx),
                }
            }
            let stored = rows(&db, &t).await;
            let expected = if action == "autocommit" {
                2
            } else if action == "commit" {
                5
            } else {
                0
            };
            assert_eq!(stored.len(), expected, "{b:?}/{action}");
            let ids: std::collections::BTreeSet<_> = stored.iter().map(|r| r.id).collect();
            assert_eq!(ids.len(), expected);
            for r in &stored {
                assert!(r.id > 0);
                defaults(r);
            }
            common::drop_table(&db, &t).await;
        }
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn strict_invalid_bulk_is_atomic_and_prior_transaction_write_can_commit() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        for mode in [
            "STRICT_ALL_TABLES",
            "STRICT_ALL_TABLES,NO_BACKSLASH_ESCAPES",
            "STRICT_ALL_TABLES,ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            let t = create(&db, false).await;
            let mut c = db.pool().acquire().await.unwrap();
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            let mut tx = c.begin().await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {t}(id) VALUES(?)"
            )))
            .bind(100_i64)
            .execute(&mut *tx)
            .await
            .unwrap();
            let error = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {t}(id,title) VALUES(?,?),(?,?)"
            )))
            .bind(Some(200_i64))
            .bind("valid row")
            .bind(None::<i64>)
            .bind("invalid row")
            .execute(&mut *tx)
            .await
            .unwrap_err();
            assert!(
                matches!(error, sqlx::Error::Database(_)),
                "original server error expected: {error}"
            );
            eprintln!("{b:?}/{mode} strict invalid bulk: {error}");
            tx.commit().await.unwrap();
            drop(c);
            let stored = rows(&db, &t).await;
            assert_eq!(stored.len(), 1);
            assert_eq!(stored[0].id, 100);
            defaults(&stored[0]);
            common::drop_table(&db, &t).await;
        }
        let required = common::name();
        let suffix = if b == Backend::Doris {
            " UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
        } else {
            ""
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {required}(id BIGINT NOT NULL,required_value BIGINT NOT NULL){suffix}"
            )))
            .await
            .unwrap();
        db.pool()
            .execute("SET sql_mode='STRICT_ALL_TABLES'")
            .await
            .unwrap();
        for body in ["(id) VALUES(?)", "(id,required_value) VALUES(?,DEFAULT)"] {
            let e = sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO {required}{body}")))
                .bind(1_i64)
                .execute(db.pool())
                .await
                .unwrap_err();
            assert!(
                matches!(e, sqlx::Error::Database(_)),
                "{b:?}: missing default must retain server error: {e}"
            );
            eprintln!("{b:?} no-default control: {e}");
        }
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM {required}"
        )))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(count, 0);
        common::drop_table(&db, &required).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn unsupported_bulk_shapes_fail_before_send_and_leave_prior_defaults_committable() {
    let (db, p) = proxied(Backend::Doris, proxy::Mode::Trace).await;
    let t = create(&db, false).await;
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {t}(id) VALUES(?)"
    )))
    .bind(1_i64)
    .execute(&mut *tx)
    .await
    .unwrap();
    for tail in [
        "VALUES()",
        "VALUES(1,DEFAULT),(2)",
        "VALUES(,DEFAULT)",
        "VALUES(DEFAULT,)",
        "VALUES(1),",
        "VALUES(1);DELETE FROM x",
        "VALUES(?)",
        "VALUE()",
    ] {
        let before = p.trace().len();
        let e = sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO {t} {tail}")))
            .execute(&mut *tx)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("Doris client binding"), "{tail}/{e}");
        assert_eq!(before, p.trace().len(), "{tail}");
    }
    tx.commit().await.unwrap();
    let stored = rows(&db, &t).await;
    assert_eq!(stored.len(), 1);
    defaults(&stored[0]);
    common::drop_table(&db, &t).await;
    db.close().await;
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn current_timestamp_default_batch_granularity_remains_a_server_semantic_control() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = create(&db, false).await;
        let mut c = db.pool().acquire().await.unwrap();
        for fold in [false, true] {
            if b == Backend::Doris {
                c.execute(sqlx::AssertSqlSafe(format!(
                    "SET enable_fold_constant_by_be={fold}"
                )))
                .await
                .unwrap();
            }
            let before = now(&mut c).await;
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {t}(id,created) VALUES(1,DEFAULT),(2,DEFAULT),(3,DEFAULT),(4,DEFAULT)"
            )))
            .execute(&mut *c)
            .await
            .unwrap();
            let after = now(&mut c).await;
            let stored: Vec<BusinessRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id,title,priority,optional,payload,created FROM {t} ORDER BY id"
            )))
            .fetch_all(&mut *c)
            .await
            .unwrap();
            assert!(
                stored
                    .iter()
                    .all(|r| r.created >= before && r.created <= after)
            );
            let distinct: std::collections::BTreeSet<_> =
                stored.iter().map(|r| r.created).collect();
            if b == Backend::MySql {
                assert_eq!(distinct.len(), 1);
            } else {
                assert!(distinct.len() > 1);
            }
            eprintln!(
                "{b:?}/BE-fold={fold}: CURRENT_TIMESTAMP(6) default distinct values per four-row batch = {} (server semantics, not cross-db equivalence)",
                distinct.len()
            );
            c.execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                .await
                .unwrap();
        }
        drop(c);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

fn fault_batch(
    sql: String,
    all_default: bool,
) -> sqlx::query::Query<'static, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    let q = sqlx::query(sqlx::AssertSqlSafe(sql));
    if all_default {
        q
    } else {
        q.bind(500_i64)
            .bind("cancel'\\\0😀")
            .bind(vec![255_u8, 0, 128, 39, 92])
            .bind(501_i64)
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn bulk_defaults_cancel_and_lost_replies_never_replay_whole_or_partial_batches() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = create(&admin, true).await;
        for all_default in [false, true] {
            for native_tx in [false, true] {
                for persistent in [true, false] {
                    for lost in [false, true] {
                        let mode = if b == Backend::Doris && native_tx {
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
                        let (db, p) = proxied(b, mode).await;
                        let body = if all_default {
                            "VALUES(DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT),(DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT,DEFAULT)"
                        } else {
                            "(id,title,priority,payload) VALUE(?,?,DEFAULT,?),(?,DEFAULT,NULL,DEFAULT)"
                        };
                        let sql = format!("INSERT INTO {t} {body} /* {} */", proxy::MARKER);
                        p.suspend_faults(true);
                        fault_batch(sql.clone(), all_default)
                            .persistent(persistent)
                            .execute(db.pool())
                            .await
                            .unwrap();
                        admin
                            .pool()
                            .execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                            .await
                            .unwrap();
                        let before = p.trace().len();
                        p.suspend_faults(false);
                        if native_tx {
                            let mut tx = db.pool().begin().await.unwrap();
                            {
                                let future = fault_batch(sql.clone(), all_default)
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
                            let future = fault_batch(sql.clone(), all_default)
                                .persistent(persistent)
                                .execute(db.pool());
                            tokio::pin!(future);
                            if lost {
                                assert!(future.await.is_err());
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
                        let stored = rows(&admin, &t).await;
                        assert_eq!(
                            stored.len(),
                            if native_tx { 0 } else { 2 },
                            "{b:?}/{all_default}/{native_tx}/{persistent}/{lost}"
                        );
                        if !native_tx {
                            assert_ne!(stored[0].id, stored[1].id);
                            if all_default {
                                for r in &stored {
                                    defaults(r);
                                }
                            } else {
                                assert_eq!(stored[0].id, 500);
                                assert_eq!(
                                    stored[0].payload.as_deref(),
                                    Some([255_u8, 0, 128, 39, 92].as_slice())
                                );
                                assert_eq!(stored[1].id, 501);
                                assert_eq!(stored[1].priority, None);
                            }
                        }
                        let trace = p.trace();
                        let trace = &trace[before..];
                        if b == Backend::Doris && native_tx {
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
                                "one uncertain batch and one health query"
                            );
                        }
                        db.close().await;
                        admin
                            .pool()
                            .execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                            .await
                            .unwrap();
                    }
                }
            }
        }
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn querybuilder_named_bulk_defaults_cross_null_bitmaps_without_splitting() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = create(&db, false).await;
        for fold in [false, true] {
            for native_tx in [false, true] {
                for count in [1, 8, 9, 17, 65, 257] {
                    let mut c = db.pool().acquire().await.unwrap();
                    c.execute(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                        .await
                        .unwrap();
                    c.execute("SET sql_mode='ANSI_QUOTES,NO_BACKSLASH_ESCAPES'")
                        .await
                        .unwrap();
                    if b == Backend::Doris {
                        c.execute(sqlx::AssertSqlSafe(format!(
                            "SET enable_fold_constant_by_be={fold}"
                        )))
                        .await
                        .unwrap();
                    }
                    let mut q = sqlx::QueryBuilder::<sqlx::MySql>::new(format!(
                        "INSERT INTO {t}(title,payload,id,priority) "
                    ));
                    q.push_values(0..count, |mut row, i| {
                        row.push_bind(if i % 2 == 0 {
                            Some("DEFAULT'\\\0😀")
                        } else {
                            None
                        });
                        if i % 3 == 0 {
                            row.push("DEFAULT");
                        } else {
                            row.push_bind((i % 3 == 1).then_some(vec![255_u8, 0, (i % 256) as u8]));
                        }
                        row.push_bind(1000 + i as i64);
                        if i % 2 == 0 {
                            row.push("DEFAULT");
                        } else {
                            row.push_bind(None::<i64>);
                        }
                    });
                    if native_tx {
                        let mut tx = c.begin().await.unwrap();
                        q.build()
                            .persistent(count % 2 == 0)
                            .execute(&mut *tx)
                            .await
                            .unwrap();
                        tx.commit().await.unwrap();
                    } else {
                        q.build()
                            .persistent(count % 2 == 0)
                            .execute(&mut *c)
                            .await
                            .unwrap();
                    }
                    drop(c);
                    let stored = rows(&db, &t).await;
                    assert_eq!(stored.len(), count, "{b:?}/{fold}/{native_tx}/{count}");
                    for (i, r) in stored.iter().enumerate() {
                        let payload = if i % 3 == 0 {
                            Some(b"seed".to_vec())
                        } else if i % 3 == 1 {
                            Some(vec![255_u8, 0, (i % 256) as u8])
                        } else {
                            None
                        };
                        expect(
                            r,
                            1000 + i as i64,
                            if i % 2 == 0 {
                                Some("DEFAULT'\\\0😀")
                            } else {
                                None
                            },
                            if i % 2 == 0 { Some(7) } else { None },
                            None,
                            payload.as_deref(),
                        );
                    }
                }
            }
        }
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
