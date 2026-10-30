//! Same application filters, pagination and reports on the public protocols.
mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;

use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{self, Executor},
};
use std::{str::FromStr, time::Duration};

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

fn options(b: Backend) -> sqlx::mysql::MySqlConnectOptions {
    sqlx::mysql::MySqlConnectOptions::from_str(
        &std::env::var(if b == Backend::Doris {
            "DORIS_TEST_URL"
        } else {
            "MYSQL_TEST_URL"
        })
        .expect("dedicated database URL required"),
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

#[derive(Clone, Debug, PartialEq, sqlx::FromRow)]
struct Event {
    id: i64,
    tenant: i64,
    priority: Option<i64>,
    title: Option<String>,
    payload: Option<Vec<u8>>,
}
fn events() -> Vec<Event> {
    (0..48)
        .map(|id| Event {
            id,
            tenant: id % 3,
            priority: (id % 5 != 0).then_some(id % 4),
            title: (id % 7 != 0).then(|| format!("event ? '\\中\0 {}", id % 6)),
            payload: match id % 6 {
                0 => None,
                1 => Some(Vec::new()),
                2 => Some(vec![255, 0, 128, 39, 92]),
                3 => Some((0..=255).collect()),
                4 => Some(b"same ".to_vec()),
                _ => Some(b"same".to_vec()),
            },
        })
        .collect()
}
async fn seed(db: &Client) -> TableName {
    let t = common::name();
    let suffix = if db.backend() == Backend::Doris {
        " UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    } else {
        ""
    };
    let id = if db.backend() == Backend::Doris {
        ""
    } else {
        " PRIMARY KEY"
    };
    let bytes = if db.backend() == Backend::Doris {
        "STRING"
    } else {
        "LONGBLOB"
    };
    db.pool().execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {t}(id BIGINT NOT NULL{id},tenant BIGINT NOT NULL,priority BIGINT NULL,title VARCHAR(128) NULL,payload {bytes} NULL){suffix}"))).await.unwrap();
    let mut q: sqlx::QueryBuilder<sqlx::MySql> =
        sqlx::QueryBuilder::new(format!("INSERT INTO {t} "));
    let data = events();
    q.push_values(&data, |mut b, v| {
        b.push_bind(v.id)
            .push_bind(v.tenant)
            .push_bind(v.priority)
            .push_bind(&v.title)
            .push_bind(&v.payload);
    });
    q.build().execute(db.pool()).await.unwrap();
    t
}
fn selected(tenant: Option<i64>, minimum: Option<i64>, payload: Option<&[u8]>) -> Vec<Event> {
    let mut v: Vec<_> = events()
        .into_iter()
        .filter(|r| {
            tenant.is_none_or(|v| r.tenant == v)
                && minimum.is_none_or(|v| r.priority.is_some_and(|p| p >= v))
                && payload.is_none_or(|v| r.payload.as_deref() == Some(v))
        })
        .collect();
    // Explicit NULL placement and an id tie breaker define portable pagination.
    v.sort_by_key(|r| {
        (
            r.priority.is_none(),
            std::cmp::Reverse(r.priority),
            std::cmp::Reverse(r.id),
        )
    });
    v
}
fn page_query(t: &TableName) -> String {
    format!(
        "SELECT id,tenant,priority,title,payload FROM {t} WHERE (? IS NULL OR tenant=?) AND (? IS NULL OR priority>=?) AND (? IS NULL OR payload=?) ORDER BY priority IS NULL,priority DESC,id DESC LIMIT ? OFFSET ?"
    )
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn optional_filters_bound_pages_match_independent_business_rows() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = seed(&db).await;
        let sql = page_query(&t);
        let mut c = db.pool().acquire().await.unwrap();
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for persistent in [true, true, false] {
                for tenant in [None, Some(1), Some(99)] {
                    for minimum in [None, Some(0), Some(3)] {
                        for payload in [
                            None,
                            Some(Vec::new()),
                            Some(vec![255, 0, 128, 39, 92]),
                            Some(b"same ".to_vec()),
                        ] {
                            let expected = selected(tenant, minimum, payload.as_deref());
                            for (limit, offset) in [(0_u64, 0_u64), (1, 0), (7, 3), (65, 99)] {
                                let actual: Vec<Event> =
                                    sqlx::query_as(sqlx::AssertSqlSafe(sql.clone()))
                                        .bind(tenant)
                                        .bind(tenant)
                                        .bind(minimum)
                                        .bind(minimum)
                                        .bind(payload.clone())
                                        .bind(payload.clone())
                                        .bind(limit)
                                        .bind(offset)
                                        .persistent(persistent)
                                        .fetch_all(&mut *c)
                                        .await
                                        .unwrap();
                                let oracle: Vec<_> = expected
                                    .iter()
                                    .skip(offset as usize)
                                    .take(limit as usize)
                                    .cloned()
                                    .collect();
                                assert_eq!(
                                    actual, oracle,
                                    "{b:?}/{mode}/{persistent}/{tenant:?}/{minimum:?}/{payload:?}/{limit}/{offset}"
                                );
                            }
                        }
                    }
                }
            }
        }
        drop(c);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn querybuilder_keyset_comma_and_nested_pages_keep_order_and_bindings() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = seed(&db).await;
        for tenant in [None, Some(0), Some(2), Some(99)] {
            for persistent in [true, true, false] {
                let mut cursor = -1_i64;
                let mut all = Vec::<Event>::new();
                loop {
                    let mut q: sqlx::QueryBuilder<sqlx::MySql> = sqlx::QueryBuilder::new(format!(
                        "SELECT id,tenant,priority,title,payload FROM {t} WHERE id>"
                    ));
                    q.push_bind(cursor);
                    if let Some(tenant) = tenant {
                        q.push(" AND tenant=").push_bind(tenant);
                    }
                    q.push(" ORDER BY id LIMIT ").push_bind(7_i32);
                    let page = q
                        .build_query_as::<Event>()
                        .persistent(persistent)
                        .fetch_all(db.pool())
                        .await
                        .unwrap();
                    if page.is_empty() {
                        break;
                    }
                    cursor = page.last().unwrap().id;
                    all.extend(page);
                }
                let expected: Vec<_> = events()
                    .into_iter()
                    .filter(|r| tenant.is_none_or(|v| r.tenant == v))
                    .collect();
                assert_eq!(all, expected);
            }
        }
        for persistent in [true, true, false] {
            for sql in [
                format!("SELECT id FROM {t} WHERE tenant=? ORDER BY id LIMIT ?,?"),
                format!(
                    "SELECT id FROM (SELECT id FROM {t} WHERE tenant=? ORDER BY id LIMIT 15) a ORDER BY id LIMIT ? OFFSET ?"
                ),
            ] {
                let comma = sql.contains("?,?");
                let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
                    .bind(1_i64)
                    .bind(if comma { 3_i64 } else { 5_i64 })
                    .bind(if comma { 5_i64 } else { 3_i64 })
                    .persistent(persistent)
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                assert_eq!(ids, [10, 13, 16, 19, 22]);
            }
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT id FROM (SELECT id FROM {t} WHERE tenant=? ORDER BY id LIMIT ?) a ORDER BY id DESC LIMIT ? OFFSET ?")))
                .bind(1_i64).bind(9_i64).bind(3_i64).bind(2_i64).persistent(persistent).fetch_all(db.pool()).await.unwrap();
            assert_eq!(ids, [19, 16, 13]);
        }
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

#[derive(Debug, PartialEq, sqlx::FromRow)]
struct Report {
    priority: Option<i64>,
    row_count: i64,
    populated: i64,
    total: Option<i64>,
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn filtered_aggregate_having_and_empty_reports_share_dto_and_bound_pages() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = seed(&db).await;
        for tenant in [None, Some(0_i64), Some(99)] {
            for threshold in [1_i64, 5, 99] {
                for persistent in [true, true, false] {
                    let rows: Vec<Report> = sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT priority,CAST(COUNT(*) AS SIGNED) row_count,CAST(COUNT(title) AS SIGNED) populated,CAST(SUM(id) AS SIGNED) total FROM {t} WHERE (? IS NULL OR tenant=?) GROUP BY priority HAVING COUNT(*)>=? ORDER BY priority IS NULL,priority LIMIT ? OFFSET ?")))
                        .bind(tenant).bind(tenant).bind(threshold).bind(3_i64).bind(1_i64).persistent(persistent).fetch_all(db.pool()).await.unwrap();
                    let mut oracle = Vec::new();
                    for priority in [Some(0), Some(1), Some(2), Some(3), None] {
                        let group: Vec<_> = events()
                            .into_iter()
                            .filter(|r| {
                                tenant.is_none_or(|v| r.tenant == v) && r.priority == priority
                            })
                            .collect();
                        if group.len() as i64 >= threshold {
                            oracle.push(Report {
                                priority,
                                row_count: group.len() as i64,
                                populated: group.iter().filter(|r| r.title.is_some()).count()
                                    as i64,
                                total: Some(group.iter().map(|r| r.id).sum()),
                            });
                        }
                    }
                    assert_eq!(
                        rows,
                        oracle.into_iter().skip(1).take(3).collect::<Vec<_>>(),
                        "{b:?}/{tenant:?}/{threshold}/{persistent}"
                    );
                }
            }
        }
        let empty: (i64,i64,Option<i64>)=sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT CAST(COUNT(*) AS SIGNED),CAST(COUNT(title) AS SIGNED),CAST(SUM(id) AS SIGNED) FROM {t} WHERE tenant=? LIMIT ?")))
            .bind(99_i64).bind(1_i64).fetch_one(db.pool()).await.unwrap();
        assert_eq!(empty, (0, 0, None));
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn pagination_wire_cache_and_invalid_values_keep_native_mysql_protocol() {
    use sqlx::SqlSafeStr;
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = seed(&admin).await;
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        let sql = format!("SELECT id FROM {t} WHERE payload=? ORDER BY id LIMIT ? OFFSET ?");
        let before = p.trace().len();
        for persistent in [true, true, false] {
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                .bind(vec![255_u8, 0, 128, 39, 92])
                .bind(2_i64)
                .bind(1_i64)
                .persistent(persistent)
                .fetch_all(&mut *c)
                .await
                .unwrap();
            assert_eq!(ids, [8, 14]);
        }
        let trace = p.trace();
        let trace = &trace[before..];
        if b == Backend::Doris {
            assert!(!trace.iter().any(|v| matches!(v[0], 22 | 23)));
            let pages: Vec<_> = trace
                .iter()
                .filter(|v| v[0] == 3 && String::from_utf8_lossy(v).contains("ORDER BY id LIMIT"))
                .collect();
            assert_eq!(pages.len(), 3);
            assert!(
                pages
                    .iter()
                    .all(|v| String::from_utf8_lossy(v).contains("LIMIT 2 OFFSET 1"))
            );
        } else {
            assert_eq!(trace.iter().filter(|v| v[0] == 22).count(), 2);
            assert_eq!(trace.iter().filter(|v| v[0] == 23).count(), 3);
            assert!(
                trace.iter().filter(|v| v[0] == 23).all(|v| v[12] == 252),
                "original native BLOB wire type"
            );
        }
        if b == Backend::Doris {
            for kind in 0..7 {
                let before = p.trace().len();
                let q = sqlx::query_scalar::<_, i64>("SELECT 1 LIMIT ?");
                let result = match kind {
                    0 => q.bind(-1_i64).fetch_all(&mut *c).await,
                    1 => q.bind(u64::MAX).fetch_all(&mut *c).await,
                    2 => q.bind(None::<i64>).fetch_all(&mut *c).await,
                    3 => q.bind("2").fetch_all(&mut *c).await,
                    4 => q.bind("1; DELETE FROM impossible").fetch_all(&mut *c).await,
                    5 => q.bind(1.0_f64).fetch_all(&mut *c).await,
                    _ => q.bind(vec![1_u8]).fetch_all(&mut *c).await,
                };
                assert!(
                    matches!(result, Err(sqlx::Error::Protocol(_))),
                    "{result:?}"
                );
                assert_eq!(
                    before,
                    p.trace().len(),
                    "invalid pagination emitted a packet"
                );
            }
            // Explicit metadata-only preparation has no page values. Preserve
            // its native database error rather than inventing column metadata.
            let error = c
                .prepare("SELECT 7 LIMIT ?".into_sql_str())
                .await
                .unwrap_err();
            assert!(matches!(error, sqlx::Error::Database(_)));
        }
        let health: i64 = sqlx::query_scalar("SELECT 42")
            .fetch_one(&mut *c)
            .await
            .unwrap();
        assert_eq!(health, 42);
        drop(c);
        db.close().await;
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn optional_byte_null_guards_use_native_prepared_results_without_pagination() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = seed(&db).await;
        for value in [
            None,
            Some(Vec::<u8>::new()),
            Some(vec![255, 0, 128, 39, 92]),
        ] {
            for persistent in [true, true, false] {
                let truth: (i64, i64) = sqlx::query_as(
                    "SELECT CAST(? IS NULL AS SIGNED),CAST(? IS NOT NULL AS SIGNED)",
                )
                .bind(value.clone())
                .bind(value.clone())
                .persistent(persistent)
                .fetch_one(db.pool())
                .await
                .unwrap();
                assert_eq!(
                    truth,
                    (i64::from(value.is_none()), i64::from(value.is_some()))
                );
                let actual:Vec<Event>=sqlx::query_as(sqlx::AssertSqlSafe(format!("SELECT id,tenant,priority,title,payload FROM {t} WHERE (? IS NULL OR payload=?) ORDER BY id")))
                    .bind(value.clone()).bind(value.clone()).persistent(persistent).fetch_all(db.pool()).await.unwrap();
                let expected: Vec<_> = events()
                    .into_iter()
                    .filter(|r| value.as_ref().is_none_or(|v| r.payload.as_ref() == Some(v)))
                    .collect();
                assert_eq!(actual, expected, "{b:?}/{value:?}/{persistent}");
            }
        }
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn composed_optional_mutations_commit_rollback_drop_then_page_outside_transaction() {
    use sqlx::Acquire;
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        for action in ["commit", "rollback", "drop"] {
            let t = seed(&db).await;
            let mut c = db.pool().acquire().await.unwrap();
            let mut tx = c.begin().await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE {t} SET title=?,priority=priority+? WHERE (? IS NULL OR tenant=?) AND (? IS NULL OR payload=?)")))
                .bind("changed ? '\\中\0").bind(10_i64).bind(Some(1_i64)).bind(Some(1_i64)).bind(None::<Vec<u8>>).bind(None::<Vec<u8>>)
                .execute(&mut *tx).await.unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {t} WHERE tenant=? AND (? IS NULL OR payload=?)"
            )))
            .bind(2_i64)
            .bind(Some(vec![255_u8, 0, 128, 39, 92]))
            .bind(Some(vec![255_u8, 0, 128, 39, 92]))
            .persistent(false)
            .execute(&mut *tx)
            .await
            .unwrap();
            match action {
                "commit" => tx.commit().await.unwrap(),
                "rollback" => tx.rollback().await.unwrap(),
                _ => drop(tx),
            }
            // Doris intentionally retires a connection after transaction drop;
            // the pool supplies a clean connection for the next business call.
            drop(c);
            let actual: Vec<Event> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id,tenant,priority,title,payload FROM {t} ORDER BY id LIMIT ? OFFSET ?"
            )))
            .bind(65_i64)
            .bind(0_i64)
            .fetch_all(db.pool())
            .await
            .unwrap();
            let mut expected = events();
            if action == "commit" {
                expected.retain(|r| {
                    !(r.tenant == 2
                        && r.payload.as_deref() == Some([255_u8, 0, 128, 39, 92].as_slice()))
                });
                for r in &mut expected {
                    if r.tenant == 1 {
                        r.title = Some("changed ? '\\中\0".into());
                        r.priority = r.priority.map(|v| v + 10);
                    }
                }
            }
            assert_eq!(actual, expected, "{b:?}/{action}");
            common::drop_table(&db, &t).await;
        }
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn cancelled_and_lost_paginated_results_are_not_replayed_and_connection_recovers() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = seed(&admin).await;
        for persistent in [true, false] {
            for lost in [false, true] {
                let mode = if b == Backend::Doris {
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
                let sql = format!(
                    "SELECT id,tenant,priority,title,payload FROM {t} ORDER BY id LIMIT ? OFFSET ? /* {} */",
                    proxy::MARKER
                );
                p.suspend_faults(true);
                let warm: Vec<Event> = sqlx::query_as(sqlx::AssertSqlSafe(sql.clone()))
                    .bind(7_i64)
                    .bind(3_i64)
                    .persistent(persistent)
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                assert_eq!(warm, events()[3..10]);
                let before = p.trace().len();
                p.suspend_faults(false);
                {
                    let future = sqlx::query_as::<_, Event>(sqlx::AssertSqlSafe(sql))
                        .bind(7_i64)
                        .bind(3_i64)
                        .persistent(persistent)
                        .fetch_all(db.pool());
                    tokio::pin!(future);
                    if lost {
                        assert!(future.await.is_err());
                    } else {
                        tokio::select! {_=&mut future=>panic!("result did not pause"),_=p.reached()=>{}}
                    }
                }
                assert_ne!(p.fault_reply(), 255);
                p.release();
                let health: i64 = tokio::time::timeout(
                    Duration::from_secs(5),
                    sqlx::query_scalar("SELECT 42").fetch_one(db.pool()),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(health, 42);
                assert_eq!(p.hits(), 1);
                let trace = p.trace();
                let trace = &trace[before..];
                if b == Backend::Doris {
                    assert_eq!(
                        trace
                            .iter()
                            .filter(
                                |v| v[0] == 3 && String::from_utf8_lossy(v).contains(proxy::MARKER)
                            )
                            .count(),
                        1
                    );
                } else {
                    assert_eq!(
                        trace.iter().filter(|v| v[0] == 23).count(),
                        2,
                        "one interrupted page and one health query"
                    );
                }
                let actual: Vec<Event> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT id,tenant,priority,title,payload FROM {t} ORDER BY id"
                )))
                .fetch_all(admin.pool())
                .await
                .unwrap();
                assert_eq!(actual, events());
                db.close().await;
            }
        }
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn bound_pagination_source_control() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let result = sqlx::query_scalar::<_, i64>("SELECT 7 AS id LIMIT ? OFFSET ?")
            .bind(1_u64)
            .bind(0_u64)
            .fetch_all(db.pool())
            .await;
        assert_eq!(result.unwrap(), [7], "{b:?}");
        db.close().await;
    }
}
