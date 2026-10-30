//! BETWEEN keeps both inclusive boundaries and SQL three-valued logic.
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
        "(id BIGINT,b STRING,n BIGINT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) \
            BUCKETS 1 PROPERTIES('replication_num'='1')"
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
async fn between_source_controls_native_parameters_and_public_hex() {
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = seed(&db).await;
        let original = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {t} WHERE b BETWEEN ? AND ? ORDER BY id"
        )))
        .bind(vec![255_u8])
        .bind(vec![255_u8, 0])
        .fetch_all(db.pool())
        .await;
        assert_eq!(original.unwrap(), [17, 18]);
        let public: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT id FROM {t} WHERE HEX(b) BETWEEN ? AND ? ORDER BY id"
        )))
        .bind("FF")
        .bind("FF00")
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(public, [17, 18]);
        let nulls: (Option<i64>, Option<i64>, Option<i64>, Option<i64>) = sqlx::query_as(
            "SELECT UNHEX('FF') BETWEEN NULL AND UNHEX('00'),UNHEX('FF') NOT BETWEEN \
            NULL AND UNHEX('00'),UNHEX('00') BETWEEN UNHEX('FF') AND NULL,UNHEX('00') \
            NOT BETWEEN UNHEX('FF') AND NULL",
        )
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(nulls, (Some(0), Some(1), Some(0), Some(1)));
        eprintln!("{b:?} NULL false-range controls: {nulls:?}");
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}

#[derive(Clone, Debug)]
enum Bound {
    IntegerNull,
    Bytes(Option<Vec<u8>>),
    Text(Option<&'static str>),
    Literal(&'static str),
    Null,
}
impl Bound {
    fn sql(&self) -> String {
        match self {
            Self::Literal(v) => format!("'{v}'"),
            Self::Null => "NULL".into(),
            _ => "((?))".into(),
        }
    }
    fn bytes(&self) -> Option<Vec<u8>> {
        match self {
            Self::Bytes(v) => v.clone(),
            Self::Text(v) => v.map(|s| s.as_bytes().to_vec()),
            Self::Literal(v) => Some(v.as_bytes().to_vec()),
            Self::Null | Self::IntegerNull => None,
        }
    }
}
fn bounds_query(
    sql: String,
    low: &Bound,
    high: &Bound,
) -> sqlx::query::Query<'static, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    let mut q = sqlx::query(sqlx::AssertSqlSafe(sql));
    for v in [low, high] {
        q = match v {
            Bound::Bytes(v) => q.bind(v.clone()),
            Bound::Text(v) => q.bind(v.map(str::to_owned)),
            Bound::IntegerNull => q.bind(None::<i64>),
            _ => q,
        };
    }
    q
}
fn between(value: &Option<Vec<u8>>, low: &Bound, high: &Bound, not: bool) -> Option<i64> {
    let a = compare(value, &low.bytes(), ">=");
    let b = compare(value, &high.bytes(), "<=");
    let result = if a == Some(0) || b == Some(0) {
        Some(0)
    } else if a == Some(1) && b == Some(1) {
        Some(1)
    } else {
        None
    };
    result.map(|v| if not { 1 - v } else { v })
}
fn pairs() -> Vec<(Bound, Bound)> {
    use Bound::*;
    vec![
        (Bytes(Some(vec![255])), Bytes(Some(vec![255, 0]))),
        (Bytes(Some(vec![255, 0])), Bytes(Some(vec![255, 0]))),
        (Bytes(Some(vec![255, 32])), Bytes(Some(vec![255, 0]))),
        (Bytes(Some(vec![])), Bytes(Some(vec![0]))),
        (Bytes(None), Bytes(Some(vec![]))),
        (Bytes(Some(vec![255])), Bytes(None)),
        (Bytes(None), Bytes(None)),
        (Bytes(Some(vec![])), Text(Some("a "))),
        (Text(Some("A")), Bytes(Some(vec![65, 0]))),
        (Bytes(Some(vec![0])), Text(None)),
        (Text(None), Bytes(Some(vec![]))),
        (Literal("A"), Bytes(Some(vec![97]))),
        (Bytes(Some(vec![0])), Literal("中😀")),
        (Null, Bytes(Some(vec![]))),
        (Bytes(Some(vec![255])), Null),
        (Bytes(None), Null),
        (Null, Bytes(None)),
        (IntegerNull, Bytes(Some(vec![]))),
        (Bytes(Some(vec![255])), IntegerNull),
    ]
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn between_inclusive_reversed_and_null_bounds_match_independent_three_valued_oracle() {
    let values = values();
    for b in [Backend::MySql, Backend::Doris] {
        let db = client(b).await;
        let t = seed(&db).await;
        let mut c = db.pool().acquire().await.unwrap();
        let mut count = 0;
        for mode in [
            "",
            "NO_BACKSLASH_ESCAPES",
            "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
        ] {
            c.execute(sqlx::AssertSqlSafe(format!("SET sql_mode='{mode}'")))
                .await
                .unwrap();
            for persistent in [true, true, false] {
                for not in [false, true] {
                    for (low, high) in pairs() {
                        let predicate = format!(
                            "((a.`b`)) {}BETWEEN /* AND ? */ {} AND {}",
                            if not { "NOT " } else { "" },
                            low.sql(),
                            high.sql()
                        );
                        let expected: Vec<_> = values
                            .iter()
                            .enumerate()
                            .map(|(i, v)| (i as i64, between(v, &low, &high, not)))
                            .collect();
                        let actual: Vec<(i64, Option<i64>)> = bounds_query(
                            format!("SELECT a.id,({predicate}) FROM {t} a ORDER BY a.id"),
                            &low,
                            &high,
                        )
                        .persistent(persistent)
                        .try_map(|r| Ok((sqlx::Row::try_get(&r, 0)?, sqlx::Row::try_get(&r, 1)?)))
                        .fetch_all(&mut *c)
                        .await
                        .unwrap();
                        assert_eq!(
                            actual, expected,
                            "{b:?}/{mode}/{persistent}/{not}/{low:?}/{high:?}"
                        );
                        let actual: Vec<i64> = bounds_query(
                            format!("SELECT a.id FROM {t} a WHERE {predicate} ORDER BY a.id"),
                            &low,
                            &high,
                        )
                        .persistent(persistent)
                        .try_map(|r| sqlx::Row::try_get(&r, 0))
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
                        count += 2;
                    }
                }
            }
        }
        // Full ordered pair corpus, including all 256 octets and significant tails.
        for low in &values {
            for high in &values {
                for not in [false, true] {
                    let (low, high) = (Bound::Bytes(low.clone()), Bound::Bytes(high.clone()));
                    let predicate = format!("b {}BETWEEN ? AND ?", if not { "NOT " } else { "" });
                    let actual: Vec<(i64, Option<i64>)> = bounds_query(
                        format!("SELECT id,({predicate}) FROM {t} ORDER BY id"),
                        &low,
                        &high,
                    )
                    .try_map(|r| Ok((sqlx::Row::try_get(&r, 0)?, sqlx::Row::try_get(&r, 1)?)))
                    .fetch_all(&mut *c)
                    .await
                    .unwrap();
                    assert_eq!(
                        actual,
                        values
                            .iter()
                            .enumerate()
                            .map(|(i, v)| (i as i64, between(v, &low, &high, not)))
                            .collect::<Vec<_>>(),
                        "{b:?}/{low:?}/{high:?}/{not}"
                    );
                    count += 1;
                }
            }
        }
        for (low, high) in pairs().into_iter().skip(4).take(3) {
            for not in [false, true] {
                for is_not in [false, true] {
                    for bare in [false, true] {
                        let condition = format!(
                            "{}b {}BETWEEN {} AND {}{} IS {}NULL",
                            if bare { "" } else { "(" },
                            if not { "NOT " } else { "" },
                            low.sql(),
                            high.sql(),
                            if bare { "" } else { ")" },
                            if is_not { "NOT " } else { "" }
                        );
                        let actual: Vec<i64> = bounds_query(
                            format!("SELECT id FROM {t} WHERE {condition} ORDER BY id"),
                            &low,
                            &high,
                        )
                        .try_map(|r| sqlx::Row::try_get(&r, 0))
                        .fetch_all(&mut *c)
                        .await
                        .unwrap();
                        assert_eq!(
                            actual,
                            values
                                .iter()
                                .enumerate()
                                .filter(|(_, v)| between(v, &low, &high, not).is_some() == is_not)
                                .map(|(i, _)| i as i64)
                                .collect::<Vec<_>>()
                        );
                        count += 1;
                    }
                }
            }
        }
        eprintln!(
            "{b:?}: BETWEEN/NOT BETWEEN {count} independent-oracle SELECTs; NULL \
                false-range semantics included"
        );
        drop(c);
        common::drop_table(&db, &t).await;
        db.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn between_cache_native_wire_querybuilder_lengths_and_boolean_scopes() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = seed(&admin).await;
        let (db, p) = proxied(b, proxy::Mode::Trace).await;
        let mut c = db.pool().acquire().await.unwrap();
        let sql = format!("SELECT id FROM {t} WHERE b BETWEEN ? AND ? ORDER BY id");
        c.prepare(sqlx::AssertSqlSafe(sql.clone()).into_sql_str())
            .await
            .unwrap();
        for persistent in [true, true, false] {
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql.clone()))
                .bind(vec![255_u8])
                .bind(vec![255_u8, 0])
                .persistent(persistent)
                .fetch_all(&mut *c)
                .await
                .unwrap();
            assert_eq!(ids, [17, 18]);
        }
        let trace = p.trace();
        assert!(trace.iter().any(|v| v[0] == 25));
        assert!(trace.iter().any(|v| v[0] == 23));
        assert!(
            trace
                .iter()
                .filter(|v| v[0] == 23)
                .all(|v| v[12] == if b == Backend::Doris { 253 } else { 252 }
                    && v[14] == if b == Backend::Doris { 253 } else { 252 })
        );
        if b == Backend::Doris {
            assert!(trace.iter().any(
                |v| v[0] == 22 && String::from_utf8_lossy(v).contains("HEX(b) BETWEEN ? AND ?")
            ));
        } else {
            assert!(
                !trace
                    .iter()
                    .any(|v| v[0] == 22 && String::from_utf8_lossy(v).contains("HEX(b)"))
            );
        }
        for persistent in [true, true, false] {
            for high in [Some("A"), None, Some("a ")] {
                let low = Bound::Bytes(Some(vec![]));
                let high = Bound::Text(high);
                let ids: Vec<i64> = bounds_query(sql.clone(), &low, &high)
                    .persistent(persistent)
                    .try_map(|r| sqlx::Row::try_get(&r, 0))
                    .fetch_all(&mut *c)
                    .await
                    .unwrap();
                assert_eq!(
                    ids,
                    values()
                        .iter()
                        .enumerate()
                        .filter(|(_, v)| between(v, &low, &high, false) == Some(1))
                        .map(|(i, _)| i as i64)
                        .collect::<Vec<_>>()
                );
            }
        }
        for fold in [false, true] {
            if b == Backend::Doris {
                c.execute(sqlx::AssertSqlSafe(format!(
                    "SET enable_fold_constant_by_be={fold}"
                )))
                .await
                .unwrap();
            }
            for count in [1, 4, 5, 8, 9, 17, 65] {
                let mut q =
                    sqlx::QueryBuilder::<sqlx::MySql>::new(format!("SELECT id FROM {t} WHERE ("));
                for i in 0..count {
                    if i > 0 {
                        q.push(" OR ");
                    }
                    q.push("b BETWEEN ")
                        .push_bind((i == count - 1).then_some(vec![255_u8]))
                        .push(" AND ")
                        .push_bind((i == count - 1).then_some(vec![255_u8, 0]));
                }
                q.push(") AND id>=").push_bind(0_i64).push(" ORDER BY id");
                let ids: Vec<i64> = q.build_query_scalar().fetch_all(&mut *c).await.unwrap();
                assert_eq!(ids, [17, 18], "{b:?}/{fold}/{count}");
            }
            for n in [0, 250, 251, 65535, 65536] {
                let low = Bound::Bytes(Some(vec![0; n]));
                let high = Bound::Bytes(Some(vec![255; n]));
                for not in [false, true] {
                    let actual: Vec<i64> = bounds_query(
                        format!(
                            "SELECT id FROM {t} WHERE b {}BETWEEN ? AND ? ORDER BY id",
                            if not { "NOT " } else { "" }
                        ),
                        &low,
                        &high,
                    )
                    .try_map(|r| sqlx::Row::try_get(&r, 0))
                    .fetch_all(&mut *c)
                    .await
                    .unwrap();
                    assert_eq!(
                        actual,
                        values()
                            .iter()
                            .enumerate()
                            .filter(|(_, v)| between(v, &low, &high, not) == Some(1))
                            .map(|(i, _)| i as i64)
                            .collect::<Vec<_>>()
                    );
                }
            }
            let ids: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT a.id FROM {t} a JOIN {t} r ON a.b BETWEEN ? AND ? AND r.id=a.id \
                WHERE NOT(a.b NOT BETWEEN ? AND ?) ORDER BY a.id"
            )))
            .bind(vec![255_u8])
            .bind(vec![255_u8, 0])
            .bind(vec![255_u8])
            .bind(vec![255_u8, 0])
            .fetch_all(&mut *c)
            .await
            .unwrap();
            assert_eq!(ids, [17, 18]);
        }
        drop(c);
        db.close().await;
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}
#[tokio::test]
#[ignore = "requires dedicated unmodified MySQL and Doris"]
async fn between_update_delete_commit_rollback_drop_preserve_both_bounds_and_nulls() {
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
                    "UPDATE {t} SET n=n+1 WHERE b BETWEEN ? AND ?"
                )))
                .bind(vec![255_u8])
                .bind(vec![255_u8, 0])
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {t} SET n=n+1 WHERE b NOT BETWEEN ? AND ? AND id=20"
                )))
                .bind(None::<Vec<u8>>)
                .bind(Vec::<u8>::new())
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {t} WHERE b NOT BETWEEN ? AND ? AND id=19"
                )))
                .bind(None::<Vec<u8>>)
                .bind(Vec::<u8>::new())
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {t} WHERE b BETWEEN ? AND ?"
                )))
                .bind(None::<Vec<u8>>)
                .bind(Vec::<u8>::new())
                .execute(&mut *tx)
                .await
                .unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {t} WHERE b BETWEEN ? AND ?"
                )))
                .bind(vec![255_u8, 32])
                .bind(vec![255_u8, 0])
                .execute(&mut *tx)
                .await
                .unwrap();
                let replacement = vec![255_u8, 128, 0, 39, 92];
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {t} SET b=? WHERE b BETWEEN ? AND ? AND id=5"
                )))
                .bind(&replacement)
                .bind(vec![0_u8])
                .bind(vec![0_u8])
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
                        .filter(|v| v.0 != 19)
                        .map(|(id, v, n)| {
                            (
                                id,
                                if id == 5 {
                                    Some(replacement.clone())
                                } else {
                                    v
                                },
                                n + if [17, 18, 20].contains(&id) { 1 } else { 0 },
                            )
                        })
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
async fn cached_between_and_not_between_writes_cancel_or_lose_reply_without_replay() {
    for b in [Backend::MySql, Backend::Doris] {
        let admin = client(b).await;
        let t = seed(&admin).await;
        for not in [false, true] {
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
                            "UPDATE {t} SET n=n+1 WHERE b {}BETWEEN ? AND ? AND id=17 /* {} */",
                            if not { "NOT " } else { "" },
                            proxy::MARKER
                        );
                        p.suspend_faults(true);
                        sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                            .bind(if not {
                                None::<Vec<u8>>
                            } else {
                                Some(vec![255_u8])
                            })
                            .bind(if not {
                                Vec::<u8>::new()
                            } else {
                                vec![255_u8, 0]
                            })
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
                                    .bind(if not {
                                        None::<Vec<u8>>
                                    } else {
                                        Some(vec![255_u8])
                                    })
                                    .bind(if not {
                                        Vec::<u8>::new()
                                    } else {
                                        vec![255_u8, 0]
                                    })
                                    .persistent(persistent)
                                    .execute(&mut *tx);
                                tokio::pin!(future);
                                if lost {
                                    assert!(future.await.is_err())
                                } else {
                                    tokio::select! {
                                        _ = &mut future => panic!("write did not pause"),
                                        _ = p.reached() => {}
                                    }
                                }
                            }
                            drop(tx);
                        } else {
                            let future = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                                .bind(if not {
                                    None::<Vec<u8>>
                                } else {
                                    Some(vec![255_u8])
                                })
                                .bind(if not {
                                    Vec::<u8>::new()
                                } else {
                                    vec![255_u8, 0]
                                })
                                .persistent(persistent)
                                .execute(db.pool());
                            tokio::pin!(future);
                            if lost {
                                assert!(future.await.is_err())
                            } else {
                                tokio::select! {
                                    _ = &mut future => panic!("write did not pause"),
                                    _ = p.reached() => {}
                                }
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
                                if !tx && id == 17 { 1 } else { 0 },
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
        }
        common::drop_table(&admin, &t).await;
        admin.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated unmodified Doris"]
async fn unsupported_between_shapes_send_zero_packets_and_preserve_prior_transaction_writes() {
    let (db, p) = proxied(Backend::Doris, proxy::Mode::Trace).await;
    let t = seed(&db).await;
    let invalid = [
        "b BETWEEN ?+1 AND ?",
        "b BETWEEN ? AND ABS(?)",
        "CONCAT(b,'x') BETWEEN ? AND ?",
        "b+1 BETWEEN ? AND ?",
        "b BETWEEN ? AND ?=id",
        "b BETWEEN ? AND ? COLLATE utf8mb4_bin",
        "b BETWEEN (? AND 1) AND ?",
        "b BETWEEN (SELECT ?) AND ?",
        "b BETWEEN ? AND CASE WHEN id=1 THEN ? END",
        "b BETWEEN ? AND ? IS FALSE",
        "CASE WHEN b BETWEEN ? AND ? THEN 1 END",
        "? BETWEEN b AND ?",
        "CONCAT(?,'x') BETWEEN 'a' AND ?",
        "?+1 BETWEEN 'a' AND ?",
        "b BETWEEN ? AND (? IN(1,2))",
    ];
    let mut rejected = 0;
    for expression in invalid {
        for sql in [
            format!("SELECT ({expression}) FROM {t}"),
            format!("SELECT id FROM {t} WHERE {expression}"),
        ] {
            let before = p.trace().len();
            let e = sqlx::query(sqlx::AssertSqlSafe(sql.clone()))
                .bind(vec![255_u8])
                .bind(vec![255_u8, 0])
                .fetch_all(db.pool())
                .await
                .unwrap_err();
            assert!(e.to_string().contains("Doris client binding"), "{sql}/{e}");
            assert_eq!(before, p.trace().len(), "{sql}");
            rejected += 1;
        }
    }
    for expression in [
        "b BETWEEN ?",
        "b BETWEEN ? AND",
        "b BETWEEN AND ?",
        "b BETWEEN ? OR id=1",
    ] {
        let before = p.trace().len();
        let e = sqlx::query(sqlx::AssertSqlSafe(format!("SELECT {expression} FROM {t}")))
            .bind(vec![255_u8])
            .fetch_all(db.pool())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("Doris client binding"));
        assert_eq!(before, p.trace().len());
        rejected += 1;
    }
    for typed in [false, true] {
        let before = p.trace().len();
        let q = sqlx::query(sqlx::AssertSqlSafe(format!(
            "SELECT b BETWEEN ? AND ? FROM {t}"
        )))
        .bind(vec![255_u8]);
        let e = if typed {
            q.bind(sqlx::mysql::types::MySqlTime::ZERO)
                .fetch_all(db.pool())
                .await
                .unwrap_err()
        } else {
            q.bind(7_i64).fetch_all(db.pool()).await.unwrap_err()
        };
        assert!(e.to_string().contains("Doris client binding"));
        assert_eq!(before, p.trace().len());
        rejected += 1;
    }
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {t} SET n=n+1 WHERE b BETWEEN ? AND ?"
    )))
    .bind(vec![255_u8])
    .bind(vec![255_u8, 0])
    .execute(&mut *tx)
    .await
    .unwrap();
    let before = p.trace().len();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DELETE FROM {t} WHERE b BETWEEN ? AND ?+1"
    )))
    .bind(vec![255_u8])
    .bind(vec![255_u8, 0])
    .execute(&mut *tx)
    .await
    .unwrap_err();
    assert_eq!(before, p.trace().len());
    tx.commit().await.unwrap();
    for (id, _, n) in stored(&db, &t).await {
        assert_eq!(n, if [17, 18].contains(&id) { 1 } else { 0 });
    }
    eprintln!(
        "BETWEEN invalid shapes: {rejected} preflight errors sent zero packets; \
            prior transaction write committed"
    );
    common::drop_table(&db, &t).await;
    db.close().await;
}
