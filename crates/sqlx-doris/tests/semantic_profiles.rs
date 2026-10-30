mod common;
#[path = "support/sql_proxy.rs"]
mod proxy;

use chrono::{DateTime, NaiveDate, Utc};
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::TableName,
    sqlx::{
        self, Executor,
        mysql::{MySqlConnectOptions, MySqlPoolOptions, types::MySqlTime},
        types::Json,
    },
};
use std::str::FromStr;

fn options(backend: Backend) -> MySqlConnectOptions {
    MySqlConnectOptions::from_str(
        &std::env::var(if backend == Backend::MySql {
            "MYSQL_TEST_URL"
        } else {
            "DORIS_TEST_URL"
        })
        .expect("dedicated test URL required"),
    )
    .unwrap()
}
fn negative() -> MySqlTime {
    MySqlTime::new(sqlx::mysql::types::MySqlTimeSign::Negative, 0, 0, 0, 1).unwrap()
}
async fn table(db: &Client) -> TableName {
    let table = common::name();
    let schema = if db.backend() == Backend::MySql {
        "(id BIGINT PRIMARY KEY,t TIME(6),n BIGINT)"
    } else {
        "(id BIGINT,t VARCHAR(64),n BIGINT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) \
            BUCKETS 1 PROPERTIES('replication_num'='1')"
    };
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}{schema}")))
        .await
        .unwrap();
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES(0,NULL,0),(1,'-00:00:00.000001',0),(2,'00:00:00',\
            0),(3,'25:00:00',0)"
        )))
        .await
        .unwrap();
    table
}
async fn ids(db: &Client, table: &TableName, persistent: bool) -> Vec<i64> {
    sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {table} WHERE t IN(?,?) ORDER BY id"
    )))
    .bind(negative())
    .bind(MySqlTime::ZERO)
    .persistent(persistent)
    .fetch_all(db.pool())
    .await
    .unwrap()
}
async fn rows(db: &Client, table: &TableName) -> Vec<(i64, i64)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,n FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated MySQL; native expectations have an isolated official SQLx control"]
async fn plain_mysql_retains_native_coercion_and_sdk_can_select_each_time_domain() {
    assert!(!MySqlConnectOptions::new().get_time_membership_compatibility());
    for profile in ["raw", "raw-opt-in", "managed", "managed-native"] {
        let normalized = matches!(profile, "raw-opt-in" | "managed");
        let db = if profile.starts_with("raw") {
            Client::from_pool(
                Backend::MySql,
                MySqlPoolOptions::new()
                    .max_connections(1)
                    .connect_with(options(Backend::MySql).time_membership_compatibility(normalized))
                    .await
                    .unwrap(),
            )
        } else {
            PoolBuilder::new(Backend::MySql)
                .max_connections(1)
                .mysql_time_membership_compatibility(normalized)
                .connect_with(options(Backend::MySql))
                .await
                .unwrap()
        };
        assert_eq!(
            db.pool()
                .connect_options()
                .get_time_membership_compatibility(),
            normalized
        );
        let table = table(&db).await;
        for persistent in [true, true, false] {
            assert_eq!(
                ids(&db, &table, persistent).await,
                if normalized { vec![1, 2] } else { vec![1] },
                "{profile}"
            );
            let mixed: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {table} WHERE t IN(?,?) ORDER BY id"
            )))
            .bind(MySqlTime::ZERO)
            .bind("25:00:00")
            .persistent(persistent)
            .fetch_all(db.pool())
            .await
            .unwrap();
            assert_eq!(
                mixed,
                if normalized {
                    vec![2, 3]
                } else if persistent {
                    vec![]
                } else {
                    vec![3]
                },
                "{profile}/{persistent}: matches the isolated official cache sequence"
            );
            let numeric = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {table} WHERE t IN(?,?) ORDER BY id"
            )))
            .bind(MySqlTime::ZERO)
            .bind(0_i64)
            .persistent(persistent)
            .fetch_all(db.pool())
            .await;
            if normalized {
                assert!(matches!(numeric.unwrap_err(), sqlx::Error::Protocol(_)));
            } else {
                assert_eq!(numeric.unwrap(), if persistent { vec![] } else { vec![2] });
            }
        }
        let nulls: Vec<(i64, Option<i64>, Option<i64>)> =
            sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id,CAST(t IN(?,?) AS SIGNED),CAST(t NOT IN(?,?) AS SIGNED) FROM \
                {table} ORDER BY id"
            )))
            .bind(negative())
            .bind(None::<MySqlTime>)
            .bind(negative())
            .bind(None::<MySqlTime>)
            .fetch_all(db.pool())
            .await
            .unwrap();
        assert_eq!(
            nulls,
            [
                (0, None, None),
                (1, Some(1), Some(0)),
                (2, None, None),
                (3, None, None)
            ]
        );
        for outcome in 0..3 {
            let mut tx = db.pool().begin().await.unwrap();
            let affected = sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {table} SET n=n+1 WHERE t IN(?,?)"
            )))
            .bind(negative())
            .bind(MySqlTime::ZERO)
            .execute(&mut *tx)
            .await
            .unwrap()
            .rows_affected();
            assert_eq!(affected, if normalized { 2 } else { 1 });
            match outcome {
                0 => tx.rollback().await.unwrap(),
                1 => drop(tx),
                _ => tx.commit().await.unwrap(),
            }
            assert_eq!(
                rows(&db, &table).await,
                if outcome == 2 {
                    vec![(0, 0), (1, 1), (2, i64::from(normalized)), (3, 0)]
                } else {
                    vec![(0, 0), (1, 0), (2, 0), (3, 0)]
                }
            );
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris; records actual PREPARE/EXECUTE versus COM_QUERY"]
async fn time_domain_selection_preserves_wire_types_cache_and_doris_convenience() {
    for backend in [Backend::MySql, Backend::Doris] {
        for normalize in [false, true] {
            let p = proxy::Proxy::start(&options(backend), proxy::Mode::Trace).await;
            let db = PoolBuilder::new(backend)
                .max_connections(1)
                .mysql_time_membership_compatibility(normalize)
                .connect_with(p.options(options(backend)))
                .await
                .unwrap();
            assert_eq!(
                db.pool()
                    .connect_options()
                    .get_time_membership_compatibility(),
                backend == Backend::MySql && normalize
            );
            let table = table(&db).await;
            let start = p.trace().len();
            for persistent in [true, true, false] {
                assert_eq!(
                    ids(&db, &table, persistent).await,
                    if backend == Backend::Doris || normalize {
                        vec![1, 2]
                    } else {
                        vec![1]
                    }
                );
            }
            let trace = p.trace();
            let packets = &trace[start..];
            if backend == Backend::MySql {
                let sql = format!("SELECT id FROM {table} WHERE t IN(?,?) ORDER BY id");
                let prepares: Vec<_> = packets
                    .iter()
                    .filter(|p| p.first() == Some(&0x16))
                    .collect();
                assert_eq!(prepares.len(), 2);
                for packet in prepares {
                    let actual = std::str::from_utf8(&packet[1..]).unwrap();
                    if normalize {
                        assert!(actual.contains("REPLACE(CAST(CAST(t AS TIME(6)) AS CHAR)"));
                    } else {
                        assert_eq!(actual, sql);
                    }
                }
                let executes: Vec<_> = packets
                    .iter()
                    .filter(|p| p.first() == Some(&0x17))
                    .collect();
                assert_eq!(executes.len(), 3);
                for packet in executes {
                    // Command, statement id, cursor flag, iteration count,
                    // null bitmap, new-types flag, then two native TIME types.
                    assert_eq!(&packet[12..16], &[0x0b, 0, 0x0b, 0]);
                }
            } else {
                assert_eq!(
                    packets.iter().filter(|p| p.first() == Some(&0x16)).count(),
                    0
                );
                assert_eq!(
                    packets.iter().filter(|p| p.first() == Some(&0x17)).count(),
                    0
                );
                assert_eq!(
                    packets.iter().filter(|p| p.first() == Some(&0x03)).count(),
                    3
                );
            }
            common::drop_table(&db, &table).await;
            db.close().await;
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris; lost replies must never replay either \
    selected SQL domain"]
async fn native_and_shared_time_domains_keep_uncertain_mutations_single() {
    for backend in [Backend::MySql, Backend::Doris] {
        for normalized in [false, true] {
            for lost in [false, true] {
                let p = proxy::Proxy::start(
                    &options(backend),
                    match (backend, lost) {
                        (Backend::MySql, false) => proxy::Mode::PauseExecute,
                        (Backend::MySql, true) => proxy::Mode::LostExecuteReply,
                        (Backend::Doris, false) => proxy::Mode::PauseReply,
                        (Backend::Doris, true) => proxy::Mode::LostWriteReply,
                    },
                )
                .await;
                let db = PoolBuilder::new(backend)
                    .max_connections(1)
                    .mysql_time_membership_compatibility(normalized)
                    .connect_with(p.options(options(backend)))
                    .await
                    .unwrap();
                let table = table(&db).await;
                let sql = format!(
                    "UPDATE {table} SET n=n+1 WHERE t IN(?,?) /* {} */",
                    proxy::MARKER
                );
                let query = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(negative())
                    .bind(MySqlTime::ZERO)
                    .execute(db.pool());
                if lost {
                    assert!(query.await.is_err());
                } else {
                    let mut query = Box::pin(query);
                    tokio::select! {
                        r = &mut query => panic!("write finished before controlled cancellation: {r:?}"),
                        _ = p.reached() => {}
                    };
                    drop(query);
                    p.release();
                }
                assert_eq!(p.hits(), 1);
                let expected = vec![
                    (0, 0),
                    (1, 1),
                    (2, i64::from(backend == Backend::Doris || normalized)),
                    (3, 0),
                ];
                assert_eq!(rows(&db, &table).await, expected);
                assert_eq!(p.hits(), 1);
                common::drop_table(&db, &table).await;
                db.close().await;
            }
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris; uses native SQLx Json bindings in and \
    outside transactions"]
async fn native_json_binding_keeps_sql_null_json_null_and_missing_paths_separate() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        let schema = if backend == Backend::MySql {
            "(id BIGINT PRIMARY KEY,j JSON)"
        } else {
            "(id BIGINT,j JSON) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 \
                PROPERTIES('replication_num'='1')"
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}{schema}")))
            .await
            .unwrap();
        let inputs = [
            None,
            Some(Json(serde_json::Value::Null)),
            Some(Json(serde_json::json!({
                "a": null,
                "b": [],
                "c": {},
                "n": u64::MAX
            }))),
            Some(Json(serde_json::json!("null"))),
        ];
        for outcome in 0..3 {
            let mut tx = db.pool().begin().await.unwrap();
            for (id, input) in inputs.iter().enumerate() {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table}(id,j) VALUES(?,?)"
                )))
                .bind(id as i64)
                .bind(input)
                .execute(&mut *tx)
                .await
                .unwrap();
            }
            match outcome {
                0 => tx.rollback().await.unwrap(),
                1 => drop(tx),
                _ => tx.commit().await.unwrap(),
            }
            let actual: Vec<Option<Json<serde_json::Value>>> = sqlx::query_scalar(
                sqlx::AssertSqlSafe(format!("SELECT j FROM {table} ORDER BY id")),
            )
            .fetch_all(db.pool())
            .await
            .unwrap();
            if outcome < 2 {
                assert!(actual.is_empty());
            } else {
                assert_eq!(actual, inputs);
            }
        }
        let distinct: Vec<(i64, i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id,CAST(j IS NULL AS SIGNED),CAST(JSON_EXTRACT(j,'$.missing') IS \
            NULL AS SIGNED) FROM {table} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(distinct, [(0, 1, 1), (1, 0, 1), (2, 0, 1), (3, 0, 1)]);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris; UTC decoding does not override server \
    session interpretation"]
async fn utc_chrono_binding_and_wall_time_follow_the_explicit_session_zone() {
    let value = NaiveDate::from_ymd_opt(2026, 10, 26)
        .unwrap()
        .and_hms_micro_opt(12, 34, 56, 123456)
        .unwrap()
        .and_utc();
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        let schema = if backend == Backend::MySql {
            "(id BIGINT PRIMARY KEY,at_time DATETIME(6))"
        } else {
            "(id BIGINT,at_time DATETIME(6)) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) \
                BUCKETS 1 PROPERTIES('replication_num'='1')"
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}{schema}")))
            .await
            .unwrap();
        let mut connection = db.pool().acquire().await.unwrap();
        for (id, zone, offset) in [(0, "+00:00", 0_i64), (1, "+08:00", 28800_i64)] {
            sqlx::query(sqlx::AssertSqlSafe(format!("SET time_zone='{zone}'")))
                .execute(&mut *connection)
                .await
                .unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES(?,?)"
            )))
            .bind(id)
            .bind(value)
            .execute(&mut *connection)
            .await
            .unwrap();
            let (decoded, seconds): (DateTime<Utc>, i64) =
                sqlx::query_as(sqlx::AssertSqlSafe(format!(
                    "SELECT at_time,CAST(UNIX_TIMESTAMP(at_time) AS SIGNED) FROM {table} WHERE id=?"
                )))
                .bind(id)
                .fetch_one(&mut *connection)
                .await
                .unwrap();
            assert_eq!(decoded, value);
            // Use a whole second oracle: the explicit server zone affects
            // interpreting a zone-less DATETIME, although Rust labels UTC.
            assert_eq!(seconds, value.timestamp() - offset);
        }
        sqlx::query("SET time_zone='+00:00'")
            .execute(&mut *connection)
            .await
            .unwrap();
        drop(connection);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris; checks IEEE finite values across \
    prepared and typed-text binding"]
async fn finite_float_bindings_keep_negative_zero_and_boundary_bits_across_paths() {
    let values = [
        (Some(-0.0_f32), Some(-0.0_f64)),
        (Some(0.0), Some(0.0)),
        (Some(f32::MIN_POSITIVE), Some(f64::MIN_POSITIVE)),
        (Some(f32::MAX), Some(f64::MAX)),
        (Some(f32::from_bits(1)), Some(f64::from_bits(1))),
        (None, None),
    ];
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        let schema = if backend == Backend::MySql {
            "(id BIGINT PRIMARY KEY,a FLOAT,b DOUBLE)"
        } else {
            "(id BIGINT,a FLOAT,b DOUBLE) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS \
                1 PROPERTIES('replication_num'='1')"
        };
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}{schema}")))
            .await
            .unwrap();
        for outcome in 0..3 {
            let mut tx = db.pool().begin().await.unwrap();
            for (id, (a, b)) in values.iter().enumerate() {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES(?,?,?)"
                )))
                .bind(id as i64)
                .bind(a)
                .bind(b)
                .execute(&mut *tx)
                .await
                .unwrap();
            }
            match outcome {
                0 => tx.rollback().await.unwrap(),
                1 => drop(tx),
                _ => tx.commit().await.unwrap(),
            }
            let stored: Vec<(Option<f32>, Option<f64>)> = sqlx::query_as(sqlx::AssertSqlSafe(
                format!("SELECT a,b FROM {table} ORDER BY id"),
            ))
            .fetch_all(db.pool())
            .await
            .unwrap();
            if outcome < 2 {
                assert!(stored.is_empty());
                continue;
            }
            for ((a, b), (expected_a, expected_b)) in stored.into_iter().zip(values) {
                assert_eq!(
                    a.map(f32::to_bits),
                    expected_a.map(f32::to_bits),
                    "{backend:?} f32"
                );
                assert_eq!(
                    b.map(f64::to_bits),
                    expected_b.map(f64::to_bits),
                    "{backend:?} f64"
                );
            }
        }
        for persistent in [true, true, false] {
            for (a, b) in values {
                // A direct prepared expression is the backend-native CAST
                // oracle. Doris folds FLOAT constants through their decimal
                // string; this is distinct from Rust's exact f32 -> f64 cast.
                let native: (Option<f64>, Option<f64>) =
                    sqlx::query_as("SELECT CAST(? AS DOUBLE),CAST(? AS DOUBLE)")
                        .bind(a)
                        .bind(b)
                        .persistent(persistent)
                        .fetch_one(db.pool())
                        .await
                        .unwrap();
                let actual: (Option<f64>, Option<f64>) =
                    sqlx::query_as("SELECT CAST(? AS DOUBLE),CAST(? AS DOUBLE) LIMIT ?")
                        .bind(a)
                        .bind(b)
                        .bind(1_i64)
                        .persistent(persistent)
                        .fetch_one(db.pool())
                        .await
                        .unwrap();
                assert_eq!(
                    actual.0.map(f64::to_bits),
                    native.0.map(f64::to_bits),
                    "{backend:?} f32 projection retains native CAST"
                );
                assert_eq!(
                    actual.1.map(f64::to_bits),
                    native.1.map(f64::to_bits),
                    "{backend:?} f64 projection retains native CAST"
                );
                if backend == Backend::MySql {
                    assert_eq!(native.0.map(f64::to_bits), a.map(|v| (v as f64).to_bits()));
                }
                assert_eq!(native.1.map(f64::to_bits), b.map(f64::to_bits));
            }
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}
