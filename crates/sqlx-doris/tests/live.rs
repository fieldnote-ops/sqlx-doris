mod common;

use chrono::NaiveDateTime;
use futures_util::TryStreamExt;
use serde::{Deserialize, Serialize};
use sqlx_doris::{
    Backend, GroupCommit, PoolBuilder,
    operations::{self, Column, DataType as T, TableDefinition, TableModel},
    sqlx::{self, Executor, Row},
    types::{
        Array, BigDecimal, Binary, Hex, Ipv4, Ipv6, LargeInt, Map, Struct, TimestampTz, Variant,
    },
};
use std::{str::FromStr, time::Duration};

#[derive(Debug, ::sqlx::FromRow, PartialEq)]
struct SharedRow {
    id: i64,
    name: String,
    amount: BigDecimal,
    wide: LargeInt,
    created: NaiveDateTime,
    meta: Variant,
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn same_queries_bindings_and_derived_row_work_on_both_backends() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        if backend == Backend::MySql {
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {table} (id BIGINT PRIMARY KEY,name TEXT,amount DECIMAL(38,18),\
                wide DECIMAL(39,0),created DATETIME(6),meta JSON)"
                )))
                .await
                .unwrap();
        } else {
            db.create_table(
                &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                    .column(Column::new("id", T::BigInt).not_null())
                    .column(Column::new("name", T::String))
                    .column(Column::new(
                        "amount",
                        T::Decimal {
                            precision: 38,
                            scale: 18,
                        },
                    ))
                    .column(Column::new("wide", T::LargeInt))
                    .column(Column::new("created", T::DateTime(6)))
                    .column(Column::new("meta", T::Json))
                    .replication(1),
            )
            .await
            .unwrap();
        }
        let expected = SharedRow {
            id: 1,
            name: "中文🙂 ' \\ ?".into(),
            amount: BigDecimal::from_str("12345678901234567890.123456789012345678").unwrap(),
            wide: LargeInt(i128::MAX),
            created: NaiveDateTime::parse_from_str(
                "2026-10-26 12:34:56.123456",
                "%Y-%m-%d %H:%M:%S%.f",
            )
            .unwrap(),
            meta: Variant(serde_json::json!({ "nested": [1, null], "value": 42 })),
        };
        // Identifiers come only from the generated fixture name; every data value binds.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?,?,?,?,?)"
        )))
        .bind(expected.id)
        .bind(&expected.name)
        .bind(&expected.amount)
        .bind(expected.wide)
        .bind(expected.created)
        .bind(&expected.meta)
        .execute(db.pool())
        .await
        .unwrap();
        let actual: SharedRow = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT * FROM {table} WHERE id=?"
        )))
        .bind(1_i64)
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(actual, expected, "{backend:?}");
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct Record {
    name: String,
    points: Vec<Option<i32>>,
}
#[derive(Debug, ::sqlx::FromRow, PartialEq)]
struct NativeRow {
    id: i64,
    large: Option<LargeInt>,
    amount: Option<BigDecimal>,
    numbers: Option<Array<Option<LargeInt>>>,
    attrs: Option<Map<i32, Array<Option<String>>>>,
    record_value: Option<Struct<Record>>,
    variant: Option<Variant>,
    tz: Option<TimestampTz>,
    ip4: Option<Ipv4>,
    ip6: Option<Ipv6>,
    payload: Option<Binary>,
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn native_types_persist_and_round_trip_with_nulls_precision_and_nested_values() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
            .column(Column::new("id", T::BigInt).not_null())
            .column(Column::new("large", T::LargeInt))
            .column(Column::new(
                "amount",
                T::Decimal {
                    precision: 76,
                    scale: 30,
                },
            ))
            .column(Column::new("numbers", T::array(T::LargeInt)))
            .column(Column::new("attrs", T::map(T::Int, T::array(T::String))))
            .column(Column::new(
                "record_value",
                T::Struct(vec![
                    ("name".into(), T::String),
                    ("points".into(), T::array(T::Int)),
                ]),
            ))
            .column(Column::new("variant", T::Variant))
            .column(Column::new("tz", T::TimestampTz(6)))
            .column(Column::new("ip4", T::Ipv4))
            .column(Column::new("ip6", T::Ipv6))
            .column(Column::new("payload_hex", T::String))
            .replication(1),
    )
    .await
    .unwrap();
    let expected = NativeRow {
        id: 1,
        large: Some(LargeInt(i128::MAX)),
        amount: Some(
            BigDecimal::from_str(
                "9999999999999999999999999999999999999999999999.123456789012345678901234567890",
            )
            .unwrap(),
        ),
        numbers: Some(Array(vec![
            Some(LargeInt(i128::MAX)),
            None,
            Some(LargeInt(-9)),
        ])),
        attrs: Some(Map(vec![
            (1, Array(vec![Some("中文🙂 \" \\ \n \0".into()), None])),
            (2, Array(vec![])),
        ])),
        record_value: Some(Struct(Record {
            name: "Ada \"中文\"".into(),
            points: vec![Some(1), None, Some(3)],
        })),
        variant: Some(Variant(
            serde_json::json!({ "large": u64::MAX, "nested": [1, null] }),
        )),
        tz: Some(TimestampTz::parse("2026-10-26 10:20:30.123456+08:00").unwrap()),
        ip4: Some(Ipv4("192.0.2.1".parse().unwrap())),
        ip6: Some(Ipv6("2001:db8::1".parse().unwrap())),
        payload: Some(Binary(vec![0, 97, 255, 0])),
    };
    let insert = format!(
        "INSERT INTO {table} VALUES (?,?,?,CAST(? AS ARRAY<LARGEINT>),CAST(? AS \
            MAP<INT,ARRAY<STRING>>),CAST(? AS STRUCT<name:STRING,points:ARRAY<INT>>),\
            CAST(? AS JSON),CAST(? AS TIMESTAMPTZ(6)),CAST(? AS IPV4),CAST(? AS IPV6),?)"
    );
    sqlx::query(sqlx::AssertSqlSafe(insert.clone()))
        .bind(expected.id)
        .bind(expected.large)
        .bind(&expected.amount)
        .bind(&expected.numbers)
        .bind(&expected.attrs)
        .bind(&expected.record_value)
        .bind(&expected.variant)
        .bind(&expected.tz)
        .bind(expected.ip4)
        .bind(expected.ip6)
        .bind(expected.payload.as_ref().map(Binary::as_hex))
        .execute(db.pool())
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(insert))
        .bind(2_i64)
        .bind(None::<LargeInt>)
        .bind(None::<BigDecimal>)
        .bind(None::<Array<Option<LargeInt>>>)
        .bind(None::<Map<i32, Array<Option<String>>>>)
        .bind(None::<Struct<Record>>)
        .bind(None::<Variant>)
        .bind(None::<TimestampTz>)
        .bind(None::<Ipv4>)
        .bind(None::<Ipv6>)
        .bind(None::<Hex<'_>>)
        .execute(db.pool())
        .await
        .unwrap();
    let numbers = operations::project("numbers", &T::array(T::LargeInt)).unwrap();
    let attrs = operations::project("attrs", &T::map(T::Int, T::array(T::String))).unwrap();
    let record = operations::project(
        "record_value",
        &T::Struct(vec![
            ("name".into(), T::String),
            ("points".into(), T::array(T::Int)),
        ]),
    )
    .unwrap();
    let tz = operations::project("tz", &T::TimestampTz(6)).unwrap();
    let mut rows: Vec<NativeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,large,amount,{numbers},{attrs},{record},variant,{tz},ip4,ip6,\
        UNHEX(payload_hex) AS payload FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    let actual = rows.remove(0);
    assert_eq!(actual, expected);
    let native_large: LargeInt = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT CAST(variant['large'] AS LARGEINT) FROM {table} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(
        native_large,
        LargeInt(i128::from(u64::MAX)),
        "verify the stored VARIANT is a native object, not JSON-looking text"
    );
    let native_type: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT VARIANT_TYPE(variant) FROM {table} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert!(
        serde_json::from_str::<serde_json::Value>(&native_type)
            .unwrap()
            .get("large")
            .is_some()
    );
    assert_eq!(
        rows[0],
        NativeRow {
            id: 2,
            large: None,
            amount: None,
            numbers: None,
            attrs: None,
            record_value: None,
            variant: None,
            tz: None,
            ip4: None,
            ip6: None,
            payload: None
        }
    );
    let schema = db.describe_table(&table).await.unwrap();
    assert_eq!(schema.len(), 11);
    assert!(schema[0].is_key);
    assert!(!schema[0].nullable);
    assert!(
        schema
            .iter()
            .find(|c| c.name == "attrs")
            .unwrap()
            .data_type
            .to_ascii_lowercase()
            .contains("map")
    );
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn map_constructor_duplicate_keys_follow_the_servers_last_value_semantics() {
    let db = common::client(Backend::Doris).await;
    let value: Map<i32, i32> = sqlx::query_scalar("SELECT map(1,10,1,20)")
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(value.0, vec![(1, 20)]);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris; records a server-side VARIANT parser limitation"]
async fn variant_object_text_alone_does_not_prove_a_native_object() {
    let db = common::client(Backend::Doris).await;
    let value = Variant(serde_json::json!({ "n": 42 }));
    let cast_type: String = sqlx::query_scalar("SELECT VARIANT_TYPE(CAST(? AS VARIANT))")
        .bind(&value)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert!(matches!(
        serde_json::from_str::<serde_json::Value>(&cast_type).unwrap()[""].as_str(),
        Some("string" | "varchar")
    ));
    if common::doris_4_0_8() {
        let error = sqlx::query_scalar::<_, String>("SELECT VARIANT_TYPE(PARSE_TO_VARIANT(?))")
            .bind(&value)
            .fetch_one(db.pool())
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Can not found function 'PARSE_TO_VARIANT'"),
            "{error:?}"
        );
        db.close().await;
        return;
    }
    let parsed_type: String = sqlx::query_scalar("SELECT VARIANT_TYPE(PARSE_TO_VARIANT(?))")
        .bind(&value)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&parsed_type).unwrap()["n"],
        "bigint"
    );
    let wide = Variant(serde_json::json!({
        "n": serde_json::from_str::<serde_json::Value>("170141183460469231731687303715884105727")
                .unwrap()
    }));
    let fallback_type: String = sqlx::query_scalar("SELECT VARIANT_TYPE(PARSE_TO_VARIANT(?))")
        .bind(&wide)
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&fallback_type).unwrap()[""],
        "string"
    );
    let field: Option<LargeInt> =
        sqlx::query_scalar("SELECT CAST(PARSE_TO_VARIANT(?)['n'] AS LARGEINT)")
            .bind(&wide)
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        field,
        Some(LargeInt(i128::MAX)),
        "subpath extraction alone does not prove a native object; compare VARIANT_TYPE above"
    );
    db.close().await;
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct NestedRecord {
    label: String,
    attrs: Map<String, Array<Option<String>>>,
    wide: LargeInt,
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn schema_projection_preserves_maps_inside_nullable_records_and_arrays() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let kind = T::array(T::Struct(vec![
        ("label".into(), T::String),
        ("attrs".into(), T::map(T::String, T::array(T::String))),
        ("wide".into(), T::LargeInt),
    ]));
    let definition = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("records", kind.clone()))
        .replication(1);
    db.create_table(&definition).await.unwrap();
    let records = Array(vec![
        None,
        Some(Struct(NestedRecord {
            label: "[\"quoted\",{null}] 中文🙂 \r\n\t\0".into(),
            attrs: Map(vec![
                (
                    "key\"\\\n\0".into(),
                    Array(vec![None, Some("value\"\\\n\0".into())]),
                ),
                ("empty".into(), Array(vec![])),
            ]),
            wide: LargeInt(i128::MAX),
        })),
        Some(Struct(NestedRecord {
            label: String::new(),
            attrs: Map(vec![]),
            wide: LargeInt(i128::MIN),
        })),
    ]);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?,CAST(? AS {kind}))"
    )))
    .bind(1_i64)
    .bind(&records)
    .execute(db.pool())
    .await
    .unwrap();
    let mut select = definition.select_builder().unwrap();
    select.push(" WHERE id=").push_bind(1_i64);
    let actual: (i64, Array<Option<Struct<NestedRecord>>>) =
        select.build_query_as().fetch_one(db.pool()).await.unwrap();
    assert_eq!(actual, (1, records));
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn per_connection_group_commit_and_decimal_settings_are_consistent() {
    let db = PoolBuilder::new(Backend::Doris)
        .max_connections(2)
        .group_commit(GroupCommit::Async)
        .connect(&std::env::var("DORIS_TEST_URL").unwrap())
        .await
        .unwrap();
    let mut one = db.pool().acquire().await.unwrap();
    let mut two = db.pool().acquire().await.unwrap();
    for c in [&mut one, &mut two] {
        let mode: String = sqlx::query_scalar("SELECT @@group_commit")
            .fetch_one(&mut **c)
            .await
            .unwrap();
        assert_eq!(mode, "async_mode");
        let enabled: i64 = sqlx::query_scalar("SELECT CAST(@@enable_decimal256 AS BIGINT)")
            .fetch_one(&mut **c)
            .await
            .unwrap();
        assert_eq!(enabled, 1);
    }
    drop(one);
    drop(two);
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn raw_transaction_commit_rollback_drop_and_cancellation_recover_pool_capacity() {
    let db = PoolBuilder::new(Backend::Doris)
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(10))
        .connect(&std::env::var("DORIS_TEST_URL").unwrap())
        .await
        .unwrap();
    let table = common::name();
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Duplicate, ["id"])
            .column(Column::new("id", T::BigInt).not_null())
            .replication(1),
    )
    .await
    .unwrap();
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (1)"
    )))
    .await
    .unwrap();
    tx.commit().await.unwrap();
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (2)"
    )))
    .await
    .unwrap();
    tx.rollback().await.unwrap();
    let mut tx = db.doris_transaction().await.unwrap();
    tx.execute(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (3)"
    )))
    .await
    .unwrap();
    drop(tx);
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    let pool = db.clone();
    let table_for_task = table.clone();
    let (ready, received) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let mut tx = pool.doris_transaction().await.unwrap();
        tx.execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table_for_task} VALUES (4)"
        )))
        .await
        .unwrap();
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    received.await.unwrap();
    task.abort();
    let _ = task.await;
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated databases"]
async fn mysql_transactions_and_partial_stream_consumption_still_work() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = PoolBuilder::new(backend)
            .max_connections(1)
            .connect(
                &std::env::var(if backend == Backend::MySql {
                    "MYSQL_TEST_URL"
                } else {
                    "DORIS_TEST_URL"
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let mut stream =
            sqlx::query("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3").fetch(db.pool());
        assert!(stream.try_next().await.unwrap().is_some());
        drop(stream);
        let row = sqlx::query("SELECT CAST(? AS SIGNED)")
            .bind(42_i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(row.try_get::<i64, _>(0).unwrap(), 42);
        db.pool().begin().await.unwrap().rollback().await.unwrap();
        assert!(db.capabilities().sqlx_transactions);
        db.close().await;
    }
}

#[tokio::test]
async fn visibility_wait_has_a_deadline_and_returns_query_errors() {
    let error = operations::wait_until(
        Duration::from_millis(5),
        Duration::from_millis(1),
        || async { Ok(false) },
    )
    .await
    .unwrap_err();
    assert!(matches!(error, sqlx_doris::Error::VisibilityTimeout));
    let error = operations::wait_until(Duration::from_secs(1), Duration::ZERO, || async {
        Err(sqlx::Error::RowNotFound)
    })
    .await
    .unwrap_err();
    assert!(matches!(
        error,
        sqlx_doris::Error::Database(sqlx::Error::RowNotFound)
    ));
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn sqlx_connect_options_and_application_hooks_survive_the_profile_wrapper() {
    for backend in [Backend::MySql, Backend::Doris] {
        let key = if backend == Backend::MySql {
            "MYSQL_TEST_URL"
        } else {
            "DORIS_TEST_URL"
        };
        let options = sqlx::mysql::MySqlConnectOptions::from_str(&std::env::var(key).unwrap())
            .unwrap()
            .statement_cache_capacity(0);
        let db = PoolBuilder::new(backend)
            .max_connections(2)
            .decimal256(false)
            .after_connect(move |connection| {
                Box::pin(async move {
                    if backend == Backend::Doris {
                        let enabled: i64 =
                            sqlx::query_scalar("SELECT CAST(@@enable_decimal256 AS SIGNED)")
                                .fetch_one(&mut *connection)
                                .await?;
                        assert_eq!(
                            enabled, 0,
                            "application hook runs after the explicit profile setting"
                        );
                    }
                    connection.execute("SET time_zone = '+08:00'").await?;
                    Ok(())
                })
            })
            .connect_with(options)
            .await
            .unwrap();
        let mut one = db.pool().acquire().await.unwrap();
        let mut two = db.pool().acquire().await.unwrap();
        for connection in [&mut one, &mut two] {
            let zone: String = sqlx::query_scalar("SELECT @@time_zone")
                .fetch_one(&mut **connection)
                .await
                .unwrap();
            assert_eq!(zone, "+08:00");
        }
        drop(one);
        drop(two);
        let wrapped = sqlx_doris::Client::from_pool(backend, db.pool().clone());
        assert_eq!(wrapped.backend(), backend);
        let pool: sqlx_doris::MySqlPool = wrapped.into_pool();
        let zone: String = sqlx::query_scalar("SELECT @@time_zone")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(zone, "+08:00");
        pool.close().await;
        assert!(db.pool().is_closed());
    }
}
