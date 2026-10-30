mod common;

use serde::{Deserialize, Serialize};
use sqlx_doris::{
    Backend, PoolBuilder,
    operations::{
        Aggregate, AggregateArgument, Column, DataType as T, TableDefinition, TableModel,
        push_complex_value, push_variant_path,
    },
    sqlx::{self, Executor, MySql, QueryBuilder},
    types::{Array, BigDecimal, Map, Struct, Variant},
};
use std::{env, net::Ipv6Addr, str::FromStr};
type MergedGroup = (i64, Option<String>, i64, Option<f64>, i64);

#[tokio::test]
#[ignore = "requires Doris"]
async fn variant_template_and_dynamic_paths_survive_separate_loads_type_changes_and_sparse_budget()
{
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Duplicate, ["id"])
            .column(Column::new("id", T::BigInt).not_null())
            .column(Column::new(
                "v",
                T::VariantSchema {
                    paths: vec![("fixed".into(), T::Int)],
                    max_subcolumns: 2,
                },
            ))
            .replication(1),
    )
    .await
    .unwrap();
    let values = [
        serde_json::json!({ "fixed": 1, "x": 1, "nested": { "text": "a'\\\n\0中文" } }),
        serde_json::json!({ "fixed": "2", "x": 2.5, "new": 17 }),
        serde_json::json!({ "fixed": 3, "x": "123", "other": [1, 2] }),
        serde_json::json!({ "fixed": 4, "x": [1, "a", true], "tail": 8 }),
    ];
    for (id, value) in values.iter().enumerate() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,CAST(? AS JSON))"
        )))
        .bind(id as i64)
        .bind(Variant(value))
        .execute(db.pool())
        .await
        .unwrap();
        // Re-read every earlier row after each separate persisted schema addition.
        let mut select = QueryBuilder::<MySql>::new("SELECT id,");
        push_variant_path(&mut select, "v", &["fixed"], &T::Int).unwrap();
        select.push(",");
        push_variant_path(&mut select, "v", &["x"], &T::Json).unwrap();
        select.push(format!(" FROM {table} ORDER BY id"));
        let rows: Vec<(i64, i32, Variant)> =
            select.build_query_as().fetch_all(db.pool()).await.unwrap();
        for (i, (actual_id, fixed, x)) in rows.iter().enumerate() {
            assert_eq!(*actual_id, i as i64);
            assert_eq!(*fixed, i as i32 + 1);
            assert_eq!(x.0, values[i]["x"]);
        }
        assert_eq!(rows.len(), id + 1);
    }
    let mut select = QueryBuilder::<MySql>::new("SELECT ");
    push_variant_path(&mut select, "v", &["nested", "text"], &T::String).unwrap();
    select.push(",");
    push_variant_path(&mut select, "v", &["missing"], &T::Json).unwrap();
    select
        .push(format!(" FROM {table} WHERE id="))
        .push_bind(0_i64);
    let (text, missing): (String, Option<Variant>) =
        select.build_query_as().fetch_one(db.pool()).await.unwrap();
    assert_eq!(text, values[0]["nested"]["text"].as_str().unwrap());
    assert_eq!(missing, None);
    let metadata = db.describe_table(&table).await.unwrap();
    assert!(metadata[1].data_type.contains("fixed"));
    assert!(
        metadata[1]
            .data_type
            .contains("variant_max_subcolumns_count")
    );
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn variant_json_projection_preserves_scalar_roots_and_binds_special_path_components() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Duplicate, ["id"])
            .column(Column::new("id", T::BigInt))
            .column(Column::new("v", T::Variant))
            .replication(1),
    )
    .await
    .unwrap();
    let special = "quote'\\\n中文";
    let values = [
        serde_json::json!("123"),
        serde_json::json!(123),
        serde_json::json!("1.25"),
        serde_json::json!(1.25),
        serde_json::json!("null"),
        serde_json::json!({ special: "payload" }),
        serde_json::json!(true),
        serde_json::json!(false),
    ];
    for (id, v) in values.iter().enumerate() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,CAST(? AS JSON))"
        )))
        .bind(id as i64)
        .bind(Variant(v))
        .execute(db.pool())
        .await
        .unwrap();
    }
    let mut select = QueryBuilder::<MySql>::new("SELECT ");
    push_variant_path(&mut select, "v", &[], &T::Json).unwrap();
    select.push(format!(" FROM {table} ORDER BY id"));
    let actual: Vec<Variant> = select
        .build_query_scalar()
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert_eq!(actual.into_iter().map(|v| v.0).collect::<Vec<_>>(), values);
    for mode in ["SET sql_mode=''", "SET sql_mode='NO_BACKSLASH_ESCAPES'"] {
        let mut conn = db.pool().acquire().await.unwrap();
        sqlx::raw_sql(mode).execute(&mut *conn).await.unwrap();
        let mut select = QueryBuilder::<MySql>::new("SELECT ");
        push_variant_path(&mut select, "v", &[special], &T::String).unwrap();
        select.push(format!(" FROM {table} WHERE id=5"));
        assert!(!select.sql().as_str().contains(special));
        let actual: String = select
            .build_query_scalar()
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(actual, "payload");
    }
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris; records dotted-key normalization"]
async fn variant_dotted_keys_are_server_paths_while_json_preserves_literal_keys() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Duplicate, ["id"])
            .column(Column::new("id", T::BigInt))
            .column(Column::new("v", T::Variant))
            .column(Column::new("j", T::Json))
            .replication(1),
    )
    .await
    .unwrap();
    let value = Variant(serde_json::json!({ "a.b": { "q": "value" } }));
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?,CAST(? AS JSON),CAST(? AS JSON))"
    )))
    .bind(1_i64)
    .bind(&value)
    .bind(&value)
    .execute(db.pool())
    .await
    .unwrap();
    let (v, j): (Variant, Variant) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT CAST(v AS JSON),j FROM {table}"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(j, value);
    assert_eq!(v.0, serde_json::json!({"a":{"b":{"q":"value"}}}));
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Nested {
    amounts: Map<i64, Array<Option<BigDecimal>>>,
    ip: Ipv6Addr,
    label: String,
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn nested_numeric_maps_decimal76_arrays_and_ip_values_persist_without_float_conversion() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let kind = T::array(T::Struct(vec![
        (
            "amounts".into(),
            T::map(
                T::BigInt,
                T::array(T::Decimal {
                    precision: 76,
                    scale: 38,
                }),
            ),
        ),
        ("ip".into(), T::Ipv6),
        ("label".into(), T::String),
    ]));
    let schema = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("records", kind.clone()))
        .replication(1);
    db.create_table(&schema).await.unwrap();
    let expected = Array(vec![
        None,
        Some(Struct(Nested {
            amounts: Map(vec![
                (
                    i64::MIN,
                    Array(vec![
                        Some(BigDecimal::from_str(
                            "99999999999999999999999999999999999999.12345678901234567890123456789012345678"
                        ).unwrap()),
                        None
                    ])
                ),
                (i64::MAX, Array(vec![]))
            ]),
            ip: Ipv6Addr::from_str("2001:db8::1").unwrap(),
            label: "quote'\\\0中文".into()
        }))
    ]);
    let mut insert = QueryBuilder::<MySql>::new(format!("INSERT INTO {table} VALUES ("));
    insert.push_bind(1_i64).push(",");
    push_complex_value(&mut insert, &kind, &expected).unwrap();
    insert.push(")").build().execute(db.pool()).await.unwrap();
    let mut select = schema.select_builder().unwrap();
    let (id, actual): (i64, Array<Option<Struct<Nested>>>) =
        select.build_query_as().fetch_one(db.pool()).await.unwrap();
    assert_eq!(id, 1);
    assert_eq!(actual, expected);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn sum_count_avg_states_merge_null_inputs_empty_sets_and_union_of_stored_groups() {
    let db = PoolBuilder::new(Backend::Doris)
        .max_connections(2)
        .after_connect(|c| {
            Box::pin(async move {
                c.execute("SET enable_agg_state=true").await?;
                Ok(())
            })
        })
        .connect(&env::var("DORIS_TEST_URL").unwrap())
        .await
        .unwrap();
    let rejected = T::AggState {
        function: "count".into(),
        arguments: vec![AggregateArgument::new(T::Int)],
    };
    let bad = TableDefinition::new(common::name(), TableModel::Aggregate, ["id"])
        .column(Column::new("id", T::BigInt))
        .column(Column::new("c", rejected).aggregate(Aggregate::Generic))
        .replication(1);
    assert!(matches!(
        db.create_table(&bad).await,
        Err(sqlx_doris::Error::Unsupported(_))
    ));
    let table = common::name();
    let mut schema = TableDefinition::new(table.clone(), TableModel::Aggregate, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .replication(1);
    for (column, function, nullable) in [
        ("s", "sum", true),
        ("c", "sum", false),
        ("a", "avg", true),
        ("r", "count", false),
    ] {
        schema = schema.column(
            Column::new(
                column,
                T::AggState {
                    function: function.into(),
                    arguments: vec![AggregateArgument {
                        data_type: T::Int,
                        nullable,
                    }],
                },
            )
            .aggregate(Aggregate::Generic),
        );
    }
    db.create_table(&schema).await.unwrap();
    for (id, value) in [(1_i64, Some(2_i32)), (1, Some(4)), (1, None), (2, None)] {
        // Explicit 0/1 SUM preserves COUNT(value), including all-NULL groups.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,SUM_STATE(CAST(? AS INT)),SUM_STATE(CAST(? AS \
            INT)),AVG_STATE(CAST(? AS INT)),COUNT_STATE(CAST(1 AS INT)))"
        )))
        .bind(id)
        .bind(value)
        .bind(i32::from(value.is_some()))
        .bind(value)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let rows: Vec<MergedGroup> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,CAST(SUM_MERGE(s) AS STRING),COALESCE(SUM_MERGE(c),0),\
            AVG_MERGE(a),COUNT_MERGE(r) FROM {table} GROUP BY id ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        rows,
        [
            (1, Some("6".into()), 2, Some(3.0), 3),
            (2, None, 0, None, 1)
        ]
    );
    let union: (Option<String>, i64, Option<f64>, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT CAST(SUM_MERGE(s) AS STRING),COALESCE(SUM_MERGE(c),0),AVG_MERGE(a),\
            COUNT_MERGE(r) FROM (SELECT SUM_UNION(s) s,SUM_UNION(c) c,AVG_UNION(a) a,\
            COUNT_UNION(r) r FROM {table} GROUP BY id) t"
        )))
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(union, (Some("6".into()), 2, Some(3.0), 4));
    let empty: (Option<String>, i64, Option<f64>, i64) =
        sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT CAST(SUM_MERGE(s) AS STRING),COALESCE(SUM_MERGE(c),0),AVG_MERGE(a),\
            COUNT_MERGE(r) FROM {table} WHERE id=999"
        )))
        .fetch_one(db.pool())
        .await
        .unwrap();
    assert_eq!(empty, (None, 0, None, 0));
    let mismatch = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (3,MAX_STATE(CAST(7 AS INT)),SUM_STATE(CAST(1 AS \
        INT)),AVG_STATE(CAST(7 AS INT)),COUNT_STATE(CAST(1 AS INT)))"
    )))
    .execute(db.pool())
    .await
    .unwrap_err();
    // The short PrepareOk is decoded and the server's actual type error survives.
    assert!(matches!(mismatch, sqlx::Error::Database(_)), "{mismatch:?}");
    let text_error = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (3,MAX_STATE(CAST(7 AS INT)),SUM_STATE(CAST(1 AS \
        INT)),AVG_STATE(CAST(7 AS INT)),COUNT_STATE(CAST(1 AS INT)))"
    )))
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(
        matches!(text_error, sqlx::Error::Database(_)),
        "{text_error:?}"
    );
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {table} WHERE id=3"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(n, 0);
    common::drop_table(&db, &table).await;
    db.close().await;
}
