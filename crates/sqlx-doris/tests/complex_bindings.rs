mod common;

use serde::{Deserialize, Serialize};
use sqlx_doris::{
    Backend,
    operations::{Column, DataType as T, TableDefinition, TableModel, push_complex_value},
    sqlx::{self, MySql, QueryBuilder},
    types::{Array, BigDecimal, LargeInt, Map, Struct},
};
use std::str::FromStr;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct Record {
    #[serde(rename = "label'中文")]
    label: String,
    attrs: Map<String, Array<Option<String>>>,
    wide: LargeInt,
    amount: BigDecimal,
    flags: Array<Option<bool>>,
}
type Records = Option<Array<Option<Struct<Record>>>>;

#[tokio::test]
#[ignore = "requires Doris"]
async fn bound_constructors_and_json_text_projection_preserve_nested_values_in_both_sql_modes() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let kind = T::array(T::Struct(vec![
        ("label'中文".into(), T::String),
        ("attrs".into(), T::map(T::String, T::array(T::String))),
        ("wide".into(), T::LargeInt),
        (
            "amount".into(),
            T::Decimal {
                precision: 76,
                scale: 30,
            },
        ),
        ("flags".into(), T::array(T::Boolean)),
    ]));
    let definition = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("records", kind.clone()))
        .replication(1);
    db.create_table(&definition).await.unwrap();
    let special = "'\"\\ [,{}] ? /* data */ ; DROP TABLE anything; --\r\n\t\0tail\0中文🙂";
    let values = vec![
        None,
        Some(Array(vec![])),
        Some(Array(vec![
            None,
            Some(Struct(Record {
                label: special.into(),
                attrs: Map(vec![
                    (
                        special.into(),
                        Array(vec![None, Some(special.into()), Some("".into())]),
                    ),
                    ("empty".into(), Array(vec![])),
                ]),
                wide: LargeInt(i128::MAX),
                amount: BigDecimal::from_str(
                    "1234567890123456789012345678901234567890123456.123456789012345678901234567890",
                )
                .unwrap(),
                flags: Array(vec![Some(true), Some(false), None]),
            })),
            Some(Struct(Record {
                label: String::new(),
                attrs: Map(vec![]),
                wide: LargeInt(i128::MIN),
                amount: BigDecimal::from_str("-0.000000000000000000000000000001").unwrap(),
                flags: Array(vec![]),
            })),
        ])),
    ];
    let mut connection = db.pool().acquire().await.unwrap();
    for mode in ["SET sql_mode=''", "SET sql_mode='NO_BACKSLASH_ESCAPES'"] {
        sqlx::raw_sql(mode).execute(&mut *connection).await.unwrap();
        for (id, value) in values.iter().enumerate() {
            let mut insert = QueryBuilder::<MySql>::new(format!("INSERT INTO {table} VALUES ("));
            insert.push_bind(id as i64).push(",");
            push_complex_value(&mut insert, &kind, value).unwrap();
            assert!(
                !insert.sql().as_str().contains(special),
                "business data must stay bound"
            );
            insert
                .push(")")
                .build()
                .execute(&mut *connection)
                .await
                .unwrap();
        }
        let mut select = definition.select_builder().unwrap();
        select.push(" ORDER BY id");
        let rows: Vec<(i64, Records)> = select
            .build_query_as()
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        assert_eq!(rows.len(), values.len());
        for ((_, actual), expected) in rows.into_iter().zip(&values) {
            assert_eq!(actual.as_ref(), expected.as_ref(), "{mode}");
        }
    }
    drop(connection);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn bound_map_constructors_preserve_numeric_keys_and_follow_duplicate_key_semantics() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let kind = T::map(T::LargeInt, T::array(T::String));
    let definition = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("attrs", kind.clone()))
        .replication(1);
    db.create_table(&definition).await.unwrap();
    let value = Map(vec![
        (LargeInt(i128::MAX), Array(vec![Some("first".to_owned())])),
        (
            LargeInt(i128::MAX),
            Array(vec![None, Some("last\0\"\\".to_owned())]),
        ),
        (LargeInt(i128::MIN), Array(vec![])),
    ]);
    let mut insert = QueryBuilder::<MySql>::new(format!("INSERT INTO {table} VALUES (1,"));
    push_complex_value(&mut insert, &kind, &value).unwrap();
    insert.push(")").build().execute(db.pool()).await.unwrap();
    let mut select = definition.select_builder().unwrap();
    let (_, actual): (i64, Map<LargeInt, Array<Option<String>>>) =
        select.build_query_as().fetch_one(db.pool()).await.unwrap();
    assert_eq!(actual, Map(value.0[1..].to_vec()));
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[test]
fn constructor_encoding_rejects_non_finite_values_shape_errors_and_opaque_leaves() {
    let mut query = QueryBuilder::<MySql>::new("SELECT ");
    assert!(push_complex_value(&mut query, &T::array(T::Double), &Array(vec![f64::NAN])).is_err());
    assert_eq!(
        query.sql(),
        "SELECT ",
        "serialization fails before touching SQL"
    );
    let mut query = QueryBuilder::<MySql>::new("SELECT ");
    assert!(
        push_complex_value(
            &mut query,
            &T::array(T::Json),
            &Array(vec![serde_json::json!({"x":1})])
        )
        .is_err()
    );
    let mut query = QueryBuilder::<MySql>::new("SELECT ");
    assert!(
        push_complex_value(
            &mut query,
            &T::Struct(vec![("x".into(), T::Int)]),
            &serde_json::json!({"y":1})
        )
        .is_err()
    );
    let mut query = QueryBuilder::<MySql>::new("SELECT ");
    assert!(
        push_complex_value(&mut query, &T::array(T::Int), &serde_json::json!({"x":1})).is_err()
    );
}
