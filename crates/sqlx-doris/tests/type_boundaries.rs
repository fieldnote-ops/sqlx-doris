mod common;
use chrono::{NaiveDate, NaiveDateTime};
use sqlx_doris::{
    Backend, PoolBuilder,
    operations::{Column, DataType as T, TableDefinition, TableModel},
    sqlx::{self, Executor, FromRow},
    types::{Array, BigDecimal, Ipv6, LargeInt, Map, TimestampTz, Variant},
};
use std::{str::FromStr, time::Duration};

#[derive(Clone, Debug, FromRow, PartialEq)]
struct Scalars {
    id: i64,
    b: Option<bool>,
    tiny: Option<i8>,
    small: Option<i16>,
    n: Option<i32>,
    big: Option<i64>,
    f: Option<f32>,
    d: Option<f64>,
    name: Option<String>,
    fixed: Option<String>,
    day: Option<NaiveDate>,
    created: Option<NaiveDateTime>,
    j: Option<Variant>,
    amount: Option<BigDecimal>,
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn scalar_boundaries_and_nulls_round_trip_through_the_same_binding_and_dto() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        if backend == Backend::MySql {
            db.pool().execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table} (id BIGINT PRIMARY KEY,b BOOLEAN,tiny TINYINT,small SMALLINT,n INT,big BIGINT,f FLOAT,d DOUBLE,name VARCHAR(255),fixed CHAR(5),day DATE,created DATETIME(6),j JSON,amount DECIMAL(38,10))"))).await.unwrap();
        } else {
            let mut schema = TableDefinition::new(table.clone(), TableModel::Unique, ["id"]);
            for (name, kind) in [
                ("id", T::BigInt),
                ("b", T::Boolean),
                ("tiny", T::TinyInt),
                ("small", T::SmallInt),
                ("n", T::Int),
                ("big", T::BigInt),
                ("f", T::Float),
                ("d", T::Double),
                ("name", T::VarChar(255)),
                ("fixed", T::Char(5)),
                ("day", T::Date),
                ("created", T::DateTime(6)),
                ("j", T::Json),
                (
                    "amount",
                    T::Decimal {
                        precision: 38,
                        scale: 10,
                    },
                ),
            ] {
                schema = schema.column(if name == "id" {
                    Column::new(name, kind).not_null()
                } else {
                    Column::new(name, kind)
                });
            }
            db.create_table(&schema.replication(1)).await.unwrap();
        }
        let mut rows = vec![];
        for minimum in [true, false] {
            rows.push(Scalars {
                id: if minimum { 1 } else { 2 },
                b: Some(!minimum),
                tiny: Some(if minimum { i8::MIN } else { i8::MAX }),
                small: Some(if minimum { i16::MIN } else { i16::MAX }),
                n: Some(if minimum { i32::MIN } else { i32::MAX }),
                big: Some(if minimum { i64::MIN } else { i64::MAX }),
                f: Some(if minimum { f32::MIN_POSITIVE } else { f32::MAX }),
                d: Some(if minimum { f64::MIN_POSITIVE } else { f64::MAX }),
                name: Some("中文🙂 ' \\ ? \n \0".into()),
                fixed: Some("x y".into()),
                day: Some(
                    NaiveDate::from_ymd_opt(
                        if minimum { 1000 } else { 9999 },
                        if minimum { 1 } else { 12 },
                        if minimum { 1 } else { 31 },
                    )
                    .unwrap(),
                ),
                created: Some(
                    NaiveDateTime::parse_from_str(
                        if minimum {
                            "1000-01-01 00:00:00.000001"
                        } else {
                            "9999-12-31 23:59:59.999999"
                        },
                        "%Y-%m-%d %H:%M:%S%.f",
                    )
                    .unwrap(),
                ),
                j: Some(Variant(if minimum {
                    serde_json::Value::Null
                } else {
                    serde_json::json!({"n":u64::MAX,"a":[null]})
                })),
                amount: Some(
                    BigDecimal::from_str(if minimum {
                        "-9999999999999999999999999999.9999999999"
                    } else {
                        "9999999999999999999999999999.9999999999"
                    })
                    .unwrap(),
                ),
            });
        }
        rows.push(Scalars {
            id: 3,
            b: None,
            tiny: None,
            small: None,
            n: None,
            big: None,
            f: None,
            d: None,
            name: None,
            fixed: None,
            day: None,
            created: None,
            j: None,
            amount: None,
        });
        for row in &rows {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
            )))
            .bind(row.id)
            .bind(row.b)
            .bind(row.tiny)
            .bind(row.small)
            .bind(row.n)
            .bind(row.big)
            .bind(row.f)
            .bind(row.d)
            .bind(&row.name)
            .bind(&row.fixed)
            .bind(row.day)
            .bind(row.created)
            .bind(&row.j)
            .bind(&row.amount)
            .execute(db.pool())
            .await
            .unwrap();
        }
        let actual: Vec<Scalars> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT * FROM {table} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(actual, rows, "{backend:?}");
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn wide_integer_binding_narrow_decoding_and_server_overflow_are_explicit() {
    for backend in [Backend::MySql, Backend::Doris] {
        let key = if backend == Backend::MySql {
            "MYSQL_TEST_URL"
        } else {
            "DORIS_TEST_URL"
        };
        let db = PoolBuilder::new(backend)
            .max_connections(1)
            .acquire_timeout(Duration::from_secs(5))
            .connect(&std::env::var(key).unwrap())
            .await
            .unwrap();
        for value in [i128::MIN, i128::MAX, i128::from(u64::MAX)] {
            let actual: LargeInt = sqlx::query_scalar("SELECT CAST(? AS DECIMAL(39,0))")
                .bind(LargeInt(value))
                .fetch_one(db.pool())
                .await
                .unwrap();
            assert_eq!(actual, LargeInt(value));
        }
        let bad =
            sqlx::query_scalar::<_, LargeInt>("SELECT '170141183460469231731687303715884105728'")
                .fetch_one(db.pool())
                .await
                .unwrap_err();
        assert!(matches!(bad, sqlx::Error::ColumnDecode { .. }));
        let after: i64 = sqlx::query_scalar("SELECT CAST(? AS SIGNED)")
            .bind(42_i64)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(after, 42);
        let table = common::name();
        if backend == Backend::MySql {
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {table} (id BIGINT PRIMARY KEY,value DECIMAL(38,10))"
                )))
                .await
                .unwrap();
        } else {
            db.create_table(
                &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                    .column(Column::new("id", T::BigInt).not_null())
                    .column(Column::new(
                        "value",
                        T::Decimal {
                            precision: 38,
                            scale: 10,
                        },
                    ))
                    .replication(1),
            )
            .await
            .unwrap();
        }
        let error = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(1_i64)
        .bind(BigDecimal::from_str("100000000000000000000000000000000000000000000000000").unwrap())
        .execute(db.pool())
        .await
        .unwrap_err();
        assert!(matches!(error, sqlx::Error::Database(_)));
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(count, 0);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn unsigned_parameter_extremes_are_retained() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let actual: LargeInt = sqlx::query_scalar("SELECT CAST(? AS DECIMAL(39,0))")
            .bind(u64::MAX)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(actual, LargeInt(i128::from(u64::MAX)), "{backend:?}");
        let one: LargeInt = sqlx::query_scalar("SELECT CAST(? AS DECIMAL(39,0))")
            .bind(u8::MAX)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(one, LargeInt(i128::from(u8::MAX)));
        let two: LargeInt = sqlx::query_scalar("SELECT CAST(? AS DECIMAL(39,0))")
            .bind(u16::MAX)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(two, LargeInt(i128::from(u16::MAX)));
        let three: LargeInt = sqlx::query_scalar("SELECT CAST(? AS DECIMAL(39,0))")
            .bind(u32::MAX)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(three, LargeInt(i128::from(u32::MAX)));
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn native_floating_point_writes_preserve_finite_sign_and_refuse_or_retain_non_finite_values()
{
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        if backend == Backend::MySql {
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {table} (id BIGINT PRIMARY KEY,value DOUBLE)"
                )))
                .await
                .unwrap();
        } else {
            db.create_table(
                &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                    .column(Column::new("id", T::BigInt).not_null())
                    .column(Column::new("value", T::Double))
                    .replication(1),
            )
            .await
            .unwrap();
        }
        for (id, value) in [f64::MIN, -1.25, f64::NAN, f64::INFINITY, f64::NEG_INFINITY]
            .into_iter()
            .enumerate()
        {
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?)"
            )))
            .bind(id as i64)
            .bind(value)
            .execute(db.pool())
            .await;
            match result {
                Ok(_) => {
                    let actual: Option<f64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                        "SELECT value FROM {table} WHERE id=?"
                    )))
                    .bind(id as i64)
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
                    let actual =
                        actual.expect("a non-finite float must never silently become SQL NULL");
                    if value.is_nan() {
                        assert!(actual.is_nan());
                    } else {
                        assert_eq!(actual, value);
                    }
                    if !value.is_finite() {
                        eprintln!("{backend:?} retained native non-finite value {value}");
                    }
                }
                Err(error) => {
                    assert!(!value.is_finite(), "{backend:?}: finite {value}: {error}");
                    assert!(error.as_database_error().is_some(), "{error:?}");
                    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                        "SELECT COUNT(*) FROM {table} WHERE id=?"
                    )))
                    .bind(id as i64)
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
                    assert_eq!(count, 0);
                    eprintln!("{backend:?} explicitly refused native non-finite value {value}");
                }
            }
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn local_encoding_rejects_non_finite_values_and_submicrosecond_timestamps_without_writing() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Duplicate, ["id"])
            .column(Column::new("id", T::BigInt).not_null())
            .column(Column::new("values", T::array(T::Double)))
            .column(Column::new("at", T::TimestampTz(6)))
            .replication(1),
    )
    .await
    .unwrap();
    for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let error = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,CAST(? AS ARRAY<DOUBLE>),?)"
        )))
        .bind(1_i64)
        .bind(Array(vec![Some(number)]))
        .bind(None::<TimestampTz>)
        .execute(db.pool())
        .await
        .unwrap_err();
        assert!(error.to_string().contains("non-finite"));
    }
    let timestamp = TimestampTz::parse("2026-10-26T12:34:56.123456789Z").unwrap();
    let error = sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?,?,CAST(? AS TIMESTAMPTZ(6)))"
    )))
    .bind(1_i64)
    .bind(None::<Array<f64>>)
    .bind(timestamp)
    .execute(db.pool())
    .await
    .unwrap_err();
    assert!(error.to_string().contains("microsecond"));
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn decimal256_in_arrays_and_numeric_maps_preserves_sign_scale_and_nulls() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let decimal = T::Decimal {
        precision: 76,
        scale: 30,
    };
    let schema = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("values", T::array(decimal.clone())))
        .column(Column::new("mapped", T::map(T::Int, decimal.clone())))
        .replication(1);
    db.create_table(&schema).await.unwrap();
    let positive = BigDecimal::from_str(
        "9999999999999999999999999999999999999999999999.123456789012345678901234567890",
    )
    .unwrap();
    let negative = -positive.clone();
    let zero = BigDecimal::from_str("0.000000000000000000000000000001").unwrap();
    let values = Array(vec![
        Some(positive.clone()),
        Some(negative.clone()),
        None,
        Some(zero.clone()),
    ]);
    let mapped = Map(vec![(1, Some(negative)), (2, None), (3, Some(zero))]);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {table} VALUES (?,CAST(? AS ARRAY<{decimal}>),CAST(? AS MAP<INT,{decimal}>))"
    )))
    .bind(1_i64)
    .bind(&values)
    .bind(&mapped)
    .execute(db.pool())
    .await
    .unwrap();
    let mut select = schema.select_builder().unwrap();
    type DecimalRow = (i64, Array<Option<BigDecimal>>, Map<i32, Option<BigDecimal>>);
    let actual: DecimalRow = select.build_query_as().fetch_one(db.pool()).await.unwrap();
    assert_eq!(actual, (1, values, mapped));
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn strict_cast_choice_is_explicit_and_consistent_across_new_sessions() {
    for enabled in [true, false] {
        let db = PoolBuilder::new(Backend::Doris)
            .max_connections(2)
            .strict_cast(enabled)
            .connect(&std::env::var("DORIS_TEST_URL").unwrap())
            .await
            .unwrap();
        let mut one = db.pool().acquire().await.unwrap();
        let mut two = db.pool().acquire().await.unwrap();
        for connection in [&mut one, &mut two] {
            let setting: i64 = sqlx::query_scalar("SELECT CAST(@@enable_strict_cast AS SIGNED)")
                .fetch_one(&mut **connection)
                .await
                .unwrap();
            assert_eq!(setting, i64::from(enabled));
            let result: sqlx::Result<Option<i32>> =
                sqlx::query_scalar("SELECT CAST('invalid integer' AS INT)")
                    .fetch_one(&mut **connection)
                    .await;
            if enabled {
                assert!(result.is_err());
            } else {
                assert_eq!(result.unwrap(), None);
            }
        }
        drop(one);
        drop(two);
        db.close().await;
    }
}

type TemporalRow = (
    i64,
    Array<Option<bool>>,
    Array<Option<NaiveDate>>,
    Array<Option<NaiveDateTime>>,
    Array<Option<TimestampTz>>,
    Array<Option<Ipv6>>,
);
#[tokio::test]
#[ignore = "requires Doris"]
async fn boolean_date_timestamp_and_ip_collections_round_trip_in_a_non_utc_session() {
    let db = PoolBuilder::new(Backend::Doris)
        .after_connect(|connection| {
            Box::pin(async move {
                connection.execute("SET time_zone = '+05:30'").await?;
                Ok(())
            })
        })
        .connect(&std::env::var("DORIS_TEST_URL").unwrap())
        .await
        .unwrap();
    let table = common::name();
    let schema = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("bools", T::array(T::Boolean)))
        .column(Column::new("days", T::array(T::Date)))
        .column(Column::new("locals", T::array(T::DateTime(6))))
        .column(Column::new("instants", T::array(T::TimestampTz(6))))
        .column(Column::new("ips", T::array(T::Ipv6)))
        .replication(1);
    db.create_table(&schema).await.unwrap();
    let expected: TemporalRow = (
        1,
        Array(vec![Some(true), Some(false), None]),
        Array(vec![
            Some(NaiveDate::from_ymd_opt(1000, 1, 1).unwrap()),
            Some(NaiveDate::from_ymd_opt(9999, 12, 31).unwrap()),
            None,
        ]),
        Array(vec![
            Some(
                NaiveDateTime::parse_from_str("2026-10-26 12:34:56.123456", "%Y-%m-%d %H:%M:%S%.f")
                    .unwrap(),
            ),
            None,
        ]),
        Array(vec![
            Some(TimestampTz::parse("2026-11-01T01:30:00.123456-04:00").unwrap()),
            Some(TimestampTz::parse("2026-11-01T01:30:00.123456-05:00").unwrap()),
            None,
        ]),
        Array(vec![
            Some(Ipv6("2001:db8::1".parse().unwrap())),
            Some(Ipv6("::ffff:192.0.2.1".parse().unwrap())),
            None,
        ]),
    );
    sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO {table} VALUES (?,CAST(? AS ARRAY<BOOLEAN>),CAST(? AS ARRAY<DATE>),CAST(? AS ARRAY<DATETIME(6)>),CAST(? AS ARRAY<TIMESTAMPTZ(6)>),CAST(? AS ARRAY<IPV6>))"))).bind(expected.0).bind(&expected.1).bind(&expected.2).bind(&expected.3).bind(&expected.4).bind(&expected.5).execute(db.pool()).await.unwrap();
    let mut select = schema.select_builder().unwrap();
    let actual: TemporalRow = select.build_query_as().fetch_one(db.pool()).await.unwrap();
    assert_eq!(actual, expected);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn json_columns_preserve_root_kinds_and_distinguish_sql_null_from_json_null() {
    let inputs = vec![
        None,
        Some(Variant(serde_json::Value::Null)),
        Some(Variant(serde_json::json!(true))),
        Some(Variant(serde_json::json!(false))),
        Some(Variant(serde_json::json!(1))),
        Some(Variant(serde_json::json!(1.25))),
        Some(Variant(serde_json::json!("123"))),
        Some(Variant(serde_json::json!("中文🙂 \" \\ \n \0"))),
        Some(Variant(serde_json::json!([]))),
        Some(Variant(serde_json::json!({}))),
        Some(Variant(
            serde_json::json!({"a":null,"b":[],"c":{},"n":u64::MAX}),
        )),
    ];
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        if backend == Backend::MySql {
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {table} (id BIGINT PRIMARY KEY,payload JSON)"
                )))
                .await
                .unwrap();
        } else {
            db.create_table(
                &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                    .column(Column::new("id", T::BigInt).not_null())
                    .column(Column::new("payload", T::Json))
                    .replication(1),
            )
            .await
            .unwrap();
        }
        for (id, input) in inputs.iter().enumerate() {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?)"
            )))
            .bind(id as i64)
            .bind(input)
            .execute(db.pool())
            .await
            .unwrap();
        }
        let actual: Vec<Option<Variant>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT payload FROM {table} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(actual, inputs, "{backend:?}");
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires Doris; records native VARIANT storage normalization"]
async fn known_gap_variant_normalizes_empty_values_while_json_keeps_the_original_document() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(
        &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
            .column(Column::new("id", T::BigInt).not_null())
            .column(Column::new("payload", T::Variant))
            .replication(1),
    )
    .await
    .unwrap();
    let inputs = [
        serde_json::Value::Null,
        serde_json::json!({}),
        serde_json::json!([]),
        serde_json::json!("123"),
        serde_json::json!(123),
        serde_json::json!(true),
        serde_json::json!({"a":null,"b":[],"c":{},"n":42}),
    ];
    for (id, input) in inputs.iter().enumerate() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,CAST(? AS JSON))"
        )))
        .bind(id as i64)
        .bind(Variant(input))
        .execute(db.pool())
        .await
        .unwrap();
    }
    let actual: Vec<Variant> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT payload FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    let expected = vec![
        serde_json::json!({}),
        serde_json::json!({}),
        serde_json::json!({}),
        serde_json::json!("123"),
        serde_json::json!(123),
        serde_json::json!(true),
        serde_json::json!({"n":42}),
    ]
    .into_iter()
    .map(Variant)
    .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "this is a server normalization limit, not a lossless JSON contract"
    );
    common::drop_table(&db, &table).await;
    db.close().await;
}
