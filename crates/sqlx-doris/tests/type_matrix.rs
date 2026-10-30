mod common;

use chrono::{NaiveDate, NaiveDateTime};
use sqlx_doris::{
    Backend, Client, PoolBuilder,
    operations::{self, Column, DataType as T, TableDefinition, TableModel, TableName},
    sqlx::{self, Executor, Row},
    types::{Array, BigDecimal, TimestampTz},
};
use std::{env, str::FromStr};

async fn decimal_table(db: &Client, precision: u8, scale: u8) -> TableName {
    let table = common::name();
    if db.backend() == Backend::MySql {
        db.pool()
            .execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY,value DECIMAL({precision},{scale}))"
            )))
            .await
            .unwrap();
    } else {
        db.create_table(
            &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                .column(Column::new("id", T::BigInt).not_null())
                .column(Column::new("value", T::Decimal { precision, scale }))
                .replication(1),
        )
        .await
        .unwrap();
    }
    table
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn decimal_physical_widths_scale_extremes_nulls_and_overflow_round_trip_exactly() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let mut shapes = vec![
            (1, 0),
            (9, 0),
            (9, 9),
            (10, 2),
            (18, 9),
            (19, 10),
            (38, 0),
            (38, 30),
            (65, 30),
        ];
        if backend == Backend::Doris {
            shapes.extend([(38, 38), (39, 39), (76, 0), (76, 30), (76, 76)]);
        }
        for (precision, scale) in shapes {
            let table = decimal_table(&db, precision, scale).await;
            let integers = usize::from(precision - scale);
            let mut largest = if integers == 0 {
                "0".to_owned()
            } else {
                "9".repeat(integers)
            };
            if scale > 0 {
                largest.push('.');
                largest.push_str(&"9".repeat(usize::from(scale)));
            }
            let smallest = if scale == 0 {
                "1".to_owned()
            } else {
                format!("0.{}1", "0".repeat(usize::from(scale) - 1))
            };
            let values = [
                Some(largest.clone()),
                Some(format!("-{largest}")),
                Some(smallest),
                Some("0".into()),
                None,
            ]
            .map(|value| value.map(|value| BigDecimal::from_str(&value).unwrap()));
            for (id, value) in values.iter().enumerate() {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table} VALUES (?,?)"
                )))
                .bind(id as i64)
                .bind(value)
                .execute(db.pool())
                .await
                .unwrap();
            }
            let binary: Vec<Option<BigDecimal>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT value FROM {table} ORDER BY id"
            )))
            .fetch_all(db.pool())
            .await
            .unwrap();
            assert_eq!(binary, values, "{backend:?} decimal({precision},{scale})");
            let text = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "SELECT value FROM {table} ORDER BY id"
            )))
            .fetch_all(db.pool())
            .await
            .unwrap();
            for ((row, binary), expected) in text.iter().zip(&binary).zip(&values) {
                let actual: Option<BigDecimal> = row.try_get(0).unwrap();
                assert_eq!(&actual, expected);
                if let Some(value) = binary {
                    assert_eq!(
                        value.as_bigint_and_exponent().1,
                        i64::from(scale),
                        "{backend:?} decimal({precision},{scale}): {value}"
                    );
                    assert_eq!(actual.unwrap().as_bigint_and_exponent().1, i64::from(scale));
                }
            }
            let overflow = BigDecimal::from_str(&format!("1{}", "0".repeat(integers))).unwrap();
            let error = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?)"
            )))
            .bind(99_i64)
            .bind(overflow)
            .execute(db.pool())
            .await
            .unwrap_err();
            assert!(
                matches!(error, sqlx::Error::Database(_)),
                "{backend:?}: {error:?}"
            );
            let count: i64 =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                    .fetch_one(db.pool())
                    .await
                    .unwrap();
            assert_eq!(count, 5);
            common::drop_table(&db, &table).await;
        }
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn decimal_assignment_rounds_to_declared_scale_and_rejects_rounding_overflow() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = decimal_table(&db, 5, 2).await;
        let cases = [
            ("1.234", "1.23"),
            ("1.235", "1.24"),
            ("1.245", "1.25"),
            ("-1.235", "-1.24"),
            ("0.005", "0.01"),
            ("-0.005", "-0.01"),
            ("99.999", "100.00"),
        ];
        for (id, (input, _)) in cases.iter().enumerate() {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?)"
            )))
            .bind(id as i64)
            .bind(BigDecimal::from_str(input).unwrap())
            .execute(db.pool())
            .await
            .unwrap();
        }
        let values: Vec<BigDecimal> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT value FROM {table} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(
            values,
            cases.map(|(_, expected)| BigDecimal::from_str(expected).unwrap()),
            "{backend:?}"
        );
        let error = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,?)"
        )))
        .bind(99_i64)
        .bind(BigDecimal::from_str("999.995").unwrap())
        .execute(db.pool())
        .await
        .unwrap_err();
        assert!(matches!(error, sqlx::Error::Database(_)), "{error:?}");
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(count, 7);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn char_padding_rules_are_explicit_while_varchar_keeps_trailing_spaces_and_nul() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        if backend == Backend::MySql {
            db.pool().execute(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY,fixed CHAR(16),variable VARCHAR(32))")))
                .await.unwrap();
        } else {
            db.create_table(
                &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                    .column(Column::new("id", T::BigInt).not_null())
                    .column(Column::new("fixed", T::Char(16)))
                    .column(Column::new("variable", T::VarChar(32)))
                    .replication(1),
            )
            .await
            .unwrap();
        }
        let values = [
            None,
            Some(""),
            Some("tail  "),
            Some("nul\0"),
            Some("中文"),
            Some("in\0side"),
        ];
        for (id, value) in values.iter().enumerate() {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?,?)"
            )))
            .bind(id as i64)
            .bind(value)
            .bind(value)
            .execute(db.pool())
            .await
            .unwrap();
        }
        let binary: Vec<(Option<String>, Option<String>)> = sqlx::query_as(sqlx::AssertSqlSafe(
            format!("SELECT fixed,variable FROM {table} ORDER BY id"),
        ))
        .fetch_all(db.pool())
        .await
        .unwrap();
        let text = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "SELECT fixed,variable FROM {table} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        for (index, ((fixed, variable), original)) in binary.iter().zip(values).enumerate() {
            let expected = original.map(|value| {
                if backend == Backend::MySql {
                    value.trim_end_matches(' ')
                } else {
                    value.split('\0').next().unwrap()
                }
            });
            assert_eq!(fixed.as_deref(), expected, "{backend:?}, row {index}");
            assert_eq!(variable.as_deref(), original);
            assert_eq!(text[index].try_get::<Option<String>, _>(0).unwrap(), *fixed);
            assert_eq!(
                text[index].try_get::<Option<String>, _>(1).unwrap(),
                *variable
            );
        }
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn early_calendar_years_invalid_dates_and_native_sqlx_datetime_precision_are_explicit() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        if backend == Backend::MySql {
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {table} (id BIGINT PRIMARY KEY,day DATE,created DATETIME(6))"
                )))
                .await
                .unwrap();
        } else {
            db.create_table(
                &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                    .column(Column::new("id", T::BigInt).not_null())
                    .column(Column::new("day", T::Date))
                    .column(Column::new("created", T::DateTime(6)))
                    .replication(1),
            )
            .await
            .unwrap();
        }
        let dates = [
            NaiveDate::from_ymd_opt(0, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(1, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(999, 12, 31).unwrap(),
            NaiveDate::from_ymd_opt(9999, 12, 31).unwrap(),
        ];
        let mut expected = vec![];
        for (id, day) in dates.into_iter().enumerate() {
            let dt = day.and_hms_micro_opt(12, 34, 56, 123456).unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,?,?)"
            )))
            .bind(id as i64)
            .bind(day)
            .bind(dt)
            .execute(db.pool())
            .await
            .unwrap();
            expected.push((day, dt));
        }
        let actual: Vec<(NaiveDate, NaiveDateTime)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT day,created FROM {table} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(actual, expected, "{backend:?}");
        let year_zero_leap_day = NaiveDate::from_ymd_opt(0, 2, 29).unwrap();
        let result = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} (id,day) VALUES (?,?)"
        )))
        .bind(90_i64)
        .bind(year_zero_leap_day)
        .execute(db.pool())
        .await;
        // Chrono accepts this proleptic leap day; neither tested server does.
        assert!(
            matches!(result.unwrap_err(), sqlx::Error::Database(_)),
            "{backend:?}"
        );
        for bad in ["0000-00-00", "2023-02-29", "2024-13-01"] {
            let error = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} (id,day) VALUES (?,CAST(? AS DATE))"
            )))
            .bind(100_i64)
            .bind(bad)
            .execute(db.pool())
            .await
            .unwrap_err();
            assert!(
                matches!(error, sqlx::Error::Database(_)),
                "{backend:?}, {bad}: {error:?}"
            );
        }
        for year in [-1, 65536] {
            let error = sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} (id,day) VALUES (?,?)"
            )))
            .bind(101_i64)
            .bind(NaiveDate::from_ymd_opt(year, 1, 1).unwrap())
            .execute(db.pool())
            .await
            .unwrap_err();
            assert!(
                matches!(error, sqlx::Error::Encode(_)),
                "{backend:?}: {error:?}"
            );
        }
        let input = NaiveDate::from_ymd_opt(2026, 10, 26)
            .unwrap()
            .and_hms_nano_opt(12, 34, 56, 123_456_789)
            .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} (id,created) VALUES (?,?)"
        )))
        .bind(200_i64)
        .bind(input)
        .execute(db.pool())
        .await
        .unwrap();
        let actual: NaiveDateTime = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT created FROM {table} WHERE id=200"
        )))
        .fetch_one(db.pool())
        .await
        .unwrap();
        assert_eq!(
            actual,
            input.date().and_hms_micro_opt(12, 34, 56, 123456).unwrap(),
            "native SQLx uses microseconds"
        );
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(db.pool())
                .await
                .unwrap();
        assert_eq!(count, 5);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires MySQL"]
async fn mysql_relaxed_zero_date_is_distinct_in_sql_but_not_a_valid_chrono_date() {
    let db = PoolBuilder::new(Backend::MySql)
        .max_connections(1)
        .connect(&env::var("MYSQL_TEST_URL").unwrap())
        .await
        .unwrap();
    db.pool().execute("SET sql_mode=''").await.unwrap();
    let table = common::name();
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE TABLE {table} (id BIGINT PRIMARY KEY,day DATE)"
        )))
        .await
        .unwrap();
    db.pool()
        .execute(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (1,'0000-00-00'),(2,NULL)"
        )))
        .await
        .unwrap();
    let rows: Vec<(i64, Option<NaiveDate>, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,day,CAST(day IS NULL AS SIGNED) FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(rows, [(1, None, 0), (2, None, 1)]);
    let text = sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
        "SELECT day FROM {table} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    let error = text.try_get::<Option<NaiveDate>, _>(0).unwrap_err();
    assert!(
        matches!(error, sqlx::Error::ColumnDecode { .. }),
        "{error:?}"
    );
    let original: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT CAST(day AS CHAR) FROM {table} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(original, "0000-00-00");
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn no_backslash_escapes_mode_keeps_parameter_bytes_and_doris_complex_casts() {
    let value = "中文🙂 '\\ ? /* comment */ \r\n\t\0 tail  ";
    for backend in [Backend::MySql, Backend::Doris] {
        let db = PoolBuilder::new(backend)
            .max_connections(1)
            .after_connect(|connection| {
                Box::pin(async move {
                    connection
                        .execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
                        .await?;
                    Ok(())
                })
            })
            .connect(
                &env::var(if backend == Backend::MySql {
                    "MYSQL_TEST_URL"
                } else {
                    "DORIS_TEST_URL"
                })
                .unwrap(),
            )
            .await
            .unwrap();
        let mode: String = sqlx::query_scalar("SELECT @@sql_mode")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert!(mode.contains("NO_BACKSLASH_ESCAPES"));
        let actual: String = sqlx::query_scalar("SELECT CAST(? AS CHAR(128))")
            .bind(value)
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(actual, value);
        if backend == Backend::Doris {
            let table = common::name();
            let schema = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                .column(Column::new("id", T::BigInt).not_null())
                .column(Column::new("items", T::array(T::String)))
                .replication(1);
            db.create_table(&schema).await.unwrap();
            let values = Array(vec![Some(value.to_owned()), None, Some(String::new())]);
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {table} VALUES (?,CAST(? AS ARRAY<STRING>))"
            )))
            .bind(1_i64)
            .bind(&values)
            .execute(db.pool())
            .await
            .unwrap();
            let projection = operations::project("items", &T::array(T::String)).unwrap();
            let actual: Array<Option<String>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT {projection} FROM {table}"
            )))
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert_eq!(actual, values);
            common::drop_table(&db, &table).await;
        }
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires Doris"]
async fn timestamp_schema_projection_preserves_instant_and_session_offset_across_wire_versions() {
    let db = PoolBuilder::new(Backend::Doris)
        .max_connections(1)
        .after_connect(|connection| {
            Box::pin(async move {
                connection.execute("SET time_zone='+05:30'").await?;
                Ok(())
            })
        })
        .connect(&env::var("DORIS_TEST_URL").unwrap())
        .await
        .unwrap();
    let table = common::name();
    let schema = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("at", T::TimestampTz(6)))
        .replication(1);
    db.create_table(&schema).await.unwrap();
    let values = [
        Some(TimestampTz::parse("2026-11-01T01:30:00.123456-04:00").unwrap()),
        Some(TimestampTz::parse("2026-11-01T01:30:00.123456-05:00").unwrap()),
        None,
    ];
    for (id, value) in values.iter().enumerate() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table} VALUES (?,CAST(? AS TIMESTAMPTZ(6)))"
        )))
        .bind(id as i64)
        .bind(value)
        .execute(db.pool())
        .await
        .unwrap();
    }
    let mut select = schema.select_builder().unwrap();
    select.push(" ORDER BY id");
    let actual: Vec<(i64, Option<TimestampTz>)> =
        select.build_query_as().fetch_all(db.pool()).await.unwrap();
    for ((id, value), expected) in actual.iter().zip(&values) {
        assert_eq!(value, expected, "row {id}");
        if let Some(value) = value {
            assert_eq!(value.0.offset().local_minus_utc(), 19800);
        }
    }
    common::drop_table(&db, &table).await;
    db.close().await;
}
