//! Assertions for the pinned Doris fixture. Known gaps are named explicitly.

use bigdecimal::BigDecimal;
use serde_json::Value;
use sqlx::{Connection, Executor, MySqlConnection, Row};
use std::{env, str::FromStr, time::Duration};

const LARGE: &str = "170141183460469231731687303715884105727";
const DECIMAL: &str =
    "9999999999999999999999999999999999999999999999.123456789012345678901234567890";

async fn connect() -> MySqlConnection {
    tokio::time::timeout(
        Duration::from_secs(15),
        MySqlConnection::connect(&env::var("DORIS_TEST_URL").expect("DORIS_TEST_URL required")),
    )
    .await
    .unwrap()
    .unwrap()
}

async fn both(conn: &mut MySqlConnection, sql: &'static str) -> [sqlx::mysql::MySqlRow; 2] {
    let text = tokio::time::timeout(Duration::from_secs(30), (&mut *conn).fetch_one(sql))
        .await
        .unwrap()
        .unwrap();
    let binary = tokio::time::timeout(
        Duration::from_secs(30),
        sqlx::query(sql).fetch_one(&mut *conn),
    )
    .await
    .unwrap()
    .unwrap();
    [text, binary]
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn largeint_128_bits_are_exact_through_text_and_prepared_results() {
    let mut c = connect().await;
    for row in both(
        &mut c,
        "SELECT CAST('170141183460469231731687303715884105727' AS LARGEINT)",
    )
    .await
    {
        assert_eq!(
            row.try_get::<String, _>(0)
                .unwrap()
                .parse::<i128>()
                .unwrap(),
            i128::MAX
        );
        assert!(row.try_get::<i64, _>(0).is_err());
    }
    let actual: String = sqlx::query_scalar("SELECT CAST(? AS LARGEINT)")
        .bind(LARGE)
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(actual, LARGE);
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn decimal256_preserves_all_76_digits_and_30_decimal_places() {
    let mut c = connect().await;
    c.execute("SET enable_decimal256 = true").await.unwrap();
    for row in both(&mut c,"SELECT CAST('9999999999999999999999999999999999999999999999.123456789012345678901234567890' AS DECIMAL(76,30))").await {
        let value:BigDecimal=row.try_get(0).unwrap();
        assert_eq!(value,BigDecimal::from_str(DECIMAL).unwrap());
        assert_eq!(value.as_bigint_and_exponent().1,30);
    }
    let actual: BigDecimal = sqlx::query_scalar("SELECT CAST(? AS DECIMAL(76,30))")
        .bind(BigDecimal::from_str(DECIMAL).unwrap())
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(actual, BigDecimal::from_str(DECIMAL).unwrap());
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn nested_array_null_and_empty_containers_survive_both_paths() {
    let mut c = connect().await;
    for row in both(&mut c, "SELECT [[1, NULL], []]").await {
        let value: sqlx::types::Json<Value> = row.try_get(0).unwrap();
        assert_eq!(value.0, serde_json::json!([[1, null], []]));
    }
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn map_integer_keys_require_a_doris_parser_or_explicit_projection() {
    let mut c = connect().await;
    for row in both(&mut c, "SELECT map(1,'a',2,'b')").await {
        let value: String = row.try_get(0).unwrap();
        assert_eq!(value, "{1:\"a\", 2:\"b\"}");
        assert!(row.try_get::<sqlx::types::Json<Value>, _>(0).is_err());
    }
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn variant_large_json_number_is_exact_and_json_null_is_preserved() {
    let mut c = connect().await;
    for row in both(
        &mut c,
        "SELECT CAST('{\"n\":170141183460469231731687303715884105727,\"a\":[1,null]}' AS VARIANT)",
    )
    .await
    {
        let value: sqlx::types::Json<Value> = row.try_get(0).unwrap();
        assert_eq!(value.0["n"].to_string(), LARGE);
        assert!(value.0["a"][1].is_null());
    }
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn varbinary_is_lossless_and_cannot_be_decoded_as_a_string() {
    let mut c = connect().await;
    for row in both(&mut c, "SELECT CAST(unhex('0061ff00') AS VARBINARY)").await {
        assert_eq!(row.try_get::<Vec<u8>, _>(0).unwrap(), vec![0, 97, 255, 0]);
        assert!(row.try_get::<String, _>(0).is_err());
    }
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn timestamptz_has_equivalent_utc_output_with_two_offset_spellings() {
    let mut c = connect().await;
    for row in both(
        &mut c,
        "SELECT CAST('2026-10-26 10:20:30.123456+08:00' AS TIMESTAMPTZ(6))",
    )
    .await
    {
        if env::var("DORIS_TEST_VERSION").is_ok_and(|version| version == "4.0.8") {
            assert!(row.try_get::<String, _>(0).is_err());
            // Its DATETIME metadata does not make the offset-bearing value a
            // NaiveDateTime. The explicit STRING projection below is portable.
            assert!(
                row.try_get::<sqlx::types::chrono::NaiveDateTime, _>(0)
                    .is_err()
            );
        } else {
            let actual: String = row.try_get(0).unwrap();
            let normalized = if let Some(s) = actual.strip_suffix('Z') {
                format!("{s}+00:00")
            } else {
                actual
            };
            assert_eq!(normalized, "2026-10-26 02:20:30.123456+00:00");
        }
    }
    for row in both(
        &mut c,
        "SELECT CAST(CAST('2026-10-26 10:20:30.123456+08:00' AS TIMESTAMPTZ(6)) AS STRING)",
    )
    .await
    {
        let actual: String = row.try_get(0).unwrap();
        let normalized = if let Some(s) = actual.strip_suffix('Z') {
            format!("{s}+00:00")
        } else {
            actual
        };
        assert_eq!(normalized, "2026-10-26 02:20:30.123456+00:00");
    }
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn opaque_bitmap_default_output_is_null_but_explicit_projection_is_not() {
    let mut c = connect().await;
    for row in both(&mut c, "SELECT to_bitmap(42)").await {
        assert_eq!(row.try_get::<Option<String>, _>(0).unwrap(), None);
    }
    let row = (&mut c)
        .fetch_one("SELECT bitmap_to_string(to_bitmap(42))")
        .await
        .unwrap();
    assert_eq!(row.try_get::<String, _>(0).unwrap(), "42");
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn known_gap_sqlx_begin_fails_despite_a_successful_raw_begin() {
    let mut c = connect().await;
    c.execute("BEGIN").await.unwrap();
    c.execute("ROLLBACK").await.unwrap();
    assert!(matches!(c.begin().await, Err(sqlx::Error::BeginFailed)));
    c.execute("ROLLBACK").await.unwrap();
    c.close().await.unwrap();
}

#[tokio::test]
#[ignore = "requires the pinned Doris 4.1.5 fixture"]
async fn known_gap_blob_parameter_is_rejected_but_explicit_hex_projection_is_lossless() {
    let mut c = connect().await;
    let error = sqlx::query("SELECT ?")
        .bind(vec![0_u8, 97, 255, 0])
        .fetch_one(&mut c)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("Unsupported MySQL type"),
        "{error}"
    );
    println!("known BLOB binding gap: {error}");
    let value: Vec<u8> = sqlx::query_scalar("SELECT unhex(?)")
        .bind("0061ff00")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(value, vec![0, 97, 255, 0]);
    c.close().await.unwrap();
}
