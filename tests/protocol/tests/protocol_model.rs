//! These tests prove client behavior under controlled packets, not Doris certification.

mod support;

use bigdecimal::BigDecimal;
use serde_json::Value;
use sqlx::{
    Connection, Decode, Executor, MySql, Row, Type,
    mysql::{MySqlTypeInfo, MySqlValueRef},
};
use std::str::FromStr;
use support::{Cell, Column, Peer};

const LARGE: &str = "170141183460469231731687303715884105727";
const DECIMAL: &str =
    "9999999999999999999999999999999999999999999999.123456789012345678901234567890";

#[derive(Debug, PartialEq)]
struct LargeInt(i128);
impl Type<MySql> for LargeInt {
    fn type_info() -> MySqlTypeInfo {
        <String as Type<MySql>>::type_info()
    }
    fn compatible(ty: &MySqlTypeInfo) -> bool {
        <String as Type<MySql>>::compatible(ty)
    }
}
impl Decode<'_, MySql> for LargeInt {
    fn decode(value: MySqlValueRef<'_>) -> Result<Self, sqlx::error::BoxDynError> {
        Ok(Self(<String as Decode<MySql>>::decode(value)?.parse()?))
    }
}

#[tokio::test]
async fn largeint_newtype_preserves_128_bits_in_both_result_formats() {
    for deprecated in [false, true] {
        let peer = Peer::start(
            vec![Column::new(254)],
            vec![vec![Cell::from(LARGE)]],
            0,
            deprecated,
        )
        .await;
        let mut conn = peer.connect().await;
        let text = (&mut conn).fetch_one("SELECT value").await.unwrap();
        let binary = sqlx::query("SELECT value")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        for row in [text, binary] {
            assert_eq!(row.try_get::<LargeInt, _>(0).unwrap(), LargeInt(i128::MAX));
            assert!(row.try_get::<i64, _>(0).is_err());
        }
        conn.close().await.unwrap();
        peer.finish().await;
    }
}

#[tokio::test]
async fn decimal256_uses_length_encoded_text_even_in_binary_rows() {
    let peer = Peer::start(
        vec![Column::new(246)],
        vec![vec![Cell::from(DECIMAL)]],
        0,
        true,
    )
    .await;
    let mut conn = peer.connect().await;
    for row in [
        (&mut conn).fetch_one("SELECT value").await.unwrap(),
        sqlx::query("SELECT value")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
    ] {
        assert_eq!(
            row.try_get::<BigDecimal, _>(0).unwrap(),
            BigDecimal::from_str(DECIMAL).unwrap()
        );
        assert!(row.try_get::<String, _>(0).is_err());
    }
    conn.close().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn variant_json_preserves_large_numbers_with_arbitrary_precision_enabled() {
    let value = format!("{{\"n\":{LARGE},\"a\":[1,null,3]}}");
    let peer = Peer::start(
        vec![Column::new(245)],
        vec![vec![Cell::from(value.as_str())]],
        0,
        true,
    )
    .await;
    let mut conn = peer.connect().await;
    let row = sqlx::query("SELECT value")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    let value: sqlx::types::Json<Value> = row.try_get(0).unwrap();
    assert_eq!(value.0["n"].to_string(), LARGE);
    assert!(value.0["a"][1].is_null());
    conn.close().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn map_text_with_integer_keys_is_not_implicitly_json() {
    let peer = Peer::start(
        vec![Column::new(254)],
        vec![vec![Cell::from("{1:\"a\", 2:\"b\"}")]],
        0,
        true,
    )
    .await;
    let mut conn = peer.connect().await;
    let row = sqlx::query("SELECT value")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(row.try_get::<String, _>(0).unwrap(), "{1:\"a\", 2:\"b\"}");
    assert!(row.try_get::<sqlx::types::Json<Value>, _>(0).is_err());
    conn.close().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn varbinary_charset_and_flag_preserve_nul_and_non_utf8() {
    let mut column = Column::new(253);
    column.charset = 63;
    column.flags = 128;
    let peer = Peer::start(
        vec![column],
        vec![vec![Cell::Bytes(vec![0, 97, 255, 0])]],
        0,
        true,
    )
    .await;
    let mut conn = peer.connect().await;
    for row in [
        (&mut conn).fetch_one("SELECT value").await.unwrap(),
        sqlx::query("SELECT value")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
    ] {
        assert_eq!(row.try_get::<Vec<u8>, _>(0).unwrap(), vec![0, 97, 255, 0]);
        assert!(row.try_get::<String, _>(0).is_err());
    }
    conn.close().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn null_bitmap_crosses_byte_boundaries_and_distinguishes_empty_strings() {
    let columns = (0..10)
        .map(|i| {
            let mut c = Column::new(254);
            c.name = format!("c{i}");
            c
        })
        .collect();
    let cells = (0..10)
        .map(|i| {
            if [0, 7, 8].contains(&i) {
                Cell::Null
            } else {
                Cell::from("")
            }
        })
        .collect();
    let peer = Peer::start(columns, vec![cells], 0, true).await;
    let mut conn = peer.connect().await;
    for row in [
        (&mut conn).fetch_one("SELECT value").await.unwrap(),
        sqlx::query("SELECT value")
            .fetch_one(&mut conn)
            .await
            .unwrap(),
    ] {
        for i in 0..10 {
            assert_eq!(
                row.try_get::<Option<String>, _>(i).unwrap(),
                if [0, 7, 8].contains(&i) {
                    None
                } else {
                    Some(String::new())
                }
            );
        }
    }
    conn.close().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn begin_without_in_trans_status_is_rejected_by_sqlx() {
    let peer = Peer::start(vec![], vec![], 0, true).await;
    let mut conn = peer.connect().await;
    assert!(matches!(conn.begin().await, Err(sqlx::Error::BeginFailed)));
    // A failed client-side begin check does not prove that the server rolled back.
    conn.execute("ROLLBACK").await.unwrap();
    conn.close().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn begin_with_in_trans_status_allows_sqlx_transaction() {
    let peer = Peer::start(vec![], vec![], 3, true).await;
    let mut conn = peer.connect().await;
    conn.begin().await.unwrap().rollback().await.unwrap();
    conn.close().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn doris_opt_in_does_not_accept_an_unidentified_statusless_begin_ack() {
    let peer = Peer::start(vec![], vec![], 0, true).await;
    let settings = sqlx::mysql::MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(peer.port)
        .username("fixture")
        .ssl_mode(sqlx::mysql::MySqlSslMode::Disabled)
        .doris_transaction_compatibility(true);
    let mut conn = sqlx::MySqlConnection::connect_with(&settings)
        .await
        .unwrap();
    assert!(matches!(conn.begin().await, Err(sqlx::Error::BeginFailed)));
    let commands_before = peer.commands.lock().unwrap().len();
    assert!(matches!(conn.ping().await, Err(sqlx::Error::Protocol(_))));
    assert_eq!(
        peer.commands.lock().unwrap().len(),
        commands_before,
        "a retired connection must not send PING"
    );
    conn.close_hard().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn doris_control_rejects_unexpected_multiple_results_and_retires_the_connection() {
    let peer = Peer::start(vec![], vec![], 8, true).await;
    let settings = sqlx::mysql::MySqlConnectOptions::new()
        .host("127.0.0.1")
        .port(peer.port)
        .username("fixture")
        .ssl_mode(sqlx::mysql::MySqlSslMode::Disabled)
        .doris_transaction_compatibility(true);
    let mut conn = sqlx::MySqlConnection::connect_with(&settings)
        .await
        .unwrap();
    let error = conn.begin().await.unwrap_err();
    assert!(error.to_string().contains("multiple results"));
    assert!(conn.ping().await.is_err());
    conn.close_hard().await.unwrap();
    peer.finish().await;
}

#[tokio::test]
async fn persistent_false_still_uses_prepare_execute_and_close() {
    let peer = Peer::start(
        vec![Column::new(254)],
        vec![vec![Cell::from("ok")]],
        0,
        true,
    )
    .await;
    let mut conn = peer.connect().await;
    let row = sqlx::query("SELECT ?")
        .bind("ok")
        .persistent(false)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(row.try_get::<String, _>(0).unwrap(), "ok");
    conn.close().await.unwrap();
    let commands = peer.commands.clone();
    peer.finish().await;
    assert!(
        commands
            .lock()
            .unwrap()
            .windows(3)
            .any(|p| p == [22, 23, 25])
    );
}

#[tokio::test]
async fn a_server_error_does_not_poison_the_next_result() {
    let peer = Peer::start(
        vec![Column::new(254)],
        vec![vec![Cell::from("ok")]],
        0,
        true,
    )
    .await;
    let mut conn = peer.connect().await;
    let error = conn.execute("FAIL").await.unwrap_err();
    assert_eq!(error.as_database_error().unwrap().code().unwrap(), "HY000");
    assert_eq!(
        error
            .as_database_error()
            .unwrap()
            .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
            .unwrap()
            .number(),
        1105
    );
    let row = (&mut conn).fetch_one("SELECT value").await.unwrap();
    assert_eq!(row.try_get::<String, _>(0).unwrap(), "ok");
    conn.close().await.unwrap();
    peer.finish().await;
}
