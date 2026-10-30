#![cfg(feature = "stream-load")]
mod common;

use sqlx_doris::{
    Backend, Error, GroupCommit,
    operations::{self, Column, DataType as T, TableDefinition, TableModel, TableName},
    sqlx::{self, Row},
    stream_load::{LoadFormat, LoadReceipt, LoadState, StreamLoadClient},
    types::{Array, BigDecimal, LargeInt, Map, Struct, Variant},
};
use std::{env, str::FromStr, time::Duration};

fn http() -> StreamLoadClient {
    let client = StreamLoadClient::with_client_builder(
        &env::var("DORIS_HTTP_URL").expect("DORIS_HTTP_URL is required"),
        env::var("DORIS_HTTP_USER").unwrap_or_else(|_| "root".into()),
        env::var("DORIS_HTTP_PASSWORD").ok(),
        reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30)),
    )
    .unwrap();
    match env::var("DORIS_BE_HTTP_URL") {
        Ok(be) => client.backend_endpoint(&be).unwrap(),
        Err(_) => client,
    }
}
async fn visible(db: &sqlx_doris::Client, table: &TableName, expected: i64) {
    operations::wait_until(
        Duration::from_secs(20),
        Duration::from_millis(50),
        || async {
            let count: i64 =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                    .fetch_one(db.pool())
                    .await?;
            Ok(count == expected)
        },
    )
    .await
    .unwrap();
}
fn committed(receipt: &LoadReceipt) {
    assert!(
        matches!(receipt.state(), LoadState::Visible | LoadState::Committed),
        "{receipt:?}"
    );
    assert_eq!(
        receipt.redirects, 1,
        "the fixture must exercise the FE to BE redirect"
    );
}
fn definition(table: TableName) -> TableDefinition {
    TableDefinition::new(table, TableModel::Duplicate, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("name", T::String))
        .replication(1)
}

#[tokio::test]
#[ignore = "requires dedicated Doris SQL and HTTP endpoints"]
async fn json_load_redirects_to_be_and_duplicate_label_does_not_duplicate_rows() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(&definition(table.clone())).await.unwrap();
    let http = http();
    let label = format!("{}_json", table.table);
    let rows = vec![
        serde_json::json!({"id":1,"name":"中文🙂 \" \\ \n \0"}),
        serde_json::json!({"id":2,"name":null}),
    ];
    let first = http
        .load(&table)
        .unwrap()
        .label(&label)
        .send_json(&rows)
        .await
        .unwrap();
    committed(&first);
    assert_eq!(first.number_loaded_rows, 2);
    assert_eq!(first.number_filtered_rows, 0);
    visible(&db, &table, 2).await;
    let actual: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT name FROM {table} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(actual.as_deref(), Some("中文🙂 \" \\ \n \0"));
    let duplicate = http
        .load(&table)
        .unwrap()
        .label(&label)
        .send_json(&rows)
        .await
        .unwrap();
    assert_eq!(
        duplicate.state(),
        LoadState::AlreadyExists { finished: true }
    );
    assert_eq!(duplicate.redirects, 1);
    visible(&db, &table, 2).await;
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris SQL and HTTP endpoints"]
async fn csv_file_and_json_lines_distinguish_empty_strings_and_nulls() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(&definition(table.clone())).await.unwrap();
    let path = std::env::temp_dir().join(format!("{}.csv", table.table));
    tokio::fs::write(&path, "1,\n2,\\N\n3,中文🙂\n")
        .await
        .unwrap();
    let http = http();
    let receipt = http
        .load(&table)
        .unwrap()
        .format(LoadFormat::Csv)
        .header("column_separator", ",")
        .unwrap()
        .send_file(&path)
        .await
        .unwrap();
    tokio::fs::remove_file(path).await.unwrap();
    committed(&receipt);
    assert_eq!(receipt.number_loaded_rows, 3);
    let receipt = http
        .load(&table)
        .unwrap()
        .format(LoadFormat::JsonLines)
        .send_bytes(b"{\"id\":4,\"name\":\"line\"}\n{\"id\":5,\"name\":null}\n".to_vec())
        .await
        .unwrap();
    committed(&receipt);
    assert_eq!(receipt.number_loaded_rows, 2);
    visible(&db, &table, 5).await;
    let names: Vec<Option<String>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT name FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap();
    assert_eq!(
        names,
        vec![
            Some(String::new()),
            None,
            Some("中文🙂".into()),
            Some("line".into()),
            None
        ]
    );
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris SQL and HTTP endpoints"]
async fn filtered_rows_fail_the_batch_by_default_and_remain_inspectable_when_allowed() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(&definition(table.clone())).await.unwrap();
    let http = http();
    let rows = [
        serde_json::json!({ "id": 1, "name": "valid" }),
        serde_json::json!({ "id": "not an integer", "name": "invalid" }),
    ];
    let error = http
        .load(&table)
        .unwrap()
        .send_json(&rows)
        .await
        .unwrap_err();
    match error {
        Error::Load { receipt } => {
            assert_eq!(receipt.number_filtered_rows, 1);
            assert!(receipt.error_url.is_some());
            assert_eq!(receipt.redirects, 1);
        }
        other => panic!("unexpected error: {other}"),
    }
    visible(&db, &table, 0).await;
    let receipt = http
        .load(&table)
        .unwrap()
        .header("max_filter_ratio", "0.5")
        .unwrap()
        .send_json(&rows)
        .await
        .unwrap();
    committed(&receipt);
    assert_eq!(receipt.number_total_rows, 2);
    assert_eq!(receipt.number_filtered_rows, 1);
    assert_eq!(receipt.number_loaded_rows, 1);
    assert!(receipt.error_url.is_some());
    visible(&db, &table, 1).await;
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris SQL and HTTP endpoints"]
async fn async_group_commit_acknowledges_acceptance_then_becomes_visible() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(&definition(table.clone()).property("group_commit_interval_ms", "1000"))
        .await
        .unwrap();
    let receipt = http()
        .load(&table)
        .unwrap()
        .group_commit(GroupCommit::Async)
        .send_json(&[serde_json::json!({ "id": 1, "name": "async" })])
        .await
        .unwrap();
    assert_eq!(receipt.state(), LoadState::Accepted);
    assert!(!receipt.is_visible());
    assert!(receipt.group_commit);
    assert_eq!(receipt.redirects, 1);
    visible(&db, &table, 1).await;
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris SQL and HTTP endpoints"]
async fn synchronous_group_commit_http_is_visible_on_return_and_invalid_batch_is_not_written() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    db.create_table(&definition(table.clone()).property("group_commit_interval_ms", "1000"))
        .await
        .unwrap();
    let receipt = http()
        .load(&table)
        .unwrap()
        .group_commit(GroupCommit::Sync)
        .send_json(&[serde_json::json!({ "id": 1, "name": "sync" })])
        .await
        .unwrap();
    assert_eq!(receipt.state(), LoadState::Visible, "{receipt:?}");
    assert!(receipt.is_visible());
    assert!(receipt.group_commit);
    assert_eq!(
        receipt.group_commit_mode.as_deref(),
        if common::doris_4_0_8() {
            None
        } else {
            Some("sync_mode")
        }
    );
    assert_eq!(
        receipt.requested_group_commit_mode.as_deref(),
        Some("sync_mode")
    );
    assert_eq!(receipt.redirects, 1);
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 1, "sync mode must be visible without polling");
    let error = http()
        .load(&table)
        .unwrap()
        .group_commit(GroupCommit::Sync)
        .send_json(&[serde_json::json!({ "id": "bad-integer", "name": "invalid" })])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Load { .. }), "{error:?}");
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
#[ignore = "requires dedicated Doris SQL and HTTP endpoints"]
async fn partial_update_preserves_other_columns_and_rejects_new_keys_when_requested() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let definition = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("name", T::String))
        .column(Column::new("score", T::Int))
        .replication(1)
        .property("enable_unique_key_merge_on_write", "true");
    db.create_table(&definition).await.unwrap();
    let http = http();
    let first = http
        .load(&table)
        .unwrap()
        .send_json(&[serde_json::json!({ "id": 1, "name": "preserved", "score": 10 })])
        .await
        .unwrap();
    committed(&first);
    visible(&db, &table, 1).await;
    let update = http
        .load(&table)
        .unwrap()
        .header("columns", "id,score")
        .unwrap()
        .header("unique_key_update_mode", "UPDATE_FIXED_COLUMNS")
        .unwrap()
        .header("partial_update_new_key_behavior", "ERROR")
        .unwrap()
        .send_json(&[serde_json::json!({ "id": 1, "score": 20 })])
        .await
        .unwrap();
    committed(&update);
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT name,score FROM {table} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("name"), "preserved");
    assert_eq!(row.get::<i32, _>("score"), 20);
    let error = http
        .load(&table)
        .unwrap()
        .header("columns", "id,score")
        .unwrap()
        .header("unique_key_update_mode", "UPDATE_FIXED_COLUMNS")
        .unwrap()
        .header("partial_update_new_key_behavior", "ERROR")
        .unwrap()
        .send_json(&[serde_json::json!({ "id": 2, "score": 30 })])
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Load { .. }));
    visible(&db, &table, 1).await;
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct Record {
    label: String,
}
type NativeLoadRow = (
    i64,
    Array<Option<String>>,
    Map<i32, Array<Option<String>>>,
    Struct<Record>,
    Variant,
    BigDecimal,
    LargeInt,
);

#[tokio::test]
#[ignore = "requires dedicated Doris SQL and HTTP endpoints"]
async fn json_native_types_persist_precision_nested_strings_and_variant_fields() {
    let db = common::client(Backend::Doris).await;
    let table = common::name();
    let definition = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("tags", T::array(T::String)))
        .column(Column::new("attrs", T::map(T::Int, T::array(T::String))))
        .column(Column::new(
            "record",
            T::Struct(vec![("label".into(), T::String)]),
        ))
        .column(Column::new("meta", T::Variant))
        .column(Column::new(
            "amount",
            T::Decimal {
                precision: 76,
                scale: 30,
            },
        ))
        .column(Column::new("wide", T::LargeInt))
        .replication(1);
    db.create_table(&definition).await.unwrap();
    let text = "中文🙂 \" \\ \r\n\t\0";
    let amount = "9999999999999999999999999999999999999999999999.123456789012345678901234567890";
    let receipt = http()
        .load_table(&definition)
        .unwrap()
        .send_json(&[
            serde_json::json!({
                "id": 1,
                "tags": [text, null, ""],
                "attrs": { "1": [text, null], "2": [] },
                "record": { "label": text },
                "meta": { "n": 42 },
                "amount": amount,
                "wide": i128::MAX.to_string()
            }),
            serde_json::json!({
                "id": 2,
                "tags": null,
                "attrs": null,
                "record": null,
                "meta": null,
                "amount": null,
                "wide": null
            }),
        ])
        .await
        .unwrap();
    committed(&receipt);
    visible(&db, &table, 2).await;
    let mut select = definition.select_builder().unwrap();
    select.push(" WHERE id=").push_bind(1_i64);
    let row: NativeLoadRow = select.build_query_as().fetch_one(db.pool()).await.unwrap();
    assert_eq!(
        row,
        (
            1,
            Array(vec![Some(text.into()), None, Some(String::new())]),
            Map(vec![
                (1, Array(vec![Some(text.into()), None])),
                (2, Array(vec![]))
            ]),
            Struct(Record { label: text.into() }),
            Variant(serde_json::json!({"n":42})),
            BigDecimal::from_str(amount).unwrap(),
            LargeInt(i128::MAX)
        )
    );
    let n: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT CAST(meta['n'] AS BIGINT) FROM {table} WHERE id=1"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(n, 42);
    let nulls: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT COUNT(*) FROM {table} WHERE id=2 AND tags IS NULL AND attrs IS NULL \
        AND record IS NULL AND meta IS NULL AND amount IS NULL AND wide IS NULL"
    )))
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(nulls, 1);
    common::drop_table(&db, &table).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris endpoints and scripts/prepare-load-fixtures"]
async fn parquet_and_orc_files_preserve_nulls_unicode_and_control_bytes() {
    let db = common::client(Backend::Doris).await;
    let http = http();
    let fixture_root = std::path::PathBuf::from(
        env::var_os("SQLX_DORIS_BUILD_ROOT").expect("scripts/dev required"),
    )
    .join("fixtures/stream-load");
    let manifest: serde_json::Value = serde_json::from_slice(
        &tokio::fs::read(fixture_root.join("manifest.json"))
            .await
            .expect("run scripts/prepare-load-fixtures"),
    )
    .unwrap();
    let expected: Vec<Option<String>> = manifest["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["name"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(
        expected,
        vec![Some("中文🙂 \" \\ \n \0".into()), None, Some(String::new())]
    );
    for (file, format) in [
        ("sample.parquet", LoadFormat::Parquet),
        ("sample.orc", LoadFormat::Orc),
    ] {
        let table = common::name();
        db.create_table(&definition(table.clone())).await.unwrap();
        let receipt = http
            .load(&table)
            .unwrap()
            .format(format)
            .send_file(fixture_root.join(file))
            .await
            .unwrap();
        committed(&receipt);
        assert_eq!(receipt.number_loaded_rows, 3);
        visible(&db, &table, 3).await;
        let names: Vec<Option<String>> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT name FROM {table} ORDER BY id"
        )))
        .fetch_all(db.pool())
        .await
        .unwrap();
        assert_eq!(names, expected, "{file}");
        common::drop_table(&db, &table).await;
    }
    db.close().await;
}
