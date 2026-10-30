mod common;

use sqlx_doris::{
    Backend, Client,
    operations::{Column, DataType, TableDefinition, TableModel, TableName},
    sqlx::{self, Executor, mysql::MySqlQueryResult},
};
use std::{collections::BTreeSet, env, fs, path::PathBuf};

fn receipt(step: &str, result: &MySqlQueryResult) -> serde_json::Value {
    serde_json::json!({"step":step,"rows_affected":result.rows_affected(),"last_insert_id":result.last_insert_id()})
}
fn record(backend: Backend, table: &TableName, observations: &[serde_json::Value]) {
    let directory =
        PathBuf::from(env::var_os("SQLX_DORIS_ARTIFACTS_DIR").expect("scripts/dev required"))
            .join("mutation-receipts");
    fs::create_dir_all(&directory).unwrap();
    let report = serde_json::json!({"backend":format!("{backend:?}"),"doris_version":env::var("DORIS_TEST_VERSION").ok(),"table":table.to_string(),"receipts":observations});
    let file = directory.join(format!("{backend:?}-{}.json", table.table));
    fs::write(&file, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    eprintln!("Mutation receipts: {}", file.display());
}
async fn rows(db: &Client, table: &TableName) -> Vec<(i64, Option<String>)> {
    sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,note FROM {table} ORDER BY id"
    )))
    .fetch_all(db.pool())
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn dml_receipts_are_recorded_separately_from_verified_data_changes() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        if backend == Backend::MySql {
            db.pool()
                .execute(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {table}(id BIGINT PRIMARY KEY,note VARCHAR(128))"
                )))
                .await
                .unwrap();
        } else {
            db.create_table(
                &TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
                    .column(Column::new("id", DataType::BigInt).not_null())
                    .column(Column::new("note", DataType::String))
                    .replication(1),
            )
            .await
            .unwrap();
        }
        let mut observations = Vec::new();
        let result = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table}(id,note) VALUES (?,?),(?,?)"
        )))
        .bind(1_i64)
        .bind("before")
        .bind(2_i64)
        .bind(Option::<String>::None)
        .execute(db.pool())
        .await
        .unwrap();
        observations.push(receipt("insert_two", &result));
        assert_eq!(
            rows(&db, &table).await,
            vec![(1, Some("before".into())), (2, None)]
        );
        for (step, id, note) in [
            ("update_changed", 1, "after"),
            ("update_same", 1, "after"),
            ("update_missing", 404, "absent"),
        ] {
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {table} SET note=? WHERE id=?"
            )))
            .bind(note)
            .bind(id as i64)
            .execute(db.pool())
            .await
            .unwrap();
            observations.push(receipt(step, &result));
            assert_eq!(
                rows(&db, &table).await,
                vec![(1, Some("after".into())), (2, None)]
            );
        }
        for (step, id) in [("delete_existing", 2_i64), ("delete_missing", 404_i64)] {
            let result = sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {table} WHERE id=?"
            )))
            .bind(id)
            .execute(db.pool())
            .await
            .unwrap();
            observations.push(receipt(step, &result));
            assert_eq!(rows(&db, &table).await, vec![(1, Some("after".into()))]);
        }
        // Pin the observed contract for this configuration, in addition to
        // checking each intermediate stored state. SQLx requests FOUND_ROWS:
        // a matched UPDATE reports 1 even when the value is unchanged.
        assert_eq!(
            observations
                .iter()
                .map(|v| v["rows_affected"].as_u64().unwrap())
                .collect::<Vec<_>>(),
            [2, 1, 1, 0, 1, 0]
        );
        assert!(observations.iter().all(|v| v["last_insert_id"] == 0));
        record(backend, &table, &observations);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated MySQL and Doris databases"]
async fn generated_ids_are_unique_while_doris_ok_packets_do_not_return_the_id() {
    for backend in [Backend::MySql, Backend::Doris] {
        let db = common::client(backend).await;
        let table = common::name();
        let ddl = if backend == Backend::MySql {
            format!(
                "CREATE TABLE {table}(id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,note VARCHAR(128) NOT NULL) AUTO_INCREMENT=100"
            )
        } else {
            format!(
                "CREATE TABLE {table}(id BIGINT NOT NULL AUTO_INCREMENT(100),note STRING NOT NULL) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num'='1','enable_unique_key_merge_on_write'='true')"
            )
        };
        db.pool().execute(sqlx::AssertSqlSafe(ddl)).await.unwrap();
        let mut observations = Vec::new();
        for (step, explicit_null) in [("omit_id", false), ("null_id", true)] {
            let result = if explicit_null {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table}(id,note) VALUES (?,?)"
                )))
                .bind(Option::<i64>::None)
                .bind(step)
                .execute(db.pool())
                .await
                .unwrap()
            } else {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "INSERT INTO {table}(note) VALUES (?)"
                )))
                .bind(step)
                .execute(db.pool())
                .await
                .unwrap()
            };
            observations.push(receipt(step, &result));
            assert_eq!(result.rows_affected(), 1);
            let generated: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM {table} WHERE note=?"
            )))
            .bind(step)
            .fetch_one(db.pool())
            .await
            .unwrap();
            assert!(generated >= 100);
            if backend == Backend::MySql {
                assert_eq!(result.last_insert_id(), generated as u64);
            } else {
                // Fixed FE MysqlOkPacket writes LAST_INSERT_ID=0; do not invent
                // a generated key from connection-local state or replay a write.
                assert_eq!(result.last_insert_id(), 0);
            }
        }
        let result = sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {table}(note) VALUES (?),(?)"
        )))
        .bind("batch_a")
        .bind("batch_b")
        .execute(db.pool())
        .await
        .unwrap();
        observations.push(receipt("batch_generated", &result));
        assert_eq!(result.rows_affected(), 2);
        let stored = rows(&db, &table).await;
        assert_eq!(stored.len(), 4);
        assert_eq!(stored.iter().map(|r| r.0).collect::<BTreeSet<_>>().len(), 4);
        assert_eq!(
            stored
                .iter()
                .map(|r| r.1.clone().unwrap())
                .collect::<BTreeSet<_>>(),
            ["omit_id", "null_id", "batch_a", "batch_b"]
                .map(str::to_owned)
                .into_iter()
                .collect::<BTreeSet<_>>()
        );
        if backend == Backend::Doris {
            assert_eq!(result.last_insert_id(), 0);
        }
        record(backend, &table, &observations);
        common::drop_table(&db, &table).await;
        db.close().await;
    }
}
