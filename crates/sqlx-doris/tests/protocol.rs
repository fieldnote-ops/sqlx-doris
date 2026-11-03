#[path = "support/peer.rs"]
mod peer;
use futures_util::TryStreamExt;
use peer::{Cell, Column, Peer};
use sqlx::{Connection, Executor};
use sqlx_doris::{DorisConnectOptions, DorisConnection, DorisSslMode};

#[cfg(feature = "cli")]
#[tokio::test]
async fn history_cli_distinguishes_invalid_identifiers_from_malformed_server_metadata() {
    use sqlx::ConnectOptions;
    let folder = std::path::PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap()).join(
        format!("history-classification-{}", uuid::Uuid::new_v4().simple()),
    );
    std::fs::create_dir(&folder).unwrap();
    for (table, expected, executes) in [("_sqlx_migrations_gate", 2, 0), ("custom_history", 7, 1)] {
        let server = Peer::start(
            vec![Column::new(253)],
            vec![vec![Cell::from("app")]],
            1,
            false,
        )
        .await;
        server
            .row_fault
            .store(1, std::sync::atomic::Ordering::SeqCst);
        let url = DorisConnectOptions::new()
            .host("127.0.0.1")
            .port(server.port)
            .username("fixture")
            .database("app")
            .ssl_mode(DorisSslMode::Disabled)
            .to_url_lossy()
            .to_string();
        let source = folder.clone();
        let output = tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
                .args([
                    "--database-url",
                    &url,
                    "--json",
                    "migrate",
                    "--table",
                    table,
                    "--source",
                    source.to_str().unwrap(),
                    "info",
                ])
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(
            output.status.code(),
            Some(expected),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(output.stderr.is_empty());
        let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["exit_code"], expected);
        if expected == 2 {
            assert_eq!(value["issues"][0]["code"], "migration_table_invalid");
        } else {
            assert_eq!(value["status"], "unavailable");
        }
        let trace = server.commands.clone();
        server.finish().await;
        assert_eq!(
            trace.lock().unwrap().iter().filter(|&&v| v == 23).count(),
            executes
        );
    }
    std::fs::remove_dir(folder).unwrap();
}

#[tokio::test]
async fn oversized_response_is_rejected_before_body_allocation_and_retires_connection() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::Bytes(vec![b'x'; 1024])]],
        1,
        false,
    )
    .await;
    let options = DorisConnectOptions::new()
        .host("127.0.0.1")
        .port(server.port)
        .username("fixture")
        .ssl_mode(DorisSslMode::Disabled)
        .max_packet_size(128);
    let mut c = DorisConnection::connect_with(&options).await.unwrap();
    assert!(matches!(
        sqlx::query("SELECT value").fetch_one(&mut c).await,
        Err(sqlx::Error::Protocol(_))
    ));
    assert!(c.ping().await.is_err());
    c.close_hard().await.unwrap();
    let trace = server.commands.clone();
    server.finish().await;
    let trace = trace.lock().unwrap();
    assert_eq!(trace.iter().filter(|&&v| v == 23).count(), 1);
    assert!(!trace.contains(&14));
}

#[tokio::test]
async fn malformed_rows_and_sequences_retire_the_connection_without_a_second_command() {
    for fault in 1..=4 {
        let server = Peer::start(
            vec![Column::new(253)],
            vec![vec![Cell::from("payload")]],
            1,
            false,
        )
        .await;
        let mut c = server.connect().await;
        server
            .row_fault
            .store(fault, std::sync::atomic::Ordering::SeqCst);
        assert!(
            sqlx::query("SELECT value").fetch_one(&mut c).await.is_err(),
            "fault {fault}"
        );
        assert!(c.ping().await.is_err());
        c.close_hard().await.unwrap();
        let trace = server.commands.clone();
        server.finish().await;
        let trace = trace.lock().unwrap();
        assert_eq!(trace.iter().filter(|&&v| v == 23).count(), 1);
        assert!(!trace.contains(&14));
    }
}

#[tokio::test]
async fn cache_eviction_and_clear_close_each_prepared_handle_once() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("payload")]],
        1,
        false,
    )
    .await;
    let mut c = DorisConnection::connect_with(
        &DorisConnectOptions::new()
            .host("127.0.0.1")
            .port(server.port)
            .username("fixture")
            .ssl_mode(DorisSslMode::Disabled)
            .statement_cache_capacity(3),
    )
    .await
    .unwrap();
    for i in 0..64 {
        let value: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT value /* shape {} */",
            i % 11
        )))
        .fetch_one(&mut c)
        .await
        .unwrap();
        assert_eq!(value, "payload");
        assert!(c.cached_statements_size() <= 3);
    }
    c.clear_cached_statements().await.unwrap();
    assert_eq!(c.cached_statements_size(), 0);
    c.close().await.unwrap();
    let trace = server.commands.clone();
    let handles = server.handle_events.clone();
    server.finish().await;
    let trace = trace.lock().unwrap();
    assert_eq!(trace.iter().filter(|&&v| v == 22).count(), 64);
    assert_eq!(trace.iter().filter(|&&v| v == 25).count(), 64);
    let handles = handles.lock().unwrap();
    let prepared = handles
        .iter()
        .filter_map(|(tag, id)| (*tag == 22).then_some(*id))
        .collect::<std::collections::BTreeSet<_>>();
    let closed = handles
        .iter()
        .filter_map(|(tag, id)| (*tag == 25).then_some(*id))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(prepared.len(), 64);
    assert_eq!(prepared, closed);
}

#[tokio::test]
async fn cached_prepared_statements_use_one_prepare_and_two_executes() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("hello")]],
        1,
        false,
    )
    .await;
    let mut conn = server.connect().await;
    for n in 0..2 {
        let v: String = sqlx::query_scalar("SELECT ?")
            .bind(n)
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(v, "hello");
    }
    assert_eq!(conn.cached_statements_size(), 1);
    conn.close().await.unwrap();
    let trace = server.commands.clone();
    server.finish().await;
    let commands = trace.lock().unwrap().clone();
    assert_eq!(commands.iter().filter(|&&v| v == 22).count(), 1);
    assert_eq!(commands.iter().filter(|&&v| v == 23).count(), 2);
}

#[tokio::test]
async fn nonpersistent_execution_does_not_invalidate_an_existing_cached_statement() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("x")]],
        1,
        false,
    )
    .await;
    let mut conn = server.connect().await;
    for cached in [true, false, true] {
        let _: String = sqlx::query_scalar("SELECT ?")
            .bind(1_i64)
            .persistent(cached)
            .fetch_one(&mut conn)
            .await
            .unwrap();
    }
    assert_eq!(conn.cached_statements_size(), 1);
    conn.clear_cached_statements().await.unwrap();
    assert_eq!(conn.cached_statements_size(), 0);
    conn.close().await.unwrap();
    let trace = server.commands.clone();
    server.finish().await;
    let c = trace.lock().unwrap().clone();
    assert_eq!(c.iter().filter(|&&n| n == 22).count(), 2);
    assert_eq!(c.iter().filter(|&&n| n == 25).count(), 2);
}

#[tokio::test]
async fn null_empty_and_utf8_remain_distinct_in_streamed_rows() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![
            vec![Cell::Null],
            vec![Cell::from("")],
            vec![Cell::from("中🙂")],
        ],
        1,
        false,
    )
    .await;
    let mut conn = server.connect().await;
    let values: Vec<Option<String>> = sqlx::query_scalar("SELECT value")
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(values, vec![None, Some(String::new()), Some("中🙂".into())]);
    conn.ping().await.unwrap();
    conn.close().await.unwrap();
    server.finish().await;
}

#[tokio::test]
async fn local_binding_and_server_errors_leave_connection_usable() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("ok")]],
        1,
        false,
    )
    .await;
    let mut conn = server.connect().await;
    assert!(
        sqlx::query("SELECT ?,?")
            .bind(1)
            .execute(&mut conn)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("SELECT ?")
            .bind(f64::NAN)
            .execute(&mut conn)
            .await
            .is_err()
    );
    let error = conn.execute("FAIL").await.unwrap_err();
    let db = error.as_database_error().unwrap();
    assert_eq!(db.code().as_deref(), Some("HY000"));
    assert_eq!(db.message(), "fixture failure");
    conn.ping().await.unwrap();
    conn.close().await.unwrap();
    server.finish().await;
}

#[tokio::test]
async fn dropped_stream_retires_connection_without_draining_or_replaying() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("a")], vec![Cell::from("b")]],
        1,
        false,
    )
    .await;
    let mut conn = server.connect().await;
    let mut rows = sqlx::query("SELECT value").fetch(&mut conn);
    assert!(rows.try_next().await.unwrap().is_some());
    drop(rows);
    assert!(conn.ping().await.is_err());
    conn.close_hard().await.unwrap();
    server.finish().await;
}

#[tokio::test]
async fn transactions_reject_savepoints_and_drop_retires_the_connection() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("ok")]],
        1,
        false,
    )
    .await;
    let mut conn = server.connect().await;
    let mut tx = conn.begin().await.unwrap();
    assert!(tx.begin().await.is_err());
    tx.rollback().await.unwrap();
    conn.ping().await.unwrap();
    let tx = conn.begin().await.unwrap();
    drop(tx);
    assert!(conn.ping().await.is_err());
    conn.close_hard().await.unwrap();
    server.finish().await;
}

#[test]
fn doris_options_have_doris_identity_and_redact_passwords() {
    use sqlx::{ConnectOptions, Database};
    assert_eq!(sqlx_doris::Doris::NAME, "Doris");
    assert_eq!(DorisConnectOptions::new().get_port(), 9030);
    let raw = "doris://user:p%40ss@localhost:19030/db%20name?ssl-mode=verify-identity&statement-cache-capacity=8";
    let o: DorisConnectOptions = raw.parse().unwrap();
    assert_eq!(o.get_database(), Some("db name"));
    let back = DorisConnectOptions::from_url(&o.to_url_lossy()).unwrap();
    assert_eq!(back.get_username(), "user");
    assert_eq!(back.get_port(), 19030);
    assert!(!format!("{o:?}").contains("p@ss"));
    assert!(
        "mysql://localhost/db"
            .parse::<DorisConnectOptions>()
            .is_err()
    );
    assert!(
        "doris://localhost/db?typo=true"
            .parse::<DorisConnectOptions>()
            .is_err()
    );
}

#[tokio::test]
async fn disabled_statement_cache_closes_each_prepared_handle() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("ok")]],
        1,
        false,
    )
    .await;
    let options = DorisConnectOptions::new()
        .host("127.0.0.1")
        .port(server.port)
        .ssl_mode(DorisSslMode::Disabled)
        .statement_cache_capacity(0);
    let mut c = DorisConnection::connect_with(&options).await.unwrap();
    for _ in 0..2 {
        let _: String = sqlx::query_scalar("SELECT ?")
            .bind(42)
            .fetch_one(&mut c)
            .await
            .unwrap();
    }
    assert_eq!(c.cached_statements_size(), 0);
    c.close().await.unwrap();
    let commands = server.commands.clone();
    server.finish().await;
    let trace = commands.lock().unwrap().clone();
    assert_eq!(trace.iter().filter(|&&n| n == 22).count(), 2);
    assert_eq!(trace.iter().filter(|&&n| n == 25).count(), 2);
}

#[tokio::test]
async fn uncertain_commit_is_not_replayed_and_retires_connection() {
    use std::sync::atomic::Ordering;
    let server = Peer::start(vec![Column::new(253)], vec![], 1, false).await;
    let mut c = server.connect().await;
    server.fail_commit.store(true, Ordering::SeqCst);
    let tx = c.begin().await.unwrap();
    assert!(tx.commit().await.is_err());
    assert!(c.ping().await.is_err());
    c.close_hard().await.unwrap();
    server.finish().await;
}

#[tokio::test]
async fn missing_begin_acknowledgement_retires_connection() {
    let server = Peer::start(vec![Column::new(253)], vec![], 0, false).await;
    let mut c = server.connect().await;
    assert!(matches!(c.begin().await, Err(sqlx::Error::BeginFailed)));
    assert!(c.ping().await.is_err());
    c.close_hard().await.unwrap();
    server.finish().await;
}

#[tokio::test]
async fn explicit_prepare_cannot_bypass_pagination_text_binding() {
    use sqlx::{SqlStr, Statement};
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("ok")]],
        1,
        false,
    )
    .await;
    let mut c = server.connect().await;
    let statement = c
        .prepare(SqlStr::from_static("SELECT ? LIMIT ?"))
        .await
        .unwrap();
    for _ in 0..2 {
        let v: String = statement
            .query_scalar()
            .bind("ok")
            .bind(1_i64)
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(v, "ok");
    }
    c.close().await.unwrap();
    let trace = server.commands.clone();
    server.finish().await;
    assert!(!trace.lock().unwrap().contains(&23));
}

#[tokio::test]
async fn cached_query_checks_new_argument_count_before_sending() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("ok")]],
        1,
        false,
    )
    .await;
    let mut c = server.connect().await;
    let _: String = sqlx::query_scalar("SELECT ?")
        .bind(1)
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert!(
        sqlx::query("SELECT ?")
            .bind(1)
            .bind(2)
            .execute(&mut c)
            .await
            .is_err()
    );
    c.ping().await.unwrap();
    c.close().await.unwrap();
    let trace = server.commands.clone();
    server.finish().await;
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .iter()
            .filter(|&&tag| tag == 23)
            .count(),
        1
    );
}

#[tokio::test]
async fn unknown_column_types_fail_closed_and_retire_the_connection() {
    let server = Peer::start(vec![Column::new(99)], vec![], 1, false).await;
    let mut c = server.connect().await;
    assert!(sqlx::query("SELECT value").fetch_all(&mut c).await.is_err());
    assert!(c.ping().await.is_err());
    c.close_hard().await.unwrap();
    server.finish().await;
}

#[tokio::test]
async fn short_doris_prepare_reply_omits_only_the_optional_warnings() {
    use std::sync::atomic::Ordering;
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("ok")]],
        1,
        false,
    )
    .await;
    server.short_prepare.store(1, Ordering::SeqCst);
    let mut c = server.connect().await;
    let v: String = sqlx::query_scalar("SELECT ?")
        .bind(1_i64)
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(v, "ok");
    c.close().await.unwrap();
    server.finish().await;
}

#[tokio::test]
async fn cached_numeric_statement_rechecks_byte_and_time_argument_adaptation() {
    let server = Peer::start(
        vec![Column::new(253)],
        vec![vec![Cell::from("ok")]],
        1,
        false,
    )
    .await;
    let mut c = server.connect().await;
    for _ in 0..2 {
        let _: String = sqlx::query_scalar("SELECT ?")
            .bind(1_i64)
            .fetch_one(&mut c)
            .await
            .unwrap();
    }
    let _: String = sqlx::query_scalar("SELECT ?")
        .bind(vec![255_u8, 0])
        .fetch_one(&mut c)
        .await
        .unwrap();
    let _: String = sqlx::query_scalar("SELECT ?")
        .bind(sqlx_doris::types::Time::from_microseconds(-1).unwrap())
        .fetch_one(&mut c)
        .await
        .unwrap();
    c.close().await.unwrap();
    let trace = server.commands.clone();
    server.finish().await;
    let commands = trace.lock().unwrap();
    assert_eq!(commands.iter().filter(|&&n| n == 22).count(), 2);
    assert_eq!(commands.iter().filter(|&&n| n == 23).count(), 3);
}

#[tokio::test]
async fn prepare_metadata_extension_is_accepted_and_half_warning_count_is_rejected() {
    use std::sync::atomic::Ordering;
    for variant in [2, 3] {
        let server = Peer::start(
            vec![Column::new(253)],
            vec![vec![Cell::from("ok")]],
            1,
            false,
        )
        .await;
        server.short_prepare.store(variant, Ordering::SeqCst);
        let mut c = server.connect().await;
        let result = sqlx::query_scalar::<_, String>("SELECT ?")
            .bind(1)
            .fetch_one(&mut c)
            .await;
        if variant == 2 {
            assert_eq!(result.unwrap(), "ok");
            c.ping().await.unwrap();
        } else {
            assert!(result.is_err());
            assert!(c.ping().await.is_err());
        }
        c.close_hard().await.unwrap();
        server.finish().await;
    }
}
