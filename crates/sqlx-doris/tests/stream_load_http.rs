#![cfg(feature = "stream-load")]

use sqlx_doris::{
    Error, GroupCommit,
    operations::TableName,
    stream_load::{LoadFormat, LoadState, StreamLoadClient},
};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Debug)]
struct Request {
    line: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

async fn read_request(socket: &mut TcpStream) -> Request {
    let mut bytes = Vec::new();
    let end = loop {
        let mut chunk = [0; 1024];
        let n = socket.read(&mut chunk).await.unwrap();
        assert_ne!(n, 0, "request closed before headers");
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let head = std::str::from_utf8(&bytes[..end]).unwrap();
    let mut lines = head.split("\r\n");
    let line = lines.next().unwrap().to_owned();
    let headers: BTreeMap<_, _> = lines
        .filter_map(|s| s.split_once(':'))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    let len: usize = headers["content-length"].parse().unwrap();
    if headers.get("expect").is_some_and(|v| v == "100-continue") {
        socket
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
            .unwrap();
    }
    let mut body = bytes[end..].to_vec();
    while body.len() < len {
        let mut chunk = vec![0; len - body.len()];
        let n = socket.read(&mut chunk).await.unwrap();
        assert_ne!(n, 0, "request closed before body");
        body.extend_from_slice(&chunk[..n]);
    }
    assert_eq!(body.len(), len);
    Request {
        line,
        headers,
        body,
    }
}

async fn respond(socket: &mut TcpStream, status: &str, extra: &str, body: &str) {
    socket
        .write_all(
            format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nContent-Type: \
        application/json\r\nConnection: close\r\n{extra}\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
}
async fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}
fn url(listener: &TcpListener) -> String {
    format!("http://{}", listener.local_addr().unwrap())
}
fn client(endpoint: &str) -> StreamLoadClient {
    StreamLoadClient::with_client_builder(
        endpoint,
        "user",
        Some("secret".into()),
        reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3)),
    )
    .unwrap()
}

#[tokio::test]
async fn json_nan_and_infinity_are_rejected_before_transport_for_both_json_paths() {
    use sqlx_doris::operations::{Column, DataType, TableDefinition, TableModel};
    #[derive(serde::Serialize)]
    struct Row {
        id: i64,
        readings: Vec<f64>,
    }
    let http = client("http://127.0.0.1:1");
    let schema = TableDefinition::new(TableName::new("db", "table"), TableModel::Duplicate, ["id"])
        .column(Column::new("id", DataType::BigInt))
        .column(Column::new("readings", DataType::array(DataType::Double)));
    for number in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        for request in [
            http.load(&schema.name).unwrap(),
            http.load_table(&schema).unwrap(),
        ] {
            let error = request
                .send_json(&[Row {
                    id: 1,
                    readings: vec![number],
                }])
                .await
                .unwrap_err();
            assert!(matches!(error, Error::Json(_)));
            assert!(error.to_string().contains("non-finite"));
        }
    }
}

#[tokio::test]
async fn file_redirect_preserves_body_auth_label_and_headers() {
    let fe = listener().await;
    let be = listener().await;
    let endpoint = url(&fe);
    let target = url(&be);
    let fe_task = tokio::spawn(async move {
        let (mut socket, _) = fe.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        respond(
            &mut socket,
            "307 Temporary Redirect",
            &format!("Location: {target}/api/db/table/_stream_load?route=be\r\n"),
            "",
        )
        .await;
        request
    });
    let be_task = tokio::spawn(async move {
        let (mut socket, _) = be.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        respond(
            &mut socket,
            "200 OK",
            "",
            r#"{"Status":"Success","Label":"file-label","NumberLoadedRows":2}"#,
        )
        .await;
        request
    });
    // scripts/dev sets TMPDIR to the external drive for project development.
    let path = std::env::temp_dir().join(format!("load-{}.csv", uuid::Uuid::new_v4()));
    let payload = "1,中文🙂\n2,second\n";
    tokio::fs::write(&path, payload).await.unwrap();
    let receipt = client(&endpoint)
        .load(&TableName::new("db", "table"))
        .unwrap()
        .label("file-label")
        .format(LoadFormat::Csv)
        .header("column_separator", ",")
        .unwrap()
        .send_file(&path)
        .await
        .unwrap();
    tokio::fs::remove_file(path).await.unwrap();
    assert_eq!(receipt.state(), LoadState::Visible);
    assert_eq!(receipt.redirects, 1);
    let first = fe_task.await.unwrap();
    let second = be_task.await.unwrap();
    assert_eq!(first.line, "PUT /api/db/table/_stream_load HTTP/1.1");
    assert_eq!(
        second.line,
        "PUT /api/db/table/_stream_load?route=be HTTP/1.1"
    );
    for request in [first, second] {
        assert_eq!(request.body, payload.as_bytes());
        assert_eq!(request.headers["authorization"], "Basic dXNlcjpzZWNyZXQ=");
        assert_eq!(request.headers["label"], "file-label");
        assert_eq!(request.headers["format"], "csv");
        assert_eq!(request.headers["column_separator"], ",");
    }
}

#[tokio::test]
async fn http_200_failure_retains_counts_label_and_error_url() {
    let server = listener().await;
    let endpoint = url(&server);
    let task = tokio::spawn(async move {
        let (mut socket, _) = server.accept().await.unwrap();
        read_request(&mut socket).await;
        respond(
            &mut socket,
            "200 OK",
            "",
            "{\"Status\":\"Fail\",\
            \"Message\":\"filtered rows\",\
            \"NumberTotalRows\":2,\
            \"NumberFilteredRows\":1,\
            \"ErrorURL\":\"http://be/errors/fixture\"}",
        )
        .await;
    });
    let error = client(&endpoint)
        .load(&TableName::new("db", "table"))
        .unwrap()
        .label("failed-label")
        .send_json(&[serde_json::json!({ "id": 1 })])
        .await
        .unwrap_err();
    match error {
        Error::Load { receipt } => {
            assert_eq!(receipt.label, "failed-label");
            assert_eq!(receipt.number_total_rows, 2);
            assert_eq!(receipt.number_filtered_rows, 1);
            assert_eq!(
                receipt.error_url.as_deref(),
                Some("http://be/errors/fixture")
            );
        }
        other => panic!("unexpected error: {other}"),
    }
    task.await.unwrap();
}

#[tokio::test]
async fn publish_timeout_is_committed_and_is_never_resubmitted() {
    let server = listener().await;
    let endpoint = url(&server);
    let task = tokio::spawn(async move {
        let (mut socket, _) = server.accept().await.unwrap();
        read_request(&mut socket).await;
        respond(
            &mut socket,
            "200 OK",
            "",
            r#"{"Status":"Publish Timeout","Label":"committed","TxnId":17}"#,
        )
        .await;
        drop(socket);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), server.accept())
                .await
                .is_err()
        );
    });
    let receipt = client(&endpoint)
        .load(&TableName::new("db", "table"))
        .unwrap()
        .label("committed")
        .send_bytes(b"[]".to_vec())
        .await
        .unwrap();
    assert_eq!(receipt.state(), LoadState::Committed);
    assert!(!receipt.is_visible());
    assert_eq!(receipt.txn_id, Some(17));
    task.await.unwrap();
}

#[tokio::test]
async fn lost_response_after_receiving_the_body_is_uncertain_without_write_retry() {
    let server = listener().await;
    let endpoint = url(&server);
    let task = tokio::spawn(async move {
        let (mut socket, _) = server.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        assert_eq!(request.body, b"[1]");
        drop(socket); // The write may already have committed; its response is lost.
        assert!(
            tokio::time::timeout(Duration::from_millis(200), server.accept())
                .await
                .is_err()
        );
    });
    let error = client(&endpoint)
        .load(&TableName::new("db", "table"))
        .unwrap()
        .label("uncertain")
        .send_bytes(b"[1]".to_vec())
        .await
        .unwrap_err();
    assert!(matches!(error,Error::Transport {label:Some(label),..} if label=="uncertain"));
    task.await.unwrap();
}

#[tokio::test]
async fn group_commit_omits_the_label_and_reports_server_acceptance() {
    for server_mode in [Some("async_mode"), None, Some("sync_mode")] {
        let server = listener().await;
        let endpoint = url(&server);
        let task = tokio::spawn(async move {
            let (mut socket, _) = server.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(!request.headers.contains_key("label"));
            assert_eq!(request.headers["group_commit"], "async_mode");
            let mut body = serde_json::json!({
                "Status": "Success",
                "GroupCommit": true,
                "GroupCommitMode": server_mode,
                "Label": "group-assigned"
            });
            if server_mode.is_none() {
                body.as_object_mut().unwrap().remove("GroupCommitMode");
            }
            respond(&mut socket, "200 OK", "", &body.to_string()).await;
        });
        let receipt = client(&endpoint)
            .load(&TableName::new("db", "table"))
            .unwrap()
            .group_commit(GroupCommit::Async)
            .send_json(&[serde_json::json!({ "id": 1 })])
            .await
            .unwrap();
        assert_eq!(
            receipt.state(),
            if server_mode == Some("sync_mode") {
                LoadState::Visible
            } else {
                LoadState::Accepted
            }
        );
        assert_eq!(receipt.is_visible(), server_mode == Some("sync_mode"));
        assert_eq!(receipt.group_commit_mode.as_deref(), server_mode);
        assert_eq!(
            receipt.requested_group_commit_mode.as_deref(),
            Some("async_mode")
        );
        let restored: sqlx_doris::stream_load::LoadReceipt =
            serde_json::from_value(serde_json::to_value(&receipt).unwrap()).unwrap();
        assert_eq!(restored.state(), receipt.state());
        assert_eq!(receipt.label, "group-assigned");
        task.await.unwrap();
        let error = client(&endpoint)
            .load(&TableName::new("db", "table"))
            .unwrap()
            .header("Label", "explicit")
            .unwrap()
            .group_commit(GroupCommit::Async)
            .send_bytes(b"[]".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::InvalidInput(_)));
    }
}
