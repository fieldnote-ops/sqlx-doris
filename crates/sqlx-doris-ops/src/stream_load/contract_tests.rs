//! Exercise the public HTTP contract, including uncertain sends and file redirects.
use super::*;
#[cfg(unix)]
use std::path::PathBuf;
use std::{collections::BTreeMap, io};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

struct Request {
    line: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

async fn read(stream: &mut TcpStream) -> Request {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).await.unwrap();
    let mut headers = BTreeMap::new();
    loop {
        let mut header = String::new();
        assert_ne!(reader.read_line(&mut header).await.unwrap(), 0);
        if header == "\r\n" {
            break;
        }
        let (name, value) = header.split_once(':').unwrap();
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }
    if headers.contains_key("expect") {
        reader
            .get_mut()
            .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
            .await
            .unwrap();
    }
    let length = headers
        .get("content-length")
        .map_or(0, |v| v.parse::<usize>().unwrap());
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await.unwrap();
    Request {
        line,
        headers,
        body,
    }
}

async fn reply(stream: &mut TcpStream, status: &str, headers: &str, body: &str) -> io::Result<()> {
    stream.write_all(format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
        body.len(),
    ).as_bytes()).await
}

async fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    (listener, url)
}

fn client(url: &str) -> StreamLoadClient {
    StreamLoadClient::with_client_builder(
        url,
        "root",
        None,
        reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(3)),
    )
    .unwrap()
}

#[tokio::test]
async fn label_observation_is_one_encoded_get_and_preserves_unknown_states() {
    for (state, expected) in [
        ("UNKNOWN", LabelState::Unknown),
        ("PREPARE", LabelState::Prepare),
        ("PRECOMMITTED", LabelState::Precommitted),
        ("COMMITTED", LabelState::Committed),
        ("VISIBLE", LabelState::Visible),
        ("ABORTED", LabelState::Aborted),
        ("FUTURE", LabelState::Other("FUTURE".into())),
    ] {
        let (listener, url) = listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read(&mut stream).await;
            reply(
                &mut stream,
                "200 OK",
                "Content-Type: application/json\r\n",
                &format!(r#"{{"code":0,"data":"{state}"}}"#),
            )
            .await
            .unwrap();
            request
        });
        let observation = client(&url)
            .observe_label("app space", "a?b&c")
            .await
            .unwrap();
        assert_eq!(observation.state, expected);
        let request = server.await.unwrap();
        assert_eq!(
            request.line,
            "GET /api/app%20space/get_load_state?label=a%3Fb%26c HTTP/1.1\r\n"
        );
        assert!(request.body.is_empty());
    }
}

#[tokio::test]
async fn observation_errors_and_redirects_never_turn_into_missing_history() {
    for (status, headers, body) in [
        ("200 OK", "", r#"{"code":1,"data":"UNKNOWN"}"#),
        ("200 OK", "", r#"{"code":0,"data":null}"#),
        ("200 OK", "", "not json"),
        ("200 OK", "", r#"{"code":0,"data":""}"#),
        ("302 Found", "Location: http://127.0.0.1:1/\r\n", ""),
        ("403 Forbidden", "", ""),
    ] {
        let (listener, url) = listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read(&mut stream).await;
            reply(&mut stream, status, headers, body).await.unwrap();
            request
        });
        assert!(client(&url).observe_label("app", "batch").await.is_err());
        assert!(server.await.unwrap().line.starts_with("GET "));
    }
}

#[tokio::test]
async fn upload_identity_and_typed_options_reach_one_request() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read(&mut stream).await;
        reply(&mut stream, "200 OK", "", r#"{"Status":"Success"}"#)
            .await
            .unwrap();
        request
    });
    let http = client(&url);
    let request = http
        .load(&TableName::new("app", "events"))
        .unwrap()
        .columns("id,name")
        .unwrap()
        .partial_columns(true)
        .strict_mode(false)
        .max_filter_ratio(0.25)
        .unwrap();
    let label = request.label_ref().unwrap().to_owned();
    let receipt = request.send_bytes(b"payload".as_slice()).await.unwrap();
    assert_eq!(receipt.label, label);
    let request = server.await.unwrap();
    assert_eq!(request.headers["label"], label);
    assert_eq!(request.headers["columns"], "id,name");
    assert_eq!(request.headers["partial_columns"], "true");
    assert_eq!(request.headers["strict_mode"], "false");
    assert_eq!(request.headers["max_filter_ratio"], "0.25");
    assert_eq!(request.body, b"payload");
    assert!(
        http.load(&TableName::new("app", "events"))
            .unwrap()
            .group_commit(GroupCommit::Async)
            .label_ref()
            .is_none()
    );
    for ratio in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        assert!(
            http.load(&TableName::new("app", "events"))
                .unwrap()
                .max_filter_ratio(ratio)
                .is_err()
        );
    }
}

#[tokio::test]
async fn acknowledged_nonvisible_receipts_never_trigger_a_second_write() {
    for (body, mode, expected) in [
        (
            r#"{"Status":"Publish Timeout","TxnId":77}"#,
            None,
            LoadState::Committed,
        ),
        (
            r#"{"Status":"Success","TwoPhaseCommit":"true"}"#,
            None,
            LoadState::Prepared,
        ),
        (
            r#"{"Status":"Success","GroupCommit":true,"GroupCommitMode":"future_mode"}"#,
            Some(GroupCommit::Async),
            LoadState::Unconfirmed,
        ),
    ] {
        let (listener, url) = listener().await;
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read(&mut stream).await;
            reply(&mut stream, "200 OK", "", body).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
            request
        });
        let http = client(&url);
        let mut request = http.load(&TableName::new("app", "events")).unwrap();
        if let Some(mode) = mode {
            request = request.group_commit(mode);
        }
        let receipt = request.send_bytes(b"payload".as_slice()).await.unwrap();
        assert_eq!(receipt.state(), expected);
        assert!(!receipt.is_visible());
        assert!(server.await.unwrap().line.starts_with("PUT "));
    }
}

#[tokio::test]
async fn http_timeout_preserves_batch_identity_and_does_not_imply_rollback() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let (mut upload, _) = listener.accept().await.unwrap();
        let request = read(&mut upload).await;
        // Model an admitted write whose response exceeds the client's deadline.
        let (mut observation, _) = listener.accept().await.unwrap();
        assert!(read(&mut observation).await.line.starts_with("GET "));
        reply(
            &mut observation,
            "200 OK",
            "",
            r#"{"code":0,"data":"COMMITTED"}"#,
        )
        .await
        .unwrap();
        request
    });
    let http = StreamLoadClient::with_client_builder(
        &url,
        "root",
        None,
        reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(200)),
    )
    .unwrap();
    let request = http.load(&TableName::new("app", "events")).unwrap();
    let label = request.label_ref().unwrap().to_owned();
    assert!(matches!(request.send_bytes(b"payload".as_slice()).await,
        Err(Error::Transport { label: Some(value), source }) if value == label && source.is_timeout()));
    assert_eq!(
        http.observe_label("app", &label).await.unwrap().state,
        LabelState::Committed
    );
    assert_eq!(server.await.unwrap().headers["label"], label);
}

#[tokio::test]
async fn disconnected_upload_keeps_identity_and_only_read_observation_follows() {
    let (listener, url) = listener().await;
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let upload = read(&mut stream).await;
        assert!(upload.line.starts_with("PUT "));
        drop(stream); // No acknowledgement: the client cannot infer commit failure.
        let (mut stream, _) = listener.accept().await.unwrap();
        let observation = read(&mut stream).await;
        assert!(observation.line.starts_with("GET "));
        reply(&mut stream, "200 OK", "", r#"{"code":0,"data":"UNKNOWN"}"#)
            .await
            .unwrap();
        upload
    });
    let http = client(&url);
    let request = http.load(&TableName::new("app", "events")).unwrap();
    let label = request.label_ref().unwrap().to_owned();
    assert!(matches!(request.send_bytes(b"payload".as_slice()).await,
        Err(Error::Transport { label: Some(value), .. }) if value == label));
    assert_eq!(
        http.observe_label("app", &label).await.unwrap().state,
        LabelState::Unknown
    );
    assert_eq!(server.await.unwrap().headers["label"], label);
}

#[tokio::test]
async fn cancellation_after_upload_never_resubmits_before_read_only_observation() {
    let (listener, url) = listener().await;
    let (sent, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let upload = read(&mut stream).await;
        assert!(upload.line.starts_with("PUT "));
        sent.send(()).unwrap();
        // Keep this admitted request unanswered while the client cancels.
        let (mut observation_stream, _) = listener.accept().await.unwrap();
        let observation = read(&mut observation_stream).await;
        assert!(observation.line.starts_with("GET "));
        reply(
            &mut observation_stream,
            "200 OK",
            "",
            r#"{"code":0,"data":"VISIBLE"}"#,
        )
        .await
        .unwrap();
        let _ = reply(&mut stream, "200 OK", "", r#"{"Status":"Success"}"#).await;
        upload
    });
    let http = client(&url);
    let request = http.load(&TableName::new("app", "events")).unwrap();
    let label = request.label_ref().unwrap().to_owned();
    {
        let upload = request.send_bytes(b"payload".as_slice());
        tokio::pin!(upload);
        tokio::select! {
            result = &mut upload => panic!("unexpected response: {result:?}"),
            result = received => result.unwrap(),
        }
    }
    assert_eq!(
        http.observe_label("app", &label).await.unwrap().state,
        LabelState::Visible
    );
    assert_eq!(server.await.unwrap().headers["label"], label);
}

#[cfg(unix)]
fn input_directory() -> PathBuf {
    use std::os::unix::fs::MetadataExt;
    let volume = PathBuf::from(std::env::var_os("SQLX_DORIS_EXTERNAL_VOLUME").unwrap());
    let base = PathBuf::from(std::env::var_os("SQLX_DORIS_BUILD_ROOT").unwrap());
    assert!(base.starts_with(&volume));
    assert_eq!(
        base.metadata().unwrap().dev(),
        volume.metadata().unwrap().dev()
    );
    assert_ne!(
        volume.metadata().unwrap().dev(),
        Path::new("/").metadata().unwrap().dev()
    );
    let path = base.join(format!("stream-source-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&path).unwrap();
    path
}

#[cfg(unix)]
#[tokio::test]
async fn redirect_keeps_the_opened_file_when_its_path_is_replaced() {
    let directory = input_directory();
    let path = directory.join("input");
    std::fs::write(&path, b"original bytes").unwrap();
    let (backend, backend_url) = listener().await;
    let be = tokio::spawn(async move {
        let (mut stream, _) = backend.accept().await.unwrap();
        let request = read(&mut stream).await;
        reply(&mut stream, "200 OK", "", r#"{"Status":"Success"}"#)
            .await
            .unwrap();
        request
    });
    let (frontend, url) = listener().await;
    let source_path = path.clone();
    let fe = tokio::spawn(async move {
        let (mut stream, _) = frontend.accept().await.unwrap();
        let request = read(&mut stream).await;
        std::fs::rename(&source_path, source_path.with_extension("original")).unwrap();
        std::fs::write(&source_path, b"replacement bytes").unwrap();
        reply(
            &mut stream,
            "307 Temporary Redirect",
            &format!("Location: {backend_url}/load\r\n"),
            "",
        )
        .await
        .unwrap();
        request
    });
    let receipt = client(&url)
        .load(&TableName::new("app", "events"))
        .unwrap()
        .send_file(&path)
        .await
        .unwrap();
    assert_eq!(receipt.redirects, 1);
    assert_eq!(fe.await.unwrap().body, b"original bytes");
    assert_eq!(be.await.unwrap().body, b"original bytes");
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement bytes");
    std::fs::remove_dir_all(directory).unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn in_place_change_before_redirect_refuses_the_second_upload_with_identity() {
    let directory = input_directory();
    let path = directory.join("input");
    std::fs::write(&path, b"original bytes").unwrap();
    let (backend, backend_url) = listener().await;
    let (frontend, url) = listener().await;
    let source_path = path.clone();
    let fe = tokio::spawn(async move {
        let (mut stream, _) = frontend.accept().await.unwrap();
        let request = read(&mut stream).await;
        std::fs::write(source_path, b"changed").unwrap();
        reply(
            &mut stream,
            "307 Temporary Redirect",
            &format!("Location: {backend_url}/load\r\n"),
            "",
        )
        .await
        .unwrap();
        request
    });
    let http = client(&url);
    let request = http.load(&TableName::new("app", "events")).unwrap();
    let label = request.label_ref().unwrap().to_owned();
    assert!(matches!(request.send_file(&path).await,
        Err(Error::LoadSource { label: Some(value), .. }) if value == label));
    assert_eq!(fe.await.unwrap().body, b"original bytes");
    assert!(
        tokio::time::timeout(Duration::from_millis(50), backend.accept())
            .await
            .is_err()
    );
    std::fs::remove_dir_all(directory).unwrap();
}
