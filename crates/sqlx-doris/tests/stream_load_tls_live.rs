#![cfg(feature = "stream-load")]
mod common;

use sqlx_doris::{
    Backend, Client, Error,
    operations::{Column, DataType, TableDefinition, TableModel, TableName},
    sqlx,
    stream_load::StreamLoadClient,
};
use std::{
    env,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct Proxy {
    child: Child,
    fe: String,
    be: String,
    events: PathBuf,
}
impl Proxy {
    fn start(flags: &[&str]) -> Self {
        let root =
            PathBuf::from(env::var_os("SQLX_DORIS_BUILD_ROOT").expect("scripts/dev required"));
        let directory = root.join("artifacts/http-tls").join(common::name().table);
        fs::create_dir_all(&directory).unwrap();
        let events = directory.join("proxy.jsonl");
        let child = Command::new("python3")
            .arg("-B")
            .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/support/tls_proxy.py"))
            .arg("--events")
            .arg(&events)
            .arg("--fe")
            .arg(env::var("DORIS_HTTP_URL").expect("dedicated FE required"))
            .arg("--be")
            .arg(env::var("DORIS_BE_HTTP_URL").expect("dedicated BE required"))
            .args(flags)
            .stdout(Stdio::piped())
            .stderr(File::create(directory.join("stderr.log")).unwrap())
            .spawn()
            .unwrap();
        let mut proxy = Self {
            child,
            fe: String::new(),
            be: String::new(),
            events,
        };
        let mut line = String::new();
        BufReader::new(proxy.child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        let endpoints: serde_json::Value =
            serde_json::from_str(&line).expect("TLS proxy startup; inspect stderr.log");
        proxy.fe = endpoints["fe"].as_str().unwrap().into();
        proxy.be = endpoints["be"].as_str().unwrap().into();
        proxy
    }
    fn events(&self) -> Vec<serde_json::Value> {
        fs::read_to_string(&self.events)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}
fn cert(name: &str) -> Vec<u8> {
    fs::read(
        PathBuf::from(env::var_os("SQLX_DORIS_BUILD_ROOT").unwrap())
            .join("fixtures/tls")
            .join(name),
    )
    .expect("run scripts/prepare-test-certificates first")
}
fn client(endpoint: &str, ca: &str, identity: bool) -> StreamLoadClient {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .tls_certs_only([reqwest::Certificate::from_pem(&cert(ca)).unwrap()]);
    if identity {
        let mut pem = cert("client-cert.pem");
        pem.extend(cert("client-key.pem"));
        builder = builder.identity(reqwest::Identity::from_pem(&pem).unwrap());
    }
    StreamLoadClient::with_client_builder(endpoint, "root", None, builder).unwrap()
}
async fn table(db: &Client) -> TableName {
    let name = common::name();
    db.create_table(
        &TableDefinition::new(name.clone(), TableModel::Duplicate, ["id"])
            .column(Column::new("id", DataType::BigInt).not_null())
            .column(Column::new("note", DataType::String))
            .replication(1),
    )
    .await
    .unwrap();
    name
}
async fn send_and_read(http: &StreamLoadClient, db: &Client, name: &TableName) {
    let note = "HTTPS 中文🙂 \" \\ \n \0";
    let receipt = http
        .load(name)
        .unwrap()
        .send_json(&serde_json::json!([{"id":1,"note":note}]))
        .await
        .unwrap();
    assert!(receipt.is_visible());
    assert_eq!(receipt.redirects, 1);
    assert_eq!(receipt.number_loaded_rows, 1);
    let value: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT note FROM {name} WHERE id=?"
    )))
    .bind(1_i64)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(value, note);
}

#[tokio::test]
#[ignore = "requires dedicated Doris HTTP/SQL and external TLS certificates"]
async fn custom_ca_https_and_private_be_override_preserve_the_real_load() {
    let db = common::client(Backend::Doris).await;
    let name = table(&db).await;
    let proxy = Proxy::start(&["--private-location"]);
    let http = client(&proxy.fe, "ca.pem", false)
        .backend_endpoint(&proxy.be)
        .unwrap();
    send_and_read(&http, &db, &name).await;
    let events = proxy.events();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["service"], "fe");
    assert_eq!(events[1]["service"], "be");
    assert_eq!(events[0]["body_sha256"], events[1]["body_sha256"]);
    assert!(events.iter().all(|e| e["basic_auth_present"] == true));
    assert!(events.iter().all(|e| e["expect_continue_present"] == true));
    common::drop_table(&db, &name).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris HTTP/SQL and external TLS certificates"]
async fn wrong_https_ca_and_hostname_are_rejected_before_upstream_requests() {
    let proxy = Proxy::start(&[]);
    let name = common::name();
    for (endpoint, ca, reason) in [
        (proxy.fe.clone(), "wrong-ca.pem", "UnknownIssuer"),
        (
            proxy.fe.replace("localhost", "127.0.0.1"),
            "ca.pem",
            "NotValidForName",
        ),
    ] {
        let error = client(&endpoint, ca, false)
            .load(&name)
            .unwrap()
            .send_bytes(b"[]".to_vec())
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Transport { .. }), "{error:?}");
        assert!(format!("{error:?}").contains(reason), "{error:?}");
    }
    assert!(proxy.events().is_empty());
}

#[tokio::test]
#[ignore = "requires dedicated Doris HTTP/SQL and external TLS certificates"]
async fn mtls_requires_a_client_certificate_and_accepts_the_signed_identity() {
    let db = common::client(Backend::Doris).await;
    let name = table(&db).await;
    let proxy = Proxy::start(&["--mtls"]);
    let error = client(&proxy.fe, "ca.pem", false)
        .load(&name)
        .unwrap()
        .send_bytes(b"[]".to_vec())
        .await
        .unwrap_err();
    assert!(matches!(error, Error::Transport { .. }), "{error:?}");
    assert!(proxy.events().is_empty());
    send_and_read(&client(&proxy.fe, "ca.pem", true), &db, &name).await;
    let events = proxy.events();
    assert_eq!(events.len(), 2);
    assert!(
        events
            .iter()
            .all(|e| e["client_certificate_present"] == true)
    );
    common::drop_table(&db, &name).await;
    db.close().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris HTTP/SQL and external TLS certificates"]
async fn https_to_http_redirect_is_rejected_without_a_backend_write() {
    let db = common::client(Backend::Doris).await;
    let name = table(&db).await;
    let proxy = Proxy::start(&["--downgrade"]);
    let error = client(&proxy.fe, "ca.pem", false)
        .load(&name)
        .unwrap()
        .send_json(&serde_json::json!([{"id":1,"note":"must not load"}]))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidInput(_)), "{error:?}");
    assert!(error.to_string().contains("downgrade"));
    let events = proxy.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["upstream_status"], 307);
    let count: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {name}")))
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(count, 0);
    common::drop_table(&db, &name).await;
    db.close().await;
}
