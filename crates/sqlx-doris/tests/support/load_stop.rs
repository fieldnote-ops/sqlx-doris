//! Real FE/BE HTTP forwarding with deterministic delivery/response faults.
//! Only endpoint routing and transport are changed; Doris owns all job state.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};

#[derive(Clone, Copy, PartialEq)]
enum Fault {
    Pass,
    Before,
    LostAck,
    Identity,
    Redirect,
    BackendRedirect,
}
struct HttpProxy {
    url: String,
    submitted: Arc<AtomicUsize>,
    forwarded: Arc<AtomicUsize>,
    acknowledged: Arc<AtomicUsize>,
    release: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for HttpProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl HttpProxy {
    async fn start(fault: Fault) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let submitted = Arc::new(AtomicUsize::new(0));
        let forwarded = Arc::new(AtomicUsize::new(0));
        let acknowledged = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Notify::new());
        let (s, f, a, r) = (
            submitted.clone(),
            forwarded.clone(),
            acknowledged.clone(),
            release.clone(),
        );
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (s, f, a, r) = (s.clone(), f.clone(), a.clone(), r.clone());
                tasks.spawn(async move {
                    let mut bytes = Vec::new();
                    loop {
                        let byte = socket.read_u8().await.unwrap();
                        bytes.push(byte);
                        assert!(bytes.len() < 65536);
                        if bytes.ends_with(b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8(bytes).unwrap();
                    let mut lines = request.split("\r\n");
                    let mut first = lines.next().unwrap().split_whitespace();
                    let method = first.next().unwrap().parse::<reqwest::Method>().unwrap();
                    let target = reqwest::Url::parse(first.next().unwrap()).unwrap();
                    let fe =
                        reqwest::Url::parse(&std::env::var("DORIS_HTTP_URL").unwrap()).unwrap();
                    let is_fe = target.host_str() == fe.host_str()
                        && target.port_or_known_default() == fe.port_or_known_default();
                    let is_abort = method == reqwest::Method::PUT && !is_fe;
                    if is_abort {
                        assert!(target.path().ends_with("/_stream_load_2pc"));
                        s.fetch_add(1, Ordering::SeqCst);
                        if fault == Fault::Before {
                            r.notified().await;
                        }
                    }
                    let base = if is_fe {
                        fe
                    } else {
                        reqwest::Url::parse(&std::env::var("DORIS_BE_HTTP_URL").unwrap()).unwrap()
                    };
                    let client = reqwest::Client::builder()
                        .no_proxy()
                        .redirect(reqwest::redirect::Policy::none())
                        .retry(reqwest::retry::never())
                        .build()
                        .unwrap();
                    let mut builder = client.request(method, base.join(target.path()).unwrap());
                    for line in lines.filter(|s| !s.is_empty()) {
                        let (key, value) = line.split_once(':').unwrap();
                        if ["authorization", "txn_id", "txn_operation"]
                            .contains(&key.to_ascii_lowercase().as_str())
                        {
                            builder = builder.header(key, value.trim());
                        }
                        assert!(!key.eq_ignore_ascii_case("label"));
                    }
                    if is_abort {
                        f.fetch_add(1, Ordering::SeqCst);
                    }
                    let response = builder.send().await.unwrap();
                    let mut status = response.status().as_u16();
                    let mut location = response
                        .headers()
                        .get("location")
                        .map(|v| v.to_str().unwrap().to_owned());
                    let mut body = response.bytes().await.unwrap().to_vec();
                    if is_abort {
                        a.fetch_add(1, Ordering::SeqCst);
                        if fault == Fault::LostAck {
                            return;
                        }
                        if fault == Fault::BackendRedirect {
                            status = 307;
                            location = Some("http://127.0.0.1:9/never".into());
                        }
                    }
                    if fault == Fault::Identity && target.path().ends_with("/node/frontends") {
                        let mut value: serde_json::Value = serde_json::from_slice(&body).unwrap();
                        let index = value["data"]["columnNames"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .position(|v| v == "ClusterId")
                            .unwrap();
                        value["data"]["rows"][0][index] = "wrong-cluster".into();
                        body = serde_json::to_vec(&value).unwrap();
                    }
                    if fault == Fault::Redirect
                        && is_fe
                        && target.path().ends_with("/_stream_load_2pc")
                    {
                        location = Some("http://127.0.0.1:9/never".into());
                    }
                    let mut head = format!(
                        "HTTP/1.1 {status} Fixture\r\nConnection: close\r\nContent-Length: {}\r\n",
                        body.len()
                    );
                    if let Some(location) = location {
                        head += &format!("Location: {location}\r\n");
                    }
                    head += "\r\n";
                    // A queued request can reach Doris after the CLI exits.
                    let _ = socket.write_all(head.as_bytes()).await;
                    let _ = socket.write_all(&body).await;
                });
            }
        });
        Self {
            url,
            submitted,
            forwarded,
            acknowledged,
            release,
            task,
        }
    }
    async fn count(&self, field: &AtomicUsize, expected: usize) {
        tokio::time::timeout(Duration::from_secs(15), async {
            while field.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    fn spawn(
        &self,
        url: &str,
        name: &str,
        job: &str,
        txn: &str,
        force: bool,
        timeout: &str,
    ) -> (std::process::Child, PathBuf, PathBuf) {
        let root = PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap())
            .join("load-stop-contracts");
        std::fs::create_dir_all(&root).unwrap();
        let out = root.join(format!("{}.out", uuid::Uuid::new_v4().simple()));
        let err = out.with_extension("err");
        let mut command = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"));
        command.env("DATABASE_URL", url);
        for key in [
            "HTTP_PROXY",
            "http_proxy",
            "HTTPS_PROXY",
            "https_proxy",
            "ALL_PROXY",
            "all_proxy",
        ] {
            command.env(key, &self.url);
        }
        command.env("NO_PROXY", "").env("no_proxy", "");
        command.args([
            "--replication-num",
            "1",
            "--lock-timeout",
            "0",
            "--ddl-timeout",
            timeout,
            "admin",
            "drain",
            "cancel-load",
            "--name",
            name,
            "--job-id",
            job,
        ]);
        if force {
            command.args(["--transaction-id", txn, "--maintenance", "--force"]);
        }
        command
            .stdout(Stdio::from(std::fs::File::create(&out).unwrap()))
            .stderr(Stdio::from(std::fs::File::create(&err).unwrap()));
        (command.spawn().unwrap(), out, err)
    }
    async fn call(
        &self,
        url: &str,
        name: &str,
        job: &str,
        txn: &str,
        force: bool,
        success: bool,
    ) -> String {
        let (child, out, err) = self.spawn(url, name, job, txn, force, "30");
        exited(child, &err, success).await;
        format!(
            "{}{}",
            std::fs::read_to_string(out).unwrap(),
            std::fs::read_to_string(err).unwrap()
        )
    }
}
async fn bound(
    c: &mut DorisConnection,
    store: &object_store::Store,
    label: &str,
) -> (String, String) {
    let job = load(c, store, label).await;
    let txn = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let rows = c.fetch_all("SHOW LOAD").await.unwrap();
            let row = rows
                .iter()
                .find(|r| r.get::<&str, _>("JobId") == job)
                .unwrap();
            assert!(matches!(row.get::<&str, _>("State"), "PENDING" | "LOADING"));
            let txn = row.get::<String, _>("TransactionId");
            if txn != "0" {
                break txn;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    (job, txn)
}
async fn state(c: &mut DorisConnection, job: &str) -> String {
    c.fetch_all("SHOW LOAD")
        .await
        .unwrap()
        .iter()
        .find(|r| r.get::<&str, _>("JobId") == job)
        .unwrap()
        .get("State")
}

#[tokio::test]
#[ignore = "requires real Doris HTTP and external load fixture"]
async fn load_abort_preview_exact_binding_repetition_and_recovery_guard() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    seed_owner(&mut c, true, GATE_OWNER).await;
    let store = object_store::Store::start().await;
    let (job, txn) = bound(&mut c, &store, "l1").await;
    let proxy = HttpProxy::start(Fault::Pass).await;
    assert!(
        proxy
            .call(&url, &account.name, &job, &txn, false, true)
            .await
            .contains("Preview only")
    );
    proxy
        .call(&url, &account.name, &job, "1", true, false)
        .await;
    assert_eq!(proxy.submitted.load(Ordering::SeqCst), 0);
    guarded(&url, &account.name, &owner, false).await;
    assert!(
        proxy
            .call(&url, &account.name, &job, &txn, true, true)
            .await
            .contains("CANCELLED; exact transaction")
    );
    assert_eq!(state(&mut c, &job).await, "CANCELLED");
    proxy
        .call(&url, &account.name, &job, &txn, true, false)
        .await;
    assert_eq!(proxy.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(proxy.forwarded.load(Ordering::SeqCst), 1);
    assert_eq!(
        c.migration_transition_owner().await.unwrap().as_deref(),
        Some(GATE_OWNER)
    );
    assert_eq!(
        c.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner.as_str())
    );
    assert_eq!(c.dirty_version("_sqlx_migrations").await.unwrap(), Some(1));
    c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
        .await
        .unwrap();
    // Explicit transition recovery intentionally retires its connection.
    let c = DorisConnection::connect_with(&opts).await.unwrap();
    store.release();
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires real Doris HTTP and external load fixture"]
async fn late_load_abort_never_cancels_reused_label_or_undoes_committed_data() {
    for commit in [false, true] {
        let (mut c, url, _) = fixture().await;
        let (account, owner) = seed(&mut c, &url).await;
        let old_store = object_store::Store::start().await;
        let (job, txn) = bound(&mut c, &old_store, "l1").await;
        let proxy = HttpProxy::start(Fault::Before).await;
        let (child, _, err) = proxy.spawn(&url, &account.name, &job, &txn, true, "30");
        proxy.count(&proxy.submitted, 1).await;
        if commit {
            old_store.release();
            tokio::time::timeout(Duration::from_secs(20), async {
                while state(&mut c, &job).await != "FINISHED" {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap();
        } else {
            c.execute("CANCEL LOAD WHERE LABEL='l1'").await.unwrap();
        }
        let database: String = sqlx::query_scalar("SELECT DATABASE()")
            .fetch_one(&mut c)
            .await
            .unwrap();
        c.execute(sqlx::AssertSqlSafe(format!(
            "CLEAN LABEL l1 FROM `{database}`"
        )))
        .await
        .unwrap();
        let new_store = object_store::Store::start().await;
        let (next, next_txn) = bound(&mut c, &new_store, "l1").await;
        assert_ne!(job, next);
        assert_ne!(txn, next_txn);
        proxy.release.notify_one();
        exited(child, &err, false).await;
        assert!(
            std::fs::read_to_string(err)
                .unwrap()
                .contains("unconfirmed/rejected")
        );
        assert!(matches!(
            state(&mut c, &next).await.as_str(),
            "PENDING" | "LOADING"
        ));
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&mut c)
            .await
            .unwrap();
        assert_eq!(rows, if commit { 1 } else { 0 });
        assert_eq!(proxy.forwarded.load(Ordering::SeqCst), 1);
        let direct = HttpProxy::start(Fault::Pass).await;
        direct
            .call(&url, &account.name, &next, &next_txn, true, true)
            .await;
        old_store.release();
        new_store.release();
        cleanup(c, &url, account, &owner).await;
    }
}

#[tokio::test]
#[ignore = "requires real Doris HTTP and external load fixture"]
async fn load_abort_lost_ack_killed_cli_and_deadline_never_replay_or_recover() {
    for mode in [0, 1, 2] {
        let (mut c, url, _) = fixture().await;
        let (account, owner) = seed(&mut c, &url).await;
        let store = object_store::Store::start().await;
        let (job, txn) = bound(&mut c, &store, "l1").await;
        let proxy = HttpProxy::start(if mode == 0 {
            Fault::LostAck
        } else {
            Fault::Before
        })
        .await;
        let (mut child, _, err) = proxy.spawn(
            &url,
            &account.name,
            &job,
            &txn,
            true,
            if mode == 2 { "2" } else { "30" },
        );
        proxy.count(&proxy.submitted, 1).await;
        if mode == 1 {
            child.kill().unwrap();
        }
        if mode == 2 {
            exited(child, &err, false).await;
            assert!(
                std::fs::read_to_string(&err)
                    .unwrap()
                    .contains("LOAD ABORT submitted; outcome uncertain")
            );
        } else {
            exited(child, &err, false).await;
        }
        if mode != 0 {
            proxy.release.notify_one();
        }
        proxy.count(&proxy.acknowledged, 1).await;
        assert_eq!(state(&mut c, &job).await, "CANCELLED");
        assert_eq!(proxy.submitted.load(Ordering::SeqCst), 1);
        assert_eq!(proxy.forwarded.load(Ordering::SeqCst), 1);
        store.release();
        cleanup(c, &url, account, &owner).await;
    }
}

#[tokio::test]
#[ignore = "requires real Doris HTTP and external load fixture"]
async fn load_abort_rejects_http_identity_and_redirect_drift_without_fallback() {
    for fault in [Fault::Identity, Fault::Redirect, Fault::BackendRedirect] {
        let (mut c, url, _) = fixture().await;
        let (account, owner) = seed(&mut c, &url).await;
        let store = object_store::Store::start().await;
        let (job, txn) = bound(&mut c, &store, "l1").await;
        let proxy = HttpProxy::start(fault).await;
        proxy
            .call(&url, &account.name, &job, &txn, true, false)
            .await;
        let sent = if fault == Fault::BackendRedirect {
            1
        } else {
            0
        };
        assert_eq!(proxy.submitted.load(Ordering::SeqCst), sent);
        assert_eq!(proxy.forwarded.load(Ordering::SeqCst), sent);
        if sent == 0 {
            assert!(matches!(
                state(&mut c, &job).await.as_str(),
                "PENDING" | "LOADING"
            ));
            HttpProxy::start(Fault::Pass)
                .await
                .call(&url, &account.name, &job, &txn, true, true)
                .await;
        } else {
            assert_eq!(state(&mut c, &job).await, "CANCELLED");
        }
        store.release();
        cleanup(c, &url, account, &owner).await;
    }
}

#[tokio::test]
#[ignore = "requires real Doris HTTP and external load fixture"]
async fn acknowledged_load_abort_with_failed_verification_is_not_replayed() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    let store = object_store::Store::start().await;
    let (job, txn) = bound(&mut c, &store, "l1").await;
    let proxy = HttpProxy::start(Fault::Pass).await;
    let mut sql = Proxy::start_main(&opts, Point::LoadRead(3)).await;
    let (child, _, err) = proxy.spawn(
        sql.options.to_url_lossy().as_str(),
        &account.name,
        &job,
        &txn,
        true,
        "30",
    );
    sql.reached().await;
    sql.release(Release::Disconnect);
    exited(child, &err, false).await;
    assert!(
        std::fs::read_to_string(err)
            .unwrap()
            .contains("LOAD ABORT acknowledged; terminal verification failed")
    );
    assert_eq!(proxy.forwarded.load(Ordering::SeqCst), 1);
    assert_eq!(state(&mut c, &job).await, "CANCELLED");
    sql.finish().await;
    store.release();
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires real Doris and external Kafka fixture"]
async fn routine_load_name_only_stop_really_stops_a_successor_after_final_id_check() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    let first = routine(&mut c).await;
    let mut proxy = Proxy::start_routine(&opts, Point::BeforeDrop).await;
    let options = proxy.options.clone();
    let pending = tokio::spawn(async move {
        let mut old = DorisConnection::connect_with(&options).await.unwrap();
        // Even an ID check immediately before this cannot condition the STOP.
        let rows = old.fetch_all("SHOW ALL ROUTINE LOAD FOR rl").await.unwrap();
        assert!(rows.iter().any(|r| r.get::<&str, _>("Id") == first));
        old.execute("STOP ROUTINE LOAD FOR rl").await.unwrap();
        old.close().await.unwrap();
    });
    proxy.reached().await;
    c.execute("STOP ROUTINE LOAD FOR rl").await.unwrap();
    let next = routine(&mut c).await;
    proxy.release(Release::Forward);
    pending.await.unwrap();
    proxy.finish().await;
    assert_eq!(
        c.fetch_all("SHOW ALL ROUTINE LOAD FOR rl")
            .await
            .unwrap()
            .iter()
            .find(|r| r.get::<&str, _>("Id") == next)
            .unwrap()
            .get::<&str, _>("State"),
        "STOPPED"
    );
    // Neither an ID predicate nor an atomic rename is exposed by this grammar.
    assert!(
        c.execute("STOP ROUTINE LOAD FOR rl WHERE Id=1")
            .await
            .is_err()
    );
    assert!(
        c.execute("ALTER ROUTINE LOAD rl RENAME rl_retired")
            .await
            .is_err()
    );
    cleanup(c, &url, account, &owner).await;
}
