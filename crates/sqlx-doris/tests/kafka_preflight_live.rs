#![cfg(feature = "cli-admin-kafka")]
//! Dedicated Kafka source mutations are fixture setup; product commands are read-only.
use serde_json::{Value, json};
use std::{
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
#[path = "support/kafka_tool.rs"]
mod kafka_tool;
use kafka_tool::run as kafka;
#[path = "support/routine_matrix/kafka.rs"]
mod broker;
fn root() -> PathBuf {
    let p = PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap())
        .join("kafka-preflight-contracts")
        .join(uuid::Uuid::new_v4().simple().to_string());
    std::fs::create_dir_all(&p).unwrap();
    p
}
fn connection(sasl: bool) -> PathBuf {
    std::env::var(if sasl {
        "DORIS_KAFKA_SASL_CONNECTION_FILE"
    } else {
        "DORIS_KAFKA_CONNECTION_FILE"
    })
    .expect("private external Kafka connection files required")
    .into()
}
fn private(path: &Path, value: &Value) {
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    write!(f, "{value}").unwrap();
}
fn call(config: &Path, args: &[&str], code: i32) -> Value {
    let p = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
        .args(["admin", "kafka"])
        .args(args)
        .args(["--connection-file", config.to_str().unwrap(), "--json"])
        .env("DATABASE_URL", "not-a-doris-url-kafka-must-not-connect")
        .output()
        .unwrap();
    assert_eq!(
        p.status.code(),
        Some(code),
        "stdout {} stderr {}",
        String::from_utf8_lossy(&p.stdout),
        String::from_utf8_lossy(&p.stderr)
    );
    assert!(p.stderr.is_empty());
    let v: Value = serde_json::from_slice(&p.stdout).unwrap();
    assert_eq!(v["schema"], "sqlx-doris-cli-result-v1");
    assert_eq!(v["exit_code"], code);
    assert_eq!(v["server_read_only"], true);
    for key in [
        "safe_to_recover",
        "kafka_continuation_verified",
        "automatic_offset_transfer",
        "automatic_offset_reset",
        "atomic_snapshot",
    ] {
        assert_eq!(v[key], false);
    }
    v
}
fn snapshot(topic: &str, path: &Path) -> Value {
    call(
        &connection(false),
        &[
            "snapshot",
            "--topic",
            topic,
            "--output",
            path.to_str().unwrap(),
        ],
        0,
    )
}
fn preflight(topic: &str, cp: Option<&Path>, offsets: &[(u32, i64)], code: i32) -> Value {
    let mut args = vec!["preflight".to_owned(), "--topic".into(), topic.into()];
    if let Some(cp) = cp {
        args.extend(["--checkpoint".into(), cp.to_str().unwrap().into()]);
    }
    for (p, n) in offsets {
        args.extend(["--offset".into(), format!("{p}={n}")]);
    }
    call(
        &connection(false),
        &args.iter().map(String::as_str).collect::<Vec<_>>(),
        code,
    )
}
fn has(v: &Value, code: &str) -> bool {
    v["issues"]
        .as_array()
        .unwrap()
        .iter()
        .any(|i| i["code"] == code)
}
fn record(name: &str, value: Value) {
    let p = PathBuf::from(std::env::var("SQLX_DORIS_ARTIFACTS_DIR").unwrap()).join(format!(
        "kafka-preflight-{name}-{}.json",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(p, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated externally stored Kafka TLS fixture"]
async fn diagnosis_keeps_independent_kafka_evidence_when_doris_is_unavailable() {
    let dir = root();
    let topic = broker::topic(1, None).await;
    let before = broker::send(&topic, &[(0, 1), (0, 2)]).await;
    let cp = dir.join("before.json");
    snapshot(&topic, &cp);
    let original = std::fs::read(&cp).unwrap();
    let config = connection(false);
    let p = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
        .args([
            "admin",
            "diagnose",
            "--json",
            "--connection-file",
            config.to_str().unwrap(),
            "--topic",
            &topic,
            "--checkpoint",
            cp.to_str().unwrap(),
            "--offset",
            "0=2",
        ])
        .env_remove("DORIS_TEST_URL")
        .env("DATABASE_URL", "doris://root@127.0.0.1:9/app")
        .output()
        .unwrap();
    assert_eq!(p.status.code(), Some(7));
    assert!(p.stderr.is_empty());
    let v: Value = serde_json::from_slice(&p.stdout).unwrap();
    assert_eq!(v["schema"], "sqlx-doris-cli-result-v1");
    assert_eq!(v["components"]["database_read"]["exit_code"], 7);
    assert_eq!(v["components"]["kafka"]["exit_code"], 0);
    assert_eq!(v["coverage"]["kafka_source_bound_to_routine"], false);
    assert_eq!(v["safe_to_recover"], false);
    assert_eq!(std::fs::read(cp).unwrap(), original);
    let after = broker::snapshot(&topic).await;
    assert_eq!(before.end, after.end);
    assert_eq!(before.topic_id, after.topic_id);
    record("diagnosis-partial", v);
    broker::delete(&topic).await;
}
#[tokio::test]
#[ignore = "requires externally stored dedicated Kafka 4.0.0 with host-reachable verified TLS"]
async fn preflight_identity_ranges_and_no_baseline_never_mutate_source_or_auto_create() {
    let root = root();
    let topic = broker::topic(2, Some("1,2")).await;
    let before = broker::send(&topic, &[(0, 1), (0, 2), (1, 101)]).await;
    let cp = root.join("checkpoint.json");
    let saved = snapshot(&topic, &cp);
    assert_eq!(saved["phase"], "local_write");
    let good = preflight(&topic, Some(&cp), &[(1, 1), (0, 2)], 0);
    assert_eq!(good["current"]["cluster_id"], before.cluster);
    assert_eq!(good["observation"], "consistent_observations");
    assert!(has(
        &preflight(&topic, None, &[(0, 2), (1, 1)], 4),
        "historical_checkpoint_missing"
    ));
    let bytes = std::fs::read(&cp).unwrap();
    call(
        &connection(false),
        &[
            "snapshot",
            "--topic",
            &topic,
            "--output",
            cp.to_str().unwrap(),
        ],
        2,
    );
    assert_eq!(std::fs::read(&cp).unwrap(), bytes);
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&cp).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let after = broker::snapshot(&topic).await;
    assert_eq!(before.topic_id, after.topic_id);
    assert_eq!(before.begin, after.begin);
    assert_eq!(before.end, after.end);
    let absent = format!("sdk_rl_matrix_{}", uuid::Uuid::new_v4().simple());
    call(
        &connection(false),
        &["preflight", "--topic", &absent, "--offset", "0=0"],
        7,
    );
    let topics = kafka("kafka-topics", vec!["--list".into()], None).await;
    assert!(!topics.lines().any(|v| v == absent));
    record(
        "identity",
        json!({
            "saved": saved,
            "preflight": good,
            "before": before,
            "after": after
        }),
    );
    broker::delete(&topic).await;
}
#[tokio::test]
#[ignore = "requires dedicated Kafka TLS and owned test topics"]
async fn preflight_partition_growth_and_expired_prefix_are_blocked() {
    let root = root();
    let topic = broker::topic(2, None).await;
    broker::send(&topic, &[(0, 1), (0, 2), (1, 101)]).await;
    let cp = root.join("checkpoint.json");
    snapshot(&topic, &cp);
    broker::grow(&topic, 3).await;
    let changed = preflight(&topic, Some(&cp), &[(0, 2), (1, 1), (2, 0)], 3);
    assert!(has(&changed, "partition_set_changed"));
    broker::trim_prefix(&topic, 0, 1).await;
    let expired = preflight(&topic, Some(&cp), &[(0, 0), (1, 1), (2, 0)], 3);
    assert!(has(&expired, "candidate_outside_observed_range"));
    record(
        "growth-expiry",
        json!({
            "growth": changed,
            "expired": expired
        }),
    );
    broker::delete(&topic).await;
}
#[tokio::test]
#[ignore = "requires dedicated Kafka TLS and owned topic replacement"]
async fn preflight_retained_epoch_loss_and_same_name_topic_replacement_remain_distinct() {
    let root = root();
    let topic = broker::topic(1, None).await;
    broker::send(&topic, &[(0, 1), (0, 2)]).await;
    let cp = root.join("checkpoint.json");
    let old = snapshot(&topic, &cp);
    broker::trim_prefix(&topic, 0, 2).await;
    let expired = preflight(&topic, Some(&cp), &[(0, 2)], 4);
    assert!(has(&expired, "history_anchor_empty_or_expired"));
    broker::delete(&topic).await;
    broker::create(&topic, 1, None).await;
    broker::send(&topic, &[(0, 1001), (0, 1002), (0, 1003)]).await;
    let changed = preflight(&topic, Some(&cp), &[(0, 2)], 3);
    assert!(has(&changed, "topic_identity_changed"));
    assert_ne!(old["current"]["topic_id"], changed["current"]["topic_id"]);
    record(
        "retention-replacement",
        json!({
            "old": old,
            "expired": expired,
            "replaced": changed
        }),
    );
    broker::delete(&topic).await;
}
#[tokio::test]
#[ignore = "requires dedicated Kafka TLS, SASL_SSL PLAIN and private external credentials"]
async fn preflight_tls_sasl_private_inputs_and_json_errors_fail_without_secret_output() {
    let root = root();
    let topic = broker::topic(1, None).await;
    broker::send(&topic, &[(0, 1)]).await;
    let cp = root.join("checkpoint.json");
    snapshot(&topic, &cp);
    let cfg = connection(true);
    let good = call(
        &cfg,
        &[
            "preflight",
            "--topic",
            &topic,
            "--checkpoint",
            cp.to_str().unwrap(),
            "--offset",
            "0=1",
        ],
        0,
    );
    let value: Value = serde_json::from_slice(&std::fs::read(&cfg).unwrap()).unwrap();
    let secret = "PRIVATE_KAFKA_PASSWORD_MUST_NOT_APPEAR";
    let mut wrong = value.clone();
    wrong["sasl_plain"]["password"] = json!(secret);
    let wrong_path = root.join("wrong-auth.json");
    private(&wrong_path, &wrong);
    let denied = call(
        &wrong_path,
        &["preflight", "--topic", &topic, "--offset", "0=1"],
        7,
    );
    assert!(!denied.to_string().contains(secret));
    let mut insecure = value.clone();
    insecure["security_protocol"] = json!("SASL_PLAINTEXT");
    let p = root.join("insecure.json");
    private(&p, &insecure);
    call(&p, &["preflight", "--topic", &topic, "--offset", "0=1"], 2);
    let link = root.join("link.json");
    std::os::unix::fs::symlink(&cfg, &link).unwrap();
    call(
        &link,
        &["preflight", "--topic", &topic, "--offset", "0=1"],
        2,
    );
    let mut wrong_ca: Value =
        serde_json::from_slice(&std::fs::read(connection(false)).unwrap()).unwrap();
    let ca = root.join("wrong-ca.pem");
    std::fs::copy(
        PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap())
            .join("fixtures/tls/wrong-ca.pem"),
        &ca,
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&ca, std::fs::Permissions::from_mode(0o600)).unwrap();
    wrong_ca["ca_file"] = json!(ca);
    let p = root.join("wrong-ca.json");
    private(&p, &wrong_ca);
    call(&p, &["preflight", "--topic", &topic, "--offset", "0=1"], 7);
    let mut ip = wrong_ca.clone();
    ip["ca_file"] = value["ca_file"].clone();
    ip["bootstrap_servers"] = json!(
        value["bootstrap_servers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().replace("localhost", "127.0.0.1"))
            .collect::<Vec<_>>()
    );
    let p = root.join("wrong-hostname.json");
    private(&p, &ip);
    call(&p, &["preflight", "--topic", &topic, "--offset", "0=1"], 7);
    let out = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
        .args(["admin", "kafka", "preflight", "--json"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<Value>(&out.stdout).unwrap()["schema"],
        "sqlx-doris-cli-result-v1"
    );
    record(
        "security-json",
        json!({
            "authenticated": good,
            "denied": denied
        }),
    );
    broker::delete(&topic).await;
}
#[tokio::test]
#[ignore = "requires two attested exclusively owned broker-only fault nodes; never shared controller"]
async fn preflight_z_real_truncation_is_detected_after_offset_regrowth_and_new_baselines_have_limits()
 {
    let root = root();
    let (leader, follower) = broker::node_ids();
    let topic = broker::topic(1, Some(&format!("{leader}:{follower}"))).await;
    broker::wait_replicas(&topic, leader, 2).await;
    broker::send(&topic, &[(0, 1), (0, 2), (0, 3)]).await;
    broker::control(1, false);
    broker::wait_replicas(&topic, leader, 1).await;
    broker::send(&topic, &[(0, 4), (0, 5)]).await;
    let cp = root.join("before.json");
    let old = snapshot(&topic, &cp);
    assert_eq!(old["current"]["partitions"][0]["end"], 5);
    broker::control(0, false);
    broker::control(1, true);
    broker::elect(&topic).await;
    broker::wait_replicas(&topic, follower, 1).await;
    let truncated = preflight(&topic, Some(&cp), &[(0, 5)], 3);
    assert_eq!(truncated["current"]["partitions"][0]["end"], 3);
    broker::send(&topic, &[(0, 103), (0, 104), (0, 105)]).await;
    let regrown = preflight(&topic, Some(&cp), &[(0, 5)], 3);
    assert_eq!(regrown["current"]["partitions"][0]["end"], 6);
    assert_eq!(old["current"]["topic_id"], regrown["current"]["topic_id"]);
    assert_eq!(regrown["checks"][0]["range"], "in_range");
    assert!(has(&regrown, "checkpoint_tail_truncated"));
    let fresh = root.join("after.json");
    snapshot(&topic, &fresh);
    let limited = preflight(&topic, Some(&fresh), &[(0, 5)], 0);
    assert_eq!(limited["kafka_continuation_verified"], false);
    broker::control(0, true);
    broker::wait_replicas(&topic, follower, 2).await;
    record(
        "real-tail",
        json!({
            "before": old,
            "truncated": truncated,
            "regrown": regrown,
            "new_baseline_cannot_prove_earlier_history": limited
        }),
    );
    broker::delete(&topic).await;
}
