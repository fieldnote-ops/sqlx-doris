#![cfg(feature = "cli")]
use serde_json::Value;
use std::process::Command;

fn call(args: &[&str], code: i32) -> Value {
    let out = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
        .env_remove("DATABASE_URL")
        .env_remove("DORIS_TEST_URL")
        .args(args)
        .output()
        .unwrap();
    assert_eq!(
        out.status.code(),
        Some(code),
        "args={args:?} stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.stderr.is_empty());
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["schema"], "sqlx-doris-cli-result-v1");
    assert_eq!(v["exit_code"], code);
    assert_eq!(v["automatic_retry"], false);
    assert_eq!(v["safe_to_recover"], false);
    assert!(!v.to_string().contains("PRIVATE_PASSWORD"));
    v
}
#[test]
fn enabled_namespaces_share_json_usage_errors_and_text_help() {
    let mut paths = vec![vec!["database"], vec!["prepare"], vec!["migrate"]];
    if cfg!(feature = "cli-admin") {
        paths.extend([
            vec!["admin"],
            vec!["admin", "lock"],
            vec!["admin", "identity"],
            vec!["admin", "drain"],
            vec!["admin", "routine"],
            vec!["admin", "diagnose"],
            vec!["admin", "migrate"],
        ]);
    }
    if cfg!(feature = "cli-admin-kafka") {
        paths.push(vec!["admin", "kafka"]);
    }
    for path in paths {
        let mut args = vec!["--json"];
        args.extend_from_slice(&path);
        args.push("--unknown-option");
        let v = call(&args, 2);
        assert_eq!(v["status"], "invalid_input");
        let out = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
            .args(&path)
            .args(["--json", "--help"])
            .output()
            .unwrap();
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).contains("Usage:"));
    }
}

#[test]
fn old_top_level_administration_is_not_a_compatibility_alias() {
    for name in ["lock", "identity", "drain", "routine", "kafka", "diagnose"] {
        let result = call(&["--json", name], 2);
        assert_eq!(result["operation"], "arguments");
    }
}

#[test]
fn invalid_urls_and_inputs_are_read_failures_without_secret_echo_or_mutation() {
    let mut commands = vec![vec!["database", "exists"], vec!["database", "create"]];
    if cfg!(feature = "cli-admin") {
        commands.extend([
            vec!["admin", "lock", "status"],
            vec![
                "admin",
                "identity",
                "status",
                "--name",
                "sqlx_mig_00000000000040008000000000000001",
            ],
            vec!["admin", "diagnose"],
        ]);
    }
    for args in commands {
        let mut cmd = vec![
            "--json",
            "--database-url",
            "mysql://user:PRIVATE_PASSWORD@host/app",
        ];
        cmd.extend(args);
        let v = call(&cmd, 2);
        assert_eq!(v["phase"], "read_only");
        assert_eq!(v["server_read_only"], true);
    }
    if cfg!(feature = "cli-admin") {
        call(
            &[
                "admin",
                "routine",
                "status",
                "--job-name",
                "PRIVATE_PASSWORD",
                "--json",
            ],
            2,
        );
    }
    if cfg!(feature = "cli-admin-kafka") {
        call(
            &[
                "admin",
                "kafka",
                "preflight",
                "--connection-file",
                "missing",
                "--topic",
                "events",
                "--offset",
                "0=0",
                "--json",
            ],
            2,
        );
    }
}

#[cfg(feature = "cli-admin")]
#[tokio::test]
async fn unavailable_diagnosis_does_not_echo_an_unvalidated_table_fragment() {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let url = format!(
        "doris://fixture@127.0.0.1:{}/app?ssl-mode=disabled",
        socket.local_addr().unwrap().port()
    );
    let folder = std::path::PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap()).join(
        format!("unverified-table-{}", uuid::Uuid::new_v4().simple()),
    );
    std::fs::create_dir(&folder).unwrap();
    let result = call(
        &[
            "--database-url",
            &url,
            "admin",
            "diagnose",
            "--source",
            folder.to_str().unwrap(),
            "--table",
            "t;SELECT 'PRIVATE_PASSWORD'",
            "--json",
        ],
        7,
    );
    assert_eq!(
        result["components"]["migrations"]["observation"]["state"],
        "not_observed"
    );
    std::fs::remove_dir(folder).unwrap();
}

#[cfg(feature = "cli-admin")]
#[test]
fn diagnosis_rejects_partial_kafka_inputs_and_every_mutation_flag() {
    for flag in ["--force", "--maintenance", "--no-lock", "--connection-file"] {
        call(&["admin", "diagnose", "--json", flag], 2);
    }
}

#[test]
fn prepare_rejects_a_directory_manifest_with_one_controlled_json_error() {
    let v = call(&["prepare", "--manifest-path", "/", "--json"], 2);
    assert_eq!(v["phase"], "read_only");
    assert_eq!(v["status"], "invalid_input");
}
