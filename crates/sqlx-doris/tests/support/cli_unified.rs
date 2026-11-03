//! Native JSON and read-only composition, including deterministic DROP faults.
use super::*;
use serde_json::{Value, json};

#[tokio::test]
#[ignore = "requires native Doris and a dedicated migration database"]
async fn unified_qualified_migration_table_uses_the_same_rules_for_run_info_and_diagnose() {
    let (mut c, url, _) = fixture().await;
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut c)
        .await
        .unwrap();
    let table = format!("`{database}`.`custom_history`");
    let source = PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap())
        .join(format!("qualified-history-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("1_first.sql"), "SELECT 1;").unwrap();
    json_call(
        &url,
        &[
            "migrate",
            "--source",
            source.to_str().unwrap(),
            "--table",
            &table,
            "run",
        ],
        0,
    )
    .await;
    let info = json_call(
        &url,
        &[
            "migrate",
            "--source",
            source.to_str().unwrap(),
            "--table",
            &table,
            "info",
        ],
        0,
    )
    .await;
    assert_eq!(info["migrations"]["table_present"], true);
    assert_eq!(info["migration_status"][0]["state"], "applied");
    let diagnostic = json_call(
        &url,
        &[
            "admin",
            "diagnose",
            "--source",
            source.to_str().unwrap(),
            "--table",
            &table,
        ],
        4,
    )
    .await;
    assert_eq!(diagnostic["components"]["migrations"]["exit_code"], 0);
    assert_eq!(
        diagnostic["components"]["migrations"]["observation"]["rows"][0]["version"],
        1
    );
    json_call(
        &url,
        &[
            "admin",
            "diagnose",
            "--table",
            "other_database.custom_history",
        ],
        2,
    )
    .await;
    json_call(
        &url,
        &[
            "migrate",
            "--source",
            source.to_str().unwrap(),
            "--table",
            "_sqlx_migrations_gate",
            "info",
        ],
        2,
    )
    .await;
    let defaults:i64=sqlx::query_scalar("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=DATABASE() AND table_name='_sqlx_migrations'").fetch_one(&mut c).await.unwrap();
    assert_eq!(defaults, 0);
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_dir_all(source).unwrap();
}

#[tokio::test]
#[ignore = "requires native Doris and deterministic FE packet barriers"]
async fn unified_identity_json_preserves_uncertain_and_acknowledged_phases() {
    for (point, code, phase) in [
        (Point::DropReply, 5, "submitted"),
        (Point::IdentityRead(3), 6, "acknowledged"),
    ] {
        let (mut c, url, opts) = fixture().await;
        let account = create(&url).await;
        seed_owner(&mut c, false, GATE_OWNER).await;
        let mut proxy = Proxy::start_identity(&opts, point).await;
        let proxy_url = proxy.options.to_url_lossy().to_string();
        let action = [
            "admin",
            "identity",
            "retire",
            "--name",
            &account.name,
            "--maintenance",
            "--force",
        ];
        let future = json_call(&proxy_url, &action, code);
        let drive = async {
            proxy.reached().await;
            proxy.release(Release::Disconnect);
        };
        let (v, ()) = tokio::join!(future, drive);
        assert_eq!(v["phase"], phase);
        assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
        assert_eq!(
            c.migration_lock_owner().await.unwrap().as_deref(),
            Some(GATE_OWNER)
        );
        proxy.finish().await;
        c.close().await.unwrap();
        Doris::drop_database(&url).await.unwrap();
        std::fs::remove_file(account.path).unwrap();
    }
}

#[tokio::test]
#[ignore = "requires native Doris and deterministic FE packet barriers"]
async fn unified_diagnosis_deadline_preserves_locks_and_reports_partial_observations() {
    let (mut c, url, opts) = fixture().await;
    seed_owner(&mut c, true, GATE_OWNER).await;
    let mut proxy = Proxy::start(&opts, Point::GateRead(1)).await;
    let proxy_url = proxy.options.to_url_lossy().to_string();
    let task = tokio::spawn(async move {
        json_call(&proxy_url, &["admin", "diagnose", "--timeout", "1"], 7).await
    });
    proxy.reached().await;
    let v = task.await.unwrap();
    proxy.release(Release::Forward);
    assert_eq!(v["phase"], "read_only");
    assert_eq!(v["components"]["incomplete"]["exit_code"], 7);
    assert!(
        v["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["code"] == "diagnosis_deadline_expired")
    );
    assert!(
        v["components"]["migrations"]["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["code"] == "component_not_observed")
    );
    assert_eq!(
        v["components"]["drain"]["observation"]["state"],
        "not_observed"
    );
    assert_eq!(v["components"]["migrations"]["status"], "inconclusive");
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 0);
    assert_eq!(
        c.migration_transition_owner().await.unwrap().as_deref(),
        Some(GATE_OWNER)
    );
    proxy.finish().await;
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

async fn json_call(url: &str, args: &[&str], code: i32) -> Value {
    let mut args = args.to_vec();
    args.push("--json");
    let out = call(url, &args, code == 0).await;
    assert_eq!(out.status.code(), Some(code), "{}", text(&out));
    assert!(out.stderr.is_empty());
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["schema"], "sqlx-doris-cli-result-v1");
    assert_eq!(v["exit_code"], code);
    assert_eq!(v["safe_to_recover"], false);
    v
}
#[tokio::test]
#[ignore = "requires dedicated native Doris 4.1.5"]
async fn unified_diagnosis_is_read_only_and_info_does_not_create_state_tables() {
    let (mut c, url, _) = fixture().await;
    let src = PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap())
        .join(format!("cli-diagnose-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir(&src).unwrap();
    std::fs::write(src.join("1_observe.sql"), "SELECT 1;").unwrap();
    let before: Vec<String> = c
        .fetch_all("SHOW TABLES")
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    let info = json_call(
        &url,
        &["migrate", "--source", src.to_str().unwrap(), "info"],
        0,
    )
    .await;
    assert_eq!(info["migration_status"][0]["state"], "pending");
    assert_eq!(info["migrations"]["table_present"], false);
    let v = json_call(
        &url,
        &["admin", "diagnose", "--source", src.to_str().unwrap()],
        4,
    )
    .await;
    assert_eq!(v["server_read_only"], true);
    assert_eq!(
        v["components"]["migrations"]["observation"]["rows"],
        json!([])
    );
    let after: Vec<String> = c
        .fetch_all("SHOW TABLES")
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(before, after);
    let account = create(&url).await;
    let blocked = json_call(
        &url,
        &["admin", "diagnose", "--retired-identity", &account.name],
        3,
    )
    .await;
    assert_eq!(blocked["components"]["drain"]["exit_code"], 3);
    assert!(
        blocked["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["code"] == "identity_present")
    );
    retire(&url, &account).await;
    let empty = json_call(
        &url,
        &["admin", "diagnose", "--retired-identity", &account.name],
        0,
    )
    .await;
    assert_eq!(
        empty["components"]["drain"]["observation"]["scoped_empty"],
        true
    );
    json_call(
        &url,
        &["migrate", "--source", src.to_str().unwrap(), "run"],
        0,
    )
    .await;
    c.execute("UPDATE _sqlx_migrations SET success=false WHERE version=1")
        .await
        .unwrap();
    let (main, gate) = (
        c.migration_lock_owner().await.unwrap(),
        c.migration_transition_owner().await.unwrap(),
    );
    let dirty = json_call(
        &url,
        &[
            "admin",
            "diagnose",
            "--retired-identity",
            &account.name,
            "--source",
            src.to_str().unwrap(),
        ],
        3,
    )
    .await;
    assert_eq!(dirty["components"]["migrations"]["exit_code"], 3);
    assert!(
        dirty["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["code"] == "migration_dirty" && i["context"]["version"] == 1)
    );
    let human = call(
        &url,
        &[
            "admin",
            "diagnose",
            "--retired-identity",
            &account.name,
            "--source",
            src.to_str().unwrap(),
        ],
        false,
    )
    .await;
    assert!(String::from_utf8_lossy(&human.stdout).contains("migration_dirty"));
    assert!(String::from_utf8_lossy(&human.stdout).contains("\"version\":1"));
    std::fs::write(src.join("1_observe.sql"), "SELECT 2;").unwrap();
    let mismatch = json_call(
        &url,
        &[
            "admin",
            "diagnose",
            "--retired-identity",
            &account.name,
            "--source",
            src.to_str().unwrap(),
        ],
        3,
    )
    .await;
    assert!(
        mismatch["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["code"] == "migration_checksum_mismatch" && i["context"]["version"] == 1)
    );
    std::fs::remove_file(src.join("1_observe.sql")).unwrap();
    let missing = json_call(
        &url,
        &[
            "admin",
            "diagnose",
            "--retired-identity",
            &account.name,
            "--source",
            src.to_str().unwrap(),
        ],
        3,
    )
    .await;
    assert!(
        missing["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["code"] == "migration_source_missing")
    );
    assert_eq!(
        (main, gate),
        (
            c.migration_lock_owner().await.unwrap(),
            c.migration_transition_owner().await.unwrap()
        )
    );
    assert_eq!(c.dirty_version("_sqlx_migrations").await.unwrap(), Some(1));
    c.execute("UPDATE _sqlx_migrations SET checksum='invalid' WHERE version=1")
        .await
        .unwrap();
    let corrupt = json_call(
        &url,
        &["admin", "diagnose", "--retired-identity", &account.name],
        4,
    )
    .await;
    assert_eq!(corrupt["components"]["database_read"]["exit_code"], 4);
    assert!(
        corrupt["issues"]
            .as_array()
            .unwrap()
            .iter()
            .any(|i| i["code"] == "migration_history_invalid")
    );
    assert_eq!(c.dirty_version("_sqlx_migrations").await.unwrap(), Some(1));

    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_dir_all(src).unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated native Doris and deterministic FE packet barriers"]
async fn unified_transition_recovery_json_distinguishes_not_sent_uncertain_and_acknowledged() {
    for (point, code, phase) in [
        (Point::GateRead(1), 7, "read_only"),
        (Point::DropReply, 5, "submitted"),
        (Point::GateRead(6), 6, "acknowledged"),
    ] {
        let (mut c, url, opts) = fixture().await;
        seed_owner(&mut c, true, GATE_OWNER).await;
        let mut proxy = Proxy::start(&opts, point).await;
        let proxy_url = proxy.options.to_url_lossy().to_string();
        let future = json_call(
            &proxy_url,
            &[
                "admin",
                "lock",
                "recover-transition",
                "--owner",
                GATE_OWNER,
                "--lock-owner",
                "unlocked",
                "--maintenance",
                "--force",
            ],
            code,
        );
        let drive = async {
            proxy.reached().await;
            proxy.release(Release::Disconnect);
        };
        let (v, ()) = tokio::join!(future, drive);
        assert_eq!(v["phase"], phase);
        assert_eq!(v["target"]["requested_owner"], GATE_OWNER);
        assert_eq!(proxy.trace.lock().unwrap().parent_drops, 0);
        proxy.finish().await;
        c.close().await.unwrap();
        Doris::drop_database(&url).await.unwrap();
    }
}
