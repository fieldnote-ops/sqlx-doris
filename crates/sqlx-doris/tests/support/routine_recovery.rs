//! Receipt/JSON/exit contracts against real FE metadata and delivery faults.
use super::*;
pub(super) async fn json_call(url: &str, args: &[&str], code: i32) -> serde_json::Value {
    let mut args = args.to_vec();
    args.push("--json");
    let output = call(url, &args, code == 0).await;
    assert_eq!(output.status.code(), Some(code), "{}", text(&output));
    assert!(output.stderr.is_empty(), "{}", text(&output));
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)
        .expect("one complete JSON document, no progress chatter");
    assert_eq!(value["exit_code"], code);
    assert_eq!(value["schema"], "sqlx-doris-cli-result-v1");
    assert_eq!(value["safe_to_recover"], false);
    assert_eq!(value["automatic_retry"], false);
    value
}
fn confirmation(p: &std::path::Path) -> PathBuf {
    let mut value = p.as_os_str().to_os_string();
    value.push(".confirmed");
    value.into()
}
fn private(path: &std::path::Path, value: &serde_json::Value) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, serde_json::to_vec(value).unwrap()).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}
#[tokio::test]
#[ignore = "requires native Doris and dedicated Kafka"]
async fn recovery_receipt_locates_database_and_json_preserves_terminal_binding() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    let input = source();
    let path = input.with_extension("json");
    let created = json_call(
        &url,
        &[
            "admin",
            "routine",
            "create",
            "--runner-identity-file",
            account.path.to_str().unwrap(),
            "--table",
            "events",
            "--source",
            input.to_str().unwrap(),
            "--output",
            path.to_str().unwrap(),
            "--managed-namespace",
        ],
        0,
    )
    .await;
    let name = created["job_name"].as_str().unwrap();
    let id = created["job"]["id"].as_u64().unwrap().to_string();
    assert_eq!(created["outcome"], "created");
    assert!(confirmation(&path).is_file());
    let mut unreachable = opts_url(&url);
    unreachable.set_port(Some(9)).unwrap();
    let partial = json_call(
        unreachable.as_str(),
        &[
            "admin",
            "diagnose",
            "--receipt",
            path.to_str().unwrap(),
            "--retired-identity",
            &account.name,
        ],
        7,
    )
    .await;
    assert_eq!(
        partial["components"]["routine"]["observation"]["job_name"],
        name
    );
    assert_eq!(
        partial["components"]["routine"]["observation"]["last_confirmed_job"]["id"],
        created["job"]["id"]
    );

    let mut admin = opts_url(&url);
    admin.set_path("/not_the_receipt_database");
    let status = json_call(
        admin.as_str(),
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            path.to_str().unwrap(),
        ],
        0,
    )
    .await;
    assert_eq!(status["database"], created["database"]);
    assert_eq!(status["job"]["id"], created["job"]["id"]);
    retire(&url, &account).await;
    let preview = json_call(
        &url,
        &[
            "admin",
            "routine",
            "stop",
            "--receipt",
            path.to_str().unwrap(),
        ],
        0,
    )
    .await;
    assert_eq!(preview["outcome"], "preview");
    let stopped = json_call(
        &url,
        &[
            "admin",
            "routine",
            "stop",
            "--receipt",
            path.to_str().unwrap(),
            "--job-id",
            &id,
            "--maintenance",
            "--managed-namespace",
            "--force",
        ],
        0,
    )
    .await;
    assert_eq!(stopped["observation"], "terminal");
    assert_eq!(stopped["job"]["state"], "STOPPED");
    json_call(&url, &stop_args(name, &id, true), 3).await;
    let terminal = json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            path.to_str().unwrap(),
        ],
        0,
    )
    .await;
    assert_eq!(terminal["observation"], "terminal");
    let diagnostic = json_call(
        admin.as_str(),
        &[
            "admin",
            "diagnose",
            "--receipt",
            path.to_str().unwrap(),
            "--retired-identity",
            &account.name,
        ],
        3,
    )
    .await;
    assert_eq!(diagnostic["server_read_only"], true);
    assert_eq!(diagnostic["components"]["routine"]["exit_code"], 0);
    assert_eq!(
        diagnostic["components"]["routine"]["observation"]["job"]["id"],
        created["job"]["id"]
    );
    assert_eq!(diagnostic["components"]["migrations"]["exit_code"], 3);
    assert_eq!(
        diagnostic["components"]["locks"]["observation"]["main_owner"],
        owner
    );
    cleanup(c, &url, account, &owner).await;
}
fn opts_url(url: &str) -> reqwest::Url {
    reqwest::Url::parse(url).unwrap()
}

#[tokio::test]
#[ignore = "requires native Doris and dedicated Kafka"]
async fn recovery_receipts_reject_input_tampering_without_authorizing_a_stop() {
    use std::os::unix::fs::PermissionsExt;
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    let (name, id, path) = job(&mut c, &url, &account).await;
    let original: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let invalid = path.with_extension("invalid.json");
    let mut value = original.clone();
    value["format"] = "PRIVATE_INPUT_MUST_NOT_APPEAR".into();
    private(&invalid, &value);
    let failure = json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            invalid.to_str().unwrap(),
        ],
        2,
    )
    .await;
    assert!(
        !failure
            .to_string()
            .contains("PRIVATE_INPUT_MUST_NOT_APPEAR")
    );
    value = original.clone();
    value["cluster"] = "1".into();
    private(&invalid, &value);
    json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            invalid.to_str().unwrap(),
        ],
        3,
    )
    .await;
    value = original.clone();
    let absent = format!("sqlx_rl_{}", uuid::Uuid::new_v4().simple());
    value["job_name"] = absent.clone().into();
    value["reservation"] = format!("_{absent}").into();
    private(&invalid, &value);
    let missing = json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            invalid.to_str().unwrap(),
        ],
        4,
    )
    .await;
    assert_eq!(missing["observation"], "reservation_missing");
    json_call(
        &url,
        &[
            "admin",
            "routine",
            "stop",
            "--receipt",
            invalid.to_str().unwrap(),
            "--job-id",
            &id,
            "--maintenance",
            "--managed-namespace",
            "--force",
        ],
        3,
    )
    .await;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            path.to_str().unwrap(),
        ],
        2,
    )
    .await;
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let symlink = path.with_extension("link.json");
    std::os::unix::fs::symlink(&path, &symlink).unwrap();
    json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            symlink.to_str().unwrap(),
        ],
        2,
    )
    .await;
    let checkpoint = confirmation(&path);
    let saved = std::fs::read(&checkpoint).unwrap();
    std::fs::write(&checkpoint, b"invalid PRIVATE_INPUT_MUST_NOT_APPEAR").unwrap();
    json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            path.to_str().unwrap(),
        ],
        2,
    )
    .await;
    std::fs::write(checkpoint, saved).unwrap();
    json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            path.to_str().unwrap(),
            "--job-name",
            &name,
        ],
        2,
    )
    .await;
    assert_ne!(state(&mut c, &name).await, "STOPPED");
    retire(&url, &account).await;
    json_call(&url, &stop_args(&name, &id, true), 0).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires native Doris and dedicated Kafka"]
async fn recovery_reserved_unobserved_can_register_later_and_history_loss_is_inconclusive() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    let mut proxy = Proxy::start_routine_create(&opts, Point::BeforeDrop).await;
    let mut credentials: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&account.path).unwrap()).unwrap();
    let original = credentials["database_url"].clone();
    credentials["database_url"] = account
        .options
        .clone()
        .host(proxy.options.get_host())
        .port(proxy.options.get_port())
        .ssl_mode(sqlx_doris::DorisSslMode::Disabled)
        .to_url_lossy()
        .to_string()
        .into();
    private(&account.path, &credentials);
    let input = source();
    let path = input.with_extension("json");
    let (mut child, _, err) = spawn(
        proxy.options.to_url_lossy().as_str(),
        &[
            "admin",
            "routine",
            "create",
            "--runner-identity-file",
            account.path.to_str().unwrap(),
            "--table",
            "events",
            "--source",
            input.to_str().unwrap(),
            "--output",
            path.to_str().unwrap(),
            "--managed-namespace",
            "--json",
        ],
    );
    proxy.reached().await;
    let pending = json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            path.to_str().unwrap(),
        ],
        4,
    )
    .await;
    assert_eq!(pending["observation"], "reserved_unobserved");
    child.kill().unwrap();
    proxy.release(Release::Forward);
    proxy.acknowledged().await;
    exited(child, &err, false).await;
    proxy.finish().await;
    credentials["database_url"] = original;
    private(&account.path, &credentials);
    let active = json_call(
        &url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            path.to_str().unwrap(),
        ],
        0,
    )
    .await;
    assert_eq!(active["observation"], "active");
    let name = active["job_name"].as_str().unwrap();
    let id = active["job"]["id"].as_u64().unwrap().to_string();
    retire(&url, &account).await;
    json_call(&url, &stop_args(name, &id, true), 0).await;
    let next_account = create(&url).await;
    let (next, next_id, known) = job(&mut c, &url, &next_account).await;
    // Real server filtered visibility, not fabricated rows or a GC completion claim.
    let hidden = Proxy::start_routine(&opts, Point::RoutineHidden).await;
    let unavailable = json_call(
        hidden.options.to_url_lossy().as_str(),
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            known.to_str().unwrap(),
        ],
        4,
    )
    .await;
    assert_eq!(unavailable["observation"], "history_unavailable");
    assert!(unavailable["last_confirmed_job"]["id"].is_u64());
    assert!(unavailable["job"].is_null());
    hidden.finish().await;
    assert_ne!(state(&mut c, &next).await, "STOPPED");
    retire(&url, &next_account).await;
    json_call(&url, &stop_args(&next, &next_id, true), 0).await;
    std::fs::remove_file(next_account.path).unwrap();
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires native Doris and dedicated Kafka"]
async fn recovery_json_exit_codes_separate_reads_unknown_writes_and_ack_failures() {
    for mode in 0..4 {
        let (mut c, url, opts) = fixture().await;
        let (account, owner) = setup(&mut c, &url).await;
        let (name, id, path) = job(&mut c, &url, &account).await;
        retire(&url, &account).await;
        let point = match mode {
            0 => Point::RoutineRead(1),
            1 => Point::DropReply,
            2 => Point::RoutineRead(3),
            _ => Point::BeforeDrop,
        };
        let mut proxy = Proxy::start_routine(&opts, point).await;
        let mut args = stop_args(&name, &id, true);
        args.extend([
            "--json",
            "--ddl-timeout",
            if mode == 3 { "2" } else { "30" },
        ]);
        let (child, out, err) = spawn(proxy.options.to_url_lossy().as_str(), &args);
        proxy.reached().await;
        if mode != 3 {
            proxy.release(Release::Disconnect);
        }
        exited(child, &err, false).await;
        assert!(std::fs::read_to_string(err).unwrap().is_empty());
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(out).unwrap()).unwrap();
        assert_eq!(
            report["exit_code"],
            match mode {
                0 => 7,
                2 => 6,
                _ => 5,
            }
        );
        assert_eq!(report["safe_to_recover"], false);
        if mode != 0 {
            assert_eq!(report["observation"], "unconfirmed");
        }
        if mode == 3 {
            proxy.release(Release::Forward);
            proxy.acknowledged().await;
        }
        assert_eq!(
            proxy.trace.lock().unwrap().drop_forwarded,
            usize::from(mode != 0)
        );
        proxy.finish().await;
        if mode == 0 {
            json_call(&url, &stop_args(&name, &id, true), 0).await;
        }
        let terminal = json_call(
            &url,
            &[
                "admin",
                "routine",
                "status",
                "--receipt",
                path.to_str().unwrap(),
            ],
            0,
        )
        .await;
        assert_eq!(terminal["job"]["state"], "STOPPED");
        cleanup(c, &url, account, &owner).await;
    }
}

#[tokio::test]
#[ignore = "requires native Doris and dedicated Kafka"]
async fn recovery_create_ack_with_failed_confirmation_never_replays_or_overwrites() {
    let invalid = json_call(
        "postgres://PRIVATE_PASSWORD@localhost/db",
        &[
            "admin",
            "routine",
            "status",
            "--job-name",
            "sqlx_rl_00000000000040008000000000000001",
        ],
        2,
    )
    .await;
    assert!(!invalid.to_string().contains("PRIVATE_PASSWORD"));
    assert_eq!(
        invalid["job_name"],
        "sqlx_rl_00000000000040008000000000000001"
    );
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    let input = source();
    let path = input.with_extension("json");
    let checkpoint = confirmation(&path);
    private(
        &checkpoint,
        &serde_json::json!({"sentinel":"do not overwrite"}),
    );
    let created = json_call(
        &url,
        &[
            "admin",
            "routine",
            "create",
            "--runner-identity-file",
            account.path.to_str().unwrap(),
            "--table",
            "events",
            "--source",
            input.to_str().unwrap(),
            "--output",
            path.to_str().unwrap(),
            "--managed-namespace",
        ],
        6,
    )
    .await;
    assert_eq!(created["step"], "create_acknowledged");
    assert_eq!(created["observation"], "unconfirmed");
    assert_eq!(created["job_observed_phase"], "create_acknowledged");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&std::fs::read(&checkpoint).unwrap()).unwrap()
            ["sentinel"],
        "do not overwrite"
    );
    let name = created["job_name"].as_str().unwrap();
    let id = created["job"]["id"].as_u64().unwrap().to_string();
    let current = json_call(&url, &["admin", "routine", "status", "--job-name", name], 0).await;
    assert_eq!(current["job"]["id"], created["job"]["id"]);
    retire(&url, &account).await;
    json_call(&url, &stop_args(name, &id, true), 0).await;
    cleanup(c, &url, account, &owner).await;
}
