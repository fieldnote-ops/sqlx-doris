//! Actual Kafka Routine Load with permanent UUID reservations and late SQL barriers.
use super::*;
#[path = "routine_data.rs"]
mod data;
#[path = "routine_recovery.rs"]
mod recovery;

fn source() -> PathBuf {
    let root =
        PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap()).join("routine-contracts");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!("{}.sql", uuid::Uuid::new_v4().simple()));
    let brokers = std::env::var("DORIS_KAFKA_BROKERS").expect("DORIS_KAFKA_BROKERS required");
    let topic = std::env::var("DORIS_KAFKA_TOPIC").expect("DORIS_KAFKA_TOPIC required");
    assert!(
        brokers
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b".-:,".contains(&c))
    );
    assert!(
        topic
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b".-_".contains(&c))
    );
    std::fs::write(&path,format!("COLUMNS TERMINATED BY ',', COLUMNS(id,value) PROPERTIES('desired_concurrent_number'='1','max_batch_interval'='5') FROM KAFKA('kafka_broker_list'='{brokers}','kafka_topic'='{topic}','kafka_partitions'='0','kafka_offsets'='OFFSET_END')")).unwrap();
    path
}
async fn job(c: &mut DorisConnection, url: &str, account: &Account) -> (String, String, PathBuf) {
    let source = source();
    let receipt = source.with_extension("json");
    call(
        url,
        &[
            "admin",
            "routine",
            "create",
            "--runner-identity-file",
            account.path.to_str().unwrap(),
            "--table",
            "events",
            "--source",
            source.to_str().unwrap(),
            "--output",
            receipt.to_str().unwrap(),
            "--managed-namespace",
        ],
        true,
    )
    .await;
    let value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&receipt).unwrap()).unwrap();
    let name = value["job_name"].as_str().unwrap().to_owned();
    let rows = c
        .fetch_all(sqlx::AssertSqlSafe(format!(
            "SHOW ALL ROUTINE LOAD FOR {name}"
        )))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let id = rows[0].get("Id");
    (name, id, receipt)
}
fn stop_args<'a>(name: &'a str, id: &'a str, force: bool) -> Vec<&'a str> {
    let mut args = vec![
        "admin",
        "routine",
        "stop",
        "--job-name",
        name,
        "--job-id",
        id,
    ];
    if force {
        args.extend(["--maintenance", "--managed-namespace", "--force"]);
    }
    args
}
async fn state(c: &mut DorisConnection, name: &str) -> String {
    c.fetch_all(sqlx::AssertSqlSafe(format!(
        "SHOW ALL ROUTINE LOAD FOR {name}"
    )))
    .await
    .unwrap()[0]
        .get("State")
}
async fn setup(c: &mut DorisConnection, url: &str) -> (Account, String) {
    c.execute(CREATE).await.unwrap();
    let account = create(url).await;
    let mut runner = connect(&account).await;
    runner.lock().await.unwrap();
    let owner = runner.migration_lock_owner().await.unwrap().unwrap();
    runner
        .ensure_migrations_table("_sqlx_migrations")
        .await
        .unwrap();
    runner.execute("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(1,'reconcile',false,REPEAT('0',96),-1)").await.unwrap();
    runner.close_hard().await.unwrap();
    (account, owner)
}
#[tokio::test]
#[ignore = "requires real Doris and externally stored Kafka"]
async fn managed_routine_creation_preview_stop_repeat_and_permanent_reservation() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    let (name, id, _receipt) = job(&mut c, &url, &account).await;
    assert!(
        text(
            &call(
                &url,
                &["admin", "routine", "status", "--job-name", &name],
                true
            )
            .await
        )
        .contains(&id)
    );
    call(&url, &stop_args(&name, &id, true), false).await; // creator still live
    retire(&url, &account).await;
    assert!(text(&call(&url, &stop_args(&name, &id, false), true).await).contains("Preview only"));
    call(&url, &stop_args(&name, "1", true), false).await;
    assert!(
        text(&call(&url, &stop_args(&name, &id, true), true).await).contains("Routine STOPPED")
    );
    call(&url, &stop_args(&name, &id, true), false).await;
    assert_eq!(state(&mut c, &name).await, "STOPPED");
    let token: String =
        sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT token FROM _{name}")))
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert!(token.ends_with(&name));
    assert!(
        c.execute(sqlx::AssertSqlSafe(format!(
            "CREATE VIEW _{name} AS SELECT 'replacement' AS token"
        )))
        .await
        .is_err()
    );
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires real Doris and externally stored Kafka"]
async fn managed_routine_late_stop_after_final_check_kill_or_deadline_spares_successors() {
    for mode in 0..4 {
        let (mut c, url, opts) = fixture().await;
        let (account, owner) = setup(&mut c, &url).await;
        let (old, id, _) = job(&mut c, &url, &account).await;
        retire(&url, &account).await;
        let mut proxy = Proxy::start_routine(&opts, Point::BeforeDrop).await;
        let mut args = stop_args(&old, &id, true);
        args.extend(["--ddl-timeout", if mode == 2 { "2" } else { "30" }]);
        let (child, _, err) = spawn(proxy.options.to_url_lossy().as_str(), &args);
        let mut child = Some(child);
        proxy.reached().await;
        if matches!(mode, 1 | 3) {
            child.as_mut().unwrap().kill().unwrap();
        }
        if mode != 0 {
            exited(child.take().unwrap(), &err, false).await;
            if mode == 2 {
                assert!(
                    std::fs::read_to_string(&err)
                        .unwrap()
                        .contains("Routine STOP outcome uncertain")
                );
            }
        }
        if mode != 3 {
            c.execute(sqlx::AssertSqlSafe(format!("STOP ROUTINE LOAD FOR {old}")))
                .await
                .unwrap();
        }
        let next_account = create(&url).await;
        let (next, next_id, _) = job(&mut c, &url, &next_account).await;
        assert_ne!(old, next);
        assert_ne!(id, next_id);
        proxy.release(Release::Forward);
        proxy.responded().await;
        if mode == 0 {
            exited(child.take().unwrap(), &err, false).await;
        }
        assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
        assert_eq!(proxy.trace.lock().unwrap().drop_forwarded, 1);
        assert_eq!(proxy.trace.lock().unwrap().drop_ok, usize::from(mode == 3));
        proxy.finish().await;
        assert_eq!(state(&mut c, &old).await, "STOPPED");
        assert!(matches!(
            state(&mut c, &next).await.as_str(),
            "NEED_SCHEDULE" | "RUNNING" | "PAUSED"
        ));
        retire(&url, &next_account).await;
        call(&url, &stop_args(&next, &next_id, true), true).await;
        std::fs::remove_file(next_account.path).unwrap();
        cleanup(c, &url, account, &owner).await;
    }
}

#[tokio::test]
#[ignore = "requires real Doris and externally stored Kafka"]
async fn managed_routine_lost_ack_and_acknowledged_verification_disconnect_preserve_history() {
    for lost in [true, false] {
        let (mut c, url, opts) = fixture().await;
        let (account, owner) = setup(&mut c, &url).await;
        let (name, id, _) = job(&mut c, &url, &account).await;
        retire(&url, &account).await;
        let mut proxy = Proxy::start_routine(
            &opts,
            if lost {
                Point::DropReply
            } else {
                Point::RoutineRead(3)
            },
        )
        .await;
        let (child, _, err) = spawn(
            proxy.options.to_url_lossy().as_str(),
            &stop_args(&name, &id, true),
        );
        proxy.reached().await;
        proxy.release(Release::Disconnect);
        exited(child, &err, false).await;
        let message = std::fs::read_to_string(err).unwrap();
        assert!(
            message.contains(if lost {
                "STOP outcome uncertain"
            } else {
                "STOP acknowledged; terminal verification failed"
            }),
            "{message}"
        );
        assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
        assert_eq!(proxy.trace.lock().unwrap().drop_forwarded, 1);
        assert_eq!(proxy.trace.lock().unwrap().drop_ok, 1);
        proxy.finish().await;
        assert_eq!(state(&mut c, &name).await, "STOPPED");
        call(&url, &stop_args(&name, &id, true), false).await;
        cleanup(c, &url, account, &owner).await;
    }
}

#[tokio::test]
#[ignore = "requires real Doris and externally stored Kafka"]
async fn managed_routine_refuses_adoption_corrupt_reservations_and_unsafe_sources() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    let raw = format!("sqlx_rl_{}", uuid::Uuid::new_v4().simple());
    let input = source();
    let body = std::fs::read_to_string(&input).unwrap();
    c.execute(sqlx::AssertSqlSafe(format!(
        "CREATE ROUTINE LOAD {raw} ON events {body}"
    )))
    .await
    .unwrap();
    let raw_id = c
        .fetch_all(sqlx::AssertSqlSafe(format!(
            "SHOW ALL ROUTINE LOAD FOR {raw}"
        )))
        .await
        .unwrap()[0]
        .get::<String, _>("Id");
    assert!(
        text(&call(&url, &stop_args(&raw, &raw_id, true), false).await)
            .contains("UUID shape alone is insufficient")
    );
    let (name, id, _) = job(&mut c, &url, &account).await;
    c.execute(sqlx::AssertSqlSafe(format!(
        "ALTER VIEW _{name} AS SELECT 'untrusted' AS token"
    )))
    .await
    .unwrap();
    retire(&url, &account).await;
    call(&url, &stop_args(&name, &id, true), false).await;
    assert!(state(&mut c, &name).await != "STOPPED");
    c.execute(sqlx::AssertSqlSafe(format!("STOP ROUTINE LOAD FOR {name}")))
        .await
        .unwrap();
    c.execute(sqlx::AssertSqlSafe(format!("STOP ROUTINE LOAD FOR {raw}")))
        .await
        .unwrap();
    std::fs::write(&input, "FROM KAFKA(); STOP ROUTINE LOAD FOR other").unwrap();
    let output = input.with_extension("invalid.json");
    assert!(
        text(
            &call(
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
                    output.to_str().unwrap(),
                    "--managed-namespace"
                ],
                false
            )
            .await
        )
        .contains("without any semicolon")
    );
    assert!(!output.exists());
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires real Doris and externally stored Kafka"]
async fn managed_routine_raw_namespace_bypass_remains_an_explicit_unfenced_boundary() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    let (name, id, _) = job(&mut c, &url, &account).await;
    retire(&url, &account).await;
    let mut proxy = Proxy::start_routine(&opts, Point::BeforeDrop).await;
    let (child, _, err) = spawn(
        proxy.options.to_url_lossy().as_str(),
        &stop_args(&name, &id, true),
    );
    proxy.reached().await;
    c.execute(sqlx::AssertSqlSafe(format!("STOP ROUTINE LOAD FOR {name}")))
        .await
        .unwrap();
    // A privileged actor can bypass the view reservation: Doris does not
    // consult our protocol. Never advertise protection against this behavior.
    let input = source();
    let body = std::fs::read_to_string(input).unwrap();
    c.execute(sqlx::AssertSqlSafe(format!(
        "CREATE ROUTINE LOAD {name} ON events {body}"
    )))
    .await
    .unwrap();
    proxy.release(Release::Forward);
    exited(child, &err, false).await;
    assert!(
        std::fs::read_to_string(err)
            .unwrap()
            .contains("STOP acknowledged; terminal verification failed")
    );
    assert_eq!(proxy.trace.lock().unwrap().drop_ok, 1);
    proxy.finish().await;
    let rows = c
        .fetch_all(sqlx::AssertSqlSafe(format!(
            "SHOW ALL ROUTINE LOAD FOR {name}"
        )))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.get::<&str, _>("State") == "STOPPED"));
    call(&url, &stop_args(&name, &id, true), false).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires real Doris and externally stored Kafka"]
async fn managed_routine_creation_lost_ack_or_killed_cli_never_reuses_the_reservation() {
    for kill in [false, true] {
        let (mut c, url, opts) = fixture().await;
        let (account, owner) = setup(&mut c, &url).await;
        let mut proxy = Proxy::start_routine_create(
            &opts,
            if kill {
                Point::BeforeDrop
            } else {
                Point::DropReply
            },
        )
        .await;
        let mut value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&account.path).unwrap()).unwrap();
        let original = value["database_url"].clone();
        value["database_url"] = account
            .options
            .clone()
            .host(proxy.options.get_host())
            .port(proxy.options.get_port())
            .ssl_mode(sqlx_doris::DorisSslMode::Disabled)
            .to_url_lossy()
            .to_string()
            .into();
        std::fs::write(&account.path, serde_json::to_vec(&value).unwrap()).unwrap();
        let input = source();
        let output = input.with_extension("json");
        let create_args = [
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
            output.to_str().unwrap(),
            "--managed-namespace",
            "--json",
        ];
        let (mut child, json_output, err) =
            spawn(proxy.options.to_url_lossy().as_str(), &create_args);
        proxy.reached().await;
        if kill {
            child.kill().unwrap();
            proxy.release(Release::Forward);
            proxy.acknowledged().await;
        } else {
            proxy.release(Release::Disconnect);
        }
        exited(child, &err, false).await;
        if !kill {
            let report: serde_json::Value =
                serde_json::from_slice(&std::fs::read(json_output).unwrap()).unwrap();
            assert_eq!(report["exit_code"], 5);
            assert_eq!(report["phase"], "submitted");
            assert_eq!(report["step"], "create_submitted");
            assert!(std::fs::read_to_string(&err).unwrap().is_empty());
        }
        assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
        assert_eq!(proxy.trace.lock().unwrap().drop_forwarded, 1);
        assert_eq!(proxy.trace.lock().unwrap().drop_ok, 1);
        proxy.finish().await;
        let receipt: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&output).unwrap()).unwrap();
        let name = receipt["job_name"].as_str().unwrap();
        value["database_url"] = original;
        std::fs::write(&account.path, serde_json::to_vec(&value).unwrap()).unwrap();
        call(&url, &create_args, false).await; // output is create-new; no replay/new reservation
        let rows = c.fetch_all("SHOW ALL ROUTINE LOAD").await.unwrap();
        assert_eq!(rows.len(), 1);
        let id = rows[0].get::<String, _>("Id");
        retire(&url, &account).await;
        call(&url, &stop_args(name, &id, true), true).await;
        cleanup(c, &url, account, &owner).await;
    }
}
