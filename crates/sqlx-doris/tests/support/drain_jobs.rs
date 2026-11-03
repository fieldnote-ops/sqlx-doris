//! Real LOAD, Kafka Routine Load and async MV drain contracts.
use super::*;
#[path = "load_stop.rs"]
mod load_stop;
#[path = "routine.rs"]
mod managed_routine;
#[path = "object_store.rs"]
mod object_store;

async fn load(c: &mut DorisConnection, store: &object_store::Store, label: &str) -> String {
    let bucket = format!("fixture{}", uuid::Uuid::new_v4().simple());
    c.execute(sqlx::AssertSqlSafe(format!("LOAD LABEL {label} (DATA INFILE('s3://{bucket}/probe.csv') INTO TABLE events COLUMNS TERMINATED BY ',' (id,value)) WITH S3 ('AWS_ENDPOINT'='{}','AWS_ACCESS_KEY'='fixture','AWS_SECRET_KEY'='fixture','AWS_REGION'='us-east-1','use_path_style'='true') PROPERTIES('timeout'='60')",store.endpoint))).await.unwrap();
    let rows = c.fetch_all("SHOW LOAD").await.unwrap();
    rows.iter()
        .find(|r| r.get::<&str, _>("Label") == label)
        .unwrap()
        .get("JobId")
}
async fn routine(c: &mut DorisConnection) -> String {
    let brokers =
        std::env::var("DORIS_KAFKA_BROKERS").expect("dedicated DORIS_KAFKA_BROKERS is required");
    let topic =
        std::env::var("DORIS_KAFKA_TOPIC").expect("dedicated DORIS_KAFKA_TOPIC is required");
    assert!(
        brokers
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b':' | b','))
    );
    assert!(
        topic
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_'))
    );
    c.execute(sqlx::AssertSqlSafe(format!("CREATE ROUTINE LOAD rl ON events COLUMNS TERMINATED BY ',', COLUMNS(id,value) PROPERTIES('desired_concurrent_number'='1','max_batch_interval'='5') FROM KAFKA('kafka_broker_list'='{brokers}','kafka_topic'='{topic}','kafka_partitions'='0','kafka_offsets'='OFFSET_END')"))).await.unwrap();
    c.fetch_all("SHOW ALL ROUTINE LOAD FOR rl")
        .await
        .unwrap()
        .iter()
        .find(|r| !matches!(r.get::<&str, _>("State"), "STOPPED" | "CANCELLED"))
        .unwrap()
        .get("Id")
}
async fn view(c: &mut DorisConnection, name: &str, trigger: &str, slow: bool) -> String {
    c.execute(sqlx::AssertSqlSafe(format!("CREATE MATERIALIZED VIEW {name} BUILD DEFERRED REFRESH COMPLETE {trigger} DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1') AS SELECT id,COUNT(*)+SUM(SLEEP(IF(id>0,{},0))) AS total FROM events GROUP BY id",if slow {8} else {0}))).await.unwrap();
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    let rows = c
        .fetch_all(sqlx::AssertSqlSafe(format!(
            "SELECT Id FROM mv_infos('database'='{}') WHERE Name='{name}'",
            database
        )))
        .await
        .unwrap();
    rows[0].get::<i64, _>("Id").to_string()
}
async fn task(c: &mut DorisConnection, name: &str, job: &str) -> String {
    c.execute(sqlx::AssertSqlSafe(format!(
        "REFRESH MATERIALIZED VIEW {name} COMPLETE"
    )))
    .await
    .unwrap();
    let rows = c
        .fetch_all(sqlx::AssertSqlSafe(format!(
            "SELECT TaskId FROM tasks('type'='mv') WHERE JobId='{job}' AND Status IN ('PENDING','RUNNING')"
        )))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    rows[0].get("TaskId")
}
async fn seed(c: &mut DorisConnection, url: &str) -> (Account, String) {
    c.execute(CREATE).await.unwrap();
    let account = create(url).await;
    let mut runner = connect(&account).await;
    runner.lock().await.unwrap();
    let owner = runner.migration_lock_owner().await.unwrap().unwrap();
    runner
        .ensure_migrations_table("_sqlx_migrations")
        .await
        .unwrap();
    runner.execute("INSERT INTO _sqlx_migrations(version,description,success,checksum,execution_time) VALUES(1,'reconcile',false,'checksum',-1)").await.unwrap();
    runner.close_hard().await.unwrap();
    retire(url, &account).await;
    (account, owner)
}
async fn cleanup(mut c: DorisConnection, url: &str, account: Account, owner: &str) {
    assert_eq!(
        c.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner)
    );
    assert_eq!(c.dirty_version("_sqlx_migrations").await.unwrap(), Some(1));
    c.recover_migration_lock(owner).await.unwrap();
    c.close().await.unwrap();
    Doris::drop_database(url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}
async fn guarded(url: &str, name: &str, owner: &str, success: bool) -> Output {
    call(
        url,
        &[
            "admin",
            "lock",
            "recover",
            "--owner",
            owner,
            "--retired-identity",
            name,
            "--maintenance",
            "--force",
        ],
        success,
    )
    .await
}
async fn cancel(
    url: &str,
    name: &str,
    job: &str,
    task: &str,
    force: bool,
    success: bool,
) -> Output {
    let mut args = vec![
        "admin",
        "drain",
        "cancel-mv-task",
        "--name",
        name,
        "--view",
        "mv",
        "--job-id",
        job,
        "--task-id",
        task,
    ];
    if force {
        args.extend(["--maintenance", "--force"]);
    }
    call(url, &args, success).await
}

#[tokio::test]
#[ignore = "requires Doris admin and externally reachable load fixture"]
async fn load_blocks_recovery_and_wait_observes_success_preserving_dirty_history() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    seed_owner(&mut c, true, GATE_OWNER).await;
    let store = object_store::Store::start().await;
    load(&mut c, &store, "l1").await;
    let status = call(
        &url,
        &["admin", "drain", "status", "--name", &account.name],
        false,
    )
    .await;
    assert!(text(&status).contains("LOAD job "), "{}", text(&status));
    assert!(
        text(&guarded(&url, &account.name, &owner, false).await).contains("no recovery DROP sent")
    );
    let bare = call(
        &url,
        &["admin", "lock", "recover", "--owner", &owner, "--force"],
        false,
    )
    .await;
    assert!(text(&bare).contains("no recovery DROP sent"));
    let bare_transition = call(
        &url,
        &[
            "admin",
            "lock",
            "recover-transition",
            "--owner",
            GATE_OWNER,
            "--lock-owner",
            &owner,
            "--maintenance",
            "--force",
        ],
        false,
    )
    .await;
    assert!(text(&bare_transition).contains("no recovery DROP sent"));
    assert_eq!(
        c.migration_transition_owner().await.unwrap().as_deref(),
        Some(GATE_OWNER)
    );
    let (child, out, err) = spawn(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "30",
            "--maintenance",
        ],
    );
    observed(&out, "LOAD job ").await;
    store.release();
    exited(child, &err, true).await;
    assert!(
        std::fs::read_to_string(out)
            .unwrap()
            .contains("observed FINISHED")
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(count, 1);
    call(
        &url,
        &[
            "admin",
            "lock",
            "recover-transition",
            "--owner",
            GATE_OWNER,
            "--lock-owner",
            &owner,
            "--maintenance",
            "--force",
        ],
        true,
    )
    .await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires Doris admin and externally reachable load fixture"]
async fn load_cancel_terminal_is_observed_but_label_reuse_is_not_an_id_fence() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    let store = object_store::Store::start().await;
    let first = load(&mut c, &store, "l1").await;
    let (child, out, err) = spawn(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "30",
            "--maintenance",
        ],
    );
    observed(&out, "LOAD job ").await;
    c.execute("CANCEL LOAD WHERE LABEL='l1'").await.unwrap();
    exited(child, &err, true).await;
    assert!(
        std::fs::read_to_string(out)
            .unwrap()
            .contains("observed CANCELLED")
    );
    let db: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut c)
        .await
        .unwrap();
    c.execute(sqlx::AssertSqlSafe(format!("CLEAN LABEL l1 FROM `{db}`")))
        .await
        .unwrap();
    let next = load(&mut c, &store, "l1").await;
    assert_ne!(first, next);
    // The same stale name-only SQL targets the new JobId. Do not wrap it as an
    // allegedly exact-ID CLI operation.
    c.execute("CANCEL LOAD WHERE LABEL='l1'").await.unwrap();
    assert_eq!(
        c.fetch_all("SHOW LOAD")
            .await
            .unwrap()
            .iter()
            .find(|r| r.get::<&str, _>("JobId") == next)
            .unwrap()
            .get::<&str, _>("State"),
        "CANCELLED"
    );
    store.release();
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires Doris admin and dedicated Kafka topic"]
async fn paused_routine_blocks_until_stopped_and_name_reuse_has_no_id_condition() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    let first = routine(&mut c).await;
    c.execute("PAUSE ROUTINE LOAD FOR rl").await.unwrap();
    let status = call(
        &url,
        &["admin", "drain", "status", "--name", &account.name],
        false,
    )
    .await;
    assert!(
        text(&status).contains("ROUTINE LOAD job ") && text(&status).contains("PAUSED"),
        "{}",
        text(&status)
    );
    guarded(&url, &account.name, &owner, false).await;
    let (child, out, err) = spawn(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "30",
            "--maintenance",
        ],
    );
    observed(&out, "ROUTINE LOAD job ").await;
    c.execute("STOP ROUTINE LOAD FOR rl").await.unwrap();
    exited(child, &err, true).await;
    assert!(
        std::fs::read_to_string(out)
            .unwrap()
            .contains("observed STOPPED")
    );
    let next = routine(&mut c).await;
    assert_ne!(first, next);
    c.execute("STOP ROUTINE LOAD FOR rl").await.unwrap();
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
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires Doris admin fixture"]
async fn manual_mv_waits_for_task_success_and_failed_task_does_not_repair_dirty_state() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    c.execute("INSERT INTO events VALUES(1,'source')")
        .await
        .unwrap();
    let job = view(&mut c, "mv", "ON MANUAL", true).await;
    task(&mut c, "mv", &job).await;
    let status = call(
        &url,
        &["admin", "drain", "status", "--name", &account.name],
        false,
    )
    .await;
    assert!(text(&status).contains("MV job "), "{}", text(&status));
    let (child, out, err) = spawn(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "30",
            "--maintenance",
        ],
    );
    observed(&out, "MV job ").await;
    exited(child, &err, true).await;
    assert!(
        std::fs::read_to_string(out)
            .unwrap()
            .contains("observed SUCCESS")
    );
    c.execute("DROP TABLE events").await.unwrap();
    c.execute("REFRESH MATERIALIZED VIEW mv COMPLETE")
        .await
        .unwrap();
    call(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "30",
            "--maintenance",
        ],
        true,
    )
    .await;
    assert!(
        c.fetch_all(sqlx::AssertSqlSafe(format!(
            "SELECT Status FROM tasks('type'='mv') WHERE JobId='{job}'"
        )))
        .await
        .unwrap()
        .iter()
        .any(|r| r.get::<&str, _>("Status") == "FAILED")
    );
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires Doris admin fixture"]
async fn automatic_mv_producers_block_without_tasks_and_pause_is_only_a_snapshot() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    let commit = view(&mut c, "committed", "ON COMMIT", false).await;
    // DEFERRED skips the initial build, but an unspecified schedule start can
    // still admit its first periodic task immediately. Keep the producer active
    // with its first run tomorrow, using the server's clock and time zone.
    let starts: String =
        sqlx::query_scalar("SELECT DATE_FORMAT(NOW() + INTERVAL 1 DAY, '%Y-%m-%d %H:%i:%s')")
            .fetch_one(&mut c)
            .await
            .unwrap();
    let scheduled = view(
        &mut c,
        "scheduled",
        &format!("ON SCHEDULE EVERY 1 HOUR STARTS '{starts}'"),
        false,
    )
    .await;
    // The table function can also expose retained tasks from other fixtures.
    assert!(
        c.fetch_all(sqlx::AssertSqlSafe(format!(
            "SELECT TaskId FROM tasks('type'='mv') WHERE JobId IN ('{commit}','{scheduled}')"
        )))
        .await
        .unwrap()
        .is_empty()
    );
    let status = call(
        &url,
        &["admin", "drain", "status", "--name", &account.name],
        false,
    )
    .await;
    assert!(text(&status).contains("MV scheduler "), "{}", text(&status));
    guarded(&url, &account.name, &owner, false).await;
    let (child, out, err) = spawn(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "30",
            "--maintenance",
        ],
    );
    observed(&out, "MV scheduler ").await;
    // Generic job pause is intentionally rejected for inner MV job names.
    assert!(
        c.execute(sqlx::AssertSqlSafe(format!(
            "PAUSE JOB WHERE jobName='inner_mtmv_{commit}'"
        )))
        .await
        .is_err()
    );
    c.execute("PAUSE MATERIALIZED VIEW JOB ON committed")
        .await
        .unwrap();
    c.execute("PAUSE MATERIALIZED VIEW JOB ON scheduled")
        .await
        .unwrap();
    exited(child, &err, true).await;
    // A paused job still admits explicit REFRESH. It is not a credential fence.
    c.execute("INSERT INTO events VALUES(1,'manual trigger')")
        .await
        .unwrap();
    c.execute("REFRESH MATERIALIZED VIEW committed COMPLETE")
        .await
        .unwrap();
    call(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "30",
            "--maintenance",
        ],
        true,
    )
    .await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires Doris admin fixture"]
async fn mv_cancel_preview_exact_ids_repeat_and_successor_tasks_are_safe() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    c.execute("INSERT INTO events VALUES(1,'source')")
        .await
        .unwrap();
    let job = view(&mut c, "mv", "ON MANUAL", true).await;
    let first = task(&mut c, "mv", &job).await;
    assert!(
        text(&cancel(&url, &account.name, &job, &first, false, true).await)
            .contains("Preview only")
    );
    cancel(&url, &account.name, "1", &first, true, false).await;
    assert!(
        text(&cancel(&url, &account.name, &job, &first, true, true).await)
            .contains("reported terminal CANCELED")
    );
    let next = task(&mut c, "mv", &job).await;
    assert_ne!(first, next);
    cancel(&url, &account.name, &job, &first, true, false).await;
    assert!(
        c.fetch_all(sqlx::AssertSqlSafe(format!(
            "SELECT Status FROM tasks('type'='mv') WHERE TaskId='{next}'"
        )))
        .await
        .unwrap()
        .iter()
        .any(|r| matches!(r.get::<&str, _>("Status"), "PENDING" | "RUNNING"))
    );
    cancel(&url, &account.name, &job, &next, true, true).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires Doris admin fixture"]
async fn mv_parent_generation_fences_late_cancel_after_view_replacement() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    c.execute("INSERT INTO events VALUES(1,'source')")
        .await
        .unwrap();
    let job = view(&mut c, "mv", "ON MANUAL", true).await;
    let id = task(&mut c, "mv", &job).await;
    let mut proxy = Proxy::start_mv(&opts, Point::BeforeDrop).await;
    let (child, _, err) = spawn(
        proxy.options.to_url_lossy().as_str(),
        &[
            "admin",
            "drain",
            "cancel-mv-task",
            "--name",
            &account.name,
            "--view",
            "mv",
            "--job-id",
            &job,
            "--task-id",
            &id,
            "--maintenance",
            "--force",
        ],
    );
    proxy.reached().await;
    c.execute("DROP MATERIALIZED VIEW mv").await.unwrap();
    let next_job = view(&mut c, "mv", "ON MANUAL", true).await;
    let next = task(&mut c, "mv", &next_job).await;
    assert_ne!(job, next_job);
    proxy.release(Release::Forward);
    exited(child, &err, false).await;
    assert!(
        std::fs::read_to_string(err)
            .unwrap()
            .contains("outcome is uncertain")
    );
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
    assert_eq!(proxy.trace.lock().unwrap().drop_ok, 0);
    proxy.finish().await;
    assert!(
        c.fetch_all(sqlx::AssertSqlSafe(format!(
            "SELECT Status FROM tasks('type'='mv') WHERE TaskId='{next}'"
        )))
        .await
        .unwrap()
        .iter()
        .any(|r| matches!(r.get::<&str, _>("Status"), "PENDING" | "RUNNING"))
    );
    cancel(&url, &account.name, &next_job, &next, true, true).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires Doris admin fixture"]
async fn mv_cancel_lost_ack_and_killed_cli_never_replay_or_clear_locks() {
    for kill in [false, true] {
        let (mut c, url, opts) = fixture().await;
        let (account, owner) = seed(&mut c, &url).await;
        c.execute("INSERT INTO events VALUES(1,'source')")
            .await
            .unwrap();
        let job = view(&mut c, "mv", "ON MANUAL", true).await;
        let id = task(&mut c, "mv", &job).await;
        let mut proxy = Proxy::start_mv(
            &opts,
            if kill {
                Point::BeforeDrop
            } else {
                Point::DropReply
            },
        )
        .await;
        let (mut child, _, err) = spawn(
            proxy.options.to_url_lossy().as_str(),
            &[
                "admin",
                "drain",
                "cancel-mv-task",
                "--name",
                &account.name,
                "--view",
                "mv",
                "--job-id",
                &job,
                "--task-id",
                &id,
                "--maintenance",
                "--force",
            ],
        );
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
            assert!(
                std::fs::read_to_string(err)
                    .unwrap()
                    .contains("outcome is uncertain")
            );
        }
        let trace = proxy.trace.lock().unwrap().clone();
        assert_eq!(trace.drop_requests, 1);
        assert_eq!(trace.drop_forwarded, 1);
        assert_eq!(trace.drop_ok, 1);
        assert_eq!(trace.parent_drops, 0);
        proxy.finish().await;
        assert_eq!(
            c.fetch_all(sqlx::AssertSqlSafe(format!(
                "SELECT Status FROM tasks('type'='mv') WHERE TaskId='{id}'"
            )))
            .await
            .unwrap()[0]
                .get::<&str, _>("Status"),
            "CANCELED"
        );
        cleanup(c, &url, account, &owner).await;
    }
}

#[tokio::test]
#[ignore = "requires Doris admin fixture"]
async fn missing_mv_history_and_async_read_disconnect_never_authorize_recovery() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = seed(&mut c, &url).await;
    c.execute("INSERT INTO events VALUES(1,'source')")
        .await
        .unwrap();
    let job = view(&mut c, "mv", "ON MANUAL", true).await;
    task(&mut c, "mv", &job).await;
    let mut proxy = Proxy::start_mv(&opts, Point::MvTasks(2)).await;
    let (child, out, err) = spawn(
        proxy.options.to_url_lossy().as_str(),
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "30",
            "--maintenance",
        ],
    );
    observed(&out, "MV job ").await;
    proxy.reached().await;
    c.execute("DROP MATERIALIZED VIEW mv").await.unwrap();
    proxy.release(Release::Forward);
    exited(child, &err, false).await;
    assert!(
        std::fs::read_to_string(err)
            .unwrap()
            .contains("disappeared")
    );
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 0);
    proxy.finish().await;
    view(&mut c, "mv", "ON MANUAL", false).await;
    let mut proxy = Proxy::start_main(&opts, Point::MvTasks(1)).await;
    let (child, _, err) = spawn(
        proxy.options.to_url_lossy().as_str(),
        &[
            "admin",
            "lock",
            "recover",
            "--owner",
            &owner,
            "--retired-identity",
            &account.name,
            "--maintenance",
            "--force",
        ],
    );
    proxy.reached().await;
    proxy.release(Release::Disconnect);
    exited(child, &err, false).await;
    assert!(
        std::fs::read_to_string(err)
            .unwrap()
            .contains("no recovery DROP sent")
    );
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 0);
    proxy.finish().await;
    cleanup(c, &url, account, &owner).await;
}
