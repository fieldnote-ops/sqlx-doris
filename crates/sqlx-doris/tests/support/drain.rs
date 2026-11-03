//! Native drain observations and recovery guards; all temporary output external.
use super::*;
#[path = "drain_jobs.rs"]
mod jobs;

fn spawn(url: &str, args: &[&str]) -> (std::process::Child, PathBuf, PathBuf) {
    let base = PathBuf::from(std::env::var("SQLX_DORIS_ARTIFACTS_DIR").unwrap())
        .join(format!("drain-{}", uuid::Uuid::new_v4().simple()));
    let out = base.with_extension("stdout.log");
    let err = base.with_extension("stderr.log");
    let child = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
        .args(["--replication-num", "1", "--lock-timeout", "0"])
        .args(args)
        .env("DATABASE_URL", url)
        .stdout(Stdio::from(std::fs::File::create(&out).unwrap()))
        .stderr(Stdio::from(std::fs::File::create(&err).unwrap()))
        .spawn()
        .unwrap();
    (child, out, err)
}
async fn observed(path: &std::path::Path, needle: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if std::fs::read_to_string(path).unwrap().contains(needle) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
async fn exited(mut child: std::process::Child, err: &std::path::Path, success: bool) {
    let status = tokio::task::spawn_blocking(move || child.wait().unwrap())
        .await
        .unwrap();
    assert_eq!(
        status.success(),
        success,
        "{}",
        std::fs::read_to_string(err).unwrap()
    );
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn wait_observes_admitted_commit_and_idle_session_then_fresh_guard_recovers() {
    let (mut admin, url, _) = fixture().await;
    admin.execute(CREATE).await.unwrap();
    let account = create(&url).await;
    let mut old = connect(&account).await;
    old.lock().await.unwrap();
    let owner = old.migration_lock_owner().await.unwrap().unwrap();
    let mut tx = old.begin().await.unwrap();
    tx.execute("INSERT INTO events VALUES(1,'admitted')")
        .await
        .unwrap();
    retire(&url, &account).await;
    let status = call(
        &url,
        &["admin", "drain", "status", "--name", &account.name],
        false,
    )
    .await;
    assert!(text(&status).contains("PREPARE"));
    assert!(text(&status).contains("session "));
    let blocked = call(
        &url,
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
        false,
    )
    .await;
    assert!(text(&blocked).contains("no recovery DROP sent"));
    let (child, out, err) = spawn(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "15",
            "--maintenance",
        ],
    );
    observed(&out, "PREPARE").await;
    tx.commit().await.unwrap();
    observed(&out, "terminal VISIBLE").await;
    // Idle retired sessions block even though their transactions are terminal.
    assert_eq!(
        admin.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner.as_str())
    );
    old.close_hard().await.unwrap();
    exited(child, &err, true).await;
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!(count, 1);
    call(
        &url,
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
        true,
    )
    .await;
    assert!(admin.migration_lock_owner().await.unwrap().is_none());
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn unrelated_transaction_blocks_and_rollback_terminal_is_observed() {
    let (mut admin, url, opts) = fixture().await;
    admin.execute(CREATE).await.unwrap();
    let account = create(&url).await;
    retire(&url, &account).await;
    let mut other = DorisConnection::connect_with(&opts).await.unwrap();
    let mut tx = other.begin().await.unwrap();
    tx.execute("INSERT INTO events VALUES(1,'must rollback')")
        .await
        .unwrap();
    let (child, out, err) = spawn(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "15",
            "--maintenance",
        ],
    );
    observed(&out, "owner not available").await;
    tx.rollback().await.unwrap();
    exited(child, &err, true).await;
    assert!(
        std::fs::read_to_string(out)
            .unwrap()
            .contains("terminal ABORTED")
    );
    other.close().await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn column_job_waits_to_terminal_and_native_cancel_ignores_wrong_id() {
    let (mut admin, url, opts) = fixture().await;
    admin.execute("CREATE TABLE heavy(id BIGINT NOT NULL,v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1','light_schema_change'='false')").await.unwrap();
    let account = create(&url).await;
    let mut old = connect(&account).await;
    old.lock().await.unwrap();
    let owner = old.migration_lock_owner().await.unwrap().unwrap();
    let mut writer = DorisConnection::connect_with(&opts).await.unwrap();
    let mut tx = writer.begin().await.unwrap();
    tx.execute("INSERT INTO heavy VALUES(1,1)").await.unwrap();
    old.execute("ALTER TABLE heavy MODIFY COLUMN v BIGINT")
        .await
        .unwrap();
    retire(&url, &account).await;
    old.close_hard().await.unwrap();
    let status = call(
        &url,
        &["admin", "drain", "status", "--name", &account.name],
        false,
    )
    .await;
    assert!(text(&status).contains("COLUMN job"));
    // Demonstrate the real FE bug: do not expose this SQL as a safe CLI action.
    admin
        .execute("CANCEL ALTER TABLE COLUMN FROM heavy (9223372036854775807)")
        .await
        .unwrap();
    assert!(
        admin
            .fetch_all("SHOW ALTER TABLE COLUMN")
            .await
            .unwrap()
            .iter()
            .all(|r| r.get::<&str, _>("State") == "CANCELLED")
    );
    old = DorisConnection::connect_with(&opts).await.unwrap();
    old.execute("ALTER TABLE heavy MODIFY COLUMN v BIGINT")
        .await
        .unwrap();
    let jobs = admin.fetch_all("SHOW ALTER TABLE COLUMN").await.unwrap();
    assert!(
        jobs.iter()
            .any(|r| !matches!(r.get::<&str, _>("State"), "FINISHED" | "CANCELLED"))
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
    observed(&out, "COLUMN job").await;
    tx.rollback().await.unwrap();
    exited(child, &err, true).await;
    assert!(
        std::fs::read_to_string(out)
            .unwrap()
            .contains("terminal FINISHED")
    );
    assert_eq!(
        admin.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner.as_str())
    );
    old.close().await.unwrap();
    writer.close().await.unwrap();
    admin.recover_migration_lock(&owner).await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn explicit_rollup_cancel_is_preview_first_exact_id_and_repeat_safe() {
    let (mut admin, url, opts) = fixture().await;
    admin.execute("CREATE TABLE heavy(id BIGINT NOT NULL,v INT,z INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')").await.unwrap();
    let account = create(&url).await;
    let mut old = connect(&account).await;
    let mut writer = DorisConnection::connect_with(&opts).await.unwrap();
    let mut tx = writer.begin().await.unwrap();
    tx.execute("INSERT INTO heavy VALUES(1,1,1)").await.unwrap();
    old.execute("ALTER TABLE heavy ADD ROLLUP r1(v,id)")
        .await
        .unwrap();
    retire(&url, &account).await;
    let jobs = admin.fetch_all("SHOW ALTER TABLE ROLLUP").await.unwrap();
    let id = jobs[0].get::<String, _>("JobId");
    let preview = call(
        &url,
        &[
            "admin",
            "drain",
            "cancel-rollup",
            "--name",
            &account.name,
            "--table",
            "heavy",
            "--job-id",
            &id,
        ],
        true,
    )
    .await;
    assert!(text(&preview).contains("Preview only"));
    call(
        &url,
        &[
            "admin",
            "drain",
            "cancel-rollup",
            "--name",
            &account.name,
            "--table",
            "heavy",
            "--job-id",
            "9223372036854775807",
            "--maintenance",
            "--force",
        ],
        false,
    )
    .await;
    call(
        &url,
        &[
            "admin",
            "drain",
            "cancel-rollup",
            "--name",
            &account.name,
            "--table",
            "heavy",
            "--job-id",
            &id,
            "--maintenance",
            "--force",
        ],
        true,
    )
    .await;
    // A successor on the same table must survive repeated cancellation of r1.
    old.close_hard().await.unwrap();
    admin
        .execute("ALTER TABLE heavy ADD ROLLUP r2(v,id)")
        .await
        .unwrap();
    let repeat = call(
        &url,
        &[
            "admin",
            "drain",
            "cancel-rollup",
            "--name",
            &account.name,
            "--table",
            "heavy",
            "--job-id",
            &id,
            "--maintenance",
            "--force",
        ],
        false,
    )
    .await;
    assert!(text(&repeat).contains("already terminal; no CANCEL sent"));
    let jobs = admin.fetch_all("SHOW ALTER TABLE ROLLUP").await.unwrap();
    let next = jobs
        .iter()
        .find(|r| r.get::<&str, _>("JobId") != id)
        .unwrap();
    assert!(!matches!(
        next.get::<&str, _>("State"),
        "FINISHED" | "CANCELLED"
    ));
    let next_id = next.get::<String, _>("JobId");
    call(
        &url,
        &[
            "admin",
            "drain",
            "cancel-rollup",
            "--name",
            &account.name,
            "--table",
            "heavy",
            "--job-id",
            &next_id,
            "--maintenance",
            "--force",
        ],
        true,
    )
    .await;
    tx.rollback().await.unwrap();
    writer.close().await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn timeout_disconnect_cancel_and_recreated_identity_preserve_both_locks() {
    let (mut admin, url, opts) = fixture().await;
    let account = create(&url).await;
    let mut old = connect(&account).await;
    old.lock().await.unwrap();
    let owner = old.migration_lock_owner().await.unwrap().unwrap();
    seed_owner(&mut admin, true, GATE_OWNER).await;
    let present = call(
        &url,
        &["admin", "drain", "status", "--name", &account.name],
        false,
    )
    .await;
    assert!(text(&present).contains("still present"));
    retire(&url, &account).await;
    call(
        &url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "1",
            "--maintenance",
        ],
        false,
    )
    .await;
    for outcome in ["disconnect", "kill", "timeout"] {
        let mut proxy = Proxy::start(&opts, Point::DrainRead).await;
        let (mut child, _, err) = spawn(
            proxy.options.to_url_lossy().as_str(),
            &[
                "--ddl-timeout",
                "1",
                "admin",
                "lock",
                "recover-transition",
                "--owner",
                GATE_OWNER,
                "--lock-owner",
                &owner,
                "--retired-identity",
                &account.name,
                "--maintenance",
                "--force",
            ],
        );
        proxy.reached().await;
        if outcome == "kill" {
            child.kill().unwrap();
        }
        if outcome == "timeout" {
            exited(child, &err, false).await;
            assert!(
                std::fs::read_to_string(&err)
                    .unwrap()
                    .contains("drain check timed out")
            );
            proxy.release(Release::Disconnect);
        } else {
            proxy.release(Release::Disconnect);
            exited(child, &err, false).await;
        }
        assert_eq!(proxy.trace.lock().unwrap().drop_requests, 0);
        proxy.finish().await;
    }
    old.close_hard().await.unwrap();
    // A new identity at the retired name invalidates the check, even on empty DB.
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE USER '{}'@'%' IDENTIFIED BY 'not-reused-by-cli'",
            account.name
        )))
        .await
        .unwrap();
    call(
        &url,
        &["admin", "drain", "status", "--name", &account.name],
        false,
    )
    .await;
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "DROP USER '{}'@'%'",
            account.name
        )))
        .await
        .unwrap();
    assert_eq!(
        admin.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner.as_str())
    );
    assert_eq!(
        admin.migration_transition_owner().await.unwrap().as_deref(),
        Some(GATE_OWNER)
    );
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
            "--retired-identity",
            &account.name,
            "--maintenance",
            "--force",
        ],
        true,
    )
    .await;
    call(
        &url,
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
        true,
    )
    .await;
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn disappearing_transaction_history_is_never_treated_as_terminal() {
    let (mut admin, url, opts) = fixture().await;
    admin.execute(CREATE).await.unwrap();
    let account = create(&url).await;
    retire(&url, &account).await;
    let mut writer = DorisConnection::connect_with(&opts).await.unwrap();
    let mut tx = writer.begin().await.unwrap();
    tx.execute("INSERT INTO events VALUES(1,'unobserved terminal')")
        .await
        .unwrap();
    let mut proxy = Proxy::start_identity(&opts, Point::TransactionRead).await;
    let (child, out, err) = spawn(
        proxy.options.to_url_lossy().as_str(),
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--timeout",
            "15",
            "--maintenance",
        ],
    );
    proxy.reached().await;
    observed(&out, "PREPARE").await;
    tx.rollback().await.unwrap();
    let database = opts.get_database().unwrap();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "CLEAN LABEL FROM `{database}`"
        )))
        .await
        .unwrap();
    proxy.release(Release::Forward);
    exited(child, &err, false).await;
    // SHOW TRANSACTION may return a server "not found" error rather than zero
    // rows. Both must stop the observer, with no completion output or mutation.
    assert!(
        !std::fs::read_to_string(out)
            .unwrap()
            .contains("Scoped snapshot empty")
    );
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 0);
    proxy.finish().await;
    writer.close().await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn rollup_cancel_lost_ack_or_killed_cli_never_replays_or_recovers_locks() {
    for kill in [false, true] {
        let (mut admin, url, opts) = fixture().await;
        admin.execute("CREATE TABLE heavy(id BIGINT NOT NULL,v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')").await.unwrap();
        let account = create(&url).await;
        let mut old = connect(&account).await;
        old.lock().await.unwrap();
        let owner = old.migration_lock_owner().await.unwrap().unwrap();
        let mut writer = DorisConnection::connect_with(&opts).await.unwrap();
        let mut tx = writer.begin().await.unwrap();
        tx.execute("INSERT INTO heavy VALUES(1,1)").await.unwrap();
        old.execute("ALTER TABLE heavy ADD ROLLUP r1(v,id)")
            .await
            .unwrap();
        retire(&url, &account).await;
        let id =
            admin.fetch_all("SHOW ALTER TABLE ROLLUP").await.unwrap()[0].get::<String, _>("JobId");
        let mut proxy = Proxy::start_rollup(
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
                "cancel-rollup",
                "--name",
                &account.name,
                "--table",
                "heavy",
                "--job-id",
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
                    .contains("CANCEL outcome is uncertain")
            );
        }
        let trace = proxy.trace.lock().unwrap().clone();
        assert_eq!(trace.drop_requests, 1);
        assert_eq!(trace.drop_forwarded, 1);
        assert_eq!(trace.drop_ok, 1);
        assert_eq!(trace.parent_drops, 0);
        proxy.finish().await;
        assert_eq!(
            admin.fetch_all("SHOW ALTER TABLE ROLLUP").await.unwrap()[0].get::<&str, _>("State"),
            "CANCELLED"
        );
        assert_eq!(
            admin.migration_lock_owner().await.unwrap().as_deref(),
            Some(owner.as_str())
        );
        tx.rollback().await.unwrap();
        writer.close().await.unwrap();
        old.close_hard().await.unwrap();
        admin.recover_migration_lock(&owner).await.unwrap();
        admin.close().await.unwrap();
        Doris::drop_database(&url).await.unwrap();
        std::fs::remove_file(account.path).unwrap();
    }
}
