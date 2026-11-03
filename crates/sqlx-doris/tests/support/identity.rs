//! Real built-in RBAC and managed-identity CLI contracts.
#[path = "drain.rs"]
mod drain;
use super::*;
use std::{
    path::PathBuf,
    process::{Command, Output, Stdio},
};

struct Account {
    name: String,
    path: PathBuf,
    options: DorisConnectOptions,
}
async fn call(url: &str, args: &[&str], success: bool) -> Output {
    let url = url.to_owned();
    let args: Vec<String> = args.iter().map(|s| s.to_string()).collect();
    let output = tokio::task::spawn_blocking(move || {
        let mut command = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"));
        command.args(["--replication-num", "1", "--lock-timeout", "0"]);
        if args.iter().any(|v| v == "--identity-file") {
            command
                .env_remove("DATABASE_URL")
                .env_remove("DORIS_TEST_URL");
        } else {
            command.env("DATABASE_URL", url);
        }
        command.args(args).output().unwrap()
    })
    .await
    .unwrap();
    assert_eq!(
        output.status.success(),
        success,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    output
}
fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}
async fn create(url: &str) -> Account {
    let path = PathBuf::from(std::env::var("SQLX_DORIS_BUILD_ROOT").unwrap())
        .join("identity-contracts")
        .join(format!("{}.json", uuid::Uuid::new_v4().simple()));
    let output = call(
        url,
        &[
            "admin",
            "identity",
            "create",
            "--output",
            path.to_str().unwrap(),
        ],
        true,
    )
    .await;
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    let name = json["name"].as_str().unwrap().to_owned();
    let options = json["database_url"]
        .as_str()
        .unwrap()
        .parse::<DorisConnectOptions>()
        .unwrap()
        .migration_replication(1)
        .migration_lock_timeout(Duration::from_millis(100));
    assert!(text(&output).contains(&name));
    assert!(!text(&output).contains(options.to_url_lossy().password().unwrap()));
    Account {
        name,
        path,
        options,
    }
}
async fn retire(url: &str, account: &Account) -> Output {
    call(
        url,
        &[
            "admin",
            "identity",
            "retire",
            "--name",
            &account.name,
            "--maintenance",
            "--force",
        ],
        true,
    )
    .await
}
async fn connect(account: &Account) -> DorisConnection {
    DorisConnection::connect_with(&account.options)
        .await
        .unwrap()
}
async fn account_exists(c: &mut DorisConnection, name: &str) -> bool {
    c.fetch_all("SHOW ALL GRANTS")
        .await
        .unwrap()
        .iter()
        .any(|r| r.get::<&str, _>("UserIdentity") == format!("'{name}'@'%'"))
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn managed_retirement_blocks_raw_cached_and_new_sessions_preserving_dirty_history() {
    let (mut admin, url, opts) = fixture().await;
    let account = create(&url).await;
    let mut runner = connect(&account).await;
    let m = migrator(vec![migration(
        1,
        &format!(
            "{CREATE}; INSERT INTO events VALUES(1,'committed'); INSERT INTO events(missing) VALUES(2)"
        ),
        MigrationType::Simple,
    )]);
    assert!(m.run(&mut runner).await.is_err());
    runner.close_hard().await.unwrap();
    let owner = admin.migration_lock_owner().await.unwrap().unwrap();
    let history: Vec<(i64, String, bool, i64)> =
        sqlx::query_as("SELECT version,checksum,success,execution_time FROM _sqlx_migrations")
            .fetch_all(&mut admin)
            .await
            .unwrap();
    let mut old = connect(&account).await;
    sqlx::query("INSERT INTO events VALUES(?,'cached')")
        .bind(2_i64)
        .execute(&mut old)
        .await
        .unwrap();
    let preview = call(
        &url,
        &["admin", "identity", "retire", "--name", &account.name],
        true,
    )
    .await;
    assert!(text(&preview).contains("Preview only; no changes made"));
    call(
        &url,
        &[
            "admin",
            "identity",
            "retire",
            "--name",
            &account.name,
            "--force",
        ],
        false,
    )
    .await;
    sqlx::query("INSERT INTO events VALUES(?,'cached')")
        .bind(3_i64)
        .execute(&mut old)
        .await
        .unwrap();
    let output = retire(&url, &account).await;
    assert!(text(&output).contains("No locks/history removed"));
    assert!(
        old.execute("INSERT INTO events VALUES(4,'denied')")
            .await
            .is_err()
    );
    assert!(
        sqlx::query("INSERT INTO events VALUES(?,'cached')")
            .bind(5_i64)
            .execute(&mut old)
            .await
            .is_err()
    );
    assert!(old.execute("CREATE TABLE denied(id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')").await.is_err());
    assert!(
        DorisConnection::connect_with(&account.options)
            .await
            .is_err()
    );
    assert_eq!(
        admin.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner.as_str())
    );
    let after: Vec<(i64, String, bool, i64)> =
        sqlx::query_as("SELECT version,checksum,success,execution_time FROM _sqlx_migrations")
            .fetch_all(&mut admin)
            .await
            .unwrap();
    assert_eq!(after, history);
    assert_eq!(
        admin.dirty_version("_sqlx_migrations").await.unwrap(),
        Some(1)
    );
    let mut next = DorisConnection::connect_with(&opts).await.unwrap();
    assert!(next.lock().await.is_err());
    call(
        &url,
        &[
            "admin",
            "identity",
            "retire",
            "--name",
            &account.name,
            "--maintenance",
            "--force",
        ],
        false,
    )
    .await;
    old.close_hard().await.unwrap();
    admin.recover_migration_lock(&owner).await.unwrap();
    assert_eq!(
        admin.dirty_version("_sqlx_migrations").await.unwrap(),
        Some(1)
    );
    next.close().await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn direct_revoke_is_not_login_isolation_and_inherited_roles_still_authorize() {
    let (mut admin, url, _) = fixture().await;
    admin.execute(CREATE).await.unwrap();
    let one = create(&url).await;
    let two = create(&url).await;
    let db = one.options.get_database().unwrap();
    let mut old = connect(&one).await;
    let mut other = connect(&two).await;
    sqlx::query("INSERT INTO events VALUES(?,'prepared')")
        .bind(1_i64)
        .execute(&mut old)
        .await
        .unwrap();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "REVOKE LOAD_PRIV ON `{db}`.* FROM '{}'@'%'",
            one.name
        )))
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO events VALUES(?,'prepared')")
            .bind(2_i64)
            .execute(&mut old)
            .await
            .is_err()
    );
    connect(&one).await.close().await.unwrap();
    let role = format!("sdk_role_{}", uuid::Uuid::new_v4().simple());
    admin
        .execute(sqlx::AssertSqlSafe(format!("CREATE ROLE {role}")))
        .await
        .unwrap();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "GRANT LOAD_PRIV ON `{db}`.* TO ROLE {role}"
        )))
        .await
        .unwrap();
    for name in [&one.name, &two.name] {
        admin
            .execute(sqlx::AssertSqlSafe(format!("GRANT {role} TO '{name}'@'%'")))
            .await
            .unwrap();
    }
    sqlx::query("INSERT INTO events VALUES(?,'prepared')")
        .bind(3_i64)
        .execute(&mut old)
        .await
        .unwrap();
    let rejected = call(
        &url,
        &[
            "admin",
            "identity",
            "retire",
            "--name",
            &one.name,
            "--maintenance",
            "--force",
        ],
        false,
    )
    .await;
    assert!(text(&rejected).contains("Roles"));
    // Native DROP removes only this identity's role links, never a shared role.
    admin
        .execute(sqlx::AssertSqlSafe(format!("DROP USER '{}'@'%'", one.name)))
        .await
        .unwrap();
    assert!(
        sqlx::query("INSERT INTO events VALUES(?,'prepared')")
            .bind(4_i64)
            .execute(&mut old)
            .await
            .is_err()
    );
    other
        .execute("INSERT INTO events VALUES(5,'unaffected role member')")
        .await
        .unwrap();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "REVOKE {role} FROM '{}'@'%'",
            two.name
        )))
        .await
        .unwrap();
    retire(&url, &two).await;
    admin
        .execute(sqlx::AssertSqlSafe(format!("DROP ROLE {role}")))
        .await
        .unwrap();
    old.close_hard().await.unwrap();
    other.close_hard().await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(one.path).unwrap();
    std::fs::remove_file(two.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn retirement_rejects_host_variants_unmarked_users_and_expanded_permissions() {
    let (mut admin, url, _) = fixture().await;
    let account = create(&url).await;
    call(
        &url,
        &[
            "admin",
            "identity",
            "retire",
            "--name",
            "root",
            "--maintenance",
            "--force",
        ],
        false,
    )
    .await;
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "CREATE USER '{}'@'localhost' IDENTIFIED BY 'fixture-only'",
            account.name
        )))
        .await
        .unwrap();
    assert!(
        text(
            &call(
                &url,
                &[
                    "admin",
                    "identity",
                    "retire",
                    "--name",
                    &account.name,
                    "--maintenance",
                    "--force"
                ],
                false
            )
            .await
        )
        .contains("host identities")
    );
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "DROP USER '{}'@'localhost'",
            account.name
        )))
        .await
        .unwrap();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "GRANT SELECT_PRIV ON *.*.* TO '{}'@'%'",
            account.name
        )))
        .await
        .unwrap();
    assert!(
        text(
            &call(
                &url,
                &[
                    "admin",
                    "identity",
                    "retire",
                    "--name",
                    &account.name,
                    "--maintenance",
                    "--force"
                ],
                false
            )
            .await
        )
        .contains("unexpected")
    );
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "REVOKE SELECT_PRIV ON *.*.* FROM '{}'@'%'",
            account.name
        )))
        .await
        .unwrap();
    let row = admin
        .fetch_all(sqlx::AssertSqlSafe(format!(
            "SHOW GRANTS FOR '{}'@'%'",
            account.name
        )))
        .await
        .unwrap();
    let marker = row[0].get::<String, _>("Comment");
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER USER '{}'@'%' COMMENT 'not tool managed'",
            account.name
        )))
        .await
        .unwrap();
    assert!(
        text(
            &call(
                &url,
                &[
                    "admin",
                    "identity",
                    "retire",
                    "--name",
                    &account.name,
                    "--maintenance",
                    "--force"
                ],
                false
            )
            .await
        )
        .contains("marker")
    );
    assert!(account_exists(&mut admin, &account.name).await);
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER USER '{}'@'%' COMMENT '{marker}'",
            account.name
        )))
        .await
        .unwrap();
    retire(&url, &account).await;
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn pending_transaction_can_commit_after_identity_retirement() {
    let (mut admin, url, _) = fixture().await;
    admin.execute(CREATE).await.unwrap();
    let account = create(&url).await;
    let mut old = connect(&account).await;
    old.lock().await.unwrap();
    let owner = old.migration_lock_owner().await.unwrap().unwrap();
    old.execute("INSERT INTO events VALUES(1,'committed earlier')")
        .await
        .unwrap();
    let mut tx = old.begin().await.unwrap();
    tx.execute("INSERT INTO events VALUES(2,'admitted transaction')")
        .await
        .unwrap();
    retire(&url, &account).await;
    assert!(
        DorisConnection::connect_with(&account.options)
            .await
            .is_err()
    );
    // This commit is intentionally allowed by Doris: isolation is not rollback.
    tx.commit().await.unwrap();
    assert!(
        old.execute("INSERT INTO events VALUES(3,'new denied')")
            .await
            .is_err()
    );
    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM events ORDER BY id")
        .fetch_all(&mut admin)
        .await
        .unwrap();
    assert_eq!(ids, [1, 2]);
    assert_eq!(
        admin.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner.as_str())
    );
    old.close_hard().await.unwrap();
    admin.recover_migration_lock(&owner).await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn submitted_schema_job_outlives_identity_retirement() {
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
    let jobs = admin.fetch_all("SHOW ALTER TABLE COLUMN").await.unwrap();
    assert!(
        jobs.iter()
            .any(|r| !matches!(r.get::<&str, _>("State"), "FINISHED" | "CANCELLED"))
    );
    assert!(
        old.execute("ALTER TABLE heavy ADD COLUMN denied INT")
            .await
            .is_err()
    );
    assert_eq!(
        admin.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner.as_str())
    );
    tx.rollback().await.unwrap();
    writer.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let jobs = admin.fetch_all("SHOW ALTER TABLE COLUMN").await.unwrap();
            if jobs.iter().all(|r| r.get::<&str, _>("State") == "FINISHED") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    old.close_hard().await.unwrap();
    admin.recover_migration_lock(&owner).await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(account.path).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn credential_files_are_private_external_and_work_with_native_cli_migrations() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let (admin, url, _) = fixture().await;
    let account = create(&url).await;
    assert_eq!(
        std::fs::metadata(&account.path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let original = std::fs::read(&account.path).unwrap();
    call(
        &url,
        &[
            "admin",
            "identity",
            "create",
            "--output",
            account.path.to_str().unwrap(),
        ],
        false,
    )
    .await;
    assert_eq!(std::fs::read(&account.path).unwrap(), original);
    let exposed = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
        .args([
            "--identity-file",
            account.path.to_str().unwrap(),
            "admin",
            "lock",
            "status",
        ])
        .env("DATABASE_URL", &url)
        .env_remove("DORIS_TEST_URL")
        .output()
        .unwrap();
    assert!(!exposed.status.success());
    assert!(text(&exposed).contains("do not pass administrator/database URLs"));
    assert!(!text(&exposed).contains(&url));
    let source = account.path.with_extension("migrations");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("1_create.sql"), CREATE).unwrap();
    let run = call(
        &url,
        &[
            "--identity-file",
            account.path.to_str().unwrap(),
            "migrate",
            "--source",
            source.to_str().unwrap(),
            "run",
        ],
        true,
    )
    .await;
    assert!(text(&run).contains(&format!("managed-identity {} main-owner", account.name)));
    assert!(!text(&run).contains(account.options.to_url_lossy().password().unwrap()));
    struct RestorePrivateMode(PathBuf);
    impl Drop for RestorePrivateMode {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(0o600));
        }
    }
    let restore_private_mode = RestorePrivateMode(account.path.clone());
    std::fs::set_permissions(&account.path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        text(
            &call(
                &url,
                &[
                    "--identity-file",
                    account.path.to_str().unwrap(),
                    "admin",
                    "lock",
                    "status"
                ],
                false
            )
            .await
        )
        .contains("0600")
    );
    std::fs::set_permissions(&account.path, std::fs::Permissions::from_mode(0o600)).unwrap();
    drop(restore_private_mode);
    let link = account.path.with_extension("link");
    symlink(&account.path, &link).unwrap();
    assert!(
        text(
            &call(
                &url,
                &[
                    "--identity-file",
                    link.to_str().unwrap(),
                    "admin",
                    "lock",
                    "status"
                ],
                false
            )
            .await
        )
        .contains("symlink")
    );
    call(
        &url,
        &[
            "--identity-file",
            account.path.to_str().unwrap(),
            "admin",
            "identity",
            "retire",
            "--name",
            &account.name,
            "--maintenance",
            "--force",
        ],
        false,
    )
    .await;
    let mut modified: serde_json::Value = serde_json::from_slice(&original).unwrap();
    modified["database_url"] = format!(
        "{}&ssl-cert=administrator.pem",
        modified["database_url"].as_str().unwrap()
    )
    .into();
    std::fs::write(&account.path, serde_json::to_vec(&modified).unwrap()).unwrap();
    assert!(
        text(
            &call(
                &url,
                &[
                    "--identity-file",
                    account.path.to_str().unwrap(),
                    "admin",
                    "lock",
                    "status"
                ],
                false
            )
            .await
        )
        .contains("client certificates")
    );
    std::fs::write(&account.path, &original).unwrap();
    let another = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("identity-must-not-create.json");
    call(
        &url,
        &[
            "admin",
            "identity",
            "create",
            "--output",
            another.to_str().unwrap(),
        ],
        false,
    )
    .await;
    assert!(!another.exists());
    retire(&url, &account).await;
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
    std::fs::remove_file(link).unwrap();
    std::fs::remove_file(account.path).unwrap();
    std::fs::remove_dir_all(source).unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn retirement_disconnect_and_process_cancellation_never_replay_account_drop() {
    for (point, kill) in [
        (Point::BeforeDrop, false),
        (Point::DropReply, false),
        (Point::IdentityRead(3), false),
        (Point::BeforeDrop, true),
    ] {
        let (mut admin, url, opts) = fixture().await;
        let account = create(&url).await;
        let mut old = connect(&account).await;
        old.lock().await.unwrap();
        let owner = old.migration_lock_owner().await.unwrap().unwrap();
        let mut proxy = Proxy::start_identity(&opts, point).await;
        let base = PathBuf::from(std::env::var("SQLX_DORIS_ARTIFACTS_DIR").unwrap())
            .join(format!("identity-fault-{}", uuid::Uuid::new_v4().simple()));
        let err = base.with_extension("stderr.log");
        let out = base.with_extension("stdout.log");
        let mut child = Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
            .args([
                "admin",
                "identity",
                "retire",
                "--name",
                &account.name,
                "--maintenance",
                "--force",
            ])
            .env("DATABASE_URL", proxy.options.to_url_lossy().as_str())
            .stdout(Stdio::from(std::fs::File::create(&out).unwrap()))
            .stderr(Stdio::from(std::fs::File::create(&err).unwrap()))
            .spawn()
            .unwrap();
        proxy.reached().await;
        if kill {
            child.kill().unwrap();
            assert!(!child.wait().unwrap().success());
            proxy.release(Release::Forward);
            proxy.acknowledged().await;
        } else {
            proxy.release(Release::Disconnect);
            let status = tokio::task::spawn_blocking(move || child.wait().unwrap())
                .await
                .unwrap();
            assert!(!status.success());
            let error = std::fs::read_to_string(&err).unwrap();
            assert!(
                error.contains(if point == Point::IdentityRead(3) {
                    "DROP acknowledged; verification failed"
                } else {
                    "outcome is uncertain"
                }),
                "{error}"
            );
        }
        let trace = proxy.trace.lock().unwrap().clone();
        assert_eq!(trace.drop_requests, 1);
        assert_eq!(trace.parent_drops, 0);
        proxy.finish().await;
        assert_eq!(
            account_exists(&mut admin, &account.name).await,
            point == Point::BeforeDrop && !kill
        );
        assert_eq!(
            admin.migration_lock_owner().await.unwrap().as_deref(),
            Some(owner.as_str())
        );
        if point == Point::BeforeDrop && !kill {
            retire(&url, &account).await;
        } else {
            call(
                &url,
                &[
                    "admin",
                    "identity",
                    "retire",
                    "--name",
                    &account.name,
                    "--maintenance",
                    "--force",
                ],
                false,
            )
            .await;
        }
        assert!(
            DorisConnection::connect_with(&account.options)
                .await
                .is_err()
        );
        old.close_hard().await.unwrap();
        admin.recover_migration_lock(&owner).await.unwrap();
        admin.close().await.unwrap();
        Doris::drop_database(&url).await.unwrap();
        std::fs::remove_file(account.path).unwrap();
    }
}

#[path = "cli_unified.rs"]
mod cli_unified;
