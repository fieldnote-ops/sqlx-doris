#![cfg(feature = "migrate")]
#[cfg(feature = "cli-admin")]
#[path = "support/identity.rs"]
mod identity;
#[path = "support/recovery_proxy.rs"]
mod recovery_proxy;
use recovery_proxy::{Point, Proxy, Release};
use sqlx::{
    ConnectOptions, Connection, Executor, Row, SqlSafeStr,
    migrate::{Migrate, MigrateDatabase, MigrateError, Migration, MigrationType, Migrator},
};
use sqlx_doris::{Doris, DorisConnectOptions, DorisConnection};
use std::{borrow::Cow, time::Duration};

async fn fixture() -> (DorisConnection, String, DorisConnectOptions) {
    let db = format!(
        "sdk_migrate_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let opts = std::env::var("DORIS_TEST_URL")
        .expect("DORIS_TEST_URL required")
        .parse::<DorisConnectOptions>()
        .unwrap()
        .database(&db)
        .migration_replication(1)
        .migration_lock_timeout(Duration::from_millis(200))
        .migration_timeout(Duration::from_secs(30));
    let url = opts.to_url_lossy().to_string();
    Doris::create_database(&url).await.unwrap();
    (
        DorisConnection::connect_with(&opts).await.unwrap(),
        url,
        opts,
    )
}
fn migration(version: i64, sql: &str, kind: MigrationType) -> Migration {
    Migration::new(
        version,
        "fixture".into(),
        kind,
        sqlx::AssertSqlSafe(sql.to_owned()).into_sql_str(),
        false,
    )
}
fn migrator(m: Vec<Migration>) -> Migrator {
    Migrator {
        migrations: Cow::Owned(m),
        ..Migrator::DEFAULT
    }
}
const CREATE: &str = "CREATE TABLE events(id BIGINT NOT NULL,value STRING) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')";
const GATE_OWNER: &str = "00000000-0000-4000-8000-000000000001";
const SUCCESSOR_OWNER: &str = "00000000-0000-4000-8000-000000000002";

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn invalid_lock_timeout_is_rejected_before_coordination_metadata_is_created() {
    let (connection, url, options) = fixture().await;
    connection.close().await.unwrap();
    let options = options.migration_lock_timeout(Duration::MAX);
    let mut connection = DorisConnection::connect_with(&options).await.unwrap();
    assert!(matches!(
        connection.lock().await,
        Err(MigrateError::Execute(sqlx::Error::Configuration(_)))
    ));
    let tables = connection.fetch_all("SHOW TABLES").await.unwrap();
    assert!(
        tables.is_empty(),
        "invalid input must not create lock parents"
    );
    connection.ping().await.unwrap();
    connection.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

async fn seed_owner(admin: &mut DorisConnection, gate: bool, owner: &str) {
    assert!(owner.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-'));
    let (table, format) = if gate {
        ("_sqlx_migrations_gate", "sqlx-doris transition gate v2")
    } else {
        ("_sqlx_migrations_lock", "sqlx-doris migration lock v2")
    };
    admin.execute(sqlx::AssertSqlSafe(format!("CREATE TABLE IF NOT EXISTS {table}(id TINYINT NOT NULL) DUPLICATE KEY(id) COMMENT '{format}' PARTITION BY RANGE(id)(PARTITION sqlx_anchor VALUES [('-1'),('0'))) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"))).await.unwrap();
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {table} ADD PARTITION sqlx_owner_{} VALUES [('0'),('1'))",
            owner.replace('-', "")
        )))
        .await
        .unwrap();
}
async fn clear_owner(admin: &mut DorisConnection, gate: bool) {
    let (table, owner) = if gate {
        (
            "_sqlx_migrations_gate",
            admin.migration_transition_owner().await.unwrap().unwrap(),
        )
    } else {
        (
            "_sqlx_migrations_lock",
            admin.migration_lock_owner().await.unwrap().unwrap(),
        )
    };
    admin
        .execute(sqlx::AssertSqlSafe(format!(
            "ALTER TABLE {table} DROP PARTITION sqlx_owner_{}",
            owner.replace('-', "")
        )))
        .await
        .unwrap();
}
async fn seed_gate(admin: &mut DorisConnection, owner: &str) {
    seed_owner(admin, true, owner).await;
}
async fn clear_gate_owner(admin: &mut DorisConnection) {
    clear_owner(admin, true).await;
}

struct RecoveryFixture {
    admin: DorisConnection,
    url: String,
    options: DorisConnectOptions,
    main_owner: String,
    history: Vec<(i64, String, bool, i64)>,
}
impl RecoveryFixture {
    async fn new() -> Self {
        let (mut c, url, options) = fixture().await;
        let failed = migrator(vec![migration(
            1,
            "SELECT no_such_column FROM information_schema.tables",
            MigrationType::Simple,
        )]);
        assert!(failed.run(&mut c).await.is_err());
        c.close_hard().await.unwrap();
        let mut admin = DorisConnection::connect_with(&options).await.unwrap();
        let main_owner = admin.migration_lock_owner().await.unwrap().unwrap();
        let history = sqlx::query_as(
            "SELECT version,checksum,success,execution_time FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(&mut admin)
        .await
        .unwrap();
        let mut f = Self {
            admin,
            url,
            options,
            main_owner,
            history,
        };
        f.create_lock(true, GATE_OWNER).await;
        f
    }
    async fn create_lock(&mut self, gate: bool, owner: &str) {
        seed_owner(&mut self.admin, gate, owner).await;
    }
    async fn replace_lock(&mut self, gate: bool) {
        clear_owner(&mut self.admin, gate).await;
        self.create_lock(gate, SUCCESSOR_OWNER).await;
    }
    async fn assert_state(&mut self, gate: Option<&str>, main: &str) {
        assert_eq!(
            self.admin
                .migration_transition_owner()
                .await
                .unwrap()
                .as_deref(),
            gate
        );
        assert_eq!(
            self.admin.migration_lock_owner().await.unwrap().as_deref(),
            Some(main)
        );
        let history: Vec<(i64, String, bool, i64)> = sqlx::query_as(
            "SELECT version,checksum,success,execution_time FROM _sqlx_migrations ORDER BY version",
        )
        .fetch_all(&mut self.admin)
        .await
        .unwrap();
        assert_eq!(history, self.history);
        assert_eq!(
            self.admin.dirty_version("_sqlx_migrations").await.unwrap(),
            Some(1)
        );
    }
    async fn finish(self) {
        self.admin.close().await.unwrap();
        Doris::drop_database(&self.url).await.unwrap();
    }
}

async fn assert_retired_without_more_commands(c: &mut DorisConnection, proxy: &Proxy, owner: &str) {
    assert_eq!(proxy.trace.lock().unwrap().parent_drops, 0);
    let count = proxy.trace.lock().unwrap().commands;
    assert!(c.ping().await.is_err());
    assert!(
        c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(owner))
            .await
            .is_err()
    );
    assert_eq!(
        proxy.trace.lock().unwrap().commands,
        count,
        "retired connection sent another command"
    );
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_partition_fences_owner_change_after_the_final_check() {
    let mut f = RecoveryFixture::new().await;
    let owner = f.main_owner.clone();
    let mut proxy = Proxy::start(&f.options, Point::BeforeDrop).await;
    let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
    let mut recovery =
        Box::pin(c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner)));
    tokio::select! {
        () = proxy.reached() => {},
        result = &mut recovery => panic!("no final deletion barrier: {result:?}"),
    }
    // The request already contains the old partition name. Replacing its owner
    // at the exact former check-to-DROP gap must not release the successor.
    f.replace_lock(true).await;
    proxy.release(Release::Forward);
    assert!(recovery.await.is_err());
    assert_retired_without_more_commands(&mut c, &proxy, &owner).await;
    c.close_hard().await.unwrap();
    let trace = proxy.trace.lock().unwrap().clone();
    assert_eq!(
        (trace.drop_requests, trace.drop_ok, trace.parent_drops),
        (1, 0, 0)
    );
    proxy.finish().await;
    f.assert_state(Some(SUCCESSOR_OWNER), &owner).await;
    f.finish().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_partition_fences_a_cancelled_late_delete_after_owner_change() {
    let mut f = RecoveryFixture::new().await;
    let owner = f.main_owner.clone();
    let mut proxy = Proxy::start(&f.options, Point::BeforeDrop).await;
    let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
    let mut recovery =
        Box::pin(c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner)));
    tokio::select! {
        () = proxy.reached() => {},
        result = &mut recovery => panic!("no final deletion barrier: {result:?}"),
    }
    drop(recovery);
    f.replace_lock(true).await;
    proxy.release(Release::Forward);
    proxy.responded().await;
    assert_eq!(proxy.trace.lock().unwrap().drop_ok, 0);
    assert_retired_without_more_commands(&mut c, &proxy, &owner).await;
    c.close_hard().await.unwrap();
    proxy.finish().await;
    f.assert_state(Some(SUCCESSOR_OWNER), &owner).await;
    f.finish().await;
}

#[cfg(feature = "cli-admin")]
#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_partition_fences_a_killed_cli_late_delete_after_owner_change() {
    let mut f = RecoveryFixture::new().await;
    let owner = f.main_owner.clone();
    let mut proxy = Proxy::start(&f.options, Point::BeforeDrop).await;
    let mut child = RecoveryCli::start(&proxy, &owner, "killed-stale-partition-drop");
    proxy.reached().await;
    child.child.kill().unwrap();
    assert!(!child.wait().await.success());
    f.replace_lock(true).await;
    proxy.release(Release::Forward);
    proxy.responded().await;
    let trace = proxy.trace.lock().unwrap().clone();
    assert_eq!(
        (trace.drop_requests, trace.drop_ok, trace.parent_drops),
        (1, 0, 0)
    );
    proxy.finish().await;
    f.assert_state(Some(SUCCESSOR_OWNER), &owner).await;
    f.finish().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_partition_fences_normal_unlock_and_main_recovery_cleanup() {
    for force_recovery in [false, true] {
        let (c, url, opts) = fixture().await;
        c.close().await.unwrap();
        let mut proxy = Proxy::start(&opts, Point::BeforeDrop).await;
        let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
        c.lock().await.unwrap();
        let owner = c.migration_lock_owner().await.unwrap().unwrap();
        let mut release = Box::pin(async {
            if force_recovery {
                c.recover_migration_lock(&owner).await
            } else {
                c.unlock()
                    .await
                    .map_err(|e| sqlx::Error::Protocol(e.to_string()))
            }
        });
        tokio::select! {
            () = proxy.reached() => {},
            result = &mut release => panic!("no cleanup barrier: {result:?}"),
        }
        let mut admin = DorisConnection::connect_with(&opts).await.unwrap();
        clear_gate_owner(&mut admin).await;
        seed_gate(&mut admin, SUCCESSOR_OWNER).await;
        proxy.release(Release::Forward);
        assert!(release.await.is_err());
        assert!(c.ping().await.is_err());
        c.close_hard().await.unwrap();
        assert_eq!(proxy.trace.lock().unwrap().drop_ok, 0);
        assert_eq!(proxy.trace.lock().unwrap().parent_drops, 0);
        proxy.finish().await;
        assert_eq!(
            admin.migration_transition_owner().await.unwrap().as_deref(),
            Some(SUCCESSOR_OWNER)
        );
        assert!(admin.migration_lock_owner().await.unwrap().is_none());
        admin.close().await.unwrap();
        Doris::drop_database(&url).await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_partition_range_allows_exactly_one_concurrent_owner() {
    let (mut a, url, opts) = fixture().await;
    seed_gate(&mut a, GATE_OWNER).await;
    clear_gate_owner(&mut a).await;
    let mut b = DorisConnection::connect_with(&opts).await.unwrap();
    let (one, two) = tokio::join!(
        a.execute("ALTER TABLE _sqlx_migrations_gate ADD PARTITION sqlx_owner_00000000000040008000000000000001 VALUES [('0'),('1'))"),
        b.execute("ALTER TABLE _sqlx_migrations_gate ADD PARTITION sqlx_owner_00000000000040008000000000000002 VALUES [('0'),('1'))")
    );
    assert_ne!(one.is_ok(), two.is_ok(), "{one:?} {two:?}");
    let expected = if one.is_ok() {
        GATE_OWNER
    } else {
        SUCCESSOR_OWNER
    };
    assert_eq!(
        a.migration_transition_owner().await.unwrap().as_deref(),
        Some(expected)
    );
    let rows = a
        .fetch_all("SHOW PARTITIONS FROM _sqlx_migrations_gate")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "anchor plus exactly one owner");
    a.close().await.unwrap();
    b.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_partition_rejects_old_comment_only_gates_without_conversion() {
    let (mut c, url, _) = fixture().await;
    c.execute("CREATE TABLE _sqlx_migrations_gate(id TINYINT NOT NULL) DUPLICATE KEY(id) COMMENT 'sqlx-doris transition owner 00000000-0000-4000-8000-000000000001' DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')").await.unwrap();
    let e = c
        .recover_migration_transition_in_maintenance(GATE_OWNER, None)
        .await
        .unwrap_err();
    assert!(e.to_string().contains("no automatic conversion"), "{e}");
    assert!(c.lock().await.is_err());
    assert!(c.migration_lock_owner().await.unwrap().is_none());
    let comment: String = sqlx::query_scalar("SELECT TABLE_COMMENT FROM information_schema.tables WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='_sqlx_migrations_gate'").fetch_one(&mut c).await.unwrap();
    assert!(comment.ends_with(GATE_OWNER));
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_partition_rejects_wrong_ranges_and_missing_anchor() {
    for missing_anchor in [false, true] {
        let (mut c, url, _) = fixture().await;
        seed_gate(&mut c, GATE_OWNER).await;
        if missing_anchor {
            c.execute("ALTER TABLE _sqlx_migrations_gate DROP PARTITION sqlx_anchor")
                .await
                .unwrap();
        } else {
            clear_gate_owner(&mut c).await;
            c.execute("ALTER TABLE _sqlx_migrations_gate ADD PARTITION sqlx_owner_00000000000040008000000000000001 VALUES [('1'),('2'))").await.unwrap();
        }
        assert!(c.migration_transition_owner().await.is_err());
        assert!(c.lock().await.is_err());
        assert!(c.migration_lock_owner().await.unwrap().is_none());
        let rows = c
            .fetch_all("SHOW PARTITIONS FROM _sqlx_migrations_gate")
            .await
            .unwrap();
        assert_eq!(rows.len(), if missing_anchor { 1 } else { 2 });
        c.close().await.unwrap();
        Doris::drop_database(&url).await.unwrap();
    }
}

// Create both API paths with the same persisted dirty history. The normal
// unlock path acquires its own main owner; recovery targets a crashed owner.
async fn main_actor(
    recover: bool,
    point: Point,
) -> (RecoveryFixture, Proxy, DorisConnection, String) {
    let mut f = RecoveryFixture::new().await;
    clear_gate_owner(&mut f.admin).await;
    if !recover {
        clear_owner(&mut f.admin, false).await;
    }
    let proxy = Proxy::start_main(&f.options, point).await;
    let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
    if !recover {
        c.lock().await.unwrap();
        f.main_owner = c.migration_lock_owner().await.unwrap().unwrap();
    }
    let owner = f.main_owner.clone();
    (f, proxy, c, owner)
}
async fn release_main(
    c: &mut DorisConnection,
    owner: &str,
    recover: bool,
) -> Result<(), sqlx::Error> {
    if recover {
        c.recover_migration_lock(owner).await
    } else {
        c.unlock()
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))
    }
}
async fn assert_main_actor_retired(c: &mut DorisConnection, proxy: &Proxy, owner: &str) {
    let count = proxy.trace.lock().unwrap().commands;
    assert!(c.ping().await.is_err());
    assert!(c.unlock().await.is_err());
    assert!(c.recover_migration_lock(owner).await.is_err());
    let trace = proxy.trace.lock().unwrap();
    assert_eq!(trace.commands, count, "retired actor sent another command");
    assert_eq!(trace.parent_drops, 0);
    assert_eq!(trace.drop_requests, 1);
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn main_partition_fences_normal_release_and_recovery_after_final_check() {
    for recover in [false, true] {
        let (mut f, mut proxy, mut c, owner) = main_actor(recover, Point::BeforeDrop).await;
        let mut release = Box::pin(release_main(&mut c, &owner, recover));
        tokio::select! {
            () = proxy.reached() => {},
            result = &mut release => panic!("no final main deletion barrier: {result:?}"),
        }
        // Replace ownership after every check has finished but before the FE
        // sees the old request. Only the retired UUID may be addressed.
        f.replace_lock(false).await;
        proxy.release(Release::Forward);
        assert!(
            release
                .await
                .unwrap_err()
                .to_string()
                .contains("main lock DROP outcome is uncertain")
        );
        assert_main_actor_retired(&mut c, &proxy, &owner).await;
        c.close_hard().await.unwrap();
        assert_eq!(proxy.trace.lock().unwrap().drop_ok, 0);
        proxy.finish().await;
        let gate = f.admin.migration_transition_owner().await.unwrap().unwrap();
        f.assert_state(Some(&gate), SUCCESSOR_OWNER).await;
        // All old requests have now ended. Explicitly release the gate, then
        // retry the stale command on a fresh connection: never release successor.
        f.admin
            .recover_migration_transition_in_maintenance(&gate, Some(SUCCESSOR_OWNER))
            .await
            .unwrap();
        f.admin.close_hard().await.unwrap();
        f.admin = DorisConnection::connect_with(&f.options).await.unwrap();
        assert!(f.admin.recover_migration_lock(&owner).await.is_err());
        f.assert_state(None, SUCCESSOR_OWNER).await;
        f.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn main_partition_fences_cancelled_release_and_recovery() {
    for recover in [false, true] {
        let (mut f, mut proxy, mut c, owner) = main_actor(recover, Point::BeforeDrop).await;
        let mut release = Box::pin(release_main(&mut c, &owner, recover));
        tokio::select! {
            () = proxy.reached() => {},
            result = &mut release => panic!("no cancellation barrier: {result:?}"),
        }
        drop(release);
        f.replace_lock(false).await;
        proxy.release(Release::Forward);
        proxy.responded().await;
        assert_eq!(proxy.trace.lock().unwrap().drop_ok, 0);
        assert_main_actor_retired(&mut c, &proxy, &owner).await;
        c.close_hard().await.unwrap();
        proxy.finish().await;
        let gate = f.admin.migration_transition_owner().await.unwrap().unwrap();
        f.assert_state(Some(&gate), SUCCESSOR_OWNER).await;
        f.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn main_partition_disconnects_preserve_state_without_replaying() {
    for recover in [false, true] {
        for point in [Point::BeforeDrop, Point::DropReply] {
            let (mut f, mut proxy, mut c, owner) = main_actor(recover, point).await;
            let mut release = Box::pin(release_main(&mut c, &owner, recover));
            tokio::select! {
                () = proxy.reached() => {},
                result = &mut release => panic!("no disconnect barrier: {result:?}"),
            }
            proxy.release(Release::Disconnect);
            assert!(
                release
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("main lock DROP outcome is uncertain")
            );
            assert_main_actor_retired(&mut c, &proxy, &owner).await;
            c.close_hard().await.unwrap();
            proxy.finish().await;
            let gate = f.admin.migration_transition_owner().await.unwrap().unwrap();
            if point == Point::DropReply {
                assert!(f.admin.migration_lock_owner().await.unwrap().is_none());
                // A successor may be installed before a stale operator retries.
                f.create_lock(false, SUCCESSOR_OWNER).await;
            }
            f.assert_state(
                Some(&gate),
                if point == Point::DropReply {
                    SUCCESSOR_OWNER
                } else {
                    &owner
                },
            )
            .await;
            f.finish().await;
        }
    }
}

#[cfg(feature = "cli-admin")]
#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn main_partition_cli_kill_preserves_successor_and_history() {
    let mut f = RecoveryFixture::new().await;
    clear_gate_owner(&mut f.admin).await;
    let owner = f.main_owner.clone();
    let mut proxy = Proxy::start_main(&f.options, Point::BeforeDrop).await;
    let mut child = RecoveryCli::start_main(&proxy, &owner, "killed-stale-main-drop");
    proxy.reached().await;
    child.child.kill().unwrap();
    assert!(!child.wait().await.success());
    f.replace_lock(false).await;
    proxy.release(Release::Forward);
    proxy.responded().await;
    let trace = proxy.trace.lock().unwrap().clone();
    assert_eq!(
        (trace.drop_requests, trace.drop_ok, trace.parent_drops),
        (1, 0, 0)
    );
    proxy.finish().await;
    let gate = f.admin.migration_transition_owner().await.unwrap().unwrap();
    f.assert_state(Some(&gate), SUCCESSOR_OWNER).await;
    f.finish().await;
}

#[cfg(feature = "cli-admin")]
#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn main_partition_cli_disconnects_report_uncertainty_and_do_not_retry() {
    for point in [Point::BeforeDrop, Point::DropReply] {
        let mut f = RecoveryFixture::new().await;
        clear_gate_owner(&mut f.admin).await;
        let owner = f.main_owner.clone();
        let mut proxy = Proxy::start_main(&f.options, point).await;
        let mut child = RecoveryCli::start_main(
            &proxy,
            &owner,
            if point == Point::BeforeDrop {
                "main-before-drop"
            } else {
                "main-lost-reply"
            },
        );
        proxy.reached().await;
        proxy.release(Release::Disconnect);
        assert!(!child.wait().await.success());
        let error = std::fs::read_to_string(&child.stderr).unwrap();
        assert!(
            error.contains("main lock DROP outcome is uncertain") && error.contains("lock status"),
            "{error}"
        );
        let trace = proxy.trace.lock().unwrap().clone();
        assert_eq!((trace.drop_requests, trace.parent_drops), (1, 0));
        proxy.finish().await;
        let gate = f.admin.migration_transition_owner().await.unwrap().unwrap();
        if point == Point::DropReply {
            f.create_lock(false, SUCCESSOR_OWNER).await;
        }
        f.assert_state(
            Some(&gate),
            if point == Point::DropReply {
                SUCCESSOR_OWNER
            } else {
                &owner
            },
        )
        .await;
        f.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn main_partition_rejects_legacy_format_and_invalid_layouts() {
    for mode in ["legacy", "range", "anchor"] {
        let (mut c, url, _) = fixture().await;
        if mode == "legacy" {
            c.execute("CREATE TABLE _sqlx_migrations_lock(id TINYINT NOT NULL) DUPLICATE KEY(id) COMMENT 'sqlx-doris migration owner 00000000-0000-4000-8000-000000000001' DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')").await.unwrap();
        } else {
            seed_owner(&mut c, false, GATE_OWNER).await;
            if mode == "range" {
                clear_owner(&mut c, false).await;
                c.execute("ALTER TABLE _sqlx_migrations_lock ADD PARTITION sqlx_owner_00000000000040008000000000000001 VALUES [('1'),('2'))").await.unwrap();
            } else {
                c.execute("ALTER TABLE _sqlx_migrations_lock DROP PARTITION sqlx_anchor")
                    .await
                    .unwrap();
            }
        }
        assert!(c.migration_lock_owner().await.is_err());
        assert!(c.lock().await.is_err());
        let e = c.recover_migration_lock(GATE_OWNER).await.unwrap_err();
        if mode == "legacy" {
            assert!(e.to_string().contains("no automatic conversion"), "{e}");
        }
        let rows = c
            .fetch_all("SHOW PARTITIONS FROM _sqlx_migrations_lock")
            .await
            .unwrap();
        assert_eq!(rows.len(), if mode == "range" { 2 } else { 1 });
        c.close().await.unwrap();
        Doris::drop_database(&url).await.unwrap();
    }
}

#[cfg(feature = "cli-admin")]
struct RecoveryCli {
    child: std::process::Child,
    stdout: std::path::PathBuf,
    stderr: std::path::PathBuf,
}
#[cfg(feature = "cli-admin")]
impl RecoveryCli {
    fn start(proxy: &Proxy, owner: &str, case: &str) -> Self {
        Self::start_command(
            proxy,
            case,
            &[
                "admin",
                "lock",
                "recover-transition",
                "--owner",
                GATE_OWNER,
                "--lock-owner",
                owner,
                "--maintenance",
                "--force",
            ],
        )
    }
    fn start_main(proxy: &Proxy, owner: &str, case: &str) -> Self {
        Self::start_command(
            proxy,
            case,
            &["admin", "lock", "recover", "--owner", owner, "--force"],
        )
    }
    fn start_command(proxy: &Proxy, case: &str, args: &[&str]) -> Self {
        let artifacts = std::path::PathBuf::from(
            std::env::var("SQLX_DORIS_ARTIFACTS_DIR").expect("run live tests through scripts/dev"),
        );
        let stdout = artifacts.join(format!("recovery-cli-{case}.stdout.log"));
        let stderr = artifacts.join(format!("recovery-cli-{case}.stderr.log"));
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_cargo-sqlx-doris"))
            .args(["--replication-num", "1", "--lock-timeout", "0"])
            .args(args)
            .env("DATABASE_URL", proxy.options.to_url_lossy().as_str())
            .stdout(std::fs::File::create(&stdout).unwrap())
            .stderr(std::fs::File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        Self {
            child,
            stdout,
            stderr,
        }
    }
    async fn wait(&mut self) -> std::process::ExitStatus {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(status) = self.child.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap()
    }
}
#[cfg(feature = "cli-admin")]
impl Drop for RecoveryCli {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(feature = "cli-admin")]
#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_recovery_cli_disconnect_reports_uncertainty_without_retrying() {
    for (point, case) in [
        (Point::GateRead(1), "preflight-disconnect"),
        (Point::BeforeDrop, "before-drop"),
        (Point::DropReply, "lost-drop-reply"),
    ] {
        let mut f = RecoveryFixture::new().await;
        let owner = f.main_owner.clone();
        let mut proxy = Proxy::start(&f.options, point).await;
        let mut child = RecoveryCli::start(&proxy, &owner, case);
        proxy.reached().await;
        proxy.release(Release::Disconnect);
        assert!(!child.wait().await.success());
        let stderr = std::fs::read_to_string(&child.stderr).unwrap();
        assert!(
            stderr.contains(if point == Point::GateRead(1) {
                "no DROP sent"
            } else {
                "uncertain"
            }) && stderr.contains("lock status"),
            "{stderr}"
        );
        if point != Point::GateRead(1) {
            assert!(
                std::fs::read_to_string(&child.stdout)
                    .unwrap()
                    .contains("transition-owner")
            );
        }
        assert_eq!(
            proxy.trace.lock().unwrap().drop_requests,
            usize::from(point != Point::GateRead(1))
        );
        proxy.finish().await;
        f.assert_state(
            if point != Point::DropReply {
                Some(GATE_OWNER)
            } else {
                None
            },
            &owner,
        )
        .await;
        if point == Point::DropReply {
            assert!(
                f.admin
                    .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
                    .await
                    .is_err()
            );
        }
        f.finish().await;
    }
}

#[cfg(feature = "cli-admin")]
#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_recovery_cli_kill_does_not_cancel_an_already_submitted_command() {
    let mut f = RecoveryFixture::new().await;
    let owner = f.main_owner.clone();
    let mut proxy = Proxy::start(&f.options, Point::BeforeDrop).await;
    let mut child = RecoveryCli::start(&proxy, &owner, "killed-pending-drop");
    proxy.reached().await;
    child.child.kill().unwrap();
    assert!(!child.wait().await.success());
    // The proxy already received the command. Even after process termination,
    // that old command can reach Doris and finish; no cleanup may race with it.
    proxy.release(Release::Forward);
    proxy.acknowledged().await;
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
    proxy.finish().await;
    f.assert_state(None, &owner).await;
    f.create_lock(true, SUCCESSOR_OWNER).await;
    assert!(
        f.admin
            .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
            .await
            .is_err()
    );
    f.assert_state(Some(SUCCESSOR_OWNER), &owner).await;
    f.finish().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_recovery_disconnects_before_drop_after_apply_and_during_verification() {
    for point in [
        Point::GateRead(1),
        Point::GateRead(2),
        Point::BeforeDrop,
        Point::DropReply,
        Point::GateRead(4),
    ] {
        let mut f = RecoveryFixture::new().await;
        let owner = f.main_owner.clone();
        let mut proxy = Proxy::start(&f.options, point).await;
        let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
        let mut recovery =
            Box::pin(c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner)));
        tokio::select! {
            () = proxy.reached() => {},
            result = &mut recovery => panic!("recovery ended before {point:?}: {result:?}"),
        }
        proxy.release(Release::Disconnect);
        let error = recovery.await.unwrap_err().to_string();
        let preflight = matches!(point, Point::GateRead(1 | 2));
        assert!(
            error.contains(if preflight {
                "no DROP sent"
            } else if point == Point::GateRead(4) {
                "DROP acknowledged"
            } else {
                "uncertain"
            }) && error.contains("lock status"),
            "{point:?}: {error}"
        );
        assert_retired_without_more_commands(&mut c, &proxy, &owner).await;
        c.close_hard().await.unwrap();
        let trace = proxy.trace.lock().unwrap().clone();
        assert_eq!(trace.drop_requests, usize::from(!preflight));
        assert_eq!(
            trace.drop_forwarded,
            usize::from(!preflight && point != Point::BeforeDrop)
        );
        assert_eq!(
            trace.drop_ok,
            usize::from(!preflight && point != Point::BeforeDrop)
        );
        proxy.finish().await;
        f.assert_state(
            if preflight || point == Point::BeforeDrop {
                Some(GATE_OWNER)
            } else {
                None
            },
            &owner,
        )
        .await;
        if !preflight && point != Point::BeforeDrop {
            assert!(
                f.admin
                    .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
                    .await
                    .is_err()
            );
            f.create_lock(true, SUCCESSOR_OWNER).await;
            assert!(
                f.admin
                    .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
                    .await
                    .is_err()
            );
            f.assert_state(Some(SUCCESSOR_OWNER), &owner).await;
        }
        f.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_recovery_cancellation_never_replays_a_possibly_committed_drop() {
    for point in [
        Point::GateRead(1),
        Point::GateRead(2),
        Point::BeforeDrop,
        Point::DropReply,
        Point::GateRead(4),
    ] {
        let mut f = RecoveryFixture::new().await;
        let owner = f.main_owner.clone();
        let mut proxy = Proxy::start(&f.options, point).await;
        let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
        let mut recovery =
            Box::pin(c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner)));
        tokio::select! {
            () = proxy.reached() => {},
            result = &mut recovery => panic!("recovery ended before {point:?}: {result:?}"),
        }
        drop(recovery);
        let preflight = matches!(point, Point::GateRead(1 | 2));
        // A queued command can still commit after its future is cancelled.
        proxy.release(Release::Forward);
        if !preflight {
            proxy.acknowledged().await;
        }
        assert_retired_without_more_commands(&mut c, &proxy, &owner).await;
        c.close_hard().await.unwrap();
        assert_eq!(
            proxy.trace.lock().unwrap().drop_requests,
            usize::from(!preflight)
        );
        proxy.finish().await;
        f.assert_state(if preflight { Some(GATE_OWNER) } else { None }, &owner)
            .await;
        if !preflight {
            assert!(
                f.admin
                    .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
                    .await
                    .is_err()
            );
        }
        f.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_recovery_rechecks_both_owners_before_sending_drop() {
    for gate in [true, false] {
        let mut f = RecoveryFixture::new().await;
        let owner = f.main_owner.clone();
        let point = if gate {
            Point::GateRead(3)
        } else {
            Point::MainRead(3)
        };
        let mut proxy = Proxy::start(&f.options, point).await;
        let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
        let mut recovery =
            Box::pin(c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner)));
        tokio::select! {
            () = proxy.reached() => {},
            result = &mut recovery => panic!("recovery ended before {point:?}: {result:?}"),
        }
        f.replace_lock(gate).await;
        proxy.release(Release::Forward);
        let error = recovery.await.unwrap_err().to_string();
        assert!(
            error.contains("before DROP") && !error.contains("uncertain"),
            "{error}"
        );
        assert_eq!(proxy.trace.lock().unwrap().drop_requests, 0);
        assert_retired_without_more_commands(&mut c, &proxy, &owner).await;
        c.close_hard().await.unwrap();
        proxy.finish().await;
        f.assert_state(
            Some(if gate { SUCCESSOR_OWNER } else { GATE_OWNER }),
            if gate { &owner } else { SUCCESSOR_OWNER },
        )
        .await;
        f.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_recovery_post_drop_owner_drift_preserves_successor_objects() {
    for gate in [true, false] {
        let mut f = RecoveryFixture::new().await;
        let owner = f.main_owner.clone();
        let point = if gate {
            Point::GateRead(4)
        } else {
            Point::MainRead(4)
        };
        let mut proxy = Proxy::start(&f.options, point).await;
        let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
        let mut recovery =
            Box::pin(c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner)));
        tokio::select! {
            () = proxy.reached() => {},
            result = &mut recovery => panic!("recovery ended before {point:?}: {result:?}"),
        }
        if gate {
            f.create_lock(true, SUCCESSOR_OWNER).await;
        } else {
            f.replace_lock(false).await;
        }
        proxy.release(Release::Forward);
        let error = recovery.await.unwrap_err().to_string();
        assert!(
            error.contains("DROP acknowledged") && error.contains("lock status"),
            "{error}"
        );
        assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
        assert_retired_without_more_commands(&mut c, &proxy, &owner).await;
        c.close_hard().await.unwrap();
        proxy.finish().await;
        f.assert_state(
            if gate { Some(SUCCESSOR_OWNER) } else { None },
            if gate { &owner } else { SUCCESSOR_OWNER },
        )
        .await;
        assert!(
            f.admin
                .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
                .await
                .is_err()
        );
        f.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn transition_recovery_repeated_calls_do_not_reuse_a_retired_connection() {
    let mut f = RecoveryFixture::new().await;
    let owner = f.main_owner.clone();
    let proxy = Proxy::start(&f.options, Point::Observe).await;
    let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
    c.recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
        .await
        .unwrap();
    assert_retired_without_more_commands(&mut c, &proxy, &owner).await;
    c.close_hard().await.unwrap();
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
    proxy.finish().await;
    f.assert_state(None, &owner).await;
    assert!(
        f.admin
            .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
            .await
            .is_err()
    );
    f.create_lock(true, SUCCESSOR_OWNER).await;
    assert!(
        f.admin
            .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
            .await
            .is_err()
    );
    f.assert_state(Some(SUCCESSOR_OWNER), &owner).await;
    f.finish().await;
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn runner_resumption_checks_owner_before_next_sql_or_completion() {
    for point in [Point::BeforeRunner, Point::RunnerReply] {
        for last_step in [false, true] {
            let (mut admin, url, opts) = fixture().await;
            admin.execute(CREATE).await.unwrap();
            let mut proxy = Proxy::start(&opts, point).await;
            let mut c = DorisConnection::connect_with(&proxy.options).await.unwrap();
            let sql = format!(
                "INSERT INTO events VALUES(1,'already committed') /*runner_checkpoint*/;{}",
                if last_step {
                    ""
                } else {
                    "INSERT INTO events VALUES(2,'must not run');"
                }
            );
            let m = migrator(vec![migration(1, &sql, MigrationType::Simple)]);
            let mut run = Box::pin(m.run(&mut c));
            tokio::select! {
                () = proxy.reached() => {},
                result = &mut run => panic!("no committed-statement barrier: {result:?}"),
            }
            let before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
                .fetch_one(&mut admin)
                .await
                .unwrap();
            assert_eq!(before, if point == Point::BeforeRunner { 0 } else { 1 });
            let owner = admin.migration_lock_owner().await.unwrap().unwrap();
            let history: Vec<(i64, String, bool, i64)> = sqlx::query_as(
                "SELECT version,checksum,success,execution_time FROM _sqlx_migrations",
            )
            .fetch_all(&mut admin)
            .await
            .unwrap();
            // Adversarial maintenance violation: the old process is still alive.
            // The first INSERT is either queued after its owner check, or already
            // committed with its acknowledgement withheld. A checkpoint is not a fence.
            admin.recover_migration_lock(&owner).await.unwrap();
            let mut successor = DorisConnection::connect_with(&opts).await.unwrap();
            successor.lock().await.unwrap();
            let next = successor.migration_lock_owner().await.unwrap().unwrap();
            proxy.release(Release::Forward);
            let error = run.await.unwrap_err().to_string();
            assert!(error.contains("migration lock lost"), "{error}");
            assert_retired_without_more_commands(&mut c, &proxy, &owner).await;
            c.close_hard().await.unwrap();
            proxy.finish().await;
            let rows: Vec<(i64, String)> = sqlx::query_as("SELECT id,value FROM events")
                .fetch_all(&mut admin)
                .await
                .unwrap();
            assert_eq!(rows, [(1, "already committed".into())]);
            let after: Vec<(i64, String, bool, i64)> = sqlx::query_as(
                "SELECT version,checksum,success,execution_time FROM _sqlx_migrations",
            )
            .fetch_all(&mut admin)
            .await
            .unwrap();
            assert_eq!(
                after, history,
                "old runner changed completion/dirty history"
            );
            assert_eq!(
                admin.dirty_version("_sqlx_migrations").await.unwrap(),
                Some(1)
            );
            assert_eq!(
                admin.migration_lock_owner().await.unwrap().as_deref(),
                Some(next.as_str())
            );
            successor.unlock().await.unwrap();
            successor.close().await.unwrap();
            admin.close().await.unwrap();
            Doris::drop_database(&url).await.unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn runner_initial_checkpoint_rejects_a_lost_owner_before_dirty_write() {
    let (mut c, url, opts) = fixture().await;
    c.execute(CREATE).await.unwrap();
    c.lock().await.unwrap();
    c.ensure_migrations_table("_sqlx_migrations").await.unwrap();
    let owner = c.migration_lock_owner().await.unwrap().unwrap();
    let mut admin = DorisConnection::connect_with(&opts).await.unwrap();
    admin.recover_migration_lock(&owner).await.unwrap();
    let m = migration(
        1,
        "INSERT INTO events VALUES(1,'must not run')",
        MigrationType::Simple,
    );
    assert!(
        c.apply("_sqlx_migrations", &m)
            .await
            .unwrap_err()
            .to_string()
            .contains("migration lock lost")
    );
    assert!(c.ping().await.is_err());
    c.close_hard().await.unwrap();
    let records: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    let writes: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&mut admin)
        .await
        .unwrap();
    assert_eq!((records, writes), (0, 0));
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

async fn server_connection_id(c: &mut DorisConnection) -> i64 {
    sqlx::query_scalar("SELECT CAST(CONNECTION_ID() AS BIGINT)")
        .fetch_one(c)
        .await
        .unwrap()
}
async fn kill_fixture_connection(admin: &mut DorisConnection, id: i64) {
    assert!(id >= 0);
    // This fixture connects directly to its single FE. Production numeric IDs
    // are FE-local and cannot safely identify a persisted owner after restart.
    admin
        .execute(sqlx::AssertSqlSafe(format!("KILL CONNECTION {id}")))
        .await
        .unwrap();
}
async fn running_fixture_query(admin: &mut DorisConnection, id: i64) -> String {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let rows = admin.fetch_all("SHOW FULL PROCESSLIST").await.unwrap();
            for row in rows {
                if row.get::<String, _>("Id") == id.to_string()
                    && row.get::<String, _>("Info").contains("SLEEP(30)")
                {
                    let query = row.get::<String, _>("QueryId");
                    assert!(
                        !query.is_empty()
                            && query.bytes().all(|v| v.is_ascii_hexdigit() || v == b'-')
                    );
                    return query;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn server_kill_query_does_not_fence_future_writes_after_recovery() {
    let (mut c, url, opts) = fixture().await;
    c.execute(CREATE).await.unwrap();
    c.lock().await.unwrap();
    let owner = c.migration_lock_owner().await.unwrap().unwrap();
    let id = server_connection_id(&mut c).await;
    let mut admin = DorisConnection::connect_with(&opts).await.unwrap();
    let mut query = Box::pin(c.fetch_all("SELECT SLEEP(30)"));
    let query_id = tokio::select! {
        id=running_fixture_query(&mut admin,id)=>id,
        result=&mut query=>panic!("query ended before cancellation: {result:?}"),
    };
    admin
        .execute(sqlx::AssertSqlSafe(format!("KILL QUERY '{query_id}'")))
        .await
        .unwrap();
    assert!(query.await.is_err());
    admin.recover_migration_lock(&owner).await.unwrap();
    // KILL QUERY did not disconnect or revoke write privileges. Generic SQL is
    // deliberately outside the migrator's cooperative ownership checkpoints.
    c.execute("INSERT INTO events VALUES(1,'still writable')")
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events")
            .fetch_one(&mut admin)
            .await
            .unwrap(),
        1
    );
    assert!(admin.migration_lock_owner().await.unwrap().is_none());
    c.close_hard().await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn server_kill_connection_blocks_old_socket_but_allows_reconnection() {
    let (mut c, url, opts) = fixture().await;
    c.execute(CREATE).await.unwrap();
    c.lock().await.unwrap();
    c.execute("INSERT INTO events VALUES(1,'committed before kill')")
        .await
        .unwrap();
    let owner = c.migration_lock_owner().await.unwrap().unwrap();
    let id = server_connection_id(&mut c).await;
    let mut admin = DorisConnection::connect_with(&opts).await.unwrap();
    kill_fixture_connection(&mut admin, id).await;
    assert!(
        c.execute("INSERT INTO events VALUES(2,'dead socket')")
            .await
            .is_err()
    );
    c.close_hard().await.unwrap();
    admin.recover_migration_lock(&owner).await.unwrap();
    let mut reconnected = DorisConnection::connect_with(&opts).await.unwrap();
    reconnected
        .execute("INSERT INTO events VALUES(3,'same credentials')")
        .await
        .unwrap();
    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM events ORDER BY id")
        .fetch_all(&mut admin)
        .await
        .unwrap();
    assert_eq!(ids, [1, 3]);
    reconnected.close().await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn server_kill_connection_does_not_stop_a_submitted_schema_job() {
    let (mut old, url, opts) = fixture().await;
    old.execute("CREATE TABLE heavy(id BIGINT NOT NULL,v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1','light_schema_change'='false')").await.unwrap();
    old.lock().await.unwrap();
    let owner = old.migration_lock_owner().await.unwrap().unwrap();
    let id = server_connection_id(&mut old).await;
    let mut writer = DorisConnection::connect_with(&opts).await.unwrap();
    let mut tx = writer.begin().await.unwrap();
    tx.execute("INSERT INTO heavy VALUES(1,1)").await.unwrap();
    old.execute("ALTER TABLE heavy MODIFY COLUMN v BIGINT")
        .await
        .unwrap();
    let mut admin = DorisConnection::connect_with(&opts).await.unwrap();
    kill_fixture_connection(&mut admin, id).await;
    assert!(old.ping().await.is_err());
    old.close_hard().await.unwrap();
    let jobs = admin.fetch_all("SHOW ALTER TABLE COLUMN").await.unwrap();
    assert!(
        jobs.iter()
            .any(|r| !matches!(r.get::<&str, _>("State"), "FINISHED" | "CANCELLED")),
        "KILL must not be mistaken for schema-job completion"
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
    let columns = admin.fetch_all("DESC heavy").await.unwrap();
    assert!(columns.iter().any(|r| r.get::<&str, _>("Field") == "v"
        && r.get::<&str, _>("Type").eq_ignore_ascii_case("BIGINT")));
    admin.recover_migration_lock(&owner).await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn server_kill_transaction_does_not_undo_prior_commits() {
    let (mut c, url, opts) = fixture().await;
    c.execute(CREATE).await.unwrap();
    c.lock().await.unwrap();
    c.execute("INSERT INTO events VALUES(1,'already committed')")
        .await
        .unwrap();
    let owner = c.migration_lock_owner().await.unwrap().unwrap();
    let id = server_connection_id(&mut c).await;
    let mut tx = c.begin().await.unwrap();
    tx.execute("INSERT INTO events VALUES(2,'uncommitted')")
        .await
        .unwrap();
    let mut admin = DorisConnection::connect_with(&opts).await.unwrap();
    kill_fixture_connection(&mut admin, id).await;
    assert!(tx.commit().await.is_err());
    c.close_hard().await.unwrap();
    let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM events ORDER BY id")
        .fetch_all(&mut admin)
        .await
        .unwrap();
    assert_eq!(ids, [1]);
    assert_eq!(
        admin.migration_lock_owner().await.unwrap().as_deref(),
        Some(owner.as_str())
    );
    admin.recover_migration_lock(&owner).await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn atomic_metadata_lock_excludes_contenders_and_checks_ownership() {
    let (mut first, url, opts) = fixture().await;
    let mut second = DorisConnection::connect_with(&opts).await.unwrap();
    let (a, b) = tokio::join!(first.lock(), second.lock());
    assert_ne!(a.is_ok(), b.is_ok(), "{a:?} {b:?}");
    let (winner, loser) = if a.is_ok() {
        (&mut first, &mut second)
    } else {
        (&mut second, &mut first)
    };
    let owner = winner.migration_lock_owner().await.unwrap().unwrap();
    assert!(loser.recover_migration_lock("wrong-owner").await.is_err());
    assert_eq!(loser.migration_lock_owner().await.unwrap(), Some(owner));
    winner.unlock().await.unwrap();
    let partitions = winner
        .fetch_all("SHOW PARTITIONS FROM _sqlx_migrations_lock")
        .await
        .unwrap();
    assert_eq!(
        partitions.len(),
        1,
        "unlocked parent retains only its anchor"
    );
    assert_eq!(partitions[0].get::<&str, _>("PartitionName"), "sqlx_anchor");
    loser.lock().await.unwrap();
    loser.unlock().await.unwrap();
    first.close().await.unwrap();
    second.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn native_migrator_applies_once_validates_checksums_and_reverts() {
    let (mut c, url, _) = fixture().await;
    let up = migration(
        1,
        &format!("{CREATE}; INSERT INTO events VALUES(1,'a;''b');"),
        MigrationType::ReversibleUp,
    );
    let down = migration(1, "DROP TABLE events", MigrationType::ReversibleDown);
    let m = migrator(vec![up.clone(), down]);
    m.run(&mut c).await.unwrap();
    m.run(&mut c).await.unwrap();
    let rows: Vec<(i64, String)> = sqlx::query_as("SELECT id,value FROM events")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(rows, [(1, "a;'b".into())]);
    let changed = migrator(vec![migration(
        1,
        "SELECT 123",
        MigrationType::ReversibleUp,
    )]);
    assert!(matches!(
        changed.run(&mut c).await,
        Err(MigrateError::VersionMismatch(1))
    ));
    c.unlock().await.unwrap();
    m.undo(&mut c, 0).await.unwrap();
    assert!(
        c.list_applied_migrations("_sqlx_migrations")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(c.migration_lock_owner().await.unwrap().is_none());
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn scripts_cannot_modify_recovery_objects_or_change_database() {
    let (mut c, url, _) = fixture().await;
    for sql in [
        "DROP TABLE `_sqlx_migrations_lock`",
        "DROP TABLE `_sqlx_migrations_gate`",
        "DELETE FROM _sqlx_migrations",
        "CREATE DATABASE migration_escape",
        "SELECT 1; BEGIN",
    ] {
        let m = migrator(vec![migration(1, sql, MigrationType::Simple)]);
        assert!(m.run(&mut c).await.is_err(), "{sql}");
        assert_eq!(c.dirty_version("_sqlx_migrations").await.unwrap(), None);
        c.unlock().await.unwrap();
    }
    assert!(
        c.ensure_migrations_table("_sqlx_migrations_lock")
            .await
            .is_err()
    );
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn pooled_checksum_failure_releases_healthy_lock_on_return() {
    let (c, url, opts) = fixture().await;
    c.close().await.unwrap();
    let pool = sqlx_doris::DorisPoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .unwrap();
    let m = migrator(vec![migration(1, "SELECT 1", MigrationType::Simple)]);
    m.run(&pool).await.unwrap();
    let changed = migrator(vec![migration(1, "SELECT 2", MigrationType::Simple)]);
    assert!(matches!(
        changed.run(&pool).await,
        Err(MigrateError::VersionMismatch(1))
    ));
    let mut c = pool.acquire().await.unwrap();
    assert_eq!(c.migration_lock_owner().await.unwrap(), None);
    c.lock().await.unwrap();
    c.unlock().await.unwrap();
    drop(c);
    pool.close().await;
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn partial_failure_persists_dirty_state_and_never_replays_completed_ddl() {
    let (mut c, url, opts) = fixture().await;
    let m = migrator(vec![migration(
        1,
        &format!("{CREATE}; INSERT INTO events(missing) VALUES(1)"),
        MigrationType::Simple,
    )]);
    assert!(m.run(&mut c).await.is_err());
    c.close_hard().await.unwrap();
    let mut c = DorisConnection::connect_with(&opts).await.unwrap();
    assert_eq!(c.dirty_version("_sqlx_migrations").await.unwrap(), Some(1));
    let owner = c.migration_lock_owner().await.unwrap().unwrap();
    assert!(c.lock().await.is_err());
    c.recover_migration_lock(&owner).await.unwrap();
    assert!(matches!(m.run(&mut c).await, Err(MigrateError::Dirty(1))));
    c.unlock().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(count, 0);
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn schema_change_finishes_before_the_next_step_and_async_session_is_restored() {
    let (c, url, opts) = fixture().await;
    c.close().await.unwrap();
    let mut c = DorisConnection::connect_with(&opts.group_commit(sqlx_doris::GroupCommit::Async))
        .await
        .unwrap();
    let m = migrator(vec![migration(
        1,
        "CREATE TABLE values_t(id BIGINT NOT NULL,v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1','light_schema_change'='false'); INSERT INTO values_t VALUES(1,7); ALTER TABLE values_t MODIFY COLUMN v BIGINT; INSERT INTO values_t VALUES(2,9223372036854775807);",
        MigrationType::Simple,
    )]);
    m.run(&mut c).await.unwrap();
    let mode: String = sqlx::query_scalar("SELECT @@group_commit")
        .fetch_one(&mut c)
        .await
        .unwrap();
    assert_eq!(mode, "async_mode");
    let rows: Vec<i64> = sqlx::query_scalar("SELECT v FROM values_t ORDER BY id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(rows, [7, i64::MAX]);
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn cancellation_retires_connection_and_retains_durable_recovery_markers() {
    let (mut c, url, opts) = fixture().await;
    c.ensure_migrations_table("_sqlx_migrations").await.unwrap();
    let mut observer = DorisConnection::connect_with(&opts).await.unwrap();
    let m = migrator(vec![migration(1, "SELECT SLEEP(5)", MigrationType::Simple)]);
    {
        let running = m.run(&mut c);
        tokio::pin!(running);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    result = &mut running => panic!("migration finished before cancellation: {result:?}"),
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        if observer.dirty_version("_sqlx_migrations").await.unwrap() == Some(1) {
                            break;
                        }
                    }
                }
            }
        }).await.unwrap();
        // Drop the future only after its committed dirty marker is visible.
    }
    observer.close().await.unwrap();
    assert!(c.ping().await.is_err());
    c.close_hard().await.unwrap();
    let mut c = DorisConnection::connect_with(&opts).await.unwrap();
    assert_eq!(c.dirty_version("_sqlx_migrations").await.unwrap(), Some(1));
    assert!(c.migration_lock_owner().await.unwrap().is_some());
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn ping_cannot_silently_leave_a_usable_connection_outside_its_lock() {
    let (mut c, url, opts) = fixture().await;
    c.lock().await.unwrap();
    assert!(c.ping().await.is_err());
    assert!(sqlx::query("SELECT 1").execute(&mut c).await.is_err());
    c.close_hard().await.unwrap();
    let mut successor = DorisConnection::connect_with(&opts).await.unwrap();
    assert!(successor.migration_lock_owner().await.unwrap().is_none());
    successor.lock().await.unwrap();
    successor.unlock().await.unwrap();
    successor.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn gate_arriving_after_main_acquisition_retires_without_dropping_either_owner() {
    let (mut admin, url, options) = fixture().await;
    // Keep valid parent tables, with no owner in either exclusive range.
    admin.lock().await.unwrap();
    admin.unlock().await.unwrap();
    let mut proxy = Proxy::start(&options, Point::GateRead(2)).await;
    let runner_options = proxy.options.clone().migration_lock_timeout(Duration::ZERO);
    let mut runner = DorisConnection::connect_with(&runner_options)
        .await
        .unwrap();
    let mut acquisition = Box::pin(runner.lock());
    tokio::select! {
        () = proxy.reached() => {},
        result = &mut acquisition => panic!("acquisition ended before the post-ADD gate check: {result:?}"),
    }
    let owner = admin.migration_lock_owner().await.unwrap().unwrap();
    seed_gate(&mut admin, GATE_OWNER).await;
    proxy.release(Release::Forward);
    let error = acquisition.await.unwrap_err();
    assert!(
        matches!(
            error,
            MigrateError::Execute(sqlx::Error::Protocol(ref message))
                if message.contains("newly acquired lock retained for recovery")
        ),
        "{error:?}"
    );
    let commands = proxy.trace.lock().unwrap().commands;
    assert!(runner.ping().await.is_err());
    assert_eq!(proxy.trace.lock().unwrap().commands, commands);
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 0);
    runner.close_hard().await.unwrap();
    proxy.finish().await;
    assert_eq!(
        admin.migration_lock_owner().await.unwrap(),
        Some(owner.clone())
    );
    assert_eq!(
        admin.migration_transition_owner().await.unwrap().as_deref(),
        Some(GATE_OWNER)
    );
    // This test owns both simulated actors and has closed the runner. Cleanup
    // uses separate explicit maintenance calls; acquisition never steals a gate.
    admin
        .recover_migration_transition_in_maintenance(GATE_OWNER, Some(&owner))
        .await
        .unwrap();
    admin.close_hard().await.unwrap();
    let mut cleanup = DorisConnection::connect_with(&options).await.unwrap();
    cleanup.recover_migration_lock(&owner).await.unwrap();
    cleanup.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn concurrent_recovery_and_new_owner_never_delete_the_successor_lock() {
    let (mut c, url, opts) = fixture().await;
    let opts = opts.migration_lock_timeout(Duration::from_secs(2));
    for _ in 0..8 {
        c.lock().await.unwrap();
        let old = c.migration_lock_owner().await.unwrap().unwrap();
        c.close_hard().await.unwrap();
        let mut first = DorisConnection::connect_with(&opts).await.unwrap();
        let mut second = DorisConnection::connect_with(&opts).await.unwrap();
        let mut next = DorisConnection::connect_with(&opts).await.unwrap();
        let (a, b, n) = tokio::join!(
            first.recover_migration_lock(&old),
            second.recover_migration_lock(&old),
            next.lock()
        );
        assert_ne!(a.is_ok(), b.is_ok(), "{a:?} {b:?}");
        let mut observer = DorisConnection::connect_with(&opts).await.unwrap();
        let owner = observer.migration_lock_owner().await.unwrap().unwrap();
        assert_ne!(owner, old);
        assert!(
            observer
                .migration_transition_owner()
                .await
                .unwrap()
                .is_none()
        );
        assert!(first.recover_migration_lock(&old).await.is_err());
        assert_eq!(
            observer.migration_lock_owner().await.unwrap(),
            Some(owner.clone())
        );
        first.close().await.unwrap();
        second.close().await.unwrap();
        match n {
            Ok(()) => {
                assert_eq!(next.migration_lock_owner().await.unwrap(), Some(owner));
                next.unlock().await.unwrap();
                observer.close().await.unwrap();
                c = next;
            }
            Err(error) => {
                // A contender may acquire the main partition just before the
                // second recovery acquires the gate. Its finite deadline may
                // then expire. This is a safe refusal to run, not acquisition
                // success: the connection must retire and retain its partition.
                assert!(
                    matches!(
                        error,
                        MigrateError::Execute(sqlx::Error::Protocol(ref message))
                            if message.contains("newly acquired lock retained for recovery")
                    ),
                    "unexpected acquisition failure: {error:?}"
                );
                assert!(next.ping().await.is_err());
                next.close_hard().await.unwrap();
                assert_eq!(
                    observer.migration_lock_owner().await.unwrap(),
                    Some(owner.clone())
                );
                // Explicit cleanup of this test's own lock only after all three
                // actors have finished and their connections have been closed.
                observer.recover_migration_lock(&owner).await.unwrap();
                c = observer;
            }
        }
    }
    c.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn interrupted_transition_blocks_new_locks_and_recovery_without_takeover() {
    let (mut c, url, opts) = fixture().await;
    c.lock().await.unwrap();
    let owner = c.migration_lock_owner().await.unwrap().unwrap();
    c.close_hard().await.unwrap();
    let mut admin = DorisConnection::connect_with(&opts).await.unwrap();
    seed_gate(&mut admin, GATE_OWNER).await;
    let mut contender = DorisConnection::connect_with(&opts).await.unwrap();
    assert!(contender.lock().await.is_err());
    assert!(contender.recover_migration_lock(&owner).await.is_err());
    assert_eq!(
        admin.migration_lock_owner().await.unwrap(),
        Some(owner.clone())
    );
    assert_eq!(
        admin.migration_transition_owner().await.unwrap().as_deref(),
        Some("00000000-0000-4000-8000-000000000001")
    );
    let gate = "00000000-0000-4000-8000-000000000001";
    assert!(
        admin
            .check_migration_transition_recovery(
                "00000000-0000-4000-8000-000000000002",
                Some(&owner)
            )
            .await
            .is_err()
    );
    assert!(
        admin
            .check_migration_transition_recovery(gate, None)
            .await
            .is_err()
    );
    admin
        .check_migration_transition_recovery(gate, Some(&owner))
        .await
        .unwrap();
    assert!(
        admin.migration_transition_owner().await.unwrap().is_some(),
        "preflight must be read-only"
    );
    let mut tx = admin.begin().await.unwrap();
    assert!(
        tx.check_migration_transition_recovery(gate, Some(&owner))
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
    // All transition actors have stopped; one administrator explicitly removes
    // only the gate, preserving the main owner and retiring the connection.
    admin
        .recover_migration_transition_in_maintenance(gate, Some(&owner))
        .await
        .unwrap();
    assert!(admin.ping().await.is_err());
    admin.close().await.unwrap();
    let mut admin = DorisConnection::connect_with(&opts).await.unwrap();
    assert_eq!(
        admin.migration_lock_owner().await.unwrap(),
        Some(owner.clone())
    );
    assert!(admin.migration_transition_owner().await.unwrap().is_none());
    assert!(
        admin
            .recover_migration_transition_in_maintenance(gate, Some(&owner))
            .await
            .is_err()
    );
    admin.recover_migration_lock(&owner).await.unwrap();
    contender.close().await.unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn maintenance_recovery_handles_a_gate_left_after_main_lock_removal() {
    let (mut admin, url, opts) = fixture().await;
    let gate = "00000000-0000-4000-8000-000000000001";
    seed_gate(&mut admin, GATE_OWNER).await;
    assert!(
        admin
            .check_migration_transition_recovery(gate, Some(gate))
            .await
            .is_err()
    );
    admin
        .check_migration_transition_recovery(gate, None)
        .await
        .unwrap();
    admin
        .recover_migration_transition_in_maintenance(gate, None)
        .await
        .unwrap();
    assert!(admin.ping().await.is_err());
    admin.close().await.unwrap();
    let mut next = DorisConnection::connect_with(&opts).await.unwrap();
    next.lock().await.unwrap();
    next.unlock().await.unwrap();
    next.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn maintenance_recovery_rejects_inflight_schema_jobs_until_reconciled() {
    let (mut admin, url, opts) = fixture().await;
    admin.execute("CREATE TABLE heavy(id BIGINT NOT NULL,v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1','light_schema_change'='false')").await.unwrap();
    let mut writer = DorisConnection::connect_with(&opts).await.unwrap();
    let mut tx = writer.begin().await.unwrap();
    sqlx::query("INSERT INTO heavy VALUES(1,1)")
        .execute(&mut *tx)
        .await
        .unwrap();
    admin
        .execute("ALTER TABLE heavy MODIFY COLUMN v BIGINT")
        .await
        .unwrap();
    seed_gate(&mut admin, GATE_OWNER).await;
    let gate = "00000000-0000-4000-8000-000000000001";
    let blocked = admin
        .recover_migration_transition_in_maintenance(gate, None)
        .await
        .unwrap_err();
    assert!(
        blocked
            .to_string()
            .contains("recovery blocked by COLUMN job"),
        "{blocked}"
    );
    assert_eq!(
        admin.migration_transition_owner().await.unwrap().as_deref(),
        Some(gate)
    );
    admin.ping().await.unwrap(); // Read-only preflight did not begin removal.
    tx.rollback().await.unwrap();
    writer.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let jobs = sqlx::query("SHOW ALTER TABLE COLUMN")
                .fetch_all(&mut admin)
                .await
                .unwrap();
            if jobs.iter().all(|row| {
                matches!(
                    row.try_get::<&str, _>("State").unwrap(),
                    "FINISHED" | "CANCELLED"
                )
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    admin
        .recover_migration_transition_in_maintenance(gate, None)
        .await
        .unwrap();
    admin.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}

#[tokio::test]
#[ignore = "requires dedicated Doris admin fixture"]
async fn async_schema_change_timeout_preserves_dirty_state_and_lock() {
    let (c, url, opts) = fixture().await;
    c.close().await.unwrap();
    let mut c =
        DorisConnection::connect_with(&opts.clone().migration_timeout(Duration::from_millis(1)))
            .await
            .unwrap();
    let m = migrator(vec![migration(
        1,
        "CREATE TABLE heavy(id BIGINT NOT NULL,v INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1','light_schema_change'='false'); INSERT INTO heavy SELECT number,CAST(number AS INT) FROM numbers('number'='10000'); ALTER TABLE heavy MODIFY COLUMN v BIGINT",
        MigrationType::Simple,
    )]);
    let err = m.run(&mut c).await.unwrap_err();
    assert!(err.to_string().contains("schema-change timeout"), "{err}");
    assert!(c.ping().await.is_err());
    c.close_hard().await.unwrap();
    let mut observer = DorisConnection::connect_with(&opts).await.unwrap();
    assert_eq!(
        observer.dirty_version("_sqlx_migrations").await.unwrap(),
        Some(1)
    );
    assert!(observer.migration_lock_owner().await.unwrap().is_some());
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let jobs = sqlx::query("SHOW ALTER TABLE COLUMN")
                .fetch_all(&mut observer)
                .await
                .unwrap();
            if jobs.iter().all(|row| {
                matches!(
                    row.try_get::<&str, _>("State").unwrap(),
                    "FINISHED" | "CANCELLED"
                )
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let owner = observer.migration_lock_owner().await.unwrap().unwrap();
    observer.recover_migration_lock(&owner).await.unwrap();
    assert_eq!(
        observer.dirty_version("_sqlx_migrations").await.unwrap(),
        Some(1)
    );
    observer.close().await.unwrap();
    Doris::drop_database(&url).await.unwrap();
}
