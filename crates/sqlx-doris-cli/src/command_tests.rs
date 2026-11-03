use super::*;
use clap::Parser;

#[test]
fn drain_mutations_and_recovery_guard_require_explicit_maintenance() {
    use clap::Parser;
    for args in [
        vec![
            "cli",
            "admin",
            "drain",
            "cancel-load",
            "--name",
            "sqlx_mig_00000000000040008000000000000001",
            "--job-id",
            "1",
            "--transaction-id",
            "2",
            "--http-url",
            "http://localhost:8030",
            "--force",
        ],
        vec![
            "cli",
            "admin",
            "drain",
            "wait",
            "--name",
            "sqlx_mig_00000000000040008000000000000001",
        ],
        vec![
            "cli",
            "admin",
            "drain",
            "cancel-rollup",
            "--name",
            "sqlx_mig_00000000000040008000000000000001",
            "--table",
            "events",
            "--job-id",
            "1",
            "--force",
        ],
        vec![
            "cli",
            "admin",
            "lock",
            "recover",
            "--owner",
            "00000000-0000-4000-8000-000000000001",
            "--retired-identity",
            "sqlx_mig_00000000000040008000000000000001",
            "--force",
        ],
        vec![
            "cli",
            "admin",
            "drain",
            "cancel-mv-task",
            "--name",
            "sqlx_mig_00000000000040008000000000000001",
            "--view",
            "mv",
            "--job-id",
            "1",
            "--task-id",
            "2",
            "--force",
        ],
    ] {
        assert!(Args::try_parse_from(&args).is_err());
        let mut confirmed = args;
        confirmed.push("--maintenance");
        assert!(Args::try_parse_from(confirmed).is_ok());
    }
}

#[test]
fn namespace_and_maintenance_are_explicit_for_stop_execution() {
    use clap::Parser;
    let args = [
        "cli",
        "admin",
        "routine",
        "stop",
        "--job-name",
        "sqlx_rl_00000000000040008000000000000001",
        "--job-id",
        "1",
        "--force",
    ];
    assert!(Args::try_parse_from(args).is_err());
    let mut args = args.to_vec();
    args.push("--maintenance");
    assert!(Args::try_parse_from(args.clone()).is_err());
    args.push("--managed-namespace");
    assert!(Args::try_parse_from(args).is_ok());
}

#[cfg(feature = "kafka-admin")]
#[test]
fn json_usage_namespace_and_explicit_offsets_match_the_command() {
    for args in [
        vec![
            "app",
            "admin",
            "kafka",
            "preflight",
            "--connection-file",
            "x",
            "--topic",
            "events",
            "--offset",
            "0=5",
            "--json",
        ],
        vec![
            "app",
            "--database-url",
            "kafka",
            "admin",
            "routine",
            "status",
            "--job-name",
            "x",
            "--json",
        ],
    ] {
        assert!(Args::try_parse_from(args).is_ok());
    }
    assert!(
        Args::try_parse_from([
            "app",
            "admin",
            "kafka",
            "preflight",
            "--connection-file",
            "x",
            "--topic",
            "events",
            "--offset",
            "0=OFFSET_ZERO"
        ])
        .is_err()
    );
}

#[test]
fn transition_recovery_requires_explicit_maintenance_for_mutation() {
    let command = [
        "cargo-sqlx-doris",
        "admin",
        "lock",
        "recover-transition",
        "--owner",
        "00000000-0000-4000-8000-000000000001",
        "--lock-owner",
        "unlocked",
    ];
    let args = Args::try_parse_from(command).unwrap();
    assert!(matches!(
        args.command,
        Action::Admin {
            command: AdminAction::Lock {
                command: LockAction::RecoverTransition { force: false, .. }
            }
        }
    ));
    assert!(Args::try_parse_from(command.into_iter().chain(["--force"])).is_err());
    assert!(Args::try_parse_from(command.into_iter().chain(["--maintenance", "--force"])).is_ok());
    assert!(
        Args::try_parse_from([
            "cli",
            "admin",
            "lock",
            "recover-transition",
            "--owner",
            "invalid",
            "--lock-owner",
            "unlocked"
        ])
        .is_err()
    );
}
