//! Explicit owner-partition recovery and read-only lock inspection.
use super::{Context, args::LockAction, output};
use sqlx_core::connection::Connection;
use sqlx_doris_core::DorisConnection;
use sqlx_doris_ops::{self as ops, failure::Result};

pub(super) async fn run(args: &Context, command: &LockAction) -> Result<()> {
    match command {
        LockAction::Recover { owner, .. } => args.observation.data(
            "target",
            serde_json::json!({
                "kind": "main_lock",
                "requested_owner": owner,
                "preview": false
            }),
        ),
        LockAction::RecoverTransition {
            owner,
            lock_owner,
            force,
            ..
        } => args.observation.data(
            "target",
            serde_json::json!({
                "kind": "transition_lock",
                "requested_owner": owner,
                "expected_main_owner": lock_owner,
                "preview": !force
            }),
        ),
        LockAction::Status => {}
    }
    if !matches!(command, LockAction::Status) {
        args.observation.data(
            "recovery_drop",
            serde_json::json!({
                "phase": "not_submitted",
                "coordination_metadata_may_have_changed": false
            }),
        );
    }
    let mut c = args.connect().await?;
    match command {
        LockAction::Status => {
            let main_owner = c.migration_lock_owner().await?;
            let transition_owner = c.migration_transition_owner().await?;
            args.observation.data(
                "locks",
                serde_json::json!({
                    "main_owner": main_owner,
                    "transition_owner": transition_owner
                }),
            );
            args.line(format_args!(
                "{}",
                main_owner.as_deref().unwrap_or("unlocked")
            ));
            if let Some(owner) = transition_owner {
                args.line(format_args!(
                    "transition-owner {owner}; recovery/release is in progress or interrupted"
                ));
                args.line(format_args!(
                            "Preview with lock recover-transition --owner {owner} --lock-owner <main-UUID-or-unlocked>; removal requires --maintenance --force"
                        ));
            }
        }
        LockAction::Recover {
            owner,
            retired_identity,
            ..
        } => {
            ops::maintenance::drain::guard(
                &mut c,
                retired_identity.as_deref(),
                args.ddl_timeout,
                &args.observation,
            )
            .await?;
            args.observation
                .phase(output::Phase::Submitted, "main_recovery_coordination");
            let result = c.recover_migration_lock(owner).await;
            recovery_phase(args, &c, false);
            result?;
            args.line(format_args!(
                        "Recovered lock owned by {owner}; owner partition removed, lock tables and migration records preserved. This does not stop the previous runner or block reconnects; exclusive maintenance remains required."
                    ));
        }
        LockAction::RecoverTransition {
            owner,
            lock_owner,
            force,
            retired_identity,
            ..
        } => {
            let expected_lock = (lock_owner != "unlocked").then_some(lock_owner.as_str());
            let database = c
                        .check_migration_transition_recovery(owner, expected_lock)
                        .await
                        .map_err(|e| format!("transition recovery stopped before DROP; no DROP sent: {e}; inspect lock status before retrying"))?;
            ops::maintenance::drain::guard(
                &mut c,
                retired_identity.as_deref(),
                args.ddl_timeout,
                &args.observation,
            )
            .await?;
            args.line(format_args!(
                        "database {database}\ntransition-owner {owner}\nlock-owner {lock_owner}\nNo nonterminal COLUMN/ROLLUP jobs found; this does not prove old commands have stopped."
                    ));
            if *force {
                let result = c
                    .recover_migration_transition_in_maintenance(owner, expected_lock)
                    .await;
                recovery_phase(args, &c, true);
                result?;
                args.line(format_args!(
                            "Removed transition owner partition {owner}; gate table, main lock and migration records preserved. Run lock status before recovering the main lock or resuming migrations."
                        ));
            } else {
                args.line(format_args!(
                            "Preview only; no changes made. Stop all migration/unlock/recovery actors, ensure no old commands can commit, and allow one administrator across machines. Then repeat with --maintenance --force to remove only this owner's partition; the gate table remains."
                        ));
            }
        }
    }
    c.close().await?;
    Ok(())
}

fn recovery_phase(args: &Context, c: &DorisConnection, transition: bool) {
    use sqlx_doris_core::MigrationRecoveryPhase as R;
    let (phase, label) = match c.migration_recovery_phase() {
        R::NotSubmitted => (
            if transition {
                output::Phase::Read
            } else {
                output::Phase::Submitted
            },
            "not_submitted",
        ),
        R::Submitted => (output::Phase::Submitted, "submitted"),
        R::Acknowledged => (output::Phase::Acknowledged, "acknowledged"),
        R::Verified => (output::Phase::Verified, "verified"),
    };
    args.observation.data(
        "recovery_drop",
        serde_json::json!({
            "phase": label,
            "coordination_metadata_may_have_changed": !transition
        }),
    );
    args.observation
        .phase(phase, "recovery_owner_partition_drop");
}
