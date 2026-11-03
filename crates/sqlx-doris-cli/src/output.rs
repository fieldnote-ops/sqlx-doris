use serde_json::{Value, json};
pub(crate) use sqlx_doris_ops::failure::{Failure, fault, status};
use sqlx_doris_ops::observation::Observation;
pub(crate) use sqlx_doris_ops::observation::Phase;
fn classify(phase: Phase, requested: u8) -> u8 {
    if requested == 0 {
        0
    } else {
        match phase {
            Phase::Submitted => 5,
            Phase::Acknowledged | Phase::Verified => 6,
            _ => requested,
        }
    }
}

pub(crate) fn finish(
    observation: &Observation,
    json_output: bool,
    operation: &str,
    requested: Option<u8>,
) -> u8 {
    let s = observation.snapshot();
    let code = classify(s.phase, requested.unwrap_or(7));
    let mut value = s.details.clone();
    if value.get("message").is_none() {
        value["message"] = json!(match code {
            0 => "Command completed within its documented scope",
            2 => "Invalid command input; consult command help",
            3 => "Observed prerequisite conflict; inspect and reconcile",
            4 => "Observation incomplete or ambiguous",
            5 => "Mutation may have committed; inspect on a new connection; never replay blindly",
            6 => "Mutation acknowledged; later completion verification failed",
            _ => "Connection, permission, metadata or local operation unavailable",
        });
    }
    if value.get("issues").is_none() {
        value["issues"] = if code == 0 {
            json!([])
        } else {
            json!([{ "code":status(code) }])
        };
    }
    let failure_category = if code == 0 {
        Value::Null
    } else {
        json!(status(requested.unwrap_or(7)))
    };
    let next_actions = match code {
        0 => vec!["review_scope_and_reconcile_before_recovery"],
        2 => vec!["correct_inputs"],
        3 | 4 => vec!["keep_isolation", "inspect_and_reconcile"],
        5 | 6 => vec![
            "preserve_receipts_locks_and_history",
            "inspect_on_new_connection",
            "do_not_replay_mutations",
        ],
        _ => vec!["restore_read_access", "inspect_on_new_connection"],
    };
    let root = json!({
        "schema": "sqlx-doris-cli-result-v1",
        "operation": operation,
        "exit_code": code,
        "status": status(code),
        "phase": s.phase.label(),
        "step": s.step,
        "failure_category": failure_category,
        "server_read_only": matches!(s.phase, Phase::Read | Phase::Local),
        "atomic_snapshot": false,
        "safe_to_recover": false,
        "kafka_continuation_verified": false,
        "automatic_retry": false,
        "automatic_lock_takeover": false,
        "automatic_offset_transfer": false,
        "automatic_offset_reset": false,
        "next_actions": next_actions
    });
    for (k, v) in root.as_object().unwrap() {
        value[k] = v.clone();
    }
    if json_output {
        println!("{value}");
    } else {
        eprintln!(
            "status {}; phase {}; step {}; exit-code {code}",
            status(code),
            s.phase.label(),
            s.step
        );
    }
    code
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn submitted_and_acknowledged_work_override_later_failure_categories() {
        for code in [2, 3, 4, 7] {
            assert_eq!(classify(Phase::Submitted, code), 5);
            assert_eq!(classify(Phase::Acknowledged, code), 6);
            assert_eq!(classify(Phase::Read, code), code);
        }
        assert_eq!(classify(Phase::Verified, 7), 6);
        assert_eq!(classify(Phase::Verified, 0), 0);
    }
}
