//! Stable routine result schema and exit classification; no server error echo.
use super::{Job, Reservation};
use crate::observation::{Observation, Phase as GenericPhase};
use serde_json::{Value, json};

use crate::failure::Failure;
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Phase {
    Read,
    ReceiptWriting,
    Receipt,
    ReserveSent,
    ReserveAck,
    CreateSent,
    CreateAck,
    CreateVerified,
    StopSent,
    StopAck,
    StopVerified,
}
impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::Read => "read_only",
            Self::ReceiptWriting => "receipt_writing",
            Self::Receipt => "receipt_persisted",
            Self::ReserveSent => "reservation_submitted",
            Self::ReserveAck => "reservation_acknowledged",
            Self::CreateSent => "create_submitted",
            Self::CreateAck => "create_acknowledged",
            Self::CreateVerified => "create_verified",
            Self::StopSent => "stop_submitted",
            Self::StopAck => "stop_acknowledged",
            Self::StopVerified => "stop_verified",
        }
    }
    pub(super) fn failure_code(self) -> Option<u8> {
        match self {
            Self::ReserveSent | Self::CreateSent | Self::StopSent => Some(5),
            Self::ReserveAck | Self::CreateAck | Self::StopAck => Some(6),
            _ => None,
        }
    }
}
pub(super) struct Report {
    evidence: Observation,
    pub operation: &'static str,
    pub observation: &'static str,
    pub outcome: &'static str,
    pub phase: Phase,
    pub job_observed_phase: Option<Phase>,
    pub database: Option<String>,
    pub cluster: Option<String>,
    pub database_id: Option<String>,
    pub name: Option<String>,
    pub receipt: Option<String>,
    pub reservation: Option<Reservation>,
    pub job: Option<Job>,
    pub confirmed: Option<Job>,
    pub code: u8,
    pub message: String,
}
fn job(value: &Option<Job>) -> Value {
    value.as_ref().map_or(Value::Null, |j| {
        json!({
            "id": j.id,
            "created": j.created,
            "state": j.state,
            "progress": j.progress,
            "progress_semantics": "doris_reported_cursor",
            "progress_scope": "job_reported_partitions_only",
            "progress_proves_consumption": false,
            "kafka_continuation_verified": false
        })
    })
}
impl Report {
    pub fn new(operation: &'static str, observation: Observation) -> Self {
        Self {
            evidence: observation,
            operation,
            observation: "unknown",
            outcome: "not_sent",
            phase: Phase::Read,
            job_observed_phase: None,
            database: None,
            cluster: None,
            database_id: None,
            name: None,
            receipt: None,
            reservation: None,
            job: None,
            confirmed: None,
            code: 0,
            message: String::new(),
        }
    }
    pub fn capture(&mut self, error: &(dyn std::error::Error + Send + Sync + 'static)) {
        self.code = self.phase.failure_code().unwrap_or_else(|| {
            error
                .downcast_ref::<Failure>()
                .map(|f| f.code())
                .unwrap_or(7)
        });
        if self.phase.failure_code().is_some() {
            self.observation = "unconfirmed";
        } else if self.code == 3 && self.observation == "unknown" {
            self.observation = "invalid_binding";
        }
        self.outcome = match self.code {
            2 => "invalid_input",
            3 => "refused",
            4 => "inconclusive",
            5 => "uncertain",
            6 => "acknowledged_unverified",
            _ => "unavailable",
        };
        self.message = if self.code == 5 {
            match self.phase {
                Phase::StopSent => "Routine STOP outcome uncertain; inspect original UUID/JobId on a new connection; never retry by name",
                Phase::CreateSent => "Routine CREATE outcome unconfirmed; inspect original receipt/name on a new connection; never replay CREATE or reuse name",
                _ => "Reservation CREATE unconfirmed; no job CREATE sent; preserve receipt and never reuse name",
            }
            .into()
        } else if self.code == 6 {
            match self.phase {
                Phase::StopAck => "Routine STOP acknowledged; terminal verification failed; use a new connection, do not replay",
                Phase::CreateAck => "Routine CREATE acknowledged; verification or confirmation receipt failed; inspect on a new connection, do not replay",
                _ => "Reservation acknowledged; follow-up failed; no job CREATE sent; preserve receipt and reservation",
            }
            .into()
        } else {
            error.downcast_ref::<Failure>().map_or_else(
                || "Routine metadata/connection read failed or deadline expired; result unconfirmed; use a new connection".into(),
                |failure| failure.message.to_string(),
            )
        };
    }
    pub fn value(&self) -> Value {
        json!({
            "operation": self.operation,
            "exit_code": self.code,
            "observation": self.observation,
            "outcome": self.outcome,
            "step": self.phase.label(),
            "database": self.database,
            "database_id": self.database_id,
            "cluster": self.cluster,
            "job_name": self.name,
            "receipt": self.receipt,
            "reservation": self.reservation.as_ref().map(|r|json!({
                "name": format!("_{}",r.name),
                "creator": r.creator,
                "table": r.table,
                "permanent": true
            })),
            "job": job(&self.job),
            "job_observed_phase": self.job_observed_phase.map(Phase::label),
            "last_confirmed_job": job(&self.confirmed),
            "message": self.message,
            "safe_to_recover": false,
            "automatic_retry": false,
            "next_actions": if self.code == 0 {
                vec!["keep_namespace_isolated", "drain_and_reconcile_before_lock_recovery"]
            } else {
                vec![
                    "inspect_original_receipt_on_new_connection",
                    "preserve_reservations_and_history",
                    "do_not_replay_mutations"
                ]
            }
        })
    }
    pub fn set_phase(&mut self, phase: Phase) {
        self.phase = phase;
        let generic = match phase {
            Phase::Read => GenericPhase::Read,
            Phase::ReceiptWriting | Phase::Receipt => GenericPhase::Local,
            Phase::ReserveSent | Phase::CreateSent | Phase::StopSent => GenericPhase::Submitted,
            Phase::ReserveAck | Phase::CreateAck | Phase::StopAck => GenericPhase::Acknowledged,
            _ => GenericPhase::Verified,
        };
        self.evidence.phase(generic, phase.label());
        self.record();
    }

    pub fn record(&self) {
        self.evidence.details(self.value());
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::failure::fault;

    #[test]
    fn a_dropped_report_retains_target_and_last_submission_without_relabeling_the_job_snapshot() {
        let evidence = Observation::default();
        let mut report = Report::new("stop", evidence.clone());
        report.name = Some("sqlx_rl_00000000000040008000000000000001".into());
        report.job_observed_phase = Some(Phase::Read);
        report.set_phase(Phase::StopSent);
        drop(report);
        let snapshot = evidence.snapshot();
        assert_eq!(snapshot.phase, GenericPhase::Submitted);
        assert_eq!(
            snapshot.details["job_name"],
            "sqlx_rl_00000000000040008000000000000001"
        );
        assert_eq!(snapshot.details["job_observed_phase"], "read_only");
    }
    #[test]
    fn mutation_phase_overrides_error_classification_and_retains_snapshot_phase() {
        for (phase, code) in [
            (Phase::ReserveSent, 5),
            (Phase::CreateSent, 5),
            (Phase::StopSent, 5),
            (Phase::ReserveAck, 6),
            (Phase::CreateAck, 6),
            (Phase::StopAck, 6),
        ] {
            let mut r = Report::new("stop", Observation::default());
            r.phase = phase;
            r.job_observed_phase = Some(Phase::Read);
            r.capture(fault(2, "PRIVATE_SERVER_SQL_PASSWORD").as_ref());
            assert_eq!(r.code, code);
            assert_eq!(r.observation, "unconfirmed");
            assert_eq!(r.value()["job_observed_phase"], "read_only");
            assert!(
                !r.value()
                    .to_string()
                    .contains("PRIVATE_SERVER_SQL_PASSWORD")
            );
            assert_eq!(r.value()["safe_to_recover"], false);
        }
    }
    #[test]
    fn a_numeric_initial_cursor_never_certifies_consumption_or_kafka_continuation() {
        let mut r = Report::new("create", Observation::default());
        r.job = Some(Job {
            id: 1,
            created: "2026-11-01 00:00:00".into(),
            state: "NEED_SCHEDULE".into(),
            progress: json!({
                "0": "8",
                "1": "OFFSET_ZERO"
            }),
        });
        let value = r.value();
        assert_eq!(value["job"]["progress_semantics"], "doris_reported_cursor");
        assert_eq!(value["job"]["progress_proves_consumption"], false);
        assert_eq!(value["job"]["kafka_continuation_verified"], false);
        assert_eq!(value["safe_to_recover"], false);
    }
    #[test]
    fn read_errors_preserve_stable_categories_and_hide_untrusted_details() {
        for phase in [Phase::Read, Phase::ReceiptWriting] {
            for code in [2, 3, 4, 7] {
                let evidence = Observation::default();
                let mut r = Report::new("status", evidence.clone());
                r.set_phase(phase);
                r.capture(fault(code, "controlled diagnostic").as_ref());
                assert_eq!(r.code, code);
                assert!(matches!(
                    evidence.snapshot().phase,
                    GenericPhase::Read | GenericPhase::Local
                ));
            }
        }
        let mut r = Report::new("status", Observation::default());
        r.capture(&std::io::Error::other("PRIVATE_SERVER_SQL_PASSWORD"));
        assert_eq!(r.code, 7);
        assert!(
            !r.value()
                .to_string()
                .contains("PRIVATE_SERVER_SQL_PASSWORD")
        );
    }
}
