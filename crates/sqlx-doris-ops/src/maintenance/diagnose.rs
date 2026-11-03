//! Read-only composition. Observations never authorize a mutation or offset transfer.
use super::{Context, Result, drain, identity};
use crate::failure;
#[cfg(feature = "kafka")]
use crate::kafka;
use crate::migration_history as history;
use serde_json::{Value, json};
use sqlx_core::connection::ConnectOptions;
use sqlx_core::connection::Connection;
use std::{path::PathBuf, time::Duration};

#[derive(Clone)]
pub struct Options {
    /// Original managed Routine receipt; selects and verifies its database.
    pub receipt: Option<PathBuf>,
    /// Managed identity to attest absent, including its surviving sessions.
    pub retired_identity: Option<String>,
    /// Optional migration source for checksum/missing-source reconciliation.
    pub source: Option<PathBuf>,

    pub table: String,
    /// Independent Kafka credentials; requires explicit topic, checkpoint and candidates.
    #[cfg(feature = "kafka")]
    pub connection_file: Option<PathBuf>,

    #[cfg(feature = "kafka")]
    pub topic: Option<String>,

    #[cfg(feature = "kafka")]
    pub checkpoint: Option<PathBuf>,

    #[cfg(feature = "kafka")]
    pub offsets: Vec<kafka::Candidate>,
    /// Network-observation deadline; local input validation runs first.
    pub timeout: u64,
}

impl Options {
    pub fn new(
        receipt: Option<PathBuf>,
        retired_identity: Option<String>,
        source: Option<PathBuf>,
        table: String,
        timeout: u64,
    ) -> Self {
        Self {
            receipt,
            retired_identity,
            source,
            table,
            timeout,
            #[cfg(feature = "kafka")]
            connection_file: None,
            #[cfg(feature = "kafka")]
            topic: None,
            #[cfg(feature = "kafka")]
            checkpoint: None,
            #[cfg(feature = "kafka")]
            offsets: Vec::new(),
        }
    }

    #[cfg(feature = "kafka")]
    pub fn with_kafka(
        mut self,
        connection_file: Option<PathBuf>,
        topic: Option<String>,
        checkpoint: Option<PathBuf>,
        offsets: Vec<kafka::Candidate>,
    ) -> Self {
        self.connection_file = connection_file;
        self.topic = topic;
        self.checkpoint = checkpoint;
        self.offsets = offsets;
        self
    }
}

#[path = "diagnose/report.rs"]
mod report;
fn combine(codes: &[u8]) -> u8 {
    [2, 7, 3, 4]
        .into_iter()
        .find(|c| codes.contains(c))
        .unwrap_or(0)
}
fn failure(
    e: &(dyn std::error::Error + Send + Sync + 'static),
    fallback: &'static str,
) -> (u8, &'static str) {
    e.downcast_ref::<failure::Failure>()
        .map_or((7, fallback), |e| (e.code(), e.reason.unwrap_or(fallback)))
}

async fn observe_database(
    args: &Context,
    o: &Options,
    source: Option<&sqlx_core::migrate::Migrator>,
    r: &mut report::Report,
    c: &mut sqlx_doris_core::DorisConnection,
    stage: &mut &'static str,
) -> Result<()> {
    let before = (
        c.migration_lock_owner().await?,
        c.migration_transition_owner().await?,
    );
    r.component(
        "locks",
        0,
        json!({
            "main_owner": before.0,
            "transition_owner": before.1,
            "held_locks_are_not_automatically_recovered": true
        }),
    );
    if before.0.is_some() || before.1.is_some() {
        r.issue(
            "locks",
            "lock_held",
            0,
            "Lock ownership observed; no takeover attempted",
            json!({
                "main_owner": before.0,
                "transition_owner": before.1
            }),
            "keep_external_isolation_and_inspect_owners",
        );
    }
    *stage = "migration_history_unavailable";
    let baseline = history::observe(c, &o.table).await?;
    report::migrations(r, &baseline, source)?;
    match drain::diagnostic(c, o.retired_identity.as_deref(), &args.observation).await {
        Ok(empty) => {
            r.component(
                "drain",
                if empty { 0 } else { 3 },
                json!({
                    "scoped_empty": empty,
                    "identity_sessions_attested": o.retired_identity.is_some(),
                    "atomic_fence": false
                }),
            );
            if !empty {
                let listed_active_items = args
                    .observation
                    .observed_work()
                    .as_object()
                    .map(|work| work.values().filter(|item| item["blocked"] == true).count());
                r.issue(
                    "drain",
                    "work_active",
                    3,
                    "Transactions, sessions or asynchronous producers remain active",
                    json!({
                        "listed_active_items": listed_active_items,
                        "listing_proves_completeness": false
                    }),
                    "inspect_observed_work_and_reconcile_at_source",
                );
            }
        }
        Err(e) => {
            let (code, reason) = failure(e.as_ref(), "drain_unavailable");
            r.component(
                "drain",
                code,
                json!({
                    "state": "unconfirmed",
                    "work": args.observation.observed_work()
                }),
            );
            r.issue(
                "drain",
                reason,
                code,
                if reason == "identity_present" {
                    "Managed identity is still present; isolation is unconfirmed"
                } else {
                    "Drain metadata or topology could not be completely verified"
                },
                json!({"identity":o.retired_identity}),
                "inspect_identity_and_drain_prerequisites",
            );
        }
    }
    *stage = "lock_recheck_unavailable";
    let after = (
        c.migration_lock_owner().await?,
        c.migration_transition_owner().await?,
    );
    *stage = "migration_history_recheck_unavailable";
    let history_after = history::observe(c, &o.table).await?;
    let changed = after != before || history_after != baseline;
    r.component(
        "observation_drift",
        if changed { 4 } else { 0 },
        json!({
            "changed": changed,
            "atomic_snapshot": false
        }),
    );
    if changed {
        r.issue(
            "observation_drift",
            "observation_changed",
            4,
            "Lock ownership or migration history changed during diagnosis",
            json!({
                "owners_before": {"main":before.0,"transition":before.1},
                "owners_after": {"main":after.0,"transition":after.1},
                "history_changed": history_after!=baseline
            }),
            "stop_concurrent_actors_and_repeat_read_only_diagnosis",
        );
    }
    Ok(())
}

pub async fn run(args: &Context, o: &Options) -> Result<()> {
    #[cfg(feature = "kafka")]
    let kafka_requested = o.connection_file.is_some()
        || o.topic.is_some()
        || o.checkpoint.is_some()
        || !o.offsets.is_empty();
    #[cfg(feature = "kafka")]
    let kafka_inputs_valid = !kafka_requested
        || (o.connection_file.is_some()
            && o.topic.is_some()
            && o.checkpoint.is_some()
            && !o.offsets.is_empty());
    #[cfg(not(feature = "kafka"))]
    let kafka_inputs_valid = true;
    if !(1..=3600).contains(&o.timeout) || !kafka_inputs_valid {
        return Err(failure::coded(
            2,
            "diagnosis_input_invalid",
            "diagnosis requires bounded deadlines and complete explicit Kafka inputs",
        ));
    }
    if args.identity_file.is_some() {
        return Err(failure::coded(
            2,
            "administrator_connection_required",
            "diagnose requires an independent administrator URL",
        ));
    }
    if let Some(n) = &o.retired_identity {
        identity::identity_id(n)
            .map_err(|_| failure::coded(2, "identity_input_invalid", "invalid managed identity"))?;
    }
    let receipt = o
        .receipt
        .as_deref()
        .map(drain::routine::Diagnostic::open)
        .transpose()
        .map_err(|e| {
            let code = e
                .downcast_ref::<failure::Failure>()
                .map(|e| e.code())
                .unwrap_or(7);
            failure::coded(
                code,
                if code == 2 {
                    "receipt_input_invalid"
                } else {
                    "receipt_unavailable"
                },
                "receipt could not be read and validated",
            )
        })?;
    let options = args.options(
        receipt
            .as_ref()
            .and_then(drain::routine::Diagnostic::database),
    )?;
    let source = match &o.source {
        Some(path) => {
            let source = sqlx_core::migrate::Migrator::new(path.as_path())
                .await
                .map_err(|_| {
                    failure::coded(2, "migration_source_invalid", "invalid migration source")
                })?;
            history::sources(&source)?;
            Some(source)
        }
        None => None,
    };
    args.observation
        .phase(crate::observation::Phase::Read, "recovery_diagnosis");
    let mut r = report::Report::new();
    let observations = async {
        match options.connect().await {
            Err(_) => {
                r.component("database_read", 7, json!({"state":"unavailable"}));
                r.issue(
                    "database_read",
                    "database_unavailable",
                    7,
                    "Database connection unavailable",
                    json!({}),
                    "restore_database_read_access",
                );
            }
            Ok(mut c) => {
                let mut stage = "lock_metadata_unavailable";
                let core =
                    observe_database(args, o, source.as_ref(), &mut r, &mut c, &mut stage).await;
                let _ = c.close_hard().await;
                if let Err(e) = core {
                    let (code, reason) = failure(e.as_ref(), stage);
                    r.component(
                        "database_read",
                        code,
                        json!({
                            "state": "incomplete",
                            "failed_check": stage
                        }),
                    );
                    r.issue(
                        "database_read",
                        reason,
                        code,
                        "Database metadata or history check did not complete",
                        json!({"failed_check":stage}),
                        "inspect_metadata_permissions_and_migration_history",
                    );
                }
            }
        }
        if let Some(receipt) = &receipt {
            match receipt.observe(args).await {
                Ok(v) => {
                    let mut code = v["exit_code"].as_u64().unwrap_or(7) as u8;
                    if code == 0
                        && !matches!(v["job"]["state"].as_str(), Some("STOPPED" | "CANCELLED"))
                    {
                        code = 3;
                        r.issue(
                            "routine",
                            "routine_active",
                            3,
                            "Managed Routine job is not terminal",
                            json!({
                                "job_id": v["job"]["id"],
                                "state": v["job"]["state"]
                            }),
                            "inspect_receipt_job_and_reconcile_at_source",
                        );
                    } else if code != 0 {
                        r.issue(
                            "routine",
                            "routine_unverified",
                            code,
                            "Receipt job binding or current state could not be verified",
                            json!({
                                "observation": v["observation"],
                                "last_confirmed_job": v["last_confirmed_job"]
                            }),
                            "inspect_original_receipt_and_current_job_metadata",
                        );
                    }
                    if let Some(name) = &o.retired_identity
                        && v["reservation"]["creator"]
                            .as_str()
                            .is_some_and(|creator| creator != name)
                    {
                        r.component(
                            "identity_binding",
                            3,
                            json!({
                                "supplied_identity": name,
                                "receipt_creator": v["reservation"]["creator"]
                            }),
                        );
                        r.issue(
                            "identity_binding",
                            "routine_identity_mismatch",
                            3,
                            "Supplied retired identity differs from the verified receipt creator",
                            json!({
                                "supplied_identity": name,
                                "receipt_creator": v["reservation"]["creator"]
                            }),
                            "correlate_original_runner_without_changing_identities",
                        );
                    }
                    r.component("routine", code, v);
                }
                Err(_) => {
                    r.component("routine", 2, json!({"state":"invalid"}));
                    r.issue(
                        "routine",
                        "receipt_input_invalid",
                        2,
                        "Receipt could not be validated",
                        json!({}),
                        "inspect_original_private_receipt",
                    );
                }
            }
        }
        #[cfg(feature = "kafka")]
        if let Some(file) = &o.connection_file {
            let action = kafka::Action::Preflight {
                connection_file: file.clone(),
                topic: o.topic.clone().unwrap(),
                checkpoint: o.checkpoint.clone(),
                offsets: o.offsets.clone(),
                timeout: o.timeout,
            };
            let v = kafka::evaluate(args, &action).await;
            let code = v["exit_code"].as_u64().unwrap_or(7) as u8;
            if code != 0 {
                r.issue(
                    "kafka",
                    "kafka_preflight_failed",
                    code,
                    "Independent Kafka preflight did not pass",
                    json!({
                        "checks": v["checks"],
                        "issues": v["issues"]
                    }),
                    "inspect_kafka_checks_without_resetting_or_transferring_offsets",
                );
            }
            r.component("kafka", code, v);
        }
    };
    if tokio::time::timeout(Duration::from_secs(o.timeout), observations)
        .await
        .is_err()
    {
        r.component(
            "incomplete",
            7,
            json!({
                "state": "deadline_expired",
                "timeout_seconds": o.timeout
            }),
        );
        r.issue(
            "incomplete",
            "diagnosis_deadline_expired",
            7,
            "Observation deadline expired; partial evidence is preserved",
            json!({"timeout_seconds":o.timeout}),
            "restore_read_access_and_repeat_read_only_diagnosis",
        );
    }
    if o.retired_identity.is_none() {
        r.component("identity", 4, json!({"state":"not_supplied"}));
        r.issue(
            "identity",
            "identity_not_supplied",
            4,
            "Old credentials and sessions were not attested",
            json!({}),
            "correlate_and_supply_the_original_retired_identity",
        );
    }
    let mut expected = vec!["locks", "migrations", "drain", "observation_drift"];
    if o.receipt.is_some() {
        expected.push("routine");
    }
    #[cfg(feature = "kafka")]
    if o.connection_file.is_some() {
        expected.push("kafka");
    }
    for name in expected {
        if r.components.get(name).is_none() {
            r.component(
                name,
                4,
                json!({
                    "state": "not_observed",
                    "reason": "prior_failure_or_deadline"
                }),
            );
            r.issue(
                name,
                "component_not_observed",
                4,
                "Check was not completed after an earlier failure or deadline",
                json!({}),
                "resolve_prior_failure_and_repeat_read_only_diagnosis",
            );
        }
    }
    r.finish_components();
    let code = r.code();

    let mut commands = vec![json!({
        "purpose": "inspect_lock_owners",
        "argv": ["cargo","sqlx-doris","admin","lock","status","--json"]
    })];
    let mut migration_argv = vec![
        "cargo".to_owned(),
        "sqlx-doris".to_owned(),
        "migrate".to_owned(),
        "--table".to_owned(),
        o.table.clone(),
    ];
    let source_representable = if let Some(path) = &o.source {
        if let Some(path) = path.to_str() {
            migration_argv.extend(["--source".to_owned(), path.to_owned()]);
            true
        } else {
            false
        }
    } else {
        false
    };
    if source_representable
        && r.components["migrations"]["observation"]["table_present"].is_boolean()
    {
        migration_argv.extend(["info".to_owned(), "--json".to_owned()]);
        commands.push(json!({
            "purpose": "inspect_migration_source_and_history",
            "argv": migration_argv
        }));
    }
    if let Some(name) = &o.retired_identity {
        commands.push(json!({
            "purpose": "inspect_scoped_drain",
            "argv": ["cargo","sqlx-doris","admin","drain","status","--name",name,"--json"]
        }));
    }
    if let Some(path) = o.receipt.as_ref().and_then(|p| p.to_str()) {
        commands.push(json!({
            "purpose": "inspect_receipt_job",
            "argv": ["cargo","sqlx-doris","admin","routine","status","--receipt",path,"--json"]
        }));
    }
    args.observation.data(
        "read_only_commands",
        json!({
            "reuse_same_administrator_connection": true,
            "commands": commands
        }),
    );
    args.observation.data("components", r.components);
    args.observation.data("issues", json!(r.issues));
    #[cfg(not(feature = "kafka"))]
    let kafka_requested = false;
    args.observation.data(
        "coverage",
        json!({
            "receipt_requested": o.receipt.is_some(),
            "kafka_requested": kafka_requested,
            "source_requested": o.source.is_some(),
            "identity_requested": o.retired_identity.is_some(),
            "external_actor_isolation_verified": false,
            "data_reconciliation_verified": false,
            "complete_async_drain_verified": false,
            "kafka_source_bound_to_routine": false,
            "identity_provenance_verified": false,
            "identity_to_lock_binding_verified": false
        }),
    );

    if code == 0 {
        Ok(())
    } else {
        Err(failure::fault(
            code,
            "Recovery diagnosis incomplete, blocked or unavailable; inspect stable issue codes and evidence",
        ))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unavailable_components_prevent_a_clean_result_and_known_conflicts_remain_visible() {
        assert_eq!(combine(&[0, 4, 3]), 3);
        assert_eq!(combine(&[0, 3, 7]), 7);
        assert_eq!(combine(&[0, 0]), 0);
    }
}
