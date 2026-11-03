use serde_json::{Value, json};
pub(crate) fn render(components: &Value, issues: &[Value], work: &Value) -> Vec<String> {
    let mut lines = Vec::new();
    for (name, result) in components.as_object().unwrap() {
        lines.push(format!(
            "{name}: {} (exit {})",
            result["status"].as_str().unwrap_or("unavailable"),
            result["exit_code"]
        ));
        if name == "locks" {
            let o = &result["observation"];
            lines.push(format!(
                "  main-owner {}; transition-owner {}",
                o["main_owner"], o["transition_owner"]
            ));
        }
        if name == "migrations"
            && let Some(rows) = result["observation"]["rows"].as_array()
        {
            lines.push(format!(
                "  recorded versions {}; pending source versions {}",
                rows.len(),
                result["observation"]["pending_versions"]
            ));
        }
        if name == "routine" {
            let job = &result["observation"]["job"];
            lines.push(format!(
                "  reported job {}; state {}; observation {}",
                job["id"], job["state"], result["observation"]["observation"]
            ));
        }
        for i in issues.iter().filter(|v| v["component"] == name.as_str()) {
            lines.push(format!(
                "  {}: {}",
                i["code"].as_str().unwrap(),
                i["message"].as_str().unwrap()
            ));
            if i["context"] != json!({}) {
                lines.push(format!("    evidence {}", i["context"]));
            }
            lines.push(format!(
                "    next: {}",
                next_text(i["next_action"].as_str().unwrap())
            ));
        }
    }
    if let Some(work) = work.as_object() {
        for entry in work.values().filter(|v| v["blocked"] == true).take(20) {
            lines.push(format!("active work: {entry}"));
        }
        let active = work.values().filter(|v| v["blocked"] == true).count();
        if active > 20 {
            lines.push(format!(
                "{} more active items; use --json for the complete observed list",
                active - 20
            ));
        }
    }
    lines
}
fn next_text(action: &str) -> &'static str {
    match action {
        "review_migration_source" => {
            "Review the requested migration source; info/diagnose will not create history tables."
        }
        "reconcile_schema_and_data_before_explicit_repair" => {
            "Compare actual schema and committed data with this migration before any explicit repair."
        }
        "restore_the_original_migration_source" => {
            "Locate the original up migration file for the recorded version."
        }
        "compare_original_source_without_rewriting_history" => {
            "Compare the recorded and source checksums; preserve the original history."
        }
        "restore_database_read_access" => {
            "Check the administrator endpoint, authentication, TLS and read permissions."
        }
        "inspect_metadata_permissions_and_migration_history" => {
            "Check metadata permissions and the indicated history check; retain all locks and history."
        }
        "keep_external_isolation_and_inspect_owners" => {
            "Keep external runner isolation; inspect the exact main and transition owners with lock status."
        }
        "inspect_observed_work_and_reconcile_at_source" => {
            "Inspect the listed work at its source; use drain status to obtain a fresh scoped observation."
        }
        "inspect_identity_and_drain_prerequisites" => {
            "Inspect the original identity and supported topology; account retirement requires a separate explicit command."
        }
        "stop_concurrent_actors_and_repeat_read_only_diagnosis" => {
            "Stop concurrent recovery/administration actors externally, then repeat this read-only diagnosis."
        }
        "inspect_receipt_job_and_reconcile_at_source" => {
            "Use routine status with the original receipt and reconcile its admitted work."
        }
        "inspect_original_receipt_and_current_job_metadata"
        | "inspect_original_private_receipt" => {
            "Check the original private receipt, confirmation and current job metadata without repeating CREATE or STOP."
        }
        "correlate_original_runner_without_changing_identities"
        | "correlate_and_supply_the_original_retired_identity" => {
            "Correlate the original runner and supply its exact managed identity; a receipt does not prove lock ownership."
        }
        "inspect_kafka_checks_without_resetting_or_transferring_offsets" => {
            "Inspect the Kafka partition checks and issue codes; preserve candidate offsets and the earlier checkpoint."
        }
        _ => {
            "Resolve the failed read or deadline, then repeat the same read-only diagnosis; an empty observation is not a recovery fence."
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_text_is_escaped_and_independent_findings_are_rendered() {
        let components = json!({
            "locks": {"status": "blocked", "exit_code": 3,
                "observation": {"main_owner": "bad\n\u{1b}[31m", "transition_owner": null}},
            "migrations": {"status": "blocked", "exit_code": 3,
                "observation": {"rows": [], "pending_versions": []}}
        });
        let issues = vec![
            json!({"component": "migrations", "code": "migration_dirty", "message": "dirty", "context": {}, "next_action": "reconcile_schema_and_data_before_explicit_repair"}),
            json!({"component": "migrations", "code": "migration_checksum_mismatch", "message": "mismatch", "context": {"version": 1}, "next_action": "compare_original_source_without_rewriting_history"}),
        ];
        let lines = render(
            &components,
            &issues,
            &json!({
                "x": {"blocked": true, "name": "x\n\u{1b}[31m"}
            }),
        );
        assert!(lines.iter().all(|line| !line.contains(['\n', '\u{1b}'])));
        let output = lines.join("\n");
        assert!(output.contains("migration_dirty"));
        assert!(output.contains("migration_checksum_mismatch"));
        assert!(output.contains("\\n"));
        assert!(output.contains("next:"));
    }
}
