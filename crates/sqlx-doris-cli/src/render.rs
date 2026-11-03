use serde_json::Value;
use sqlx_doris_ops::observation::Event;
use std::io::Write;

/// Escape control characters from paths and server metadata without hiding Unicode.
pub(crate) fn text(value: &Value) -> String {
    value.as_str().map_or_else(
        || value.to_string(),
        |value| value.chars().flat_map(char::escape_debug).collect(),
    )
}

fn work(entry: &Value) -> String {
    let kind = entry["kind"].as_str().unwrap_or("work");
    let state = text(&entry["state"]);
    let state = if entry["blocked"] == false {
        format!("terminal {state}")
    } else {
        state
    };
    match kind {
        "session" => format!(
            "session {} command {}",
            text(&entry["id"]),
            text(&entry["command"])
        ),
        "transaction" => format!(
            "transaction {} {state}; owner not available",
            text(&entry["id"])
        ),
        "COLUMN" | "ROLLUP" => format!(
            "{kind} job {} table {} {state}",
            text(&entry["id"]),
            text(&entry["table"])
        ),
        "load_registry" => format!(
            "LOAD registry pending {} running {}",
            entry["pending"], entry["running"]
        ),
        "load" => format!(
            "LOAD job {} target {} observed {}",
            entry["job_id"],
            text(&entry["name_or_target"]),
            text(&entry["state"])
        ),
        "routine_load" => format!(
            "ROUTINE LOAD job {} target {} observed {}",
            entry["job_id"],
            text(&entry["name_or_target"]),
            text(&entry["state"])
        ),
        "mv_task" => format!(
            "MV job {} task {} target {} observed {}",
            entry["job_id"],
            entry["task_id"],
            text(&entry["name_or_target"]),
            text(&entry["state"])
        ),
        "mv_scheduler" => format!(
            "MV scheduler {} target {} observed {}",
            entry["job_id"],
            text(&entry["name_or_target"]),
            text(&entry["state"])
        ),
        _ => format!("work {entry}"),
    }
}

pub(crate) fn progress(event: &Event) -> sqlx_doris_ops::failure::Result<()> {
    let mut output = std::io::stdout().lock();
    match event {
        Event::ReceiptPersisted { name, path } => writeln!(
            output,
            "managed-routine {name}\nreceipt {}\nThe name and reservation are permanently consumed, including on failure; no CREATE retry.",
            text(&Value::String(path.clone()))
        )?,
        Event::CredentialsPersisted { name, path } => {
            writeln!(
                output,
                "managed-identity {name}\ncredential-file {}",
                text(&Value::String(path.clone()))
            )?;
        }
        Event::WorkObserved { entry } => writeln!(output, "{}", work(entry))?,
    }
    output.flush()?;
    Ok(())
}

#[cfg(feature = "admin")]
fn summary(operation: &str, value: &Value) -> Vec<String> {
    let mut lines = Vec::new();
    if operation.starts_with("routine.") {
        for key in [
            "database",
            "job_name",
            "receipt",
            "observation",
            "outcome",
            "step",
            "message",
        ] {
            if let Some(value) = value.get(key).filter(|value| !value.is_null()) {
                lines.push(format!("{key} {}", text(value)));
            }
        }
        if let Some(output) = value.get("receipt_output") {
            lines.push(format!(
                "receipt-output {} persistence {}",
                text(&output["path"]),
                text(&output["state"])
            ));
        }
        if let Some(job) = value.get("job").filter(|job| !job.is_null()) {
            lines.push(format!(
                "job-id {} state {} reported-progress {}",
                job["id"],
                text(&job["state"]),
                job["progress"]
            ));
        }
    } else if operation.starts_with("identity.") {
        if let Some(identity) = value.get("identity") {
            lines.push(format!(
                "identity {} database {} cluster {}",
                text(&identity["name"]),
                text(&identity["database"]),
                text(&identity["cluster"])
            ));
            if identity["preview"] == true {
                lines.push(
                    "Preview only; no changes made. Execution requires --maintenance --force."
                        .into(),
                );
            } else if identity["retired"] == true {
                lines.push("Managed identity retired. No locks/history removed. Previously admitted transactions and async jobs still require drain and reconciliation.".into());
            } else if let Some(present) = identity["present"].as_bool() {
                lines.push(format!(
                    "present {present}; absence alone does not attest isolation or drain"
                ));
            } else if let Some(path) = identity.get("credential_file") {
                lines.push(format!(
                    "credential-file {}; retain it when creation is unconfirmed",
                    text(path)
                ));
            }
        }
    } else if operation.starts_with("drain.") {
        if let Some(target) = value.get("target") {
            lines.push(format!("target {target}"));
            if target["preview"] == true {
                lines.push("Preview only; no CANCEL/ABORT sent. Execution requires explicit maintenance and exact work identities.".into());
            }
        }
        if let Some(completion) = value.get("completion") {
            if completion["kind"] == "broker_load" {
                lines.push(format!("LOAD job {} CANCELLED; exact transaction {} ABORTED. Committed data cannot be undone; locks/history retained.", completion["job_id"], completion["transaction_id"]));
            } else {
                lines.push(format!(
                    "job {} task {} reported terminal {}; locks/history retained",
                    completion["job_id"],
                    completion["task_id"],
                    text(&completion["state"])
                ));
            }
        }
        if let Some(drain) = value.get("drain") {
            lines.push(if drain["scoped_empty"] == true {
                "Scoped snapshot empty; this is not an atomic write fence or a recovery credential. Reconcile uncovered work and maintain external isolation.".into()
            } else {
                "Drain blocked; inspect the observed work. No lock recovery performed.".into()
            });
        }
    }
    lines
}

pub(crate) fn command(operation: &str, value: &Value) {
    #[cfg(not(feature = "admin"))]
    let _ = (operation, value);
    #[cfg(feature = "admin")]
    if let Some(operation) = operation.strip_prefix("admin.") {
        #[cfg(feature = "kafka-admin")]
        if operation.starts_with("kafka.") {
            super::render_kafka::render(value);
            return;
        }
        if operation == "diagnose.recovery" && value["components"].is_object() {
            let issues = value["issues"].as_array().map(Vec::as_slice).unwrap_or(&[]);
            for line in super::render_diagnose::render(&value["components"], issues, &value["work"])
            {
                println!("{line}");
            }
        } else {
            for line in summary(operation, value) {
                println!("{line}");
            }
        }
    }
}

pub(crate) fn runner(value: &Value) {
    if let Some(identity) = value.get("runner_identity") {
        println!(
            "managed-identity {} main-owner {}",
            text(&identity["name"]),
            text(&identity["main_owner"])
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn terminal_output_distinguishes_observation_from_safety_and_escapes_server_text() {
        assert_eq!(text(&json!("中文\n\u{1b}[31m")), "中文\\n\\u{1b}[31m");
        let active =
            work(&json!({"kind": "transaction", "id": 7, "state": "PREPARE", "blocked": true}));
        assert!(active.contains("owner not available"));
        let terminal =
            work(&json!({"kind": "transaction", "id": 7, "state": "ABORTED", "blocked": false}));
        assert!(terminal.contains("terminal ABORTED"));
        let paused = work(
            &json!({"kind": "mv_scheduler", "job_id": 8, "state": "PAUSED", "blocked": false}),
        );
        assert!(paused.contains("MV scheduler 8"));
        assert!(paused.contains("observed PAUSED"));
        assert!(!paused.contains("terminal"));
        #[cfg(feature = "admin")]
        let lines = summary("drain.wait", &json!({"drain": {"scoped_empty": true}}));
        #[cfg(feature = "admin")]
        assert!(lines.join("\n").contains("not an atomic write fence"));
    }
}
