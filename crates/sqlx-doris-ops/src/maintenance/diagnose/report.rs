//! Stable, allowlisted observations shared by human and machine renderers.
use super::{Value, json};

pub(super) struct Report {
    pub components: Value,
    pub issues: Vec<Value>,
}
impl Report {
    pub fn new() -> Self {
        Self {
            components: json!({}),
            issues: Vec::new(),
        }
    }
    pub fn component(&mut self, name: &str, code: u8, observation: Value) {
        self.components[name] = json!({
            "exit_code": code,
            "status": crate::failure::status(code),
            "observation": observation,
            "recorded_at_unix_ms": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d|d.as_millis()).unwrap_or(0)
        });
    }
    pub fn issue(
        &mut self,
        component: &str,
        code: &'static str,
        exit: u8,
        message: &'static str,
        context: Value,
        next_action: &'static str,
    ) {
        self.issues.push(json!({
            "component": component,
            "code": code,
            "exit_code": exit,
            "status": crate::failure::status(exit),
            "message": message,
            "context": context,
            "next_action": next_action
        }));
    }
    pub fn code(&self) -> u8 {
        let codes = self
            .components
            .as_object()
            .unwrap()
            .values()
            .map(|v| v["exit_code"].as_u64().unwrap_or(7) as u8)
            .chain(
                self.issues
                    .iter()
                    .map(|v| v["exit_code"].as_u64().unwrap_or(7) as u8),
            )
            .collect::<Vec<_>>();
        super::combine(&codes)
    }
    pub fn finish_components(&mut self) {
        for (name, component) in self.components.as_object_mut().unwrap() {
            component["issues"] = self
                .issues
                .iter()
                .filter(|v| v["component"] == name.as_str())
                .cloned()
                .collect();
        }
    }
}

pub(super) fn migrations(
    report: &mut Report,
    baseline: &super::history::History,
    source: Option<&sqlx_core::migrate::Migrator>,
) -> super::Result<()> {
    use super::history::{self, SourceState};
    let mut code = 0;
    let mut rows = Vec::new();
    if !baseline.present {
        report.issue(
            "migrations",
            "migration_table_absent",
            0,
            "Migration history table is absent; it was not created",
            json!({}),
            "review_migration_source",
        );
    }
    for comparison in history::compare(baseline, source)? {
        let record = &comparison.record;
        if !record.success {
            code = 3;
            report.issue(
                "migrations",
                "migration_dirty",
                3,
                "Migration has a durable dirty marker",
                json!({"version": record.version}),
                "reconcile_schema_and_data_before_explicit_repair",
            );
        }
        match comparison.state {
            SourceState::Missing => {
                code = 3;
                report.issue(
                    "migrations",
                    "migration_source_missing",
                    3,
                    "Recorded migration has no matching up source",
                    json!({"version": record.version}),
                    "restore_the_original_migration_source",
                );
            }
            SourceState::ChecksumMismatch => {
                code = 3;
                report.issue(
                    "migrations",
                    "migration_checksum_mismatch",
                    3,
                    "Recorded and source checksums differ",
                    json!({
                        "version": record.version,
                        "recorded_checksum": record.checksum,
                        "source_checksum": comparison.source_checksum
                    }),
                    "compare_original_source_without_rewriting_history",
                );
            }
            _ => {}
        }
        let mut row = serde_json::to_value(record)?;
        row["source_state"] = json!(comparison.state);
        rows.push(row);
    }
    let pending = source.map(history::sources).transpose()?.map(|versions| {
        versions
            .keys()
            .filter(|version| !baseline.rows.iter().any(|row| row.version == **version))
            .copied()
            .collect::<Vec<_>>()
    });
    report.component(
        "migrations",
        code,
        json!({
            "table_present": baseline.present,
            "rows": rows,
            "source": if source.is_some() { "compared" } else { "not_requested" },
            "pending_versions": pending
        }),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostics_keep_independent_failures_and_render_structured_evidence() {
        let mut r = Report::new();
        r.component("migrations", 3, json!({}));
        r.issue(
            "migrations",
            "migration_dirty",
            3,
            "Dirty marker",
            json!({"version":42}),
            "reconcile_schema_and_data_before_explicit_repair",
        );
        r.component("kafka", 7, json!({}));
        r.issue(
            "kafka",
            "kafka_preflight_failed",
            7,
            "Kafka unavailable",
            json!({}),
            "restore_kafka_read_access",
        );
        r.finish_components();
        assert_eq!(r.code(), 7);
        assert_eq!(
            r.components["migrations"]["issues"][0]["code"],
            "migration_dirty"
        );
    }
}
