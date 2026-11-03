use super::{Result, fault};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(super) const FORMAT: &str = "sqlx-doris-kafka-checkpoint-v1";
pub(super) const MAX_PARTITIONS: usize = 4096;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Partition {
    pub partition: i32,
    pub leader_epoch: i32,
    pub earliest: i64,
    pub end: i64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Snapshot {
    pub format: String,
    pub isolation_level: String,
    pub captured_at_unix_ms: u64,
    pub cluster_id: String,
    pub topic: String,
    pub topic_id: String,
    pub partitions: Vec<Partition>,
}
pub(super) fn topic(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 249
        || value.starts_with("__")
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(fault(2, "invalid or internal Kafka topic"));
    }
    Ok(())
}
impl Snapshot {
    pub fn validate(&self) -> Result<()> {
        topic(&self.topic)?;
        if self.isolation_level != "read_uncommitted"
            || self.format != FORMAT
            || self.captured_at_unix_ms == 0
            || self.cluster_id.is_empty()
            || self.cluster_id.len() > 256
            || !self
                .cluster_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            || uuid::Uuid::parse_str(&self.topic_id)
                .ok()
                .filter(|id| !id.is_nil() && id.to_string() == self.topic_id)
                .is_none()
            || self.partitions.is_empty()
            || self.partitions.len() > MAX_PARTITIONS
        {
            return Err(fault(2, "invalid Kafka checkpoint identity/format"));
        }
        for (i, p) in self.partitions.iter().enumerate() {
            if p.partition != i as i32 || p.leader_epoch < 0 || p.earliest < 0 || p.end < p.earliest
            {
                return Err(fault(
                    2,
                    "invalid/duplicate/unsorted checkpoint partitions or bounds",
                ));
            }
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug)]
pub(super) struct Epoch {
    pub error: i16,
    pub epoch: i32,
    pub end: i64,
}
#[derive(Clone, Debug)]
pub struct Offset {
    pub partition: i32,
    pub next: i64,
}
pub fn offset(raw: &str) -> std::result::Result<Offset, String> {
    let bad = || "offset must be partition=nonnegative-next-offset (numeric only)".to_owned();
    let (p, n) = raw.split_once('=').ok_or_else(bad)?;
    if p.is_empty()
        || n.is_empty()
        || !p.bytes().all(|b| b.is_ascii_digit())
        || !n.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(bad());
    }
    Ok(Offset {
        partition: p.parse().map_err(|_| bad())?,
        next: n.parse().map_err(|_| bad())?,
    })
}
pub(super) fn offsets(values: &[Offset]) -> Result<BTreeMap<i32, i64>> {
    let mut out = BTreeMap::new();
    for v in values {
        if v.partition < 0 || v.next < 0 {
            return Err(fault(
                2,
                "candidate partition and offset must be nonnegative",
            ));
        }
        if out.insert(v.partition, v.next).is_some() {
            return Err(fault(
                2,
                "duplicate candidate partition; offsets are never inferred",
            ));
        }
    }
    if out.is_empty() || out.len() > MAX_PARTITIONS {
        return Err(fault(2, "explicit candidate offsets required"));
    }
    Ok(out)
}
pub(super) fn compare(
    current: &Snapshot,
    baseline: Option<&Snapshot>,
    candidates: &BTreeMap<i32, i64>,
    epochs: &BTreeMap<i32, Epoch>,
) -> (u8, Vec<Value>, Vec<Value>) {
    let mut issues = Vec::new();
    let mut checks = Vec::new();
    let mut code = 0;
    let mut issue = |severity: u8, name: &str, p: Option<i32>| {
        if code == 0 || severity == 3 {
            code = severity;
        }
        issues.push(json!({
            "code": name,
            "partition": p
        }));
    };
    let ids: BTreeSet<_> = current.partitions.iter().map(|p| p.partition).collect();
    if ids != candidates.keys().copied().collect() {
        issue(3, "candidate_partition_set_differs", None);
    }
    let same_identity = baseline.is_some_and(|old| {
        old.cluster_id == current.cluster_id
            && old.topic == current.topic
            && old.topic_id == current.topic_id
    });
    if let Some(old) = baseline {
        if old.cluster_id != current.cluster_id {
            issue(3, "cluster_id_changed", None);
        }
        if old.topic != current.topic || old.topic_id != current.topic_id {
            issue(3, "topic_identity_changed", None);
        }
        if old
            .partitions
            .iter()
            .map(|p| p.partition)
            .collect::<BTreeSet<_>>()
            != ids
        {
            issue(3, "partition_set_changed", None);
        }
    } else {
        issue(4, "historical_checkpoint_missing", None);
    }
    for p in &current.partitions {
        let n = candidates.get(&p.partition).copied();
        let range = match n {
            None => "missing",
            Some(v) if v < p.earliest => "expired",
            Some(v) if v > p.end => "beyond_end",
            _ => "in_range",
        };
        if range != "in_range" {
            issue(3, "candidate_outside_observed_range", Some(p.partition));
        }
        let old = baseline
            .filter(|_| same_identity)
            .and_then(|o| o.partitions.iter().find(|v| v.partition == p.partition));
        let e = epochs.get(&p.partition);
        let history = if let Some(old) = old {
            if p.leader_epoch < old.leader_epoch || p.end < old.end || p.earliest < old.earliest {
                issue(3, "epoch_or_log_bounds_regressed", Some(p.partition));
                "diverged"
            } else if e.is_some_and(|e| e.error == 0 && e.end >= 0 && e.end < old.end) {
                issue(3, "checkpoint_tail_truncated", Some(p.partition));
                "diverged"
            } else if old.end == old.earliest || p.earliest >= old.end {
                // Kafka may echo a requested expired epoch with the first retained
                // epoch's start offset. Equality alone is not historical proof.
                issue(4, "history_anchor_empty_or_expired", Some(p.partition));
                "unverifiable"
            } else if !e
                .is_some_and(|e| e.error == 0 && e.epoch == old.leader_epoch && e.end >= old.end)
            {
                issue(4, "retained_epoch_evidence_unavailable", Some(p.partition));
                "unverifiable"
            } else if n.is_some_and(|v| v > old.end || v < old.earliest) {
                issue(
                    4,
                    "candidate_outside_checkpoint_coverage",
                    Some(p.partition),
                );
                "outside_anchor"
            } else {
                "no_anchor_divergence_observed"
            }
        } else {
            "unverifiable"
        };
        checks.push(json!({
            "partition": p.partition,
            "next_offset": n,
            "range": range,
            "history": history,
            "requested_epoch": old.map(|p|p.leader_epoch),
            "returned_epoch": e.map(|e|e.epoch),
            "epoch_end": e.map(|e|e.end),
            "epoch_error_code": e.map(|e|e.error)
        }));
    }
    (code, issues, checks)
}
