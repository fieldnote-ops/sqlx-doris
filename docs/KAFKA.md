<a name="doc-top"></a>

# Kafka continuation preflight

[English](KAFKA.md) | [简体中文](KAFKA.zh-CN.md)

[Documentation](README.md)

Specialist opt-in: install the `cli-admin-kafka` feature of `sqlx-doris`; commands live under `cargo sqlx-doris admin`. Everyday queries and Stream Load do not require it.

Read-only evidence for administrator-chosen candidates. No offset is transferred or reset, and no result authorizes a replacement job or lock recovery.

<a name="doc-contents"></a>

## Contents

- [Progress, commits and partition changes](#doc-progress)
- [Capture and check source history](#doc-preflight)

<a name="doc-progress"></a>

## Progress, commits and partition changes

Actual Kafka messages, a DUPLICATE KEY table and server transactions verify both orders. After confirming BE acknowledgement of the same JobId/TxnId PREPARE task, STOP winning before commit aborts the batch without writing it. A commit can also win after the final CLI read while STOP is queued, retaining data and advancing Progress. FE registers PREPARE before submission and can retain it after BE refusal. Neither RUNNING nor FE-only PREPARE proves data processing: match JobId/TxnId, positive BeId and a valid execution start. Retiring the account does not revoke the internal commit authorization of already dispatched BE tasks. Retirement plus STOP acknowledgement therefore does not imply no late writes; drain, terminal transaction checks, data reconciliation and all partitions still matter.

In verified 4.1.5, JSON `job.progress` is Doris's reported cursor: a number normally equals the internal next position minus one, including **configured starts before any commit**. A fresh job starting partition 0 at 3 can show `{"0":"2"}` with zero inserted rows. JSON labels this `progress_semantics:doris_reported_cursor`, `progress_scope:job_reported_partitions_only`, `progress_proves_consumption:false` and `kafka_continuation_verified:false`. The confirmation sidecar certifies creation binding, not consumption. See [4.1.5 cursor implementation](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/load/routineload/KafkaProgress.java).

After the old job is terminal, transactions have ended and actual data has been reconciled, interpret its final cursor by partition ID and check Kafka identity, partition coverage and ranges. Adding one yields only a candidate next offset. `{"0":"0","1":"2"}` yields `{0:1,1:3}`; a global maximum of 3 skips partition-0 records. Keep kafka_partitions and kafka_offsets paired in the same order. A pre-STOP snapshot may advance after the final check and replay committed records if copied; recheck terminal metadata, transactions and rows.

| Kafka change | Continuation requirement |
| --- | --- |
| Added partition | Explicit kafka_partitions does not expand automatically. An absent Progress key does not prove an empty partition; choose and reconcile its origin explicitly. |
| Dynamic discovery | Native Doris discovers partitions when none are specified. Default kafka_default_offsets is OFFSET_END and may skip records present before discovery. Explicitly use property.kafka_default_offsets=OFFSET_BEGINNING when those records are required, and reconcile ranges/data. |
| Expired/deleted prefix | next below broker earliest cannot recover removed records. Do not rely on PAUSE/refusal: the verified combination can commit zero rows and advance to end while staying RUNNING. Resetting to earliest still loses the expired interval and requires external recovery or explicitly accepted loss. |
| Tail regression | next above end is invalid. If end later regrows past next, reused offsets may contain new records that the old number skips. Resetting to earliest may instead replay retained records already written to Doris. An in-range number alone does not prove unchanged history. |
| Same-name topic recreation | Names, cluster ID and numeric ranges can match while topic ID changes. Old progress cannot be transferred across that generation. |

OFFSET_END/OFFSET_ZERO/OFFSET_BEGINNING are unresolved sentinels; do not increment them. Missing partitions, overflow, gaps, cluster/topic identity, leader epochs or unverified truncation require continued isolation and explicit data reconciliation. Kafka's ordinary partition-increase operation cannot shrink the partition count; delete/recreate is a new generation. Reads do not atomically fence later source changes; prohibit rebinding, truncation, deletion and other admission changes during maintenance.

The read-only [Kafka preflight](#doc-preflight) compares observations with an earlier baseline; it does not certify Doris data completeness, transfer/reset offsets, create replacements or promise end-to-end exactly-once. UUID reservations isolate job names without verifying Kafka topic ID, partition coverage, leader epochs or log contents. See [Doris source parameters](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/load-and-export/CREATE-ROUTINE-LOAD/) and [Kafka Admin API](https://kafka.apache.org/40/javadoc/org/apache/kafka/clients/admin/Admin.html).

STOPPED does not undo committed data or prove all admitted tasks/transactions/BE resources are finished. Rerun drain wait, reconcile other work/data, then explicitly recover locks. There is no automatic takeover. Ordinary drain still observes and blocks unmanaged jobs without requiring reservation views.

<a name="doc-preflight"></a>

## Capture and check source history

`kafka snapshot` and `kafka preflight` use public Kafka APIs directly, without Java/Docker/Kafka tools or a Doris connection. Capture a trusted baseline at a reconciled point before source changes, isolate source administration during checks, and explicitly supply each partition's candidate next offset. Commands never read records, join groups, commit/reset/transfer offsets, create replacement producers or recover locks. Doris Progress is never converted automatically.

Keep connection JSON and its CA/optional client PEM certificate and unencrypted key in private regular external files (0600, no symlinks):

```json
{
  "format": "sqlx-doris-kafka-connection-v1",
  "bootstrap_servers": ["broker.example:9093"],
  "security_protocol": "SASL_SSL",
  "ca_file": "<external-volume>/kafka/ca.pem",
  "sasl_plain": {"username": "readonly", "password": "<set privately>"}
}
```

Use SSL without sasl_plain. Optional client_certificate_file/client_key_file must be paired. PLAINTEXT rejects credentials/TLS files. SCRAM, Kerberos, OAuth and SASL_PLAINTEXT are unsupported and rejected; no insecure TLS fallback. CA and broker hostname verification are mandatory; all advertised addresses must be reachable. Grant only the read permissions for these metadata APIs. Kafka 4.0.0 is verified; this adds no older Doris support.

Set KAFKA_CONFIG to that private JSON file and configure SQLX_DORIS_EXTERNAL_VOLUME as in Installation. Create the external kafka output directory before capturing a new baseline.

```sh
mkdir -p "$SQLX_DORIS_EXTERNAL_VOLUME/kafka"
cargo sqlx-doris admin kafka snapshot --connection-file "$KAFKA_CONFIG"   --topic events --output "$SQLX_DORIS_EXTERNAL_VOLUME/kafka/events-before.json" --json

# After STOP, transaction drain and business-data reconciliation; candidates are explicit.
cargo sqlx-doris admin kafka preflight --connection-file "$KAFKA_CONFIG"   --topic events --checkpoint "$SQLX_DORIS_EXTERNAL_VOLUME/kafka/events-before.json"   --offset 0=123 --offset 1=456 --timeout 30 --json
```

Snapshot writes only a new private local file, never overwriting an earlier baseline. Its parent must exist. It retains cluster/topic IDs, full partitions, leader epochs, earliest/end and READ_UNCOMMITTED semantics. Limits: 64 KiB connection JSON, 1 MiB checkpoint/certificate, 4096 partitions, 256 brokers and 1 MiB response frames. Unknown fields, duplicate partitions, symlinks, broad permissions, negative/sentinel/overflow offsets fail. Topic UUID hex differs textually from Kafka tools' Base64 representation of the same 16 bytes.

Preflight batches ranges and OffsetForLeaderEpoch by leader, rechecks metadata/bounds for observation-time changes, compares full identities/partition sets and `[earliest,end]` per explicit next offset (end itself is valid). Earlier epoch/end anchors can expose truncation even after end regrows past the old next. Missing baseline, candidates outside its coverage, empty/fully expired anchors and missing/mismatched epoch evidence remain inconclusive. Kafka may echo an expired requested epoch with a newer epoch's start; numerical equality alone is insufficient.

| Exit | Meaning |
| --- | --- |
| 0 | Snapshot saved or observations consistent with the baseline/candidates; never continuation authorization. |
| 2 | Invalid arguments, private input, configuration or checkpoint. |
| 3 | Observed identity/partition change, range conflict or truncated historical tail. |
| 4 | History inconclusive or source changed during observation; retain isolation/reconcile. |
| 7 | Connection, authentication, permission, protocol read or total deadline failed. |

`--json` after kafka or its leaf command emits one `sqlx-doris-cli-result-v1` stdout object for controlled success/error, with empty stderr; help/version stay text. `checks` contain per-partition range/history and requested/returned epoch; `issues` contain stable reason codes. Checkpoint isolation_level is read_uncommitted. Every result has server_read_only=true, atomic_snapshot=false, kafka_continuation_verified=false and safe_to_recover=false, without passwords, connection configuration or raw server errors. Forced termination/crashes cannot guarantee JSON.

**Exit 0 does not prove lossless or duplicate-free continuation.** Reads are not an atomic snapshot or a fence against later administration. Baseline files have no server signature and must be trusted/protected by their administrator; edited contents are not historical proof. Retained epochs are not a complete truncation audit. Pre-baseline divergence, restored/reused metadata/IDs/epochs, compaction/interior record gaps, content, Kafka READ_COMMITTED visibility and actual Doris consumption/writes remain unverified. READ_UNCOMMITTED bounds do not prove every record exists or is visible. A new post-event snapshot cannot recover missing old history; never replace a conflicting baseline to erase that evidence. Reconcile data/source history before explicitly choosing a new known boundary. See [Kafka protocol](https://kafka.apache.org/40/design/protocol/) and [epoch retention behavior](https://github.com/apache/kafka/blob/4.0.0/storage/src/main/java/org/apache/kafka/storage/internals/epoch/LeaderEpochFileCache.java).
