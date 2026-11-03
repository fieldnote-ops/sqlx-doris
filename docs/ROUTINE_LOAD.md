<a name="doc-top"></a>

# Managed Routine Load

[English](ROUTINE_LOAD.md) | [简体中文](ROUTINE_LOAD.zh-CN.md)

[Documentation](README.md)

Specialist opt-in: install the `cli-admin` feature of `sqlx-doris`; commands live under `cargo sqlx-doris admin`. Everyday queries and Stream Load do not require it.

For new single-table Kafka jobs on the verified topology. Existing ordinary jobs are not adopted. Use the [recovery runbook](RECOVERY.md) to retire the old creator and reconcile work before an explicit STOP. For Progress and offsets see [Kafka continuation](KAFKA.md#doc-progress).

<a name="doc-contents"></a>

## Contents

- [Create, inspect and explicitly stop](#doc-protocol)
- [Receipts, states and exits](#doc-receipts)
- [Non-reuse boundaries](#doc-non-reuse)

<a name="doc-protocol"></a>

## Create, inspect and explicitly stop

**New jobs can use routine create/status/stop, conditional on every creator following permanent non-reuse.** Supported scope is shared-nothing 4.1.5, one master FE, built-in authentication/authorization and a single current-internal-database Kafka table. The administrator CLI retains its own URL and opens a separate restricted connection using a live private file from identity create. Actual job User must match that runner; administrator/runner endpoint, database and connection options must match. Global --identity-file is not accepted for routine administration.

Successful create confirms registration/binding, not consumption or import success; inspect actual state. Creation generates a fresh `sqlx_rl_<v4 UUID>` without accepting caller names, reuse or adoption. Before any mutation it syncs a new private external 0600 receipt. Atomic CREATE VIEW, without IF NOT EXISTS/OR REPLACE, permanently reserves `_sqlx_rl_<UUID>` with protocol, cluster/database ID, target, creator and name. Only after acknowledgement/validation is one Routine CREATE sent. Keep this ordinary view forever, including failures/cancellation: no rows/tablets, but one FE metadata object per generation. Never rewrite, remove or restore reservations as cache maintenance. A collision with an existing reservation cannot advance to job creation.

Supply a native SQL suffix after ON <table>, including FROM KAFKA, in an external UTF-8 file, maximum 64 KiB, with no semicolon or NUL anywhere:

```sql
COLUMNS TERMINATED BY ',', COLUMNS(id,value)
PROPERTIES('desired_concurrent_number'='1','max_batch_interval'='5')
FROM KAFKA('kafka_broker_list'='kafka:9092','kafka_topic'='events',
           'kafka_partitions'='0','kafka_offsets'='OFFSET_END')
```

Create the target table first. With the administrator environment and live `$credentials` above, and an existing receipt parent directory:

```sh
cargo sqlx-doris admin routine create --runner-identity-file "$credentials" --table events \
  --source "$SQLX_DORIS_EXTERNAL_VOLUME/routine.sql" \
  --output "$SQLX_DORIS_EXTERNAL_VOLUME/routine-receipt.json" --managed-namespace
# Receipt selects its database and verifies cluster/database IDs with independent admin credentials.
cargo sqlx-doris admin routine status --receipt "$SQLX_DORIS_EXTERNAL_VOLUME/routine-receipt.json" --json
# Alternatively use --job-name in the administrator URL database.
# Isolate old actors/restart paths and retire the reported creator:
cargo sqlx-doris admin identity retire --name 'sqlx_mig_<32-hex-digit UUID>' --maintenance --force
cargo sqlx-doris admin routine stop --receipt "$SQLX_DORIS_EXTERNAL_VOLUME/routine-receipt.json" --json
cargo sqlx-doris admin routine stop --receipt "$SQLX_DORIS_EXTERNAL_VOLUME/routine-receipt.json" --job-id 12345 \
  --maintenance --managed-namespace --force --json
```

Stop previews by default and can discover the current ID without --job-id. Force requires an explicit job-id, maintenance and managed-namespace. Choose exactly one of --receipt/--job-name; a receipt locates and verifies work without authorizing stopping. It checks the permanent view, cluster/database, unique retained history, actual creator/target/Kafka source and exact JobId/creation identity. The creator must be retired. Rechecks precede one UUID-name STOP. UUID shape alone is insufficient: missing/corrupt reservations, wrong IDs, reused history, renamed targets and terminal jobs are rejected, without adoption or rename. Success requires the same job STOPPED. Reservations, locks and dirty/applied history stay intact. Global --ddl-timeout covers connection, reads and mutations.

<a name="doc-receipts"></a>

## Receipts, states and exits

The primary receipt is created and synced before server mutations, never overwritten. After CREATE acknowledgement and job verification, a separate new `<receipt>.confirmed` records JobId, creation time and the then-observed state/Progress. Each is limited to 1 MiB and must be a private regular external file (0600); symlinks, broad permissions, unknown formats/extra fields and mismatched cluster/database/reservation/job identities are rejected. Neither stores passwords or SQL. The confirmation is a **historical checkpoint**, never current server proof. Receipts without this sidecar still support inspection; removing it loses historical evidence and must never imply that no job exists.

All routine commands accept --json after routine or its leaf command. Controlled success/failure writes exactly one complete JSON object to stdout and leaves stderr empty, using schema `sqlx-doris-cli-result-v1`. Help/version retain normal text; signals/crashes cannot guarantee JSON. Raw server errors, SQL and Kafka properties are omitted to avoid secret disclosure. Every namespace uses the same global protocol; see [CLI results](CLI.md#doc-results).

`observation` distinguishes active (including PAUSED), terminal (STOPPED/CANCELLED), reservation_missing, reserved_unobserved, history_unavailable, invalid_binding, unconfirmed and unknown. `outcome` distinguishes observed/preview/created/stopped and invalid_input/refused/inconclusive/uncertain/acknowledged_unverified/unavailable. A reservation without visible job history is reserved_unobserved; if a confirmed JobId existed locally, it is history_unavailable. **Neither means never_created or drained**: CREATE may arrive late and history may be removed or hidden.

`phase` uses the common lifecycle states; Routine mutation `step` values include receipt_writing, receipt_persisted, reservation/create/stop submitted or acknowledged, and create_verified/stop_verified. `receipt_output` retains the requested path and last persistence checkpoint: writing does not prove a saved file; persisted follows file and parent-directory sync. Pre-creation input failures exit 2; write/sync failures exit 7. Neither proceeds to a server mutation. `job` is a snapshot labeled by job_observed_phase, not current proof after a later uncertain mutation. `last_confirmed_job` comes only from the historical sidecar. Results always include `safe_to_recover:false` and `automatic_retry:false`; terminal observations still require drain and data reconciliation.

| Exit (routine) | Meaning and next step |
| --- | --- |
| 0 | Observation/preview succeeded, or create/stop verified; not lock recovery clearance. |
| 2 | Invalid arguments, URL, receipt/source or local input; no server mutation sent. |
| 3 | Safety/generation conditions unmet, creator not retired, wrong ID or repeated terminal STOP; this STOP not sent. |
| 4 | Inconclusive metadata: missing reservation, job not yet visible or history unavailable. Preserve evidence. |
| 5 | Mutation may have been submitted without successful acknowledgement. Inspect original receipt/name/ID; never replay. |
| 6 | Mutation acknowledged, subsequent verification or confirmation file failed. Inspect on a new connection; never replay. |
| 7 | Connection/read failure or deadline before mutation submission. Maintain isolation and reobserve. |

Submitted/acknowledged phases take precedence over later file/read errors: they cannot become ordinary invalid-input failures. Scripts must interpret exit_code, outcome, phase and snapshot phase together. Never loop force or treat every nonzero exit as “nothing executed”.

<a name="doc-non-reuse"></a>

## Non-reuse boundaries

A successor must use a new create, not repurpose the old UUID through ALTER/RESUME. Within this protocol, a successor always has a different UUID: a delayed STOP can only resolve the old name. Native barriers verify successors survive disconnect, killed CLI and timeout cancellation. Lost responses and acknowledged STOP with failed verification have separate diagnostics. Inspect original name/ID on a new connection; never resend. After uncertain creation, an absent job does not prove a queued CREATE cannot arrive later. Reconcile the original receipt before starting a replacement; UUID separation neither deduplicates data nor transfers Kafka offsets.

**Doris does not consult the reservation view during Routine CREATE.** --managed-namespace attests exclusive use of sqlx_rl_/_sqlx_rl_ by every creator/tool/administrator; it is not a server ACL. A privileged raw SQL name reuse still lets delayed STOP affect that job, as a native negative test proves. Forbid bypass, reservation removal, same-name database recreation, metadata rollback/restore, ID/name reuse and endpoint switching. If those conditions cannot be maintained, do not force; keep inspection/waiting and external isolation. The CLI changes no server admission configuration.
