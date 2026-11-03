<a name="doc-top"></a>

# Migration recovery runbook

[English](RECOVERY.md) | [简体中文](RECOVERY.zh-CN.md)

[Documentation](README.md)

Specialist opt-in: install the `cli-admin` feature of `sqlx-doris`; commands live under `cargo sqlx-doris admin`. Everyday queries and Stream Load do not require it.

Use a separate administrator connection and stop new deployments before starting. Diagnostics are observations, never recovery clearance. Keep all old migration/unlock/recovery actors and restart paths stopped; serialize administrators across machines.

<a name="doc-contents"></a>

## Contents

- [Choose the next action](#doc-sequence)
- [Dedicated migration identity](#doc-identity)
- [Observe and wait for drain](#doc-drain)
- [LOAD termination boundaries](#doc-load)
- [MV task cancellation and scheduling](#doc-mv-tasks)
- [Recover exact owners and repair history](#doc-locks)
- [Isolation and coverage limits](#doc-limits)

<a name="doc-sequence"></a>

## Choose the next action

1. Run [read-only diagnose](CLI.md#doc-diagnose) and retain the original receipt, owners and history.
2. Correlate the old runner; isolate its processes, restart paths and dedicated credentials.
3. Use retirement and drain observations below; reconcile actual data/schema, admitted statements and uncovered jobs.
4. If the transition gate is held, preview and explicitly remove its exact owner only after reconciliation. Reobserve main ownership.
5. Explicitly recover the current main owner, then repair a reconciled dirty version if necessary.
6. Verify state before a new runner. Never blindly retry CREATE/STOP/DROP or uncertain migration SQL.

`lock status` is read-only. **Only `recover-transition` has a preview; main `lock recover` is a mutation: `--force` confirms execution.** This runbook also supplies `--maintenance`; the CLI requires it when `--retired-identity` is present. **Do not use --force for inspection.** Account deletion, STOP acknowledgement and empty work lists do not prove all writes ended.

<a name="doc-identity"></a>

## Dedicated migration identity

identity create/status/retire manage a generated, non-reusable `sqlx_mig_<UUID>`@'%' account. Automated handling is limited to **Doris 4.1.5, one live master FE, built-in authentication_type=default and access_controller_type=default**. Multi-FE propagation, external authentication/authorization and unknown versions fail closed before changes. This is not a cluster-wide isolation claim for unsupported deployments.

Creation grants only SELECT/LOAD/CREATE/ALTER/DROP on the selected internal database, without ADMIN/GRANT. Keep administrator credentials/process permissions separate; the old instance must have no alternate writable identity. --identity-file rejects DATABASE_URL, DORIS_TEST_URL and --database-url alongside the file. Use one exclusive identity per runner/deployment instance; successors get new names. Never share credentials or recreate/regrant retired names. Cross-database/catalog migrations and extra privileges are outside this policy.

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
export DATABASE_URL='doris://administrator:password@doris-fe:9030/app'
credentials="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/identities/deployment.json"
cargo sqlx-doris admin identity create --output "$credentials"
env -u DATABASE_URL -u DORIS_TEST_URL cargo sqlx-doris --identity-file "$credentials" migrate --source migrations run
# Runner environments must contain no administrator secrets or fallback URLs.
cargo sqlx-doris admin identity status --name 'sqlx_mig_<32-hex-digit UUID>'
cargo sqlx-doris admin identity retire --name 'sqlx_mig_<32-hex-digit UUID>'
cargo sqlx-doris admin identity retire --name 'sqlx_mig_<32-hex-digit UUID>' --maintenance --force
cargo sqlx-doris admin lock status
```

This password-based workflow rejects inherited administrator ssl-cert/ssl-key material during creation and credential loading; CA/server verification settings remain supported. Credentials stay in a new external JSON file with Unix mode 0600, without password output. Existing files, symlinks, loose permissions and internal paths are rejected. Applications can read database_url and use native SQLx/Doris options. The file is synced before CREATE/GRANT; interruption can leave a partially provisioned identity. Inspect status rather than retrying automatically. Never commit or log the file. Account administration requires a separate administrator URL and rejects --identity-file.

Disable new deployments/runners before isolation and keep that entry closed through reconciliation. Retirement previews by default; --force requires --maintenance. It validates the generated name, database/cluster marker, unique host identity and restricted grants. Named roles, elevated/cross-database grants, host variants, certificate constraints and unknown metadata are rejected. Shared users are never automatically revoked. Account administration must be serialized: DROP USER has no generation-conditioned CAS, and retired names must never be reused. Previewed lock owners provide context, not proof of account-to-lock ownership; correlate the old deployment's logs.

A verified DROP USER removes the selected identity and checks absence on that FE. Native tests show old sessions' later raw/cached writes are denied and old credentials cannot reconnect. Direct REVOKE alone still allows login and inherited roles can authorize writes. Acknowledged DROP with failed account/environment verification is reported separately. Lost acknowledgements remain uncertain; use a new connection to inspect status, without recreating or blindly retrying a name.

**Retirement never removes locks/history or proves old work drained.** Previously admitted statements may finish; a transaction written before deletion can still COMMIT afterward, and submitted ALTER jobs continue. Stop old processes/restart paths, reconcile transactions/jobs/data, then explicitly recover exact lock owners and dirty state. There is no automatic takeover, transaction cancellation or replay.

See [built-in authorization](https://doris.apache.org/docs/4.x/admin-manual/auth/authorization/internal/), [REVOKE](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/account-management/REVOKE-FROM/) and [DROP USER](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/account-management/DROP-USER/).

<a name="doc-drain"></a>

## Observe and wait for drain

Use `drain status/wait` with a separate administrator URL on the verified single-FE, Doris 4.1.5, built-in-RBAC, shared-nothing topology (deploy_mode empty/local and cloud_unique_id empty). Shared-storage and unknown deployments fail closed. Drain/CLI recovery currently require a non-system ASCII database identifier. Correlate the managed name with that database's old deployment credentials/logs; absence alone does not attest provenance. Recreated names and unexpected identity metadata fail closed.

| Scope | Blocking conditions and terminal states |
| --- | --- |
| All old-name PROCESSLIST sessions | Includes idle sessions and sessions using other databases. Close them at their source; no automatic numeric-ID KILL. |
| ALL current-database running transactions | User ownership is unavailable, so unrelated users' transactions also block. PREPARE, PRECOMMITTED and COMMITTED are nonterminal; only VISIBLE/ABORTED are terminal. No arbitrary transaction abort; only the explicitly bound BROKER LOAD exception below is supported. |
| Current-database COLUMN/ROLLUP jobs | FINISHED/CANCELLED are terminal. |
| Current-database jobs visible through SHOW LOAD | PENDING/ETL/LOADING/COMMITTED block; FINISHED/CANCELLED are terminal. Legacy pending/loading registry counters are checked too, protecting against some omitted/unrenderable rows. |
| SHOW ALL ROUTINE LOAD | NEED_SCHEDULE, RUNNING and PAUSED block; only STOPPED/CANCELLED are terminal. Paused jobs can resume and dispatched tasks may continue. |
| Async MV schedulers/tasks | Pair mv_infos/jobs/tasks by database and exact MvId/JobId. Running ON COMMIT/scheduled producers block even without tasks. ExecuteType=MANUAL alone is insufficient: ON COMMIT uses it too. A manual-only idle producer or PAUSED/STOPPED/FINISHED scheduler is currently inactive; tasks must report SUCCESS/FAILED/CANCELED. |

Observed work must reach an exact terminal state or observed disabled-scheduler status. Missing history, identity changes, unknown states, the 2000-row transaction cap, permission errors and disconnects never imply completion. Transactions are read again after producers/tasks. FAILED/CANCELED does not mark a migration applied or undo committed partitions; dirty history stays unchanged.

status exits nonzero when blocked. wait polls once per second; `--timeout` covers connection, metadata reads and sleeps. status, cancel-load, cancel-rollup, cancel-mv-task and recovery preflight use global `--ddl-timeout` (default 300 seconds). Database/cluster/topology/identity drift also stops the command. Neither changes locks/history nor replays business SQL. An empty snapshot only describes these interfaces at read time; it is not an atomic server fence for previously admitted but unregistered statements, other job classes or future writes. `--maintenance` attests that the operator stopped old actors, restart/admission paths and other writers, serialized administration and reconciled other async work; the flag cannot perform those actions.

```sh
# Stop old actors/restarts/admission and explicitly retire first; never reuse/regrant the name.
cargo sqlx-doris admin drain status --name 'sqlx_mig_<32-hex-digit UUID>'
cargo sqlx-doris admin drain wait --name 'sqlx_mig_<32-hex-digit UUID>' --timeout 300 --maintenance
# Separately reconcile data/schema, Stream Load/COPY, generic scheduled INSERT,
# EXPORT, backup/restore, index-build and other uncovered work.
cargo sqlx-doris admin lock recover-transition --owner '<gate UUID>' --lock-owner '<main UUID or unlocked>' --retired-identity 'sqlx_mig_<32-hex-digit UUID>' --maintenance --force
cargo sqlx-doris admin lock recover --owner '<main UUID>' --retired-identity 'sqlx_mig_<32-hex-digit UUID>' --maintenance --force
```

Every CLI lock recover/recover-transition performs fresh transaction and documented-job checks, including transition previews. Missing privileges, unsupported deployments, metadata failures and blockers stop before recovery DROP. `--retired-identity` additionally verifies name retirement and old sessions; without it, session/credential isolation is explicitly unattested. A prior wait is not a reusable takeover permit. There is no automatic wait, retirement or lock takeover. Maintenance must remain active across the non-atomic final snapshot-to-DROP interval. Core recovery APIs remain explicit maintenance primitives requiring external drain; they do not promise the CLI's job diagnostics.

To terminate one attributed old ROLLUP job explicitly:

```sh
cargo sqlx-doris admin drain cancel-rollup --name 'sqlx_mig_<32-hex-digit UUID>' --table events --job-id 12345
cargo sqlx-doris admin drain cancel-rollup --name 'sqlx_mig_<32-hex-digit UUID>' --table events --job-id 12345 --maintenance --force
```

The first command previews. Execution sends one exact-JobId CANCEL and polls that job to a terminal state. Missing/mismatched/already-terminal jobs are rejected before sending; an old ID cannot cancel a successor on the table. FINISHED can win the race with cancellation, so verify actual schema rather than assuming rollback. Lost acknowledgements, process cancellation and deadlines may leave an already-submitted CANCEL completing later; inspect on a new connection, without blind retries. Cancellation does not confirm database drain or recover locks/history.

<a name="doc-load"></a>

## LOAD termination boundaries

**A registered BROKER LOAD can be terminated through its exact transaction**, without label-based CANCEL LOAD. Preview first and execute with the same JobId/TransactionId:

```bash
export DORIS_HTTP_URL='http://doris-fe:8030'
cargo sqlx-doris admin drain cancel-load --name 'sqlx_mig_<32-hex-digit UUID>' --job-id 12345
cargo sqlx-doris admin drain cancel-load --name 'sqlx_mig_<32-hex-digit UUID>' --job-id 12345 --transaction-id 67890 --maintenance --force
```

Read-only preview needs no HTTP address; execution requires DORIS_HTTP_URL or explicit `--http-url`. Execution requires BROKER, PENDING/LOADING, a positive registered transaction, and matching transaction ListenerId=JobId, label, BATCH_LOAD_JOB source and PREPARE/PRECOMMITTED status. Binding is checked again before sending. The public `_stream_load_2pc` request carries only `txn_id` and `txn_operation:abort`; no label, automatic redirects or retries. The 4.1.5 transaction manager atomically changes that exact transaction and invokes its LOAD callback. A delayed request cannot select a new same-label job after CLEAN LABEL. If commit wins, abort is rejected and committed data remains. Success additionally requires the same transaction ABORTED and job CANCELLED. See the [public transaction interface](https://doris.apache.org/docs/4.x/data-operate/transaction/), [4.1.5 transaction manager](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/transaction/DatabaseTransactionMgr.java) and [LOAD callback](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/LoadJob.java). BROKER support is implementation-specific and natively tested on this release, not promised for other releases.

This path supports only shared-nothing Doris 4.1.5 with built-in authentication/authorization, one master FE and one live BE. Password authentication comes from the independent SQL administrator URL, not DORIS_HTTP_USER/PASSWORD; SQL client certificates/keys are not inherited. The FE origin must have no credentials/path/query. HTTP FE metadata must match the SQL FE name, ClusterId, ports, version, start time and other identity fields. The redirect must address the registered SHOW BACKENDS node and exact path. After node/binding rechecks, one BE PUT is sent, discarding Location credentials and authenticating independently. The registered BE address must be reachable; there is no arbitrary BE override. SQL TLS requirements also require HTTPS; `ssl-ca` supplies the HTTP CA, without disabling HTTP certificate verification. Container/proxy deployments need trusted, stable routing.

`--maintenance` also attests no cluster switching, endpoint rebinding, metadata restore or ID reuse. HTTP identity reads are not atomic with abort and cannot protect against an endpoint being replaced afterward. Already submitted requests may finish after disconnect, timeout or CLI exit. Missing acknowledgements and acknowledged aborts with failed verification are distinguished: inspect original IDs on a new connection, never fall back to names or blindly resend. Locks/dirty history stay intact. Transaction termination does not prove all BE resources are cleaned up; rerun drain wait and reconcile other work/data before explicit recovery.

Unregistered LOAD (TransactionId=0), SPARK/INSERT, ordinary SQL/Stream Load transactions and other sources remain rejected. Ordinary/manually created Routine Load remains inspection/wait only: STOP resolves a reusable name without a JobId condition or atomic rename. The zero Routine quota checks `count > limit`, still admitting the first job. CREATE and STOP both require table LOAD privileges, so privilege removal cannot independently disable creation while retaining stopping. The CLI changes no global quotas. See [STOP syntax/permissions](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/load-and-export/STOP-ROUTINE-LOAD/) and the [4.1.5 manager](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadManager.java).

<a name="doc-mv-tasks"></a>

## MV task cancellation and scheduling

One MV task can be cancelled explicitly using its parent generation and TaskId:

```sh
cargo sqlx-doris admin drain cancel-mv-task --name 'sqlx_mig_<32-hex-digit UUID>' --view summary --job-id 12345 --task-id 67890
cargo sqlx-doris admin drain cancel-mv-task --name 'sqlx_mig_<32-hex-digit UUID>' --view summary --job-id 12345 --task-id 67890 --maintenance --force
```

Read JobId/TaskId from the `MV job ... task ...` blocking output and use that target view (ASCII identifiers currently). The first command previews. Execution rechecks parent/view/task identity and sends one public CANCEL TASK using the verified generated inner_mtmv_<MvId> parent name rather than resolving a reusable view name at execution. This public generic statement was verified with MV internal jobs on 4.1.5, without inferring support on other versions. Maintenance must forbid spoofing/reusing generated job names. Missing/replaced parents, mismatched/terminal tasks and changed targets are rejected. TaskIds derive from a process clock and randomness, are not monotonic and have no verified never-reuse guarantee across restart. Ordering/newest-row selection cannot establish attribution; parent-generation protection does not prove atomic safety under TaskId collision/reuse. Reinspect complete identity after faults/restarts rather than replaying old IDs. See the [cancellation handler](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/job/manager/JobManager.java).

Lost acknowledgements, killed processes and deadlines can leave submitted CANCEL completing later, without automatic replay. An acknowledged CANCEL still needs an exact observed task terminal state; removed history/views or read failures remain unconfirmed. SUCCESS may win the race, while FAILED/CANCELED cannot establish that nothing was written. Committed partitions remain, and reported terminal metadata is not a complete backend-drain barrier.

Cancelling a task does not disable commit/scheduled refresh. Scheduler isolation requires separate exclusive maintenance, for example Doris PAUSE MATERIALIZED VIEW JOB. That operation resolves a reusable view name, without an MvId condition; generic PAUSE JOB rejects internal MV jobs. The CLI does not present either as generation-safe pause. Paused MVs still accept explicit REFRESH. Prevent actors from refreshing, resuming or reconfiguring them; no persistent pause guarantee across restart is claimed. Recheck after connection/restart/configuration changes. See [MV scheduler handling](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/mtmv/MTMVJobManager.java).

The scope remains limited: SHOW LOAD excludes some Stream Load/COPY and generic scheduled INSERT work; EXPORT, backup/restore and index building need separate reconciliation. Public lists can omit removed/orphaned/retained-out or otherwise hidden server records. Visible errors and disappearance of observed work fail closed, but a first snapshot cannot exclude already-hidden work or admitted, unregistered commands. Checks cover the current database and never infer account-to-job/lock ownership or a global write fence.

**No COLUMN cancellation is exposed.** The [4.1.5 SchemaChangeHandler](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/alter/SchemaChangeHandler.java) ignores CANCEL's JobId and selects the table's current job; native tests show a nonexistent ID still cancels it. A prior ID check cannot protect a successor, so the CLI only waits for COLUMN. The [ROLLUP handler](https://github.com/apache/doris/blob/917b20be838/fe/fe-core/src/main/java/org/apache/doris/alter/MaterializedViewHandler.java) selects exact IDs. See [SHOW TRANSACTION](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/transaction/SHOW-TRANSACTION/) and [load transactions](https://doris.apache.org/docs/4.x/key-features/load-transaction/) for transaction terminal/visibility semantics. End ordinary uncommitted SQL transactions at their original session, verify after disconnection, or wait for server expiry. Retirement, recovery and job cancellation never undo committed data.

<a name="doc-locks"></a>

## Recover exact owners and repair history

Use owners from fresh lock status, not old logs. The transition preview requires both the gate and main owners (or unlocked). Execution requires maintenance and force. Main recover has no preview: --force confirms execution. This runbook also supplies --maintenance, and --retired-identity requires it. Main recover always performs fresh scoped drain checks; --retired-identity additionally verifies identity absence/sessions. Unknown layout, wrong/missing/changed owners or nonterminal work block recovery. Neither command automatically repairs dirty/applied history.

```sh
cargo sqlx-doris admin lock status
cargo sqlx-doris admin lock recover-transition --owner '<transition-owner UUID>' --lock-owner '<main-lock UUID>'
# Use --lock-owner unlocked only when lock status shows no main owner.
cargo sqlx-doris admin lock recover-transition --owner '<transition-owner UUID>' \
  --lock-owner '<main-lock UUID>' --maintenance --force
cargo sqlx-doris admin lock status
cargo sqlx-doris admin lock recover --owner '<current main-owner UUID>' --maintenance --force
cargo sqlx-doris admin migrate repair --version 123 --action mark-applied --force
# Alternative after reconciliation: --action remove permits the next run to execute the file.
```

Repair changes only the current dirty version metadata and does not execute SQL. mark-applied records its up-source checksum; remove makes the next run execute the file. Establish replay safety first; a failed down migration must be reconciled against actual down effects, not blindly marked as applied. `database drop --force` deletes the entire URL database.

DROP outcomes differ: not_submitted means no owner DROP sent; uncertain means no successful acknowledgement and requires inspection; acknowledged with failed verification means retain successors and reobserve. Main recovery may have changed coordination metadata even before owner DROP. Once owner removal starts, its connection is retired on success, failure or cancellation. Submitted commands can finish after process/future cancellation. Never replay them blindly.

<a name="doc-limits"></a>

## Isolation and coverage limits

UUID partition names protect successor lock partitions from delayed DROP; they do not fence old business SQL. KILL QUERY stops only a query; KILL CONNECTION does not prevent reconnects. Numeric IDs are FE-local and restart-reusable, so the CLI never automatically kills persisted IDs or treats PROCESSLIST absence as takeover proof.

Identity retirement is verified only on shared-nothing Doris 4.1.5, one master FE with built-in auth. Admitted transactions may still COMMIT and submitted jobs may finish. Drain lists are non-atomic and scoped; removed/orphaned work, unsupported jobs, restores/rebinding and unregistered commands remain unconfirmed. See [Compatibility](COMPATIBILITY.md).
