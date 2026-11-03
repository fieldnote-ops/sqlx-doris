<a name="doc-top"></a>

# CLI reference and diagnosis

[English](CLI.md) | [简体中文](CLI.zh-CN.md)

[Documentation](README.md)

Install through the public package as described in [Installation](INSTALLATION.md#doc-cli). Commands use DATABASE_URL or --database-url; with cli-admin, managed runners instead use --identity-file in an environment without administrator URLs. Run `cargo sqlx-doris <command> --help` for the complete argument list.

<a name="doc-contents"></a>

## Contents

- [Choose a command](#doc-commands)
- [JSON, status and stable exits](#doc-results)
- [Read-only recovery diagnosis](#doc-diagnose)
- [Diagnostic evidence and reason codes](#doc-reasons)

A basic cli installation contains only prepare, database and ordinary migrate. cli-admin enables admin lock/identity/drain/routine/diagnose and admin migrate repair; cli-admin-kafka also enables admin kafka and Kafka diagnosis arguments. Disabled capabilities are absent from help; old top-level aliases are not retained.

<a name="doc-commands"></a>

## Choose a command

| Command | Purpose |
| --- | --- |
| `prepare` | [Checked query metadata and offline cache](USAGE.md#doc-offline) |
| `database`, `migrate` | [Database lifecycle and migration files](MIGRATIONS.md) |
| `admin lock`, `admin identity`, `admin drain` | [Explicit recovery and scoped observations](RECOVERY.md) |
| `admin routine` | [Permanent UUID Routine Load protocol](ROUTINE_LOAD.md) |
| `admin kafka` | [Read-only source continuation preflight](KAFKA.md) |
| `admin diagnose` | Read-only composition described below |

<a name="doc-results"></a>

## JSON, status and stable exits

Every namespace accepts global `--json` before or after subcommands, before prepare's Cargo `--` separator. Controlled success/errors produce one `sqlx-doris-cli-result-v1` stdout object and empty stderr. Help/version remain text; a crash, force kill or stdout failure cannot promise a complete object.

Human output ends with the same status/phase/step/exit-code summary on stderr; scripts should parse JSON for owners and state. failure_category retains the pre-phase error category (unavailable when unclassified). work contains only observed entries; an empty object never proves drain.

Common fields are operation, exit_code, status, phase, step, issues, message and next_actions. Domain observations include locks/recovery_drop, identity, drain/work/target, migrations, prepare/cargo_diagnostics_file, Routine receipt/job and Kafka checkpoint/checks. No raw SQL/server errors or credentials are echoed. JSON prepare saves Cargo diagnostics to a private external file and returns its location.

| Exit | Meaning |
| --- | --- |
| 0 | Command completed within its stated scope; no drain, continuation or takeover certification. |
| 2 | Invalid arguments, URL, configuration or input. |
| 3 | Observed blocking/binding/checksum conflict, or failed Cargo compilation check. |
| 4 | Inconclusive/missing attestation or observation-time drift. |
| 5 | Server mutation may have been submitted without acknowledgement; inspect original objects, never replay blindly. |
| 6 | Mutation acknowledged but later work, verification or cleanup failed. |
| 7 | Connection, permission, metadata, deadline or local operation failed. |

Common phase values are read_only/local_write/submitted/acknowledged/verified; step names the exact operation, such as stop_submitted. Mutation phases override subsequent errors. Multi-step migrations can have partial completed SQL/coordination work; exits 5/6 never imply file rollback. local_write describes local files only. server_read_only excludes persistent server mutations; connection initialization still sets session options. recovery_drop.phase=not_submitted only attests no owner-partition DROP submission; main recovery may have changed coordination metadata. The driver supplies a typed recovery phase, without parsing error text.

Every result keeps safe_to_recover and atomic_snapshot false and disables automatic command replay/offset transfers/resets. automatic_retry:false concerns command replay and uncertain mutations; documented read polling, bootstrap connection fallback and lock waiting after confirmed exclusion conflicts still apply. Kafka's domain status is observation; status uses the common classification. migrate info no longer creates its table on an empty database; migrations holds observed history and human output also compares source entries.

migrate and diagnose --table share the driver parser for quoted/current-database-qualified state tables, for example ``--table '`app`.`schema_history`'``. Other databases, SQL fragments and reserved lock tables are rejected.

<a name="doc-diagnose"></a>

## Read-only recovery diagnosis

`diagnose` composes exact lock owners, migration history/optional source checksums, retired identity/sessions, database transactions and scoped async work, plus optional Routine receipt and independent Kafka preflight. It never creates metadata tables, retires users, cancels jobs, repairs history, recovers locks or replaces jobs. Use a separate administrator URL; --identity-file and mutation/maintenance flags are rejected.

Before copying these examples, set OLD_ID to the verified old identity, RECEIPT to its original private receipt, KAFKA_CONFIG to the private Kafka configuration, BEFORE to the trusted earlier checkpoint, and REPORT to a new external report path. Supply only the optional groups you intend to inspect; offsets are explicit reconciled candidates.

```sh
cargo sqlx-doris admin diagnose --retired-identity "$OLD_ID" --source migrations --json
cargo sqlx-doris admin diagnose --receipt "$RECEIPT" --retired-identity "$OLD_ID" --json
cargo sqlx-doris admin diagnose --receipt "$RECEIPT" --retired-identity "$OLD_ID" \
  --connection-file "$KAFKA_CONFIG" --topic events --checkpoint "$BEFORE" \
  --offset 0=123 --offset 1=456 --timeout 30 --json
```

Receipts select the database and reuse cluster/database/reservation/JobId validation. A supplied identity differing from the verified creator is a conflict. Each component records its exit, observation and recording time; coverage records requested scope. If Doris cannot connect, independent Kafka reads can still proceed within the overall deadline. Aggregate precedence is 2,7,3,4,0; individual conflicts remain visible even when an unavailable component dominates the aggregate. Components skipped after prior failure/deadline are explicitly not_observed.

Missing identity yields 4 because credentials/sessions are unattested. A present identity, active work, dirty history or source mismatch yields 3. Optional source/receipt/Kafka scope is never invented; Kafka requires its complete explicit input group. Held locks are reported, never acquired or released. Lock/history drift across database observations yields 4. Deadlines/read failures preserve partial results and yield 7.

Exit 0 never authorizes recovery. Sequential component times are not an atomic snapshot; external actor/admission isolation and data/uncovered-work reconciliation remain necessary. Independently supplied Kafka broker/topic configuration is not certified as the Routine source: kafka_source_bound_to_routine remains false. Candidates are administrator-confirmed, never derived from Progress. Complete history, payloads, business consistency and complete backend drain remain unverified. Recovery commands perform fresh checks and never accept this report as a token.

<a name="doc-reasons"></a>

## Diagnostic evidence and reason codes

Human output includes dirty versions, checksum differences, component states and readable next steps. Child progress is suppressed during observation; server text is JSON-escaped. At most 20 active items are expanded; JSON `work` retains the observed list. Migration `observation.pending_versions` lists source up versions without history and does not claim execution.

Completed diagnostic issues contain `code/component/exit_code/status/message/context/next_action` and also appear under their component. Independent conflicts remain visible. Early input issues may contain only `code/message`. `read_only_commands` supplies argv suggestions without URLs/credentials; reuse the original administrator connection. Non-UTF-8 path suggestions are omitted. Nothing is executed automatically.

| Stable reasons | Meaning |
| --- | --- |
| `migration_dirty` | Inspect `context.version`, actual schema and committed data before explicit repair |
| `migration_checksum_mismatch`, `migration_source_missing` | Compare original checksums or restore original up source; preserve history |
| `migration_history_invalid`, `migration_table_ambiguous` | History metadata or table identity unconfirmed |
| `migration_table_invalid`, `migration_source_invalid` | Correct table/source input, including duplicate up versions |
| `identity_present`, `identity_not_supplied` | Original identity still exists or was not supplied; no lock ownership proof |
| `work_active` | Inspect observed work and terminal states; absent rows do not cover hidden work |
| `lock_held`, `migration_table_absent` | Informational exit 0: owner observed or history table absent without creation |
| `observation_changed` | Owners/history changed; externally stop concurrent administrators before another read |
| `routine_active`, `routine_unverified`, `routine_identity_mismatch` | Job active, binding unconfirmed or creator differs; inspect original receipt without replaying CREATE/STOP |
| `kafka_preflight_failed` | Inspect Kafka component `observation.issues/checks`; preserve candidates and earlier checkpoint |
| `diagnosis_deadline_expired`, `component_not_observed` | Deadline/prior failure left checks incomplete; partial evidence retained |
| `database_unavailable`, `lock_metadata_unavailable`, `lock_recheck_unavailable`, `migration_history_unavailable`, `migration_history_recheck_unavailable`, `drain_unavailable` | Connection/named metadata stage incomplete; check endpoint, TLS, permissions and topology |
| `diagnosis_input_invalid`, `administrator_connection_required`, `identity_input_invalid`, `receipt_input_invalid`, `receipt_unavailable` | Correct inputs or inspect original receipt, permissions and external storage |

Reasons come from typed failures and named stages, never server-error text matching. Issues may include informational exit-0 entries; use process and issue exit codes, not an empty-array test.

```sh
diagnose_exit=0
cargo sqlx-doris admin diagnose --receipt "$RECEIPT" --retired-identity "$OLD_ID" --json > "$REPORT" || diagnose_exit=$?
jq '.issues[] | {component, code, exit_code, context, next_action}' "$REPORT"
jq '.read_only_commands' "$REPORT"
```
