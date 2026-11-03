<a name="doc-top"></a>

# Release notes

[English](CHANGELOG.md) | [简体中文](CHANGELOG.zh-CN.md)

<a name="doc-contents"></a>

## Contents

- [0.2.0 (2026-11-03)](#doc-v0-2-0)

<a name="doc-v0-2-0"></a>

## 0.2.0 (2026-11-03)

- Standalone SQLx Doris identity and doris:// URLs. Remove bundled sqlx-mysql, dual backends, compatibility patches and the Client query wrapper.
- Five aligned packages: public sqlx-doris, core driver/migration traits, compile-time macros, ops workflows and CLI interaction/prepare. Applications declare no separate internal packages.
- Use upstream SQLx queries, pools, FromRow, Migrator and migrate! directly. No generic SQLx re-exports. Enable operations/stream-load only when needed.
- Six checked query macros, conservative type/nullability inference, renamed dependencies and offline metadata. Prepare respects application Cargo configuration, atomically replaces caches, releases file locks and retains private external diagnostics.
- Preserve precise types, safe bindings and Doris table semantics. Cancelled/unread/uncertain work and dropped transactions retire connections, without automatic write replay.
- Select basic CLI with cli, Doris administration with cli-admin, and independent Kafka observations with cli-admin-kafka. Advanced commands live under admin; no old-path aliases. Doris-only diagnosis compiles without Kafka, and basic migration info shares read-only history without maintenance HTTP.
- Read an ordinary-load label before await, observe label state without writes/retries, and configure common ingestion options explicitly. Shared byte buffers avoid full-payload redirect copies; file bodies pin one opened generation with bounded reads. Immutable input remains the caller's responsibility.
- Daily-use bilingual ingestion/troubleshooting, type/session/MV recipes, isolated documentation consumers, scoped live groups, validated analytical/ingestion workloads and three CLI installation profiles.
- Stream Load schema conversion preserves duplicate MAP entries and rejects duplicate top-level fields. Missing/unknown actual Group Commit modes yield Unconfirmed; requested modes never certify visibility.
- Persist migration dirty/checksum state and wait for async DDL. Main/transition locks use UUID owner partitions and remove only the exact owner; delayed commands, disconnects and cancellation never cause automatic takeover.
- Explicit recovery distinguishes no submission, uncertain submission and acknowledged-but-unverified outcomes. Cooperative owner checks cannot atomically fence old business SQL; external exclusive maintenance and data reconciliation remain required.
- Dedicated one-time migration identities and private credentials, explicit retirement and scoped drain across sessions, transactions, schema jobs, LOAD, Routine Load and async MV schedulers/tasks. Previously admitted work and committed data still require reconciliation.
- Explicit exact ROLLUP/MV-task cancellation, registered BROKER LOAD transaction-ID abort and Routine STOP under permanent UUID non-reuse. Preview first, explicit maintenance, no retries; document missing server generation fences.
- Receipt location/confirmation retains mutation and snapshot phases. Kafka snapshot/preflight checks cluster/topic IDs, partitions, ranges and retained epochs, without certifying consumption or transferring/resetting offsets.
- One versioned CLI JSON protocol, stable statuses/reason codes/exits. Read-only diagnose composes locks/history, identity/drain, receipts and optional Kafka, preserving independent conflicts, partial evidence and coverage gaps.
- Shared history, private file and per-call observation implementations; remove global command state. Improve macro bodies, control flow, indentation and templates. Formatting gates include Rust, standalone fixtures and Python.
- Real Kafka partition/change/expiry/truncation and STOP/commit data contracts, reproducible randomized tests, sustained workloads, five-package installation and fresh application acceptance.
- All builds, caches, Doris/Kafka data and intermediate artifacts stay external. Current verified baseline: Doris 4.1.5; no older-version support.
- Publish `sqlx-doris` 0.2.0 with `sqlx-doris-core`, `sqlx-doris-macros`, `sqlx-doris-ops` and `sqlx-doris-cli` on crates.io. Install with `cargo add sqlx-doris@0.2.0`; no patch section, separate internal package or 0.1.0 `sqlx-doris-mysql` dependency is declared.
