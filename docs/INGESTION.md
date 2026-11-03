<a name="doc-top"></a>

# Batch ingestion

[English](INGESTION.md) | [简体中文](INGESTION.zh-CN.md)

[Documentation](README.md)

Use Stream Load for application batches and file uploads; keep ordinary small SQL writes on SQLx. This guide targets an existing table and needs no schema-management privileges.

<a name="doc-contents"></a>

## Contents

- [Choose a write path](#doc-choose)
- [Configure a trusted HTTP client](#doc-setup)
- [Import one JSON batch](#doc-json)
- [Interpret the receipt](#doc-result)
- [Upload immutable files](#doc-files)
- [Handle cancellation and unknown outcomes](#doc-uncertain)
- [Group Commit and 2PC boundaries](#doc-group-commit)

<a name="doc-choose"></a>

## Choose a write path

Use bound INSERT for small writes or explicit Doris write transactions. For larger application batches, choose Stream Load. High-frequency small batches may benefit from Group Commit; asynchronous acknowledgement and visibility are different events. Kafka continuous ingestion belongs to Doris Routine Load or an existing connector, rather than a driver-owned Kafka consumer.

Duplicate, Unique and Aggregate tables have different write semantics. Unique full-row UPSERT may replace omitted columns with defaults or NULL; partial updates require the server's explicit partial-update settings. A Unique table hiding duplicate keys does not prove a transport was consumed exactly once. See [Doris data models](https://doris.apache.org/docs/4.x/key-features/data-model/).

<a name="doc-setup"></a>

## Configure a trusted HTTP client

Enable stream-load on the same public Doris package. Configure [external build paths](INSTALLATION.md#doc-storage); upstream SQLx features remain application choices.

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
sqlx-doris = { path = "../sqlx-doris-clean/crates/sqlx-doris", features = ["stream-load"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Construct StreamLoadClient::new with the trusted FE HTTP endpoint, username and optional password. Reuse this HTTP client. SQL and HTTP endpoint ports differ. with_client_builder configures TLS roots/timeouts; backend_endpoint is an explicit trusted private/container BE mapping. Neither endpoint implies automatic failover or business retry.

<a name="doc-json"></a>

## Import one JSON batch

Create the events table and grant the appropriate load permission beforehand. Before calling this function, durably record an application batch ID and the identity/digest of its immutable payload. Supply that stable ID as batch_id; do not generate a new ID to retry an uncertain attempt. This function submits once and returns the complete receipt.

```rust
use sqlx_doris::{
    operations::{Result, TableName},
    stream_load::{LoadReceipt, StreamLoadClient},
};

pub async fn import_events(
    client: &StreamLoadClient,
    database: &str,
    batch_id: &str,
) -> Result<LoadReceipt> {
    let table = TableName::new(database, "events");
    client
        .load(&table)?
        .label(batch_id)
        .send_json(&[serde_json::json!({"id": 42, "name": "Doris 中文🙂"})])
        .await
}
```

For a generated ordinary-load label, request.label_ref() is available before await; durably retain it with the batch. Group Commit returns None because its label is assigned/shared by the server. columns, partial_columns, strict_mode and max_filter_ratio are explicit conveniences; columns accepts trusted Doris expressions, not bound values.

For ARRAY/MAP/STRUCT JSON conversion, load_table accepts a TableDefinition describing the existing table; it does not create that table. Raw send_json uses the supplied JSON shape. Verify the schema and returned values, especially arbitrary strings in complex types.

<a name="doc-result"></a>

## Interpret the receipt

Always inspect state(), row counts and the application batch identity together. HTTP success alone is insufficient.

| LoadState | Meaning / action |
| --- | --- |
| Visible | Server reports publication; verify loaded, filtered and unselected counts |
| Accepted | Async Group Commit accepted the data; read visibility separately |
| Committed | Publish Timeout: committed, possibly not yet visible; do not resend |
| Unconfirmed | Actual Group Commit mode is missing/unknown; preserve uncertainty |
| Prepared | 2PC preparation, requiring a separate external coordinator |
| AlreadyExists | Inspect finished and the original batch; this is not payload equality proof |
| Failed | Business failure returned as Error::Load with the full receipt |

Default strict_mode=true and max_filter_ratio=0 reject filtered batches. An explicitly allowed filter ratio can produce success with rejected rows; inspect number_filtered_rows and error_url without automatically downloading rejected data. number_unselected_rows describes the where filter. is_visible() is a visibility convenience, not complete business reconciliation.

<a name="doc-files"></a>

## Upload immutable files

Use send_file for file-backed streaming without buffering the whole file. send_bytes and send_json hold a complete batch in memory. There is no arbitrary Stream/AsyncRead upload API. The attempt pins one opened regular file when sending begins; a path replacement does not switch the FE/BE bodies to another file. Each body has an independent logical cursor and bounded reads. Observed length/mtime changes are rejected. Keep the underlying bytes immutable: these checks do not freeze a file, detect changes whose metadata is restored, or undo bytes already submitted.

```rust
use sqlx_doris::{
    operations::{Result, TableName},
    stream_load::{LoadFormat, LoadReceipt, StreamLoadClient},
};
use std::path::Path;

pub async fn import_parquet(
    client: &StreamLoadClient,
    database: &str,
    batch_id: &str,
    path: &Path,
) -> Result<LoadReceipt> {
    client
        .load(&TableName::new(database, "events"))?
        .label(batch_id)
        .format(LoadFormat::Parquet)
        .send_file(path)
        .await
}
```

```rust
use sqlx_doris::{
    operations::{Result, TableName},
    stream_load::{LoadFormat, LoadReceipt, StreamLoadClient},
};
use std::path::Path;

pub async fn import_csv(
    client: &StreamLoadClient,
    database: &str,
    batch_id: &str,
    path: &Path,
) -> Result<LoadReceipt> {
    client
        .load(&TableName::new(database, "events"))?
        .label(batch_id)
        .format(LoadFormat::Csv)
        .columns("id,name")?
        .header("column_separator", ",")?
        .send_file(path)
        .await
}
```

For CSV choose LoadFormat::Csv and explicit column_separator/columns headers. JSON lines use LoadFormat::JsonLines; ordinary JSON arrays use LoadFormat::Json. Parquet/ORC bytes are sent without client-side decoding.

<a name="doc-uncertain"></a>

## Handle cancellation and unknown outcomes

A transport failure or canceled future may occur after server commit. Retain the original batch ID and immutable input; inspect the original label/transaction and business data through read-only interfaces. Error::Transport, LoadSource and LoadRedirect retain an ordinary-load label when available. Canceling a future cannot return a receipt, so record identity before await. Do not resend automatically, invent a replacement label or infer failure from missing history.

observe_label performs one authenticated FE GET without redirects/retries and returns LabelState::Unknown/Prepare/Precommitted/Committed/Visible/Aborted/Other. It neither sends a new load nor commits/aborts one. Permission, protocol and local failures return errors, never Unknown. Unknown is inconclusive, including after history expiry. This endpoint does not bind a label to an immutable payload, database generation or write authorization; keep the original batch evidence.

```rust
use sqlx_doris::{
    operations::Result,
    stream_load::{LabelState, StreamLoadClient},
};

pub async fn observe_batch(
    client: &StreamLoadClient,
    database: &str,
    original_label: &str,
) -> Result<LabelState> {
    Ok(client.observe_label(database, original_label).await?.state)
}
```

Doris Stream Load history recording is not enabled by default and may expire or be unavailable. An empty SHOW STREAM LOAD result is not proof of no commit. An existing FINISHED label proves that historical label completed, not that a newly supplied payload was identical. See [troubleshooting](TROUBLESHOOTING.md#doc-ingestion) and [official history requirements](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/load-and-export/SHOW-STREAM-LOAD/).

<a name="doc-group-commit"></a>

## Group Commit and 2PC boundaries

group_commit(GroupCommit::Sync/Async/Off) selects the HTTP mode; DorisConnectOptions.group_commit configures each new SQL connection separately. Requested mode is evidence, not proof of actual mode or visibility. Async acceptance does not guarantee immediate visibility. Group Commit uses a server-assigned shared label and cannot be combined with an explicit application label in this client.

Raw headers remain available for Doris options. This client can represent a Prepared receipt but does not implement a complete 2PC commit/abort coordinator or an exactly-once checkpoint sink. Do not enable two_phase_commit without an independent coordinator. See [Group Commit](https://doris.apache.org/docs/4.x/key-features/group-commit/) and [load transactions](https://doris.apache.org/docs/4.x/key-features/load-transaction/).
