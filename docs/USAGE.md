<a name="doc-top"></a>

# Queries, types and offline builds

[English](USAGE.md) | [简体中文](USAGE.zh-CN.md)

[Documentation](README.md)

Start with the [installation guide](INSTALLATION.md). Use upstream SQLx for generic APIs and this driver for Doris types and checked queries.

<a name="doc-contents"></a>

## Contents

- [Queries and pools](#doc-queries)
- [Per-connection session settings](#doc-sessions)
- [Transactions](#doc-transactions)
- [Types and operations](#doc-types)
- [Stream Load](#doc-stream-load)
- [Async MV refresh and observation](#doc-mv)
- [Checked query macros](#doc-macros)
- [Prepare and offline builds](#doc-offline)
- [Error types](#doc-errors)

<a name="doc-queries"></a>

## Queries and pools

Pass a native `DorisPool` to upstream `sqlx::query`, `query_as`, `query_scalar`, `raw_sql` or `QueryBuilder`. Use `.bind()` for values. Struct mapping uses upstream `#[derive(sqlx::FromRow)]`; tuples work directly. The complete [quickstart](../README.md#doc-quickstart) is ready to copy. SQLx 0.9 dynamic SQL uses `sqlx::AssertSqlSafe`; build identifiers with trusted helpers, never concatenate data.

Each connection caches 100 statements by default. `statement_cache_capacity(0)` disables caching; `persistent(false)` disables it for one execution. `fetch` streams results. `fetch_one`/`fetch_optional` finish the remaining response before reuse; use streaming for large results.

<a name="doc-sessions"></a>

## Per-connection session settings

Use SQLx after_connect to initialize each new physical connection, including pool replacements. No additional client or per-variable options API is needed. This recipe is verified on 4.1.5 and changes sessions, not global policies or quotas.

```rust
use sqlx_doris::{DorisPool, DorisPoolOptions};

pub async fn connect_application(url: &str) -> sqlx::Result<DorisPool> {
    DorisPoolOptions::new()
        .max_connections(8)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET time_zone = '+00:00'")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET query_timeout = 30")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET workload_group = 'normal'")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
}
```

normal is the existing group used by this example; select an authorized group for your application. Do not interpolate untrusted group names/settings into SQL. query_timeout controls server SQL execution, not pool queueing, network time or the entire application operation. Existing DorisConnectOptions.group_commit, decimal256 and strict_cast still configure their dedicated connection settings. See [Doris workload groups](https://doris.apache.org/docs/4.x/key-features/workload-group/).

<a name="doc-transactions"></a>

## Transactions

Begin with pool.begin(), execute queries on `&mut *tx`, then commit() or rollback(). Doris supports top-level write transactions, with no savepoints, nested transactions or custom BEGIN. Typed bindings inside transactions use validated public text-protocol commands. Ordinary queries prefer cached binary statements.

Dropping a transaction closes its connection; Doris handles disconnect rollback. Cancellation, unread streams and uncertain completion retire connections. Pools replace them automatically. Do not replay uncertain writes; reconcile using application identifiers. DorisConnectOptions.group_commit(GroupCommit::Sync/Async/Off) applies to each new SQL connection. Async Group Commit acknowledgement does not guarantee query visibility. operations::wait_until polls reads without resubmitting writes.

This function assumes an existing events table:

```rust
pub async fn insert_event(pool: &sqlx_doris::DorisPool) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO events(id, name) VALUES (?, ?)")
        .bind(1_i64)
        .bind("Doris")
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await
}
```

<a name="doc-types"></a>

## Types and operations

Doris adapters include LargeInt, Time, BigDecimal, TimestampTz, Ipv4/Ipv6, Binary/Hex, Array, Map, Struct and Variant. Time is a signed duration supporting ±838:59:59.999999. Unsupported precision is rejected. For arbitrary strings in complex values, use operations::push_complex_value and project.

| Doris type | Rust type | Everyday constraint |
| --- | --- | --- |
| BOOLEAN / integers | bool; i8/i16/i32/i64 | Range-checked; ambiguous expression metadata may need CAST/overrides |
| LARGEINT | sqlx_doris::types::LargeInt | Signed i128; never pass through floating point |
| DECIMAL / DECIMAL256 | sqlx_doris::types::BigDecimal | Preserve decimal text and server-supported precision/scale |
| DATE / DATETIME | chrono::NaiveDate / NaiveDateTime | No time-zone identity; reject unsupported submicrosecond precision |
| TIMESTAMPTZ | sqlx_doris::types::TimestampTz | DateTime<FixedOffset>; session time zone affects presentation |
| TIME | sqlx_doris::types::Time | Signed duration ±838:59:59.999999; not an OLAP column type |
| IPv4 / IPv6 | sqlx_doris::types::Ipv4 / Ipv6 | Explicit adapters around std::net addresses |
| Binary | sqlx_doris::types::Binary / Hex | Bind Hex to explicit UNHEX(?) where needed |
| ARRAY / MAP / STRUCT | sqlx_doris::types::Array / Map / Struct | Complex string reads may require operations::project |
| JSON / VARIANT | sqlx::types::Json / sqlx_doris::types::Variant | Keep documented scalar-root and large-number server limits |
| NULL | Option<T> | Choose T from the logical result; do not force macro non-null without proof |

Declare chrono and serde_json as application dependencies when using their names. Import BigDecimal and Doris adapters directly from sqlx_doris::types. See [ingestion](INGESTION.md#doc-choose) for table-model versus label deduplication; neither proves network exactly-once.

Schema helpers include TableName, TableDefinition, Column, DataType and Duplicate/Unique/Aggregate models. Enable `operations` and pass `&DorisPool` directly to free functions such as `create_table`, `describe_table` and MV helpers; no client wrapper is needed. Server table-model, affected-row and visibility semantics remain Doris's.

Set DORIS_URL, then run the source examples:

```sh
./scripts/dev cargo run --locked --example query
./scripts/dev cargo run --locked --features operations --example native_types
```

Source examples and benchmarks remove their owned tables/databases after success. Failed operations retain the named fixture and external evidence for inspection; do not repeat uncertain writes merely to clean up.

Operation wait helpers return InvalidInput if timeout cannot be represented by the clock. An oversized interval waits only until the deadline; no new read starts after it. Zero timeout still permits one immediately completed observation. MV cancel_materialized_view_task and wait_materialized_view_task require a positive TaskId, validated before acquiring a connection. An unrepresentable migration lock timeout returns a SQLx Configuration error without creating coordination metadata.

<a name="doc-stream-load"></a>

## Stream Load

See [batch ingestion](INGESTION.md) to load JSON, CSV or Parquet/ORC files into an existing table. The guide provides minimal stream-load dependencies, copyable Rust, label/filter/visibility handling and unknown-outcome limits. send_file streams a file; send_bytes/send_json retain a complete memory batch. There is no arbitrary Stream/AsyncRead upload API or automatic write retry.

<a name="doc-mv"></a>

## Async MV refresh and observation

Enable operations and use an existing async materialized view. request_refresh acknowledges submission only; this interface does not atomically return a task identity. Correlate the intended task and parent view from trusted evidence before passing its TaskId to observation. Task IDs are not chronological; the latest/largest ID is not proof of your submission, especially with concurrent refreshes.

```rust
use sqlx_doris::{
    DorisPool,
    operations::{
        MaterializedViewTask, RefreshMode, Result, TableName,
        refresh_materialized_view, wait_materialized_view_task,
    },
};
use std::time::Duration;

pub async fn request_refresh(pool: &DorisPool, view: &TableName) -> Result<()> {
    refresh_materialized_view(pool, view, RefreshMode::Auto).await
}

pub async fn observe_known_task(
    pool: &DorisPool,
    view: &TableName,
    verified_task_id: i64,
) -> Result<MaterializedViewTask> {
    wait_materialized_view_task(
        pool,
        view,
        verified_task_id,
        Duration::from_secs(60),
        Duration::from_millis(250),
    )
    .await
}
```

Inspect the returned task status: Success, Failed and Canceled are terminal; failures/cancellation are not converted to success. A timeout neither cancels the server task nor resubmits refresh. A successful task does not reconcile the entire view's business freshness. A full schema DSL, background scheduler and automatic refresh orchestration are outside these helpers; specialist cancellation/generation checks are described in [advanced recovery](RECOVERY.md#doc-mv-tasks).

<a name="doc-macros"></a>

## Checked query macros

Enable sqlx-doris/macros and invoke checked query macros from the driver; generic APIs remain in upstream sqlx:

```rust
pub async fn checked(pool: &sqlx_doris::DorisPool) -> sqlx::Result<()> {
    let row = sqlx_doris::query!(
        "SELECT CAST(? AS BIGINT) AS `id!: i64`,CAST(? AS STRING) AS `name!: String`",
        42_i64,
        "Doris"
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(row.id, 42);
    Ok(())
}
```

Supports query!, query_as!, query_scalar! and their query_file!/query_file_as!/query_file_scalar! counterparts. SQL must be literal SQL, concatenated literals or a file; runtime SQL continues to use query/bind. query_as! constructs named fields without a FromRow implementation. Renamed dependencies work with the same APIs and inferred Doris types.

Set DATABASE_URL for online compilation. Macros PREPARE SQL and read metadata; they never execute business statements. They check SQL validity, parameter count, encodable arguments and output record types. Doris does not expose reliable parameter type metadata: parameter checking is weak, as supported by SQLx. Use explicit SQL CASTs where the server must establish a type.

Some Doris expressions report LARGEINT, complex values or unbound placeholders as STRING/BLOB. Direct table columns use native DESC metadata, fetched once per origin table, to recover logical types such as BOOLEAN and LARGEINT. Ambiguous results require SQLx column overrides, for example ``AS `wide!: sqlx_doris::types::LargeInt` `` or ``AS `items!: sqlx_doris::types::Array<i32>` `` (use SQL backticks around the complete alias). `name!` forces non-null, `name?` forces nullable, and `name: Type` selects a Rust type. Default nullability is conservative; use ! only when guaranteed. Spell out columns with AS/backtick aliases for complex projections. Metadata enrichment requires DESC access. Native descriptions retain BOOLEAN where information_schema normalizes it to TINYINT(1). Expressions without usable origins may still require explicit overrides such as : bool. Use sqlx::types for shared adapters and sqlx_doris::types for Doris logical types. The official sqlx package must be a direct dependency named sqlx with derive or macros enabled for runtime helpers, matching upstream generated-code conventions.

For ambiguous logical types, use explicit SQLx overrides:

```sql
SELECT CAST(1 AS LARGEINT) AS `wide!: sqlx_doris::types::LargeInt`,
       ARRAY(1, 2) AS `items!: sqlx_doris::types::Array<i32>`
```

<a name="doc-offline"></a>

## Prepare and offline builds

```sh
export DATABASE_URL='doris://app:secret@doris-fe:9030/app'
export SQLX_OFFLINE_DIR="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/offline/app"
cargo sqlx-doris prepare --output "$SQLX_OFFLINE_DIR"
cargo sqlx-doris prepare --check --output "$SQLX_OFFLINE_DIR"
# Workspace/features: prepare -- --workspace --all-features
SQLX_OFFLINE=true cargo check
```

Keep CARGO_TARGET_DIR, CARGO_BUILD_BUILD_DIR, CARGO_HOME and TMPDIR external too. Offline JSON uses SQLx query hashes and Describe data with database name Doris. Restore this build input to external storage in CI and set SQLX_OFFLINE_DIR. Do not create a repository .sqlx cache. prepare rebuilds application packages so --check sees current schema changes. --cargo-offline controls Cargo downloads; SQLX_OFFLINE controls query metadata. Extra Cargo feature/package/workspace options follow `--`; all targets are checked by default. OS file locks exclude concurrent prepare processes for both output directory and workspace, and release on exit. Do not delete the retained .sqlx-doris-prepare.lock files. Metadata is fully staged, then each JSON file is atomically replaced before old hash files are pruned; other files remain. --check validates query/hash consistency too. Re-run prepare to regenerate corrupt JSON. After a publication I/O failure, fix its cause, rerun prepare and verify with --check.

prepare runs Cargo from the target manifest directory, loading that project and ancestor `.cargo/config.toml` files. Relative paths in extra Cargo arguments use that directory. `--cargo-offline` applies to metadata, clean and check; verified external target/build/cache environment paths remain enforced.

<a name="doc-errors"></a>

## Error types

Queries, connections and explicit core recovery primitives return `sqlx::Error`; SQLx migrations use `sqlx::migrate::MigrateError`. Schema/MV/Stream Load helpers use `sqlx_doris::operations::Error/Result` (also available in `stream_load`); the Database variant preserves the SQLx cause. Local Time construction/parsing uses `sqlx_doris::types::TimeError`.

Helper timeouts that exceed the clock range return InvalidInput before reading. Oversized intervals are clipped to the deadline; zero timeout permits one immediately ready result. MV cancel/wait require positive TaskId before pool acquisition. Migration lock timeout overflow returns SQLx Configuration before coordination metadata is created.
