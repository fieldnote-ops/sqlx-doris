# Usage

[简体中文](USAGE.md) | English

Start by running the query in the [installation guide](INSTALLATION.en.md). The Rust functions below can be
added to an existing Tokio application; configure SQL/HTTP endpoints and credentials in your application.

## Connections and pools

Select `PoolBuilder::new(Backend::MySql)` or `PoolBuilder::new(Backend::Doris)`, then call connect.
Both backends use native SQLx MySql types. Doris URLs also start with mysql://.
Configure pool size and timeouts with max_connections, acquire_timeout, idle_timeout, and max_lifetime.

For TLS verification, pass native connection options:

```rust,no_run
use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use sqlx_doris::{Backend, Client, PoolBuilder};
use std::{path::Path, str::FromStr};

pub async fn connect_doris_tls(url: &str, ca_file: &Path) -> sqlx_doris::Result<Client> {
    let options = MySqlConnectOptions::from_str(url)?
        .ssl_mode(MySqlSslMode::VerifyIdentity)
        .ssl_ca(ca_file);
    PoolBuilder::new(Backend::Doris).connect_with(options).await
}
```

Use the CA file supplied by your deployment; the URL hostname must match the certificate.
The host, username, and password methods on MySqlConnectOptions also accept raw values, avoiding manual
password URL encoding.

For Doris, Group Commit is off by default; strict CAST and Decimal256 are enabled. These settings can be
changed through PoolBuilder.
MySQL TIME membership compatibility is enabled by default. Set mysql_time_membership_compatibility(false) if
you need MySQL's native list coercion behavior.
`from_pool` only wraps an existing pool. `connect_any_with` selects an endpoint during pool creation; it does
not move running queries.

## Queries and parameters

Continue using `sqlx::query`, `query_as`, `query_scalar`, FromRow, and QueryBuilder.
Pass values through `.bind()`. Supply dynamic table names, column names, and other SQL structure through
trusted application templates.
Each placeholder binds one value; a Vec is not automatically expanded into an IN list.

The driver handles supported Doris prepared statement limitations while callers keep their SQL and `.bind()`
calls.
Unsupported SQL forms return explicit errors. See [compatibility](COMPATIBILITY.en.md) for BLOB, TIME,
defaults, and pagination limits.

## Write transactions

The following function assumes that sdk_demo_events exists. Create the demonstration table with the
appropriate SQL.

MySQL:

```sql
CREATE TABLE sdk_demo_events (
    id BIGINT NOT NULL PRIMARY KEY,
    note VARCHAR(128)
);
```

Doris:

```sql
CREATE TABLE sdk_demo_events (
    id BIGINT NOT NULL,
    note VARCHAR(128)
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1');
```

The Doris single-replica setting is for a demonstration with one BE. Adjust it to the replica requirements of
your actual cluster.
After creating the table, either database can use the same function:

```rust,no_run
pub async fn insert_event(pool: &sqlx::MySqlPool, id: i64, note: &str) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO sdk_demo_events(id, note) VALUES (?, ?)")
        .bind(id)
        .bind(note)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}
```

Run reads outside a Doris write transaction. Savepoints, queries/DDL inside transactions, and cross-database
transactions are outside the compatibility scope.
If a write or commit response is lost, the application must check the final state rather than retry every
error.
When a transaction is cancelled or dropped, the driver rolls it back or closes a connection that cannot safely
be reused. It never replays application writes whose outcome is uncertain.

## Doris types and operations

The `types` module includes LargeInt, BigDecimal, Time, TimestampTz, Array, Map, Struct, Variant, IP, and
Binary/Hex.
Use `operations::push_complex_value` to bind complex values leaf by leaf, and explicit projections to preserve
logical types when reading.
Doris determines VARIANT normalization, Decimal rounding, and OLAP storage type restrictions.

`operations` provides TableDefinition, TableModel, metadata, and asynchronous materialized view task APIs.
Choose the Doris table model explicitly when creating tables. For asynchronous operations, continue checking
task status or data visibility.
See [native_types.rs](../crates/sqlx-doris/examples/native_types.rs) for complete table creation and type
round trips.

## Stream Load

The default stream-load feature provides `stream_load::StreamLoadClient`.
SQL connections use the FE SQL port; HTTP imports use the FE HTTP endpoint. The BE address returned by a
redirect must also be reachable.
Call backend_endpoint if you need to override an internal BE address.

This function imports JSON into an existing table using the default JSON array format. It needs no additional
serde_json dependency:

```rust,no_run
use sqlx_doris::{
    operations::TableName,
    stream_load::{LoadReceipt, StreamLoadClient},
};

pub async fn import_events(
    http: &StreamLoadClient,
    table: &TableName,
) -> sqlx_doris::Result<LoadReceipt> {
    http.load(table)?
        .send_bytes(br#"[{"id":2,"note":"loaded"}]"#.to_vec())
        .await
}
```

StreamLoadClient::new accepts the FE HTTP endpoint, username, and optional password.
TableName::new accepts the database and table names. See
[stream_load.rs](../crates/sqlx-doris/examples/stream_load.rs) for full configuration and cleanup.
For complex ARRAY/MAP/STRUCT columns, use `load_table(&TableDefinition).send_json(...)` so the table
definition guides encoding.
Use `load(table).send_bytes()` to upload raw data without schema-based encoding.

Check LoadReceipt::state() and is_visible() after the call. Acceptance, commit, and visibility are distinct
states.
Handle duplicate Labels, filtered rows, Publish Timeout, and 2PC Prepared separately. The caller explicitly
commits or rolls back a Prepared transaction.
For HTTPS or mTLS, configure your CA/certificates through with_client_builder. Verification failures never
fall back to plaintext.

## Run the source examples

From the source root, configure the build volume and connection variables as shown in the
[README](../README.en.md#run-your-first-query).

| Example | Additional prerequisites | Command |
| --- | --- | --- |
| dual_pool | Both databases allow SELECT; no tables are created | ./scripts/dev cargo run --locked -p sqlx-doris --example dual_pool |
| native_types | DORIS_URL names an existing database; the account can create, write, query, and drop tables | ./scripts/dev cargo run --locked -p sqlx-doris --example native_types |
| stream_load | Same as above, plus HTTP endpoints and import credentials | ./scripts/dev cargo run --locked -p sqlx-doris --example stream_load |

native_types and stream_load create demonstration tables with random suffixes and delete them on normal
completion.
Before running stream_load, set:

```sh
export DORIS_HTTP_URL='http://doris-fe:8030'
export DORIS_HTTP_USER='user'
export DORIS_HTTP_PASSWORD='password'
# Set this only if the BE address returned by Doris is unreachable:
export DORIS_BE_HTTP_URL='http://doris-be:8040'
./scripts/dev cargo run --locked -p sqlx-doris --example stream_load
```

These variables specify the FE HTTP endpoint, HTTP credentials, and an optional BE HTTP endpoint. Do not use
SQL ports for them.

## Compile-time macros

Enable the sqlx-doris `macros` feature for `sqlx::query!` and `query_as!`.
At compile time, point DATABASE_URL at the target database, or set SQLX_OFFLINE=true and supply matching
offline metadata.
Prepare caches separately for the two databases and regenerate them after schema changes. Clearing the
statement cache does not update Rust DTOs or macro metadata.

The source includes query_macro.rs and doris_query_macro.rs. This command verifies online compilation, offline
metadata, and the missing-cache error:

```sh
export MYSQL_TEST_URL="$MYSQL_URL"
export DORIS_TEST_URL="$DORIS_URL"
./scripts/dev ./scripts/test-macros
```

It queries constants and creates no business tables. See [testing](TESTING.en.md) to run all examples or the
full suite against both databases.
