<a name="doc-top"></a>

# Troubleshooting

[English](TROUBLESHOOTING.md) | [简体中文](TROUBLESHOOTING.zh-CN.md)

[Documentation](README.md)

Start with the failing application operation and preserve its identity. Ordinary query/import failures do not require lock recovery or account retirement.

<a name="doc-contents"></a>

## Contents

- [Connection and TLS](#doc-connection)
- [Type and precision failures](#doc-types)
- [Unknown or partially filtered ingestion](#doc-ingestion)
- [Slow queries or memory growth](#doc-performance)
- [Build metadata and migration failures](#doc-tools)

<a name="doc-connection"></a>

## Connection and TLS

Check the doris:// SQL URL, database, account and FE query port independently of the HTTP ingestion endpoint. URL-encode reserved characters. Use VerifyIdentity with a trusted CA when hostname verification is required; Preferred can fall back to plaintext. Authentication currently supports mysql_native_password. See [installation](INSTALLATION.md#doc-upstream).

Keep pool size within server capacity. Canceled queries, incomplete result streams and uncertain commits retire their connection; pool replacement is expected and must not be “fixed” by replaying writes.

<a name="doc-types"></a>

## Type and precision failures

Use explicit CAST for ambiguous parameters and query-macro output overrides only when the logical type/nullability is known. Doris expression metadata is incomplete; weak parameter checking cannot infer all server conversions. LARGEINT and decimal values must not pass through f64. DATETIME has no time-zone identity; interpret TIMESTAMPTZ using the configured session semantics.

Complex strings may require operations::project or push_complex_value for correct Doris CAST text grammar. Do not replace arbitrary values with interpolated SQL. See [types and macros](USAGE.md#doc-types) and [precision limits](COMPATIBILITY.md#doc-precision).

<a name="doc-ingestion"></a>

## Unknown or partially filtered ingestion

Keep the original label, transaction ID if observed, immutable batch input and expected business keys/counts. Distinguish HTTP failure, business failure, filtering, Publish Timeout, asynchronous acceptance and visibility. Do not change labels or treat a missing server record as no submission.

Check receipt row counts and query application data. Unique-table key replacement can hide replays; it is not proof of exactly-once consumption. Raw rejected-row URLs may expose data and require separate authorization. Read [ingestion outcomes](INGESTION.md#doc-result) before choosing any manual action.

<a name="doc-performance"></a>

## Slow queries or memory growth

Compare identical SQL, data, result size, server settings and network conditions. Separate pool queueing, statement preparation, execution, row decoding, JSON encoding, upload and publication time. Use fetch for large result streams and send_file for large files. fetch_one/fetch_optional finish remaining responses before reuse.

Statement cache and pool health checks affect point-query round trips. Group Commit helps certain small-batch workloads, not every load. Keep real resource refusals visible. Existing small-row/short-duration measurements do not prove production OLAP throughput or long-term leak freedom; see [performance](PERFORMANCE.md).

<a name="doc-tools"></a>

## Build metadata and migration failures

For stale query metadata, regenerate prepare and run --check with the same application features and schema; --cargo-offline and SQLX_OFFLINE control different systems. For a dirty migration or held owner, first inspect source/history and stop old actors. Do not clear a lock merely because its age exceeds a timeout.

[Offline builds](USAGE.md#doc-offline) and [migrations](MIGRATIONS.md) explain normal workflows. Only an actual migration recovery incident calls for the separately selected [specialist recovery runbook](RECOVERY.md); empty diagnostic output never certifies takeover.
