<a name="doc-top"></a>

# Performance verification

[English](PERFORMANCE.md) | [简体中文](PERFORMANCE.zh-CN.md)

[Documentation](README.md)

Use a dedicated Doris database and an account allowed to create/drop fixture tables and temporary databases. Measurements use release builds and run serially after other builds/tests have stopped. Artifacts, caches, generated consumers and data stay external.

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
export DORIS_TEST_URL='doris://root@localhost:9030/sqlx_doris_test'
export DORIS_HTTP_URL='http://localhost:8030'
export DORIS_BE_HTTP_URL='http://localhost:8040'
export DORIS_BENCH_ITERATIONS=500
./scripts/dev ./scripts/benchmark
```

benchmark runs point queries, macros, migrations and application workloads in order. Each child script also works independently. Application workloads require an explicitly configured loopback SQL endpoint (for example, an owned SSH tunnel) plus trusted HTTP endpoints; TLS verification is retained.

- Point queries: a single-replica Unique Key table with row storage, reading a nine-byte string by BIGINT key. Five alternating rounds of 500 validated reads for cached/uncached prepare, default pool, pool without acquire ping and text protocol. Also measures an eight-way pool and a 100,000-row stream.
- Macros: query! and query_as use identical SQL, parameters and one connection, returning a CAST BIGINT and string. Five alternating rounds of 500 validated reads after warmup. This compares Rust APIs; the point-query workload compares caching and pool configuration.
- Migrations: isolated single-BE database, single-replica metadata and the default durable lock. First apply creates migration metadata and both persistent lock parents and applies SELECT 1, followed by five incremental versions and five unchanged checks. This measures coordination/checksum/dirty/state-write costs, including cooperative owner checkpoints before migration stages. Actual DDL duration depends on data and server jobs.

External SQLX_DORIS_ARTIFACTS_DIR holds benchmark.json, benchmark-macros.json, benchmark-migrations.json and logs with every trial. Tables show medians of trial metrics, including trial percentiles. First migration and additional concurrent/stream samples are single measurements.

<a name="doc-contents"></a>

## Contents

- [Tuning](#doc-tuning)
- [Application queries and ingestion](#doc-application)
- [CLI measurements](#doc-cli)
- [Sustained mixed reads](#doc-sustained)
- [Interpreting results](#doc-interpretation)
- [2026-11-02 product verification](#doc-record)

<a name="doc-tuning"></a>

## Tuning

Keep stable SQL shapes and the default per-connection statement cache (100 statements). Set max_connections for server capacity and min_connections to preconnect. test_before_acquire(false) avoids the acquire ping; stale idle sockets then fail on their first business command, without replaying writes. Defaults retain SQLx health checks.

Use Stream Load/batch writes for ingestion and fetch for large streams. Checked query metadata is obtained at compile time; runtime uses the same Doris executor and safe bindings. Lock/state queries and async DDL polling occur during migrations; healthy ordinary queries add none of those round trips.

<a name="doc-application"></a>

## Application queries and ingestion

```sh
./scripts/dev ./scripts/benchmark-workloads --rows 4096 --width 1024 --rounds 3 \
  --iterations 25 --seed 9417 --delays 0 2
```

The fixed seeded Duplicate Key dataset has BIGINT/INT values, 1024-byte UTF-8 payloads and ARRAY<BIGINT>; every returned value and row multiplicity is checked. Three rounds of 25 measured queries follow two warmups per group. Aggregation and JOIN return 16 groups over 4096 inputs. Wide/complex/slow-consumer streams return 256 or 4096 rows; the slow consumer sleeps 1 ms per 64 rows. SQL text and bound values remain the same across relay profiles.

Both 0 and 2 ms profiles use the same local TCP relay. Delay is applied to each forwarded chunk of at most 65536 bytes in both directions, not a fixed round-trip delay. This controlled transport experiment does not reproduce a production network. JSONL records include result rows/payload size, all individual latencies, relay bytes/chunks and server version.

Six ingestion modes run once per round in alternating order: INSERT batches of 256 rows, send_json, send_bytes, send_file, sync Group Commit and async Group Commit. The owned table uses group_commit_interval_ms=1000 without changing global quotas. INSERT commits each batch separately, whereas a Stream Load batch has its own transaction; the comparison does not claim identical atomicity. Separate fields record a serde_json encoding baseline, file preparation/fsync, submission acknowledgement and observed row-count visibility; complete value/multiplicity validation follows visibility. Actual Group Commit mode is required, and every load rejects filtering. Payload MiB/s uses encoded JSON size, not actual wire bytes.

Each profile has a bounded wall-time budget (default --max-seconds 1800, range 30–7200), keeping RSS collection bounded. Exceeding it fails the workload and stops only its child process; already admitted server work can still complete, so fixtures are retained without replay.

External workloads-UUID/summary.json preserves raw samples, nearest-rank P50/P95/P99, validated rows/s and payload MiB/s. Three ingestion samples per mode make tail quantiles descriptive only. RSS is sampled every 50 ms; it includes expected data and allocator history from baseline encoding, does not isolate upload-buffer allocation and is not an absolute peak. File bodies read in bounded chunks and byte bodies share storage across redirects; these implementation properties are distinct from measured RSS. Failures retain only the run's named tables and external inputs, without write retry or quota increases.

<a name="doc-cli"></a>

## CLI measurements

```sh
./scripts/dev ./scripts/benchmark-identities
./scripts/dev ./scripts/benchmark-routines
./scripts/dev ./scripts/benchmark-kafka --connection-file "$KAFKA_CONFIG" \
  --topic events --offset 0=123 --offset 1=456
./scripts/dev ./scripts/benchmark-diagnostics --retired-identity "$OLD_ID"
```

Identity trials measure creation, read-only retirement preview, explicit retirement and empty-database drain in an isolated database. Routine trials additionally require DORIS_KAFKA_BROKERS / DORIS_KAFKA_TOPIC with partition 0. They use OFFSET_END without producing messages; separate identity setup is excluded. Five generations measure JSON creation, human status by name, and receipt JSON status/preview/explicit stop. Success cleans up only the isolated fixture database; permanent reservations grow with generations. Receipt operations include physical-volume, private-file and generation checks, unlike name-only status.

Kafka trials require a stable dedicated topic with a retained record per partition, complete explicit candidates and in-range offsets. Each trial captures a new baseline before read-only preflight, including TLS/optional SASL, leader batches and private-file checks. The script produces no messages and commits/transfers/resets no offsets; separate fixture tools must acknowledge messages beforehand. Fresh measurement baselines do not prove earlier history.

Diagnosis requires an idle, reconciled database and a genuinely retired dedicated identity. Optionally add `--receipt "$RECEIPT"`; Kafka observations require the complete explicit --connection-file/--topic/--checkpoint/--offset group. Five alternating trials run lock/drain JSON and human/JSON diagnose processes; every command must succeed. The benchmark does not create identities, change locks/history/jobs or infer offsets. Empty observations do not measure real drainage time.

CLI values are release process wall times, including startup, network, file and metadata checks. Five trials and small catalogs establish neither fixed output overhead nor production latency or continuation safety. External benchmark-identities.json, benchmark-routines.json, benchmark-kafka-UUID.json and benchmark-diagnostics-UUID.json retain all samples without removing individual slow trials.

<a name="doc-sustained"></a>

## Sustained mixed reads

```sh
./scripts/dev ./scripts/benchmark-sustained --seconds 60 --rounds 3 \
  --workers 16 --pool-size 4 --cache-capacity 8 --seed 9417
```

Six logical operations cover parameterized points, 32 consecutive SQL shapes on one connection to force eviction, 64 KiB fields, delayed consumption, dropping a stream after its first row, and stalling an admitted/validated stream until its client deadline. That deadline is not a server SQL timeout. Cache capacity is 1–16 against 32 shapes; workers must exceed pool capacity. Every consumed value is checked.

Defaults are three 60-second rounds. Fixed-memory histograms report 1 ms bucket upper bounds for queue/operation P50/P95/P99, plus exact means/maxima. Different statement counts and deliberate delays make operations/s distinct from query QPS. One-second client RSS samples retain the full vector and observed maximum, without claiming absolute peak or leak freedom. Intentional replacement connections after cancellation do not alone establish leakage.

All final pool connections are acquired together, pinged and checked against cache limits. Pool closure must remove isolated-database sessions from the FE list, without KILL or write replay. Client handle counts do not prove total server handles; protocol tests separately verify unique PREPARE/CLOSE pairing. Failures preserve logs/databases; success removes only owned databases. benchmark-sustained-UUID.json and raw logs stay external.

<a name="doc-interpretation"></a>

## Interpreting results

Compare only identical SQL, data, server settings and load. Splitting packages removes operations/CLI dependencies from default consumers while ordinary queries retain the native Executor; this alone does not establish higher QPS. Runtime measurements exclude compilation. Source archive installation reuses dependency caches, so Cargo durations are neither cold-build nor public-registry download measurements. See [Testing](TESTING.md) for acceptance scope.

<a name="doc-record"></a>

## 2026-11-02 product verification

Apple M1 Max / 64 GiB / macOS arm64; external-disk aarch64 Linux VM with 4 vCPU / 12 GiB and arm64 images, official Doris 4.1.5 single FE/BE (512 MiB FE heap, 3 GiB BE limit), Kafka 4.0.0. rustc 1.99.1 release over local TCP/TLS and SSH forwarding. After correctness checks, owned Doris services restarted with the same configuration, quotas and data, then workloads ran serially. Runtime excludes compilation. Successful runs retain every trial; failure evidence is retained separately. No before/after speedup is claimed.

| Point-query mode | QPS | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | --- | --- |
| Cached connection | 537.151 | 1.646 | 3.125 | 4.884 |
| Uncached connection | 179.378 | 5.356 | 7.508 | 18.154 |
| Default pool | 483.037 | 1.983 | 2.929 | 4.922 |
| Pool without acquire ping | 747.817 | 1.237 | 2.040 | 3.224 |
| Text connection | 271.615 | 3.610 | 4.655 | 7.255 |

| Same-SQL API | QPS | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | --- | --- |
| sqlx_doris::query! | 291.411 | 3.384 | 4.439 | 5.772 |
| sqlx::query_as | 306.907 | 3.158 | 4.154 | 5.672 |

| Migration | ms |
| --- | --- |
| First apply (1) | 435.834 |
| Incremental apply (5 median) | 340.813 |
| Unchanged check (5 median) | 158.776 |

Values above are medians of five trial metrics; first migration has one sample. Actual DDL depends on data and asynchronous jobs.

| Transport | Query | Result rows | P50 ms | P95 ms | P99 ms |
| --- | --- | --- | --- | --- | --- |
| query_relay_0ms | aggregate | 16 | 11.800 | 17.261 | 20.609 |
| query_relay_0ms | join | 16 | 15.506 | 24.680 | 32.405 |
| query_relay_0ms | wide | 256 | 11.891 | 16.727 | 24.739 |
| query_relay_0ms | wide | 4096 | 30.247 | 37.182 | 42.282 |
| query_relay_0ms | complex | 256 | 9.517 | 12.408 | 13.689 |
| query_relay_0ms | complex | 4096 | 10.317 | 12.812 | 14.622 |
| query_relay_0ms | slow_consumer | 256 | 20.380 | 23.166 | 26.966 |
| query_relay_0ms | slow_consumer | 4096 | 172.963 | 180.610 | 186.749 |
| query_relay_2ms | aggregate | 16 | 21.161 | 24.822 | 29.618 |
| query_relay_2ms | join | 16 | 23.943 | 28.792 | 33.083 |
| query_relay_2ms | wide | 256 | 31.495 | 34.902 | 39.496 |
| query_relay_2ms | wide | 4096 | 202.924 | 211.742 | 221.128 |
| query_relay_2ms | complex | 256 | 19.867 | 22.632 | 23.932 |
| query_relay_2ms | complex | 4096 | 23.145 | 25.966 | 26.428 |
| query_relay_2ms | slow_consumer | 256 | 33.573 | 38.716 | 51.795 |
| query_relay_2ms | slow_consumer | 4096 | 203.607 | 215.142 | 238.368 |

| Ingestion mode | Samples | ACK P50 ms | Visible P50 ms | Visible P95 ms | Rows/s median | Payload MiB/s median |
| --- | --- | --- | --- | --- | --- | --- |
| insert_batch | 3 | 984.663 | 998.085 | 1095.731 | 4103.860 | 4.272 |
| load_json | 3 | 117.162 | 135.584 | 136.226 | 30210.017 | 31.445 |
| load_bytes | 3 | 113.252 | 130.758 | 136.888 | 31325.073 | 32.606 |
| load_file | 3 | 113.154 | 129.178 | 142.305 | 31708.104 | 33.005 |
| group_sync | 3 | 1114.742 | 1132.270 | 1146.413 | 3617.513 | 3.765 |
| group_async | 3 | 52.634 | 1129.240 | 1137.394 | 3627.217 | 3.776 |

4096 rows, 1024-byte payloads, seed 9417; 3×25 query samples per group and three ingestion samples per mode. All values/multiplicities were verified; raw encoding/file/ACK/visibility timing, byte counts and RSS vectors remain available. Three-sample ingestion tails are descriptive; per-chunk relay delay is not RTT. RSS includes validation data and allocator history.

| Round | Seconds | Operations | Ops/s | Queue P99 ms | Cache max | Pool |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 60.305 | 2874 | 47.658 | 662 | 8 | 4 |
| 1 | 60.476 | 2937 | 48.564 | 627 | 8 | 4 |
| 2 | 60.686 | 2909 | 47.935 | 629 | 8 | 4 |

Total 8720 logical operations; 182 RSS samples, observed maximum 12672 KiB. Final checks verified 4 healthy connections, 0 sessions after pool closure and owned-database cleanup. Three minutes, sampled RSS and cumulative replacement counts do not establish long-term leak freedom or production throughput.

| Specialist CLI (five-trial median) | ms |
| --- | --- |
| Identity create_ms | 218.638 |
| Identity preview_ms | 62.785 |
| Identity retire_ms | 101.924 |
| Identity drain_ms | 127.432 |
| Routine create_ms | 302.241 |
| Routine status_ms | 225.952 |
| Routine status_name_ms | 79.993 |
| Routine preview_ms | 240.886 |
| Routine stop_ms | 314.072 |
| Kafka TLS snapshot_ms | 233.221 |
| Kafka TLS preflight_ms | 219.440 |
| Diagnosis lock_json | 69.317 |
| Diagnosis drain_json | 122.453 |
| Diagnosis diagnose_human | 365.936 |
| Diagnosis diagnose_json | 363.157 |

Specialist timings include processes, networking and file/metadata checks. Kafka uses fresh baselines and explicit candidates; diagnosis includes a retired identity and independent Kafka observations, without receipt/source. Empty drain snapshots do not measure real drainage or certify continuation/takeover. Resource-refusal and external-storage failures remain recorded; quotas were not raised and slow trials were not removed.
