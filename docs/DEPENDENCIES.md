<a name="doc-top"></a>

# Dependencies and licenses

[English](DEPENDENCIES.md) | [简体中文](DEPENDENCIES.zh-CN.md)

[Documentation](README.md)

See the [five-package diagram](ARCHITECTURE.md#doc-packages) for required/optional dependency directions and the [integration map](ARCHITECTURE.md#doc-flows) for execution paths.

Apache-2.0. Applications declare `sqlx-doris` as their sole Doris extension and upstream `sqlx` for generic APIs, derives and migration macros. All five implementation packages have aligned versions and exact internal dependencies.

| Package | Responsibility | Enabled by |
| --- | --- | --- |
| sqlx-doris | Public Doris types/macros and thin installed binary | Application dependency |
| sqlx-doris-core | Sole Database identity, protocol, bindings, types, connections and migration traits | Default |
| sqlx-doris-macros | Checked queries using SQLx macro infrastructure | macros |
| sqlx-doris-ops | Schema/MV, HTTP ingestion, identity/drain/diagnosis and read-only Kafka primitives | operations, stream-load, cli/cli-admin/cli-admin-kafka; feature gated |
| sqlx-doris-cli | Arguments, prepare/Cargo integration, human output and one JSON protocol | cli, cli-admin or cli-admin-kafka |

Internal packages depend on core, never the facade. Rust's orphan rules keep SQLx Migrate implementations beside the Doris identity in core. Maintenance workflows belong to ops. Ops accepts explicit requests and per-call evidence, with no clap, process exits, printing or global command state. CLI owns interaction, rendering and migration-runner lock/state-table preparation. Shared ops migration-history reads and source comparison remain read-only.

Default and migrate-only normal/build graphs exclude ops, CLI, generic sqlx, query macros and HTTP clients. operations adds application helpers; stream-load adds HTTP ingestion. cli adds clap, Cargo integration and shared configuration/results/read-only migration history; cli-admin enables maintenance HTTP, and cli-admin-kafka enables Kafka wire/TLS. No CLI profile implicitly enables driver query macros or application Stream Load. Applications select upstream SQLx features explicitly.

- Exact sqlx-core 0.9.0 supplies traits, queries, pools, transactions, TLS and logging. Internal runtime code uses it directly, without generic API forwarding.
- Tokio/futures/async-stream/bytes/bitflags/url support the asynchronous protocol.
- sha1 serves only the server's mysql_native_password challenge.
- serde/serde_json/chrono/bigdecimal preserve values and precision; core owns shared complex-value encoding.
- sqlx-macros-core/syn/quote/proc-macro-crate serve compile-time macros and renamed dependencies.
- reqwest/percent-encoding belong to optional ops maintenance HTTP. Stream Load is enabled separately.
- Kafka reads use rustls/tokio-rustls without a C/Java Kafka client.
- clap/sha2 and CLI runtime features belong to the CLI package.

Active normal/build graphs exclude sqlx-mysql, PostgreSQL and SQLite drivers. Cargo.lock/metadata may list inactive optional upstream packages. scripts/test-consumer uses eleven independent manifests for default, migrate, macros, macros+migrate, operations, stream-load, three CLI profiles, minimal upstream SQLx and upstream migrate! without Doris query macros. It also checks full feature unification, six ops combinations, CLI admin unified with independently enabled ops Kafka, and the macro package with query disabled.

sqlx-core/offline is intentional: SQLx 0.9 gates Executor::describe and ColumnOrigin serialization behind it. This keeps Executor complete when an application independently enables upstream macros. Doris description serialization and bitflags/serde require macro-support; the macro package has no normal/build dependencies with query disabled. Ops helper errors require helpers; maintenance/Kafka observation state stays out of helpers-only builds. Default runtime dependencies do not enable tokio-macros or multithreaded execution; applications, tests and CLI select these explicitly.

scripts/test-package verifies all five packages through Cargo workspace packaging. Each run uses a fresh external registry URL and target/build paths to avoid stale same-version unpublished archives. Verified .crate files remain in external artifacts. Users need no driver patches. Mirror users should prepare the official external registry cache using cargo fetch --manifest-path <checkout>/Cargo.toml from an external working directory.

Cargo.lock records tested dependencies, which retain their own licenses. scripts/test-install installs the unchanged verified archives and checks a fresh SQLx application, prepare and forced offline builds. Test registry routing for unpublished packages exists only in the external acceptance directory.
