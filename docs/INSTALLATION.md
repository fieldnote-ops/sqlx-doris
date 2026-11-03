<a name="doc-top"></a>

# Installation

[English](INSTALLATION.md) | [简体中文](INSTALLATION.zh-CN.md)

[Documentation](README.md)

Version **0.2.0 is unreleased**. Use a complete source checkout. Rust 1.94+ and a C toolchain for TLS dependencies are required. Python 3.12+ is needed only for repository tools, not your running application.

<a name="doc-contents"></a>

## Contents

- [Add a source dependency](#doc-source)
- [Choose features](#doc-features)
- [Use upstream SQLx](#doc-upstream)
- [Configure external build storage](#doc-storage)
- [Connect and configure TLS](#doc-tls)
- [Install the companion CLI](#doc-cli)

<a name="doc-source"></a>

## Add a source dependency

In your application Cargo.toml, set `path` relative to that manifest, pointing into the complete checkout. The following is the minimal runtime setup with row derives. Do not install 0.2.0 from a public registry before it is released.

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
sqlx-doris = { path = "../sqlx-doris-clean/crates/sqlx-doris" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

<a name="doc-features"></a>

## Choose features

Use the [task map](ARCHITECTURE.md#doc-tasks) to choose a path and the [integration lanes](ARCHITECTURE.md#doc-flows) to distinguish compile-time checks from runtime execution.

| Need | `sqlx-doris` feature | Upstream `sqlx` feature |
| --- | --- | --- |
| Runtime queries, pools and Doris types | None | None; `derive` for `FromRow` |
| Batch ingestion | `stream-load` (includes `operations`) | As required by your query code |
| Schema/MV/visibility helpers | `operations` | As required by your query code |
| Checked Doris queries | `macros` | `derive` or `macros`; direct dependency named `sqlx` |
| Embedded migrations | `migrate` | `macros`, `migrate` |
| Runtime migration directory | `migrate` | `migrate` |
| Basic CLI | `cli` (includes `migrate`) | Managed by the CLI |
| Doris administration | `cli-admin` (includes `cli`) | Managed by the CLI |
| Independent Kafka preflight | `cli-admin-kafka` (includes `cli-admin`) | Managed by the CLI |

Default features are empty. JSON, BigDecimal, chrono and Doris logical types are available directly. Internal packages share exact aligned versions; applications declare only `sqlx-doris` as their Doris extension.

<a name="doc-upstream"></a>

## Use upstream SQLx

Built-in drivers are exported by upstream sqlx through mysql/postgres/sqlite features, sharing generic machinery from sqlx-core. Doris is an external driver: use upstream sqlx::query/query_as, sqlx::Pool<Doris>, sqlx::FromRow and sqlx::migrate::Migrator, importing Doris types and operations from sqlx-doris. Both share the same sqlx-core; this adds no query forwarding or runtime overhead. See the [upstream facade](https://github.com/transact-rs/sqlx/blob/v0.9.0/src/lib.rs).

SQLx is not re-exported, and there are no duplicate FromRow/migrate! entry points. Enable upstream sqlx/derive for row derives. Embedded migrations use upstream sqlx/macros + sqlx/migrate and driver sqlx-doris/migrate. Checked Doris queries use sqlx_doris::query! with the driver macros feature and a direct upstream dependency named sqlx, enabling upstream derive or macros for runtime helpers.

Upstream query! and cargo sqlx do not register Doris; use this driver's query macros and cargo sqlx-doris for metadata preparation. Upstream sqlx::migrate! does not require database registration and constructs a Migrator usable with Doris. See the [external-driver FAQ](https://github.com/transact-rs/sqlx/blob/v0.9.0/FAQ.md).

DorisConnectOptions configures credentials, TLS, statement_cache_capacity (default 100), max_packet_size (default 64 MiB), group_commit, decimal256 and strict_cast. DorisPoolOptions exposes SQLx pool configuration and lifecycle hooks. Use `connect_with(options)` to combine them.

TLS defaults to Preferred: encrypt if the FE offers TLS, otherwise use plaintext. Required requires encryption. VerifyCa verifies the certificate chain; VerifyIdentity additionally checks the hostname. Configure a trusted PEM bundle with ssl_ca and mutual TLS with ssl_client_cert/ssl_client_key. Choose verification appropriate to your deployment.

URL scheme: `doris://user:password@fe:9030/database`. URL-encode reserved characters. Supported URL parameters: ssl-mode, ssl-ca, ssl-cert, ssl-key, statement-cache-capacity. Unknown options fail. Authentication supports mysql_native_password.

<a name="doc-storage"></a>

## Configure external build storage

In the application shell, replace the mount placeholder with an existing separately mounted external volume before executing these commands. This environment is also required for direct Cargo builds; the repository wrapper does not configure unrelated application shells.

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
export CARGO_TARGET_DIR="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/app-target"
export CARGO_BUILD_BUILD_DIR="$CARGO_TARGET_DIR"
export CARGO_HOME="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/cargo-home"
export TMPDIR="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/tmp-clean"
export SQLX_OFFLINE_DIR="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/offline/app"
mkdir -p "$CARGO_TARGET_DIR" "$CARGO_HOME" "$TMPDIR" "$SQLX_OFFLINE_DIR"
```

From the repository root, use `./scripts/dev <command>` instead. It validates the mount and configures build, cache and temporary paths. Before local server startup, audit Docker/Lima disks and every data/log mount; see [Testing](TESTING.md#doc-storage). Migration SQL is application source and may live in `migrations/`.

<a name="doc-tls"></a>

## Connect and configure TLS

Set `DORIS_URL` for runtime code and create the target database beforehand. This is an application function; add it to your own program.

```rust
use sqlx_doris::{DorisConnectOptions, DorisPoolOptions, DorisSslMode};

pub async fn connect() -> sqlx::Result<sqlx_doris::DorisPool> {
    let options = std::env::var("DORIS_URL")
        .map_err(|error| sqlx::Error::Configuration(error.into()))?
        .parse::<DorisConnectOptions>()?
        .ssl_mode(DorisSslMode::VerifyIdentity)
        .ssl_ca("certificates/ca.pem")
        .statement_cache_capacity(100);
    DorisPoolOptions::new()
        .max_connections(16)
        .connect_with(options)
        .await
}
```

TLS defaults to Preferred: use encryption if the FE offers it, otherwise plaintext. Required requires encryption; VerifyCa validates the chain; VerifyIdentity also validates the hostname. `ssl_client_cert`/`ssl_client_key` configure mutual TLS. `DorisPoolOptions` supplies SQLx pool hooks. Options also include `max_packet_size` (64 MiB default), `group_commit`, `decimal256` and `strict_cast`.

Use `doris://user:password@fe:9030/database` and URL-encode reserved characters. URL parameters are `ssl-mode`, `ssl-ca`, `ssl-cert`, `ssl-key` and `statement-cache-capacity`; unknown parameters fail. Authentication is mysql_native_password.

<a name="doc-cli"></a>

## Install the companion CLI

Run from the complete repository root, not from a standalone .crate archive. Install into the external prefix and add its bin directory to your shell PATH.

Choose one installation profile: cli provides prepare/database/ordinary migrate; cli-admin adds Doris maintenance under admin; cli-admin-kafka additionally enables independent read-only Kafka preflight. All profiles install the same executable. Use cargo install --force explicitly when replacing an installed profile. The example installs the basic profile; replace the --features value as needed.

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
./scripts/dev cargo install --path crates/sqlx-doris --features cli --locked \
  --root "$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/tools"
export PATH="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/tools/bin:$PATH"
cargo sqlx-doris --help
```

Use `cargo sqlx-doris`, not the upstream `cargo sqlx`. CLI-only installation does not enable checked queries or Stream Load. See the [CLI reference](CLI.md) and [recovery runbook](RECOVERY.md).
