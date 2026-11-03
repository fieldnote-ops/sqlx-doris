<a name="doc-top"></a>

# sqlx-doris

[English](README.md) | [简体中文](README.zh-CN.md)

A native Apache Doris driver with SQLx queries, exact Doris values, streamed results and optional Stream Load ingestion. Use upstream `sqlx` for generic queries, pools, FromRow and migrations; declare `sqlx-doris` as your sole Doris extension, without internal packages. Checked queries and migration tools are optional; specialist recovery is a separate choice.

**0.2.0 is unreleased.** Requires Rust 1.94+ and a C toolchain; the verified baseline is Doris 4.1.5 with SQLx 0.9.0. Only Doris is provided; no sqlx-mysql package or driver patch is required.

<a name="doc-contents"></a>

## Contents

- [Run your first query](#doc-quickstart)
- [Enable only the capabilities you use](#doc-features)
- [Choose a guide](#doc-guide)
- [Work on the source checkout](#doc-develop)
- [License](#doc-license)

<a name="doc-quickstart"></a>

## Run your first query

Use a complete checkout beside your application, adjust the manifest path below, and configure [external build paths](docs/INSTALLATION.md#doc-storage). The target database and account must already exist. This runtime example needs no build-time DATABASE_URL.

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
sqlx-doris = { path = "../sqlx-doris-clean/crates/sqlx-doris" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Put this complete program in src/main.rs:

```rust
use sqlx_doris::DorisPool;
#[derive(Debug, sqlx::FromRow)]
struct Answer {
    id: i64,
    name: String,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = DorisPool::connect(&std::env::var("DORIS_URL")?).await?;
    let row: Answer = sqlx::query_as("SELECT CAST(? AS BIGINT) AS id, CAST(? AS STRING) AS name")
        .bind(42_i64)
        .bind("Doris 中文🙂")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.id, 42);
    assert_eq!(row.name, "Doris 中文🙂");
    println!("{row:?}");
    pool.close().await;
    Ok(())
}
```

Set DORIS_URL to `doris://user:password@fe:9030/database` and run your application with cargo run. URL-encode reserved characters in credentials/database names.

<a name="doc-features"></a>

## Enable only the capabilities you use

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

Default features are empty. Checked queries use `sqlx_doris::query!`; embedded migration files use upstream `sqlx::migrate!`. See [Installation](docs/INSTALLATION.md#doc-upstream).

<a name="doc-guide"></a>

## Choose a guide

[![Task map: native queries, ingestion, checked SQL, migrations, helpers and diagnosis](docs/assets/task-map.svg)](docs/ARCHITECTURE.md#doc-tasks)

Start with the [task map](docs/ARCHITECTURE.md#doc-tasks), trace [control and execution paths](docs/ARCHITECTURE.md#doc-flows), or explore the [five-package architecture](docs/ARCHITECTURE.md#doc-packages). A local interactive atlas accompanies the static diagrams and text.

- [Installation](docs/INSTALLATION.md): source dependency, features, external paths and TLS.
- [Everyday queries and types](docs/USAGE.md), [batch ingestion](docs/INGESTION.md) and [troubleshooting](docs/TROUBLESHOOTING.md).
- [Checked queries and offline builds](docs/USAGE.md#doc-macros), [SQLx migrations](docs/MIGRATIONS.md) and [CLI](docs/CLI.md).
- Specialist administration: [recovery](docs/RECOVERY.md), [managed Routine Load](docs/ROUTINE_LOAD.md) and [Kafka continuation preflight](docs/KAFKA.md).
- [Performance](docs/PERFORMANCE.md), [compatibility](docs/COMPATIBILITY.md), [testing](docs/TESTING.md), [dependencies](docs/DEPENDENCIES.md) and [release notes](CHANGELOG.md).
- [Full documentation and terminology](docs/README.md).

<a name="doc-develop"></a>

## Work on the source checkout

Repository tools require Python 3.12+. From the repository root, use a mounted external physical volume:

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
./scripts/dev cargo fetch --locked
./scripts/dev ./scripts/check
```

The wrapper keeps target/build, caches, downloads, temporary files and logs external and fails if the volume is unavailable. Docker/Lima disks and server data must also be external before local server startup; see [Testing](docs/TESTING.md).

<a name="doc-license"></a>

## License

Apache-2.0. Dependencies retain their own licenses.
