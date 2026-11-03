# sqlx-doris

[English](README.md) | [简体中文](README.zh-CN.md)

Native Apache Doris driver for SQLx. Requires Rust 1.94+, a C toolchain, SQLx 0.9.0 and the verified Doris 4.1.5 baseline.

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
sqlx-doris = "0.2.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

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

Create the database/account first and set DORIS_URL. Configure CARGO_TARGET_DIR, CARGO_BUILD_BUILD_DIR, CARGO_HOME, TMPDIR and SQLX_OFFLINE_DIR on your mounted external volume before building.

Use upstream sqlx for queries, FromRow, pools and migrate!. Enable macros for sqlx_doris::query!, migrate for migration traits, operations for schema/MV helpers, stream-load for HTTP ingestion, and cli for cargo-sqlx-doris. Default features are empty; applications do not declare internal packages.

[Source repository and full guides](https://github.com/fieldnote-ops/sqlx-doris). The complete checkout contains docs/README.md. This package README and its translation are both included in the source archive. Licensed Apache-2.0.

Basic cli provides prepare/database/ordinary migrate; cli-admin adds Doris maintenance under admin, and cli-admin-kafka adds independent read-only Kafka preflight. No CLI profile implicitly enables application Stream Load or query macros, and old top-level administration aliases are absent.
