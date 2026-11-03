# sqlx-doris

[English](README.md) | [简体中文](README.zh-CN.md)

原生 Apache Doris SQLx 驱动。需要 Rust 1.94+、C 工具链、SQLx 0.9.0；已验证 Doris 4.1.5。

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

目标库与账号需预先存在，并设置 DORIS_URL。构建前将 CARGO_TARGET_DIR、CARGO_BUILD_BUILD_DIR、CARGO_HOME、TMPDIR、SQLX_OFFLINE_DIR 配置到已挂载外置盘。

查询、FromRow、连接池和 migrate! 使用官方 sqlx。macros 开启 sqlx_doris::query!，migrate 开启迁移 traits，operations 开启 Schema/MV 助手，stream-load 开启 HTTP 导入，cli 开启 cargo-sqlx-doris。默认特性为空，应用无需声明内部包。

[源码仓库与完整指南](https://github.com/fieldnote-ops/sqlx-doris)。完整检出目录中的文档首页为 docs/README.zh-CN.md。本包 README 与对应译文均随源码归档提供。许可证为 Apache-2.0。

基础 cli 仅提供 prepare/database/普通 migrate；cli-admin 增加 admin 下的 Doris 维护，cli-admin-kafka 再增加 Kafka 只读预检。三档不隐含应用 Stream Load 或查询宏，也不保留旧顶层管理命令别名。
