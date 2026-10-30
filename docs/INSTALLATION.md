# 安装与接入

要求 Rust 1.94+、SQLx 0.9.0。先将 SDK 和 companion 置于本地目录；当前不依赖未发布的 crates.io 版本号。

```toml
[dependencies]
sqlx-doris = { path = "../sqlx-doris/crates/sqlx-doris" }

[patch.crates-io]
sqlx-mysql = { path = "../sqlx-doris/patches/sqlx-mysql" }
```

路径相对于应用 workspace 根 Cargo.toml。已有直接 SQLx 依赖时对齐到 `=0.9.0`。
patch 必须配置在应用 workspace 根，保证应用、宏与库使用同一个 sqlx-mysql；SDK 单个.crate 不会传递 patch，遗漏会明确编译失败。
配套包的 SDK 位于 `sdk/sqlx-doris-0.1.0`，driver 位于 `patches/sqlx-mysql`，按解包目录调整路径。

`default-features=false` 可关闭 HTTP 导入；按需要开启 `stream-load` 或 `macros`。
引入 patch 后先正常解析依赖并更新应用 Cargo.lock，再用--locked 执行检查；不是无条件更新其他依赖。

## 接入示例

```rust,no_run
use sqlx_doris::{Backend, PoolBuilder, sqlx};

#[derive(Debug, sqlx::FromRow)]
struct Item {
    id: i64,
    name: String,
}

async fn find(pool: &sqlx::MySqlPool, id: i64) -> Result<Option<Item>, sqlx::Error> {
    sqlx::query_as("SELECT id, name FROM items WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn example() -> Result<(), Box<dyn std::error::Error>> {
    let mysql = PoolBuilder::new(Backend::MySql)
        .connect(&std::env::var("MYSQL_URL")?)
        .await?;
    let doris = PoolBuilder::new(Backend::Doris)
        .connect(&std::env::var("DORIS_URL")?)
        .await?;
    let _ = find(mysql.pool(), 42).await?;
    let _ = find(doris.pool(), 42).await?;
    mysql.close().await;
    doris.close().await;
    Ok(())
}
```

应用已有连接池时可用 Client::from_pool，但它不会补装 Doris 初始化/协议选项；建议从 PoolBuilder 建池。
使用 Client::pool()借用，或 into_pool()直接取得原生 MySqlPool。没有新 Database 类型，也不使用别名掩盖不同 SQLx 版本。

## 配套包

源码检查需要先设置 `SQLX_DORIS_EXTERNAL_VOLUME`，可用 `SQLX_DORIS_BUILD_ROOT` 选择卷内工作目录，详见[测试配置](TESTING.md)。
从源码目录运行：

```sh
./scripts/dev cargo fetch --locked
./scripts/dev cargo fetch --locked --manifest-path tests/mysql-upstream/Cargo.toml
./scripts/dev ./scripts/check
./scripts/dev ./scripts/verify-delivery
```

归档位于 `scripts/dev --print-env` 中 CARGO_TARGET_DIR 所指目录的 package 子目录。应用依赖指向 compat 包内 SDK，根 patch 指向同包内 driver。
配套包有完整 SHA256SUMS.json，verify-delivery 从原始 registry crate 按顺序重建补丁并核对每份负载。
本包不包含所有 Cargo 依赖的离线镜像；首次构建需网络或已有缓存。第三方许可与来源见 DEPENDENCIES.md。
