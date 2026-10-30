# sqlx-doris

让同一套 Rust SQLx 查询、DTO 和安全的 `.bind()` 访问 MySQL 与 Apache Doris。
保留原生 `MySql`、`MySqlPool`、`MySqlConnection`、`MySqlRow` 与 `Transaction<MySql>` 类型；按连接配置选择后端。
Doris 类型、表模型、回执和事务遵循数据库自身语义。

当前版本 0.1.0，尚未公开发布。固定测试基线为 SQLx 0.9.0、Rust 1.94+、MySQL 8.4.13、官方 Doris 4.1.5。
其他版本需要自行回归；本库不承诺任意 MySQL SQL 在 Doris 上等价执行。

## 项目背景

这是对 2022 年一个 Doris 与 MySQL 混用项目的重构。当时为了优化性能，一些业务表会在 Doris 和 MySQL 之间来回调整。
Python 实现中选用的驱动能支持较新版本的 MySQL，却不能直接兼容当时的 Doris；Doris 侧还得使用兼容旧版 MySQL 的驱动。
表换了库，驱动、连接配置和数据访问代码也得跟着调整。

这次重构希望减少这类切换成本：基于 Rust SQLx，让业务尽量复用同一套查询、参数绑定和结果映射，按连接配置选择数据库。
Doris 与 MySQL 本身的语义差异仍然保留，项目不负责迁表，也不承诺所有 SQL 完全兼容。

## 安装

```toml
[dependencies]
sqlx-doris = { path = "../sqlx-doris/crates/sqlx-doris" }

[patch.crates-io]
sqlx-mysql = { path = "../sqlx-doris/patches/sqlx-mysql" }
```

路径相对于应用 workspace 根 Cargo.toml。已有直接 SQLx 依赖时对齐到 `=0.9.0`。
patch 必须配置在应用 workspace 根，保证应用、宏与库使用同一个 sqlx-mysql；SDK 单个.crate 不会传递 patch，遗漏会明确编译失败。
配套包的 SDK 位于 `sdk/sqlx-doris-0.1.0`，driver 位于 `patches/sqlx-mysql`，按解包目录调整路径。

示例假设两库已各自建好包含 id/name 的 items 表，在 Tokio 异步上下文调用 example。

## 同一业务函数访问双库

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

## 功能

- SQLx 原生查询、`FromRow`、`QueryBuilder`、连接池、缓存及顶层写事务。
- 常见 VALUES、DEFAULT、分页、BLOB/TIME 参数在声明范围内安全适配，业务继续原 SQL 和 `.bind()`。
- Doris LARGEINT、宽 Decimal、ARRAY/MAP/STRUCT/VARIANT、TIME、IP 与时区类型的显式表示。
- 显式表模型、元数据、异步物化视图任务观察，以及可选 HTTP Stream Load。
- 取消、连接损坏和不确定写入不会触发业务写重放。

## 文档与示例

- [安装与已有 SQLx 项目接入](docs/INSTALLATION.md)
- [连接、查询、事务与导入](docs/USAGE.md)
- [支持范围与数据库语义差异](docs/COMPATIBILITY.md)
- [测试命令与最近验证](docs/TESTING.md)
- [依赖来源、安全说明与许可证](docs/DEPENDENCIES.md)
- [驱动补丁来源与安装](patches/README.md)
- [可运行示例](crates/sqlx-doris/examples)

## 构建与测试

源码脚本需要 Python 3.12+；业务程序使用 SDK 时不需要这些脚本。
所有构建、下载、缓存和临时产物经外置盘入口：

先设置本机已挂载的构建盘，路径由使用者选择：

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<已挂载构建盘的绝对路径>"
# 可选：指定该卷内的工作目录；不设置时使用卷内的 sqlx-doris 目录。
# export SQLX_DORIS_BUILD_ROOT="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris"
./scripts/dev --print-env
./scripts/dev cargo fetch --locked
./scripts/dev cargo fetch --locked --manifest-path tests/mysql-upstream/Cargo.toml
./scripts/dev ./scripts/check
./scripts/dev ./scripts/test-msrv
# 已设置专用双库URL和HTTP端点后：
./scripts/dev ./scripts/test-live
```

没有默认的硬盘路径。未配置、未挂载或目录越出所选构建盘时直接失败，不回退内部盘。
`.cargo/` 属于本机配置，不纳入 Git；仓库也不提供写死的 Cargo target-dir。实际输出位置可通过 --print-env 查看。
完整测试前提、数据准备与打包方式见测试文档。

## 许可证

SDK 使用 Apache-2.0；随附 sqlx-mysql 保留上游 Apache-2.0 OR MIT 许可。完整文本随源码和配套包交付。
