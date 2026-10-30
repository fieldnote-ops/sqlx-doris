# sqlx-doris

SQLx 0.9.0 的 MySQL/Apache Doris 统一访问库，Rust 1.94+。受测基线 MySQL 8.4.13、官方 Doris 4.1.5；0.1.0 尚未公开发布。
保留原生 MySql、连接池、行和事务类型，Doris 设计差异保持原义，不模拟任意 MySQL SQL。

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

## 共用查询

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

## 类型、事务与安全

- SQLx 原生 query/`query_as`/`query_scalar`、FromRow 和 QueryBuilder 可直接使用。
- Doris 声明范围的 VALUES、静态 DEFAULT、BLOB 条件与 TIME/分页参数内部适配，继续原 SQL 和.bind()。
- BLOB 支持简单列比较、IN/NOT IN 和 BETWEEN/NOT BETWEEN；复杂 CASE/函数/LIKE 等不伪造等价支持。
- 原生 `Transaction<MySql>`支持顶层写事务；Doris 事务内查询/DDL、savepoint 和跨库事务不模拟。取消/未知状态退役连接，不重放不确定写入。
- types 提供 LargeInt、Time、`TimestampTz`、`Array/Map/Struct/Variant`、IP、Binary/Hex；
  复杂值可用 operations::push_complex_value 逐叶绑定。
- MySqlTime 保留 MySQL 原生范围；Doris 完整微秒边界使用 types::Time。Doris 内部 OLAP 不支持 TIME 列，查询和显式 STRING 表示分开。
- VARIANT 可能归一化或丢失展示信息；宽 Decimal 按目标 precision/scale 处理，不能从客户端还原已丢失的信息。
- operations 提供显式表模型、元数据及异步物化视图任务观察；提交接受与数据可见不同，需检查状态。
- 默认 stream-load 启用 HTTP 导入；macros 按需要启用。SDK 单包不会传递 Cargo patch。

PoolBuilder 默认 Doris 严格 CAST、Decimal256 开启、GroupCommit 关闭；可显式配置。
MySQL 的 TIME 列表共同域便利可通过 mysql_time_membership_compatibility(false)关闭；from_pool 不会补装初始化选项。
SQL 与 HTTP 的 TLS/CA/主机名、BE 可达地址按实际部署配置，不允许验证失败降级。

可运行示例：examples/dual_pool.rs、native_types.rs、stream_load.rs、query_macro.rs 和 doris_query_macro.rs。
完整使用、兼容边界及测试复现文档随 Git 源码提供。性能依赖 SQL、表模型和环境，不承诺全场景与原生 SQLx 等价。

SDK 使用 Apache-2.0；companion 保留上游 Apache-2.0 OR MIT 许可证及来源清单。
