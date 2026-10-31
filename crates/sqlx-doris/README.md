# sqlx-doris

简体中文 | [English](README.en.md)

MySQL 与 Apache Doris 的 Rust SQLx 统一访问库。业务使用原生 `MySqlPool`、`.bind()` 和结果映射，连接时选择数据库后端。
Doris 的类型、表模型和事务规则保持原生语义。

0.1.0 已发布到 crates.io；源码在 [GitHub](https://github.com/fieldnote-ops/sqlx-doris)。要求 Rust 1.94+，SQLx 固定为 0.9.0。
已测试 MySQL 8.4.13 和官方 Doris 4.1.5。随附的 sqlx-mysql 驱动以 `sqlx-doris-mysql` 发布，由 SDK 依赖引入，安装时不需要任何补丁配置。

## 安装

```toml
[package]
name = "sdk-demo"
version = "0.1.0"
edition = "2024"

[dependencies]
sqlx-doris = "0.1.0"
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

直接声明的 SQLx 只提供 `#[derive(sqlx::FromRow)]` 派生宏；查询、绑定和池类型从 `sqlx_doris::sqlx` 使用。
驱动由 SDK 的依赖重命名带入，不需要 `[patch]` 段，也不要再单独声明 `sqlx-mysql`。
使用配套包或源码目录时，把上面那一行换成 path：

```text
work/
  sqlx-doris-compat-0.1.0/
    crates/sqlx-doris/
    crates/sqlx-mysql/
  sdk-demo/
    Cargo.toml
    src/main.rs
```

配套包中写成 `sqlx-doris = { path = "../sqlx-doris-compat-0.1.0/crates/sqlx-doris" }`，path 相对于 sdk-demo/Cargo.toml。
已有 SQLx 项目也要对齐到 `=0.9.0`；Tokio 提供异步运行时。

## 运行第一个程序

把以下代码保存为 sdk-demo/src/main.rs：

```rust,no_run
use sqlx_doris::{
    Backend, MySqlPool, PoolBuilder,
    sqlx::query_as,
};

#[derive(Debug, sqlx::FromRow)]
struct Greeting {
    id: i64,
    name: String,
}

async fn greeting(pool: &MySqlPool) -> sqlx_doris::sqlx::Result<Greeting> {
    query_as("SELECT CAST(? AS SIGNED) AS id, ? AS name")
        .bind(42_i64)
        .bind("hello")
        .fetch_one(pool)
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    for (backend, variable) in [
        (Backend::MySql, "MYSQL_URL"),
        (Backend::Doris, "DORIS_URL"),
    ] {
        let db = PoolBuilder::new(backend)
            .connect(&std::env::var(variable)?)
            .await?;
        let row = greeting(db.pool()).await?;
        println!("{backend:?}: id={}, name={}", row.id, row.name);
        db.close().await;
    }
    Ok(())
}
```

在 sdk-demo 目录设置连接并运行。把账号和地址换成自己的值，app 库需要已存在：

```sh
cd sdk-demo
export MYSQL_URL='mysql://user:password@mysql-host:3306/app'
export DORIS_URL='mysql://user:password@doris-fe:9030/app'
cargo run
```

程序不建表，只绑定两个参数并读取结果。成功时两库都会输出 `id=42, name=hello`。
Doris 的 SQL 连接也用 mysql:// URL；`Backend::Doris` 才会启用对应兼容选项，不能只换 URL 而保留 `Backend::MySql`。

## 后续使用

默认启用 `stream-load`，HTTP 导入使用 `stream_load::StreamLoadClient`；只做 SQL 查询时可设 `default-features = false`。
查询、参数绑定、`FromRow` 和 `QueryBuilder` 从 `sqlx_doris::sqlx` 使用，类型与池都来自随附驱动。
需要 SQLx 的 `query!`、`query_as!` 编译期宏时，要改用上游 SQLx 宏并把 `sqlx-mysql` 指到本仓库的 `crates/sqlx-mysql`
（宏会展开成 `::sqlx::` 路径），具体见 docs/USAGE.md 的编译期宏一节。

支持的 BLOB/TIME 参数、默认值和分页写法有范围限制。Doris 事务内读取、savepoint、表模型与 MySQL 不同，不能把所有 SQL 当成完全等价。
写入或提交结果不确定时不会自动重发，应由业务查询最终状态。

完整源码附有 docs/INSTALLATION.md、docs/USAGE.md、docs/COMPATIBILITY.md 和 docs/TESTING.md，包含连接选项、示例命令与测试结果。
SDK 使用 Apache-2.0；随附驱动保留上游 Apache-2.0 OR MIT 许可证。
