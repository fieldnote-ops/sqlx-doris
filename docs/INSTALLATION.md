# 安装与第一个程序

简体中文 | [English](INSTALLATION.en.md)

要求 Rust 1.94+ 和 C 编译工具。业务使用 Cargo 构建，不需要 Python。
0.1.0 未发布到 crates.io；可以用配套包或 GitHub 源码接入，随附驱动已并入仓库，应用不需要任何根 manifest 的补丁配置。

## 1. 准备源码和应用目录

拿到配套包时先解压到空的工作目录：

```sh
tar -xzf sqlx-doris-compat-0.1.0.tar.gz
```

从 GitHub 接入时改为克隆仓库，目录名换成仓库目录：

```sh
git clone https://github.com/fieldnote-ops/sqlx-doris.git sqlx-doris
```

再创建应用目录：

```sh
mkdir -p sdk-demo/src
```

最终布局为：

```text
work/
  sqlx-doris-compat-0.1.0/
    crates/sqlx-doris/
    crates/sqlx-mysql/
  sdk-demo/
    Cargo.toml
    src/main.rs
```

## 2. 配置 Cargo.toml

把以下内容保存为 sdk-demo/Cargo.toml：

```toml
[package]
name = "sdk-demo"
version = "0.1.0"
edition = "2024"

[dependencies]
sqlx-doris = { path = "../sqlx-doris-compat-0.1.0/crates/sqlx-doris" }
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

这条 path 相对于 sdk-demo/Cargo.toml，目录不同就调整，不要写成本机固定的绝对路径。
SDK 通过自身的 path 依赖解析 `crates/sqlx-mysql`，所以应用不需要补丁段；重复声明驱动反而会引入第二份类型。
已有 Cargo workspace 时，依赖的 path 相对于声明它的 Cargo.toml；使用 workspace.dependencies 时相对于 workspace 根。
直接声明的 SQLx 只提供 `#[derive(sqlx::FromRow)]` 派生宏，查询函数和池类型从 `sqlx_doris::sqlx` 使用。
已有 SQLx 依赖时统一到 `=0.9.0`；Tokio 提供异步运行时。

## 3. 保存程序

把以下内容保存为 sdk-demo/src/main.rs：

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

这段程序不依赖任何表。同一个 greeting 函数接收原生 MySqlPool，在两库绑定参数并映射到同一个结构体。

## 4. 配置连接并运行

先准备可登录的 MySQL 和 Doris 账号。示例 URL 中的 app 是已有数据库名；可以换成自己有权限访问的库。
如果要新建演示库，可由管理员分别在两库执行：

```sql
CREATE DATABASE IF NOT EXISTS app;
```

然后进入应用目录：

```sh
cd sdk-demo
export MYSQL_URL='mysql://user:password@mysql-host:3306/app'
export DORIS_URL='mysql://user:password@doris-fe:9030/app'
cargo run
```

将占位账号和主机替换为实际连接信息。成功时会看到：

```text
MySql: id=42, name=hello
Doris: id=42, name=hello
```

Doris 使用 FE 的 SQL 端口，通常是 9030，URL 仍为 mysql://。
`Backend::Doris` 启用 Doris 兼容选项；切换库时修改连接配置和后端选择，查询函数可继续复用。
密码中的 @、/、#、% 等字符需要 URL 编码；不想处理 URL 编码时可用 `MySqlConnectOptions` 设置账号密码，见使用指南。

## 已有项目接入

可以直接借用 `client.pool()`，或用 `client.into_pool()` 取得原生 MySqlPool。
`Client::from_pool` 只包装现有池，不会补装 Doris 初始化和协议选项；Doris 连接建议从 PoolBuilder 建池。
业务继续使用 `sqlx_doris::sqlx` 里的 `query`、`.bind()`、`FromRow` 和 `QueryBuilder`，不要混用上游 SQLx 的池类型。

默认启用 HTTP 导入 `stream-load`；只做 SQL 查询可在 sqlx-doris 依赖上设置 `default-features = false`。
编译期宏（`query!`、`query_as!`）由上游 SQLx 提供，需要自己的 `sqlx` 依赖（`features = ["mysql", "macros"]`），
并把 `sqlx-mysql` 指向本仓库的 `crates/sqlx-mysql`；普通 query/.bind() 不需要 DATABASE_URL 或离线元数据。

## 常见错误

| 现象 | 检查 |
| --- | --- |
| 找不到本地依赖路径 | 对照目录布局，检查 sdk-demo/Cargo.toml 里的 path |
| MySqlPool 类型不匹配 | 查询从 `sqlx_doris::sqlx` 取用；用 cargo tree -i sqlx-mysql 确认只有一份驱动 |
| 启动时报 NotPresent（缺少环境变量） | 在运行程序的同一个 shell 设置对应变量 |
| 认证失败或数据库不存在 | 检查账号权限、密码编码、库名和 FE SQL 端口 |
| 编译宏时要求 DATABASE_URL | 编译期宏需要上游 SQLx 宏和连接描述；普通 query 不需要 |

首次解析依赖后可用 `cargo run --locked` 固定应用 Cargo.lock。
源码示例和完整测试的步骤见[使用指南](USAGE.md)与[测试说明](TESTING.md)。
