# 安装与第一个程序

要求 Rust 1.94+ 和 C 编译工具。业务使用 Cargo 构建，不需要 Python。
当前 0.1.0 尚未公开发布，请使用获取到的源码或 compat 配套包。
下面从源码接入；配套包用户可直接按[SDK README](../crates/sqlx-doris/README.md)操作。

## 1. 准备源码和应用目录

已有源码目录时，将它放到应用目录旁，命名为 sqlx-doris。
如果拿到的是交付文件，先放到一个空的工作目录，以下命令在那里执行。可任选一种方式：

```sh
git clone sqlx-doris-0.1.0.bundle sqlx-doris
```

或者解压源码包：

```sh
tar -xzf sqlx-doris-source-0.1.0.tar.gz
```

再创建应用目录：

```sh
mkdir -p sdk-demo/src
```

最终布局为：

```text
work/
  sqlx-doris/
    crates/sqlx-doris/
    patches/sqlx-mysql/
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
sqlx-doris = { path = "../sqlx-doris/crates/sqlx-doris" }
sqlx = { version = "=0.9.0", default-features = false, features = ["mysql", "derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

[patch.crates-io]
sqlx-mysql = { path = "../sqlx-doris/patches/sqlx-mysql" }
```

这个独立应用中，两条 path 都相对于 sdk-demo/Cargo.toml。目录不同就调整路径，不要写成本机固定的绝对路径。
已有 Cargo workspace 时，依赖的 path 相对于业务 crate 的 Cargo.toml；`[patch.crates-io]` 放到 workspace 根，
其中的 path 相对于根 Cargo.toml。使用 workspace.dependencies 时，其 path 也相对于 workspace 根。
已有 SQLx 依赖时统一到 `=0.9.0`。示例里的直接 SQLx 依赖供 FromRow 派生宏使用，Tokio 提供异步运行时。

## 3. 保存程序

把以下内容保存为 sdk-demo/src/main.rs：

```rust,no_run
use sqlx_doris::{Backend, PoolBuilder};

#[derive(Debug, sqlx::FromRow)]
struct Greeting {
    id: i64,
    name: String,
}

async fn greeting(pool: &sqlx::MySqlPool) -> sqlx::Result<Greeting> {
    sqlx::query_as("SELECT CAST(? AS SIGNED) AS id, ? AS name")
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
业务继续使用 `sqlx::query`、`.bind()`、`FromRow` 和 `QueryBuilder`。

默认启用 HTTP 导入 `stream-load`；只做 SQL 查询可在 sqlx-doris 依赖上设置 `default-features = false`。
SQLx 编译期宏需要 `features = ["macros"]`；普通 query/.bind() 不需要 DATABASE_URL 或离线元数据。

## 常见错误

| 现象 | 检查 |
| --- | --- |
| 找不到本地依赖路径 | 对照目录布局，检查应用根 Cargo.toml 中两条 path |
| 编译提示缺少 doris_transaction_compatibility | 根 patch 没生效；检查 workspace 根位置和 sqlx-mysql 路径 |
| MySqlPool 类型不匹配 | 用 cargo tree -i sqlx-mysql 检查是否混入了其他 SQLx 版本 |
| 启动时报 NotPresent（缺少环境变量） | 在运行程序的同一个 shell 设置对应变量 |
| 认证失败或数据库不存在 | 检查账号权限、密码编码、库名和 FE SQL 端口 |
| 编译宏时要求 DATABASE_URL | 设置目标数据库连接，或准备对应离线缓存；普通 query 不需要 |

首次引入 patch 时让 Cargo 正常解析依赖并更新应用 Cargo.lock，再用 `cargo run --locked` 固定解析结果。
源码示例和完整测试的步骤见[使用指南](USAGE.md)与[测试说明](TESTING.md)。
