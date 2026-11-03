<a name="doc-top"></a>

# 安装

[English](INSTALLATION.md) | [简体中文](INSTALLATION.zh-CN.md)

[文档首页](README.zh-CN.md)

**0.2.0 已发布到 crates.io**。需要 Rust 1.94+ 及构建 TLS 依赖的 C 工具链；Python 3.12+ 仅供仓库工具使用，业务程序不需要 Python。

<a name="doc-contents"></a>

## 目录

- [添加依赖](#doc-source)
- [选择特性](#doc-features)
- [使用官方 SQLx](#doc-upstream)
- [配置外置构建目录](#doc-storage)
- [连接与 TLS](#doc-tls)
- [安装配套 CLI](#doc-cli)

<a name="doc-source"></a>

## 添加依赖

在应用 Cargo.toml 中添加 `sqlx-doris`；以下为包含行映射派生的最小运行时配置，内部包无需声明。仓库内开发可改用指向检出目录的 `path` 依赖。

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
sqlx-doris = "0.2.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

<a name="doc-features"></a>

## 选择特性

先通过[使用地图](ARCHITECTURE.zh-CN.md#doc-tasks)选择路径，再通过[接入泳道](ARCHITECTURE.zh-CN.md#doc-flows)区分编译期检查与运行时执行。

| 需要的能力 | `sqlx-doris` 特性 | 官方 `sqlx` 特性 |
| --- | --- | --- |
| 运行时查询、连接池和 Doris 类型 | 无 | 无；使用 `FromRow` 时开启 `derive` |
| 批量导入 | `stream-load`（包含 `operations`） | 由查询代码决定 |
| Schema/MV/可见性助手 | `operations` | 由查询代码决定 |
| Doris 编译期查询 | `macros` | `derive` 或 `macros`；直接依赖名必须为 `sqlx` |
| 嵌入迁移文件 | `migrate` | `macros`、`migrate` |
| 运行时读取迁移目录 | `migrate` | `migrate` |
| 基础 CLI | `cli`（包含 `migrate`） | 由 CLI 管理 |
| Doris 专项管理 | `cli-admin`（包含 `cli`） | 由 CLI 管理 |
| 独立 Kafka 预检 | `cli-admin-kafka`（包含 `cli-admin`） | 由 CLI 管理 |

默认特性为空；JSON、BigDecimal、chrono 和 Doris 逻辑类型直接可用。内部包版本精确对齐，应用只需声明 `sqlx-doris` 这一 Doris 扩展。

<a name="doc-upstream"></a>

## 使用官方 SQLx

SQLx 内置驱动由官方 sqlx 按 mysql/postgres/sqlite 等 feature 导出；通用查询、traits 和连接池实现由 sqlx-core 共享。Doris 是外部驱动，应用使用官方 `sqlx::query`、`sqlx::query_as`、`sqlx::Pool<Doris>`、`sqlx::FromRow` 和 `sqlx::migrate::Migrator`，从 sqlx-doris 导入 Doris 类型与操作。两者复用同一份 sqlx-core，不增加查询转发或运行时开销。[上游入口源码](https://github.com/transact-rs/sqlx/blob/v0.9.0/src/lib.rs)

sqlx-doris 不重新导出 SQLx，也没有 FromRow/migrate! 的重复入口。普通行映射开启官方 sqlx/derive；嵌入迁移开启官方 sqlx/macros + sqlx/migrate，并开启驱动 sqlx-doris/migrate。查询宏使用 `sqlx_doris::query!`，需开启驱动 macros，并将官方 sqlx 以 sqlx 名称声明为直接依赖，开启上游 derive 或 macros 以提供宏运行时辅助。

上游 query! 和 cargo sqlx 的驱动集合尚未注册 Doris，因此 Doris 查询检查和 prepare 使用本驱动的宏与 cargo sqlx-doris；通用 `sqlx::migrate!` 不依赖数据库注册，可直接生成供 Doris 使用的 Migrator。[上游外部驱动说明](https://github.com/transact-rs/sqlx/blob/v0.9.0/FAQ.md)

<a name="doc-storage"></a>

## 配置外置构建目录

在应用终端中，先把挂载点占位符替换为已独立挂载的外置硬盘，再执行以下命令。直接调用 Cargo 同样需要这些环境变量；仓库包装脚本不会替其他应用终端配置环境。

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
export CARGO_TARGET_DIR="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/app-target"
export CARGO_BUILD_BUILD_DIR="$CARGO_TARGET_DIR"
export CARGO_HOME="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/cargo-home"
export TMPDIR="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/tmp-clean"
export SQLX_OFFLINE_DIR="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/offline/app"
mkdir -p "$CARGO_TARGET_DIR" "$CARGO_HOME" "$TMPDIR" "$SQLX_OFFLINE_DIR"
```

从仓库根目录执行开发命令时使用 `./scripts/dev <command>`，它会验证挂载点并配置构建、缓存和临时目录。启动本地服务器前还需核查 Docker/Lima 磁盘及全部数据、日志挂载，见[测试说明](TESTING.zh-CN.md#doc-storage)。迁移 SQL 属于应用源码，可放在 `migrations/`。

<a name="doc-tls"></a>

## 连接与 TLS

运行时代码读取 `DORIS_URL`；目标数据库需提前存在。以下是供应用调用的函数，请加入自己的程序。

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

默认 TLS 策略为 Preferred：FE 提供 TLS 时加密，否则使用明文。Required 要求加密，VerifyCa 校验证书链，VerifyIdentity 还校验主机名。`ssl_client_cert`/`ssl_client_key` 配置双向 TLS；`DorisPoolOptions` 提供 SQLx 连接池 hooks。连接选项还包含 `max_packet_size`（默认 64 MiB）、`group_commit`、`decimal256` 和 `strict_cast`。

连接地址为 `doris://user:password@fe:9030/database`，保留字符需做 URL 编码。URL 参数包括 `ssl-mode`、`ssl-ca`、`ssl-cert`、`ssl-key`、`statement-cache-capacity`；未知参数直接报错。认证方式为 mysql_native_password。

<a name="doc-cli"></a>

## 安装配套 CLI

将已发布的 crate 安装到外置前缀，并将其 bin 目录加入终端 PATH。

选择一个安装配置：cli 只有 prepare/database/普通 migrate；cli-admin 增加 admin 下的 Doris 维护；cli-admin-kafka 再增加独立 Kafka 只读预检。三档使用同一个可执行文件；更换已安装配置时显式使用 cargo install --force。下例安装基础配置，按需要替换 --features 的值。

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
cargo install sqlx-doris --version 0.2.0 --features cli --locked \
  --root "$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/tools"
export PATH="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/tools/bin:$PATH"
cargo sqlx-doris --help
```

使用 `cargo sqlx-doris`，而不是官方 `cargo sqlx`。单独安装 CLI 不会开启查询宏或 Stream Load。详见 [CLI 参考](CLI.zh-CN.md)和[异常恢复流程](RECOVERY.zh-CN.md)。
