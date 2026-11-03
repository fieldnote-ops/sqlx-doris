<a name="doc-top"></a>

# sqlx-doris

[English](README.md) | [简体中文](README.zh-CN.md)

遵循 SQLx 使用习惯的原生 Apache Doris 驱动，提供精确类型、流式查询和可选的 Stream Load 批量导入。通用查询、连接池、FromRow 和迁移使用官方 `sqlx`；Doris 能力统一来自 `sqlx-doris`，应用无需声明内部包。编译期查询与迁移工具按需开启，高级异常恢复单独选用。

**0.2.0 尚未发布**。需要 Rust 1.94+ 和 C 工具链，验证基线为 Doris 4.1.5 与 SQLx 0.9.0。仅提供 Doris，不需要 sqlx-mysql 包或驱动补丁。

<a name="doc-contents"></a>

## 目录

- [运行第一条查询](#doc-quickstart)
- [按需开启能力](#doc-features)
- [选择指南](#doc-guide)
- [在源码仓库中开发](#doc-develop)
- [许可证](#doc-license)

<a name="doc-quickstart"></a>

## 运行第一条查询

将完整源码检出目录放在应用旁，按实际位置修改下方依赖路径，并配置[外置构建目录](docs/INSTALLATION.zh-CN.md#doc-storage)。目标库与账号需预先存在；此运行时示例不需要编译时 DATABASE_URL。

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
sqlx-doris = { path = "../sqlx-doris-clean/crates/sqlx-doris" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

把以下完整程序放入 src/main.rs：

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

设置 DORIS_URL 为 `doris://user:password@fe:9030/database`，再运行应用 cargo run。凭证、库名中的保留字符需 URL 编码。

<a name="doc-features"></a>

## 按需开启能力

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

默认特性为空。编译期查询使用 `sqlx_doris::query!`，嵌入迁移文件使用官方 `sqlx::migrate!`，详见[安装说明](docs/INSTALLATION.zh-CN.md#doc-upstream)。

<a name="doc-guide"></a>

## 选择指南

[![使用地图：原生查询、导入、编译期 SQL、迁移、助手与诊断](docs/assets/task-map.zh-CN.svg)](docs/ARCHITECTURE.zh-CN.md#doc-tasks)

从[使用地图](docs/ARCHITECTURE.zh-CN.md#doc-tasks)选路，查看[控制与执行路径](docs/ARCHITECTURE.zh-CN.md#doc-flows)，或理解[五包架构](docs/ARCHITECTURE.zh-CN.md#doc-packages)。静态图与文字说明另有本地交互地图配套。

- [安装](docs/INSTALLATION.zh-CN.md)：源码依赖、特性、外置路径与 TLS。
- [日常查询与类型](docs/USAGE.zh-CN.md)、[批量导入](docs/INGESTION.zh-CN.md)和[问题排查](docs/TROUBLESHOOTING.zh-CN.md)。
- [编译期查询与离线构建](docs/USAGE.zh-CN.md#doc-macros)、[SQLx 迁移](docs/MIGRATIONS.zh-CN.md)和 [CLI](docs/CLI.zh-CN.md)。
- 高级维护：[异常恢复](docs/RECOVERY.zh-CN.md)、[受管 Routine Load](docs/ROUTINE_LOAD.zh-CN.md)、[Kafka 接续预检](docs/KAFKA.zh-CN.md)。
- [性能](docs/PERFORMANCE.zh-CN.md)、[兼容范围](docs/COMPATIBILITY.zh-CN.md)、[测试](docs/TESTING.zh-CN.md)、[依赖](docs/DEPENDENCIES.zh-CN.md)和[版本变化](CHANGELOG.zh-CN.md)。
- [完整文档与术语](docs/README.zh-CN.md)。

<a name="doc-develop"></a>

## 在源码仓库中开发

仓库工具需要 Python 3.12+。从仓库根目录执行，并使用已挂载的物理外置盘：

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute-external-mount>"
./scripts/dev cargo fetch --locked
./scripts/dev ./scripts/check
```

包装脚本将 target/build、缓存、下载、临时文件和日志放在外置盘；断盘时直接失败。本地服务器启动前，Docker/Lima 磁盘和服务数据也必须外置，见[测试说明](docs/TESTING.zh-CN.md)。

<a name="doc-license"></a>

## 许可证

Apache-2.0；依赖保留各自许可证。
