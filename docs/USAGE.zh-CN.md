<a name="doc-top"></a>

# 查询、类型与离线构建

[English](USAGE.md) | [简体中文](USAGE.zh-CN.md)

[文档首页](README.zh-CN.md)

先完成[安装](INSTALLATION.zh-CN.md)。通用 API 使用官方 SQLx，Doris 类型与编译期查询由本驱动提供。

<a name="doc-contents"></a>

## 目录

- [查询与连接池](#doc-queries)
- [每个连接的会话设置](#doc-sessions)
- [事务与可见性](#doc-transactions)
- [Doris 类型与操作](#doc-types)
- [Stream Load](#doc-stream-load)
- [异步 MV 刷新与观察](#doc-mv)
- [编译期查询宏](#doc-macros)
- [prepare 与离线构建](#doc-offline)
- [错误类型](#doc-errors)

<a name="doc-queries"></a>

## 查询与连接池

将原生 `DorisPool` 直接传给官方 `sqlx::query`、`query_as`、`query_scalar`、`raw_sql` 或 `QueryBuilder`，数据使用 `.bind()`。结构体映射使用官方 `#[derive(sqlx::FromRow)]`，也支持元组。可直接复制[快速开始](../README.zh-CN.md#doc-quickstart)的完整示例。SQLx 0.9 的动态 SQL 使用 `sqlx::AssertSqlSafe`；标识符使用可信构建器，数据不要拼入 SQL。

每个连接默认缓存 100 条预编译语句；`statement_cache_capacity(0)` 关闭缓存，`persistent(false)` 关闭单次缓存。`fetch` 流式读取；`fetch_one`/`fetch_optional` 在复用前读完剩余响应，大结果应使用流。

<a name="doc-sessions"></a>

## 每个连接的会话设置

沿用 SQLx 的 after_connect 为每个新物理连接设置会话参数，池补建连接时也会执行。不需要增加另一套客户端或为每个 Doris 变量建立 options API。以下配方已在 4.1.5 验证，只改变会话，不修改全局策略或配额。

```rust
use sqlx_doris::{DorisPool, DorisPoolOptions};

pub async fn connect_application(url: &str) -> sqlx::Result<DorisPool> {
    DorisPoolOptions::new()
        .max_connections(8)
        .after_connect(|connection, _| {
            Box::pin(async move {
                sqlx::query("SET time_zone = '+00:00'")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET query_timeout = 30")
                    .execute(&mut *connection)
                    .await?;
                sqlx::query("SET workload_group = 'normal'")
                    .execute(&mut *connection)
                    .await?;
                Ok(())
            })
        })
        .connect(url)
        .await
}
```

normal 是本例的既有工作负载组；实际应用选择已授权的组。不要将不可信组名或其他设置拼成 SQL。query_timeout 是服务端 SQL 执行设置，不等同于池排队、网络或整个应用操作超时。现有 DorisConnectOptions.group_commit、decimal256 和 strict_cast 仍用于其专门的连接初始化。见 [Doris 工作负载组](https://doris.apache.org/docs/4.x/key-features/workload-group/)。

<a name="doc-transactions"></a>

## 事务与可见性

事务遵循 Doris 的写事务限制，不支持嵌套事务、savepoint 和自定义 BEGIN。事务内绑定经过类型与词法校验后使用 COM_QUERY；普通查询优先使用二进制预编译协议。显式 rollback 正常结束事务；未完成的 guard 被丢弃时关闭连接，由服务器处理断连回滚。

查询取消、结果流未读完或提交被中断时，连接不会复用。独占连接此后需关闭；连接池自动淘汰并补建连接。不自动重放写入；提交结果不确定时应通过业务标识查询最终状态。

DorisConnectOptions 的 group_commit(GroupCommit::Sync/Async/Off) 对每个新连接生效。Async 返回成功不代表数据立即可见；operations::wait_until 只轮询读取条件，不重提交写入。

以下函数要求 events 表已存在：

```rust
pub async fn insert_event(pool: &sqlx_doris::DorisPool) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO events(id, name) VALUES (?, ?)")
        .bind(1_i64)
        .bind("Doris")
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await
}
```

<a name="doc-types"></a>

## Doris 类型与操作

支持 LargeInt、Time、BigDecimal、TimestampTz、Ipv4、Ipv6、Binary、Hex、Array、Map、Struct、Variant。TIME 是有符号时长，范围 ±838:59:59.999999；超过微秒精度会报错。复杂类型的任意字符串往返使用 operations::push_complex_value 和 project，避免 Doris 文本语法的转义限制。

| Doris 类型 | Rust 类型 | 日常注意点 |
| --- | --- | --- |
| BOOLEAN / integers | bool; i8/i16/i32/i64 | 范围检查；含糊表达式元数据可能需要 CAST/类型覆盖 |
| LARGEINT | sqlx_doris::types::LargeInt | 有符号 i128，不经过浮点数 |
| DECIMAL / DECIMAL256 | sqlx_doris::types::BigDecimal | 保留十进制文本及服务端支持的精度/标度 |
| DATE / DATETIME | chrono::NaiveDate / NaiveDateTime | 没有时区身份；不支持的亚微秒精度直接拒绝 |
| TIMESTAMPTZ | sqlx_doris::types::TimestampTz | DateTime<FixedOffset>；显示受会话时区影响 |
| TIME | sqlx_doris::types::Time | 有符号时长 ±838:59:59.999999，不能作为 OLAP 列类型 |
| IPv4 / IPv6 | sqlx_doris::types::Ipv4 / Ipv6 | 包装 std::net 地址的显式适配器 |
| Binary | sqlx_doris::types::Binary / Hex | 需要时将 Hex 绑定给显式 UNHEX(?) |
| ARRAY / MAP / STRUCT | sqlx_doris::types::Array / Map / Struct | 复杂字符串读取可能需要 operations::project |
| JSON / VARIANT | sqlx::types::Json / sqlx_doris::types::Variant | 保留文档所述标量根与大数服务端边界 |
| NULL | Option<T> | T 对应逻辑结果；无保证时不要强制宏非空 |

chrono 和 serde_json 是使用相应名字时显式声明的应用依赖。BigDecimal 与 Doris 包装类型直接从 sqlx_doris::types 导入。表模型和导入标签的去重语义见[批量导入](INGESTION.zh-CN.md#doc-choose)，它们不证明网络 exactly-once。

Schema 使用 TableName、TableDefinition、Column、DataType、TableModel 等构建器，选择 Duplicate、Unique 或 Aggregate 表模型。开启 `operations`，将 `&DorisPool` 直接传给 `create_table`、`describe_table`、物化视图刷新与等待等自由函数；不需要额外客户端包装。表模型、受影响行数和异步可见性保持 Doris 自身语义。

示例：

```sh
./scripts/dev cargo run --locked --example query
./scripts/dev cargo run --locked --features operations --example native_types
```

先设置 DORIS_URL，示例会查询已存在的数据库。native_types 临时创建并删除单副本演示表。

源码示例与基准只在成功后删除自有测试表/库；操作失败时保留命名现场与外置证据，供核对使用。不要为清理现场重复提交结果未知的写入。

operations 的等待助手在时钟无法表示 timeout 时返回 InvalidInput；超长 interval 只等待到截止时间，不在截止后开始下一次读取。零 timeout 仍允许一次立即完成的观察。物化视图 cancel_materialized_view_task 与 wait_materialized_view_task 要求正数 TaskId，在获取连接前拒绝无效值。迁移锁 timeout 无法表示时返回 SQLx Configuration 错误，不创建协调元数据。

<a name="doc-stream-load"></a>

## Stream Load

向已有表导入 JSON、CSV 或 Parquet/ORC 文件，见[批量导入](INGESTION.zh-CN.md)。指南给出最小 stream-load 依赖、可复制 Rust 片段、label/过滤行/可见性处理和未知结果边界。send_file 从文件流式读取；send_bytes/send_json 保留整个内存批次，当前无任意 Stream/AsyncRead API。所有写入均不自动重试。

<a name="doc-mv"></a>

## 异步 MV 刷新与观察

开启 operations 并操作已有异步物化视图。request_refresh 只确认提交；服务端没有在这个接口中原子返回任务身份。先通过可信的任务/父视图证据核对目标，再把已确认的 TaskId 交给观察函数。任务 ID 不单调，不能取“最新/最大”值当作自己的提交；并发刷新时尤其不能猜测归属。

```rust
use sqlx_doris::{
    DorisPool,
    operations::{
        MaterializedViewTask, RefreshMode, Result, TableName,
        refresh_materialized_view, wait_materialized_view_task,
    },
};
use std::time::Duration;

pub async fn request_refresh(pool: &DorisPool, view: &TableName) -> Result<()> {
    refresh_materialized_view(pool, view, RefreshMode::Auto).await
}

pub async fn observe_known_task(
    pool: &DorisPool,
    view: &TableName,
    verified_task_id: i64,
) -> Result<MaterializedViewTask> {
    wait_materialized_view_task(
        pool,
        view,
        verified_task_id,
        Duration::from_secs(60),
        Duration::from_millis(250),
    )
    .await
}
```

检查返回任务的 status：Success、Failed 和 Canceled 都是终态，后两者不会被转换为成功。超时不会取消服务端任务，也不重新提交刷新。任务成功不等于整个 MV 的业务新鲜度已核对。完整 schema DSL、后台调度与自动刷新编排不属于这些助手；专项任务取消及代际核验见[高级恢复](RECOVERY.zh-CN.md#doc-mv-tasks)。

<a name="doc-macros"></a>

## 编译期查询宏

开启 sqlx-doris 的 macros 特性，从驱动调用查询宏；通用函数与 traits 继续使用官方 sqlx：

```rust
pub async fn checked(pool: &sqlx_doris::DorisPool) -> sqlx::Result<()> {
    let row = sqlx_doris::query!(
        "SELECT CAST(? AS BIGINT) AS `id!: i64`,CAST(? AS STRING) AS `name!: String`",
        42_i64,
        "Doris"
    )
    .fetch_one(pool)
    .await?;
    assert_eq!(row.id, 42);
    Ok(())
}
```

支持 query!、query_as!、query_scalar!、query_file!、query_file_as!、query_file_scalar!；参数使用位置占位符 `?`。SQL 必须是字面量、字面量拼接或 SQL 文件，动态 SQL 继续使用 query/bind。query_as! 按字段名构造记录，无需 FromRow。驱动依赖重命名为 dbkit 后，使用 `dbkit::query!`，Doris 类型推导仍可用。官方 sqlx 必须作为直接依赖并使用 sqlx 名称，开启 derive 或 macros 特性以提供上游辅助设施；宏遵循上游生成代码的路径约定。

在线构建时设置 DATABASE_URL，宏仅 PREPARE 和读取元数据，不执行业务 SQL。会检查 SQL、参数个数、可编码参数和输出记录类型。Doris 不提供可靠的参数类型描述，因此使用 SQLx 的弱参数检查，不承诺在编译时发现每一种服务器参数类型错误；需要明确类型时写 CAST(? AS BIGINT) 等表达式。

Doris 某些表达式把 LARGEINT、复杂类型和未绑定占位符都报告为字符串或 BLOB。直接表列会按原始列来源读取原生 DESC，一次查询同一张表的列描述，补充 BOOLEAN、LARGEINT 等逻辑类型；仍有歧义时宏报错，使用标准 SQLx 别名覆盖：

```sql
SELECT CAST(1 AS LARGEINT) AS `wide!: sqlx_doris::types::LargeInt`,
       ARRAY(1, 2) AS `items!: sqlx_doris::types::Array<i32>`
```

`name!` 强制非空，`name?` 强制可空，`name: Type` 覆盖类型。默认按服务器元数据保守推导 Option；只有确认非空后才使用 `!`。复杂类型显式指定元素类型；表投影的覆盖建议逐列书写并使用 AS 与反引号。编译账号需要读取对应表的原生 DESC 元数据。information_schema 可能把 BOOLEAN 归一化为 TINYINT(1)，因此以原生描述为准；缺乏来源的表达式仍可能需要 `: bool` 等显式覆盖。

<a name="doc-offline"></a>

## prepare 与离线构建

```sh
export DATABASE_URL='doris://app:secret@doris-fe:9030/app'
export SQLX_OFFLINE_DIR="$SQLX_DORIS_EXTERNAL_VOLUME/sqlx-doris/offline/app"
cargo sqlx-doris prepare --output "$SQLX_OFFLINE_DIR"
cargo sqlx-doris prepare --check --output "$SQLX_OFFLINE_DIR"
# Workspace/features: prepare -- --workspace --all-features
SQLX_OFFLINE=true cargo check
```

离线 JSON 使用 SQLx 的 query hash 与 Describe 格式，数据库名为 Doris。将这份缓存作为构建输入，在 CI 中恢复到外置盘并设置 SQLX_OFFLINE_DIR；不要把 .sqlx 缓存写回本目录。prepare 会重新编译应用包，`--check` 对照当前数据库检查缓存差异。`--cargo-offline` 只控制 Cargo 依赖下载，SQLX_OFFLINE 才控制查询元数据模式。prepare 默认检查所有 targets，可通过 `--` 传递 features/package/workspace 等 Cargo 选项。输出目录和工作区均通过操作系统文件锁防止并行 prepare，退出时自动释放；保留的 .sqlx-doris-prepare.lock 文件不要手动删除。元数据先完整暂存，再逐文件原子替换并清理旧 hash 文件，其他文件保留。--check 还校验 query/hash 一致性；缓存 JSON 损坏时可直接重新 prepare。发布期间的 I/O 错误修复后应重跑 prepare，再用 --check 确认。

prepare 从目标 manifest 所在目录运行 Cargo，读取该应用及祖先目录的 `.cargo/config.toml`。额外 Cargo 参数中的相对路径以该目录为基准。`--cargo-offline` 同时应用于 metadata、clean、check；构建、缓存、临时目录仍须在已验证的外置盘。

<a name="doc-errors"></a>

## 错误类型

查询、连接和显式 core 恢复原语返回 `sqlx::Error`；SQLx 迁移使用 `sqlx::migrate::MigrateError`。Schema/MV/Stream Load 助手使用 `sqlx_doris::operations::Error/Result`（`stream_load` 同样提供）；Database 变体保留原 SQLx 错误。本地 Time 构造、解析使用 `sqlx_doris::types::TimeError`。

等待助手的 timeout 超出时钟范围时在读取前返回 InvalidInput；超大 interval 只等待到截止时间，零 timeout 允许一次立即完成的观察。MV 取消与等待在获取连接前要求正 TaskId。迁移锁 timeout 溢出时，在创建协调元数据前返回 SQLx Configuration。
