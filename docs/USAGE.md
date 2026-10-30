# 使用指南

简体中文 | [English](USAGE.en.md)

先按[安装指南](INSTALLATION.md)跑通查询。下面的 Rust 函数可放入已有的 Tokio 应用；SQL、HTTP 地址和账号由应用配置。

## 连接和连接池

用 `PoolBuilder::new(Backend::MySql)` 或 `PoolBuilder::new(Backend::Doris)` 选择后端，再调用 connect。
两种后端都使用原生 SQLx MySql 类型，Doris 的 URL 也以 mysql:// 开头。
池容量和超时可通过 max_connections、acquire_timeout、idle_timeout、max_lifetime 设置。

需要 TLS 校验时，可以传入原生连接选项：

```rust,no_run
use sqlx::mysql::{MySqlConnectOptions, MySqlSslMode};
use sqlx_doris::{Backend, Client, PoolBuilder};
use std::{path::Path, str::FromStr};

pub async fn connect_doris_tls(url: &str, ca_file: &Path) -> sqlx_doris::Result<Client> {
    let options = MySqlConnectOptions::from_str(url)?
        .ssl_mode(MySqlSslMode::VerifyIdentity)
        .ssl_ca(ca_file);
    PoolBuilder::new(Backend::Doris).connect_with(options).await
}
```

CA 文件由部署提供，URL 主机名要和证书一致。
也可以通过 MySqlConnectOptions 的 host、username、password 方法分别设置原始参数，避免手动编码密码。

Doris 默认关闭 Group Commit，并开启严格 CAST 与 Decimal256，可通过 PoolBuilder 修改。
MySQL 的 TIME 列表兼容默认开启；依赖原生 MySQL 列表类型转换时，设置 mysql_time_membership_compatibility(false)。
`from_pool` 只包装现有池；`connect_any_with` 只在建池时选择端点，不迁移执行中的查询。

## 查询和参数

继续使用 `sqlx::query`、`query_as`、`query_scalar`、FromRow 和 QueryBuilder。
值通过 `.bind()` 传入；动态表名、列名等 SQL 结构由应用提供受信任的模板。
一个占位符对应一个值，Vec 不会自动展开成 IN 列表。

驱动会处理支持范围内的 Doris 预处理限制，调用方仍用原 SQL 和 `.bind()`。
不支持的 SQL 形状会明确报错。BLOB、TIME、默认值和分页的具体范围见[兼容文档](COMPATIBILITY.md)。

## 写入事务

下面的函数假设已有 sdk_demo_events 表。演示建表 SQL 分别为：

MySQL：

```sql
CREATE TABLE sdk_demo_events (
    id BIGINT NOT NULL PRIMARY KEY,
    note VARCHAR(128)
);
```

Doris：

```sql
CREATE TABLE sdk_demo_events (
    id BIGINT NOT NULL,
    note VARCHAR(128)
)
UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('replication_num' = '1');
```

Doris 的单副本配置适用于单 BE 演示，实际部署按集群副本要求调整。
建好表后，两库可调用同一个函数：

```rust,no_run
pub async fn insert_event(pool: &sqlx::MySqlPool, id: i64, note: &str) -> sqlx::Result<()> {
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO sdk_demo_events(id, note) VALUES (?, ?)")
        .bind(id)
        .bind(note)
        .execute(&mut *transaction)
        .await?;
    transaction.commit().await?;
    Ok(())
}
```

Doris 写事务的读取放到事务外。savepoint、事务内查询/DDL、跨库事务不在兼容范围内。
写入或提交丢失回包时，业务需要查询最终状态，不能把所有错误统一重试。
事务取消或 drop 后，驱动会回滚，或关闭无法安全复用的连接；不会重发不确定的业务写入。

## Doris 类型和操作

`types` 模块包含 LargeInt、BigDecimal、Time、TimestampTz、Array、Map、Struct、Variant、IP 和 Binary/Hex。
复杂值可用 `operations::push_complex_value` 逐叶绑定，读取时用显式投影保留逻辑类型。
VARIANT 的归一化、Decimal 舍入和 OLAP 存储类型限制由 Doris 决定。

`operations` 提供 TableDefinition、TableModel、元数据和异步物化视图任务接口。
建表时显式选择 Doris 表模型；异步操作要继续检查任务状态或数据可见性。
完整建表和类型往返示例见 [native_types.rs](../crates/sqlx-doris/examples/native_types.rs)。

## Stream Load

默认的 stream-load feature 提供 `stream_load::StreamLoadClient`。
SQL 连接使用 FE SQL 端口，HTTP 导入使用 FE HTTP 地址；重定向到 BE 后，该地址也必须可达。
需要覆盖内网 BE 地址时，调用 backend_endpoint。

以下函数向已有表导入 JSON，使用默认 JSON 数组格式，不需要额外的 serde_json 依赖：

```rust,no_run
use sqlx_doris::{
    operations::TableName,
    stream_load::{LoadReceipt, StreamLoadClient},
};

pub async fn import_events(
    http: &StreamLoadClient,
    table: &TableName,
) -> sqlx_doris::Result<LoadReceipt> {
    http.load(table)?
        .send_bytes(br#"[{"id":2,"note":"loaded"}]"#.to_vec())
        .await
}
```

StreamLoadClient::new 接收 FE HTTP 地址、账号和可选密码，
TableName::new 接收库名和表名。完整配置和清理见 [stream_load.rs](../crates/sqlx-doris/examples/stream_load.rs)。
复杂的 ARRAY/MAP/STRUCT 列使用 `load_table(&TableDefinition).send_json(...)`，由表定义指导编码；
普通 load(table).send_bytes() 按原始数据上传。

返回后检查 LoadReceipt::state() 和 is_visible()：接受请求、提交和可见是不同状态。
重复 Label、过滤行、Publish Timeout、2PC Prepared 需要分别处理；Prepared 的后续提交/回滚由调用方显式执行。
HTTPS 或 mTLS 使用 with_client_builder 配置自有 CA/证书，校验失败不会降级为明文。

## 运行源码示例

在源码根目录先配置构建盘和连接变量，配置方式见[README](../README.md#先跑一个查询)。

| 示例 | 额外前提 | 命令 |
| --- | --- | --- |
| dual_pool | 两库可以执行 SELECT，不建表 | ./scripts/dev cargo run --locked -p sqlx-doris --example dual_pool |
| native_types | DORIS_URL 指向已有库，账号可建表、写入、查询和删表 | ./scripts/dev cargo run --locked -p sqlx-doris --example native_types |
| stream_load | 同上，另设 HTTP 端点和导入账号 | ./scripts/dev cargo run --locked -p sqlx-doris --example stream_load |

native_types 和 stream_load 创建随机后缀的演示表，正常结束时删除。
运行 stream_load 前设置：

```sh
export DORIS_HTTP_URL='http://doris-fe:8030'
export DORIS_HTTP_USER='user'
export DORIS_HTTP_PASSWORD='password'
# BE 返回的地址不可直达时才设置：
export DORIS_BE_HTTP_URL='http://doris-be:8040'
./scripts/dev cargo run --locked -p sqlx-doris --example stream_load
```

这些变量分别是 FE HTTP、HTTP 账号密码和可选的 BE HTTP 地址，不能填成 SQL 端口。

## 编译期宏

开启 sqlx-doris 的 `macros` feature 后可用 `sqlx::query!`、`query_as!`。
编译时用 DATABASE_URL 指向目标数据库，或设置 SQLX_OFFLINE=true 并提供对应离线缓存。
两库的缓存分别准备，表结构变更后重新生成；清空 statement cache 不会更新 Rust DTO 或宏缓存。

源码已有 query_macro.rs 和 doris_query_macro.rs。下面的命令会分别验证在线编译、离线缓存和缺缓存错误：

```sh
export MYSQL_TEST_URL="$MYSQL_URL"
export DORIS_TEST_URL="$DORIS_URL"
./scripts/dev ./scripts/test-macros
```

该命令只查询常量，不建业务表。运行全部示例或完整双库测试，见[测试说明](TESTING.md)。
