# 使用指南

## 连接与会话

Backend::MySql 保留 MySQL 原生协议和事务；Backend::Doris 启用公开协议兼容及 Doris 会话选项。
PoolBuilder 提供池容量、获取超时、idle_timeout、max_lifetime 和 after_connect。
connect_with 接受原生 MySqlConnectOptions，可配置 TLS、认证、时区和 statement cache。
生产连接按部署配置验证 CA/主机名，不应依赖测试夹具的空密码账号。

Doris 默认 GroupCommit::Off、Decimal256 开启、严格 CAST 开启，可显式配置。
MySQL 的 TIME 列表共同域便利默认开启，可用 mysql_time_membership_compatibility(false)恢复原生列表强制转换。
from_pool 只包装已有池，不修改已有连接；connect_any_with 只在建池阶段选择端点，不迁移在途查询。

## 查询与绑定

直接使用 sqlx::query、`query_as`、`query_scalar`、FromRow 及 QueryBuilder，共用函数示例见 README。
用户输入始终通过.bind()，动态 SQL 结构使用受信任模板；每个占位符对应一个值，不自动将 Vec 展开为 IN 列表。
Doris 受限 prepared 路径由驱动在发包前选择受检公共协议，不试错后重发写入。
String、Vec<u8>与 MySqlTime 保留各自参数域；支持的 SQL 形状见[兼容范围](COMPATIBILITY.md)。

## 原生事务

```rust,no_run
# async fn write(pool: &sqlx_doris::MySqlPool) -> Result<(), sqlx_doris::sqlx::Error> {
use sqlx_doris::sqlx;
let mut transaction = pool.begin().await?;
sqlx::query("INSERT INTO events(id, value) VALUES (?, ?)")
    .bind(1_i64)
    .bind("hello")
    .execute(&mut *transaction)
    .await?;
transaction.commit().await?;
# Ok(()) }
```

Doris 支持声明范围内的顶层写事务，事务外读取已提交结果。不模拟 savepoint、事务内查询或跨库事务。
提交/写入丢回包可能无法确定最终结果，调用方应查询业务状态；不要把所有错误统一重试。
事务 drop 或取消按状态回滚/退役连接，未知状态不会重新发送业务写入。

## Doris 类型与操作

`LargeInt`、Time、`TimestampTz`、Array/Map/Struct、Variant、Ipv4/Ipv6、Binary/Hex 位于 types 模块。
复杂值可用 operations::push_complex_value 逐叶绑定，读取时通过显式 project 保留逻辑类型。
VARIANT 的服务器归一化、查询专用类型与 OLAP 存储限制仍存在，不能还原已丢失信息。

operations 提供 TableDefinition/`TableModel`、describe_table、`CatalogTableName`、异步物化视图任务及 wait_until。
指定 Doris 表模型后建表，不把 MySQL 建表 SQL 自动转换成 Doris。异步提交成功仅表示接受，需等待可见或完成。
任务 ID 不保证单调递增，按提交前后的 ID 集合识别新任务，检查 Success/Failed/Canceled。

## Stream Load

默认 stream-load feature 提供 StreamLoader，SQL 与 HTTP 端点分别配置。
支持 JSON、CSV、Parquet、ORC、字节和文件；FE 重定向到 BE 时须确保网络可达，需要时配置 backend_endpoint。
HTTP 成功还须检查 LoadReceipt：重复 Label、过滤、Publish Timeout 和 Prepared 表示不同状态。
不会因响应未知自动重新提交；2PC Prepared 后续提交/回滚由调用方显式处理。
HTTPS/mTLS 按实际 CA、主机名和证书配置，不允许失败后降级明文。

## 宏与 schema

启用 macros 后使用 SQLx `query!`/`query_as!`。两库在线描述与离线缓存分别生成，保持 DTO 与列类型匹配。
DDL 变更后同步更新业务投影及离线元数据；清理 statement cache 不会更新 Rust DTO 或宏缓存。
参见 query_macro 和 doris_query_macro 示例。
