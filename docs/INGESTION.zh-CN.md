<a name="doc-top"></a>

# 批量导入

[English](INGESTION.md) | [简体中文](INGESTION.zh-CN.md)

[文档首页](README.zh-CN.md)

应用批次与文件上传使用 Stream Load，普通小型 SQL 写入继续使用 SQLx。本指南面向已有表，不要求建表管理权限。

<a name="doc-contents"></a>

## 目录

- [选择写入路径](#doc-choose)
- [配置可信 HTTP 客户端](#doc-setup)
- [导入一个 JSON 批次](#doc-json)
- [解释导入回执](#doc-result)
- [上传不可变文件](#doc-files)
- [处理取消与未知结果](#doc-uncertain)
- [Group Commit 与 2PC 边界](#doc-group-commit)

<a name="doc-choose"></a>

## 选择写入路径

小量写入或显式 Doris 写事务使用参数绑定 INSERT；较大应用批次选择 Stream Load。高频小批可评估 Group Commit，但异步回执与可见是不同事件。持续 Kafka 导入交给 Doris Routine Load 或已有 connector，不由驱动接管 Kafka 消费。

Duplicate、Unique、Aggregate 表有不同写入语义。Unique 整行 UPSERT 可能用默认值或 NULL 替换未提供的列；部分更新要求服务端显式设置。Unique 表隐藏重复 key 不证明传输只消费过一次，见 [Doris 表模型](https://doris.apache.org/docs/4.x/key-features/data-model/)。

<a name="doc-setup"></a>

## 配置可信 HTTP 客户端

在同一个 Doris 公共包上启用 stream-load，并配置[外置构建路径](INSTALLATION.zh-CN.md#doc-storage)。官方 SQLx 特性仍按应用需要选择。

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
sqlx-doris = { version = "0.2.0", features = ["stream-load"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

使用可信 FE HTTP 端点、用户名和可选密码构造 StreamLoadClient::new，并复用该 HTTP 客户端。SQL 与 HTTP 使用不同端口；with_client_builder 配置 TLS 根证书与超时，backend_endpoint 显式指定可信私网/容器 BE 映射。端点配置不代表自动故障转移或业务重试。

<a name="doc-json"></a>

## 导入一个 JSON 批次

先创建 events 表并授予所需导入权限。调用此函数之前，持久化应用批次 ID 与不可变 payload 的身份/摘要，将稳定 ID 传入 batch_id；结果未知时不要生成新 ID 重试。此函数只提交一次并返回完整回执。

```rust
use sqlx_doris::{
    operations::{Result, TableName},
    stream_load::{LoadReceipt, StreamLoadClient},
};

pub async fn import_events(
    client: &StreamLoadClient,
    database: &str,
    batch_id: &str,
) -> Result<LoadReceipt> {
    let table = TableName::new(database, "events");
    client
        .load(&table)?
        .label(batch_id)
        .send_json(&[serde_json::json!({"id": 42, "name": "Doris 中文🙂"})])
        .await
}
```

普通导入生成的 label 可在 await 前通过 request.label_ref() 读取，并与批次一起持久化。Group Commit 的标签由服务端分配/共享，此时返回 None。columns、partial_columns、strict_mode、max_filter_ratio 提供显式便利设置；columns 接受可信的 Doris 表达式，不是参数绑定值。

需要 ARRAY/MAP/STRUCT JSON 转换时，load_table 接受描述已有表的 TableDefinition，不负责建表；普通 send_json 使用传入 JSON 结构。应核对 schema 与读回值，特别是复杂类型中的任意字符串。

<a name="doc-result"></a>

## 解释导入回执

应结合 state()、行数与应用批次身份判断结果，不能只看 HTTP 成功。

| LoadState | 含义 / 处理 |
| --- | --- |
| Visible | 服务端报告已发布；核对 loaded、filtered、unselected 行数 |
| Accepted | 异步 Group Commit 已接受数据；另读可见性 |
| Committed | Publish Timeout：已提交，可能尚未可见；不要重发 |
| Unconfirmed | 服务端实际 Group Commit 模式缺失/未知；保留不确定性 |
| Prepared | 2PC 已预备，需独立的外部协调器 |
| AlreadyExists | 核对 finished 与原始批次；不证明 payload 一致 |
| Failed | 通过 Error::Load 返回业务失败及完整回执 |

默认 strict_mode=true、max_filter_ratio=0 拒绝过滤批次。显式放宽过滤比例后，成功结果也可能含被拒绝的行；核对 number_filtered_rows 和 error_url，不自动下载被拒数据。number_unselected_rows 是 where 筛除数量。is_visible() 只是可见性便利判断，不完成业务核对。

<a name="doc-files"></a>

## 上传不可变文件

send_file 从文件流式读取，不缓冲整个文件；send_bytes 与 send_json 在内存中保留完整批次。目前没有任意 Stream/AsyncRead 上传 API。发送开始时固定一个已打开的普通文件，路径被替换也不会让 FE/BE 请求切换到另一文件。每个 body 有独立逻辑游标并有界读取；观察到长度/mtime 变化会拒绝继续。但底层字节仍须保持不可变：这些检查不会冻结文件，无法检测恢复原元数据的修改，也不会撤回已提交的字节。

```rust
use sqlx_doris::{
    operations::{Result, TableName},
    stream_load::{LoadFormat, LoadReceipt, StreamLoadClient},
};
use std::path::Path;

pub async fn import_parquet(
    client: &StreamLoadClient,
    database: &str,
    batch_id: &str,
    path: &Path,
) -> Result<LoadReceipt> {
    client
        .load(&TableName::new(database, "events"))?
        .label(batch_id)
        .format(LoadFormat::Parquet)
        .send_file(path)
        .await
}
```

```rust
use sqlx_doris::{
    operations::{Result, TableName},
    stream_load::{LoadFormat, LoadReceipt, StreamLoadClient},
};
use std::path::Path;

pub async fn import_csv(
    client: &StreamLoadClient,
    database: &str,
    batch_id: &str,
    path: &Path,
) -> Result<LoadReceipt> {
    client
        .load(&TableName::new(database, "events"))?
        .label(batch_id)
        .format(LoadFormat::Csv)
        .columns("id,name")?
        .header("column_separator", ",")?
        .send_file(path)
        .await
}
```

CSV 使用 LoadFormat::Csv 并显式设置 column_separator/columns header；JSON 行使用 LoadFormat::JsonLines，普通 JSON 数组使用 LoadFormat::Json。Parquet/ORC 字节原样发送，不在客户端解码。

<a name="doc-uncertain"></a>

## 处理取消与未知结果

网络失败或 future 取消可能发生在服务端提交之后。保留原始批次 ID 和不可变输入，通过只读接口核对原 label/transaction 及业务数据。Error::Transport、LoadSource、LoadRedirect 在可用时保留普通导入 label；取消 future 无法返回回执，必须在 await 前记录身份。不要自动重发、换新 label 或凭缺失历史推断失败。

observe_label 只执行一次已认证的 FE GET，不跟随重定向或重试，返回 LabelState::Unknown/Prepare/Precommitted/Committed/Visible/Aborted/Other。它不提交新导入，也不 commit/abort；权限、协议及本地失败会报错，不转成 Unknown。Unknown 始终表示无法确认，包括历史过期的情况。该接口不证明 label 与不可变 payload、数据库代际或写入权限的绑定，必须保留原批次证据。

```rust
use sqlx_doris::{
    operations::Result,
    stream_load::{LabelState, StreamLoadClient},
};

pub async fn observe_batch(
    client: &StreamLoadClient,
    database: &str,
    original_label: &str,
) -> Result<LabelState> {
    Ok(client.observe_label(database, original_label).await?.state)
}
```

Doris 默认不记录 Stream Load 历史，历史也可能过期或不可访问。SHOW STREAM LOAD 空结果不证明没有提交；已有 FINISHED label 只证明历史标签已完成，不证明新传入 payload 相同。见[问题排查](TROUBLESHOOTING.zh-CN.md#doc-ingestion)和[官方历史记录要求](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/load-and-export/SHOW-STREAM-LOAD/)。

<a name="doc-group-commit"></a>

## Group Commit 与 2PC 边界

group_commit(GroupCommit::Sync/Async/Off) 选择 HTTP 模式；DorisConnectOptions.group_commit 则为每个新 SQL 连接分别配置。请求模式只是证据，不证明实际模式或可见；异步接受不保证立即可见。Group Commit 使用服务端共享 label，本客户端不允许同时传入应用显式 label。

原始 header 用于其他 Doris 参数。本客户端能表示 Prepared 回执，但没有完整 2PC commit/abort 协调器，也不提供 exactly-once checkpoint sink。没有独立协调器时不要开启 two_phase_commit。见 [Group Commit](https://doris.apache.org/docs/4.x/key-features/group-commit/)与[导入事务](https://doris.apache.org/docs/4.x/key-features/load-transaction/)。
