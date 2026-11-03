<a name="doc-top"></a>

# 问题排查

[English](TROUBLESHOOTING.md) | [简体中文](TROUBLESHOOTING.zh-CN.md)

[文档首页](README.zh-CN.md)

从失败的应用操作出发并保留其身份。普通查询或导入失败不要求恢复迁移锁或退役账号。

<a name="doc-contents"></a>

## 目录

- [连接与 TLS](#doc-connection)
- [类型与精度问题](#doc-types)
- [导入未知或部分过滤](#doc-ingestion)
- [慢查询或内存增长](#doc-performance)
- [构建元数据与迁移故障](#doc-tools)

<a name="doc-connection"></a>

## 连接与 TLS

分别核对 doris:// SQL URL、库名、账号、FE 查询端口和 HTTP 导入端点。保留字符需要 URL 编码。需要主机名验证时使用 VerifyIdentity 与可信 CA；Preferred 允许回退明文。目前认证支持 mysql_native_password，见[安装](INSTALLATION.zh-CN.md#doc-upstream)。

池大小应匹配服务端容量。查询取消、结果流未读完或提交未知会淘汰连接；连接池补建是预期行为，不应靠重放写入“修复”。

<a name="doc-types"></a>

## 类型与精度问题

参数含糊时显式 CAST；只有确认逻辑类型和 NULL 约束后才使用查询宏输出覆盖。Doris 表达式元数据不完整，弱参数检查不能推导全部服务端转换。LARGEINT 与 decimal 不要经过 f64；DATETIME 本身没有时区身份，TIMESTAMPTZ 依照配置的会话语义解释。

复杂字符串可能需要 operations::project 或 push_complex_value 适配 Doris CAST 文本语法。不要改成 SQL 插值，见[类型与宏](USAGE.zh-CN.md#doc-types)和[精度边界](COMPATIBILITY.zh-CN.md#doc-precision)。

<a name="doc-ingestion"></a>

## 导入未知或部分过滤

保留原 label、已观察到的 transaction ID、不可变输入和预期业务 key/行数。区分 HTTP 失败、业务失败、过滤、Publish Timeout、异步接受和可见；不要换 label，也不要把服务端查不到记录当作未提交。

核对回执行数并查询业务数据。Unique 表 key 覆盖可能隐藏重放，不证明 exactly-once 消费。原始拒绝行 URL 可能包含数据且需另行授权；手动处理前先阅读[导入结果](INGESTION.zh-CN.md#doc-result)。

<a name="doc-performance"></a>

## 慢查询或内存增长

对比相同 SQL、数据、结果量、服务端设置和网络条件，分别度量池排队、预编译、执行、行解码、JSON 编码、上传与发布。大结果使用 fetch，大文件使用 send_file；fetch_one/fetch_optional 复用前会读完剩余响应。

语句缓存与池健康检查影响点查往返；Group Commit 只适合特定小批场景。真实资源拒绝必须保留。现有小行/短时间测量不能证明生产 OLAP 吞吐或长期无泄漏，见[性能](PERFORMANCE.zh-CN.md)。

<a name="doc-tools"></a>

## 构建元数据与迁移故障

查询元数据过期时，用相同应用特性与 schema 重新 prepare，再执行 --check；--cargo-offline 与 SQLX_OFFLINE 控制不同系统。迁移 dirty 或 owner 仍存在时，先核对源码/历史并停止旧 actor；不能凭锁年龄超过超时就清锁。

[离线构建](USAGE.zh-CN.md#doc-offline)与[迁移](MIGRATIONS.zh-CN.md)解释普通流程。只有实际迁移恢复事故才需要单独选用的[高级恢复手册](RECOVERY.zh-CN.md)；空诊断不证明可以接管。
