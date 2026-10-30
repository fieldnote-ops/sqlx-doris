# SQLx MySQL companion

sqlx-mysql/基于上游0.9.0、commit 003b698e99e024f3621b8043a2426fde5b741171。
保留上游crate名称/版本、全部公共MySql类型及Apache-2.0 OR MIT许可，两份许可证全文随源码交付。

## 应用安装

在应用workspace根Cargo.toml配置，路径按实际目录调整：

```toml
[patch.crates-io]
sqlx-mysql = { path = "../sqlx-doris/patches/sqlx-mysql" }
```

这会统一SDK、宏和应用直接SQLx依赖的类型身份。库依赖的patch不会自动传递，遗漏会因缺少companion API编译失败。
不修改全局Cargo配置或registry缓存。SDK/driver配套包由scripts/package-compat生成，scripts/test-consumer验证实际安装。

## 改动范围

- 接受合法10字节PrepareOk，缺少warning字段时按0处理，拒绝截断/非法响应。
- 显式Doris选项处理顶层原生事务、读结果到BEGIN的安全边界、控制取消与未知状态的连接退役。
- 事务及受限prepared路径在发包前选择SQL词法/类型编码的公开协议绑定，不错误后重发。
- 声明范围的BLOB条件、TIME读取/列表、VALUES/DEFAULT与整数分页保留原参数API。
- MySQL普通连接保留原生SQL/协议；TIME列表便利为连接选项，默认关闭，SDK MySQL profile显式开启且可关闭。
- 原生TIME负零、符号/小数和范围校验修复共用；无损编码失败明确报错。

Doris方案不修改FE/BE，不猜schema/default，不拆批、不隐式合并事务，不重放不确定写入。
复杂表达式保持服务器语义或明确拒绝。完整支持范围见源码仓库docs/COMPATIBILITY.md。

## 来源与核验

sqlx-mysql-provenance.json登记73个上游文件、15个reviewed改动和两份许可证的哈希。
prepare-ok.patch、native-transactions.patch、safe-bindings.patch顺序应用后应与当前源一致。

```sh
./scripts/dev ./scripts/check-sqlx-patch
./scripts/dev ./scripts/verify-delivery
```

verify-delivery同时检查精确SDK/driver配对与manifest，不接受未登记源或服务器二进制。
此companion未另行发布，也未声明已合并上游。
