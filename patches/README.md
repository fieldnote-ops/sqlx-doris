# 随附的 SQLx MySQL 驱动

简体中文 | [English](README.en.md)

本目录的 sqlx-mysql 基于上游 0.9.0，保留包名、版本号和公共 MySql 类型。
修改的是客户端驱动，Doris FE/BE 使用官方版本，无需修改或重新编译服务器。
完整应用接入步骤见 SDK README；源码包还提供 docs/INSTALLATION.md。

## 为什么需要根 patch

在应用的 workspace 根 Cargo.toml 配置：

```toml
[patch.crates-io]
sqlx-mysql = { path = "../sqlx-doris/patches/sqlx-mysql" }
```

path 相对于这个根 Cargo.toml，按实际目录调整。
这样应用直接依赖的 SQLx、SQLx 宏和 SDK 使用同一份 sqlx-mysql，原生连接池、行和事务类型才能互换。
库内部的 patch 不会传给应用，遗漏时会因缺少兼容接口而编译失败。
无需修改全局 Cargo 配置或 registry 中的驱动源码。

## 修改内容

- 接受 Doris 合法的 10 字节 PrepareOk，缺少 warning 字段时按 0 处理，仍拒绝截断或非法响应。
- 通过显式 Doris 连接选项处理顶层写事务、取消和无法安全复用的连接。
- 在支持的 SQL 范围内处理 BLOB、TIME、VALUES、DEFAULT 和分页参数，业务继续使用 `.bind()`。
- 兼容路径在发包前选择，不会在写入失败后重发。类型或 SQL 形状不支持时明确报错。

普通 MySQL 连接保留原生协议。TIME 列表便利是独立选项：驱动默认关闭，SDK 的 MySQL 配置默认开启，可显式关闭。
数据库语义和支持范围见 SDK README，完整列表位于源码包的 docs/COMPATIBILITY.md。

## 来源、许可证和校验

对应上游 commit 003b698e99e024f3621b8043a2426fde5b741171，保留 Apache-2.0 OR MIT 双许可证全文。
sqlx-mysql-provenance.json 记录 73 个上游文件、15 个修改文件和许可证的哈希。
三份 patch 按 prepare-ok、native-transactions、safe-bindings 的顺序重建当前源码。

配置构建盘后，可单独核对源码：

```sh
./scripts/dev ./scripts/check-sqlx-patch
```

从源码根按 docs/TESTING.md 运行 check 生成包后，再用 verify-delivery 核对补丁重建、文件清单和 SDK/驱动配对。
这份驱动随项目提供，尚未另行发布或合并到上游。
