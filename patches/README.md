# 随附的 SQLx MySQL 驱动

简体中文 | [English](README.en.md)

本仓库 `crates/sqlx-mysql` 基于上游 0.9.0，保留包名、版本号和公共 MySql 类型。
修改的是客户端驱动，Doris FE/BE 使用官方版本，无需修改或重新编译服务器。
完整应用接入步骤见 SDK README；源码包还提供 docs/INSTALLATION.md。

## 集成方式

SDK 用 path 依赖引入 `crates/sqlx-mysql`，应用只声明 SDK 依赖即可，不需要根 manifest 的补丁段：

```toml
[dependencies]
sqlx-doris = { path = "../sqlx-doris/crates/sqlx-doris" }
```

驱动随 SDK 一起构建，查询、参数绑定和连接池从 `sqlx_doris::sqlx` 使用，原生 MySql/Pool/Row 类型只有一份。
为了 `FromRow` 派生宏另外声明上游 SQLx 时，只开 `default-features = false, features = ["derive"]`，
不会带入第二份驱动。
需要编译期宏（`query!`、`query_as!`）时才改用上游 SQLx 的 `mysql`、`macros` 特性，
并把 `sqlx-mysql` 指向本目录；这条路径需要根 manifest 的补丁段，宏展开的 `::sqlx::` 路径才与本包一致。

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

从源码根按 docs/TESTING.md 运行 check 生成配套包后，再用 verify-delivery 核对补丁重建、文件清单和 SDK/驱动配对。
这份驱动随项目提供，并已作为 `sqlx-doris-mysql` 0.9.0 发布到 crates.io（由 `sqlx-doris` 依赖引用）；尚未合并到上游 SQLx。
