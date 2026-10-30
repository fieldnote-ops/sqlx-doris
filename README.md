# sqlx-doris

让 MySQL 和 Apache Doris 共用 Rust SQLx 的查询、参数绑定和结果映射。
连接时选择后端，业务继续使用原生 `MySqlPool`、`FromRow`、`QueryBuilder` 和 `.bind()`。
表模型、事务、受影响行数和数据可见性仍按各数据库自身的规则处理。

0.1.0 尚未公开发布。已测试 SQLx 0.9.0、MySQL 8.4.13 和官方 Doris 4.1.5；要求 Rust 1.94+。
安装时需要同时使用本库和随附的 sqlx-mysql 驱动补丁，应用根的 Cargo.toml 要配置一次 `[patch.crates-io]`。
完整接入步骤见[安装指南](docs/INSTALLATION.md)。

## 先跑一个查询

在源码根目录运行。需要 Rust/Cargo、C 编译工具和 Python 3.12+，以及可访问的 MySQL、Doris 实例。
把下面的路径、账号和地址换成自己的配置；URL 中的 app 库需要已存在。
示例只查询常量，不建表，也不写入数据。

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<已挂载构建盘的绝对路径>"
export MYSQL_URL='mysql://user:password@mysql-host:3306/app'
export DORIS_URL='mysql://user:password@doris-fe:9030/app'
./scripts/dev --print-env
./scripts/dev cargo fetch --locked
./scripts/dev cargo run --locked -p sqlx-doris --example dual_pool
```

成功后会输出 `MySQL and Doris returned User { id: 42, ... }`。
构建、缓存和临时文件写入所选构建盘；盘未挂载时会直接失败。
Python 只用于源码脚本，使用 SDK 的业务程序不需要 Python。

## 接入和使用

- [安装指南](docs/INSTALLATION.md)：源码或配套包安装、完整程序、连接配置和常见错误。
- [使用指南](docs/USAGE.md)：连接池、事务、Doris 类型、Stream Load 和可运行示例。
- [兼容范围](docs/COMPATIBILITY.md)：BLOB、TIME、默认值、分页及两库语义差异。
- [测试说明](docs/TESTING.md)：无数据库检查、双库测试、前提和实际结果。
- [依赖与许可证](docs/DEPENDENCIES.md)：驱动来源、锁定依赖的安全说明。

## 项目背景

这是对 2022 年一个 Doris 与 MySQL 混用项目的重构。当时为了优化性能，一些业务表会在两库之间来回调整。
项目里为高版本 MySQL 选用的 Python 驱动不能兼容当时的 Doris，Doris 侧还得另用兼容旧版 MySQL 的驱动，数据访问代码也要跟着改。

这次改用 Rust SQLx，希望表换库时尽量复用查询、绑定和结果映射。项目不负责迁表；支持的 SQL 写法和数据库差异见兼容文档。

## 许可证

SDK 使用 Apache-2.0；随附 sqlx-mysql 保留上游 Apache-2.0 OR MIT 许可，许可证全文随源码和配套包提供。
