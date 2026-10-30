# 变更记录

简体中文 | [English](CHANGELOG.en.md)

## 0.1.0（未发布）

- 统一 MySQL/Doris 连接及 SQLx 原生类型，在已支持的 SQL 范围内复用查询函数与安全 `.bind()`。
- 提供 Doris 类型、表模型、任务观察和可选 Stream Load，保留数据库原生语义。
- 随附驱动修复短 PrepareOk、事务状态和 BLOB/TIME/VALUES/分页安全参数路径。
- 提供提交/回滚/取消、缓存、错误恢复及不确定写入不重放的回归测试。
- 驱动源码并入 `crates/sqlx-mysql`，SDK 改由 `sqlx-core` 加 path 依赖构建并导出 `sqlx_doris::sqlx`；
  应用不再需要根 manifest 的补丁配置。
- 提供安装消费者、原版 SQLx 对照和可核验配套包；配套包按源码分发 SDK 与驱动。
- 仓库公开在 GitHub（<https://github.com/fieldnote-ops/sqlx-doris>），可按 git 依赖安装；crates.io 暂不发布，
  原因和前提见[依赖与许可证](docs/DEPENDENCIES.md)。
