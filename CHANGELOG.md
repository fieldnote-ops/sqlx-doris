# 变更记录

## 0.1.0（未发布）

- 统一 MySQL/Doris 连接及 SQLx 原生类型，支持同一查询函数和安全.bind()的声明子集。
- 提供 Doris 类型、表模型、任务观察和可选 Stream Load，保留数据库原生语义。
- companion 修复短 PrepareOk、事务状态和 BLOB/TIME/VALUES/分页安全参数路径。
- 提供提交/回滚/取消、缓存、错误恢复及不确定写入不重放的回归测试。
- 提供安装消费者、原版 SQLx 对照和可核验配套包；应用根需配置一次 patch。
