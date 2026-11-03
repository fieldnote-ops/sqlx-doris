<a name="doc-top"></a>

# 版本变化

[English](CHANGELOG.md) | [简体中文](CHANGELOG.zh-CN.md)

<a name="doc-contents"></a>

## 目录

- [0.2.0（未发布）](#doc-v0-2-0)

<a name="doc-v0-2-0"></a>

## 0.2.0（未发布）

- 独立 SQLx Doris 驱动，仅有 Doris 类型身份和 doris:// URL；移除内置 sqlx-mysql、双后端、兼容补丁和 Client 查询包装。
- 五包分工：sqlx-doris 公开入口，core 驱动与迁移 traits，macros 编译期检查，ops 操作与运维原语，CLI 交互与 prepare。应用无需分别声明内部包。
- 普通查询、连接池、FromRow、Migrator 和 migrate! 使用官方 SQLx；不重新导出通用 SQLx API。operations 与 stream-load 按需开启，默认驱动依赖保持精简。
- 增加六种 Doris checked query 宏、保守类型/可空性推断、重命名依赖与离线缓存。prepare 使用应用 Cargo 配置，原子替换缓存，显式释放文件锁，诊断保存在私密外置文件。
- 保留精确类型、保守的参数绑定和 Doris 表模型语义。取消、未读流、事务丢弃和不确定结果退役连接；不自动重放写入。
- 使用 cli 选择基础 CLI、cli-admin 选择 Doris 专项管理、cli-admin-kafka 再启用独立 Kafka 观察。高级命令统一位于 admin 下，不保留旧路径别名；Doris 诊断不依赖 Kafka，基础迁移 info 共享只读历史且不引入维护 HTTP。
- 普通导入可在 await 前读取 label、只读观察其状态，并显式设置常用导入选项。共享字节缓冲消除重定向时的全量复制；文件请求固定已打开文件代际并有界读取。调用方仍负责保持输入不可变。
- 补充对等双语导入/排障、类型/会话/MV 配方、隔离文档消费者、分组原生测试、校验数据的分析查询/导入基准与三种 CLI 安装验收。
- Stream Load schema 转换保留重复 MAP 条目，拒绝重复顶层字段。Group Commit 的缺失/未知实际模式明确为 Unconfirmed，请求模式不再代替可见性证据。
- 迁移持久化 dirty/checksum，等待异步 DDL。主锁和转换锁采用 UUID owner 分区，只删除精确 owner；断连、取消、迟到删除和后继 owner 不触发自动接管。
- 显式维护恢复区分未发送、不确定、已确认但复查失败。owner 检查是合作式检查点，不能原子阻止旧业务 SQL；始终要求外部独占维护和旧写入核对。
- 专属迁移账号生成一次性名称及私密凭证，显式退役；排空检查旧会话、全库事务、COLUMN/ROLLUP、LOAD、ROUTINE LOAD、MV 调度器和任务。已提交数据及已准入作业仍须核对。
- 支持精确 ROLLUP/MV 任务取消、已登记 BROKER LOAD 事务 ID abort，以及永久 UUID 不复用协议下的 Routine STOP。均预览优先、显式维护、不重试；保留不具备服务端代际 fence 的边界。
- Routine receipt 定位与确认文件保留操作阶段和旧快照阶段。Kafka snapshot/preflight 只核验 cluster/topic ID、分区、范围与保留 epoch；不认证消费、不转移或重置 offset。
- CLI 统一一个版本化 JSON 协议、稳定状态/原因码和退出码。只读 diagnose 串联锁/历史、身份/排空、receipt 和可选 Kafka，保留各项冲突、部分证据与覆盖缺口。
- 共享迁移历史、私密文件和操作状态实现，取消全局命令状态。手工整理宏体、异步控制流、缩进与生成模板；格式检查覆盖 Rust、独立 fixture 和 Python。
- 测试包含真实 Kafka 多分区、分区变化、offset 过期/截断、STOP/提交竞态与数据重复/缺失核对；加入可复现随机化、持续负载、五包安装与全新应用验收。
- 构建、缓存、Doris/Kafka 数据和中间产物全部外置。当前服务器验证基线为 Doris 4.1.5，不适配低版本。
