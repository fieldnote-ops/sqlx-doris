<a name="doc-top"></a>

# 兼容范围与限制

[English](COMPATIBILITY.md) | [简体中文](COMPATIBILITY.zh-CN.md)

[文档首页](README.zh-CN.md)

验证组合为 Rust 1.94+、SQLx Core 0.9.0、官方 Doris 4.1.5。Doris 使用 MySQL 兼容线协议；本库独立实现所需协议，不包含通用 MySQL 后端。

不做低版本适配；后续 Doris 版本须重新验证依赖的元数据、权限与事务语义，不自动放宽安全操作的版本门禁。

| 能力 | 范围 |
| --- | --- |
| SQLx 运行时查询 | query、query_as、query_scalar、raw_sql、QueryBuilder、FromRow |
| 连接池 | 原生 Pool<Doris>，连接数、超时、生命周期 hooks |
| 预编译 | COM_STMT_PREPARE/EXECUTE/CLOSE，逐连接 LRU 缓存 |
| 事务 | 顶层 Doris 写事务；不支持 savepoint、嵌套与自定义 BEGIN |
| 类型 | 整数、浮点、字符串、字节、NULL、DECIMAL、DATE/DATETIME、JSON 及显式 Doris 包装类型 |
| 导入 | 可选 Stream Load；SQL 写入遵循服务器支持的语法 |
| TLS | FE TLS、CA/主机名校验与客户端证书 |
| 认证 | mysql_native_password |
| 编译期查询 | 六种 query!/query_file! 宏，重命名依赖、输出类型/参数个数检查；参数类型为弱检查 |
| 离线编译 | SQLx Describe JSON，外置 SQLX_OFFLINE_DIR，CLI prepare/--check |
| SQLx 迁移 | Migrator、migrate!、可逆文件、dirty/checksum、持久互斥、异步 ALTER 等待、显式恢复 |
| CLI | cargo sqlx-doris prepare/migrate/database; cli-admin: admin lock/identity/drain/routine/diagnose/migrate repair; cli-admin-kafka: admin kafka；排空为限定范围快照，精确取消支持已登记 BROKER LOAD / ROLLUP / MV 任务；CLI 恢复统一复查事务与作业 |
| Any、官方 cargo sqlx 的 Doris 注册 | 未提供 |
| Unix socket、压缩、LOCAL INFILE、普通查询的多语句拼接执行 | 未提供 |

<a name="doc-contents"></a>

## 目录

- [精度与绑定](#doc-precision)
- [Doris 语义](#doc-semantics)
- [宏与迁移边界](#doc-migrations)

<a name="doc-precision"></a>

## 精度与绑定

整数解码检查溢出；非有限浮点参数被拒绝。DECIMAL 和 LARGEINT 不经过浮点转换；日期/时间编码拒绝无法表示的精度。Time 保留完整 Doris 微秒边界，不能用作 OLAP 存储列。

Doris 的部分二进制 TIME 响应会截断边界小数；驱动根据 PREPARE 元数据在执行前选择文本协议。若服务器直到执行时才报告 TIME 元数据，驱动报错并淘汰连接，不重放查询。此类表达式可用 raw_sql 或显式 STRING 投影。

字节绑定通过 Doris 公开字节函数与 ASCII 十六进制参数适配。支持简单列比较、IN/NOT IN 和 BETWEEN 等已校验语法，保留 NULL。无法证明安全或无法确定混合类型含义的表达式直接报错；复杂表达式请显式使用字节函数。参数中的引号、注释符、NUL 和问号始终作为数据。

绑定 LIMIT/OFFSET 使用校验后的整数字面量，接受 i64 范围内的非负原生整数。需要文本绑定时根据实际 sql_mode 选择转义；事务开始前读取模式，事务中不要修改 sql_mode。

<a name="doc-semantics"></a>

## Doris 语义

受影响行数由 Doris 回执决定，不承诺通用关系数据库的匹配行/变更行语义。Unique、Duplicate、Aggregate 表行为不同；Group Commit Async 的提交可见性需单独等待。VARIANT、复杂类型与外部 Catalog 受服务器本身限制。官方资料见[预编译语句](https://doris.apache.org/docs/4.x/key-features/prepared-statement/)与[事务](https://doris.apache.org/docs/4.x/data-operate/transaction/)。

<a name="doc-migrations"></a>

## 宏与迁移边界

使用说明见[编译期查询](USAGE.zh-CN.md#doc-macros)及[迁移](MIGRATIONS.zh-CN.md)。Doris 表达式元数据与参数类型信息并不完整；歧义结果需显式类型覆盖，可空性默认保守处理。离线缓存必须来自匹配的 schema 与本库版本。

迁移使用 Doris 的非原子 DDL 和持久恢复标记，不提供整份文件回滚或自动重试。脚本限于表、普通视图、索引 DDL 及普通 DML/查询，异步物化视图与导入任务应使用专用 API。ALTER 仅处理当前数据库，并等待 COLUMN/ROLLUP 作业终态。主锁和转换锁都依赖固定范围 UUID 分区的原子 ADD；释放/恢复通过持久转换门串行化 owner 校验与删除；固定范围的 UUID 分区提供原子互斥，清理仅删除自己的分区，延迟的旧 DROP 无法删除后继分区。中断的门不自动接管，所有执行者必须使用相同数据库及同版本锁协议，恢复流程见[异常恢复](RECOVERY.zh-CN.md)。

官方说明：[Doris Schema Change](https://doris.apache.org/docs/4.x/table-design/schema-change/)，包含轻量同步变更和异步作业的区别。

分区操作的同步语义、范围语法与保留至少一个分区的要求见 [ALTER TABLE PARTITION](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/ALTER-TABLE-PARTITION/)。本库另在 Doris 4.1.5 上验证重叠范围互斥与延迟旧分区删除，详见测试说明。

v2 协议中遗留的 owner 分区可通过 CLI recover-transition 先预览，再显式确认独占维护后清除，父表与 anchor 保留。此前未发布的主锁/转换锁注释表协议不自动转换；未知分区元数据拒绝继续。要求所有相关进程和旧命令已停止、跨机器单管理员串行；不按超时自动接管，保留主锁和迁移记录。详见[恢复流程](RECOVERY.zh-CN.md)。

迁移 owner 检查点只在发现丢锁时停止后续步骤及完成标记，不构成 SQL 原子 fencing。Doris KILL 不能阻止旧进程重连，也不能撤回已提交的数据或异步作业；不提供按历史 Connection ID 自动终止会话或安全自动接管。恢复仍须外部隔离旧 runner，详见[恢复流程](RECOVERY.zh-CN.md)。

独立身份 CLI 当前只验证 Doris 4.1.5 单 master FE 的内置认证/授权；多 FE、LDAP/Ranger、未知版本及扩权/host 变体拒绝自动退役。一次性账号删除切断未来认证与权限检查，但已经获准的事务仍可 COMMIT，异步作业仍可继续；不会清锁或修复历史。详见[恢复流程](RECOVERY.zh-CN.md)。

CLI 显式终止仅扩展到已登记事务的 BROKER LOAD，按 JobId/TransactionId/ListenerId 绑定，调用事务 ID abort 并核验终态；当前限 4.1.5 shared-nothing 单 master FE/单 live BE。未登记 LOAD 和普通旧 Routine Load 不提供名称取消/停止，详见[恢复流程](RECOVERY.zh-CN.md)中的网络、版本、竞态和恢复边界。

受管理 Routine Load 支持永久 UUID 不复用协议下的创建/检查/显式停止，限上述单 FE/内置授权/存算一体 4.1.5、单表 Kafka。实际 STOP 仍按名字执行；手工复用、元数据恢复或命名空间绕过不受保护，旧作业不收编。参见[受管理 Routine Load](ROUTINE_LOAD.zh-CN.md)。

迁移表名解析与当前库核对共用 `DorisConnection::migration_table_identifier`。本地标识符/跨库错误使用 SQLx `Error::Configuration`；服务端协议及 I/O 读取失败保留原错误类别，CLI 不把损坏元数据响应误报为参数错误。
