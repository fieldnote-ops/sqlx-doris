# 支持范围与语义

测试基线为 SQLx 0.9.0、MySQL 8.4.13 与官方 Doris 4.1.5。支持受测子集，不提供完整 SQL 方言模拟或迁表工具。

| 能力 | 共同使用方式 | 边界 |
| --- | --- | --- |
| 类型身份 | 原生 MySql/Pool/Connection/Row/Transaction | 应用根配置 companion patch，SQLx 版本一致 |
| CRUD/DTO/流 | 原 SQL、`.bind()`、`FromRow`、`QueryBuilder` | 服务端校验 SQL/表模型；流提前 drop 后正确收尾或退役 |
| VALUES/DEFAULT | 列清单、静态默认值、NULL、多行写 | 不猜 schema、不拆批；动态默认值、空 VALUES、自增序列保持后端语义 |
| 分页 | LIMIT ?、LIMIT ? OFFSET ?、LIMIT ?,? | 原生整数参数；复杂表达式、显式 prepare/在线 describe 元数据有限 |
| BLOB 条件 | 简单列=、<>、!=、<=>、<、<=、>、>=、IN/NOT IN、BETWEEN/NOT BETWEEN | 保留字节顺序/NULL 三值语义；不模拟 LIKE、任意 CASE/函数/元组/子查询或非 NULL 数值混用 |
| BLOB 写入 | Vec<u8>与原 VALUES/`.bind()` | Doris 内部 STRING 采用公开表达式，不表示 OLAP 支持 BLOB/VARBINARY 列 |
| TIME | `MySqlTime`、chrono::`NaiveTime`，原生绑定与无损读取 | NaiveTime 拒绝负值/跨天；MySqlTime 范围与 Doris 完整微秒边界区分 |
| 显式 TIME | `types::Time` | ±838:59:59.999999；Doris 内部 OLAP 不支持 TIME 列，查询与显式 STRING 表示分开 |
| 大整数/Decimal | `LargeInt`、`BigDecimal` | 按目标 precision/scale 舍入或拒绝溢出，不经过浮点中转 |
| 复杂类型 | Array/Map/Struct/Variant 及显式投影/构造器 | MAP 可保留非字符串键；VARIANT 可能归一化、丢失根值展示信息 |
| 日期/时区 | Chrono 与 TimestampTz | 精度/时区按列定义；TimestampTz 拒绝亚微秒和闰秒 |
| 事务 | 顶层 `Transaction<MySql>` | Doris 事务内查询/DDL、savepoint 等不模拟；手工 BEGIN/raw guard 有明确限制 |
| 回执/冲突 | 各后端原生结果 | 不伪造 rows_affected、last_insert_id 或主键模型与 MySQL 的等价性 |
| Group Commit | Off/Sync/Async 显式选择 | Async 接受与可见性不同，失败不自动重放 |
| HTTP 导入 | StreamLoader 及 LoadReceipt | HTTP2PC 后续动作/全量管理 API 不在接口内；原生 HTTPS 服务端部署自行验证 |

TIME 列表适配仅处理声明的 TIME 参数共同域；String-only 条件不猜列类型。
Doris TIME 元数据在执行前选择无损文本读取，普通结果保持 prepared。
敏感字符串使用实际会话 sql_mode，不缓存猜测模式。

安全适配覆盖的 SQL 形状有限。复杂字节谓词、混合非 NULL 类型、歧义注释或不可无损编码的值会明确报错，
不使用简单问号替换、有损转换、隐式事务合并或错误后重试来制造成功。

扩展操作只封装公开 SQL/HTTP 的选定能力，不包含全部 FE 管理 API、CDC、跨库事务、自动双写、迁表或运行中自动迁池。
性能受 SQL、表模型、索引、批次和环境影响，不承诺全场景与原生 SQLx 性能等价。
