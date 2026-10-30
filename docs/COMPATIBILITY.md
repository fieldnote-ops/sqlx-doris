# 兼容范围和数据库差异

简体中文 | [English](COMPATIBILITY.en.md)

以下范围已在 SQLx 0.9.0、MySQL 8.4.13 和官方 Doris 4.1.5 上测试。其他版本和未列出的 SQL 写法需要在目标实例验证。

| 能力 | 共同使用方式 | 边界 |
| --- | --- | --- |
| 类型身份 | 原生 MySql/Pool/Connection/Row/Transaction | 应用根配置随附驱动的 patch，SQLx 版本一致 |
| CRUD/DTO/流 | 原 SQL、`.bind()`、`FromRow`、`QueryBuilder` | 服务端校验 SQL/表模型；流提前 drop 后正确收尾或关闭连接 |
| VALUES/DEFAULT | 列清单、静态默认值、NULL、多行写 | 不猜 schema、不拆批；动态默认值、空 VALUES、自增序列保持后端语义 |
| 分页 | LIMIT ?、LIMIT ? OFFSET ?、LIMIT ?,? | 原生整数参数；复杂表达式、显式 prepare/在线 describe 元数据有限 |
| BLOB 条件 | 简单列=、<>、!=、<=>、<、<=、>、>=、IN/NOT IN、BETWEEN/NOT BETWEEN | 保留字节顺序/NULL 三值语义；不模拟 LIKE、任意 CASE/函数/元组/子查询或非 NULL 数值混用 |
| BLOB 写入 | `Vec<u8>`与原 VALUES/`.bind()` | Doris 内部 STRING 采用公开表达式，不表示 OLAP 支持 BLOB/VARBINARY 列 |
| TIME | `MySqlTime`、chrono::`NaiveTime`，原生绑定与无损读取 | NaiveTime 拒绝负值/跨天；MySqlTime 范围与 Doris 完整微秒边界区分 |
| 显式 TIME | `types::Time` | ±838:59:59.999999；Doris 内部 OLAP 不支持 TIME 列，查询与显式 STRING 表示分开 |
| 大整数/Decimal | `LargeInt`、`BigDecimal` | 按目标 precision/scale 舍入或拒绝溢出，不经过浮点中转 |
| 复杂类型 | Array/Map/Struct/Variant 及显式投影/构造器 | MAP 可保留非字符串键；VARIANT 可能归一化、丢失根值展示信息 |
| 日期/时区 | Chrono 与 TimestampTz | 精度/时区按列定义；TimestampTz 拒绝亚微秒和闰秒 |
| 事务 | 顶层 `Transaction<MySql>` | Doris 事务内查询/DDL、savepoint 等不模拟；手工 BEGIN/raw guard 有明确限制 |
| 回执/冲突 | 各后端原生结果 | 直接返回数据库结果；两库的 rows_affected、last_insert_id 和主键模型含义可能不同 |
| Group Commit | Off/Sync/Async 显式选择 | Async 接受与可见性不同，失败不自动重放 |
| HTTP 导入 | StreamLoadClient 和 LoadReceipt | 2PC 后续提交/回滚由调用方执行；HTTPS 按部署配置证书 |

TIME 列表兼容只处理显式 TIME 参数；只有字符串的条件不会推断列类型。
Doris TIME 元数据在执行前选择无损文本读取，普通结果保持 prepared。
敏感字符串使用实际会话 sql_mode，不缓存猜测模式。

参数兼容支持的 SQL 形状有限。复杂字节谓词、混合非 NULL 类型、歧义注释或不可无损编码的值会明确报错，
参数按类型编码和 SQL 词法处理；不确定写入不会自动重试。

扩展操作封装部分 Doris 公开 SQL/HTTP 能力。FE 管理、CDC、跨库事务、自动双写、迁表和运行中迁池不在本库范围内。
性能受 SQL、表模型、索引、批次和环境影响，不承诺全场景与原生 SQLx 性能等价。
