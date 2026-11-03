<a name="doc-top"></a>

# 依赖与许可证

[English](DEPENDENCIES.md) | [简体中文](DEPENDENCIES.zh-CN.md)

[文档首页](README.zh-CN.md)

通过[五包架构图](ARCHITECTURE.zh-CN.md#doc-packages)查看必需/可选依赖方向，通过[接入图](ARCHITECTURE.zh-CN.md#doc-flows)查看执行路径。

库使用 Apache-2.0。应用仅声明 `sqlx-doris` 这一 Doris 扩展，通用查询、派生与迁移宏使用官方 `sqlx`。五个实现包版本完全一致，内部依赖固定到精确版本。

| 包 | 职责 | 启用方式 |
| --- | --- | --- |
| sqlx-doris | Doris 公开类型与宏入口，薄 CLI 二进制 | 应用的 Doris 依赖 |
| sqlx-doris-core | 唯一 Database 身份、协议、绑定、类型、连接与迁移 traits | 默认 |
| sqlx-doris-macros | 复用 SQLx 宏基础设施的编译期查询检查 | macros |
| sqlx-doris-ops | Schema/MV、HTTP 导入、身份/作业/恢复诊断、只读 Kafka 原语 | operations、stream-load 或 cli，按功能裁剪 |
| sqlx-doris-cli | 参数解析、prepare/Cargo 集成、人类输出和单一 JSON 协议 | cli, cli-admin or cli-admin-kafka |

内部包均向 core 依赖，不反向依赖公开入口。SQLx 的 `Migrate` trait 必须与 Doris 类型位于同一个 core 包，遵守 Rust 孤儿规则；身份管理、排空与诊断属于 ops。ops 接收显式请求和单次操作状态，不解析命令行、不退出进程、不打印，也不保存全局命令状态。CLI 负责交互、输出以及迁移 runner 的锁定与状态表准备；ops 共享的迁移历史读取与源码比较保持只读。

默认和仅 migrate 的 normal/build 依赖图不包含 ops、CLI、通用 sqlx、查询宏和 HTTP 客户端。operations 增加应用助手，stream-load 增加 HTTP 导入。cli 仅增加 clap、Cargo 集成和共用配置/状态/只读迁移历史；cli-admin 才开启维护 HTTP，cli-admin-kafka 才开启 Kafka wire/TLS。任何 CLI 档位都不隐含驱动查询宏或应用 Stream Load。应用显式选择官方 SQLx 特性。

- 精确 `sqlx-core = 0.9.0` 提供 traits、查询、连接池、事务、TLS 与日志。内部运行时代码直接使用它，不增加通用 API 转发。
- Tokio、futures、async-stream、bytes、bitflags、url 用于异步协议与连接。
- sha1 仅用于服务器的 mysql_native_password 挑战响应。
- serde、serde_json、chrono、bigdecimal 保留类型与精度；复杂值编码在 core 中共享。
- sqlx-macros-core、syn、quote、proc-macro-crate 仅用于编译期宏和重命名依赖解析。
- reqwest、percent-encoding 位于 ops 的可选维护 HTTP 路径；Stream Load 独立开启。
- Kafka 只读路径使用 rustls/tokio-rustls，不增加 C/Java Kafka 客户端。
- clap、sha2 和 CLI 运行时特性位于 CLI 包。

实际 normal/build 图不包含 sqlx-mysql、PostgreSQL 或 SQLite 驱动。Cargo.lock/metadata 可能列出未启用的上游可选包，它们不参与构建。`scripts/test-consumer` 用十一个独立 manifests 检查 default、migrate、macros、macros+migrate、operations、stream-load、三档 CLI、最小上游 SQLx 和仅启用上游 migrate! 的使用方式；另验证全部 feature 合并、六种 ops 组合、CLI admin 与独立 ops Kafka 的交叉合并及未启用 query 的空宏包。

sqlx-core/offline 有意保留：SQLx 0.9 的 Executor::describe 和 ColumnOrigin 序列化受该特性控制。这保证应用独立启用上游宏时 Executor 实现仍然完整。Doris 自身的描述序列化与 bitflags/serde 仅随 macro-support 开启；未启用 query 的宏包没有 normal/build 依赖。ops 的助手错误类型仅随 helpers 开启，维护与 Kafka 观察状态不进入 helpers-only 构建。默认运行时不强制开启 tokio-macros 或多线程执行器；由 CLI、测试或应用选择。

`scripts/test-package` 使用 Cargo workspace packaging 验证五包，每次生成外置临时 registry URL 和 target/build 目录，避免同版本包命中旧缓存。`.crate` 保存在外置 artifacts 目录；不要求用户修改上游或配置 patch。使用镜像时，先在外置工作目录执行 `cargo fetch --manifest-path <仓库>/Cargo.toml` 准备官方 registry 缓存。

Cargo.lock 记录受测依赖。各依赖保留自身许可证，可通过 cargo metadata 查看。`scripts/test-install` 从受验收的原样源码归档安装 CLI，再验证全新 SQLx 应用、prepare 和强制离线构建；内部包使用的测试 registry 路由只存在于外置验收目录。
