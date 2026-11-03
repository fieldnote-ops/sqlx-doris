<a name="doc-top"></a>

# 架构与使用地图

[English](ARCHITECTURE.md) | [简体中文](ARCHITECTURE.zh-CN.md)

[文档首页](README.zh-CN.md)

从应用实际需要完成的任务开始。通用 API 使用上游 `sqlx`，`sqlx-doris` 是唯一 Doris 扩展；应用无需声明内部实现包。这些地图说明已支持的边界，不展示实时服务状态。

<a name="doc-contents"></a>

## 目录

- [按任务选择路径](#doc-tasks)
- [读懂控制、执行与数据路径](#doc-flows)
- [理解五个包](#doc-packages)
- [选择 CLI 档位](#doc-cli)
- [使用交互地图](#doc-explorer)

<a name="doc-tasks"></a>

## 按任务选择路径

![六条任务路径及其公开 API 与 Doris 特性](assets/task-map.zh-CN.svg)

| 任务 | 公开入口 | Doris 特性 | 从这里开始 |
| --- | --- | --- | --- |
| 查询、绑定与流式读取 | `sqlx::query*` 配合 `DorisPool` | 无 | [查询与连接池](USAGE.zh-CN.md#doc-queries) |
| 向已有表导入 | `StreamLoadClient` | `stream-load` | [一个 JSON 批次](INGESTION.zh-CN.md#doc-json)、[不可变文件](INGESTION.zh-CN.md#doc-files) |
| 编译期检查 SQL | `sqlx_doris::query!` | `macros` | [编译期查询](USAGE.zh-CN.md#doc-macros)、[离线元数据](USAGE.zh-CN.md#doc-offline) |
| 应用迁移文件 | `sqlx::migrate!` / `Migrator` | `migrate` | [迁移 API](MIGRATIONS.zh-CN.md#doc-api) |
| 使用 schema、类型与 MV 助手 | `sqlx_doris::operations` | `operations` | [类型](USAGE.zh-CN.md#doc-types)、[MV 观察](USAGE.zh-CN.md#doc-mv) |
| 调查异常 | `cargo sqlx-doris admin diagnose` | `cli-admin` | [只读诊断](CLI.zh-CN.md#doc-diagnose)、[恢复步骤](RECOVERY.zh-CN.md#doc-sequence) |

表中列出 Doris 侧特性。使用 `FromRow` 时开启上游 `sqlx/derive`；Doris 编译期查询要求名为 `sqlx` 的直接依赖，并开启 `derive` 或 `macros`。嵌入 `sqlx::migrate!` 需要上游 `macros` 和 `migrate`，运行时读取迁移目录需要上游 `migrate`。详见完整[特性表](INSTALLATION.zh-CN.md#doc-features)。

CLI 安装与应用特性是两个选择。`macros` 不安装 prepare，`migrate` 不安装二进制；安装 CLI 也不会给应用启用编译期查询或 Stream Load。[CLI 档位](#doc-cli)明确了各自范围。

<a name="doc-flows"></a>

## 读懂控制、执行与数据路径

![编译期元数据、运行时 SQL 与 HTTP 数据、显式工具控制分别位于不同泳道](assets/integration.zh-CN.svg)

每条泳道从调用方读向目标端点。箭头、线型与文字标签和颜色共同表达含义：

| 关系 | 线型 | 含义 |
| --- | --- | --- |
| SQL 执行 / 结果行返回 | 青色细实线 | 原生 Doris 协议；分别画出请求与结果方向 |
| HTTP 批次数据 | 绿色粗实线 | 应用 payload；客户端向已验证 BE 目标上传 |
| 控制 / 编排 | 琥珀色虚线 | Cargo 调用、HTTP 路由或显式迁移/维护协调 |
| 编译元数据 | 紫色点线 | 在线 describe 或本地离线元数据；不执行业务 SQL |

**编译期。** 可选的 prepare 调用应用 Cargo 构建。Doris 宏从在线描述或已准备的离线元数据获取查询信息，完成检查并生成代码，不执行业务 SQL。编译后的查询仍使用普通运行时驱动。CLI 安装不代替应用选择 Cargo 特性。

**应用运行期。** 查询 API 与连接池来自 SQLx 共用机制；Doris 身份、绑定、协议和类型值来自本驱动。SQL 请求发往 FE，结果行返回调用方。Stream Load 独立使用 HTTP：FE 返回路由信息，客户端验证目标后向 BE 发送 payload。图中没有把 FE 画成 payload 转发器。收到回执后仍须[核对结果和行数](INGESTION.zh-CN.md#doc-result)。

**显式工具控制。** 正常迁移协调包含 owner/历史检查、dirty 标记、DDL 与异步完成。管理诊断先做限定范围的只读观察；变更操作需要各自显式命令及前提。独立 Kafka 预检在明确请求后直连 broker。它们是不同路径，不构成自动恢复链。详见[迁移保证](MIGRATIONS.zh-CN.md)和[恢复边界](RECOVERY.zh-CN.md#doc-limits)。

地图止于公开端点边界，不模拟 FE/BE 调度、分布式事务内部过程、延迟、资源使用量或后端排空。任何视觉状态都不意味着现场观察成功、允许接管、写入已回滚或 offset 已核对。

<a name="doc-packages"></a>

## 理解五个包

![五个 Doris 包、必需与可选依赖，以及唯一公开应用入口](assets/packages.zh-CN.svg)

包之间的箭头表示**依赖于**，不表示“下一步调用”。实线表示必需的包依赖，虚线表示由标注特性激活的可选依赖。这里画出潜在的 normal 依赖拓扑；应用实际构建由 Cargo 特性合并决定。交互页中的任务高亮用于解释，不是 Cargo 跟踪结果。

| 包 | 负责内容 | 边界 |
| --- | --- | --- |
| `sqlx-doris` | 显式 Doris 导出、查询宏、可选助手与一个薄二进制 | 应用唯一的 Doris 扩展 |
| `sqlx-doris-core` | 数据库身份、协议、绑定、精确值、SQLx traits 与迁移锁原语 | 不编排 CLI、维护 HTTP 或 Kafka |
| `sqlx-doris-macros` | 描述/类型检查、离线元数据与生成代码 | 位于编译期，复用 core 和 SQLx 宏基础设施 |
| `sqlx-doris-ops` | 应用助手、Stream Load 与受特性门控的专项工作流 | 显式请求/证据，不渲染命令输出或退出进程 |
| `sqlx-doris-cli` | 参数、Cargo/prepare、文件、命令与人类/JSON 输出 | 委托领域操作，不增加第二个驱动 |

公开入口始终依赖 core，按需依赖 macros、ops 和 CLI。macros 包的 query 特性启用 core；ops 始终依赖 core，CLI 始终依赖 core 和 ops（cli-support）。

core 拥有唯一 Doris 身份，SQLx 迁移实现也在这里。内部包向 core 依赖，不反向依赖公开入口。它们共用上游 `sqlx-core`；上游 SQLx 不是第六个 Doris 包。公开入口不重新导出 SQLx 通用命名空间。已解析图检查与上游特性细节见[依赖与特性隔离](DEPENDENCIES.zh-CN.md)。

<a name="doc-cli"></a>

## 选择 CLI 档位

| 安装特性 | 增加能力 | 范围 |
| --- | --- | --- |
| `cli` | prepare、database、普通 migrate | 基础开发与交付 |
| `cli-admin` | `cli` 加 `admin` 命令 | Doris 身份/排空/锁恢复与只读诊断 |
| `cli-admin-kafka` | `cli-admin` 加独立 Kafka 观察 | 基于显式证据的来源快照/预检 |

所有档位安装同一个 `cargo-sqlx-doris` 可执行文件。基础档位不包含维护 HTTP 和 Kafka；`cli-admin` 可独立于 Kafka 做 Doris 诊断。任何档位都不隐含应用 Stream Load 或驱动查询宏。请按[安装命令](INSTALLATION.zh-CN.md#doc-cli)与 [CLI 参考](CLI.zh-CN.md)操作。

<a name="doc-explorer"></a>

## 使用交互地图

从完整源码检出目录用浏览器打开 [ATLAS.zh-CN.html](ATLAS.zh-CN.html)，保持旁边的 assets 目录完整。[英文版](ATLAS.html)是准确对应的页面。代码托管平台可能展示 HTML 源码而不运行它；此时仍可阅读本页静态图与链接表格。这个文档页面无需数据库、构建或联网。

选择任务后可查看 API、Doris 特性列表、上游要求、请求路径、约束和指南链接。在使用地图、接入泳道和包依赖拓扑间切换。切换语言会保留任务/视图上下文；主题可选择跟随系统、深色和浅色。复制功能只复制展示的 Doris 特性列表，不是完整依赖清单。指南链接打开检出目录中对应语言的 Markdown 源文档。

任务按钮和 SVG 节点可用键盘激活；方向键/Home/End 切换视图页签。小屏幕中，使用地图改为易读卡片，复杂图示仅在图框内横向滚动，并可切换全图总览/清晰阅读。文字说明保持易读。JavaScript 不可用时仍保留静态地图及本页文字说明入口。没有自动播放、视频、遥测或远程资源加载。

图示与交互页共用 docs/assets/atlas-data.js 中的模型。文档检查将特性、必需/可选依赖箭头与 Cargo manifest 对照，验证双语指南锚点并拒绝过期 SVG。编辑图示时执行 `./scripts/dev scripts/render-maps --write`；`--check` 核对精确渲染结果。渲染额外需要 Node.js 18+；截图、浏览器配置和审查输出继续外置。最终 SVG/HTML/CSS/JS 属于用户文档资产。
