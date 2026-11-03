<a name="doc-top"></a>

# 使用文档

[English](README.md) | [简体中文](README.zh-CN.md)

按任务选择指南。安装使用已发布的 `sqlx-doris` 0.2.0 crate；仅在参与开发时才需要源码检出目录。

<a name="doc-contents"></a>

## 目录

- [从这里开始](#doc-start)
- [使用地图](#doc-map)
- [语言与 API 约定](#doc-languages)
- [双语术语](#doc-terms)

<a name="doc-start"></a>

## 从这里开始

1. [安装与连接](INSTALLATION.zh-CN.md)：运行时查询无需编译期数据库。
2. [日常查询与类型](USAGE.zh-CN.md)、[向已有表导入数据](INGESTION.zh-CN.md)和[问题排查](TROUBLESHOOTING.zh-CN.md)。
3. [编译期查询与离线构建](USAGE.zh-CN.md#doc-macros)、[SQLx 迁移](MIGRATIONS.zh-CN.md)和 [CLI](CLI.zh-CN.md)。
4. [性能](PERFORMANCE.zh-CN.md)和[兼容范围](COMPATIBILITY.zh-CN.md)：选择合适路径并理解验证边界。
5. 高级维护：[异常恢复](RECOVERY.zh-CN.md)、[受管 Routine Load](ROUTINE_LOAD.zh-CN.md)、[Kafka 接续预检](KAFKA.zh-CN.md)。这些不是日常查询或导入的前置要求。
6. 源码贡献与验收：[测试](TESTING.zh-CN.md)、[依赖](DEPENDENCIES.zh-CN.md)和[版本变化](../CHANGELOG.zh-CN.md)。

<a name="doc-map"></a>

## 使用地图

[![任务路径及其公开 API 与 Doris 特性](assets/task-map.zh-CN.svg)](ARCHITECTURE.zh-CN.md#doc-tasks)

[架构与使用指南](ARCHITECTURE.zh-CN.md)提供可点击的任务选择、独立的[控制/执行/数据泳道](ARCHITECTURE.zh-CN.md#doc-flows)与[五包依赖图](ARCHITECTURE.zh-CN.md#doc-packages)。打开[本地交互地图](ARCHITECTURE.zh-CN.md#doc-explorer)可按任务查看 API、特性和边界。无 JavaScript 时仍可阅读静态图与文字。

<a name="doc-languages"></a>

## 语言与 API 约定

英文为默认 README/文档语言，简体中文页面使用 .zh-CN.md。每页顶部通过语言名称切换到准确的对应页面；两种语言保持相同章节 ID、可执行示例和功能边界。API 名称、CLI 参数、SQL、JSON 字段/原因码及示例输出逐字共享。Rustdoc 与 CLI 消息以英文为准，中文指南完整解释同一契约。

仓库内文档链接保持当前语言；共用代码、API 或外部参考可使用原语言。程序不接受翻译后的状态字符串。在检出目录执行 `./scripts/dev cargo doc --workspace --all-features --no-deps --open` 可查看参数级 API 文档，文件仍位于外置 target。

本项目约定参考 [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/documentation.html)、[Cargo README 元数据](https://doc.rust-lang.org/cargo/reference/manifest.html#the-readme-field)、[GitHub 链接与锚点](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax)以及 [BCP 47 语言标签说明](https://www.w3.org/International/articles/language-tags/)。英文默认与 .zh-CN 文件名后缀属于本仓库的约定，并非 Markdown 的统一命名标准。

<a name="doc-terms"></a>

## 双语术语

| 标识符 / 术语 | 含义 |
| --- | --- |
| LoadReceipt | 含服务端状态、行数与诊断的 HTTP 导入结果，不是受管回执文件 |
| batch / 批次 | 应用定义的不可变数据集合，业务标识与摘要由应用保存 |
| label / 导入标签 | Doris 导入标识；不是 payload 一致性证明 |
| filtered / 过滤行 | 未通过质量检查的行，区别于 where 排除的 unselected 行 |
| accepted / 已接受 | 例如异步 Group Commit 已接受数据，尚不保证可见 |
| committed / 已提交 | 提交已完成，发布可见可能仍在进行 |
| visible / 已可见 | 服务端报告数据已发布，仍须核对业务批次和过滤行 |
| unknown / 结果未知 | 证据不足，既不能证明失败，也不能据此重发 |
| 受管回执文件 / --receipt | CREATE 前写入的私密定位文件，不是权限或当前服务端证明 |
| confirmation sidecar / 确认侧文件 | 历史 `<receipt>.confirmed` 身份与作业绑定 |
| checkpoint / 检查点 | 可信的早期 Kafka 观察，不是完整历史审计 |
| candidate offset / 候选接续位置 | 管理员指定的下一位置，不自动推导 |
| retired identity / 退役账号 | 明确删除的一次性迁移账号；已准入工作仍可继续 |
| retired connection / 淘汰连接 | 不可复用的连接，由连接池补建 |
| drain / 排空观察 | 限定范围的检查/等待，不证明全部后端资源已结束 |
| owner / 所有者标记 | 精确 UUID 分区 owner，不证明账号与锁的归属 |
| fencing / 强制隔离边界 | 可强制实施的边界；合作式 owner 读取不能原子阻止 SQL |
