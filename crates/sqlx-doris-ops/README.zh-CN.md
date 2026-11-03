# sqlx-doris-ops

[English](README.md) | [简体中文](README.zh-CN.md)

内部实现包，版本 0.2.0（未发布）。应用使用公开 sqlx-doris 包与官方 sqlx，无需单独声明此包。

Schema/MV/Stream Load 助手及显式维护、只读 Kafka 工作流；请求和观察状态按操作传入，不负责 CLI 渲染或应用连接池生命周期。

[源码仓库与完整指南](https://github.com/fieldnote-ops/sqlx-doris)。完整检出目录中的指南位于 docs/，API 文档为英文。
