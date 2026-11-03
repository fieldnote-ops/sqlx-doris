# sqlx-doris-ops

[English](README.md) | [简体中文](README.zh-CN.md)

Internal implementation package, version 0.2.0 (unreleased). Applications use the public `sqlx-doris` package and upstream `sqlx`; do not declare this package separately.

Schema/MV/Stream Load helpers and explicitly requested maintenance/read-only Kafka workflows. Requests and observations are per-call; this package does not own CLI rendering or application pool lifecycle.

[Source repository and full guides](https://github.com/fieldnote-ops/sqlx-doris). In a full checkout, guides live in docs/. API documentation is in English.
