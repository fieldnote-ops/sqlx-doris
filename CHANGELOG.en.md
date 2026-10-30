# Changelog

[简体中文](CHANGELOG.md) | English

## 0.1.0 (unreleased)

- Share MySQL/Doris connection APIs and native SQLx types, reusing query functions and safe `.bind()` calls
  within the supported SQL subset.
- Add Doris types, table models, task observation, and optional Stream Load while preserving native database
  semantics.
- Supply a companion driver for short PrepareOk responses, transaction state, and safe
  BLOB/TIME/VALUES/pagination parameter handling.
- Include regression tests for commit, rollback, cancellation, caches, error recovery, and avoiding replay of
  uncertain writes.
- Bundle the driver source at `crates/sqlx-mysql`; the SDK now builds on `sqlx-core` plus that path
  dependency and exposes `sqlx_doris::sqlx`, so applications need no root-manifest patch entry.
- Provide installation tests, an upstream SQLx comparison, and a verifiable compatibility bundle; the bundle
  ships the SDK and driver as source.
- Publish the repository on GitHub (<https://github.com/fieldnote-ops/sqlx-doris>) with git-dependency
  installation; crates.io publication is deferred, see [dependencies](docs/DEPENDENCIES.en.md) for why.
