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
- Provide installation tests, an upstream SQLx comparison, and a verifiable compatibility bundle; one root
  patch entry is required in the application.
