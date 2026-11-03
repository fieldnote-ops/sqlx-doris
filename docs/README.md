<a name="doc-top"></a>

# Documentation

[English](README.md) | [简体中文](README.zh-CN.md)

Choose your task below. The default installation is a source dependency because 0.2.0 is not published.

<a name="doc-contents"></a>

## Contents

- [Start here](#doc-start)
- [Task map](#doc-map)
- [Language and API conventions](#doc-languages)
- [Shared terminology](#doc-terms)

<a name="doc-start"></a>

## Start here

1. [Install and connect](INSTALLATION.md): runtime queries need no build-time database.
2. [Everyday queries and types](USAGE.md), [ingest into an existing table](INGESTION.md) and [troubleshoot](TROUBLESHOOTING.md).
3. [Checked queries and offline builds](USAGE.md#doc-macros), [SQLx migrations](MIGRATIONS.md) and [CLI](CLI.md).
4. [Performance](PERFORMANCE.md) and [compatibility](COMPATIBILITY.md): choose a path and understand verified limits.
5. Specialist administration: [recovery](RECOVERY.md), [managed Routine Load](ROUTINE_LOAD.md) and [Kafka continuation preflight](KAFKA.md). These are not prerequisites for ordinary queries or ingestion.
6. Source contribution and acceptance: [testing](TESTING.md), [dependencies](DEPENDENCIES.md) and [release notes](../CHANGELOG.md).

<a name="doc-map"></a>

## Task map

[![Task routes with their public APIs and Doris features](assets/task-map.svg)](ARCHITECTURE.md#doc-tasks)

The [architecture and task guide](ARCHITECTURE.md) pairs the map with linked task choices, separate [control/execution/data lanes](ARCHITECTURE.md#doc-flows) and the [five-package dependency graph](ARCHITECTURE.md#doc-packages). Open the [local interactive atlas](ARCHITECTURE.md#doc-explorer) to select tasks and inspect their API, features and boundaries. Static diagrams and text remain available without JavaScript.

<a name="doc-languages"></a>

## Language and API conventions

English is the default README/document language; Simplified Chinese pages use .zh-CN.md. Use the language names at the top of every page to switch to its exact counterpart. Both editions have the same section IDs, runnable examples and supported behavior. API names, CLI flags, SQL, JSON fields/reason codes and sample outputs are shared verbatim. Rustdoc and CLI messages are canonical English; the Chinese guides explain the same contracts.

Repository links stay in the current language. Shared code/API/external references may remain in their original language. No translated status strings are accepted by the program. For parameter-level API docs run `./scripts/dev cargo doc --workspace --all-features --no-deps --open` from the checkout; files stay in the external target directory.

The project conventions draw on the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/documentation.html), [Cargo README metadata](https://doc.rust-lang.org/cargo/reference/manifest.html#the-readme-field), [GitHub links and anchors](https://docs.github.com/en/get-started/writing-on-github/getting-started-with-writing-and-formatting-on-github/basic-writing-and-formatting-syntax), and [BCP 47 language-tag guidance](https://www.w3.org/International/articles/language-tags/). The English default and .zh-CN filename suffix are this repository's choices, not a universal Markdown filename standard.

<a name="doc-terms"></a>

## Shared terminology

| Identifier / term | Meaning |
| --- | --- |
| LoadReceipt | HTTP load result containing server state, counts and diagnostics; not a managed receipt file |
| batch | Application-defined immutable data set; the application retains its identity and digest |
| label | Doris load identity; not proof of identical payloads |
| filtered | Rows rejected by quality checks, distinct from rows excluded by where as unselected |
| accepted | For example, async Group Commit accepted data without visibility assurance |
| committed | Commit completed; publication may still be pending |
| visible | Server reports publication; reconcile the application batch and filtered rows separately |
| unknown | Insufficient evidence to prove failure or justify replay |
| managed receipt / --receipt | Private locator written before CREATE; not authority or current server proof |
| confirmation sidecar | Historical `<receipt>.confirmed` identity/job binding |
| checkpoint | Trusted earlier Kafka observation; not a complete history audit |
| candidate offset | Administrator-supplied next position; never inferred automatically |
| retired identity | One-time migration account explicitly removed; admitted work may continue |
| retired connection | Unusable connection removed from reuse; pools replace it |
| drain | Scoped observation/wait, not proof of all backend resources finishing |
| owner | Exact UUID partition owner, not an account-to-lock ownership claim |
| fencing | An enforced boundary; cooperative owner reads do not atomically fence SQL |
