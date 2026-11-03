<a name="doc-top"></a>

# Architecture and task map

[English](ARCHITECTURE.md) | [简体中文](ARCHITECTURE.zh-CN.md)

[Documentation](README.md)

Start with the work your application needs. Use upstream `sqlx` for generic APIs and `sqlx-doris` as your sole Doris extension; internal implementation packages are not application dependencies. These maps describe supported boundaries, not live server status.

<a name="doc-contents"></a>

## Contents

- [Choose a task](#doc-tasks)
- [Read control, execution and data paths](#doc-flows)
- [Understand the five packages](#doc-packages)
- [Select a CLI profile](#doc-cli)
- [Use the interactive atlas](#doc-explorer)

<a name="doc-tasks"></a>

## Choose a task

![Six task routes with their public APIs and Doris features](assets/task-map.svg)

| Task | Public entry | Doris feature | Start here |
| --- | --- | --- | --- |
| Query, bind and stream rows | `sqlx::query*` with `DorisPool` | None | [Queries and pools](USAGE.md#doc-queries) |
| Import an existing table | `StreamLoadClient` | `stream-load` | [One JSON batch](INGESTION.md#doc-json), [immutable files](INGESTION.md#doc-files) |
| Check SQL at build time | `sqlx_doris::query!` | `macros` | [Checked queries](USAGE.md#doc-macros), [offline metadata](USAGE.md#doc-offline) |
| Apply migration files | `sqlx::migrate!` / `Migrator` | `migrate` | [Migration API](MIGRATIONS.md#doc-api) |
| Use schema, values and MV helpers | `sqlx_doris::operations` | `operations` | [Types](USAGE.md#doc-types), [MV observation](USAGE.md#doc-mv) |
| Investigate an incident | `cargo sqlx-doris admin diagnose` | `cli-admin` | [Read-only diagnosis](CLI.md#doc-diagnose), [recovery sequence](RECOVERY.md#doc-sequence) |

The table lists the Doris side. For `FromRow`, enable upstream `sqlx/derive`; checked Doris queries require a direct dependency named `sqlx` with `derive` or `macros`. Embedded `sqlx::migrate!` needs upstream `macros` and `migrate`, while a runtime migration directory needs upstream `migrate`. See the complete [feature matrix](INSTALLATION.md#doc-features).

CLI installation is separate from an application's features. `macros` does not install prepare; `migrate` does not install the binary. Conversely, installing the CLI does not enable checked queries or application Stream Load. The [CLI profiles](#doc-cli) make that choice explicit.

<a name="doc-flows"></a>

## Read control, execution and data paths

![Build-time metadata, runtime SQL and HTTP data, and explicit tool control in separate lanes](assets/integration.svg)

Read each lane from its caller toward its endpoint. Arrowheads, line patterns and labels carry meaning alongside color:

| Relation | Line | Meaning |
| --- | --- | --- |
| SQL execution / returned rows | Thin solid, cyan | Native Doris wire; the request and result directions are shown separately |
| HTTP batch data | Thick solid, green | Application payload; the client uploads to the verified BE target |
| Control / orchestration | Dashed, amber | Cargo invocation, HTTP routing or explicit migration/maintenance coordination |
| Build metadata | Dotted, violet | Online describe or local offline metadata; not business SQL execution |

**Build time.** Optional prepare invokes the application's Cargo build. The Doris macro obtains a description online or from prepared offline metadata, validates the query and emits code. It does not execute business SQL. Compiled queries later use the ordinary runtime driver. A CLI install does not choose the application's Cargo features.

**Application runtime.** The query API and pool come from SQLx's shared machinery; the Doris identity, bindings, wire and values come from this driver. SQL requests go to the FE and rows return to the caller. Stream Load is a separate HTTP path: the FE returns routing information, the client validates the target and sends the payload to the BE. The diagram does not show the FE relaying the payload. A receipt still needs [outcome and row-count checks](INGESTION.md#doc-result).

**Explicit tool control.** Normal migration coordination includes owner/history checks, dirty markers, DDL and asynchronous completion. Administrative diagnosis begins with scoped read-only observations; mutations require their own explicit commands and preconditions. Independent Kafka preflight reads the brokers directly when requested. These are separate paths, not an automatic recovery chain. See [migration guarantees](MIGRATIONS.md) and [recovery limits](RECOVERY.md#doc-limits).

The map stops at public endpoint boundaries. It does not model FE/BE scheduling, distributed transaction internals, latency, resource utilization or backend drain. No visual state implies a successful live observation, takeover authorization, write rollback or offset reconciliation.

<a name="doc-packages"></a>

## Understand the five packages

![Five Doris packages, required and optional dependencies, and one public application entry](assets/packages.svg)

Package arrows mean **depends on**, not “calls next.” Solid arrows are required package dependencies; dashed arrows are optional dependencies activated by the labelled features. This is the potential normal dependency topology. Cargo feature unification determines an application's resolved build; task highlighting in the interactive view is an explanation, not a Cargo trace.

| Package | Owns | Boundary |
| --- | --- | --- |
| `sqlx-doris` | Explicit Doris exports, query macros, optional helpers and one thin binary | The application's only Doris extension |
| `sqlx-doris-core` | Database identity, protocol, bindings, exact values, SQLx traits and migration lock primitives | No CLI, HTTP maintenance or Kafka orchestration |
| `sqlx-doris-macros` | Describe/type checking, offline metadata and generated code | Build time; reuses core and SQLx macro infrastructure |
| `sqlx-doris-ops` | Application helpers, Stream Load and feature-gated specialist workflows | Explicit requests/evidence; no command rendering or process exits |
| `sqlx-doris-cli` | Arguments, Cargo/prepare, files, commands and human/JSON rendering | Delegates domain operations; does not add another driver |

The facade always depends on core and optionally on macros, ops and CLI. The macros package's query feature enables core; ops always depends on core, and CLI always depends on both core and ops (cli-support).

Core owns the single Doris identity, including SQLx migration implementations. Internal packages depend toward core and never back on the facade. They share upstream `sqlx-core`; upstream SQLx is not a sixth Doris package. The facade does not re-export SQLx's generic namespace. See [dependencies and feature isolation](DEPENDENCIES.md) for the resolved-graph checks and upstream feature details.

<a name="doc-cli"></a>

## Select a CLI profile

| Install feature | Adds | Scope |
| --- | --- | --- |
| `cli` | prepare, database, ordinary migrate | Basic development and delivery |
| `cli-admin` | `cli` plus `admin` commands | Doris identity/drain/lock recovery and read-only diagnosis |
| `cli-admin-kafka` | `cli-admin` plus independent Kafka observations | Explicit source snapshot/preflight with supplied evidence |

All profiles install the same `cargo-sqlx-doris` executable. The basic profile excludes maintenance HTTP and Kafka; `cli-admin` supports Doris-only diagnosis independently of Kafka. No profile implicitly enables application Stream Load or driver query macros. Follow the [installation commands](INSTALLATION.md#doc-cli) and [CLI reference](CLI.md).

<a name="doc-explorer"></a>

## Use the interactive atlas

Open [ATLAS.html](ATLAS.html) in a browser from the complete source checkout, keeping its assets directory beside it. The [Simplified Chinese edition](ATLAS.zh-CN.html) is the exact counterpart. Repository hosts may show HTML source instead of running it; the static diagrams and linked tables on this page remain usable there. This documentation page requires no database, build or network connection.

Choose a task to see its API, Doris feature list, upstream requirements, request path, constraints and guide links. Switch between the task map, integration lanes and package topology. The language switch retains task/view context; theme selection offers system, dark and light. Copy copies only the displayed Doris feature list, not a complete dependency manifest. Links open the paired Markdown source in this checkout.

The task buttons and SVG nodes support keyboard activation; arrow keys/Home/End switch view tabs. On narrow screens the task map becomes readable cards; complex diagrams scroll inside their frame, with a fit-to-frame/reading-size control. Text details stay readable. With JavaScript unavailable, the page retains a static map and links to this text equivalent. There is no automatic playback, video, telemetry or remote asset loading.

The diagrams and interactive view share a source model in docs/assets/atlas-data.js. Documentation checks compare features and required/optional dependency arrows with Cargo manifests, validate paired guide anchors and reject stale SVGs. To edit figures, use `./scripts/dev scripts/render-maps --write`; `--check` verifies exact rendering. Rendering additionally requires Node.js 18+; screenshots, browser profiles and review outputs stay external. Final SVG/HTML/CSS/JS are user documentation assets.
