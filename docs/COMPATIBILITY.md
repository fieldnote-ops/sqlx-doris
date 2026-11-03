<a name="doc-top"></a>

# Compatibility and limits

[English](COMPATIBILITY.md) | [简体中文](COMPATIBILITY.zh-CN.md)

[Documentation](README.md)

Validated with Rust 1.94+, SQLx Core 0.9.0 and official Doris 4.1.5. The standalone driver implements Doris's public MySQL-compatible wire protocol.

Older-version adaptation is out of scope. Later Doris versions require metadata, privilege and transaction revalidation; safety-sensitive version gates are not relaxed automatically.

| Capability | Scope |
| --- | --- |
| Runtime SQLx APIs | query/query_as/query_scalar/raw_sql/QueryBuilder/FromRow |
| Pools | Native Pool<Doris> configuration, timeouts and lifecycle hooks |
| Prepared statements | COM_STMT_PREPARE/EXECUTE/CLOSE and per-connection LRU cache |
| Transactions | Top-level Doris write transactions; no nested transactions/savepoints/custom BEGIN |
| Values | Integers, floats, strings, bytes, NULL, decimals, DATE/DATETIME, JSON and explicit Doris types |
| Ingestion | Optional Stream Load; SQL writes follow server syntax |
| TLS | FE TLS, CA/hostname verification and client certificates |
| Authentication | mysql_native_password |
| Checked queries | Six query!/query_file! macros, renamed dependencies, output/count checks; weak parameter typing |
| Offline builds | SQLx Describe JSON, external SQLX_OFFLINE_DIR, prepare/--check |
| Migrations | Migrator, migrate!, reversible files, dirty/checksum, durable exclusion, async ALTER waiting and explicit recovery |
| CLI | cargo sqlx-doris prepare/migrate/database; cli-admin: admin lock/identity/drain/routine/diagnose/migrate repair; cli-admin-kafka: admin kafka; scoped drain observations, exact registered BROKER LOAD/ROLLUP/MV-task termination; uniform CLI recovery checks |
| Any / Doris registration in upstream cargo sqlx | Not provided |
| Unix sockets, compression, LOCAL INFILE, concatenated ordinary queries | Not provided |

<a name="doc-contents"></a>

## Contents

- [Precision and binding](#doc-precision)
- [Doris semantics](#doc-semantics)
- [Macro and migration limits](#doc-migrations)

<a name="doc-precision"></a>

## Precision and binding

Integers are range-checked. Non-finite float parameters are rejected. Decimal and LARGEINT values never pass through floating point. Unsupported temporal precision is rejected. TIME supports the full Doris microsecond boundary and cannot be stored in OLAP columns.

Some Doris binary TIME results lose fractional boundaries. PREPARE metadata selects text before execution. If the FE reveals TIME only during execution, the driver fails and retires the connection without replay. Use raw_sql or an explicit STRING projection for such expressions.

Bytes use validated Doris public byte functions and ASCII hex arguments. Checked simple comparisons, IN/NOT IN and BETWEEN preserve NULLs. Ambiguous SQL/coercion is rejected; use explicit byte functions for complex expressions. Quotes, NUL, comments and question marks inside parameters stay data.

Bound LIMIT/OFFSET require native nonnegative integers within i64. Text binding uses the actual sql_mode; transactions read it before BEGIN. Do not change sql_mode inside a transaction.

<a name="doc-semantics"></a>

## Doris semantics

Affected-row counts, table models, VARIANT behavior and Group Commit visibility follow Doris. See the official [prepared statement](https://doris.apache.org/docs/4.x/key-features/prepared-statement/) and [transaction](https://doris.apache.org/docs/4.x/data-operate/transaction/) documentation.

<a name="doc-migrations"></a>

## Macro and migration limits

See [checked queries](USAGE.md#doc-macros) and [migrations](MIGRATIONS.md). Doris expression and parameter metadata is incomplete. Ambiguous outputs require explicit type overrides, and nullability defaults conservatively. Offline caches must match the schema and driver version.

Migrations use non-atomic Doris DDL and durable recovery markers, with no file-wide rollback or automatic retries. Scripts support table/ordinary-view/index DDL and ordinary DML/queries. Async materialized views and ingestion tasks use dedicated APIs. ALTER targets the current database and waits for COLUMN/ROLLUP terminal states. Atomic ADD of UUID partitions over a fixed exclusive range provides main and transition lock exclusion among runners using this protocol/version in the same database. Release/recovery uses a durable transition gate for owner validation plus deletion. UUID partitions over a fixed exclusive range provide atomic exclusion; cleanup addresses only its own partition, fencing successors from delayed DROP; interrupted gates require explicit serialized administrative recovery as described in [Recovery](RECOVERY.md).

[Doris Schema Change](https://doris.apache.org/docs/4.x/table-design/schema-change/) describes synchronous light changes and asynchronous jobs.

CLI recover-transition previews removal of an interrupted v2 owner partition before explicit exclusive maintenance, retaining the parent and anchor. Earlier unreleased comment-only main/transition protocols are not converted automatically; unknown partition metadata fails closed. All old actors/commands must be stopped, with one administrator across machines. There is no timeout takeover, and main locks/history are preserved. See [Recovery](RECOVERY.md).

See [ALTER TABLE PARTITION](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/ALTER-TABLE-PARTITION/) for synchronous partition operations, range syntax and the requirement to retain at least one partition. Overlapping-range exclusion and delayed old-partition deletion are additionally verified on Doris 4.1.5; see Testing.

Migration ownership checkpoints stop later work after observed lock loss; they are not atomic SQL fencing. Session KILL cannot prevent reconnects or undo committed work/submitted jobs. Historical connection IDs are never used for automatic termination or takeover. Exclusive recovery still requires external runner isolation; see [Recovery](RECOVERY.md).

Dedicated-identity CLI automation is verified only on a single master FE with Doris 4.1.5 built-in authentication/authorization. Multi-FE, LDAP/Ranger, unknown versions, expanded grants and host variants fail closed. Deletion cuts off future login/permission checks, while admitted commits and jobs may continue. Locks/history are never recovered automatically; see [Recovery](RECOVERY.md).

CLI explicit termination additionally supports registered BROKER LOAD through JobId/TransactionId/ListenerId binding and transaction-ID abort with terminal verification, currently on shared-nothing 4.1.5 with one master FE/one live BE. Unregistered LOAD and ordinary existing Routine Load have no name-based cancellation/stop. See [Recovery](RECOVERY.md) for network, version, race and recovery limits.

Managed Routine Load supports create/inspect/explicit stop under permanent UUID non-reuse, on single-FE built-in-auth shared-nothing 4.1.5 single-table Kafka jobs. STOP still selects by name; raw reuse, metadata restoration or namespace bypass is unfenced, and old jobs are not adopted. See [Managed Routine Load](ROUTINE_LOAD.md).

Migration table validation uses `DorisConnection::migration_table_identifier`. Local identifier/cross-database failures use SQLx `Error::Configuration`; protocol/I/O metadata failures retain their original categories so the CLI does not misclassify malformed server replies as input errors.
