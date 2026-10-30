# Compatibility and database semantics

[简体中文](COMPATIBILITY.md) | English

The following scope has been tested with SQLx 0.9.0, MySQL 8.4.13, and official Doris 4.1.5. Validate other
versions and unlisted SQL forms against your target instance.

| Capability | Shared usage | Limits |
| --- | --- | --- |
| Type identity | Native MySql/Pool/Connection/Row/Transaction | The driver builds with the SDK, so applications need no patch; use consistent SQLx versions |
| CRUD/DTO/streaming | Original SQL, `.bind()`, FromRow, QueryBuilder | The server validates SQL and table models; early stream drops complete cleanup or close the connection |
| VALUES/DEFAULT | Column lists, static defaults, NULL, multi-row writes | No schema guessing or batch splitting; dynamic defaults, empty VALUES, and auto-increment sequences keep backend semantics |
| Pagination | LIMIT ?, LIMIT ? OFFSET ?, LIMIT ?,? | Native integer parameters; complex expressions and metadata from explicit prepare/online describe have limited support |
| BLOB predicates | Simple column =, <>, !=, <=>, <, <=, >, >=, IN/NOT IN, BETWEEN/NOT BETWEEN | Preserve byte ordering and SQL NULL semantics; no emulation of LIKE, arbitrary CASE/functions/tuples/subqueries, or mixing with non-NULL numeric values |
| BLOB writes | `Vec<u8>` with original VALUES/`.bind()` | Uses public expressions with Doris STRING internally; this does not imply BLOB/VARBINARY support in OLAP columns |
| TIME | `MySqlTime`, chrono::`NaiveTime`, native binding and lossless reads | NaiveTime rejects negative durations and durations of 24 hours or more; distinguish the MySqlTime range from Doris's full microsecond boundaries |
| Explicit TIME | `types::Time` | ±838:59:59.999999; Doris OLAP does not support TIME columns, so query values and explicit STRING representations are separate |
| Large integers/Decimal | LargeInt, BigDecimal | Round to the target precision/scale or reject overflow; no conversion through floating point |
| Complex types | Array/Map/Struct/Variant with explicit projections/constructors | MAP can preserve non-string keys; VARIANT may normalize values or lose information about how root values are presented |
| Dates/time zones | Chrono and TimestampTz | Precision/time zones follow column definitions; TimestampTz rejects sub-microsecond precision and leap seconds |
| Transactions | Top-level `Transaction<MySql>` | No emulation of Doris queries/DDL within transactions or savepoints; manual BEGIN/raw guards have explicit limits |
| Receipts/conflicts | Native backend results | Database results are returned directly; rows_affected, last_insert_id, and primary key models may have different meanings |
| Group Commit | Explicit Off/Sync/Async selection | Async acceptance differs from visibility; failures never cause automatic replay |
| HTTP imports | StreamLoadClient and LoadReceipt | The caller handles subsequent 2PC commit/rollback; configure HTTPS certificates for your deployment |

TIME membership compatibility only handles explicit TIME parameters. Conditions containing only strings do not
infer the column type.
For Doris TIME results, the driver uses metadata to select lossless text reads before execution; ordinary
results retain prepared-statement handling.
String encoding uses the actual session sql_mode rather than a cached guess.

Parameter compatibility supports a limited set of SQL forms. Complex byte predicates, mixed non-NULL types,
ambiguous comments, and values that cannot be encoded losslessly return explicit errors.
Parameters are encoded by type and processed through SQL tokenization; uncertain writes are never
automatically retried.

Extended operations wrap selected public Doris SQL/HTTP capabilities. FE administration, CDC, cross-database
transactions, automatic dual writes, table migration, and moving active pools are outside this library's
scope.
Performance depends on SQL, table models, indexes, batch sizes, and the environment; performance parity with
native SQLx is not promised for every workload.
