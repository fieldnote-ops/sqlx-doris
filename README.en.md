# sqlx-doris

[简体中文](README.md) | English

Share Rust SQLx queries, parameter binding, and row mapping between MySQL and Apache Doris.
Select the backend when connecting, then use native `MySqlPool`, `FromRow`, `QueryBuilder`, and `.bind()`.
Table models, transactions, affected row counts, and data visibility follow each database's own rules.

Version 0.1.0 is not published on crates.io; the source lives on
[GitHub](https://github.com/fieldnote-ops/sqlx-doris). Tested with SQLx 0.9.0, MySQL 8.4.13, and official
Doris 4.1.5; requires Rust 1.94+.
Install both the SDK and the supplied sqlx-mysql driver patch, with one `[patch.crates-io]` entry in the
application's root Cargo.toml.
You can consume both as git dependencies straight from GitHub; see the
[installation guide](docs/INSTALLATION.en.md) for the complete setup.

## Run your first query

Run these commands from the source root. You need Rust/Cargo, a C toolchain, Python 3.12+, and accessible
MySQL and Doris instances.
Replace the volume path, credentials, and addresses with your own values. The database named `app` in each URL
must
already exist.
The example queries constants; it creates no tables and writes no data.

```sh
export SQLX_DORIS_EXTERNAL_VOLUME="<absolute path to a mounted build volume>"
export MYSQL_URL='mysql://user:password@mysql-host:3306/app'
export DORIS_URL='mysql://user:password@doris-fe:9030/app'
./scripts/dev --print-env
./scripts/dev cargo fetch --locked
./scripts/dev cargo run --locked -p sqlx-doris --example dual_pool
```

On success, it prints `MySQL and Doris returned User { id: 42, ... }`.
Build output, caches, and temporary files go to the selected build volume; an unmounted volume causes an
immediate failure.
Python is only needed for source scripts, not for applications using the SDK.

## Installation and usage

- [Installation](docs/INSTALLATION.en.md): source or compatibility bundle setup, a complete program,
  connection settings, and troubleshooting.
- [Usage](docs/USAGE.en.md): pools, transactions, Doris types, Stream Load, and runnable examples.
- [Compatibility](docs/COMPATIBILITY.en.md): BLOB, TIME, defaults, pagination, and database semantics.
- [Testing](docs/TESTING.en.md): checks without databases, tests against both databases, prerequisites, and
  actual results.
- [Dependencies and licenses](docs/DEPENDENCIES.en.md): driver provenance and security notes for locked
  dependencies.

## Background

This is a Rust rework of a 2022 Python project that used both MySQL and Doris.
Performance tuning sometimes meant moving individual application tables between the two databases. At the
time, the Python driver used for newer MySQL versions did not work with Doris, which needed a separate
driver supporting the older MySQL protocol. Moving a table also meant changing data access code.

Built on Rust SQLx, this library handles protocol and parameter binding differences in one place.
When moving a table between databases, you can change the connection settings and backend selection while
reusing SQL, `.bind()`, and row mapping where supported. This reduces the code you need to adapt and maintain
separately for each database. You still manage table migration and account for each database's own semantics.

## License

The SDK is licensed under Apache-2.0. The supplied sqlx-mysql driver retains its upstream Apache-2.0 OR MIT
license; full license texts are included in the source and compatibility bundle.
