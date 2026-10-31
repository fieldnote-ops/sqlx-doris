# Bundled SQLx MySQL driver

[简体中文](README.md) | English

`crates/sqlx-mysql` is based on upstream 0.9.0 and keeps its package name, version, and public MySql
types. Only the client driver changes; Doris FE/BE run official builds and need no rebuild.
See the SDK README for the full application setup; the source archive also ships docs/INSTALLATION.md.

## How it is integrated

The SDK pulls `crates/sqlx-mysql` through a path dependency, so applications declare only the SDK and
need no root-manifest patch entry:

```toml
[dependencies]
sqlx-doris = { path = "../sqlx-doris/crates/sqlx-doris" }
```

The driver builds with the SDK, and queries, binding, and pools come from `sqlx_doris::sqlx`, so there is
exactly one set of native MySql/Pool/Row types.
When you add upstream SQLx for the `FromRow` derive macro, use
`default-features = false, features = ["derive"]` and it brings no second driver.
Only compile-time macros (`query!`, `query_as!`) need upstream SQLx with the `mysql` and `macros`
features plus `sqlx-mysql` pointing at this directory; that path needs a root-manifest patch entry so the
`::sqlx::` paths the macros expand to match this crate.

## Changes

- Accept the valid 10-byte Doris PrepareOk, treating a missing warning field as 0 while still rejecting
  truncated or invalid responses.
- Handle top-level write transactions, cancellation, and connections that cannot be reused safely through
  explicit Doris connection options.
- Handle BLOB, TIME, VALUES, DEFAULT, and pagination parameters within the supported SQL subset; business
  code keeps using `.bind()`.
- Choose compatibility paths before sending, never replay after a failed write, and report unsupported
  types or SQL shapes explicitly.

Ordinary MySQL connections keep the native protocol. The TIME list convenience is a separate option: off
by default in the driver and on in the SDK's MySQL profile, and can be disabled explicitly.
Database semantics and the supported subset are in the SDK README, with the full list in the source
archive's docs/COMPATIBILITY.md.

## Provenance, licenses, and verification

It corresponds to upstream commit 003b698e99e024f3621b8043a2426fde5b741171 and keeps both Apache-2.0 and
MIT license texts. sqlx-mysql-provenance.json records hashes for 73 upstream files, 15 modified files, and
the licenses.
Three patches (prepare-ok, native-transactions, safe-bindings) reconstruct the current source.

With a configured build volume you can check the source on its own:

```sh
./scripts/dev ./scripts/check-sqlx-patch
```

After building the compatibility bundle with check as described in docs/TESTING.md, verify-delivery checks
patch reconstruction, the file set, and the SDK/driver pairing.
The driver ships with this project and is published as `sqlx-doris-mysql` 0.9.0 on crates.io (referenced by
`sqlx-doris`); it is not merged upstream yet.
