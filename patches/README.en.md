# Supplied SQLx MySQL driver

[简体中文](README.md) | English

The sqlx-mysql directory is based on upstream 0.9.0 and retains its package name, version, and public MySql
types.
Only the client driver is modified. Doris FE/BE runs the official version and needs no server modifications or
rebuilds.
See the SDK README for a complete application setup; the source distribution also includes
docs/INSTALLATION.en.md.

## Why the root patch is required

Configure this in the application's workspace-root Cargo.toml:

```toml
[patch.crates-io]
sqlx-mysql = { path = "../sqlx-doris/patches/sqlx-mysql" }
```

The path is relative to that root Cargo.toml; adjust it to your layout.
The application, SQLx macros, and SDK then use the same sqlx-mysql, allowing native pools, rows, and
transactions to be interchanged.
A library's patch does not propagate to its consumers. Omitting it causes a compile error for the missing
compatibility API.
No global Cargo configuration or registry source changes are needed.

## Changes

- Accept Doris's valid 10-byte PrepareOk response, treating the absent warning field as zero while still
  rejecting truncated or invalid responses.
- Handle top-level write transactions, cancellation, and connections that cannot safely be reused through
  explicit Doris connection options.
- Adapt BLOB, TIME, VALUES, DEFAULT, and pagination parameters within the supported SQL scope; application
  code keeps `.bind()`.
- Select compatibility paths before sending packets. Writes are not replayed after failures, and unsupported
  types or SQL forms return explicit errors.

Ordinary MySQL connections retain the native protocol. TIME membership convenience is a separate option: off
by default in the driver, on by default in SDK MySQL settings, and can be disabled explicitly.
See the SDK README for scope and database semantics, with the full list in docs/COMPATIBILITY.en.md in the
source distribution.

## Provenance, licenses, and verification

Based on upstream commit 003b698e99e024f3621b8043a2426fde5b741171, with full Apache-2.0 OR MIT license texts
preserved.
sqlx-mysql-provenance.json records hashes for 73 upstream files, 15 modified files, and the licenses.
The three patches reconstruct the source in prepare-ok, native-transactions, safe-bindings order.

After configuring the build volume, check the source independently:

```sh
./scripts/dev ./scripts/check-sqlx-patch
```

From the source root, follow docs/TESTING.en.md to run check and generate packages, then use verify-delivery
to check source reconstruction, file lists, and the SDK/driver pair.
This driver is supplied with the project; it has not been separately published or merged upstream.
