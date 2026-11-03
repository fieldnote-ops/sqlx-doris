<a name="doc-top"></a>

# SQLx migrations

[English](MIGRATIONS.md) | [简体中文](MIGRATIONS.zh-CN.md)

[Documentation](README.md)

Use upstream SQLx migrations with the native Doris driver. For interrupted runs, follow [Recovery](RECOVERY.md); do not rerun an uncertain file.

<a name="doc-contents"></a>

## Contents

- [Configure the Rust API](#doc-api)
- [Run migrations from the CLI](#doc-commands)
- [DDL, dirty history and completion](#doc-execution)
- [Lock protocol and failure boundaries](#doc-locking)

<a name="doc-api"></a>

## Configure the Rust API

For embedded files, update the dependency entries in your application as follows: upstream `macros` + `migrate` and driver `migrate`. Upstream `derive` alone does not export migrate!. Driver query macros are not required.

```toml
[dependencies]
sqlx = { version = "=0.9.0", default-features = false, features = ["macros", "migrate"] }
sqlx-doris = { version = "0.2.0", features = ["migrate"] }
```

Add this function to your application; the migrations directory is relative to its Cargo manifest:

```rust
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub async fn migrate(pool: &sqlx_doris::DorisPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}
```

Runtime directory loading uses `Migrator::new(Path::new("./migrations")).await?`. Files are `<version>_<description>.sql`, or reversible `.up.sql`/`.down.sql` pairs. To rebuild after embedded files change, emit `cargo:rerun-if-changed=migrations` from build.rs.

<a name="doc-commands"></a>

## Run migrations from the CLI

```sh
export DATABASE_URL='doris://administrator:password@doris-fe:9030/app'
cargo sqlx-doris database create
cargo sqlx-doris migrate add initial --reversible
cargo sqlx-doris migrate run
cargo sqlx-doris migrate info
cargo sqlx-doris migrate revert
cargo sqlx-doris migrate revert --target 0
```

Create the target database first and edit generated SQL before run. `revert` without a target reverts one applied reversible pair; target 0 explicitly reverts through version 0. Use `migrate --source <directory>` and `--table` before the leaf command. `run --target <version>` applies through a target. For a single-BE fixture use global `--replication-num 1`; default metadata replication is 3. `--ddl-timeout` defaults to 300 seconds and `--lock-timeout` to 30. `info` reads history without creating its table and reports missing/changed source files.

<a name="doc-execution"></a>

## DDL, dirty history and completion

Doris DDL is non-atomic across a migration. The driver persists dirty state before business SQL, validates versions/SHA-384 checksums and waits for asynchronous COLUMN/ROLLUP completion before the next step. Group Commit is disabled temporarily and restored on success. Successful up files become applied; successful down files remove their records. Completed SQL is never automatically rolled back or replayed.

Scripts support table/ordinary-view/index DDL and ordinary DML/queries. They reject transaction control, SET, async ingestion, CREATE MATERIALIZED VIEW, database/catalog switches and internal migration-table references. Async ALTER must target the current database. Manage MV and ingestion through their dedicated APIs.

<a name="doc-locking"></a>

## Lock protocol and failure boundaries

All runners in the database share persistent `_sqlx_migrations_lock` and `_sqlx_migrations_gate` tables. Each keeps sqlx_anchor over [-1,0); a fresh sqlx_owner_<UUID> partition over [0,1) acquires exclusion through atomic ADD PARTITION. Release/recovery drops only that UUID partition, never a parent or anchor. Never reuse owner names or change internal layouts. Interrupted gates are not taken over automatically; unknown/earlier comment-only layouts are rejected without conversion.

Normal completion or healthy close releases a lock. If ping/flush clears a pending healthy lock, its connection is retired with an error. Crashes, cancellation, partial/uncertain execution retain durable locks and dirty/applied history. Main release/recovery serializes owner checks and deletion through the gate; an uncertain main DROP retains the gate and retires the connection. Inspect actual owners with a new connection before explicit recovery.

Owner checks precede dirty markers, SQL, async polls and completion markers. They do not atomically fence business SQL queued after a check; generic queries are outside these checkpoints. Stop/isolate old actors and reconcile admitted work. Use `Migrator::set_locking(false)` or `migrate --no-lock` only with external serialization. The [recovery runbook](RECOVERY.md) distinguishes preview, confirmation and uncertain outcomes.
