# sqlx-doris

[简体中文](README.md) | English

A Rust SQLx library for accessing MySQL and Apache Doris through the same API. Use native `MySqlPool`,
`.bind()`, and row mapping, selecting the backend when connecting.
Doris types, table models, and transaction rules retain their native semantics.

Version 0.1.0 is not publicly released. Requires Rust 1.94+ and SQLx 0.9.0.
Tested with MySQL 8.4.13 and official Doris 4.1.5.

## Install the compatibility bundle

Place the supplied bundle in an empty working directory and run:

```sh
tar -xzf sqlx-doris-compat-0.1.0.tar.gz
mkdir -p sdk-demo/src
```

The directory layout should be:

```text
work/
  sqlx-doris-compat-0.1.0/
    sdk/sqlx-doris-0.1.0/
    patches/sqlx-mysql/
  sdk-demo/
    Cargo.toml
    src/main.rs
```

Save the following as sdk-demo/Cargo.toml:

```toml
[package]
name = "sdk-demo"
version = "0.1.0"
edition = "2024"

[dependencies]
sqlx-doris = { path = "../sqlx-doris-compat-0.1.0/sdk/sqlx-doris-0.1.0" }
sqlx = { version = "=0.9.0", default-features = false, features = ["mysql", "derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

[patch.crates-io]
sqlx-mysql = { path = "../sqlx-doris-compat-0.1.0/patches/sqlx-mysql" }
```

In this standalone application, both path entries are relative to sdk-demo/Cargo.toml. Adjust them if your
directory names differ.
In a Cargo workspace, dependency paths are relative to the manifest declaring them. Put `[patch.crates-io]`
in the workspace root, with its path relative to that root Cargo.toml. A library's patch entries do not
propagate to its consumers.
The SDK .crate alone does not contain the companion driver and cannot replace the complete compatibility
bundle.

The example declares SQLx directly for `#[derive(sqlx::FromRow)]`; Tokio provides the async runtime.
Existing SQLx projects must also use `=0.9.0` so the application, macros, and SDK share one sqlx-mysql.

## Run your first program

Save the following as sdk-demo/src/main.rs:

```rust,no_run
use sqlx_doris::{Backend, PoolBuilder};

#[derive(Debug, sqlx::FromRow)]
struct Greeting {
    id: i64,
    name: String,
}

async fn greeting(pool: &sqlx::MySqlPool) -> sqlx::Result<Greeting> {
    sqlx::query_as("SELECT CAST(? AS SIGNED) AS id, ? AS name")
        .bind(42_i64)
        .bind("hello")
        .fetch_one(pool)
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    for (backend, variable) in [
        (Backend::MySql, "MYSQL_URL"),
        (Backend::Doris, "DORIS_URL"),
    ] {
        let db = PoolBuilder::new(backend)
            .connect(&std::env::var(variable)?)
            .await?;
        let row = greeting(db.pool()).await?;
        println!("{backend:?}: id={}, name={}", row.id, row.name);
        db.close().await;
    }
    Ok(())
}
```

Enter sdk-demo, configure the connections, and run. Replace the credentials and addresses; the databases
named `app` must already exist:

```sh
cd sdk-demo
export MYSQL_URL='mysql://user:password@mysql-host:3306/app'
export DORIS_URL='mysql://user:password@doris-fe:9030/app'
cargo run
```

The program creates no tables. It binds two parameters and reads the result. The program should print
`id=42, name=hello` for each database.
Doris SQL connections also use mysql:// URLs. `Backend::Doris` enables the compatibility options; changing
only the URL while retaining `Backend::MySql` is insufficient.

## Next steps

The `stream-load` feature is enabled by default and provides `stream_load::StreamLoadClient` for HTTP imports.
Set `default-features = false` for SQL-only use.
Enable `macros` for SQLx `query!` and `query_as!`. These macros need a connection to the target database at
compile time or matching offline metadata.

Supported BLOB/TIME parameters, defaults, and pagination have defined limits. Doris differs from MySQL in
reads within transactions, savepoints, and table models; SQL statements are not universally equivalent.
Writes or commits with an uncertain outcome are never automatically replayed. The application must check the
final state.

The complete source includes docs/INSTALLATION.en.md, docs/USAGE.en.md, docs/COMPATIBILITY.en.md, and
docs/TESTING.en.md for connection options, example commands, and test results.
The SDK uses Apache-2.0; the supplied driver retains its upstream Apache-2.0 OR MIT license.
