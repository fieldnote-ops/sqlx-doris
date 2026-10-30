# sqlx-doris

[简体中文](README.md) | English

Share Rust SQLx queries, parameter binding, and row mapping between MySQL and Apache Doris.
Select the backend when connecting, then use native `MySqlPool`, `FromRow`, `QueryBuilder`, and `.bind()`.
Table models, transactions, affected row counts, and data visibility follow each database's own rules.

Version 0.1.0 is not published on crates.io; the source lives on
[GitHub](https://github.com/fieldnote-ops/sqlx-doris). Requires Rust 1.94+ and SQLx 0.9.0.
Tested with MySQL 8.4.13 and official Doris 4.1.5. The bundled sqlx-mysql driver ships as source in
this repository, so no patching is needed at install time.

## Install the compatibility bundle or the source

Place the supplied bundle in an empty working directory and run:

```sh
tar -xzf sqlx-doris-compat-0.1.0.tar.gz
mkdir -p sdk-demo/src
```

The resulting layout is:

```text
work/
  sqlx-doris-compat-0.1.0/
    crates/sqlx-doris/
    crates/sqlx-mysql/
  sdk-demo/
    Cargo.toml
    src/main.rs
```

With GitHub, use the cloned repository directory instead of `sqlx-doris-compat-0.1.0`; the layout is
the same.

Save this as sdk-demo/Cargo.toml:

```toml
[package]
name = "sdk-demo"
version = "0.1.0"
edition = "2024"

[dependencies]
sqlx-doris = { path = "../sqlx-doris-compat-0.1.0/crates/sqlx-doris" }
sqlx = { version = "=0.9.0", default-features = false, features = ["derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The SDK pulls `crates/sqlx-mysql` through its own path dependency, so the application needs no root
manifest patch entry. The direct SQLx dependency only supplies the `#[derive(sqlx::FromRow)]` macro;
query functions come from `sqlx_doris::sqlx`. Align existing SQLx projects to `=0.9.0`; Tokio
supplies the async runtime.

## Run your first program

Save this as sdk-demo/src/main.rs:

```rust,no_run
use sqlx_doris::{
    Backend, MySqlPool, PoolBuilder,
    sqlx::query_as,
};

#[derive(Debug, sqlx::FromRow)]
struct Greeting {
    id: i64,
    name: String,
}

async fn greeting(pool: &MySqlPool) -> sqlx_doris::sqlx::Result<Greeting> {
    query_as("SELECT CAST(? AS SIGNED) AS id, ? AS name")
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

Set the connections in the sdk-demo directory and run. Replace the credentials and hosts with your
own values; the database named `app` must already exist:

```sh
cd sdk-demo
export MYSQL_URL='mysql://user:password@mysql-host:3306/app'
export DORIS_URL='mysql://user:password@doris-fe:9030/app'
cargo run
```

The program creates no tables; it binds two parameters and reads the result. Both databases print
`id=42, name=hello` on success.
Doris also uses a mysql:// SQL URL; only `Backend::Doris` enables the matching compatibility options,
so do not keep `Backend::MySql` and change the URL alone.

## Using it afterwards

`stream-load` is enabled by default; HTTP imports use `stream_load::StreamLoadClient`. Use
`default-features = false` for query-only applications.
Take queries, binding, `FromRow`, and `QueryBuilder` from `sqlx_doris::sqlx`; the types and pools come
from the bundled driver. For SQLx compile-time `query!` and `query_as!` macros you must switch to the
upstream SQLx macros and point `sqlx-mysql` at this repository's `crates/sqlx-mysql` (macros expand to
`::sqlx::` paths); see the compile-time macros section of docs/USAGE.md.

Supported BLOB/TIME parameters, defaults, and pagination have limits. In-transaction reads, savepoints,
and table models differ between Doris and MySQL, so not every SQL statement is fully equivalent.
Uncertain writes or commits are never replayed automatically; reconcile the final state from the
business side.

The full source includes docs/INSTALLATION.md, docs/USAGE.md, docs/COMPATIBILITY.md, and
docs/TESTING.md with connection options, example commands, and test results.
The SDK uses Apache-2.0; the bundled driver retains its upstream Apache-2.0 OR MIT license.
