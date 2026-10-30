# Installation and your first program

[简体中文](INSTALLATION.md) | English

Requires Rust 1.94+ and a C toolchain. Applications build with Cargo and do not need Python.
Version 0.1.0 is not published on crates.io; use the compatibility bundle or the GitHub source. The
bundled driver ships inside this repository, so applications need no root-manifest patch entry.

## 1. Prepare the source and application directories

With the compatibility bundle, extract it into an empty working directory:

```sh
tar -xzf sqlx-doris-compat-0.1.0.tar.gz
```

With GitHub, clone the repository instead and use that directory name:

```sh
git clone https://github.com/fieldnote-ops/sqlx-doris.git sqlx-doris
```

Then create the application directory:

```sh
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

## 2. Configure Cargo.toml

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

The path is relative to sdk-demo/Cargo.toml; adjust it to your layout instead of hardcoding a
machine-specific absolute path. The SDK resolves `crates/sqlx-mysql` through its own path dependency,
so the application needs no patch section; declaring the driver again would introduce a second type.
In a Cargo workspace, a dependency's path is relative to the manifest that declares it, and
`[workspace.dependencies]` paths are relative to the workspace root.
The direct SQLx dependency only supplies the `#[derive(sqlx::FromRow)]` macro; take query functions and
pool types from `sqlx_doris::sqlx`. Align existing SQLx dependencies to `=0.9.0`; Tokio supplies the
async runtime.

## 3. Save the program

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

This program uses no tables. One greeting function takes a native MySqlPool, binds parameters on both
databases, and maps each result into the same struct.

## 4. Configure connections and run

Prepare MySQL and Doris accounts you can log in with. `app` in the example URLs is an existing database;
substitute one you may use. An administrator can create a demo database on both servers:

```sql
CREATE DATABASE IF NOT EXISTS app;
```

Then enter the application directory:

```sh
cd sdk-demo
export MYSQL_URL='mysql://user:password@mysql-host:3306/app'
export DORIS_URL='mysql://user:password@doris-fe:9030/app'
cargo run
```

Replace the placeholder credentials and hosts with real values. A successful run prints:

```text
MySql: id=42, name=hello
Doris: id=42, name=hello
```

Doris uses the FE SQL port, usually 9030, and the URL still starts with mysql://.
`Backend::Doris` enables the Doris compatibility options; switching databases changes the connection
settings and backend selection while query functions stay reusable.
Characters such as @, /, #, and % in passwords need URL encoding; use `MySqlConnectOptions` when you
prefer not to encode them, as described in the usage guide.

## Joining an existing project

Borrow `client.pool()` or take ownership with `client.into_pool()` to obtain a native MySqlPool.
`Client::from_pool` only wraps an existing pool without Doris initialization or protocol options;
create Doris pools through PoolBuilder.
Keep using `query`, `.bind()`, `FromRow`, and `QueryBuilder` from `sqlx_doris::sqlx`, and do not mix in
the upstream SQLx pool types.

HTTP import (`stream-load`) is enabled by default; set `default-features = false` on the sqlx-doris
dependency for query-only applications.
Compile-time macros (`query!`, `query_as!`) come from upstream SQLx and need their own `sqlx`
dependency (`features = ["mysql", "macros"]`) plus `sqlx-mysql` pointing at this repository's
`crates/sqlx-mysql`; ordinary query/.bind() calls need neither DATABASE_URL nor offline metadata.

## Common errors

| Symptom | Check |
| --- | --- |
| Local dependency path not found | Compare the layout and the path in sdk-demo/Cargo.toml |
| MySqlPool type mismatch | Take queries from `sqlx_doris::sqlx`; confirm a single driver with cargo tree -i sqlx-mysql |
| NotPresent at startup (missing variable) | Set the variable in the same shell that runs the program |
| Authentication failure or missing database | Check grants, password encoding, database name, and the FE SQL port |
| Macros demand DATABASE_URL | Compile-time macros need the upstream SQLx macros and a describe source; plain queries do not |

After the first resolution you can pin the application Cargo.lock with `cargo run --locked`.
See the [usage guide](USAGE.en.md) and [testing notes](TESTING.en.md) for source examples and the full
test procedure.
