# Installation and your first program

[简体中文](INSTALLATION.md) | English

Requires Rust 1.94+ and a C toolchain. Applications build with Cargo and do not need Python.
Version 0.1.0 is not publicly released; use the supplied source or compatibility bundle.
This guide uses the source distribution. Bundle users can follow the [SDK
README](../crates/sqlx-doris/README.en.md) instead.

## 1. Prepare the source and application directories

If you already have the source, place it next to your application directory and name it sqlx-doris.
If you received delivery files, place them in an empty working directory and run one of the following there:

```sh
git clone sqlx-doris-0.1.0.bundle sqlx-doris
```

Or extract the source archive:

```sh
tar -xzf sqlx-doris-source-0.1.0.tar.gz
```

Then create the application directory:

```sh
mkdir -p sdk-demo/src
```

The resulting layout is:

```text
work/
  sqlx-doris/
    crates/sqlx-doris/
    patches/sqlx-mysql/
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
sqlx-doris = { path = "../sqlx-doris/crates/sqlx-doris" }
sqlx = { version = "=0.9.0", default-features = false, features = ["mysql", "derive"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

[patch.crates-io]
sqlx-mysql = { path = "../sqlx-doris/patches/sqlx-mysql" }
```

In this standalone application, both path entries are relative to sdk-demo/Cargo.toml. Adjust them to your
layout instead of hardcoding machine-specific absolute paths.
For a Cargo workspace, dependency paths are relative to the business crate's Cargo.toml. Put
`[patch.crates-io]` in the workspace root,
with its path relative to the root Cargo.toml. Paths in workspace.dependencies are also relative to the
workspace root.
Align existing SQLx dependencies to `=0.9.0`. The direct SQLx dependency supports the FromRow derive macro;
Tokio supplies the async runtime.

## 3. Save the program

Save this as sdk-demo/src/main.rs:

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

The program needs no tables. The same greeting function accepts a native MySqlPool, binds parameters on either
database, and maps the result to the same struct.

## 4. Configure connections and run

Prepare accounts that can log in to MySQL and Doris. The app database in each example URL must already exist;
replace it with a database you can access.
To create demonstration databases, an administrator can run this on each database server:

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

Replace the placeholder credentials and hosts with real connection settings. Successful output is:

```text
MySql: id=42, name=hello
Doris: id=42, name=hello
```

Doris uses the FE SQL port, usually 9030, and still uses a mysql:// URL.
`Backend::Doris` enables Doris compatibility options. To switch databases, change the connection settings and
backend selection; the query function can be reused.
URL-encode characters such as @, /, #, and % in passwords. Alternatively, set credentials through
`MySqlConnectOptions`, as shown in the usage guide.

## Add to an existing project

Borrow `client.pool()` directly, or call `client.into_pool()` to obtain the native MySqlPool.
`Client::from_pool` only wraps an existing pool; it does not install Doris initialization or protocol options.
Use PoolBuilder to create Doris pools.
Continue using `sqlx::query`, `.bind()`, `FromRow`, and `QueryBuilder` in application code.

The HTTP import feature, `stream-load`, is enabled by default. Set `default-features = false` on the
sqlx-doris dependency for SQL-only use.
SQLx compile-time macros require `features = ["macros"]`. Ordinary query/.bind() calls do not require
DATABASE_URL or offline metadata.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Local dependency path not found | Check the directory layout and both path entries in the application's root Cargo.toml |
| Missing doris_transaction_compatibility at compile time | The root patch is inactive; check the workspace root and sqlx-mysql path |
| MySqlPool type mismatch | Use cargo tree -i sqlx-mysql to check for mixed SQLx versions |
| NotPresent at startup (missing environment variable) | Set the connection variables in the same shell that runs the program |
| Authentication failure or missing database | Check permissions, password encoding, database name, and the FE SQL port |
| Compile-time macro requires DATABASE_URL | Provide the target database connection or matching offline metadata; ordinary query does not need it |

When first adding the patch, let Cargo resolve dependencies and update the application's Cargo.lock. Then use
`cargo run --locked` to keep that resolution fixed.
See [usage](USAGE.en.md) for source examples and [testing](TESTING.en.md) for the full test procedure.
