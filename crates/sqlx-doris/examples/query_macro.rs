//! Compile with DATABASE_URL set to one backend, then run on both pools.
use sqlx_doris::{Backend, PoolBuilder, sqlx};

#[derive(Debug, PartialEq)]
struct MacroRow {
    id: i64,
    name: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    for (backend, key) in [(Backend::MySql, "MYSQL_URL"), (Backend::Doris, "DORIS_URL")] {
        let db = PoolBuilder::new(backend)
            .connect(&std::env::var(key)?)
            .await?;
        let row = sqlx::query!("SELECT CAST(? AS SIGNED) AS `id!`", 42_i64)
            .fetch_one(db.pool())
            .await?;
        assert_eq!(row.id, 42);
        for name in [Some("macro 中文🙂".to_owned()), None] {
            let actual = sqlx::query_as!(
                MacroRow,
                "SELECT CAST(? AS SIGNED) AS `id!`,CAST(? AS CHAR(32)) AS `name?`",
                42_i64,
                name
            )
            .fetch_one(db.pool())
            .await?;
            assert_eq!(actual, MacroRow { id: 42, name });
        }
        println!("{backend:?} macro query returned {}", row.id);
        db.close().await;
    }
    Ok(())
}
