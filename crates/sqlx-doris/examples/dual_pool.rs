//! Run the same query function and FromRow DTO on both backends.
use sqlx_doris::{
    Backend, MySqlPool, PoolBuilder,
    sqlx::{self, FromRow},
};

#[derive(Debug, FromRow, PartialEq)]
struct User {
    id: i64,
    name: String,
}

async fn lookup(pool: &MySqlPool, id: i64, name: &str) -> sqlx::Result<User> {
    sqlx::query_as("SELECT CAST(? AS SIGNED) AS id, ? AS name")
        .bind(id)
        .bind(name)
        .fetch_one(pool)
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mysql = PoolBuilder::new(Backend::MySql)
        .connect(&std::env::var("MYSQL_URL")?)
        .await?;
    let doris = PoolBuilder::new(Backend::Doris)
        .connect(&std::env::var("DORIS_URL")?)
        .await?;
    let expected = User {
        id: 42,
        name: "同一个查询函数 🙂".into(),
    };
    let one = lookup(mysql.pool(), expected.id, &expected.name).await?;
    let two = lookup(doris.pool(), expected.id, &expected.name).await?;
    assert_eq!(one, expected);
    assert_eq!(two, expected);
    println!("MySQL and Doris returned {expected:?}");
    mysql.close().await;
    doris.close().await;
    Ok(())
}
