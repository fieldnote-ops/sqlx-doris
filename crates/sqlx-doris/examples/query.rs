use sqlx_doris::DorisPool;
#[derive(Debug, sqlx::FromRow)]
struct Answer {
    id: i64,
    name: String,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = DorisPool::connect(&std::env::var("DORIS_URL")?).await?;
    let row: Answer = sqlx::query_as("SELECT CAST(? AS BIGINT) AS id, CAST(? AS STRING) AS name")
        .bind(42_i64)
        .bind("Doris 中文🙂")
        .fetch_one(&pool)
        .await?;
    assert_eq!(row.id, 42);
    assert_eq!(row.name, "Doris 中文🙂");
    println!("{row:?}");
    pool.close().await;
    Ok(())
}
