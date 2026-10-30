//! Load structured JSON through FE/BE, query with SQLx, and remove the demo table.
use sqlx_doris::{
    Backend, Client, PoolBuilder, Result,
    operations::{self, Column, DataType as T, TableDefinition, TableModel, TableName},
    sqlx::{self, Executor},
    stream_load::StreamLoadClient,
    types::Array,
};
use std::{
    env,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

async fn load(db: &Client, http: &StreamLoadClient, schema: &TableDefinition) -> Result<()> {
    let text = "中文🙂 \" \\ \n \0";
    let receipt = http
        .load_table(schema)?
        .label(format!("{}_batch_1", schema.name.table))
        .send_json(&[serde_json::json!({"id":1,"name":"demo","tags":[text,null]})])
        .await?;
    println!("{receipt}; state={:?}", receipt.state());
    operations::wait_until(
        Duration::from_secs(20),
        Duration::from_millis(50),
        || async {
            let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM {}",
                schema.name
            )))
            .fetch_one(db.pool())
            .await?;
            Ok(count == 1)
        },
    )
    .await?;
    let mut select = schema.select_builder()?;
    let row: (i64, String, Array<Option<String>>) =
        select.build_query_as().fetch_one(db.pool()).await?;
    assert_eq!(
        row,
        (1, "demo".into(), Array(vec![Some(text.into()), None]))
    );
    println!("Stream Load data is visible and matches the input");
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let db = PoolBuilder::new(Backend::Doris)
        .connect(&env::var("DORIS_URL")?)
        .await?;
    let mut http = StreamLoadClient::new(
        &env::var("DORIS_HTTP_URL")?,
        env::var("DORIS_HTTP_USER").unwrap_or_else(|_| "root".into()),
        env::var("DORIS_HTTP_PASSWORD").ok(),
    )?;
    if let Ok(be) = env::var("DORIS_BE_HTTP_URL") {
        http = http.backend_endpoint(&be)?;
    }
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(db.pool())
        .await?;
    let name = TableName::new(
        database,
        format!(
            "_sqlx_doris_example_{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ),
    );
    let schema = TableDefinition::new(name.clone(), TableModel::Duplicate, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("name", T::String))
        .column(Column::new("tags", T::array(T::String)))
        .replication(1);
    db.create_table(&schema).await?;
    let result = load(&db, &http, &schema).await;
    let cleanup = db
        .pool()
        .execute(sqlx::AssertSqlSafe(format!("DROP TABLE {name}")))
        .await;
    db.close().await;
    result?;
    cleanup?;
    Ok(())
}
