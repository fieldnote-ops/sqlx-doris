//! Create an owned single-replica demo table, round-trip native types, then drop it.
use sqlx_doris::{
    Backend, Client, PoolBuilder, Result,
    operations::{Column, DataType as T, TableDefinition, TableModel, TableName},
    sqlx::{self, Executor},
    types::{Array, LargeInt, Map},
};
use std::time::{SystemTime, UNIX_EPOCH};

async fn round_trip(db: &Client, schema: &TableDefinition) -> Result<()> {
    let attrs = Map(vec![
        (1, Array(vec![Some("中文🙂 \" \\ \n \0".to_owned()), None])),
        (2, Array(vec![])),
    ]);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {} VALUES (?,?,CAST(? AS MAP<INT,ARRAY<STRING>>))",
        schema.name
    )))
    .bind(1_i64)
    .bind(LargeInt(i128::MAX))
    .bind(&attrs)
    .execute(db.pool())
    .await?;
    let mut select = schema.select_builder()?;
    select.push(" WHERE id=").push_bind(1_i64);
    let row: (i64, LargeInt, Map<i32, Array<Option<String>>>) =
        select.build_query_as().fetch_one(db.pool()).await?;
    assert_eq!(row, (1, LargeInt(i128::MAX), attrs));
    println!("Native types persisted and decoded without loss");
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let db = PoolBuilder::new(Backend::Doris)
        .connect(&std::env::var("DORIS_URL")?)
        .await?;
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
    let schema = TableDefinition::new(name.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", T::BigInt).not_null())
        .column(Column::new("wide", T::LargeInt))
        .column(Column::new("attrs", T::map(T::Int, T::array(T::String))))
        .replication(1);
    db.create_table(&schema).await?;
    let result = round_trip(&db, &schema).await;
    let cleanup = db
        .pool()
        .execute(sqlx::AssertSqlSafe(format!("DROP TABLE {name}")))
        .await;
    db.close().await;
    result?;
    cleanup?;
    Ok(())
}
