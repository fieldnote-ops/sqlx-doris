//! Create an owned single-replica demo table, round-trip native types, then drop it.
use sqlx::Executor;
use sqlx_doris::operations::Result;
use sqlx_doris::{
    DorisPool,
    operations::{Column, DataType as T, TableDefinition, TableModel, TableName},
    types::{Array, LargeInt, Map},
};
use std::time::{SystemTime, UNIX_EPOCH};

async fn round_trip(db: &DorisPool, schema: &TableDefinition) -> Result<()> {
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
    .execute(db)
    .await?;
    let mut select = schema.select_builder()?;
    select.push(" WHERE id=").push_bind(1_i64);
    let row: (i64, LargeInt, Map<i32, Array<Option<String>>>) =
        select.build_query_as().fetch_one(db).await?;
    assert_eq!(row, (1, LargeInt(i128::MAX), attrs));
    println!("Native types persisted and decoded without loss");
    Ok(())
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let db = DorisPool::connect(&std::env::var("DORIS_URL")?).await?;
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&db)
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
    sqlx_doris::operations::create_table(&db, &schema).await?;
    if let Err(error) = round_trip(&db, &schema).await {
        eprintln!("Native-type demo table {name} retained for reconciliation");
        db.close().await;
        return Err(error.into());
    }
    db.execute(sqlx::AssertSqlSafe(format!("DROP TABLE {name}")))
        .await?;
    db.close().await;
    Ok(())
}
