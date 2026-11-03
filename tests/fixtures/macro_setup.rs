use dbkit::DorisConnection;
use sqlx::{Connection, Executor, Row, SqlSafeStr};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = DorisConnection::connect(&std::env::var("DORIS_TEST_URL")?).await?;
    let table = std::env::var("MACRO_TABLE")?;
    assert!(
        table
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    );
    let mode = std::env::args().nth(1).unwrap();
    if mode == "setup" {
        c.execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {table}(id BIGINT NOT NULL,name STRING,wide LARGEINT,items ARRAY<INT>,flag BOOLEAN,j JSON,`(` BIGINT) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"))).await?;
        c.execute(sqlx::AssertSqlSafe(format!("INSERT INTO {table} VALUES(1,'name',170141183460469231731687303715884105727,[1,2],true,'{{\"n\":1}}',7)"))).await?;
        let catalog:Vec<(String,String,String)>=sqlx::query_as("SELECT COLUMN_NAME,DATA_TYPE,COLUMN_TYPE FROM information_schema.columns WHERE TABLE_NAME=?").bind(&table).fetch_all(&mut c).await?;
        println!("{catalog:?}");
        for row in c
            .fetch_all(sqlx::AssertSqlSafe(format!("DESC {table}")))
            .await?
        {
            println!(
                "{:?}",
                (row.try_get::<String, _>(0)?, row.try_get::<String, _>(1)?)
            );
        }
        let meta = c
            .describe(
                sqlx::AssertSqlSafe(format!("SELECT id,name,wide,items FROM {table}"))
                    .into_sql_str(),
            )
            .await?;
        println!("{meta:?}");
    } else if mode == "count" {
        let count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                .fetch_one(&mut c)
                .await?;
        assert_eq!(count, 1, "compile-time business writes must never execute");
    } else {
        c.execute(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
            .await?;
    }
    c.close().await?;
    Ok(())
}
