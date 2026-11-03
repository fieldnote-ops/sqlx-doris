use sqlx::{AssertSqlSafe, Connection, Executor};
use sqlx_doris::DorisConnection;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = DorisConnection::connect(&std::env::var("DATABASE_URL")?).await?;
    let mode = std::env::args().nth(1).unwrap();
    let table = if mode.starts_with("main-") {
        "_sqlx_migrations_lock"
    } else {
        "_sqlx_migrations_gate"
    };
    if mode == "drop" || mode == "main-drop" {
        c.execute(AssertSqlSafe(format!("DROP TABLE {table}")))
            .await?;
    } else {
        let owner = std::env::args().nth(2).unwrap();
        assert!(owner.bytes().all(|v| v.is_ascii_hexdigit() || v == b'-'));
        if mode == "unknown" || mode == "legacy" || mode == "main-legacy" {
            c.execute(AssertSqlSafe(format!("DROP TABLE IF EXISTS {table}")))
                .await?;
            let comment = match mode.as_str() {
                "legacy" => format!("sqlx-doris transition owner {owner}"),
                "main-legacy" => format!("sqlx-doris migration owner {owner}"),
                _ => "unrecognized".to_owned(),
            };
            c.execute(AssertSqlSafe(format!("CREATE TABLE {table}(id TINYINT NOT NULL) DUPLICATE KEY(id) COMMENT '{comment}' DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"))).await?;
        } else {
            c.execute("CREATE TABLE IF NOT EXISTS _sqlx_migrations_gate(id TINYINT NOT NULL) DUPLICATE KEY(id) COMMENT 'sqlx-doris transition gate v2' PARTITION BY RANGE(id)(PARTITION sqlx_anchor VALUES [('-1'),('0'))) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')").await?;
            c.execute(AssertSqlSafe(format!("ALTER TABLE _sqlx_migrations_gate ADD PARTITION sqlx_owner_{} VALUES [('0'),('1'))",owner.replace('-',"")))).await?;
        }
    }
    c.close().await?;
    Ok(())
}
