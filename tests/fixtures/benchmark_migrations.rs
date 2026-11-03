use sqlx::{
    ConnectOptions, Connection, SqlSafeStr,
    migrate::{MigrateDatabase, Migration, MigrationType, Migrator},
};
use sqlx_doris::{Doris, DorisConnectOptions, DorisConnection};
use std::{
    borrow::Cow,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
fn migrator(n: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            (1..=n)
                .map(|v| {
                    Migration::new(
                        v,
                        "metadata benchmark".into(),
                        MigrationType::Simple,
                        sqlx::AssertSqlSafe("SELECT 1".to_owned()).into_sql_str(),
                        true,
                    )
                })
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = format!(
        "sdk_mbench_{}",
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    let options = std::env::var("DORIS_TEST_URL")?
        .parse::<DorisConnectOptions>()?
        .database(&db)
        .migration_replication(1);
    let url = options.to_url_lossy().to_string();
    Doris::create_database(&url).await?;
    let mut c = DorisConnection::connect_with(&options).await?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let start = Instant::now();
        migrator(1).run(&mut c).await?;
        println!(
            "{{\"mode\":\"first_apply\",\"ms\":{}}}",
            start.elapsed().as_secs_f64() * 1000.0
        );
        for version in 2..=6 {
            let m = migrator(version);
            let start = Instant::now();
            m.run(&mut c).await?;
            println!(
                "{{\"mode\":\"incremental_apply\",\"version\":{version},\"ms\":{}}}",
                start.elapsed().as_secs_f64() * 1000.0
            );
            let start = Instant::now();
            m.run(&mut c).await?;
            println!(
                "{{\"mode\":\"unchanged\",\"version\":{version},\"ms\":{}}}",
                start.elapsed().as_secs_f64() * 1000.0
            );
        }
        Ok(())
    }
    .await;
    c.close().await?;
    match result {
        Ok(()) => {
            Doris::drop_database(&url).await?;
            Ok(())
        }
        Err(error) => {
            eprintln!(
                "Migration benchmark database {db} retained; inspect history before any recovery"
            );
            Err(error)
        }
    }
}
