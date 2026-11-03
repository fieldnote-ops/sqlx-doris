//! Reproducible point-query and streaming measurements against a dedicated FE.
use futures_util::TryStreamExt;
use sqlx::{Connection, Executor, Row, SqlSafeStr};
use sqlx_doris::{
    DorisConnection, DorisPool, DorisPoolOptions,
    operations::{Column, DataType, TableDefinition, TableModel, TableName},
};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

fn report(mode: &str, round: u32, mut micros: Vec<f64>, elapsed: f64) {
    micros.sort_by(f64::total_cmp);
    let n = micros.len();
    println!(
        "{}",
        serde_json::json!({
            "mode": mode,
            "round": round,
            "iterations": n,
            "median_us": micros[n/2],
            "p95_us": micros[(n*95/100).min(n-1)],
            "p99_us": micros[(n*99/100).min(n-1)],
            "qps": n as f64/elapsed
        })
    );
}
async fn measure(url: &str, table: &TableName, n: usize) -> Result<(), Box<dyn std::error::Error>> {
    let mut c = DorisConnection::connect(url).await?;
    let pool = DorisPoolOptions::new()
        .max_connections(8)
        .min_connections(8)
        .connect(url)
        .await?;
    let pool_no_ping = DorisPoolOptions::new()
        .max_connections(8)
        .min_connections(8)
        .test_before_acquire(false)
        .connect(url)
        .await?;
    let sql = sqlx::AssertSqlSafe(format!("SELECT value FROM {table} WHERE id = ?")).into_sql_str();
    let literal =
        sqlx::AssertSqlSafe(format!("SELECT value FROM {table} WHERE id = 42")).into_sql_str();
    for _ in 0..30 {
        let v: String = sqlx::query_scalar(sql.clone())
            .bind(42_i64)
            .fetch_one(&mut c)
            .await?;
        assert_eq!(v, "benchmark");
    }
    for round in 0..5 {
        // Alternate order to reduce systematic warmup and time-of-run bias.
        let modes = if round % 2 == 0 {
            ["cached", "uncached", "pool", "pool_no_ping", "text"]
        } else {
            ["text", "pool_no_ping", "pool", "uncached", "cached"]
        };
        for mode in modes {
            let mut samples = Vec::with_capacity(n);
            let all = Instant::now();
            for _ in 0..n {
                let t = Instant::now();
                let v: String = match mode {
                    "pool_no_ping" => {
                        sqlx::query_scalar(sql.clone())
                            .bind(42_i64)
                            .fetch_one(&pool_no_ping)
                            .await?
                    }
                    "pool" => {
                        sqlx::query_scalar(sql.clone())
                            .bind(42_i64)
                            .fetch_one(&pool)
                            .await?
                    }
                    "text" => sqlx::raw_sql(literal.clone())
                        .fetch_one(&mut c)
                        .await?
                        .try_get(0)?,
                    _ => {
                        sqlx::query_scalar(sql.clone())
                            .bind(42_i64)
                            .persistent(mode == "cached")
                            .fetch_one(&mut c)
                            .await?
                    }
                };
                assert_eq!(v, "benchmark");
                samples.push(t.elapsed().as_secs_f64() * 1e6);
            }
            report(mode, round, samples, all.elapsed().as_secs_f64());
        }
    }
    let start = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let pool = pool_no_ping.clone();
        let sql = sql.clone();
        tasks.spawn(async move {
            for _ in 0..n {
                let v: String = sqlx::query_scalar(sql.clone())
                    .bind(42_i64)
                    .fetch_one(&pool)
                    .await?;
                assert_eq!(v, "benchmark");
            }
            Ok::<_, sqlx::Error>(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result??;
    }
    println!(
        "{}",
        serde_json::json!({
            "mode": "pool_concurrent_8",
            "queries": 8*n,
            "qps": (8*n) as f64/start.elapsed().as_secs_f64()
        })
    );
    let start = Instant::now();
    let mut stream = sqlx::query("SELECT number FROM numbers('number'='100000')").fetch(&mut c);
    let mut sum = 0_i64;
    let mut count = 0;
    while let Some(row) = stream.try_next().await? {
        sum += row.try_get::<i64, _>(0)?;
        count += 1;
    }
    drop(stream);
    assert_eq!(count, 100000);
    assert_eq!(sum, 4_999_950_000);
    println!(
        "{}",
        serde_json::json!({
            "mode": "stream",
            "rows": count,
            "rows_per_second": count as f64/start.elapsed().as_secs_f64()
        })
    );
    c.close().await?;
    pool.close().await;
    pool_no_ping.close().await;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DORIS_TEST_URL")?;
    let n = std::env::var("DORIS_BENCH_ITERATIONS").map_or(Ok(500), |v| v.parse::<usize>())?;
    if n < 30 {
        return Err("DORIS_BENCH_ITERATIONS must be at least 30".into());
    }
    let client = DorisPool::connect(&url).await?;
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(&client)
        .await?;
    let table = TableName::new(
        database,
        format!(
            "_sqlx_doris_bench_{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ),
    );
    let schema = TableDefinition::new(table.clone(), TableModel::Unique, ["id"])
        .column(Column::new("id", DataType::BigInt).not_null())
        .column(Column::new("value", DataType::String))
        .property("enable_unique_key_merge_on_write", "true")
        .property("store_row_column", "true")
        .replication(1);
    sqlx_doris::operations::create_table(&client, &schema).await?;
    let work = async {
        let sql = sqlx::AssertSqlSafe(format!("INSERT INTO {table} VALUES(42,'benchmark')"));
        client.execute(sql).await?;
        let version: String = sqlx::query_scalar("SELECT VERSION()")
            .fetch_one(&client)
            .await?;
        let comment: String = sqlx::query_scalar("SELECT @@version_comment")
            .fetch_one(&client)
            .await?;
        println!(
            "{}",
            serde_json::json!({
                "server_version_comment": comment,
                "server_version": version,
                "iterations_per_round": n,
                "rounds": 5
            })
        );
        measure(&url, &table, n).await
    }
    .await;
    if let Err(error) = work {
        eprintln!("Benchmark table {table} retained for reconciliation; no write retry");
        client.close().await;
        return Err(error);
    }
    client
        .execute(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
        .await?;
    client.close().await;
    Ok(())
}
