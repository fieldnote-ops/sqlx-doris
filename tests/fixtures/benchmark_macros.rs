use sqlx::Connection;
use sqlx_doris::DorisConnection;
use std::time::Instant;
async fn one(c: &mut DorisConnection, checked: bool) -> Result<(), sqlx::Error> {
    if checked {
        let row = sqlx_doris::query!(
            "SELECT CAST(? AS BIGINT) AS `id!`,CAST(? AS STRING) AS `name!: String`",
            42_i64,
            "Doris 中文🙂"
        )
        .fetch_one(c)
        .await?;
        assert_eq!((row.id, row.name), (42, "Doris 中文🙂".into()));
    } else {
        let row: (i64, String) = sqlx::query_as(
            "SELECT CAST(? AS BIGINT) AS `id!`,CAST(? AS STRING) AS `name!: String`",
        )
        .bind(42_i64)
        .bind("Doris 中文🙂")
        .fetch_one(c)
        .await?;
        assert_eq!(row, (42, "Doris 中文🙂".into()));
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = DorisConnection::connect(&std::env::var("DORIS_TEST_URL")?).await?;
    let n = std::env::var("DORIS_BENCH_ITERATIONS")
        .unwrap_or("500".into())
        .parse::<usize>()?;
    assert!(n >= 100);
    for _ in 0..50 {
        one(&mut c, true).await?;
        one(&mut c, false).await?;
    }
    for round in 0..5 {
        for checked in if round % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let mut samples = Vec::with_capacity(n);
            let start = Instant::now();
            for _ in 0..n {
                let t = Instant::now();
                one(&mut c, checked).await?;
                samples.push(t.elapsed().as_secs_f64() * 1000.0);
            }
            let seconds = start.elapsed().as_secs_f64();
            samples.sort_by(f64::total_cmp);
            let percentile = |p: f64| samples[((n - 1) as f64 * p).round() as usize];
            println!(
                "{{\"mode\":\"{}\",\"round\":{round},\"iterations\":{n},\"qps\":{},\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{}}}",
                if checked { "checked" } else { "runtime" },
                n as f64 / seconds,
                percentile(0.5),
                percentile(0.95),
                percentile(0.99)
            );
        }
    }
    c.close().await?;
    Ok(())
}
