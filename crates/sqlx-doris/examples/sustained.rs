//! Bounded-memory mixed read workload. Use scripts/benchmark-sustained.
use futures_util::TryStreamExt;
use sqlx::{ConnectOptions, Connection, Executor, Row};
use sqlx_doris::{DorisConnectOptions, DorisConnection, DorisPool, DorisPoolOptions};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const MODES: [&str; 6] = [
    "point",
    "cache_churn",
    "large_field",
    "slow_stream",
    "dropped_stream",
    "consumer_timeout",
];
struct Histogram {
    bins: Vec<u64>,
    count: u64,
    sum_us: u128,
    max_us: u64,
}
impl Histogram {
    fn new() -> Self {
        Self {
            bins: vec![0; 60_001],
            count: 0,
            sum_us: 0,
            max_us: 0,
        }
    }
    fn add(&mut self, d: Duration) {
        let us = d.as_micros().min(u128::from(u64::MAX)) as u64;
        self.bins[(us / 1000).min(60_000) as usize] += 1;
        self.count += 1;
        self.sum_us += u128::from(us);
        self.max_us = self.max_us.max(us);
    }
    fn quantile(&self, p: u64) -> Option<u64> {
        let target = (self.count * p).div_ceil(100);
        let mut n = 0;
        for (i, c) in self.bins.iter().enumerate() {
            n += c;
            if n >= target {
                return (i < 60_000).then_some(i as u64 + 1);
            }
        }
        None
    }
    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "count": self.count,
            "mean_us": self.sum_us as f64/self.count.max(1) as f64,
            "p50_upper_ms": self.quantile(50),
            "p95_upper_ms": self.quantile(95),
            "p99_upper_ms": self.quantile(99),
            "max_us": self.max_us,
            "over_60s": self.bins[60_000]
        })
    }
}
struct Stats {
    queue: Histogram,
    modes: BTreeMap<&'static str, Histogram>,
    cache_max: usize,
}
impl Stats {
    fn new() -> Self {
        Self {
            queue: Histogram::new(),
            modes: MODES.into_iter().map(|n| (n, Histogram::new())).collect(),
            cache_max: 0,
        }
    }
}
fn setting(key: &str, default: u64, min: u64, max: u64) -> Result<u64> {
    let n = std::env::var(key).map_or(Ok(default), |v| v.parse())?;
    if !(min..=max).contains(&n) {
        return Err(format!("{key} must be {min}..={max}").into());
    }
    Ok(n)
}
async fn operation(c: &mut DorisConnection, mode: &str, value: i64, shape: u64) -> Result<()> {
    match mode {
        "point" | "cache_churn" => {
            for index in 0..if mode == "point" { 1 } else { 32 } {
                let sql = if mode == "point" {
                    "SELECT CAST(? AS BIGINT)".to_owned()
                } else {
                    format!(
                        "SELECT CAST(? AS BIGINT) /* sustained_{} */",
                        (shape + index) % 32
                    )
                };
                let got: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
                    .bind(value)
                    .fetch_one(&mut *c)
                    .await?;
                if got != value {
                    return Err("point-query value mismatch".into());
                }
            }
        }
        "large_field" => {
            let s: String = sqlx::query_scalar("SELECT REPEAT('x',65536)")
                .fetch_one(c)
                .await?;
            if s.len() != 65536 || !s.bytes().all(|b| b == b'x') {
                return Err("large-field value mismatch".into());
            }
        }
        "slow_stream" => {
            let mut s = sqlx::query(
                "SELECT number,REPEAT('x',4096) AS payload FROM numbers('number'='16')",
            )
            .fetch(c);
            let mut seen = [false; 16];
            while let Some(row) = s.try_next().await? {
                let n: i64 = row.try_get(0)?;
                let payload: &str = row.try_get(1)?;
                if !(0..16).contains(&n)
                    || seen[n as usize]
                    || payload.len() != 4096
                    || !payload.bytes().all(|b| b == b'x')
                {
                    return Err("slow-stream data mismatch".into());
                }
                seen[n as usize] = true;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            if !seen.into_iter().all(|v| v) {
                return Err("slow-stream row missing".into());
            }
        }
        "dropped_stream" | "consumer_timeout" => {
            let mut stream = sqlx::query(
                "SELECT number,REPEAT('x',32768) AS payload FROM numbers('number'='128')",
            )
            .fetch(c);
            let row = stream
                .try_next()
                .await?
                .ok_or("cancel-stream missing row")?;
            let n: i64 = row.try_get(0)?;
            let payload: &str = row.try_get(1)?;
            if !(0..128).contains(&n)
                || payload.len() != 32768
                || !payload.bytes().all(|b| b == b'x')
            {
                return Err("cancel-stream data mismatch".into());
            }
            drop(row);
            if mode == "consumer_timeout" {
                // The first row is already checked. Expiry drops a future that
                // owns an unread stream; this is a stalled-consumer deadline.
                let stalled = async move {
                    std::future::pending::<()>().await;
                    drop(stream);
                };
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), stalled)
                        .await
                        .is_err()
                );
            } else {
                drop(stream);
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}
async fn round(
    pool: &DorisPool,
    seconds: u64,
    workers: u64,
    cache: usize,
    seed: u64,
) -> Result<serde_json::Value> {
    let stats = Arc::new(Mutex::new(Stats::new()));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(seconds);
    let mut tasks = tokio::task::JoinSet::new();
    for worker in 0..workers {
        let pool = pool.clone();
        let stats = stats.clone();
        tasks.spawn(async move {
            let mut i = 0u64;
            let mut rng = seed ^ worker.wrapping_add(1);
            while Instant::now() < deadline {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let mode = MODES[((i + worker) % MODES.len() as u64) as usize];
                let t = Instant::now();
                let mut c = pool.acquire().await?;
                let queued = t.elapsed();
                tokio::time::timeout(
                    Duration::from_secs(15),
                    operation(&mut c, mode, rng as i64, (rng >> 32) % 32),
                )
                .await??;
                let cached = c.cached_statements_size();
                if cached > cache {
                    return Err("statement cache exceeded capacity".into());
                }
                if mode == "cache_churn" && cached != cache {
                    return Err("cache-churn workload did not fill the statement cache".into());
                }
                drop(c);
                let mut s = stats.lock().unwrap();
                s.queue.add(queued);
                s.modes.get_mut(mode).unwrap().add(t.elapsed());
                s.cache_max = s.cache_max.max(cached);
                i += 1;
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }
    while let Some(task) = tasks.join_next().await {
        task??;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let s = stats.lock().unwrap();
    let total = s.modes.values().map(|m| m.count).sum::<u64>();
    if s.modes.values().any(|m| m.count == 0) {
        return Err("mixed workload did not execute every mode".into());
    }
    Ok(serde_json::json!({
        "seconds": elapsed,
        "operations": total,
        "operations_per_second": total as f64/elapsed,
        "queue": s.queue.json(),
        "latencies": s.modes.iter().map(|(k,v)|(*k,v.json())).collect::<BTreeMap<_,
        _>>(),
        "observed_cache_max": s.cache_max,
        "pool_size": pool.size(),
        "pool_idle": pool.num_idle()
    }))
}
#[tokio::main]
async fn main() -> Result<()> {
    let seconds = setting("DORIS_SOAK_SECONDS", 60, 10, 3600)?;
    let rounds = setting("DORIS_SOAK_ROUNDS", 3, 1, 20)?;
    let workers = setting("DORIS_SOAK_WORKERS", 16, 2, 256)?;
    let pool_size = setting("DORIS_SOAK_POOL_SIZE", 4, 1, 64)?;
    let cache = setting("DORIS_SOAK_CACHE", 8, 1, 16)? as usize;
    let seed = setting("DORIS_RANDOM_SEED", 9417, 1, u64::MAX)?;
    if workers <= pool_size {
        return Err("DORIS_SOAK_WORKERS must exceed pool size to exercise queueing".into());
    }
    let url = std::env::var("DORIS_TEST_URL")?;
    let options: DorisConnectOptions = url.parse()?;
    let mut admin = options.connect().await?;
    let db = format!(
        "sdk_sustained_{}_{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
    );
    admin
        .execute(sqlx::AssertSqlSafe(format!("CREATE DATABASE `{db}`")))
        .await?;
    // Failures intentionally preserve the isolated database for inspection.
    let created = Arc::new(AtomicU64::new(0));
    let count = created.clone();
    let pool = DorisPoolOptions::new()
        .max_connections(pool_size as u32)
        .min_connections(pool_size as u32)
        .acquire_timeout(Duration::from_secs(10))
        .after_connect(move |_, _| {
            count.fetch_add(1, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        })
        .connect_with(options.database(&db).statement_cache_capacity(cache))
        .await?;
    println!(
        "{}",
        serde_json::json!({
            "event": "settings",
            "seconds": seconds,
            "rounds": rounds,
            "workers": workers,
            "pool_size": pool_size,
            "cache_capacity": cache,
            "query_shapes": 32,
            "queries_per_cache_churn_operation": 32,
            "seed": seed,
            "histogram_bin_ms": 1,
            "consumer_timeout_is_not_server_query_timeout": true
        })
    );
    for index in 0..rounds {
        let mut result = round(&pool, seconds, workers, cache, seed.wrapping_add(index)).await?;
        result["event"] = serde_json::json!("round");
        result["round"] = serde_json::json!(index);
        result["connections_created_cumulative"] =
            serde_json::json!(created.load(Ordering::Relaxed));
        println!("{result}");
    }
    let mut held = Vec::new();
    for _ in 0..pool_size {
        held.push(pool.acquire().await?);
    }
    let mut cached = 0;
    for c in &mut held {
        c.ping().await?;
        if c.cached_statements_size() > cache {
            return Err("final cache capacity exceeded".into());
        }
        cached += c.cached_statements_size();
    }
    drop(held);
    pool.close().await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let rows = admin.fetch_all("SHOW FULL PROCESSLIST").await?;
        let mut remaining = 0;
        for row in rows {
            if row.try_get::<Option<&str>, _>("Db")? == Some(db.as_str()) {
                remaining += 1;
            }
        }
        if remaining == 0 {
            break;
        }
        if Instant::now() >= deadline {
            return Err("isolated database sessions did not disappear after pool close".into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    admin
        .execute(sqlx::AssertSqlSafe(format!("DROP DATABASE `{db}`")))
        .await?;
    admin.close().await?;
    println!(
        "{}",
        serde_json::json!({
            "event": "verified",
            "healthy_connections_checked": pool_size,
            "cached_handles_before_close": cached,
            "sessions_after_close": 0,
            "connections_created_total": created.load(Ordering::Relaxed),
            "server_prepared_handle_count": "not_exposed_by_this_workload",
            "cleanup": "isolated_database_removed"
        })
    );
    Ok(())
}
