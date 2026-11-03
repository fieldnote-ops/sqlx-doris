//! Validated application workloads. Use scripts/benchmark-workloads for sampling.
use futures_util::TryStreamExt;
use serde::Serialize;
use serde_json::json;
use sqlx::{Executor, SqlSafeStr};
use sqlx_doris::{
    DorisConnectOptions, DorisPool, DorisPoolOptions, GroupCommit,
    operations::TableName,
    stream_load::{LoadFormat, LoadReceipt, LoadState, StreamLoadClient},
    types::Array,
};
use std::{
    env,
    error::Error,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

#[derive(Serialize)]
struct Event {
    id: i64,
    bucket: i32,
    value: i64,
    payload: String,
    items: [i64; 2],
}

struct Config {
    rows: usize,
    width: usize,
    rounds: usize,
    iterations: usize,
    seed: u64,
    phase: String,
    profile: String,
}

fn bounded(name: &str, default: usize, min: usize, max: usize) -> Result<usize> {
    let value = env::var(name).map_or(Ok(default), |value| value.parse())?;
    if !(min..=max).contains(&value) {
        return Err(format!("{name} must be within {min}..={max}").into());
    }
    Ok(value)
}

impl Config {
    fn read() -> Result<Self> {
        let config = Self {
            rows: bounded("DORIS_WORKLOAD_ROWS", 4096, 256, 100_000)?,
            width: bounded("DORIS_WORKLOAD_WIDTH", 1024, 64, 16_384)?,
            rounds: bounded("DORIS_WORKLOAD_ROUNDS", 3, 1, 20)?,
            iterations: bounded("DORIS_WORKLOAD_ITERATIONS", 25, 5, 1000)?,
            seed: env::var("DORIS_WORKLOAD_SEED").map_or(Ok(9417), |value| value.parse())?,
            phase: env::var("DORIS_WORKLOAD_PHASE")?,
            profile: env::var("DORIS_WORKLOAD_PROFILE")?,
        };
        if config.rows * config.width > 128 * 1024 * 1024 {
            return Err("workload payload must fit within 128 MiB".into());
        }
        if !matches!(config.phase.as_str(), "queries" | "ingestion") {
            return Err("DORIS_WORKLOAD_PHASE must be queries or ingestion".into());
        }
        Ok(config)
    }
}

fn rows(config: &Config) -> Vec<Event> {
    (0..config.rows)
        .map(|id| {
            let mut state =
                config.seed ^ (id as u64).wrapping_add(1).wrapping_mul(0x9e3779b97f4a7c15);
            let mut payload = format!("row:{id:08}:中文🙂:");
            while payload.len() < config.width {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                payload.push(char::from(b'a' + (state % 26) as u8));
            }
            Event {
                id: id as i64,
                bucket: (id % 16) as i32,
                value: ((id as u64 * 17 + config.seed % 1000) % 1000) as i64,
                payload,
                items: [id as i64, id as i64 + 1],
            }
        })
        .collect()
}

fn stage(config: &Config, name: &str, rows: usize, active: bool) {
    println!(
        "{}",
        json!({
            "event": if active { "begin" } else { "end" },
            "profile": config.profile,
            "workload": name,
            "rows": rows,
        })
    );
}

fn sample(config: &Config, name: &str, rows: usize, round: usize, iteration: usize, ms: f64) {
    println!(
        "{}",
        json!({
            "event": "sample",
            "profile": config.profile,
            "workload": name,
            "rows": rows,
            "input_rows": config.rows,
            "result_rows": if matches!(name, "aggregate" | "join") { 16 } else { rows },
            "result_payload_bytes": if matches!(name, "wide" | "slow_consumer") { Some(rows * config.width) } else { None },
            "round": round,
            "iteration": iteration,
            "elapsed_ms": ms,
        })
    );
}

fn millis(start: Instant) -> f64 {
    start.elapsed().as_secs_f64() * 1000.0
}

async fn create(pool: &DorisPool, table: &TableName) -> Result<()> {
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {table}(id BIGINT NOT NULL,bucket INT,value BIGINT,payload STRING,items ARRAY<BIGINT>) \
         DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 \
         PROPERTIES('replication_num'='1','group_commit_interval_ms'='1000')"
    ))).await?;
    Ok(())
}

async fn verify(pool: &DorisPool, table: &TableName, expected: &[Event]) -> Result<()> {
    let sql = sqlx::AssertSqlSafe(format!(
        "SELECT id,bucket,value,payload,items FROM {table} ORDER BY id"
    ))
    .into_sql_str();
    let mut stream = sqlx::query_as::<_, (i64, i32, i64, String, Array<i64>)>(sql).fetch(pool);
    let mut index = 0;
    while let Some(row) = stream.try_next().await? {
        let event = expected.get(index).ok_or("extra or duplicate event")?;
        if row.0 != event.id
            || row.1 != event.bucket
            || row.2 != event.value
            || row.3 != event.payload
            || row.4.0 != event.items
        {
            return Err(format!("event mismatch at index {index}").into());
        }
        index += 1;
    }
    if index != expected.len() {
        return Err(format!("missing events: expected {}, got {index}", expected.len()).into());
    }
    Ok(())
}

async fn wait_count(pool: &DorisPool, table: &TableName, expected: usize) -> Result<()> {
    sqlx_doris::operations::wait_until(
        Duration::from_secs(30),
        Duration::from_millis(20),
        || async {
            let count: i64 =
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!("SELECT COUNT(*) FROM {table}")))
                    .fetch_one(pool)
                    .await?;
            Ok(count == expected as i64)
        },
    )
    .await?;
    Ok(())
}

fn receipt(value: &LoadReceipt) -> Result<()> {
    if !matches!(
        value.state(),
        LoadState::Visible | LoadState::Committed | LoadState::Accepted
    ) || value.number_filtered_rows != 0
        || value.number_unselected_rows != 0
    {
        return Err(format!("load result is not usable: {value:?}").into());
    }
    Ok(())
}

async fn queries(
    config: &Config,
    pool: &DorisPool,
    http: &StreamLoadClient,
    data: &[Event],
) -> Result<()> {
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(pool)
        .await?;
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let fact = TableName::new(&database, format!("_sqlx_workload_fact_{nonce}"));
    let dimension = TableName::new(&database, format!("_sqlx_workload_dim_{nonce}"));
    println!(
        "{}",
        json!({
            "event": "fixture",
            "tables": [fact.to_string(), dimension.to_string()],
        })
    );
    create(pool, &fact).await?;
    pool.execute(sqlx::AssertSqlSafe(format!(
        "CREATE TABLE {dimension}(id INT NOT NULL,label STRING) DUPLICATE KEY(id) \
         DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"
    )))
    .await?;
    receipt(&http.load(&fact)?.send_json(data).await?)?;
    let dimensions = (0..16)
        .map(|id| {
            json!({
                "id": id,
                "label": format!("group_{id:02}"),
            })
        })
        .collect::<Vec<_>>();
    receipt(&http.load(&dimension)?.send_json(&dimensions).await?)?;
    wait_count(pool, &fact, data.len()).await?;
    wait_count(pool, &dimension, 16).await?;
    verify(pool, &fact, data).await?;

    let query_url = env::var("DORIS_BENCH_QUERY_URL")?;
    let query_pool = DorisPoolOptions::new()
        .max_connections(2)
        .min_connections(2)
        .connect(&query_url)
        .await?;
    let aggregate = sqlx::AssertSqlSafe(format!(
        "SELECT bucket,CAST(COUNT(*) AS BIGINT),CAST(SUM(value) AS BIGINT) FROM {fact} \
         WHERE id >= ? GROUP BY bucket ORDER BY bucket"
    ))
    .into_sql_str();
    let join = sqlx::AssertSqlSafe(format!(
        "SELECT d.label,CAST(COUNT(*) AS BIGINT),CAST(SUM(f.value) AS BIGINT) \
         FROM {fact} f JOIN {dimension} d ON f.bucket=d.id WHERE f.id >= ? \
         GROUP BY d.label ORDER BY d.label"
    ))
    .into_sql_str();
    let wide = sqlx::AssertSqlSafe(format!("SELECT id,payload FROM {fact} ORDER BY id LIMIT ?"))
        .into_sql_str();
    let complex = sqlx::AssertSqlSafe(format!("SELECT id,items FROM {fact} ORDER BY id LIMIT ?"))
        .into_sql_str();
    let mut counts = [0_i64; 16];
    let mut sums = [0_i64; 16];
    for row in data {
        counts[row.bucket as usize] += 1;
        sums[row.bucket as usize] += row.value;
    }
    let limits = if data.len() == 256 {
        vec![256]
    } else {
        vec![256, data.len()]
    };
    for round in 0..config.rounds {
        for name in ["aggregate", "join", "wide", "complex", "slow_consumer"] {
            let sizes = if matches!(name, "aggregate" | "join") {
                &limits[limits.len() - 1..]
            } else {
                &limits[..]
            };
            for &limit in sizes {
                stage(config, name, limit, true);
                for iteration in 0..config.iterations + 2 {
                    let start = Instant::now();
                    if name == "aggregate" {
                        let actual: Vec<(i32, i64, i64)> = sqlx::query_as(aggregate.clone())
                            .bind(0_i64)
                            .fetch_all(&query_pool)
                            .await?;
                        let expected = (0..16)
                            .map(|i| (i as i32, counts[i], sums[i]))
                            .collect::<Vec<_>>();
                        if actual != expected {
                            return Err("aggregate mismatch".into());
                        }
                    } else if name == "join" {
                        let actual: Vec<(String, i64, i64)> = sqlx::query_as(join.clone())
                            .bind(0_i64)
                            .fetch_all(&query_pool)
                            .await?;
                        let expected = (0..16)
                            .map(|i| (format!("group_{i:02}"), counts[i], sums[i]))
                            .collect::<Vec<_>>();
                        if actual != expected {
                            return Err("join mismatch".into());
                        }
                    } else {
                        let mut index = 0;
                        if name == "complex" {
                            let mut stream =
                                sqlx::query_as::<_, (i64, Array<i64>)>(complex.clone())
                                    .bind(limit as i64)
                                    .fetch(&query_pool);
                            while let Some((id, items)) = stream.try_next().await? {
                                let expected = data.get(index).ok_or("extra complex result")?;
                                if id != expected.id || items.0 != expected.items {
                                    return Err("complex result mismatch".into());
                                }
                                index += 1;
                            }
                        } else {
                            let mut stream = sqlx::query_as::<_, (i64, String)>(wide.clone())
                                .bind(limit as i64)
                                .fetch(&query_pool);
                            while let Some((id, payload)) = stream.try_next().await? {
                                let expected = data.get(index).ok_or("extra wide result")?;
                                if id != expected.id || payload != expected.payload {
                                    return Err("wide result mismatch".into());
                                }
                                index += 1;
                                if name == "slow_consumer" && index % 64 == 0 {
                                    tokio::time::sleep(Duration::from_millis(1)).await;
                                }
                            }
                        }
                        if index != limit {
                            return Err("result row count mismatch".into());
                        }
                    }
                    if iteration >= 2 {
                        sample(config, name, limit, round, iteration - 2, millis(start));
                    }
                }
                stage(config, name, limit, false);
            }
        }
    }
    query_pool.close().await;
    for table in [&dimension, &fact] {
        pool.execute(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
            .await?;
    }
    Ok(())
}

async fn ingestion(
    config: &Config,
    pool: &DorisPool,
    http: &StreamLoadClient,
    data: &[Event],
) -> Result<()> {
    let database: String = sqlx::query_scalar("SELECT DATABASE()")
        .fetch_one(pool)
        .await?;
    let directory = PathBuf::from(env::var("SQLX_DORIS_BUILD_ROOT")?)
        .join(format!("workload-input-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&directory)?;
    for round in 0..config.rounds {
        let mut modes = [
            "insert_batch",
            "load_json",
            "load_bytes",
            "load_file",
            "group_sync",
            "group_async",
        ];
        if round % 2 == 1 {
            modes.reverse();
        }
        for mode in modes {
            let table = TableName::new(
                &database,
                format!("_sqlx_workload_{}", uuid::Uuid::new_v4().simple()),
            );
            println!(
                "{}",
                json!({
                    "event": "fixture",
                    "tables": [table.to_string()],
                })
            );
            create(pool, &table).await?;
            stage(config, mode, data.len(), true);
            let encode_start = Instant::now();
            let encoded = serde_json::to_vec(data)?;
            let encoding_baseline_ms = millis(encode_start);
            let payload_bytes = encoded.len();
            let prepare_start = Instant::now();
            let path = directory.join(format!("{round}_{mode}.json"));
            if mode == "load_file" {
                use std::io::Write;
                let mut file = std::fs::File::create_new(&path)?;
                file.write_all(&encoded)?;
                file.sync_all()?;
            }
            let input_prepare_ms = millis(prepare_start);
            let bytes = if mode == "load_bytes" {
                Some(encoded)
            } else {
                drop(encoded);
                None
            };
            let start = Instant::now();
            let mut load = None;
            if mode == "insert_batch" {
                let mut connection = pool.acquire().await?;
                for batch in data.chunks(256) {
                    let mut query = sqlx::QueryBuilder::new(format!(
                        "INSERT INTO {table}(id,bucket,value,payload,items) "
                    ));
                    query.push_values(batch, |mut values, row| {
                        values
                            .push_bind(row.id)
                            .push_bind(row.bucket)
                            .push_bind(row.value)
                            .push_bind(&row.payload)
                            .push_bind(Array(row.items.to_vec()));
                    });
                    query.build().execute(&mut *connection).await?;
                }
            } else {
                let mut request = http.load(&table)?;
                if mode == "group_sync" {
                    request = request.group_commit(GroupCommit::Sync);
                } else if mode == "group_async" {
                    request = request.group_commit(GroupCommit::Async);
                } else {
                    request = request.label(format!("{}_batch", table.table));
                }
                let value = match mode {
                    "load_bytes" => {
                        request
                            .send_bytes(bytes.expect("prepared byte input"))
                            .await?
                    }
                    "load_file" => request.format(LoadFormat::Json).send_file(&path).await?,
                    _ => request.send_json(data).await?,
                };
                receipt(&value)?;
                let expected_mode = match mode {
                    "group_sync" => Some("sync_mode"),
                    "group_async" => Some("async_mode"),
                    _ => None,
                };
                if let Some(expected) = expected_mode
                    && (!value.group_commit || value.group_commit_mode.as_deref() != Some(expected))
                {
                    return Err(format!(
                        "requested Group Commit mode was not confirmed: {value:?}"
                    )
                    .into());
                }
                load = Some(value);
            }
            let submit_ack_ms = millis(start);
            wait_count(pool, &table, data.len()).await?;
            let visible_ms = millis(start);
            verify(pool, &table, data).await?;
            println!(
                "{}",
                json!({
                    "event": "ingestion_sample",
                    "profile": config.profile,
                    "workload": mode,
                    "rows": data.len(),
                    "round": round,
                    "payload_bytes": payload_bytes,
                    "encoding_baseline_ms": encoding_baseline_ms,
                    "input_prepare_ms": input_prepare_ms,
                    "submit_ack_ms": submit_ack_ms,
                    "visible_ms": visible_ms,
                    "submit_ack_includes_api_encoding": !matches!(mode, "load_bytes" | "load_file"),
                    "receipt": load,
                    "exact_data_verified": true,
                })
            );
            stage(config, mode, data.len(), false);
            pool.execute(sqlx::AssertSqlSafe(format!("DROP TABLE {table}")))
                .await?;
        }
    }
    // Only inputs from this successful run are removed. Failure retains them.
    std::fs::remove_dir_all(directory)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::read()?;
    let url = env::var("DORIS_TEST_URL")?;
    let options = DorisConnectOptions::from_str(&url)?.group_commit(GroupCommit::Off);
    let pool = DorisPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await?;
    let http = StreamLoadClient::new(
        &env::var("DORIS_HTTP_URL")?,
        env::var("DORIS_HTTP_USER").unwrap_or_else(|_| "root".into()),
        env::var("DORIS_HTTP_PASSWORD").ok(),
    )?
    .backend_endpoint(&env::var("DORIS_BE_HTTP_URL")?)?;
    let version: String = sqlx::query_scalar("SELECT VERSION()")
        .fetch_one(&pool)
        .await?;
    let version_comment: String = sqlx::query_scalar("SELECT @@version_comment")
        .fetch_one(&pool)
        .await?;
    println!(
        "{}",
        json!({
            "event": "configuration",
            "wire_compatibility_version": version,
            "server_version_comment": version_comment,
            "rows": config.rows,
            "width": config.width,
            "seed": config.seed,
            "rounds": config.rounds,
            "iterations": config.iterations,
            "phase": config.phase,
            "profile": config.profile,
            "warmup_per_query_group": 2,
        })
    );
    let data = rows(&config);
    let result = if config.phase == "queries" {
        queries(&config, &pool, &http, &data).await
    } else {
        ingestion(&config, &pool, &http, &data).await
    };
    pool.close().await;
    if let Err(error) = result {
        eprintln!(
            "Workload failed; named fixture tables and external evidence are retained. No write retry: {error}"
        );
        return Err(error);
    }
    Ok(())
}
