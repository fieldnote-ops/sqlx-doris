//! Real Kafka messages, admitted transactions and explicit offset continuation.
use super::*;
use std::io::Write;
#[path = "routine_matrix.rs"]
mod matrix;

#[path = "kafka_tool.rs"]
mod kafka_tool;
use kafka_tool::run as kafka;

async fn topic() -> String {
    let name = format!("sdk_rl_data_{}", uuid::Uuid::new_v4().simple());
    kafka(
        "kafka-topics",
        vec![
            "--create".into(),
            "--topic".into(),
            name.clone(),
            "--partitions".into(),
            "1".into(),
            "--replication-factor".into(),
            "1".into(),
        ],
        None,
    )
    .await;
    name
}
async fn end(topic: &str) -> i64 {
    let output = kafka(
        "kafka-get-offsets",
        vec!["--topic".into(), topic.into(), "--time".into(), "-1".into()],
        None,
    )
    .await;
    let prefix = format!("{topic}:0:");
    let values: Vec<_> = output
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .collect();
    assert_eq!(values.len(), 1, "{output}");
    values[0].trim().parse().unwrap()
}
async fn produce(topic: &str, rows: &[&str]) -> i64 {
    let before = end(topic).await;
    kafka(
        "kafka-console-producer",
        vec![
            "--topic".into(),
            topic.into(),
            "--sync".into(),
            "--producer-property".into(),
            "acks=all".into(),
        ],
        Some(format!("{}\n", rows.join("\n"))),
    )
    .await;
    let after = end(topic).await;
    assert_eq!(
        after - before,
        rows.len() as i64,
        "all records acknowledged by actual broker"
    );
    after
}
async fn delete(topic: &str) {
    assert!(topic.starts_with("sdk_rl_data_"));
    kafka(
        "kafka-topics",
        vec!["--delete".into(), "--topic".into(), topic.into()],
        None,
    )
    .await;
}
async fn duplicate(c: &mut DorisConnection) {
    c.execute("DROP TABLE events").await.unwrap();
    c.execute("CREATE TABLE events(id BIGINT NOT NULL,value STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')").await.unwrap();
}
async fn create_data(
    c: &mut DorisConnection,
    url: &str,
    account: &Account,
    topic: &str,
    offset: i64,
    slow: bool,
) -> (String, String, PathBuf) {
    assert!(offset >= 0);
    let brokers = std::env::var("DORIS_KAFKA_BROKERS").unwrap();
    let columns = if slow {
        "COLUMNS(id,raw,value=IF(SLEEP(IF(id=2,12,0))=0,raw,raw))"
    } else {
        "COLUMNS(id,value)"
    };
    let body = format!(
        "COLUMNS TERMINATED BY ',', {columns} PROPERTIES('desired_concurrent_number'='1','max_batch_interval'='5','max_error_number'='0') FROM KAFKA('kafka_broker_list'='{brokers}','kafka_topic'='{topic}','kafka_partitions'='0','kafka_offsets'='{offset}')"
    );
    create_source(c, url, account, "events", &body).await
}
async fn create_source(
    c: &mut DorisConnection,
    url: &str,
    account: &Account,
    table: &str,
    body: &str,
) -> (String, String, PathBuf) {
    let input = source();
    let path = input.with_extension("json");
    std::fs::write(&input, body).unwrap();
    let value = recovery::json_call(
        url,
        &[
            "admin",
            "routine",
            "create",
            "--runner-identity-file",
            account.path.to_str().unwrap(),
            "--table",
            table,
            "--source",
            input.to_str().unwrap(),
            "--output",
            path.to_str().unwrap(),
            "--managed-namespace",
        ],
        0,
    )
    .await;
    let name = value["job_name"].as_str().unwrap().to_owned();
    let id = value["job"]["id"].as_u64().unwrap().to_string();
    assert_eq!(
        c.fetch_all(sqlx::AssertSqlSafe(format!(
            "SHOW ALL ROUTINE LOAD FOR {name}"
        )))
        .await
        .unwrap()[0]
            .get::<&str, _>("User"),
        account.name
    );
    (name, id, path)
}
async fn count(c: &mut DorisConnection) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(c)
        .await
        .unwrap()
}
async fn rows(c: &mut DorisConnection, n: i64) {
    let mut last = -1;
    let result = tokio::time::timeout(Duration::from_secs(65), async {
        loop {
            let actual = count(c).await;
            last = actual;
            if actual == n {
                break;
            }
            assert!(actual < n, "unexpected replay: {actual} > {n}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    if result.is_err() {
        let states: Vec<_> = c
            .fetch_all("SHOW ALL ROUTINE LOAD")
            .await
            .unwrap()
            .iter()
            .map(|r| {
                (
                    r.get::<String, _>("Name"),
                    r.get::<String, _>("State"),
                    r.get::<String, _>("Progress"),
                    r.get::<String, _>("OtherMsg"),
                )
            })
            .collect();
        panic!("real Kafka data not visible: expected {n}, last {last}; jobs {states:?}");
    }
}
async fn preparing(c: &mut DorisConnection, url: &str, job: &str, name: &str) -> String {
    preparing_partition(c, url, job, name, None).await
}
async fn preparing_partition(
    c: &mut DorisConnection,
    url: &str,
    job: &str,
    name: &str,
    partition: Option<u32>,
) -> String {
    let database = url
        .parse::<DorisConnectOptions>()
        .unwrap()
        .get_database()
        .unwrap()
        .to_owned();
    let id = c
        .fetch_all("SHOW PROC '/dbs'")
        .await
        .unwrap()
        .iter()
        .find(|r| r.get::<&str, _>("DbName") == database)
        .unwrap()
        .get::<String, _>("DbId");
    tokio::time::timeout(Duration::from_secs(30), async {
        'poll: loop {
            let rows = c
                .fetch_all(sqlx::AssertSqlSafe(format!(
                    "SHOW PROC '/transactions/{id}/running'"
                )))
                .await
                .unwrap();
            for row in rows.iter().filter(|r| {
                r.get::<&str, _>("ListenerId") == job
                    && r.get::<&str, _>("TransactionStatus") == "PREPARE"
            }) {
                let transaction = row.get::<String, _>("TransactionId");
                // FE begins PREPARE before submitting to BE. On resource refusal
                // Doris even fills ExecuteStartTime but resets BeId to -1.
                // Require the same task/txn after successful BE submission ACK.
                let tasks = c
                    .fetch_all(sqlx::AssertSqlSafe(format!(
                        "SHOW ROUTINE LOAD TASK WHERE JobName='{name}'"
                    )))
                    .await
                    .unwrap();
                if tasks.iter().any(|t| {
                    t.get::<&str, _>("JobId") == job
                        && t.get::<&str, _>("TxnId") == transaction
                        && t.get::<&str, _>("TxnStatus") == "PREPARE"
                        && t.get::<&str, _>("BeId").parse::<i64>().is_ok_and(|v| v > 0)
                        && t.get::<Option<&str>, _>("ExecuteStartTime")
                            .is_some_and(|start| {
                                chrono::NaiveDateTime::parse_from_str(start, "%Y-%m-%d %H:%M:%S")
                                    .is_ok()
                            })
                        && partition.is_none_or(|p| {
                            let offsets: serde_json::Value =
                                serde_json::from_str(t.get("DataSourceProperties")).unwrap();
                            offsets
                                .as_object()
                                .is_some_and(|v| v.len() == 1 && v.contains_key(&p.to_string()))
                        })
                }) {
                    break 'poll transaction;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("no BE-acknowledged PREPARE Routine task observed; FE-only PREPARE is insufficient")
}
async fn terminal_tx(c: &mut DorisConnection, id: &str) -> String {
    tokio::time::timeout(Duration::from_secs(65), async {
        loop {
            let rows = c
                .fetch_all(sqlx::AssertSqlSafe(format!(
                    "SHOW TRANSACTION WHERE id={id}"
                )))
                .await
                .unwrap();
            assert_eq!(rows.len(), 1);
            let state = rows[0].get::<String, _>("TransactionStatus");
            if matches!(state.as_str(), "VISIBLE" | "ABORTED") {
                break state;
            }
            assert!(matches!(
                state.as_str(),
                "PREPARE" | "PRECOMMITTED" | "COMMITTED"
            ));
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("actual transaction terminal state not observed")
}
async fn stopped(
    url: &str,
    account: &Account,
    path: &std::path::Path,
    id: &str,
) -> serde_json::Value {
    retire(url, account).await;
    recovery::json_call(
        url,
        &[
            "admin",
            "routine",
            "stop",
            "--receipt",
            path.to_str().unwrap(),
            "--job-id",
            id,
            "--maintenance",
            "--managed-namespace",
            "--force",
        ],
        0,
    )
    .await
}
async fn drained(url: &str, account: &Account) {
    call(
        url,
        &[
            "admin",
            "drain",
            "wait",
            "--name",
            &account.name,
            "--maintenance",
            "--timeout",
            "65",
        ],
        true,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires native Doris and dedicated externally stored Kafka tools"]
async fn kafka_data_numeric_progress_plus_one_resumes_without_duplicates_and_reusing_offset_replays()
 {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = topic().await;
    assert_eq!(produce(&topic, &["1,first", "2,second"]).await, 2);
    let (old, id, receipt) = create_data(&mut c, &url, &account, &topic, 0, false).await;
    rows(&mut c, 2).await;
    let terminal = stopped(&url, &account, &receipt, &id).await;
    drained(&url, &account).await;
    assert_eq!(terminal["job"]["progress"]["0"], "1");
    assert_eq!(produce(&topic, &["3,third", "4,fourth"]).await, 4);
    assert_eq!(count(&mut c).await, 2);
    let next_account = create(&url).await;
    let next_offset = terminal["job"]["progress"]["0"]
        .as_str()
        .unwrap()
        .parse::<i64>()
        .unwrap()
        + 1;
    let (next, next_id, next_receipt) =
        create_data(&mut c, &url, &next_account, &topic, next_offset, false).await;
    assert_ne!(old, next);
    rows(&mut c, 4).await;
    let next_terminal = stopped(&url, &next_account, &next_receipt, &next_id).await;
    drained(&url, &next_account).await;
    assert_eq!(next_terminal["job"]["progress"]["0"], "3");
    let actual: Vec<(i64, String)> = sqlx::query_as("SELECT id,value FROM events ORDER BY id")
        .fetch_all(&mut c)
        .await
        .unwrap();
    assert_eq!(
        actual,
        vec![
            (1, "first".into()),
            (2, "second".into()),
            (3, "third".into()),
            (4, "fourth".into())
        ]
    );
    // Wrong continuation is demonstrated on DUPLICATE KEY: UUID uniqueness
    // cannot suppress records intentionally consumed from an old offset.
    let replay_account = create(&url).await;
    let (_, replay_id, replay_receipt) =
        create_data(&mut c, &url, &replay_account, &topic, 1, false).await;
    rows(&mut c, 7).await;
    stopped(&url, &replay_account, &replay_receipt, &replay_id).await;
    drained(&url, &replay_account).await;
    let counts: Vec<(i64, i64)> =
        sqlx::query_as("SELECT id,COUNT(*) FROM events GROUP BY id ORDER BY id")
            .fetch_all(&mut c)
            .await
            .unwrap();
    assert_eq!(counts, vec![(1, 1), (2, 2), (3, 2), (4, 2)]);
    std::fs::remove_file(next_account.path).unwrap();
    std::fs::remove_file(replay_account.path).unwrap();
    delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires native Doris and dedicated externally stored Kafka tools"]
async fn kafka_data_stop_before_commit_aborts_an_admitted_batch_without_writing_it() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = topic().await;
    produce(&topic, &["2,uncommitted"]).await;
    let (name, id, receipt) = create_data(&mut c, &url, &account, &topic, 0, true).await;
    let txn = preparing(&mut c, &url, &id, &name).await;
    assert_eq!(count(&mut c).await, 0);
    let terminal = stopped(&url, &account, &receipt, &id).await;
    assert_eq!(terminal["job"]["state"], "STOPPED");
    assert_eq!(terminal_tx(&mut c, &txn).await, "ABORTED");
    drained(&url, &account).await;
    assert_eq!(count(&mut c).await, 0);
    assert_ne!(terminal["job"]["progress"]["0"], "0");
    delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires native Doris and dedicated externally stored Kafka tools"]
async fn kafka_data_commit_can_win_after_the_cli_final_read_and_before_stop_delivery() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = topic().await;
    produce(&topic, &["1,committed-first"]).await;
    let (name, id, receipt) = create_data(&mut c, &url, &account, &topic, 0, true).await;
    rows(&mut c, 1).await;
    produce(&topic, &["2,commit-wins"]).await;
    let txn = preparing(&mut c, &url, &id, &name).await;
    retire(&url, &account).await;
    let mut proxy = Proxy::start_routine(&opts, Point::BeforeDrop).await;
    let (child, out, err) = spawn(
        proxy.options.to_url_lossy().as_str(),
        &[
            "admin",
            "routine",
            "stop",
            "--receipt",
            receipt.to_str().unwrap(),
            "--job-id",
            &id,
            "--maintenance",
            "--managed-namespace",
            "--force",
            "--json",
        ],
    );
    proxy.reached().await;
    assert_eq!(count(&mut c).await, 1);
    rows(&mut c, 2).await;
    assert_eq!(terminal_tx(&mut c, &txn).await, "VISIBLE");
    proxy.release(Release::Forward);
    exited(child, &err, true).await;
    assert!(std::fs::read_to_string(err).unwrap().is_empty());
    let terminal: serde_json::Value = serde_json::from_slice(&std::fs::read(out).unwrap()).unwrap();
    assert_eq!(terminal["job"]["state"], "STOPPED");
    assert_eq!(terminal["job"]["progress"]["0"], "1");
    assert_eq!(proxy.trace.lock().unwrap().drop_requests, 1);
    assert_eq!(proxy.trace.lock().unwrap().drop_ok, 1);
    proxy.finish().await;
    drained(&url, &account).await;
    assert_eq!(state(&mut c, &name).await, "STOPPED");
    assert_eq!(count(&mut c).await, 2);
    delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}
