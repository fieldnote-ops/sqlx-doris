//! Per-partition data oracles and public Kafka metadata/faults, never simulated jobs.
use super::*;
use std::collections::BTreeMap;
#[path = "routine_matrix/kafka.rs"]
mod broker;

fn body(
    topic: &str,
    offsets: Option<&[(u32, i64)]>,
    default: Option<&str>,
    concurrency: u16,
    slow: bool,
) -> String {
    let brokers = std::env::var("DORIS_KAFKA_BROKERS").unwrap();
    let columns = if slow {
        "COLUMNS(id,raw,value=IF(SLEEP(IF(id=2,24,0))=0,raw,raw))"
    } else {
        "COLUMNS(id,value)"
    };
    let mut props = format!("'kafka_broker_list'='{brokers}','kafka_topic'='{topic}'");
    if let Some(offsets) = offsets {
        assert!(!offsets.is_empty());
        assert!(offsets.iter().all(|(_, o)| *o >= 0));
        props += &format!(
            ",'kafka_partitions'='{}','kafka_offsets'='{}'",
            offsets
                .iter()
                .map(|(p, _)| p.to_string())
                .collect::<Vec<_>>()
                .join(","),
            offsets
                .iter()
                .map(|(_, o)| o.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
    }
    if let Some(default) = default {
        assert!(matches!(default, "OFFSET_BEGINNING" | "OFFSET_END"));
        props += &format!(",'property.kafka_default_offsets'='{default}'");
    }
    format!(
        "COLUMNS TERMINATED BY ',', {columns} PROPERTIES('desired_concurrent_number'='{concurrency}','max_batch_interval'='5','max_error_number'='0') FROM KAFKA({props})"
    )
}
async fn start(
    c: &mut DorisConnection,
    url: &str,
    account: &Account,
    topic: &str,
    offsets: &[(u32, i64)],
    table: &str,
    slow: bool,
) -> (String, String, PathBuf) {
    create_source(
        c,
        url,
        account,
        table,
        &body(topic, Some(offsets), None, 2, slow),
    )
    .await
}
async fn status(url: &str, path: &std::path::Path) -> serde_json::Value {
    recovery::json_call(
        url,
        &[
            "admin",
            "routine",
            "status",
            "--receipt",
            path.to_str().unwrap(),
        ],
        0,
    )
    .await
}
fn next(value: &serde_json::Value) -> Vec<(u32, i64)> {
    value["job"]["progress"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(p, v)| {
            (
                p.parse().unwrap(),
                v.as_str()
                    .unwrap()
                    .parse::<i64>()
                    .expect("explicit numeric cursor required")
                    .checked_add(1)
                    .unwrap(),
            )
        })
        .collect()
}
async fn table(c: &mut DorisConnection, name: &str) {
    assert!(name.bytes().all(|v| v.is_ascii_lowercase() || v == b'_'));
    c.execute(sqlx::AssertSqlSafe(format!("CREATE TABLE {name}(id BIGINT NOT NULL,value STRING) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES('replication_num'='1')"))).await.unwrap();
}
async fn actual(c: &mut DorisConnection, table: &str) -> BTreeMap<i64, i64> {
    assert!(table.bytes().all(|v| v.is_ascii_lowercase() || v == b'_'));
    let rows: Vec<(i64, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id,COUNT(*) FROM {table} GROUP BY id ORDER BY id"
    )))
    .fetch_all(c)
    .await
    .unwrap();
    rows.into_iter().collect()
}
async fn exact(c: &mut DorisConnection, table: &str, expected: &[i64]) {
    let expected: BTreeMap<_, _> = expected.iter().map(|id| (*id, 1)).collect();
    tokio::time::timeout(Duration::from_secs(65), async {
        loop {
            let found = actual(c, table).await;
            if found == expected {
                break;
            }
            assert!(
                found
                    .iter()
                    .all(|(k, n)| expected.contains_key(k) && *n == 1),
                "unexpected rows/replay in {table}: {found:?}, expected {expected:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|e| {
        panic!("exact Kafka IDs not visible in {table}: {e}; expected {expected:?}")
    });
}
async fn settled(c: &mut DorisConnection, table: &str, expected: &[i64]) {
    let expected: BTreeMap<_, _> = expected.iter().map(|id| (*id, 1)).collect();
    assert_eq!(
        actual(c, table).await,
        expected,
        "post-STOP/drain multiplicities in {table}"
    );
}
fn record(label: &str, value: serde_json::Value) {
    let path = PathBuf::from(std::env::var("SQLX_DORIS_ARTIFACTS_DIR").unwrap()).join(format!(
        "kafka-matrix-{label}-{}.json",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    println!("matrix-evidence {label} {value}");
}
async fn finish_account(url: &str, account: Account, path: &std::path::Path, id: &str) {
    stopped(url, &account, path, id).await;
    drained(url, &account).await;
    std::fs::remove_file(account.path).unwrap();
}
async fn progress_has(
    c: &mut DorisConnection,
    name: &str,
    partitions: &[&str],
) -> serde_json::Value {
    let mut last = serde_json::Value::Null;
    let result = tokio::time::timeout(Duration::from_secs(130), async {
        loop {
            let r = c
                .fetch_all(sqlx::AssertSqlSafe(format!(
                    "SHOW ALL ROUTINE LOAD FOR {name}"
                )))
                .await
                .unwrap();
            assert_eq!(r.len(), 1);
            let state = r[0].get::<&str, _>("State");
            last = serde_json::json!({
                "state": state,
                "progress": r[0].get::<&str,
                _>("Progress"),
                "reason": r[0].get::<&str,
                _>("ReasonOfStateChanged"),
                "other": r[0].get::<&str,
                _>("OtherMsg")
            });
            assert!(
                matches!(state, "RUNNING" | "NEED_SCHEDULE" | "PAUSED"),
                "{state}: {}",
                r[0].get::<&str, _>("OtherMsg")
            );
            let value: serde_json::Value = serde_json::from_str(r[0].get("Progress")).unwrap();
            if state != "PAUSED" && partitions.iter().all(|p| value.get(p).is_some()) {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    result.unwrap_or_else(|e| panic!("new partitions not discovered: {e}; last {last}"))
}

#[tokio::test]
#[ignore = "requires current Doris, dedicated Kafka and owned fault-broker manifest"]
async fn kafka_matrix_cursor_before_any_commit_is_not_consumption_proof() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = broker::topic(2, None).await;
    broker::send(&topic, &[(0, 1), (0, 2), (0, 3), (1, 101)]).await;
    let (_, id, path) = start(
        &mut c,
        &url,
        &account,
        &topic,
        &[(0, 3), (1, 1)],
        "events",
        false,
    )
    .await;
    let initial = status(&url, &path).await;
    assert_eq!(
        initial["job"]["progress"],
        serde_json::json!({
            "0": "2",
            "1": "0"
        })
    );
    assert_eq!(initial["job"]["progress_proves_consumption"], false);
    assert_eq!(initial["job"]["kafka_continuation_verified"], false);
    assert_eq!(count(&mut c).await, 0);
    let terminal = stopped(&url, &account, &path, &id).await;
    drained(&url, &account).await;
    assert_eq!(count(&mut c).await, 0);
    record(
        "initial-cursor",
        serde_json::json!({
            "initial": initial,
            "terminal": terminal,
            "rows": 0
        }),
    );
    broker::delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires current Doris, dedicated Kafka and owned fault-broker manifest"]
async fn kafka_matrix_mixed_partition_commit_stop_uses_each_cursor_not_a_global_max() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = broker::topic(2, None).await;
    broker::send(&topic, &[(0, 1), (1, 101), (1, 102)]).await;
    let (name, id, path) = start(
        &mut c,
        &url,
        &account,
        &topic,
        &[(0, 0), (1, 0)],
        "events",
        true,
    )
    .await;
    rows(&mut c, 3).await;
    broker::send(&topic, &[(0, 2), (1, 103)]).await;
    rows(&mut c, 4).await;
    let transaction = preparing_partition(&mut c, &url, &id, &name, Some(0)).await;
    let terminal = stopped(&url, &account, &path, &id).await;
    assert_eq!(terminal_tx(&mut c, &transaction).await, "ABORTED");
    drained(&url, &account).await;
    assert_eq!(
        terminal["job"]["progress"],
        serde_json::json!({
            "0": "0",
            "1": "2"
        })
    );
    exact(&mut c, "events", &[1, 101, 102, 103]).await;
    let broker_after = broker::send(&topic, &[(0, 3), (1, 104)]).await;
    // A plausible scalar/global-max checkpoint loses two partition-0 records.
    table(&mut c, "wrong_events").await;
    let wrong = create(&url).await;
    let (_, wrong_id, wrong_path) = start(
        &mut c,
        &url,
        &wrong,
        &topic,
        &[(0, 3), (1, 3)],
        "wrong_events",
        false,
    )
    .await;
    exact(&mut c, "wrong_events", &[104]).await;
    finish_account(&url, wrong, &wrong_path, &wrong_id).await;
    settled(&mut c, "wrong_events", &[104]).await;
    let successor = create(&url).await;
    let mut resume = next(&terminal);
    resume.reverse();
    let (_, next_id, next_path) =
        start(&mut c, &url, &successor, &topic, &resume, "events", false).await;
    exact(&mut c, "events", &[1, 2, 3, 101, 102, 103, 104]).await;
    finish_account(&url, successor, &next_path, &next_id).await;
    settled(&mut c, "events", &[1, 2, 3, 101, 102, 103, 104]).await;
    record(
        "mixed-stop-wins",
        serde_json::json!({
            "terminal": terminal,
            "transaction": transaction,
            "broker": broker_after,
            "resume": resume,
            "actual": actual(&mut c, "events").await,
            "wrong_global_max": actual(&mut c, "wrong_events").await
        }),
    );
    broker::delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires current Doris, dedicated Kafka and owned fault-broker manifest"]
async fn kafka_matrix_commit_after_final_stop_read_requires_the_terminal_per_partition_snapshot() {
    let (mut c, url, opts) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = broker::topic(2, None).await;
    broker::send(&topic, &[(0, 1), (1, 101), (1, 102)]).await;
    let (name, id, path) = start(
        &mut c,
        &url,
        &account,
        &topic,
        &[(0, 0), (1, 0)],
        "events",
        true,
    )
    .await;
    rows(&mut c, 3).await;
    broker::send(&topic, &[(0, 2), (1, 103)]).await;
    rows(&mut c, 4).await;
    let transaction = preparing_partition(&mut c, &url, &id, &name, Some(0)).await;
    let before = status(&url, &path).await;
    assert_eq!(
        before["job"]["progress"],
        serde_json::json!({
            "0": "0",
            "1": "2"
        })
    );
    retire(&url, &account).await;
    let mut proxy = Proxy::start_routine(&opts, Point::BeforeDrop).await;
    let (child, out, err) = spawn(
        proxy.options.to_url_lossy().as_str(),
        &[
            "admin",
            "routine",
            "stop",
            "--receipt",
            path.to_str().unwrap(),
            "--job-id",
            &id,
            "--maintenance",
            "--managed-namespace",
            "--force",
            "--json",
        ],
    );
    proxy.reached().await;
    rows(&mut c, 5).await;
    assert_eq!(terminal_tx(&mut c, &transaction).await, "VISIBLE");
    proxy.release(Release::Forward);
    exited(child, &err, true).await;
    let terminal: serde_json::Value = serde_json::from_slice(&std::fs::read(out).unwrap()).unwrap();
    assert_eq!(
        terminal["job"]["progress"],
        serde_json::json!({
            "0": "1",
            "1": "2"
        })
    );
    assert_eq!(proxy.trace.lock().unwrap().drop_forwarded, 1);
    proxy.finish().await;
    drained(&url, &account).await;
    broker::send(&topic, &[(0, 3), (1, 104)]).await;
    // Reusing the pre-STOP snapshot would replay already committed id=2.
    table(&mut c, "stale_events").await;
    let stale = create(&url).await;
    let (_, stale_id, stale_path) = start(
        &mut c,
        &url,
        &stale,
        &topic,
        &next(&before),
        "stale_events",
        false,
    )
    .await;
    exact(&mut c, "stale_events", &[2, 3, 104]).await;
    finish_account(&url, stale, &stale_path, &stale_id).await;
    settled(&mut c, "stale_events", &[2, 3, 104]).await;
    let successor = create(&url).await;
    let (_, next_id, next_path) = start(
        &mut c,
        &url,
        &successor,
        &topic,
        &next(&terminal),
        "events",
        false,
    )
    .await;
    exact(&mut c, "events", &[1, 2, 3, 101, 102, 103, 104]).await;
    finish_account(&url, successor, &next_path, &next_id).await;
    settled(&mut c, "events", &[1, 2, 3, 101, 102, 103, 104]).await;
    record(
        "commit-wins",
        serde_json::json!({
            "before": before,
            "terminal": terminal,
            "transaction": transaction,
            "actual": actual(&mut c, "events").await,
            "stale_checkpoint_replays": actual(&mut c, "stale_events").await
        }),
    );
    broker::delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires current Doris, dedicated Kafka and owned fault-broker manifest"]
async fn kafka_matrix_partition_expansion_needs_an_explicit_new_partition_origin() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = broker::topic(2, None).await;
    broker::send(&topic, &[(0, 1), (1, 101)]).await;
    let (_, id, path) = start(
        &mut c,
        &url,
        &account,
        &topic,
        &[(0, 0), (1, 0)],
        "events",
        false,
    )
    .await;
    rows(&mut c, 2).await;
    broker::grow(&topic, 3).await;
    broker::send(&topic, &[(2, 201), (2, 202), (0, 2)]).await;
    rows(&mut c, 3).await;
    let terminal = stopped(&url, &account, &path, &id).await;
    drained(&url, &account).await;
    assert!(terminal["job"]["progress"].get("2").is_none());
    assert_eq!(
        actual(&mut c, "events")
            .await
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![1, 2, 101]
    );
    let snapshot = broker::snapshot(&topic).await;
    let mut resume = next(&terminal);
    resume.push((2, snapshot.begin[&2]));
    let successor = create(&url).await;
    let (_, next_id, next_path) =
        start(&mut c, &url, &successor, &topic, &resume, "events", false).await;
    exact(&mut c, "events", &[1, 2, 101, 201, 202]).await;
    finish_account(&url, successor, &next_path, &next_id).await;
    settled(&mut c, "events", &[1, 2, 101, 201, 202]).await;
    record(
        "explicit-expansion",
        serde_json::json!({
            "terminal": terminal,
            "broker": snapshot,
            "explicit_new_partition_origin": resume,
            "actual": actual(&mut c, "events").await
        }),
    );
    broker::delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires current Doris, dedicated Kafka and owned fault-broker manifest"]
async fn kafka_matrix_dynamic_partition_discovery_beginning_vs_default_end() {
    for beginning in [true, false] {
        let (mut c, url, _) = fixture().await;
        let (account, owner) = setup(&mut c, &url).await;
        duplicate(&mut c).await;
        let topic = broker::topic(2, None).await;
        let default = if beginning {
            Some("OFFSET_BEGINNING")
        } else {
            None
        };
        let (name, id, path) = create_source(
            &mut c,
            &url,
            &account,
            "events",
            &body(&topic, None, default, 2, false),
        )
        .await;
        progress_has(&mut c, &name, &["0", "1"]).await;
        broker::send(&topic, &[(0, 1), (1, 101)]).await;
        rows(&mut c, 2).await;
        // Pause this same live creator/job, not a stopped generation or handoff.
        // This holds discovery until the new partition already contains data.
        c.execute(sqlx::AssertSqlSafe(format!(
            "PAUSE ROUTINE LOAD FOR {name}"
        )))
        .await
        .unwrap();
        broker::grow(&topic, 3).await;
        broker::send(&topic, &[(2, 201), (2, 202)]).await;
        c.execute(sqlx::AssertSqlSafe(format!(
            "RESUME ROUTINE LOAD FOR {name}"
        )))
        .await
        .unwrap();
        let discovered = progress_has(&mut c, &name, &["0", "1", "2"]).await;
        broker::send(&topic, &[(2, 203)]).await;
        let expected = if beginning {
            vec![1, 101, 201, 202, 203]
        } else {
            vec![1, 101, 203]
        };
        exact(&mut c, "events", &expected).await;
        let terminal = stopped(&url, &account, &path, &id).await;
        drained(&url, &account).await;
        settled(&mut c, "events", &expected).await;
        record(
            if beginning {
                "dynamic-beginning"
            } else {
                "dynamic-default-end"
            },
            serde_json::json!({
                "discovered": discovered,
                "terminal": terminal,
                "actual": actual(&mut c, "events").await,
                "skipped_pre_discovery_ids": if beginning {
                    vec![]
                } else {
                    vec![201, 202]
                }
            }),
        );
        broker::delete(&topic).await;
        cleanup(c, &url, account, &owner).await;
    }
}

#[tokio::test]
#[ignore = "requires current Doris, dedicated Kafka and owned fault-broker manifest"]
async fn kafka_matrix_expired_prefix_empty_commit_is_not_lossless_resume_and_reset_has_a_gap() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = broker::topic(1, None).await;
    broker::send(&topic, &[(0, 1)]).await;
    let (_, id, path) = start(&mut c, &url, &account, &topic, &[(0, 0)], "events", false).await;
    rows(&mut c, 1).await;
    let terminal = stopped(&url, &account, &path, &id).await;
    drained(&url, &account).await;
    broker::send(&topic, &[(0, 2), (0, 3), (0, 4), (0, 5)]).await;
    broker::trim_prefix(&topic, 0, 3).await;
    let bounds = broker::snapshot(&topic).await;
    assert!(next(&terminal)[0].1 < bounds.begin[&0]);
    let invalid = create(&url).await;
    let (bad_name, bad_id, bad_path) = start(
        &mut c,
        &url,
        &invalid,
        &topic,
        &next(&terminal),
        "events",
        false,
    )
    .await;
    let native = tokio::time::timeout(Duration::from_secs(65), async {
        loop {
            let r = c
                .fetch_all(sqlx::AssertSqlSafe(format!(
                    "SHOW ALL ROUTINE LOAD FOR {bad_name}"
                )))
                .await
                .unwrap();
            assert_eq!(r.len(), 1);
            let stats: serde_json::Value = serde_json::from_str(r[0].get("Statistic")).unwrap();
            if stats["committedTaskNum"].as_u64().unwrap() > 0 {
                let progress: serde_json::Value =
                    serde_json::from_str(r[0].get("Progress")).unwrap();
                assert_eq!(stats["loadedRows"], 0);
                assert_eq!(progress["0"], "4");
                break serde_json::json!({
                    "state": r[0].get::<&str,
                    _>("State"),
                    "progress": progress,
                    "statistics": stats
                });
            }
            assert_ne!(r[0].get::<&str, _>("State"), "CANCELLED");
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("native expired-offset result not observed");
    let observed = status(&url, &bad_path).await;
    assert_eq!(observed["job"]["progress_proves_consumption"], false);
    assert_eq!(observed["job"]["kafka_continuation_verified"], false);
    assert_eq!(count(&mut c).await, 1);
    finish_account(&url, invalid, &bad_path, &bad_id).await;
    assert_eq!(count(&mut c).await, 1);
    let reset = create(&url).await;
    let (_, reset_id, reset_path) = start(
        &mut c,
        &url,
        &reset,
        &topic,
        &[(0, bounds.begin[&0])],
        "events",
        false,
    )
    .await;
    exact(&mut c, "events", &[1, 4, 5]).await;
    finish_account(&url, reset, &reset_path, &reset_id).await;
    settled(&mut c, "events", &[1, 4, 5]).await;
    record(
        "expired-prefix",
        serde_json::json!({
            "old": terminal,
            "broker": bounds,
            "native_empty_advance": native,
            "observed": observed,
            "actual": actual(&mut c, "events").await,
            "irrecoverable_from_retained_kafka": [2,3]
        }),
    );
    broker::delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires current Doris, dedicated Kafka and owned fault-broker manifest"]
async fn kafka_matrix_topic_recreation_invalidates_positions_even_when_ranges_fit() {
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = broker::topic(1, None).await;
    broker::send(&topic, &[(0, 1), (0, 2)]).await;
    let (_, id, path) = start(&mut c, &url, &account, &topic, &[(0, 0)], "events", false).await;
    rows(&mut c, 2).await;
    let terminal = stopped(&url, &account, &path, &id).await;
    drained(&url, &account).await;
    let before = broker::snapshot(&topic).await;
    broker::delete(&topic).await;
    broker::create(&topic, 1, None).await;
    let after = broker::send(&topic, &[(0, 1001), (0, 1002), (0, 1003)]).await;
    assert_eq!(before.cluster, after.cluster);
    assert_ne!(before.topic_id, after.topic_id);
    assert!(next(&terminal)[0].1 < after.end[&0]);
    table(&mut c, "wrong_events").await;
    let wrong = create(&url).await;
    let (_, wrong_id, wrong_path) = start(
        &mut c,
        &url,
        &wrong,
        &topic,
        &next(&terminal),
        "wrong_events",
        false,
    )
    .await;
    exact(&mut c, "wrong_events", &[1003]).await;
    finish_account(&url, wrong, &wrong_path, &wrong_id).await;
    settled(&mut c, "wrong_events", &[1003]).await;
    let reset = create(&url).await;
    let (_, reset_id, reset_path) =
        start(&mut c, &url, &reset, &topic, &[(0, 0)], "events", false).await;
    exact(&mut c, "events", &[1, 2, 1001, 1002, 1003]).await;
    finish_account(&url, reset, &reset_path, &reset_id).await;
    settled(&mut c, "events", &[1, 2, 1001, 1002, 1003]).await;
    record(
        "topic-generation",
        serde_json::json!({
            "old": terminal,
            "before": before,
            "after": after,
            "wrong_next": actual(&mut c, "wrong_events").await,
            "reconciled": actual(&mut c, "events").await
        }),
    );
    broker::delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}

#[tokio::test]
#[ignore = "requires current Doris, dedicated Kafka and owned fault-broker manifest"]
async fn kafka_matrix_z_unclean_election_really_truncates_tail_and_reuses_offsets() {
    let (leader, follower) = broker::node_ids();
    let (mut c, url, _) = fixture().await;
    let (account, owner) = setup(&mut c, &url).await;
    duplicate(&mut c).await;
    let topic = broker::topic(1, Some(&format!("{leader}:{follower}"))).await;
    broker::wait_replicas(&topic, leader, 2).await;
    broker::send(&topic, &[(0, 1), (0, 2), (0, 3)]).await;
    broker::control(1, false);
    broker::wait_replicas(&topic, leader, 1).await;
    broker::send(&topic, &[(0, 4), (0, 5)]).await;
    let (_, id, path) = start(&mut c, &url, &account, &topic, &[(0, 0)], "events", false).await;
    rows(&mut c, 5).await;
    let terminal = stopped(&url, &account, &path, &id).await;
    drained(&url, &account).await;
    let before = broker::snapshot(&topic).await;
    assert_eq!(before.end[&0], 5);
    broker::control(0, false);
    broker::control(1, true);
    // Public unclean election chooses the stale, deliberately stopped replica.
    broker::elect(&topic).await;
    broker::wait_replicas(&topic, follower, 1).await;
    let truncated = broker::snapshot(&topic).await;
    assert_eq!(before.cluster, truncated.cluster);
    assert_eq!(before.topic_id, truncated.topic_id);
    assert_eq!(truncated.end[&0], 3);
    assert!(next(&terminal)[0].1 > truncated.end[&0]);
    assert_eq!(count(&mut c).await, 5);
    let regrown = broker::send(&topic, &[(0, 103), (0, 104), (0, 105)]).await;
    assert_eq!(regrown.end[&0], 6);
    table(&mut c, "wrong_events").await;
    let wrong = create(&url).await;
    let (_, wrong_id, wrong_path) = start(
        &mut c,
        &url,
        &wrong,
        &topic,
        &next(&terminal),
        "wrong_events",
        false,
    )
    .await;
    exact(&mut c, "wrong_events", &[105]).await;
    finish_account(&url, wrong, &wrong_path, &wrong_id).await;
    settled(&mut c, "wrong_events", &[105]).await;
    table(&mut c, "earliest_events").await;
    let earliest = create(&url).await;
    let (_, earliest_id, earliest_path) = start(
        &mut c,
        &url,
        &earliest,
        &topic,
        &[(0, 0)],
        "earliest_events",
        false,
    )
    .await;
    exact(&mut c, "earliest_events", &[1, 2, 3, 103, 104, 105]).await;
    finish_account(&url, earliest, &earliest_path, &earliest_id).await;
    settled(&mut c, "earliest_events", &[1, 2, 3, 103, 104, 105]).await;
    let reconciled = create(&url).await;
    let (_, new_id, new_path) = start(
        &mut c,
        &url,
        &reconciled,
        &topic,
        &[(0, 3)],
        "events",
        false,
    )
    .await;
    exact(&mut c, "events", &[1, 2, 3, 4, 5, 103, 104, 105]).await;
    finish_account(&url, reconciled, &new_path, &new_id).await;
    settled(&mut c, "events", &[1, 2, 3, 4, 5, 103, 104, 105]).await;
    broker::control(0, true);
    broker::wait_replicas(&topic, follower, 2).await;
    record(
        "actual-tail-truncation",
        serde_json::json!({
            "old": terminal,
            "before": before,
            "truncated": truncated,
            "regrown": regrown,
            "wrong_next": actual(&mut c, "wrong_events").await,
            "earliest_replays_existing_ids": actual(&mut c, "earliest_events").await,
            "reconciled": actual(&mut c, "events").await
        }),
    );
    broker::delete(&topic).await;
    cleanup(c, &url, account, &owner).await;
}
