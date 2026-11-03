//! Public Kafka tools only. All mutations target this test's UUID topics/owned brokers.
use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct Partition {
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}
#[derive(Clone, Debug, serde::Serialize)]
pub(super) struct Snapshot {
    pub cluster: String,
    pub topic_id: String,
    pub partitions: BTreeMap<u32, Partition>,
    pub begin: BTreeMap<u32, i64>,
    pub end: BTreeMap<u32, i64>,
}
fn field<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let words: Vec<_> = line.split_whitespace().collect();
    words.windows(2).find(|p| p[0] == key).map(|p| p[1])
}
fn ids(raw: &str) -> Vec<i32> {
    raw.split(',').filter_map(|v| v.parse().ok()).collect()
}
fn owned(topic: &str) {
    let id = topic
        .strip_prefix("sdk_rl_matrix_")
        .expect("only own matrix topics");
    assert_eq!(uuid::Uuid::parse_str(id).unwrap().simple().to_string(), id);
}
pub(super) async fn metadata(topic: &str) -> (String, BTreeMap<u32, Partition>) {
    owned(topic);
    let text = kafka(
        "kafka-topics",
        vec!["--describe".into(), "--topic".into(), topic.into()],
        None,
    )
    .await;
    let topic_id = text
        .lines()
        .find_map(|l| field(l, "TopicId:"))
        .expect("real Kafka topic ID")
        .to_string();
    let mut map = BTreeMap::new();
    for line in text.lines().filter(|l| field(l, "Partition:").is_some()) {
        let id = field(line, "Partition:").unwrap().parse().unwrap();
        assert!(
            map.insert(
                id,
                Partition {
                    leader: field(line, "Leader:").unwrap().parse().unwrap(),
                    replicas: ids(field(line, "Replicas:").unwrap()),
                    isr: ids(field(line, "Isr:").unwrap_or(""))
                }
            )
            .is_none()
        );
    }
    assert!(!map.is_empty(), "{text}");
    (topic_id, map)
}
async fn offsets(topic: &str, time: &str) -> BTreeMap<u32, i64> {
    let out = kafka(
        "kafka-get-offsets",
        vec!["--topic".into(), topic.into(), "--time".into(), time.into()],
        None,
    )
    .await;
    let prefix = format!("{topic}:");
    let mut result = BTreeMap::new();
    for line in out.lines().filter_map(|v| v.strip_prefix(&prefix)) {
        let (partition, offset) = line.split_once(':').unwrap();
        assert!(
            result
                .insert(partition.parse().unwrap(), offset.parse().unwrap())
                .is_none()
        );
    }
    assert!(!result.is_empty(), "{out}");
    result
}
pub(super) async fn snapshot(topic: &str) -> Snapshot {
    let cluster = kafka("kafka-cluster", vec![], None)
        .await
        .lines()
        .find_map(|l| l.strip_prefix("Cluster ID: "))
        .expect("actual cluster ID")
        .trim()
        .to_string();
    let (topic_id, partitions) = metadata(topic).await;
    let begin = offsets(topic, "-2").await;
    let end = offsets(topic, "-1").await;
    assert_eq!(
        partitions.keys().collect::<Vec<_>>(),
        begin.keys().collect::<Vec<_>>()
    );
    assert_eq!(
        begin.keys().collect::<Vec<_>>(),
        end.keys().collect::<Vec<_>>()
    );
    for (p, b) in &begin {
        assert!(*b >= 0 && *b <= end[p]);
    }
    Snapshot {
        cluster,
        topic_id,
        partitions,
        begin,
        end,
    }
}
pub(super) async fn topic(partitions: u32, replicas: Option<&str>) -> String {
    let topic = format!("sdk_rl_matrix_{}", uuid::Uuid::new_v4().simple());
    create(&topic, partitions, replicas).await;
    topic
}
async fn published(topic: &str, partitions: u32) {
    // Admin acknowledgement precedes broker metadata publication. Poll only
    // reads; never repeat CREATE/ALTER or accept resource/connection failures.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let text = kafka(
                "kafka-topics",
                vec![
                    "--describe".into(),
                    "--if-exists".into(),
                    "--topic".into(),
                    topic.into(),
                ],
                None,
            )
            .await;
            let rows: Vec<_> = text
                .lines()
                .filter(|l| field(l, "Partition:").is_some())
                .collect();
            if rows.len() == partitions as usize
                && rows.iter().all(|l| {
                    field(l, "Leader:")
                        .and_then(|v| v.parse::<i32>().ok())
                        .is_some_and(|v| v >= 0)
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("created/altered owned topic metadata did not become readable");
}
pub(super) async fn create(topic: &str, partitions: u32, replicas: Option<&str>) {
    owned(topic);
    assert!(partitions > 0);
    let assignment =
        replicas.map_or_else(|| vec!["1"; partitions as usize].join(","), str::to_owned);
    kafka(
        "kafka-topics",
        vec![
            "--create".into(),
            "--topic".into(),
            topic.into(),
            "--replica-assignment".into(),
            assignment,
            "--config".into(),
            "min.insync.replicas=1".into(),
        ],
        None,
    )
    .await;
    published(topic, partitions).await;
    let s = snapshot(topic).await;
    assert_eq!(s.partitions.len(), partitions as usize);
    assert!(s.end.values().all(|v| *v == 0));
}
pub(super) async fn grow(topic: &str, partitions: u32) {
    owned(topic);
    let old = snapshot(topic).await;
    kafka(
        "kafka-topics",
        vec![
            "--alter".into(),
            "--topic".into(),
            topic.into(),
            "--partitions".into(),
            partitions.to_string(),
        ],
        None,
    )
    .await;
    published(topic, partitions).await;
    let new = snapshot(topic).await;
    assert_eq!(new.topic_id, old.topic_id);
    assert_eq!(new.partitions.len(), partitions as usize);
}
// Kafka's public keyed partition selection: positive murmur2(UTF-8 key) % count.
// The actual broker high watermarks below independently verify every selection.
fn murmur2(data: &[u8]) -> u32 {
    let mut h = 0x9747b28cu32 ^ data.len() as u32;
    let (chunks, tail) = data.as_chunks::<4>();
    for bytes in chunks {
        let mut k = u32::from_le_bytes(*bytes).wrapping_mul(0x5bd1e995);
        k ^= k >> 24;
        k = k.wrapping_mul(0x5bd1e995);
        h = h.wrapping_mul(0x5bd1e995) ^ k;
    }
    for (i, b) in tail.iter().enumerate() {
        h ^= u32::from(*b) << (8 * i);
    }
    if !tail.is_empty() {
        h = h.wrapping_mul(0x5bd1e995);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(0x5bd1e995);
    h ^ (h >> 15)
}
pub(super) async fn send(topic: &str, records: &[(u32, i64)]) -> Snapshot {
    owned(topic);
    let before = snapshot(topic).await;
    let count = before.partitions.len() as u32;
    let mut keys = BTreeMap::new();
    for (p, _) in records {
        assert!(*p < count);
        keys.entry(*p).or_insert_with(|| {
            (0u32..)
                .map(|n| format!("p{p}_{n}"))
                .find(|k| (murmur2(k.as_bytes()) & 0x7fff_ffff) % count == *p)
                .unwrap()
        });
    }
    let input = records
        .iter()
        .map(|(p, id)| format!("{}:{id},p{p}-{id}", keys[p]))
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    kafka(
        "kafka-console-producer",
        vec![
            "--topic".into(),
            topic.into(),
            "--sync".into(),
            "--property".into(),
            "parse.key=true".into(),
            "--property".into(),
            "key.separator=:".into(),
            "--producer-property".into(),
            "acks=all".into(),
            "--producer-property".into(),
            "partitioner.ignore.keys=false".into(),
        ],
        Some(input),
    )
    .await;
    let after = snapshot(topic).await;
    assert_eq!(before.topic_id, after.topic_id);
    assert_eq!(before.partitions.len(), after.partitions.len());
    for (p, end) in &before.end {
        assert_eq!(
            after.end[p] - end,
            records
                .iter()
                .filter(|(partition, _)| partition == p)
                .count() as i64,
            "actual broker partition {p}"
        );
    }
    after
}
pub(super) async fn trim_prefix(topic: &str, partition: u32, before: i64) {
    owned(topic);
    let path = format!(
        "/var/lib/kafka/data/sqlx-delete-{}.json",
        uuid::Uuid::new_v4().simple()
    );
    let body = serde_json::json!({
        "partitions": [{"topic":topic,"partition":partition,"offset":before}],
        "version": 1
    })
    .to_string();
    let container = std::env::var("DORIS_KAFKA_CONTAINER").unwrap();
    let mut p = Command::new("docker")
        .args([
            "exec",
            "-i",
            &container,
            "sh",
            "-c",
            "umask 077; cat > \"$1\"",
            "-",
            &path,
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    p.stdin.take().unwrap().write_all(body.as_bytes()).unwrap();
    assert!(p.wait_with_output().unwrap().status.success());
    kafka(
        "kafka-delete-records",
        vec!["--offset-json-file".into(), path],
        None,
    )
    .await;
    assert_eq!(snapshot(topic).await.begin[&partition], before);
}
pub(super) async fn delete(topic: &str) {
    owned(topic);
    kafka(
        "kafka-topics",
        vec!["--delete".into(), "--topic".into(), topic.into()],
        None,
    )
    .await;
}

#[derive(serde::Deserialize)]
struct Node {
    id: String,
    node_id: i32,
    name: String,
}
#[derive(serde::Deserialize)]
struct Faults {
    owner: String,
    image: String,
    cluster_id: String,
    nodes: Vec<Node>,
}
fn faults() -> Faults {
    let path = PathBuf::from(
        std::env::var("DORIS_KAFKA_FAULT_MANIFEST")
            .expect("private external owned fault-broker manifest required"),
    );
    assert!(
        path.canonicalize()
            .unwrap()
            .starts_with(std::env::var("SQLX_DORIS_EXTERNAL_VOLUME").unwrap())
    );
    assert!(!std::fs::symlink_metadata(&path).unwrap().is_symlink());
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o077,
        0
    );
    let v: Faults = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(v.nodes.len(), 2);
    assert!(v.nodes.iter().all(|n| n.node_id > 1));
    assert_ne!(v.nodes[0].node_id, v.nodes[1].node_id);
    assert_eq!(
        uuid::Uuid::parse_str(&v.owner)
            .unwrap()
            .simple()
            .to_string(),
        v.owner
    );
    v
}
pub(super) fn node_ids() -> (i32, i32) {
    let f = faults();
    (f.nodes[0].node_id, f.nodes[1].node_id)
}
pub(super) fn control(index: usize, start: bool) {
    let f = faults();
    let n = &f.nodes[index];
    assert!(n.name.starts_with("sqlx-kafka-matrix-"));
    let out = Command::new("docker")
        .args(["inspect", &n.id])
        .output()
        .unwrap();
    assert!(out.status.success());
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let c = &value[0];
    assert_eq!(c["Id"], n.id);
    assert_eq!(c["Image"], f.image);
    assert!(
        c["Config"]["Env"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "KAFKA_PROCESS_ROLES=broker"),
        "fault nodes must not run controllers"
    );
    assert_eq!(c["Name"], format!("/{}", n.name));
    assert_eq!(c["Config"]["Labels"]["sqlx-doris-fixture-owner"], f.owner);
    assert!(
        c["Config"]["Env"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == &format!("KAFKA_NODE_ID={}", n.node_id))
    );
    assert!(
        c["Config"]["Env"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == &format!("CLUSTER_ID={}", f.cluster_id))
    );
    let mounts = c["Mounts"].as_array().unwrap();
    assert!(mounts.iter().all(|m| m["Type"] == "volume"));
    assert!(mounts.iter().any(|m| {
        m["Destination"] == "/var/lib/kafka/data"
            && m["Name"]
                .as_str()
                .unwrap()
                .starts_with("sqlx-kafka-matrix-")
    }));
    let args = if start {
        vec!["start", n.id.as_str()]
    } else {
        vec!["stop", "--time", "10", n.id.as_str()]
    };
    assert!(
        Command::new("docker")
            .args(args)
            .output()
            .unwrap()
            .status
            .success()
    );
    if start {
        let out = Command::new("docker")
            .args(["inspect", &n.id])
            .output()
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
        let since = value[0]["State"]["StartedAt"].as_str().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(45);
        loop {
            let out = Command::new("docker")
                // Bound retained log reads and scope markers to this exact start.
                // Never accept a READY marker from an earlier process generation.
                .args(["logs", "--tail", "1000", "--since", since, &n.id])
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "owned fault broker logs are unavailable"
            );
            let logs = String::from_utf8_lossy(&out.stdout).to_string()
                + &String::from_utf8_lossy(&out.stderr);
            if logs.contains("Transition from STARTING to STARTED")
                || logs.contains("Transitioning from RECOVERY to RUNNING")
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "owned fault broker failed to reach RUNNING"
            );
            std::thread::sleep(Duration::from_millis(250));
        }
    }
}
pub(super) async fn wait_replicas(topic: &str, leader: i32, isr: usize) {
    tokio::time::timeout(Duration::from_secs(65), async {
        loop {
            let (_, p) = metadata(topic).await;
            if p[&0].leader == leader && p[&0].isr.len() == isr {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("expected actual leader/ISR not observed");
}
pub(super) async fn elect(topic: &str) {
    owned(topic);
    kafka(
        "kafka-leader-election",
        vec![
            "--election-type".into(),
            "unclean".into(),
            "--topic".into(),
            topic.into(),
            "--partition".into(),
            "0".into(),
        ],
        None,
    )
    .await;
}
