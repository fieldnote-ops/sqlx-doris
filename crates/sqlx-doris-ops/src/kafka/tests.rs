use super::*;
use model::{FORMAT, compare, offset, offsets};
use std::sync::Mutex;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use wire::{Reader, Wire, Writer};
fn snapshot() -> Snapshot {
    Snapshot {
        format: FORMAT.into(),
        isolation_level: "read_uncommitted".into(),
        captured_at_unix_ms: 1,
        cluster_id: "cluster_A".into(),
        topic: "events".into(),
        topic_id: "00000000-0000-0000-0000-000000000001".into(),
        partitions: vec![
            Partition {
                partition: 0,
                leader_epoch: 4,
                earliest: 0,
                end: 5,
            },
            Partition {
                partition: 1,
                leader_epoch: 2,
                earliest: 0,
                end: 3,
            },
        ],
    }
}
fn candidates() -> BTreeMap<i32, i64> {
    BTreeMap::from([(0, 5), (1, 3)])
}
fn epochs() -> BTreeMap<i32, Epoch> {
    BTreeMap::from([
        (
            0,
            Epoch {
                error: 0,
                epoch: 4,
                end: 5,
            },
        ),
        (
            1,
            Epoch {
                error: 0,
                epoch: 2,
                end: 3,
            },
        ),
    ])
}
#[test]
fn ranges_and_epochs_never_certify_consumption_or_recovery() {
    let s = snapshot();
    assert_eq!(compare(&s, Some(&s), &candidates(), &epochs()).0, 0);
    for key in [
        "safe_to_recover",
        "kafka_continuation_verified",
        "automatic_offset_reset",
        "automatic_offset_transfer",
        "atomic_snapshot",
    ] {
        assert_eq!(base("preflight")[key], false);
    }
    assert_eq!(compare(&s, None, &candidates(), &epochs()).0, 4);
}
#[test]
fn identity_partition_changes_and_expired_or_beyond_end_are_blocked() {
    let old = snapshot();
    for mutation in 0..6 {
        let mut s = old.clone();
        let mut c = candidates();
        match mutation {
            0 => s.cluster_id = "other".into(),
            1 => s.topic_id = "00000000-0000-0000-0000-000000000002".into(),
            2 => s.partitions.push(Partition {
                partition: 2,
                leader_epoch: 0,
                earliest: 0,
                end: 1,
            }),
            3 => {
                c.remove(&1);
            }
            4 => {
                s.partitions[0].earliest = 4;
                c.insert(0, 3);
            }
            _ => {
                c.insert(0, 6);
            }
        }
        assert_eq!(
            compare(&s, Some(&old), &c, &epochs()).0,
            3,
            "case {mutation}"
        );
    }
}
#[test]
fn regrown_tail_is_detected_even_when_every_candidate_is_in_range() {
    let old = snapshot();
    let mut s = old.clone();
    s.partitions[0].leader_epoch = 5;
    s.partitions[0].end = 6;
    let mut e = epochs();
    e.insert(
        0,
        Epoch {
            error: 0,
            epoch: 4,
            end: 3,
        },
    );
    let (code, issues, checks) = compare(&s, Some(&old), &candidates(), &e);
    assert_eq!(code, 3);
    assert_eq!(issues[0]["code"], "checkpoint_tail_truncated");
    assert_eq!(checks[0]["range"], "in_range");
}
#[test]
fn epoch_cache_loss_echo_fallback_and_coverage_gaps_remain_unknown() {
    let old = snapshot();
    for mode in 0..6 {
        let mut s = old.clone();
        let mut e = epochs();
        let mut c = candidates();
        match mode {
            0 => {
                e.remove(&0);
            }
            1 => {
                e.insert(
                    0,
                    Epoch {
                        error: 29,
                        epoch: -1,
                        end: -1,
                    },
                );
            }
            2 => {
                e.insert(
                    0,
                    Epoch {
                        error: 0,
                        epoch: 3,
                        end: 5,
                    },
                );
            }
            3 => {
                s.partitions[0].earliest = 5;
                e.insert(
                    0,
                    Epoch {
                        error: 0,
                        epoch: 4,
                        end: 5,
                    },
                );
            }
            4 => {
                s.partitions[0].end = 8;
                c.insert(0, 7);
            }
            _ => {
                s.partitions[0].end = 0;
                c.insert(0, 0);
            }
        }
        let baseline = if mode == 5 { &s } else { &old };
        assert_eq!(compare(&s, Some(baseline), &c, &e).0, 4, "case {mode}");
    }
}
#[test]
fn checkpoint_fields_and_offsets_are_strict_numeric_unique_and_canonical() {
    let s = snapshot();
    s.validate().unwrap();
    let mut bad = s.clone();
    bad.partitions[1].partition = 0;
    assert!(bad.validate().is_err());
    let mut v = serde_json::to_value(s).unwrap();
    v["unexpected"] = json!(true);
    assert!(serde_json::from_value::<Snapshot>(v).is_err());
    for text in [
        "0=OFFSET_END",
        "-1=5",
        "0=-1",
        "0=9223372036854775808",
        "x=2",
    ] {
        assert!(offset(text).is_err());
    }
    assert!(offsets(&[offset("0=1").unwrap(), offset("00=2").unwrap()]).is_err());
    assert!(offsets(&[offset("1=3").unwrap(), offset("0=5").unwrap()]).is_ok());
}
#[test]
fn malformed_lengths_are_bounded_before_allocation() {
    for bytes in [vec![255; 4], i32::MAX.to_be_bytes().to_vec()] {
        assert!(Reader::new(&bytes).count(false, 4096).is_err());
    }
    assert!(Reader::new(&[255, 255, 255, 255, 31]).var().is_err());
    assert!(Reader::new(&[1]).tags().is_err());
    assert!(Reader::new(&[0, 255]).string(false).is_err());
}

fn config(address: String) -> Config {
    Config {
        bootstrap: vec![address],
        tls: None,
        sasl: None,
    }
}
fn metadata(port: u16, drift: bool) -> Writer {
    let mut b = Writer::default();
    b.i32(0);
    b.var(2);
    b.i32(1);
    b.compact("localhost");
    b.i32(i32::from(port));
    b.var(0);
    b.var(0);
    b.compact("cluster_A");
    b.i32(1);
    b.var(2);
    b.i16(0);
    b.compact("events");
    b.0.extend([
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        if drift { 2 } else { 1 },
    ]);
    b.i8(0);
    b.var(2);
    b.i16(0);
    b.i32(0);
    b.i32(1);
    b.i32(4);
    for _ in 0..2 {
        b.var(2);
        b.i32(1);
    }
    b.var(1);
    b.var(0);
    b.i32(0);
    b.var(0);
    b.var(0);
    b
}
struct Mock {
    address: String,
    trace: Arc<Mutex<Vec<i16>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn mock(mode: &'static str) -> Mock {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let trace = Arc::new(Mutex::new(Vec::new()));
    let shared = trace.clone();
    let serial = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let trace = shared.clone();
            let serial = serial.clone();
            tasks.spawn(async move {
                while let Ok(n) = stream.read_i32().await {
                    assert!((1..=wire::FRAME_MAX as i32).contains(&n));
                    let mut data = vec![0; n as usize];
                    stream.read_exact(&mut data).await.unwrap();
                    let mut r = Reader::new(&data);
                    let key = r.i16().unwrap();
                    let version = r.i16().unwrap();
                    let corr = r.i32().unwrap();
                    r.string(false).unwrap();
                    if key == 3 {
                        r.tags().unwrap();
                    }
                    trace.lock().unwrap().push(key);
                    let mut b = Writer::default();
                    b.i32(if mode == "correlation" {
                        corr + 1
                    } else {
                        corr
                    });
                    if key == 3 {
                        b.var(0);
                    }
                    match key {
                        18 => {
                            assert_eq!(version, 0);
                            r.done().unwrap();
                            b.i16(0);
                            b.i32(4);
                            for (key, v) in [(18, 0), (3, 12), (2, 5), (23, 3)] {
                                b.i16(key);
                                b.i16(v);
                                b.i16(v);
                            }
                        }
                        3 => {
                            assert_eq!(version, 12);
                            assert_eq!(r.count(true, 1).unwrap(), 1);
                            r.bytes(16).unwrap();
                            assert_eq!(r.required(true).unwrap(), "events");
                            r.tags().unwrap();
                            assert_eq!(r.i8().unwrap(), 0, "auto-create forbidden");
                            assert_eq!(r.i8().unwrap(), 0);
                            r.tags().unwrap();
                            r.done().unwrap();
                            let call = serial.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                            b.0.extend(metadata(port, mode == "drift" && call > 1).0);
                        }
                        2 => {
                            assert_eq!(r.i32().unwrap(), -1);
                            assert_eq!(r.i8().unwrap(), 0);
                            assert_eq!(r.count(false, 1).unwrap(), 1);
                            assert_eq!(r.required(false).unwrap(), "events");
                            assert_eq!(r.count(false, 1).unwrap(), 1);
                            assert_eq!(r.i32().unwrap(), 0);
                            assert_eq!(r.i32().unwrap(), 4);
                            let timestamp = r.i64().unwrap();
                            assert!(matches!(timestamp, -1 | -2));
                            r.done().unwrap();
                            b.i32(0);
                            b.i32(1);
                            b.string("events");
                            b.i32(1);
                            b.i32(0);
                            b.i16(0);
                            b.i64(-1);
                            b.i64(if timestamp == -2 { 0 } else { 5 });
                            b.i32(if timestamp == -2 { 2 } else { 4 });
                        }
                        23 => {
                            assert_eq!(r.i32().unwrap(), -1);
                            assert_eq!(r.count(false, 1).unwrap(), 1);
                            assert_eq!(r.required(false).unwrap(), "events");
                            assert_eq!(r.count(false, 1).unwrap(), 1);
                            assert_eq!(r.i32().unwrap(), 0);
                            assert_eq!(r.i32().unwrap(), 4);
                            let epoch = r.i32().unwrap();
                            r.done().unwrap();
                            b.i32(0);
                            b.i32(1);
                            b.string("events");
                            b.i32(1);
                            b.i16(0);
                            b.i32(0);
                            b.i32(epoch);
                            b.i64(5);
                        }
                        _ => panic!("non-read-only API {key}"),
                    }
                    if mode == "oversize" {
                        stream.write_i32(wire::FRAME_MAX as i32 + 1).await.unwrap();
                        break;
                    }
                    stream.write_i32(b.0.len() as i32).await.unwrap();
                    if mode == "disconnect" {
                        stream.write_all(&b.0[..2]).await.unwrap();
                        break;
                    }
                    if mode == "stall" {
                        std::future::pending::<()>().await;
                    }
                    if stream.write_all(&b.0).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    Mock {
        address: format!("localhost:{port}"),
        trace,
        task,
    }
}
#[tokio::test]
async fn wire_observes_only_allowed_apis_and_never_auto_creates_or_joins_groups() {
    let m = mock("ok").await;
    let (s, e) = observe(&config(m.address.clone()), "events", None)
        .await
        .unwrap();
    assert_eq!(s.partitions[0].end, 5);
    assert_eq!(e[&0].end, 5);
    let trace = m.trace.lock().unwrap();
    assert!(trace.iter().all(|k| [18, 3, 2, 23].contains(k)));
    assert_eq!(trace.iter().filter(|k| **k == 2).count(), 4);
}
#[tokio::test]
async fn disconnections_correlations_and_oversize_frames_never_establish_a_snapshot() {
    for mode in ["correlation", "oversize", "disconnect"] {
        let m = mock(mode).await;
        assert!(
            Wire::connect(&config(m.address.clone()), &m.address)
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn metadata_change_and_cancellation_never_emit_a_positive_observation() {
    let m = mock("drift").await;
    let err = observe(&config(m.address.clone()), "events", None)
        .await
        .unwrap_err();
    assert_eq!(err.downcast_ref::<Failure>().unwrap().code(), 4);
    let m = mock("stall").await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            observe(&config(m.address.clone()), "events", None)
        )
        .await
        .is_err()
    );
    assert_eq!(*m.trace.lock().unwrap(), vec![18]);
}
#[tokio::test]
async fn api_allowlist_rejects_a_write_before_any_bytes_are_sent() {
    let m = mock("ok").await;
    let mut w = Wire::connect(&config(m.address.clone()), &m.address)
        .await
        .unwrap();
    assert!(w.call(0, 9, Writer::default()).await.is_err());
    assert_eq!(*m.trace.lock().unwrap(), vec![18]);
}

#[tokio::test]
async fn unreachable_bootstrap_falls_back_without_relaxing_readonly_checks() {
    // Reserve the port without listening: refusal is deterministic and another
    // parallel test cannot bind a different protocol server to a released port.
    let dead = tokio::net::TcpSocket::new_v4().unwrap();
    dead.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = dead.local_addr().unwrap().to_string();
    let m = mock("ok").await;
    let mut c = config(m.address.clone());
    c.bootstrap.insert(0, address);
    let (current, _) = observe(&c, "events", None).await.unwrap();
    assert_eq!(current.isolation_level, "read_uncommitted");
    assert!(
        m.trace
            .lock()
            .unwrap()
            .iter()
            .all(|k| [18, 3, 2, 23].contains(k))
    );
}

#[tokio::test]
async fn direct_request_deadline_is_rejected_before_timer_or_network_creation() {
    use crate::context::{ConnectionSettings, Context};
    let context = Context {
        connection: ConnectionSettings {
            database_url: None,
            fallback_url: None,
            identity_file: None,
            replication_num: 1,
            lock_timeout: 1,
            ddl_timeout: 1,
        },
        observation: crate::observation::Observation::default(),
    };
    let action = Action::Snapshot {
        connection_file: "unread-private-input".into(),
        topic: "events".into(),
        output: "unwritten-checkpoint".into(),
        timeout: u64::MAX,
    };
    let result = evaluate(&context, &action).await;
    assert_eq!(result["exit_code"], 2);
    assert!(result["current"].is_null());
    assert_eq!(
        context.observation.snapshot().phase,
        crate::observation::Phase::Read
    );
}
