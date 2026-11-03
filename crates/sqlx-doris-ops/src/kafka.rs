//! Explicit Kafka observations, independent of Doris connection/migration state.
use crate::storage::PrivateFiles;
use crate::{
    context::Context,
    failure::{self, Result},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[path = "kafka/model.rs"]
mod model;
#[path = "kafka/wire.rs"]
mod wire;
use model::{Epoch, Offset, Partition, Snapshot};
pub use model::{Offset as Candidate, offset as parse_offset};

#[derive(Clone)]
pub enum Action {
    /// Save a new private historical baseline. Never overwrites or certifies earlier history.
    Snapshot {
        connection_file: PathBuf,

        topic: String,

        output: PathBuf,

        timeout: u64,
    },
    /// Check explicitly supplied next offsets. Never derives, transfers, resets or commits offsets.
    Preflight {
        connection_file: PathBuf,

        topic: String,
        /// Earlier trusted snapshot; omitting it always leaves history inconclusive.
        checkpoint: Option<PathBuf>,

        offsets: Vec<Offset>,

        timeout: u64,
    },
}
use crate::failure::{Failure, fault};
fn connect_fault() -> Box<dyn std::error::Error + Send + Sync> {
    failure::bootstrap_unavailable()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ConnectionFile {
    format: String,
    bootstrap_servers: Vec<String>,
    security_protocol: String,
    ca_file: Option<PathBuf>,
    client_certificate_file: Option<PathBuf>,
    client_key_file: Option<PathBuf>,
    sasl_plain: Option<Plain>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plain {
    username: String,
    password: String,
}
struct Config {
    bootstrap: Vec<String>,
    tls: Option<Arc<rustls::ClientConfig>>,
    sasl: Option<Plain>,
}
fn read(path: &Path, files: &PrivateFiles, max: usize) -> Result<Vec<u8>> {
    files.read_limited(path, max).map_err(|error| {
        failure::wrap(
            2,
            "input must be a bounded private regular external file without symlinks",
            error,
        )
    })
}

impl Config {
    fn load(path: &Path, files: &PrivateFiles) -> Result<Self> {
        let f: ConnectionFile = serde_json::from_slice(&read(path, files, 65536)?)
            .map_err(|_| fault(2, "invalid Kafka connection file fields/JSON"))?;
        if f.format != "sqlx-doris-kafka-connection-v1"
            || f.bootstrap_servers.is_empty()
            || f.bootstrap_servers.len() > 16
            || !matches!(
                f.security_protocol.as_str(),
                "PLAINTEXT" | "SSL" | "SASL_SSL"
            )
        {
            return Err(fault(
                2,
                "unsupported Kafka connection format/security protocol",
            ));
        }
        for server in &f.bootstrap_servers {
            wire::endpoint(server)?;
        }
        if f.client_certificate_file.is_some() != f.client_key_file.is_some()
            || (f.security_protocol == "SASL_SSL") != f.sasl_plain.is_some()
            || f.sasl_plain.as_ref().is_some_and(|p| {
                p.username.is_empty()
                    || p.password.is_empty()
                    || p.username.len() > 4096
                    || p.password.len() > 4096
                    || p.username.contains('\0')
                    || p.password.contains('\0')
            })
        {
            return Err(fault(2, "invalid Kafka TLS/SASL credentials configuration"));
        }
        let tls = if f.security_protocol != "PLAINTEXT" {
            use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};
            let ca = read(
                f.ca_file
                    .as_deref()
                    .ok_or_else(|| fault(2, "verified TLS requires an explicit CA file"))?,
                files,
                wire::FRAME_MAX,
            )?;
            let mut roots = rustls::RootCertStore::empty();
            for der in CertificateDer::pem_slice_iter(&ca) {
                roots
                    .add(der.map_err(|_| fault(2, "invalid Kafka CA PEM"))?)
                    .map_err(|_| fault(2, "invalid Kafka CA certificate"))?;
            }
            if roots.is_empty() {
                return Err(fault(2, "Kafka CA file is empty"));
            }
            let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| fault(2, "Kafka TLS configuration failed"))?
            .with_root_certificates(roots);
            let config = if let (Some(cert), Some(key)) = (
                f.client_certificate_file.as_deref(),
                f.client_key_file.as_deref(),
            ) {
                let cert = read(cert, files, wire::FRAME_MAX)?;
                let certs = CertificateDer::pem_slice_iter(&cert)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|_| fault(2, "invalid Kafka client certificate"))?;
                let key = PrivateKeyDer::from_pem_slice(&read(key, files, 65536)?)
                    .map_err(|_| fault(2, "invalid Kafka client key"))?;
                builder
                    .with_client_auth_cert(certs, key)
                    .map_err(|_| fault(2, "Kafka client certificate/key mismatch"))?
            } else {
                builder.with_no_client_auth()
            };
            Some(Arc::new(config))
        } else {
            if f.ca_file.is_some() || f.client_certificate_file.is_some() {
                return Err(fault(2, "TLS files require SSL or SASL_SSL"));
            }
            None
        };
        Ok(Self {
            bootstrap: f.bootstrap_servers,
            tls,
            sasl: f.sasl_plain,
        })
    }
}
fn now() -> Result<u64> {
    u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()).map_err(Into::into)
}
async fn observe(
    config: &Config,
    topic: &str,
    baseline: Option<&Snapshot>,
) -> Result<(Snapshot, BTreeMap<i32, Epoch>)> {
    let mut root = None;
    // Read-only bootstrap fallback, bounded by the overall deadline. No API mutation retry.
    for server in &config.bootstrap {
        match tokio::time::timeout(Duration::from_secs(3), wire::Wire::connect(config, server))
            .await
        {
            Ok(Ok(w)) => {
                root = Some(w);
                break;
            }
            Ok(Err(e))
                if e.downcast_ref::<Failure>()
                    .is_some_and(|f| f.bootstrap_unavailable) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {}
        }
    }
    let mut root = root.ok_or_else(|| fault(7, "Kafka bootstrap unavailable before deadline"))?;
    let meta = root.metadata(topic).await?;
    let identity_matches = baseline.is_some_and(|o| {
        o.topic_id == meta.topic_id && o.topic == meta.topic && o.cluster_id == meta.cluster
    });
    let anchor: BTreeMap<_, _> = baseline
        .filter(|_| identity_matches)
        .into_iter()
        .flat_map(|s| s.partitions.iter().map(|p| (p.partition, p.leader_epoch)))
        .collect();
    let mut groups: BTreeMap<i32, Vec<(i32, i32)>> = BTreeMap::new();
    for (p, (leader, epoch)) in &meta.leaders {
        groups.entry(*leader).or_default().push((*p, *epoch));
    }
    let mut partitions = Vec::new();
    let mut epochs = BTreeMap::new();
    for (leader, parts) in groups {
        let address = &meta.brokers[&leader];
        let mut w = wire::Wire::connect(config, address).await?;
        if w.metadata(topic).await? != meta {
            return Err(fault(4, "Kafka metadata changed during observation"));
        }
        let first = w.offsets(topic, &parts, -2).await?;
        let end = w.offsets(topic, &parts, -1).await?;
        epochs.extend(w.epochs(topic, &parts, &anchor).await?);
        let last_start = w.offsets(topic, &parts, -2).await?;
        let last_end = w.offsets(topic, &parts, -1).await?;
        if w.metadata(topic).await? != meta {
            return Err(fault(4, "Kafka metadata changed during observation"));
        }
        for (p, e) in parts {
            if first[&p] != last_start[&p] || last_end[&p] < end[&p] {
                return Err(fault(
                    4,
                    "Kafka bounds changed during observation; no atomic snapshot",
                ));
            }
            if first[&p] > end[&p] {
                return Err(fault(7, "inconsistent Kafka range"));
            }
            partitions.push(Partition {
                partition: p,
                leader_epoch: e,
                earliest: first[&p],
                end: end[&p],
            });
        }
    }
    if root.metadata(topic).await? != meta {
        return Err(fault(4, "Kafka metadata changed during observation"));
    }
    partitions.sort_by_key(|p| p.partition);
    let s = Snapshot {
        format: model::FORMAT.into(),
        isolation_level: "read_uncommitted".into(),
        captured_at_unix_ms: now()?,
        cluster_id: meta.cluster,
        topic: meta.topic,
        topic_id: meta.topic_id,
        partitions,
    };
    s.validate()
        .map_err(|_| fault(7, "Kafka snapshot metadata invalid"))?;
    Ok((s, epochs))
}
fn save(path: &Path, value: &Snapshot, files: &PrivateFiles) -> Result<()> {
    let data = serde_json::to_vec_pretty(value)?;
    if data.len() > wire::FRAME_MAX {
        return Err(fault(2, "checkpoint exceeds size limit"));
    }
    files.write_new(path, &data)
}

fn base(operation: &str) -> Value {
    json!({
        "operation": operation,
        "exit_code": 0,
        "observation": "observed",
        "server_read_only": true,
        "automatic_offset_transfer": false,
        "automatic_offset_reset": false,
        "safe_to_recover": false,
        "kafka_continuation_verified": false,
        "atomic_snapshot": false,
        "checkpoint_file": null,
        "checkpoint": null,
        "current": null,
        "checks": [],
        "issues": [],
        "message": "Observations only; no Doris consumption, complete history or recovery attestation. A trusted earlier checkpoint and external source/admin isolation and data reconciliation remain required."
    })
}

pub async fn evaluate(args: &Context, action: &Action) -> Value {
    let (connection_file, topic, seconds) = match action {
        Action::Snapshot {
            connection_file,
            topic,
            timeout,
            ..
        }
        | Action::Preflight {
            connection_file,
            topic,
            timeout,
            ..
        } => (connection_file, topic, *timeout),
    };
    let mut result = base(if matches!(action, Action::Snapshot { .. }) {
        "snapshot"
    } else {
        "preflight"
    });
    result["checkpoint_file"] = match action {
        Action::Snapshot { output, .. } => json!(output),
        Action::Preflight { checkpoint, .. } => json!(checkpoint),
    };
    if !(1..=3600).contains(&seconds) {
        result["exit_code"] = json!(2);
        result["observation"] = json!("invalid_input");
        result["issues"] = json!([{"code": "invalid_input", "partition": null}]);
        result["message"] = json!("Kafka observation deadline must be between 1 and 3600 seconds");
        return result;
    }
    let operation = async {
        if args.identity_file.is_some() {
            return Err(fault(
                2,
                "Kafka commands use only --connection-file; Doris identities are not Kafka credentials",
            ));
        }
        model::topic(topic)?;
        let files = PrivateFiles::new()
            .map_err(|_| fault(2, "Kafka files require mounted external storage"))?;
        let config = Config::load(connection_file, &files)?;
        let (baseline, candidates) = if let Action::Preflight {
            checkpoint,
            offsets,
            ..
        } = action
        {
            let candidates = model::offsets(offsets)?;
            let baseline = checkpoint
                .as_ref()
                .map(|p| -> Result<Snapshot> {
                    let s: Snapshot = serde_json::from_slice(&read(p, &files, wire::FRAME_MAX)?)
                        .map_err(|_| fault(2, "invalid Kafka checkpoint JSON/fields"))?;
                    s.validate()?;
                    Ok(s)
                })
                .transpose()?;
            (baseline, Some(candidates))
        } else {
            (None, None)
        };
        result["checkpoint"] = serde_json::to_value(&baseline)?;
        let (current, epochs) = observe(&config, topic, baseline.as_ref()).await?;
        result["current"] = serde_json::to_value(&current)?;
        if let Action::Snapshot { output, .. } = action {
            // Anchor must be supported by this same leader epoch, even for an empty log.
            if current.partitions.iter().any(|p| {
                !epochs
                    .get(&p.partition)
                    .is_some_and(|e| e.error == 0 && e.epoch == p.leader_epoch && e.end >= p.end)
            }) {
                return Err(fault(
                    4,
                    "current Kafka epoch evidence unavailable; no checkpoint saved",
                ));
            }
            args.observation
                .phase(crate::observation::Phase::Local, "checkpoint_file_write");
            save(output, &current, &files)?;
            result["checkpoint"] = serde_json::to_value(&current)?;
            result["observation"] = json!("checkpoint_saved");
        } else {
            let (code, issues, checks) = model::compare(
                &current,
                baseline.as_ref(),
                candidates.as_ref().unwrap(),
                &epochs,
            );
            result["checkpoint"] = serde_json::to_value(&baseline)?;
            result["issues"] = json!(issues);
            result["checks"] = json!(checks);
            result["exit_code"] = json!(code);
            result["observation"] = json!(match code {
                0 => "consistent_observations",
                3 => "blocked",
                _ => "inconclusive",
            });
        }
        Ok(())
    };
    let error = match tokio::time::timeout(Duration::from_secs(seconds), operation).await {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e),
        Err(_) => Some(fault(
            7,
            "Kafka observation deadline expired; no mutation or automatic retry",
        )),
    };
    if let Some(e) = error {
        let f = e.downcast_ref::<Failure>();
        let code = f.map_or(7, |f| f.code());
        result["exit_code"] = json!(code);
        result["observation"] = json!(match code {
            2 => "invalid_input",
            3 => "blocked",
            4 => "inconclusive",
            _ => "unavailable",
        });
        let reason = match code {
            2 => "invalid_input",
            3 => "source_conflict",
            4 => "observation_inconclusive",
            _ => "operation_unavailable",
        };
        result["issues"] = json!([{"code": reason, "partition": null}]);
        result["message"] = json!(f.map_or("Kafka observation failed; no mutation sent", |f| {
            f.message.as_ref()
        }));
    }
    result
}
pub async fn run(args: &Context, action: &Action) -> Result<()> {
    args.observation
        .phase(crate::observation::Phase::Read, "kafka_observation");
    let result = evaluate(args, action).await;
    let code = result["exit_code"].as_u64().unwrap() as u8;
    args.observation.details(result.clone());
    if code == 0 {
        Ok(())
    } else {
        Err(fault(
            code,
            "Kafka read-only precheck did not establish consistent observations",
        ))
    }
}
#[cfg(test)]
#[path = "kafka/tests.rs"]
mod tests;
