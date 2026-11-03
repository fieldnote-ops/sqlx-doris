//! Receipts locate a database/name; they never authorize mutation by themselves.
use super::{
    FORMAT, Job, Locator, Observer, RECEIPT_MAX_BYTES, Result, fault, identifier, identity,
    marker_name, uuid_name,
};
use serde_json::Value;
use std::path::{Path, PathBuf};

pub(super) struct Located {
    pub name: String,
    pub path: Option<PathBuf>,
    pub data: Option<Value>,
    pub confirmed: Option<Job>,
}
fn document(path: &Path, files: &identity::PrivateFiles) -> Result<Value> {
    let data = files.read_limited(path, RECEIPT_MAX_BYTES).map_err(|_| {
        fault(
            2,
            "receipt must be a bounded private regular external file without symlinks",
        )
    })?;
    serde_json::from_slice(&data).map_err(|_| fault(2, "invalid receipt JSON"))
}

fn field<'a>(data: &'a Value, key: &str) -> Result<&'a str> {
    data.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| fault(2, "receipt field missing or not a string"))
}
pub(super) fn confirmation(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(".confirmed");
    PathBuf::from(name)
}
pub(super) fn validate(data: &Value) -> Result<()> {
    let object = data
        .as_object()
        .ok_or_else(|| fault(2, "receipt must be a JSON object"))?;
    if object.len() != 8 || field(data, "format")? != FORMAT {
        return Err(fault(2, "unknown receipt format/fields"));
    }
    let name = field(data, "job_name")?;
    uuid_name(name).map_err(|_| fault(2, "invalid receipt UUID name"))?;
    if field(data, "reservation")? != marker_name(name)? {
        return Err(fault(2, "receipt reservation/name mismatch"));
    }
    identity::valid_database(field(data, "database")?)
        .map_err(|_| fault(2, "invalid receipt database"))?;
    identifier(field(data, "table")?).map_err(|_| fault(2, "invalid receipt target"))?;
    identity::identity_id(field(data, "creator")?)
        .map_err(|_| fault(2, "invalid receipt creator"))?;
    for key in ["database_id", "cluster"] {
        if !field(data, key)?
            .parse::<u64>()
            .ok()
            .is_some_and(|v| v > 0 && v <= i64::MAX as u64)
        {
            return Err(fault(2, "invalid receipt numeric identity"));
        }
    }
    Ok(())
}
impl Located {
    pub fn from(locator: &Locator) -> Result<Self> {
        if locator.receipt.is_some() == locator.job_name.is_some() {
            return Err(fault(2, "provide exactly one receipt or managed job name"));
        }
        if let Some(path) = &locator.receipt {
            let files = identity::PrivateFiles::new()
                .map_err(|_| fault(2, "receipts require a mounted external volume"))?;
            let data = document(path, &files)?;
            validate(&data)?;
            let checkpoint = confirmation(path);
            let confirmed = match std::fs::symlink_metadata(&checkpoint) {
                Ok(_) => {
                    let value = document(&checkpoint, &files)?;
                    if value.as_object().map(|v| v.len()) != Some(3)
                        || value["format"] != "sqlx-doris-routine-confirmed-v1"
                        || value["receipt"] != data
                    {
                        return Err(fault(2, "confirmation receipt binding/format mismatch"));
                    }
                    let j = &value["job"];
                    if j.as_object().map(|v| v.len()) != Some(4) {
                        return Err(fault(2, "unknown confirmation job fields"));
                    }
                    let id = j["id"]
                        .as_u64()
                        .filter(|v| *v > 0 && *v <= i64::MAX as u64)
                        .ok_or_else(|| fault(2, "invalid confirmation JobId"))?;
                    let created = j["created"]
                        .as_str()
                        .filter(|s| !s.is_empty() && s.len() <= 64)
                        .ok_or_else(|| fault(2, "invalid confirmation creation identity"))?;
                    let state = j["state"]
                        .as_str()
                        .ok_or_else(|| fault(2, "invalid confirmation state"))?;
                    super::super::jobs::routine_terminal(state)
                        .map_err(|_| fault(2, "unknown confirmation state"))?;
                    Some(Job {
                        id,
                        created: created.into(),
                        state: state.into(),
                        progress: super::progress(&j["progress"].to_string())
                            .map_err(|_| fault(2, "invalid confirmation progress"))?,
                    })
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(_) => return Err(fault(2, "confirmation receipt cannot be inspected")),
            };
            Ok(Self {
                name: field(&data, "job_name")?.into(),
                path: Some(path.clone()),
                data: Some(data),
                confirmed,
            })
        } else {
            let name = locator
                .job_name
                .as_ref()
                .ok_or_else(|| fault(2, "provide --receipt or --job-name"))?;
            uuid_name(name).map_err(|_| fault(2, "invalid managed Routine UUID name"))?;
            Ok(Self {
                name: name.clone(),
                path: None,
                data: None,
                confirmed: None,
            })
        }
    }
    pub fn database(&self) -> Option<&str> {
        self.data.as_ref().and_then(|v| v["database"].as_str())
    }
    pub fn matches(&self, o: &Observer, r: &super::Reservation) -> Result<()> {
        if let Some(v) = &self.data
            && (v["database"] != o.database
                || v["database_id"] != o.db_id
                || v["cluster"] != o.cluster
                || v["creator"] != r.creator
                || v["table"] != r.table
                || v["job_name"] != r.name)
        {
            return Err(fault(
                3,
                "receipt does not match the current server reservation/database/cluster",
            ));
        }
        Ok(())
    }
    pub fn matches_job(&self, job: &Job) -> Result<()> {
        if let Some(old) = &self.confirmed
            && (old.id != job.id || old.created != job.created)
        {
            return Err(fault(
                3,
                "confirmed receipt and current JobId/creation identity differ",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn large_partition_confirmation_round_trips_and_oversize_never_creates_a_file() {
        let root = PathBuf::from(std::env::var_os("TMPDIR").expect("run via scripts/dev"))
            .join(format!("routine-size-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir(&root).unwrap();
        let path = root.join("receipt.json");
        let name = "sqlx_rl_00000000000040008000000000000001";
        let data = serde_json::json!({
            "format": FORMAT,
            "job_name": name,
            "reservation": format!("_{name}"),
            "database": "events_db",
            "database_id": "1",
            "cluster": "2",
            "table": "events",
            "creator": "sqlx_mig_00000000000040008000000000000001"
        });
        let offsets: serde_json::Map<String, Value> = (0..2000)
            .map(|i| (i.to_string(), Value::String("123".into())))
            .collect();
        let known = serde_json::json!({
            "format": "sqlx-doris-routine-confirmed-v1",
            "receipt": data,
            "job": {"id":1,"created":"2026-11-01 00:00:00","state":"RUNNING","progress":offsets}
        });
        let files = identity::PrivateFiles::new().unwrap();
        super::super::receipt(&path, &data, &files).unwrap();
        super::super::receipt(&confirmation(&path), &known, &files).unwrap();
        assert!(std::fs::metadata(confirmation(&path)).unwrap().len() > 16384);
        let located = Located::from(&Locator {
            job_name: None,
            receipt: Some(path),
        })
        .unwrap();
        assert_eq!(
            located
                .confirmed
                .unwrap()
                .progress
                .as_object()
                .unwrap()
                .len(),
            2000
        );
        let oversized = root.join("oversized.json");
        assert!(
            super::super::receipt(
                &oversized,
                &serde_json::json!({"large":"x".repeat(RECEIPT_MAX_BYTES)}),
                &files
            )
            .is_err()
        );
        assert!(!oversized.exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn receipt_requires_exact_protocol_fields_and_never_stores_credentials() {
        let name = "sqlx_rl_00000000000040008000000000000001";
        let data = serde_json::json!({
            "format": FORMAT,
            "job_name": name,
            "reservation": format!("_{name}"),
            "database": "events_db",
            "database_id": "1",
            "cluster": "2",
            "table": "events",
            "creator": "sqlx_mig_00000000000040008000000000000001"
        });
        validate(&data).unwrap();
        for key in [
            "format",
            "job_name",
            "reservation",
            "database",
            "database_id",
            "cluster",
            "table",
            "creator",
        ] {
            let mut v = data.clone();
            v.as_object_mut().unwrap().remove(key);
            assert!(validate(&v).is_err());
        }
        let mut extra = data.clone();
        extra["database_url"] = "PRIVATE".into();
        assert!(validate(&extra).is_err());
        for (key, value) in [
            ("database_id", "0"),
            ("cluster", "-1"),
            ("table", "_sqlx_migrations"),
            ("reservation", "_other"),
            ("job_name", "sqlx_rl_00000000000010008000000000000001"),
        ] {
            let mut v = data.clone();
            v[key] = value.into();
            assert!(validate(&v).is_err(), "{key}");
        }
    }
}
