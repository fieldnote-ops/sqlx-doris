//! Observations of database-wide async producers and their retained task history.
use super::{Result, number};
use sqlx_core::executor::Executor;
use sqlx_core::row::Row;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_doris_core::DorisConnection;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Key {
    Load(u64),
    Routine(u64),
    MvScheduler(u64),
    MvTask(u64, u64),
}
impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Load(id) => write!(f, "LOAD job {id}"),
            Self::Routine(id) => write!(f, "ROUTINE LOAD job {id}"),
            Self::MvScheduler(id) => write!(f, "MV scheduler {id}"),
            Self::MvTask(job, task) => write!(f, "MV job {job} task {task}"),
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Work {
    identity: Vec<String>,
    state: String,
    blocked: bool,
}
impl Work {
    pub(super) fn identity_changed(&self, other: &Self) -> bool {
        self.identity != other.identity
    }
}
#[derive(Default)]
pub(super) struct Observer {
    observed: BTreeMap<Key, Vec<String>>,
}
impl Observer {
    pub(super) async fn snapshot(
        &mut self,
        c: &mut DorisConnection,
        database: &str,
        db_id: &str,
        observation: &crate::observation::Observation,
    ) -> Result<bool> {
        let current = records(c, database, db_id).await?;
        // SHOW LOAD can omit a legacy job whose metadata cannot be rendered.
        // Cross-check the unfiltered legacy pending/loading counters as well.
        let mut clear = !load_registry_active(c, db_id, observation).await?;
        for (key, work) in &current {
            let (kind, job_id, task_id) = match *key {
                Key::Load(id) => ("load", id, None),
                Key::Routine(id) => ("routine_load", id, None),
                Key::MvScheduler(id) => ("mv_scheduler", id, None),
                Key::MvTask(job, task) => ("mv_task", job, Some(task)),
            };
            observation.work(
                &key.to_string(),
                serde_json::json!({
                    "kind": kind,
                    "job_id": job_id,
                    "task_id": task_id,
                    "name_or_target": work.identity[0],
                    "state": work.state,
                    "blocked": work.blocked
                }),
            )?;
            if work.blocked {
                clear = false;
            }
        }
        for (key, identity) in &self.observed {
            let work = current.get(key).ok_or_else(|| format!("async {key} disappeared without an observed terminal/quiescent state; drain unconfirmed"))?;
            if &work.identity != identity {
                return Err(format!("async {key} identity changed; drain unconfirmed").into());
            }
        }
        self.observed = current
            .into_iter()
            .filter(|(_, w)| w.blocked)
            .map(|(key, w)| (key, w.identity))
            .collect();
        Ok(clear)
    }
}
async fn load_registry_active(
    c: &mut DorisConnection,
    db_id: &str,
    observation: &crate::observation::Observation,
) -> Result<bool> {
    let rows = c
        .fetch_all(AssertSqlSafe(format!("SHOW PROC '/jobs/{db_id}'")))
        .await?;
    let mut found = false;
    let mut active = false;
    for row in rows {
        if row.try_get::<&str, _>("JobType")? == "load" {
            if found {
                return Err("duplicate LOAD registry row; drain unconfirmed".into());
            }
            found = true;
            let pending = number(row.try_get("Pending")?)?;
            let running = number(row.try_get("Running")?)?;
            active = pending != 0 || running != 0;
            observation.work(
                "load_registry",
                serde_json::json!({
                    "kind": "load_registry",
                    "pending": pending,
                    "running": running,
                    "blocked": active
                }),
            )?;
        }
    }
    if !found {
        return Err("LOAD registry counters unavailable; drain unconfirmed".into());
    }
    Ok(active)
}
fn load_terminal(state: &str) -> Result<bool> {
    match state {
        "FINISHED" | "CANCELLED" => Ok(true),
        // A visible quorum is not a universal completion barrier. SHOW LOAD on
        // the verified FE normally normalizes QUORUM_FINISHED to FINISHED.
        "PENDING" | "ETL" | "LOADING" | "COMMITTED" | "QUORUM_FINISHED" => Ok(false),
        _ => Err(format!("unknown LOAD state {state}; drain unconfirmed").into()),
    }
}
pub(super) fn routine_terminal(state: &str) -> Result<bool> {
    match state {
        "STOPPED" | "CANCELLED" => Ok(true),
        // PAUSED can resume automatically and already dispatched tasks survive.
        "NEED_SCHEDULE" | "RUNNING" | "PAUSED" => Ok(false),
        _ => Err(format!("unknown ROUTINE LOAD state {state}; drain unconfirmed").into()),
    }
}
fn task_terminal(state: &str) -> Result<bool> {
    match state {
        "SUCCESS" | "FAILED" | "CANCELED" => Ok(true),
        "PENDING" | "RUNNING" => Ok(false),
        _ => Err(format!("unknown MV task state {state}; drain unconfirmed").into()),
    }
}
fn automatic_refresh(info: &str) -> Result<bool> {
    let parts: Vec<_> = info.split_whitespace().collect();
    if parts.len() < 6
        || parts[0] != "BUILD"
        || !matches!(parts[1], "IMMEDIATE" | "DEFERRED")
        || parts[2] != "REFRESH"
        || !matches!(parts[3], "AUTO" | "COMPLETE")
        || parts[4] != "ON"
    {
        return Err("unknown MV refresh policy; drain unconfirmed".into());
    }
    match parts[5] {
        "MANUAL" if parts.len() == 6 => Ok(false),
        "COMMIT" if parts.len() == 6 => Ok(true),
        "SCHEDULE" if parts.len() > 6 => Ok(true),
        _ => Err("unknown MV refresh trigger; drain unconfirmed".into()),
    }
}
fn scheduler_blocked(status: &str, automatic: bool) -> Result<bool> {
    match status {
        "RUNNING" => Ok(automatic),
        "PENDING" => Ok(true),
        // These statuses only describe a disabled scheduler NOW. Manual refresh
        // can bypass PAUSED, and restart/reconfiguration can reactivate it.
        "PAUSED" | "STOPPED" | "FINISHED" => Ok(false),
        _ => Err(format!("unknown MV scheduler state {status}; drain unconfirmed").into()),
    }
}
fn insert(
    rows: &mut BTreeMap<Key, Work>,
    key: Key,
    identity: Vec<String>,
    state: String,
    blocked: bool,
) -> Result<()> {
    if rows
        .insert(
            key,
            Work {
                identity,
                state,
                blocked,
            },
        )
        .is_some()
    {
        return Err("duplicate async job/task identity; drain unconfirmed".into());
    }
    Ok(())
}
async fn records(
    c: &mut DorisConnection,
    database: &str,
    db_id: &str,
) -> Result<BTreeMap<Key, Work>> {
    let mut result = BTreeMap::new();
    for row in c
        .fetch_all(AssertSqlSafe(format!("SHOW LOAD FROM `{database}`")))
        .await?
    {
        let id = number(row.try_get("JobId")?)?;
        let state: String = row.try_get("State")?;
        let blocked = !load_terminal(&state)?;
        insert(
            &mut result,
            Key::Load(id),
            vec![
                row.try_get("Label")?,
                row.try_get("CreateTime")?,
                row.try_get("Type")?,
            ],
            state,
            blocked,
        )?;
    }
    // ALL is essential: stopped/cancelled jobs must remain observable by exact ID.
    // This command reads the current database, verified by the outer observer.
    for row in c.fetch_all("SHOW ALL ROUTINE LOAD").await? {
        if row.try_get::<&str, _>("DbName")? != database {
            return Err("ROUTINE LOAD database mismatch; drain unconfirmed".into());
        }
        let id = number(row.try_get("Id")?)?;
        let state: String = row.try_get("State")?;
        let blocked = !routine_terminal(&state)?;
        insert(
            &mut result,
            Key::Routine(id),
            vec![
                row.try_get("Name")?,
                row.try_get("CreateTime")?,
                row.try_get("TableName")?,
            ],
            state,
            blocked,
        )?;
    }
    let mut views = BTreeMap::new();
    for row in c
        .fetch_all(AssertSqlSafe(format!(
            "SELECT Id,Name,JobName,RefreshInfo FROM mv_infos('database'='{database}')"
        )))
        .await?
    {
        let id: i64 = row.try_get("Id")?;
        if id <= 0 {
            return Err("invalid MV ID; drain unconfirmed".into());
        }
        let name: String = row.try_get("Name")?;
        let job: String = row.try_get("JobName")?;
        if job != format!("inner_mtmv_{id}")
            || views
                .insert(
                    id as u64,
                    (name, job, row.try_get::<String, _>("RefreshInfo")?),
                )
                .is_some()
        {
            return Err("unknown/duplicate MV job identity; drain unconfirmed".into());
        }
    }
    let mut schedulers = BTreeMap::new();
    for row in c.fetch_all(AssertSqlSafe(format!("SELECT Id,Name,MvId,MvName,ExecuteType,Status,CreateTime FROM jobs('type'='mv') WHERE MvDatabaseId='{db_id}'"))).await? {
        let id = number(row.try_get("Id")?)?;
        let mv_id = number(row.try_get("MvId")?)?;
        let Some((view, job, policy)) = views.get(&id) else { return Err("MV scheduler has no matching view; drain unconfirmed".into()); };
        if id != mv_id || row.try_get::<&str, _>("Name")? != job || row.try_get::<&str, _>("MvName")? != view {
            return Err("MV scheduler generation mismatch; drain unconfirmed".into());
        }
        let automatic = automatic_refresh(policy)?;
        let expected = if policy.split_whitespace().nth(5) == Some("SCHEDULE") { "RECURRING" } else { "MANUAL" };
        if row.try_get::<&str, _>("ExecuteType")? != expected {
            return Err("MV refresh policy/scheduler mismatch; drain unconfirmed".into());
        }
        let state: String = row.try_get("Status")?;
        let blocked = scheduler_blocked(&state, automatic)?;
        let identity = vec![view.clone(), job.clone(), row.try_get("CreateTime")?];
        insert(&mut result, Key::MvScheduler(id), identity, state, blocked)?;
        if schedulers.insert(id, ()).is_some() { return Err("duplicate MV scheduler; drain unconfirmed".into()); }
    }
    if views.len() != schedulers.len() {
        return Err("MV view has no matching scheduler; drain unconfirmed".into());
    }
    for row in c.fetch_all(AssertSqlSafe(format!("SELECT TaskId,JobId,JobName,MvId,MvName,Status,CreateTime FROM tasks('type'='mv') WHERE MvDatabaseId='{db_id}'"))).await? {
        let job_id = number(row.try_get("JobId")?)?;
        let id = number(row.try_get("TaskId")?)?;
        let Some((view, job, _)) = views.get(&job_id) else { return Err("MV task parent is unavailable; drain unconfirmed".into()); };
        if number(row.try_get("MvId")?)? != job_id || row.try_get::<&str, _>("MvName")? != view || row.try_get::<&str, _>("JobName")? != job {
            return Err("MV task generation mismatch; drain unconfirmed".into());
        }
        let state: String = row.try_get("Status")?;
        let blocked = !task_terminal(&state)?;
        insert(&mut result, Key::MvTask(job_id, id), vec![view.clone(), job.clone(), row.try_get("CreateTime")?], state, blocked)?;
    }
    Ok(result)
}

pub(super) async fn cancellation(
    c: &mut DorisConnection,
    database: &str,
    db_id: &str,
    view: &str,
    job: u64,
    task: u64,
) -> Result<(String, Work)> {
    let rows = records(c, database, db_id).await?;
    let work = rows
        .get(&Key::MvTask(job, task))
        .ok_or("exact MV job/task not found; no CANCEL sent")?;
    if work.identity[0] != view || !work.blocked {
        return Err("MV target mismatch or task already terminal; no CANCEL sent; inspect rather than replay".into());
    }
    // The general public CANCEL TASK command accepts the immutable server-
    // generated parent name. Do not resolve the reusable view name at execution.
    Ok((
        format!("CANCEL TASK WHERE jobName='inner_mtmv_{job}' AND taskId={task}"),
        work.clone(),
    ))
}
pub(super) async fn cancelled(
    c: &mut DorisConnection,
    database: &str,
    db_id: &str,
    job: u64,
    task: u64,
    expected: &Work,
) -> Result<Option<String>> {
    let rows = records(c, database, db_id).await?;
    let work = rows
        .get(&Key::MvTask(job, task))
        .ok_or("MV task disappeared; terminal state unconfirmed")?;
    if work.identity != expected.identity {
        return Err("MV task identity changed; terminal state unconfirmed".into());
    }
    Ok((!work.blocked).then(|| work.state.clone()))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resumable_producers_and_unpublished_work_are_not_terminal() {
        assert!(!routine_terminal("PAUSED").unwrap());
        assert!(!load_terminal("COMMITTED").unwrap());
        assert!(!load_terminal("QUORUM_FINISHED").unwrap());
        assert!(
            scheduler_blocked(
                "RUNNING",
                automatic_refresh("BUILD DEFERRED REFRESH AUTO ON COMMIT").unwrap()
            )
            .unwrap()
        );
        assert!(
            !scheduler_blocked(
                "RUNNING",
                automatic_refresh("BUILD IMMEDIATE REFRESH COMPLETE ON MANUAL").unwrap()
            )
            .unwrap()
        );
        assert!(routine_terminal("STOPPED").unwrap());
        assert!(task_terminal("FAILED").unwrap());
        assert!(load_terminal("UNKNOWN").is_err());
        assert!(routine_terminal("UNKNOWN").is_err());
        assert!(task_terminal("UNKNOWN").is_err());
        assert!(scheduler_blocked("UNKNOWN", false).is_err());
        assert!(automatic_refresh("BUILD DEFERRED REFRESH AUTO ON SOMETHING").is_err());
    }
}
