//! Scoped drain observations and explicit job cancellation; never a write fence.
use super::{Context, Result, identity};
use sqlx_core::connection::Connection;
use sqlx_core::executor::Executor;
use sqlx_core::row::Row;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_doris_core::DorisConnection;
use std::{collections::BTreeMap, time::Duration};
#[path = "drain/jobs.rs"]
mod jobs;
#[path = "drain/load_stop.rs"]
mod load_stop;
#[path = "drain/routine.rs"]
pub mod routine;

#[derive(Clone)]
pub enum Action {
    /// Preview/cancel one BROKER LOAD through its exact registered transaction.
    /// Never sends label-based CANCEL; unregistered LOAD and other job types fail closed.
    CancelLoad {
        /// Retired managed identity; independently verify its attribution to this job.
        name: String,
        /// Exact LOAD JobId from drain status or a previous preview.
        job_id: u64,
        /// Exact TransactionId shown by preview; required when executing.
        transaction_id: Option<u64>,
        /// FE HTTP(S) origin of this SQL cluster. Redirects must identify a registered BE.
        http_url: Option<String>,
        /// Confirm exclusive maintenance, job attribution and stable trusted
        /// cluster/endpoints. Never rebind/restore/reuse metadata during this operation.
        maintenance: bool,
        /// Submit one exact transaction abort after rechecking. Never automatically retry.
        force: bool,
    },
    /// Read one snapshot. Exit nonzero if blocked; never changes locks or jobs.
    Status { name: String },
    /// Wait for terminal work and inactive automatic producers in the documented scope.
    /// Stop old actors/admission and reconcile work outside the documented scope.
    Wait {
        name: String,
        /// Overall deadline, including metadata reads (seconds).
        timeout: u64,
        /// Attest exclusive maintenance, retired-name provenance, stopped old
        /// actors/admission and reconciliation of other async job classes.
        maintenance: bool,
    },
    /// Preview cancellation of one exact ROLLUP JobId. COLUMN ignores JobId in
    /// Doris 4.1.5 and cannot be safely cancelled by this CLI.
    CancelRollup {
        name: String,

        table: String,

        job_id: u64,
        /// Confirm exclusive maintenance and attribution of this exact job.
        maintenance: bool,
        /// Send one CANCEL; never replay it after error or cancellation.
        force: bool,
    },
    /// Preview/cancel one MV task by exact parent JobId and TaskId. Does not
    /// disable automatic refresh or prove that the whole database is drained.
    CancelMvTask {
        name: String,

        view: String,

        job_id: u64,

        task_id: u64,
        /// Confirm exclusive maintenance, exact task attribution, no reuse of
        /// server-generated job names, and stopped refresh/DDL/admission actors.
        maintenance: bool,

        force: bool,
    },
}

impl Action {
    fn validate(&self) -> Result<()> {
        let maximum = i64::MAX as u64;
        let positive = |id: u64| id > 0 && id <= maximum;
        let invalid = match self {
            Self::Status { .. } => false,
            Self::Wait {
                timeout,
                maintenance,
                ..
            } => !maintenance || !(1..=3600).contains(timeout),
            Self::CancelLoad {
                job_id,
                transaction_id,
                http_url,
                maintenance,
                force,
                ..
            } => {
                !positive(*job_id)
                    || transaction_id.is_some_and(|id| !positive(id))
                    || (*force && (!maintenance || transaction_id.is_none() || http_url.is_none()))
            }
            Self::CancelRollup {
                job_id,
                maintenance,
                force,
                ..
            } => !positive(*job_id) || (*force && !maintenance),
            Self::CancelMvTask {
                job_id,
                task_id,
                maintenance,
                force,
                ..
            } => !positive(*job_id) || !positive(*task_id) || (*force && !maintenance),
        };
        if invalid {
            return Err(crate::failure::fault(
                2,
                "explicit maintenance and valid exact work identities are required",
            ));
        }
        Ok(())
    }
}

fn transaction_terminal(state: &str) -> Result<bool> {
    match state {
        "VISIBLE" | "ABORTED" => Ok(true),
        "PREPARE" | "PRECOMMITTED" | "COMMITTED" => Ok(false),
        _ => Err(format!("unknown transaction state {state}; drain unconfirmed").into()),
    }
}
fn job_terminal(state: &str) -> Result<bool> {
    match state {
        "FINISHED" | "CANCELLED" => Ok(true),
        "PENDING" | "WAITING_TXN" | "RUNNING" => Ok(false),
        _ => Err(format!("unknown schema job state {state}; drain unconfirmed").into()),
    }
}
fn number(value: &str) -> Result<u64> {
    value
        .parse()
        .map_err(|_| "invalid numeric server metadata; drain unconfirmed".into())
}

struct Observer {
    observation: crate::observation::Observation,
    database: String,
    cluster: String,
    db_id: String,
    // Transaction metadata does not identify its user. Block on ALL database
    // transactions. Keep observations until their exact terminal state is seen;
    // disappearing/garbage-collected history is not terminal-state evidence.
    transactions: BTreeMap<u64, ()>,
    jobs: BTreeMap<(&'static str, u64), String>,
    async_work: jobs::Observer,
}
impl Observer {
    async fn new(
        c: &mut DorisConnection,
        name: Option<&str>,
        observation: &crate::observation::Observation,
    ) -> Result<Self> {
        if let Some(name) = name {
            identity::identity_id(name)?;
        }
        let (database, cluster) = identity::environment(c).await?;
        let db_id = database_id(c, &database).await?;

        for key in ["deploy_mode", "cloud_unique_id"] {
            let mode = c
                .fetch_all(AssertSqlSafe(format!("SHOW FRONTEND CONFIG LIKE '{key}'")))
                .await?;
            if mode.len() != 1
                || !matches!(
                    (key, mode[0].try_get::<&str, _>("Value")?),
                    ("deploy_mode", "" | "local") | ("cloud_unique_id", "")
                )
            {
                return Err("async job drain is verified only for shared-nothing deployment (default/local, no cloud_unique_id); drain unconfirmed".into());
            }
        }
        Ok(Self {
            database,
            cluster,
            db_id,
            transactions: BTreeMap::new(),
            jobs: BTreeMap::new(),
            async_work: jobs::Observer::default(),
            observation: observation.clone(),
        })
    }
    async fn retired(&self, c: &mut DorisConnection, name: Option<&str>) -> Result<()> {
        if identity::environment(c).await? != (self.database.clone(), self.cluster.clone())
            || database_id(c, &self.database).await? != self.db_id
        {
            return Err("database/environment changed; drain unconfirmed".into());
        }
        if let Some(name) = name
            && identity::inspect(c, name, &self.database, &self.cluster).await?
        {
            return Err("identity is still present; retire it explicitly before draining".into());
        }
        Ok(())
    }
    async fn running_count(&self, c: &mut DorisConnection) -> Result<u64> {
        let rows = c
            .fetch_all(AssertSqlSafe(format!(
                "SHOW PROC '/transactions/{}'",
                self.db_id
            )))
            .await?;
        if rows.len() != 2 {
            return Err("unknown transaction summary layout; drain unconfirmed".into());
        }
        let mut count = None;
        for row in rows {
            match row.try_get::<&str, _>("State")? {
                "running" if count.is_none() => count = Some(number(row.try_get("Number")?)?),
                "finished" => {
                    number(row.try_get("Number")?)?;
                }
                _ => return Err("unknown transaction summary state; drain unconfirmed".into()),
            }
        }
        count.ok_or_else(|| "missing running transaction count; drain unconfirmed".into())
    }
    async fn snapshot(&mut self, c: &mut DorisConnection, name: Option<&str>) -> Result<bool> {
        self.observation.data("work", serde_json::json!({}));
        self.retired(c, name).await?;
        let mut blocked = false;
        // No WHERE/LIMIT: include idle sessions and sessions using another DB.
        // Do not KILL FE-local, reusable numeric connection IDs.
        for row in c.fetch_all("SHOW FULL PROCESSLIST").await? {
            if Some(row.try_get::<&str, _>("User")?) == name {
                self.observation.work(
                    &format!("session:{}", row.try_get::<&str, _>("Id")?),
                    serde_json::json!({
                        "kind": "session",
                        "id": row.try_get::<&str, _>("Id")?,
                        "command": row.try_get::<&str, _>("Command")?,
                        "blocked": true
                    }),
                )?;

                blocked = true;
            }
        }
        let before = self.running_count(c).await?;
        let running = c
            .fetch_all(AssertSqlSafe(format!(
                "SHOW PROC '/transactions/{}/running'",
                self.db_id
            )))
            .await?;
        // The verified FE caps this listing at 2000. A full page cannot prove
        // completeness. Nonzero summaries block even if the page races a commit.
        if running.len() >= 2000 {
            return Err(
                "transaction listing reached 2000-row server limit; drain unconfirmed".into(),
            );
        }
        for row in &running {
            let id = number(row.try_get("TransactionId")?)?;
            let state = row.try_get::<&str, _>("TransactionStatus")?;
            if !transaction_terminal(state)? {
                self.transactions.insert(id, ());
            }
            self.observation.work(
                &format!("transaction:{id}"),
                serde_json::json!({
                    "kind": "transaction",
                    "id": id,
                    "state": state,
                    "blocked": !transaction_terminal(state)?,
                    "owner_attributed": false
                }),
            )?;
        }
        let after = self.running_count(c).await?;
        blocked |= before != 0 || after != 0 || !running.is_empty();
        for id in self.transactions.keys().copied().collect::<Vec<_>>() {
            let rows = c
                .fetch_all(AssertSqlSafe(format!(
                    "SHOW TRANSACTION FROM `{}` WHERE id = {id}",
                    self.database
                )))
                .await?;
            if rows.len() != 1 || number(rows[0].try_get("TransactionId")?)? != id {
                return Err(format!("transaction {id} disappeared without an observed terminal state; drain unconfirmed").into());
            }
            let state = rows[0].try_get::<&str, _>("TransactionStatus")?;
            if transaction_terminal(state)? {
                self.observation.work(
                    &format!("transaction:{id}"),
                    serde_json::json!({
                        "kind": "transaction", "id": id, "state": state,
                        "blocked": false, "owner_attributed": false
                    }),
                )?;
                self.transactions.remove(&id);
            } else {
                blocked = true;
            }
        }
        for kind in ["COLUMN", "ROLLUP"] {
            let jobs = schema_jobs(c, &self.database, kind).await?;
            for (id, (table, state)) in &jobs {
                if !job_terminal(state)? {
                    self.observation.work(
                        &format!("{kind}:{id}"),
                        serde_json::json!({
                            "kind": kind,
                            "id": id,
                            "table": table,
                            "state": state,
                            "blocked": true
                        }),
                    )?;

                    self.jobs.insert((kind, *id), table.clone());
                    blocked = true;
                }
            }
            for ((_, id), table) in self
                .jobs
                .clone()
                .into_iter()
                .filter(|((k, _), _)| *k == kind)
            {
                let Some((observed_table, state)) = jobs.get(&id) else {
                    return Err(format!("{kind} job {id} disappeared without an observed terminal state; drain unconfirmed").into());
                };
                if observed_table != &table {
                    return Err("schema job target changed; drain unconfirmed".into());
                }
                if job_terminal(state)? {
                    self.observation.work(
                        &format!("{kind}:{id}"),
                        serde_json::json!({
                            "kind": kind, "id": id, "state": state, "table": table,
                            "blocked": false
                        }),
                    )?;
                    self.jobs.remove(&(kind, id));
                }
            }
        }
        blocked |= !self
            .async_work
            .snapshot(c, &self.database, &self.db_id, &self.observation)
            .await?;
        // Read transactions again AFTER producers/tasks: a reported job terminal
        // state is not itself proof that dispatched work stopped publishing.
        blocked |= self.running_count(c).await? != 0;
        // Detect recreation/regrant or topology drift during metadata reads.
        self.retired(c, name).await?;

        self.observation.data(
            "drain",
            serde_json::json!({
                "database": self.database,
                "database_id": self.db_id,
                "cluster": self.cluster,
                "identity": name,
                "identity_sessions_attested": name.is_some(),
                "scoped_empty": !blocked,
                "atomic_fence": false,
                "uncovered_work_requires_reconciliation": true
            }),
        );
        Ok(!blocked)
    }
}
async fn database_id(c: &mut DorisConnection, database: &str) -> Result<String> {
    let rows = c.fetch_all("SHOW PROC '/dbs'").await?;
    let ids = rows
        .iter()
        .filter_map(|r| match r.try_get::<&str, _>("DbName") {
            Ok(name) if name == database => Some(r.try_get::<String, _>("DbId")),
            _ => None,
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if ids.len() != 1 || number(&ids[0])? == 0 {
        return Err("database ID is unavailable/ambiguous; drain unconfirmed".into());
    }
    Ok(ids[0].clone())
}
async fn schema_jobs(
    c: &mut DorisConnection,
    database: &str,
    kind: &str,
) -> Result<BTreeMap<u64, (String, String)>> {
    let mut jobs = BTreeMap::new();
    for row in c
        .fetch_all(AssertSqlSafe(format!(
            "SHOW ALTER TABLE {kind} FROM `{database}`"
        )))
        .await?
    {
        let id = number(row.try_get("JobId")?)?;
        let table: String = row.try_get("TableName")?;
        let state: String = row.try_get("State")?;
        job_terminal(&state)?;
        // COLUMN may have one row per index. Conflicting duplicates are unknown.
        if let Some(previous) = jobs.insert(id, (table.clone(), state.clone()))
            && previous != (table, state)
        {
            return Err("conflicting schema job rows; drain unconfirmed".into());
        }
    }
    Ok(jobs)
}

pub(crate) async fn diagnostic(
    c: &mut DorisConnection,
    name: Option<&str>,
    observation: &crate::observation::Observation,
) -> Result<bool> {
    let mut observer = Observer::new(c, name, observation).await?;
    if let Some(name) = name
        && identity::inspect(c, name, &observer.database, &observer.cluster).await?
    {
        return Err(crate::failure::coded(
            3,
            "identity_present",
            "managed identity still present; old access not isolated",
        ));
    }
    observer.snapshot(c, name).await
}
pub async fn guard(
    c: &mut DorisConnection,
    name: Option<&str>,
    seconds: u64,
    observation: &crate::observation::Observation,
) -> Result<()> {
    let result: Result<()> = tokio::time::timeout(Duration::from_secs(seconds), async {
        let mut observer = Observer::new(c, name, observation).await?;
        if !observer.snapshot(c, name).await? {
            return Err(crate::failure::fault(
                3,
                "drain blocked; no recovery DROP sent; keep maintenance active and inspect/wait",
            ));
        }

        Ok(())
    })
    .await
    .map_err(|_| "drain check timed out; no recovery DROP sent; connection must not be reused")?;
    result.map_err(|e| {
        let code = e
            .downcast_ref::<crate::failure::Failure>()
            .map_or(7, |e| e.code());
        crate::failure::wrap(code, "drain preflight failed; no recovery DROP sent", e)
    })
}

pub async fn run(args: &Context, action: &Action) -> Result<()> {
    action.validate()?;
    if args.identity_file.is_some() {
        return Err("drain requires an independent administrator URL; omit --identity-file".into());
    }
    let (Action::Status { name }
    | Action::Wait { name, .. }
    | Action::CancelRollup { name, .. }
    | Action::CancelMvTask { name, .. }
    | Action::CancelLoad { name, .. }) = action;
    identity::identity_id(name)
        .map_err(|_| crate::failure::fault(2, "invalid managed identity"))?;
    let seconds = match action {
        Action::Wait { timeout, .. } => *timeout,
        _ => args.ddl_timeout,
    };
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(seconds))
        .ok_or("drain timeout is too large")?;
    let mut c = tokio::time::timeout_at(deadline, args.connect())
        .await
        .map_err(
            |_| "drain deadline expired during connection; no CANCEL or recovery DROP sent",
        )??;
    let mut cancel_phase = "no CANCEL/ABORT sent";
    let result = tokio::time::timeout_at(
        deadline,
        perform(args, action, name, &mut c, &mut cancel_phase),
    )
    .await;
    // Dropping on error/timeout avoids reusing a connection with a pending read
    // or CANCEL. No background reconnect or replay, no lock operations here.
    match result {
        Ok(Ok(())) => { c.close().await?; Ok(()) },
        Ok(Err(error)) => Err(error),
        Err(_) => Err(format!("drain deadline expired; {cancel_phase}; no lock recovery performed; inspect using a new connection, do not blindly retry a submitted CANCEL").into()),
    }
}

async fn perform(
    args: &Context,
    action: &Action,
    name: &str,
    c: &mut DorisConnection,
    cancel_phase: &mut &'static str,
) -> Result<()> {
    let mut observer = Observer::new(c, Some(name), &args.observation).await?;
    if matches!(action, Action::CancelLoad { .. }) {
        return load_stop::run(args, c, &observer, action, cancel_phase).await;
    }
    if let Action::CancelMvTask {
        view,
        job_id,
        task_id,
        force,
        ..
    } = action
    {
        identity::valid_database(view)?;
        observer.retired(c, Some(name)).await?;
        let (sql, expected) = jobs::cancellation(
            c,
            &observer.database,
            &observer.db_id,
            view,
            *job_id,
            *task_id,
        )
        .await?;
        args.observation.data(
            "target",
            serde_json::json!({
                "kind": "mv_task",
                "job_id": job_id,
                "task_id": task_id,
                "view": view,
                "preview": !force
            }),
        );

        if !force {
            return Ok(());
        }
        observer.retired(c, Some(name)).await?;
        let (fresh_sql, fresh) = jobs::cancellation(
            c,
            &observer.database,
            &observer.db_id,
            view,
            *job_id,
            *task_id,
        )
        .await?;
        if fresh_sql != sql || fresh.identity_changed(&expected) {
            return Err("MV task changed before cancellation; no CANCEL sent".into());
        }
        *cancel_phase = "MV CANCEL submitted; outcome uncertain";
        args.observation
            .phase(crate::observation::Phase::Submitted, "cancel_mv_task");
        c.execute(AssertSqlSafe(sql)).await
                .map_err(|_| "MV CANCEL outcome is uncertain; inspect exact job/task on a new connection; never blindly retry")?;
        *cancel_phase = "MV CANCEL acknowledged; terminal state unconfirmed";
        args.observation
            .phase(crate::observation::Phase::Acknowledged, "cancel_mv_task");
        loop {
            if let Some(state) = jobs::cancelled(
                c,
                &observer.database,
                &observer.db_id,
                *job_id,
                *task_id,
                &expected,
            )
            .await
            .map_err(|e| format!("MV CANCEL acknowledged; terminal verification failed: {e}"))?
            {
                args.observation.data(
                    "completion",
                    serde_json::json!({
                        "kind": "mv_task", "job_id": job_id, "task_id": task_id,
                        "state": state
                    }),
                );
                args.observation
                    .phase(crate::observation::Phase::Verified, "cancel_mv_task");

                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    if let Action::CancelRollup {
        table,
        job_id,
        force,
        ..
    } = action
    {
        identity::valid_database(table)?;
        observer.retired(c, Some(name)).await?;
        let jobs = schema_jobs(c, &observer.database, "ROLLUP").await?;
        let (observed_table, state) = jobs
            .get(job_id)
            .ok_or("exact ROLLUP job not found; no CANCEL sent")?;
        if observed_table != table || job_terminal(state)? {
            return Err("ROLLUP target mismatch or already terminal; no CANCEL sent; inspect rather than replay".into());
        }
        args.observation.data(
            "target",
            serde_json::json!({
                "kind": "rollup",
                "job_id": job_id,
                "table": table,
                "state": state,
                "preview": !force
            }),
        );

        if !force {
            return Ok(());
        }
        observer.retired(c, Some(name)).await?;
        // ROLLUP uses an exact global JobId; unlike COLUMN, it cannot select
        // a later table job when this ID has completed. No retries.
        *cancel_phase = "CANCEL submitted; outcome uncertain";
        args.observation
            .phase(crate::observation::Phase::Submitted, "cancel_rollup");
        c.execute(AssertSqlSafe(format!("CANCEL ALTER TABLE ROLLUP FROM `{}`.`{table}` ({job_id})", observer.database))).await
                .map_err(|_| "CANCEL outcome is uncertain; inspect exact job on a new connection; never blindly retry")?;
        *cancel_phase = "CANCEL acknowledged; terminal state unconfirmed";
        args.observation
            .phase(crate::observation::Phase::Acknowledged, "cancel_rollup");
        loop {
            let jobs = schema_jobs(c, &observer.database, "ROLLUP").await
                    .map_err(|_| "CANCEL acknowledged; terminal verification failed; inspect exact job on a new connection")?;
            let (current_table, state) = jobs
                .get(job_id)
                .ok_or("CANCEL acknowledged but job disappeared; terminal state unconfirmed")?;
            if current_table != table {
                return Err(
                    "CANCEL acknowledged but target changed; terminal state unconfirmed".into(),
                );
            }
            if job_terminal(state)? {
                args.observation.data(
                    "completion",
                    serde_json::json!({
                        "kind": "rollup", "job_id": job_id, "state": state
                    }),
                );
                args.observation
                    .phase(crate::observation::Phase::Verified, "cancel_rollup");

                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    loop {
        if observer.snapshot(c, Some(name)).await? {
            return Ok(());
        }
        if matches!(action, Action::Status { .. }) {
            return Err(crate::failure::fault(
                3,
                "drain blocked; locks/history unchanged",
            ));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direct_requests_require_explicit_maintenance_and_exact_ids() {
        let name = "sqlx_m_00000000000040008000000000000001".to_owned();
        for action in [
            Action::Wait {
                name: name.clone(),
                timeout: 1,
                maintenance: false,
            },
            Action::Wait {
                name: name.clone(),
                timeout: u64::MAX,
                maintenance: true,
            },
            Action::CancelRollup {
                name: name.clone(),
                table: "t".into(),
                job_id: 0,
                maintenance: true,
                force: true,
            },
            Action::CancelMvTask {
                name: name.clone(),
                view: "v".into(),
                job_id: 1,
                task_id: 2,
                maintenance: false,
                force: true,
            },
            Action::CancelLoad {
                name: name.clone(),
                job_id: 1,
                transaction_id: None,
                http_url: None,
                maintenance: true,
                force: true,
            },
        ] {
            let error = action.validate().unwrap_err();
            assert_eq!(
                error
                    .downcast_ref::<crate::failure::Failure>()
                    .unwrap()
                    .code(),
                2
            );
        }
        Action::CancelRollup {
            name,
            table: "t".into(),
            job_id: 1,
            maintenance: false,
            force: false,
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn committed_is_not_drained_and_unknown_states_fail_closed() {
        for state in ["PREPARE", "PRECOMMITTED", "COMMITTED"] {
            assert!(!transaction_terminal(state).unwrap());
        }
        for state in ["VISIBLE", "ABORTED"] {
            assert!(transaction_terminal(state).unwrap());
        }
        assert!(transaction_terminal("UNKNOWN").is_err());
        assert!(job_terminal("UNKNOWN").is_err());
        for state in ["PENDING", "WAITING_TXN", "RUNNING"] {
            assert!(!job_terminal(state).unwrap());
        }
        for state in ["FINISHED", "CANCELLED"] {
            assert!(job_terminal(state).unwrap());
        }
    }
}
