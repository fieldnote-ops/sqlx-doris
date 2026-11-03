//! Managed Routine Load names are reserved forever, not an exact-ID STOP API.
use super::{Context, Observer, Result, identity, number};
use sqlx_core::connection::ConnectOptions;
use sqlx_core::connection::Connection;
use sqlx_core::executor::Executor;
use sqlx_core::row::Row;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_doris_core::{DorisConnectOptions, DorisConnection};
use std::{
    io::Read,
    path::{Path, PathBuf},
    time::Duration,
};

#[path = "routine/receipt.rs"]
mod receipt_input;
#[path = "routine/report.rs"]
mod report;
use crate::failure::fault;
use receipt_input::Located;
use report::{Phase, Report};

#[derive(Clone)]
pub struct Locator {
    pub job_name: Option<String>,
    pub receipt: Option<PathBuf>,
}

const FORMAT: &str = "sqlx-doris-routine-v1";
const RECEIPT_MAX_BYTES: usize = 1_048_576;
const PREFIX: &str = "sqlx_rl_";

#[derive(Clone)]
pub enum Action {
    /// Create one job with a fresh UUID and permanent server reservation view.
    /// Never accepts a caller-chosen name or replays an uncertain CREATE.
    Create {
        /// Private credentials from identity create; the administrator URL stays separate.
        runner_identity_file: PathBuf,
        /// Current-database single target table (ASCII identifier).
        table: String,
        /// Native SQL suffix after ON TABLE, including FROM KAFKA. No semicolons/NUL.
        source: PathBuf,
        /// New private external receipt, persisted before any server mutation.
        output: PathBuf,
        /// Reserve sqlx_rl_ jobs and _sqlx_rl_ views exclusively for this protocol.
        /// All creators must use this CLI; preserve reservations/names forever.
        managed_namespace: bool,
    },
    /// Inspect the permanent reservation and exact retained job history. No mutation.
    Status { locator: Locator },
    /// Preview/stop a managed UUID job after its creator identity is retired.
    /// Safety requires the no-reuse protocol; Doris still resolves STOP by name.
    Stop {
        locator: Locator,
        /// Exact Id from routine status; checked again before submitting STOP.
        job_id: Option<u64>,
        /// Stop old actors/admission, serialize administration and keep the cluster
        /// and metadata stable. No restoration, namespace bypass or name reuse.
        maintenance: bool,
        /// Confirm every creator uses this protocol and all reservations stay intact.
        managed_namespace: bool,
        /// Submit one STOP. Never retry after timeout, disconnect or cancellation.
        force: bool,
    },
}
impl Action {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Create {
                managed_namespace: false,
                ..
            } => Err(fault(
                2,
                "creation requires exclusive managed-namespace attestation",
            )),
            Self::Stop {
                job_id,
                maintenance,
                managed_namespace,
                force,
                ..
            } => {
                if job_id.is_some_and(|id| id == 0 || id > i64::MAX as u64)
                    || (*force && (!maintenance || !managed_namespace || job_id.is_none()))
                {
                    return Err(fault(
                        2,
                        "STOP requires an exact positive JobId, maintenance and managed-namespace attestation",
                    ));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

fn identifier(value: &str) -> Result<()> {
    identity::valid_database(value).map_err(|_| fault(2, "invalid target identifier"))?;
    if value.starts_with(PREFIX) || value.starts_with("_sqlx_") {
        return Err(fault(2, "target must not be a protocol metadata object"));
    }
    Ok(())
}
fn uuid_name(name: &str) -> Result<uuid::Uuid> {
    let token = name
        .strip_prefix(PREFIX)
        .ok_or("not a managed Routine Load name; no adoption or name fallback")?;
    let id = uuid::Uuid::parse_str(token).map_err(|_| "invalid Routine Load UUID")?;
    if token != id.simple().to_string()
        || id.get_version_num() != 4
        || id.get_variant() != uuid::Variant::RFC4122
    {
        return Err("Routine Load name must be a canonical generated v4 UUID".into());
    }
    Ok(id)
}
fn marker_name(name: &str) -> Result<String> {
    uuid_name(name)?;
    Ok(format!("_{name}"))
}
fn suffix(path: &Path) -> Result<String> {
    let mut value = String::new();
    std::fs::File::open(path)
        .map_err(|_| fault(2, "Routine SQL suffix cannot be opened"))?
        .take(65_537)
        .read_to_string(&mut value)
        .map_err(|_| fault(2, "Routine SQL suffix must be readable UTF-8"))?;
    if value.trim().is_empty() || value.len() > 65_536 || value.contains([';', '\0']) {
        return Err(fault(
            2,
            "Routine SQL suffix must be nonempty UTF-8, at most 64 KiB, without any semicolon or NUL",
        ));
    }
    Ok(value)
}
fn receipt(path: &Path, value: &serde_json::Value, files: &identity::PrivateFiles) -> Result<()> {
    let serialized = serde_json::to_vec_pretty(value)?;
    if serialized.len() > RECEIPT_MAX_BYTES {
        return Err("receipt exceeds 1 MiB; confirmation not persisted".into());
    }
    files.write_new(path, &serialized)
}

#[derive(Clone, Debug, PartialEq)]
struct Reservation {
    name: String,
    creator: String,
    table: String,
    token: String,
}
async fn reservation(
    c: &mut DorisConnection,
    o: &Observer,
    name: &str,
) -> Result<Option<Reservation>> {
    let marker = marker_name(name)?;
    let kind: Option<String> = sqlx_core::query_scalar::query_scalar(
        "SELECT TABLE_TYPE FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
    )
    .bind(&o.database)
    .bind(&marker)
    .fetch_optional(&mut *c)
    .await?;
    if kind.is_none() {
        return Ok(None);
    }
    if kind.as_deref() != Some("VIEW") {
        return Err(fault(
            3,
            "unknown permanent reservation object; no STOP sent",
        ));
    }
    let rows = c
        .fetch_all(AssertSqlSafe(format!(
            "SELECT token FROM `{}`.`{marker}`",
            o.database
        )))
        .await?;
    if rows.len() != 1 {
        return Err(fault(3, "ambiguous Routine reservation; no STOP sent"));
    }
    let token: String = rows[0].try_get("token")?;
    let fields: Vec<_> = token.split(':').collect();
    if fields.len() != 6
        || fields[0] != FORMAT
        || fields[1] != o.cluster
        || fields[2] != o.db_id
        || fields[5] != name
    {
        return Err(fault(
            3,
            "Routine reservation format/cluster/database/name mismatch; no STOP sent",
        ));
    }
    identifier(fields[3])?;
    identity::identity_id(fields[4])?;
    Ok(Some(Reservation {
        name: name.into(),
        table: fields[3].into(),
        creator: fields[4].into(),
        token,
    }))
}
#[derive(Clone, Debug, PartialEq)]
struct Job {
    id: u64,
    created: String,
    state: String,
    progress: serde_json::Value,
}
fn progress(raw: &str) -> Result<serde_json::Value> {
    let value: serde_json::Value =
        serde_json::from_str(raw).map_err(|_| fault(3, "unknown Routine progress metadata"))?;
    let fields = value
        .as_object()
        .ok_or_else(|| fault(3, "Routine progress must be an object"))?;
    for (key, v) in fields {
        if key.parse::<u32>().is_err()
            || !v.as_str().is_some_and(|s| {
                s.parse::<i64>().is_ok_and(|n| n >= 0)
                    || matches!(s, "OFFSET_END" | "OFFSET_ZERO" | "OFFSET_BEGINNING")
            })
        {
            return Err(fault(3, "unknown Routine partition/offset metadata"));
        }
    }
    Ok(value)
}
async fn inspect(c: &mut DorisConnection, o: &Observer, r: &Reservation) -> Result<Option<Job>> {
    // Unfiltered current-database list: empty is an observation, not a guessed
    // "job never existed" conclusion or a swallowed SHOW error.
    let rows = c.fetch_all("SHOW ALL ROUTINE LOAD").await?;
    let mut matching = Vec::new();
    for row in &rows {
        if row.try_get::<&str, _>("Name")? == r.name {
            matching.push(row);
        }
    }
    if matching.is_empty() {
        return Ok(None);
    }
    if matching.len() != 1 {
        return Err(fault(
            3,
            "Routine history absent or name reused; no STOP sent; never choose a successor",
        ));
    }
    let row = matching[0];
    if row.try_get::<&str, _>("DbName")? != o.database
        || row.try_get::<&str, _>("TableName")? != r.table
        || row.try_get::<&str, _>("User")? != r.creator
        || row.try_get::<&str, _>("IsMultiTable")? != "false"
        || row.try_get::<&str, _>("DataSourceType")? != "KAFKA"
    {
        return Err(fault(
            3,
            "Routine job creator/target/source binding mismatch; no STOP sent",
        ));
    }
    let job = Job {
        id: number(row.try_get("Id")?)?,
        created: row.try_get("CreateTime")?,
        state: row.try_get("State")?,
        progress: progress(row.try_get("Progress")?)?,
    };
    if job.id == 0 {
        return Err(fault(3, "invalid Routine job ID"));
    }
    super::jobs::routine_terminal(&job.state)?;
    Ok(Some(job))
}
fn observed(report: &mut Report, job: Job) {
    report.observation = if matches!(job.state.as_str(), "STOPPED" | "CANCELLED") {
        "terminal"
    } else {
        "active"
    };
    report.job_observed_phase = Some(report.phase);
    report.job = Some(job);
}
async fn create(
    args: &Context,
    c: &mut DorisConnection,
    o: &Observer,
    action: &Action,
    report: &mut Report,
) -> Result<()> {
    let Action::Create {
        runner_identity_file,
        table,
        source,
        output,
        ..
    } = action
    else {
        unreachable!()
    };
    // Every field embedded in the literal reservation token is constrained.
    if number(&o.cluster)? == 0 {
        return Err("invalid cluster identity; no reservation or job CREATE sent".into());
    }
    identifier(table)?;
    let body = suffix(source)?;
    let files = identity::PrivateFiles::new()
        .map_err(|_| fault(2, "private files require a mounted external volume"))?;
    let credentials =
        identity::credentials_with_files(runner_identity_file, &files).map_err(|_| {
            fault(
                2,
                "runner credentials must be a valid private external identity file",
            )
        })?;
    let creator = credentials["name"]
        .as_str()
        .ok_or("credential name missing")?;
    let url = credentials["database_url"]
        .as_str()
        .ok_or("credential URL missing")?;
    let runner_options = url
        .parse::<DorisConnectOptions>()
        .map_err(|_| fault(2, "invalid runner credential URL"))?
        .disable_statement_logging();
    let mut runner_url = runner_options.to_url_lossy();
    let mut admin_url = args.url()?.parse::<DorisConnectOptions>()?.to_url_lossy();
    for value in [&mut runner_url, &mut admin_url] {
        value
            .set_username("")
            .map_err(|_| "invalid credential origin")?;
        value
            .set_password(None)
            .map_err(|_| "invalid credential origin")?;
    }
    if credentials["database"].as_str() != Some(&o.database)
        || runner_url != admin_url
        || !identity::inspect(c, creator, &o.database, &o.cluster).await?
    {
        return Err(fault(
            3,
            "runner credentials do not match this live managed identity and administrator endpoint/database/options",
        ));
    }
    let name = format!("{PREFIX}{}", uuid::Uuid::new_v4().simple());
    let marker = marker_name(&name)?;
    let token = format!(
        "{FORMAT}:{}:{}:{table}:{creator}:{name}",
        o.cluster, o.db_id
    );
    let data = serde_json::json!({
        "format": FORMAT,
        "job_name": name,
        "database": o.database,
        "database_id": o.db_id,
        "cluster": o.cluster,
        "creator": creator,
        "table": table,
        "reservation": marker
    });
    args.observation.data(
        "receipt_output",
        serde_json::json!({
            "path": output,
            "job_name": name,
            "state": "writing"
        }),
    );
    report.set_phase(Phase::ReceiptWriting);
    // Preserve invalid-path versus unconfirmed write/sync failures. No server
    // mutation starts until persistence has been acknowledged.
    receipt(output, &data, &files)?;
    report.name = Some(name.clone());
    report.receipt = Some(output.display().to_string());
    args.observation.data(
        "receipt_output",
        serde_json::json!({
            "path": output,
            "job_name": name,
            "state": "persisted"
        }),
    );
    report.set_phase(Phase::Receipt);
    args.observation
        .event(crate::observation::Event::ReceiptPersisted {
            name: name.clone(),
            path: output.display().to_string(),
        })?;

    // No IF NOT EXISTS, OR REPLACE, reusable names, automatic cleanup or retry.
    report.set_phase(Phase::ReserveSent);
    c.execute(AssertSqlSafe(format!("CREATE VIEW `{}`.`{marker}` AS SELECT CAST('{token}' AS STRING) AS token",o.database))).await
        .map_err(|_|"reservation creation unconfirmed; receipt retained; no job CREATE sent; never reuse name")?;
    report.set_phase(Phase::ReserveAck);
    let reserved = reservation(c, o, &name)
        .await?
        .ok_or_else(|| fault(3, "reservation disappeared; no job CREATE sent"))?;
    report.reservation = Some(reserved.clone());
    if reserved.token != token {
        return Err("reservation changed; no job CREATE sent".into());
    }
    if identity::environment(c).await? != (o.database.clone(), o.cluster.clone())
        || super::database_id(c, &o.database).await? != o.db_id
        || !identity::inspect(c, creator, &o.database, &o.cluster).await?
    {
        return Err("creation environment/identity changed; no job CREATE sent".into());
    }
    let mut runner = DorisConnection::connect_with(&runner_options)
        .await
        .map_err(
            |_| "runner connection failed; reservation/receipt retained; no job CREATE sent",
        )?;
    report.set_phase(Phase::CreateSent);
    runner.execute(AssertSqlSafe(format!("CREATE ROUTINE LOAD `{}`.`{name}` ON `{table}` {body}",o.database))).await
        .map_err(|_|"Routine CREATE outcome unconfirmed; inspect original receipt/name with a new connection; never replay CREATE or reuse name")?;
    report.set_phase(Phase::CreateAck);
    runner.close_hard().await?;
    let job = inspect(c, o, &reserved)
        .await?
        .ok_or_else(|| fault(4, "created job not observed"))?;
    observed(report, job.clone());
    receipt(
        &receipt_input::confirmation(output),
        &serde_json::json!({
            "format": "sqlx-doris-routine-confirmed-v1",
            "receipt": data,
            "job": {"id":job.id,"created":job.created,"state":job.state,"progress":job.progress}
        }),
        &files,
    )?;
    report.confirmed = Some(job);
    report.set_phase(Phase::CreateVerified);
    report.outcome = "created";
    report.message="Routine created and binding confirmed; consumption is not yet certified. No locks/history changed.".into();
    Ok(())
}
async fn operation(
    args: &Context,
    action: &Action,
    located: Option<&Located>,
    report: &mut Report,
) -> Result<()> {
    if args.identity_file.is_some() {
        return Err(fault(
            2,
            "routine requires an independent administrator URL; use --runner-identity-file only for create",
        ));
    }
    let deadline = tokio::time::Instant::now()
        .checked_add(Duration::from_secs(args.ddl_timeout))
        .ok_or_else(|| fault(2, "Routine deadline too large"))?;
    let options = args.options(located.and_then(Located::database)).map_err(|_| fault(2,"An independent valid administrator Doris URL is required; set DATABASE_URL or --database-url"))?;
    let mut c = tokio::time::timeout_at(deadline, DorisConnection::connect_with(&options))
        .await
        .map_err(|_| fault(7, "Routine connection deadline; no mutation sent"))??;
    let result = tokio::time::timeout_at(
        deadline,
        operation_inner(args, action, located, report, &mut c),
    )
    .await;
    let _ = c.close_hard().await;
    result.map_err(|_|fault(7,"Routine deadline expired; receipt/reservation and locks/history retained; no automatic retry or recovery"))?
}
async fn operation_inner(
    args: &Context,
    action: &Action,
    located: Option<&Located>,
    report: &mut Report,
    c: &mut DorisConnection,
) -> Result<()> {
    let o = Observer::new(c, None, &args.observation).await?;
    report.database = Some(o.database.clone());
    report.database_id = Some(o.db_id.clone());
    report.cluster = Some(o.cluster.clone());
    if matches!(action, Action::Create { .. }) {
        return create(args, c, &o, action, report).await;
    }
    let located = located.ok_or_else(|| fault(2, "Routine locator missing"))?;
    report.name = Some(located.name.clone());
    report.receipt = located.path.as_ref().map(|p| p.display().to_string());
    report.confirmed = located.confirmed.clone();
    if let Some(data) = &located.data
        && (data["cluster"] != o.cluster
            || data["database_id"] != o.db_id
            || data["database"] != o.database)
    {
        return Err(fault(
            3,
            "receipt cluster/database identity mismatch; no mutation sent",
        ));
    }
    let Some(r) = reservation(c, &o, &located.name).await? else {
        report.observation = "reservation_missing";
        return Err(fault(
            if matches!(action, Action::Stop { .. }) {
                3
            } else {
                4
            },
            "Permanent reservation not observed; UUID shape alone is insufficient. This does not prove a queued CREATE cannot arrive; no STOP sent.",
        ));
    };
    located.matches(&o, &r)?;
    report.reservation = Some(r.clone());
    let Some(job) = inspect(c, &o, &r).await? else {
        report.observation = if located.confirmed.is_some() {
            "history_unavailable"
        } else {
            "reserved_unobserved"
        };
        return Err(fault(
            4,
            "No job currently observed; creation may be pending/unconfirmed or retained history may be unavailable. Never infer 'never created' or completion; no STOP sent.",
        ));
    };
    located.matches_job(&job)?;
    observed(report, job.clone());
    if let Action::Stop { job_id, force, .. } = action {
        if job_id.is_some_and(|id| id != job.id) {
            return Err(fault(3, "Routine JobId differs; no STOP sent"));
        }
        o.retired(c, Some(&r.creator)).await.map_err(|_| {
            fault(
                3,
                "creator is not verifiably retired or environment changed; no STOP sent",
            )
        })?;
        if super::jobs::routine_terminal(&job.state)? {
            return Err(fault(
                3,
                "Routine job already terminal; no STOP sent or replayed; reservation remains permanent",
            ));
        }
        if !force {
            report.outcome = "preview";
            report.message="Preview only; no STOP sent. Execution requires explicit --job-id --maintenance --managed-namespace --force.".into();
            return Ok(());
        }
        if reservation(c, &o, &r.name).await?.as_ref() != Some(&r) {
            return Err(fault(3, "Routine reservation changed; no STOP sent"));
        }
        o.retired(c, Some(&r.creator))
            .await
            .map_err(|_| fault(3, "creator/environment changed; no STOP sent"))?;
        let fresh = inspect(c, &o, &r)
            .await?
            .ok_or_else(|| fault(4, "Routine job disappeared; no STOP sent"))?;
        located.matches_job(&fresh)?;
        if fresh.id != job.id
            || fresh.created != job.created
            || super::jobs::routine_terminal(&fresh.state)?
        {
            return Err(fault(3, "Routine job changed; no STOP sent"));
        }
        report.set_phase(Phase::StopSent);
        c.execute(AssertSqlSafe(format!(
            "STOP ROUTINE LOAD FOR `{}`.`{}`",
            o.database, r.name
        )))
        .await?;
        report.set_phase(Phase::StopAck);
        o.retired(c, Some(&r.creator)).await?;
        if reservation(c, &o, &r.name).await?.as_ref() != Some(&r) {
            return Err(fault(3, "reservation changed"));
        }
        let after = inspect(c, &o, &r)
            .await?
            .ok_or_else(|| fault(4, "Routine history unavailable after acknowledgement"))?;
        if after.id != job.id || after.created != job.created || after.state != "STOPPED" {
            return Err(fault(3, "STOPPED binding not confirmed"));
        }
        observed(report, after);
        report.set_phase(Phase::StopVerified);
        report.outcome = "stopped";
        report.message="Routine STOPPED under permanent UUID non-reuse protocol. No locks/history/reservation removed. Committed data and admitted tasks/transactions still require drain/reconciliation.".into();
    } else {
        report.outcome = "observed";
        report.message =
            "Read-only scoped observation; never a completion or takeover credential.".into();
    }
    Ok(())
}

pub async fn run(args: &Context, action: &Action) -> Result<()> {
    action.validate()?;
    let mut report = Report::new(
        match action {
            Action::Create { .. } => "create",
            Action::Status { .. } => "status",
            Action::Stop { .. } => "stop",
        },
        args.observation.clone(),
    );
    let result = async {
        let located = match action {
            Action::Create { .. } => None,
            Action::Status { locator } | Action::Stop { locator, .. } => {
                Some(Located::from(locator)?)
            }
        };
        if let Some(located) = &located {
            report.name = Some(located.name.clone());
            report.receipt = located.path.as_ref().map(|p| p.display().to_string());
            report.confirmed = located.confirmed.clone();
        }
        operation(args, action, located.as_ref(), &mut report).await
    }
    .await;
    if let Err(error) = &result {
        report.capture(error.as_ref());
    }
    report.record();
    if result.is_err() {
        Err(fault(report.code, report.message))
    } else {
        Ok(())
    }
}

pub(crate) struct Diagnostic {
    located: Located,
}
impl Diagnostic {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            located: Located::from(&Locator {
                job_name: None,
                receipt: Some(path.into()),
            })?,
        })
    }
    pub(crate) fn database(&self) -> Option<&str> {
        self.located.database()
    }
    pub(crate) async fn observe(&self, args: &Context) -> Result<serde_json::Value> {
        let action = Action::Status {
            locator: Locator {
                job_name: Some(self.located.name.clone()),
                receipt: None,
            },
        };
        let mut report = Report::new("status", args.observation.clone());
        report.name = Some(self.located.name.clone());
        report.confirmed = self.located.confirmed.clone();
        report.receipt = self.located.path.as_ref().map(|p| p.display().to_string());
        let result = operation(args, &action, Some(&self.located), &mut report).await;
        if let Err(e) = &result {
            report.capture(e.as_ref());
        }
        Ok(report.value())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direct_stop_cannot_bypass_maintenance_namespace_or_exact_id() {
        for (id, maintenance, managed_namespace) in [
            (None, true, true),
            (Some(0), true, true),
            (Some(u64::MAX), true, true),
            (Some(1), false, true),
            (Some(1), true, false),
        ] {
            let action = Action::Stop {
                locator: Locator {
                    job_name: None,
                    receipt: None,
                },
                job_id: id,
                maintenance,
                managed_namespace,
                force: true,
            };
            assert_eq!(
                action
                    .validate()
                    .unwrap_err()
                    .downcast_ref::<crate::failure::Failure>()
                    .unwrap()
                    .code(),
                2
            );
        }
    }

    #[test]
    fn only_canonical_fresh_uuid_namespace_is_accepted() {
        assert!(uuid_name("sqlx_rl_00000000000040008000000000000001").is_ok());
        for name in [
            "rl",
            "sqlx_rl_00000000-0000-4000-8000-000000000001",
            "sqlx_rl_00000000000010008000000000000001",
            "sqlx_rl_00000000000040000000000000000001",
            "sqlx_rl_ABCDEF00000040008000000000000001",
        ] {
            assert!(uuid_name(name).is_err());
        }
        for name in ["events;DROP", "1table", "_sqlx_migrations", "sqlx_rl_old"] {
            assert!(identifier(name).is_err());
        }
    }
    #[test]
    fn progress_is_reported_cursor_or_an_unresolved_sentinel() {
        for raw in [
            r#"{}"#,
            r#"{"0":"0","1":"9223372036854775807"}"#,
            r#"{"0":"OFFSET_ZERO","1":"OFFSET_END","2":"OFFSET_BEGINNING"}"#,
        ] {
            assert!(progress(raw).is_ok(), "{raw}");
        }
        for raw in [
            "null",
            "[]",
            r#"{"-1":"0"}"#,
            r#"{"0":0}"#,
            r#"{"0":"-1"}"#,
            r#"{"0":"9223372036854775808"}"#,
            r#"{"0":"UNKNOWN"}"#,
        ] {
            assert!(progress(raw).is_err(), "{raw}");
        }
    }
}
