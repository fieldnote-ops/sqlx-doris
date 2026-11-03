//! Explicit lifecycle for non-reusable, database-scoped migration accounts.
use super::{Context, Result};
use crate::storage::external;
use sqlx_core::column::Column;
use sqlx_core::connection::ConnectOptions;
use sqlx_core::connection::Connection;
use sqlx_core::executor::Executor;
use sqlx_core::row::Row;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_doris_core::{DorisConnectOptions, DorisConnection, DorisRow};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
};

const PREFIX: &str = "sqlx_mig_";
const FORMAT: &str = "sqlx-doris migration identity v1";
const PRIVILEGES: &str = "SELECT_PRIV,LOAD_PRIV,CREATE_PRIV,ALTER_PRIV,DROP_PRIV";

#[derive(Clone)]
pub enum Action {
    /// Create a fresh, dedicated identity and a private external credential file.
    Create { output: PathBuf },
    /// Inspect one managed identity, without changing privileges or locks.
    Status { name: String },
    /// Preview removal of one managed identity. Does not recover any lock.
    Retire {
        name: String,
        /// Confirm this identity belongs exclusively to the old runner, no
        /// administrator can recreate/regrant it, and this is the sole maintainer.
        maintenance: bool,
        /// DROP the identity after validation; without this flag, preview only.
        force: bool,
    },
}

pub(super) fn identity_id(name: &str) -> Result<uuid::Uuid> {
    let token = name
        .strip_prefix(PREFIX)
        .ok_or("not a managed migration identity")?;
    let id = uuid::Uuid::parse_str(token).map_err(|_| "invalid migration identity UUID")?;
    if token != id.simple().to_string() {
        return Err("identity must use its exact canonical generated name".into());
    }
    Ok(id)
}
pub(super) fn valid_database(database: &str) -> Result<()> {
    if database.is_empty()
        || database.len() > 64
        || !database
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || v == b'_')
        || database.bytes().next().is_some_and(|v| v.is_ascii_digit())
        || ["mysql", "information_schema"]
            .iter()
            .any(|v| database.eq_ignore_ascii_case(v))
    {
        return Err("managed identities require a non-system ASCII database identifier".into());
    }
    Ok(())
}
fn password_transport(options: &DorisConnectOptions) -> Result<()> {
    if options
        .to_url_lossy()
        .query_pairs()
        .any(|(key, _)| matches!(key.as_ref(), "ssl-cert" | "ssl-key"))
    {
        return Err("managed runner credentials cannot inherit administrative client certificates or keys; password-based identities only".into());
    }
    Ok(())
}
fn marker(name: &str, database: &str, cluster: &str) -> Result<String> {
    let id = identity_id(name)?;
    valid_database(database)?;
    Ok(format!("{FORMAT}:{cluster}:{database}:{}", id.simple()))
}

pub(crate) use crate::storage::PrivateFiles;
pub(super) fn credentials(path: &Path) -> Result<serde_json::Value> {
    credentials_with_files(path, &PrivateFiles::new()?)
}
pub(super) fn credentials_with_files(
    path: &Path,
    files: &PrivateFiles,
) -> Result<serde_json::Value> {
    let data = files.read_limited(path, 16_384)?;
    let value: serde_json::Value =
        serde_json::from_slice(&data).map_err(|_| "invalid identity credential JSON")?;
    let field = |key| {
        value
            .get(key)
            .and_then(|v| v.as_str())
            .ok_or("missing identity credential field")
    };
    if field("format")? != FORMAT {
        return Err("unsupported identity credential format".into());
    }
    let name = field("name")?;
    identity_id(name)?;
    let database = field("database")?;
    valid_database(database)?;
    let options = field("database_url")?
        .parse::<DorisConnectOptions>()
        .map_err(|_| "invalid identity credential URL")?;
    password_transport(&options)?;
    if options.get_username() != name || options.get_database() != Some(database) {
        return Err("identity credential URL does not match its name/database".into());
    }
    Ok(value)
}
pub(crate) fn credential_url(path: &Path) -> Result<String> {
    Ok(credentials(path)?["database_url"]
        .as_str()
        .unwrap()
        .to_owned())
}
pub fn credential_name(path: &Path) -> Result<String> {
    Ok(credentials(path)?["name"].as_str().unwrap().to_owned())
}

fn supported_frontend(
    count: usize,
    alive: &str,
    master: &str,
    current: &str,
    version: &str,
) -> Result<()> {
    if count != 1
        || alive != "true"
        || master != "true"
        || current != "Yes"
        || !version.starts_with("doris-4.1.5-")
    {
        return Err(crate::failure::fault(
            4,
            "identity isolation is verified only on a single live master FE running Doris 4.1.5; no change made",
        ));
    }
    Ok(())
}
pub(super) async fn environment(c: &mut DorisConnection) -> Result<(String, String)> {
    let database: Option<String> = sqlx_core::query_scalar::query_scalar("SELECT DATABASE()")
        .fetch_one(&mut *c)
        .await?;
    let database = database.ok_or("select a database for identity management")?;
    valid_database(&database)?;
    let catalog: String = sqlx_core::query_scalar::query_scalar("SELECT CURRENT_CATALOG()")
        .fetch_one(&mut *c)
        .await?;
    if catalog != "internal" {
        return Err(crate::failure::fault(
            4,
            "managed identities require the internal catalog",
        ));
    }
    let frontends = c.fetch_all("SHOW FRONTENDS").await?;
    let first = frontends
        .first()
        .ok_or("frontend topology is unavailable; no change made")?;
    supported_frontend(
        frontends.len(),
        first.try_get("Alive")?,
        first.try_get("IsMaster")?,
        first.try_get("CurrentConnected")?,
        first.try_get("Version")?,
    )?;
    for key in ["authentication_type", "access_controller_type"] {
        let rows = c
            .fetch_all(AssertSqlSafe(format!("SHOW FRONTEND CONFIG LIKE '{key}'")))
            .await?;
        if rows.len() != 1 || rows[0].try_get::<&str, _>("Value")? != "default" {
            return Err(crate::failure::fault(
                4,
                "identity isolation requires built-in authentication and authorization; no change made",
            ));
        }
    }
    Ok((database, frontends[0].try_get::<String, _>("ClusterId")?))
}
fn no_unexpected_privileges(row: &DorisRow, database: &str) -> Result<()> {
    let expected: BTreeSet<_> = [
        "UserIdentity",
        "Comment",
        "Password",
        "RequireSan",
        "Roles",
        "GlobalPrivs",
        "CatalogPrivs",
        "DatabasePrivs",
        "TablePrivs",
        "ColPrivs",
        "ResourcePrivs",
        "CloudClusterPrivs",
        "CloudStagePrivs",
        "StorageVaultPrivs",
        "WorkloadGroupPrivs",
        "ComputeGroupPrivs",
    ]
    .into_iter()
    .collect();
    if row
        .columns()
        .iter()
        .map(|v| v.name())
        .collect::<BTreeSet<_>>()
        != expected
    {
        return Err(crate::failure::fault(
            4,
            "unknown identity privilege metadata layout; no change made",
        ));
    }
    for column in row.columns() {
        let name = column.name();
        let value = row.try_get::<Option<&str>, _>(name)?.unwrap_or("");
        match name {
            "UserIdentity" | "Comment" | "Password" => {}
            "Roles" | "GlobalPrivs" | "CatalogPrivs" | "TablePrivs" | "ColPrivs"
            | "ResourcePrivs" | "CloudClusterPrivs" | "CloudStagePrivs" | "StorageVaultPrivs"
            | "ComputeGroupPrivs" | "RequireSan"
                if value.is_empty() => {}
            "WorkloadGroupPrivs" if value == "normal: Usage_priv" || value.is_empty() => {}
            "DatabasePrivs" => check_database_grants(value, database)?,
            _ => {
                return Err(crate::failure::wrap(
                    3,
                    "unexpected identity permission metadata",
                    format!("in {name}; refuse to retire a modified/shared identity").into(),
                ));
            }
        }
    }
    Ok(())
}
fn check_database_grants(value: &str, database: &str) -> Result<()> {
    let allowed: BTreeSet<_> = [
        "Select_priv",
        "Load_priv",
        "Alter_priv",
        "Create_priv",
        "Drop_priv",
    ]
    .into_iter()
    .collect();
    for entry in value.split("; ").filter(|v| !v.is_empty()) {
        let (target, privileges) = entry
            .split_once(": ")
            .ok_or("unknown database-grant format")?;
        let privileges: BTreeSet<_> = privileges.split(',').collect();
        let valid = if target == format!("internal.{database}") {
            privileges.is_subset(&allowed)
        } else {
            ["internal.mysql", "internal.information_schema"].contains(&target)
                && privileges == BTreeSet::from(["Select_priv"])
        };
        if !valid {
            return Err(crate::failure::fault(
                3,
                "unexpected database privileges; identity may be shared or expanded",
            ));
        }
    }
    Ok(())
}
pub(super) async fn inspect(
    c: &mut DorisConnection,
    name: &str,
    database: &str,
    cluster: &str,
) -> Result<bool> {
    let expected = marker(name, database, cluster)?;
    let rows = c.fetch_all("SHOW ALL GRANTS").await?;
    let prefix = format!("'{name}'@");
    let mut matches = Vec::new();
    for row in &rows {
        if row.try_get::<&str, _>("UserIdentity")?.starts_with(&prefix) {
            matches.push(row);
        }
    }
    if matches.is_empty() {
        return Ok(false);
    }
    if matches.len() != 1
        || matches[0].try_get::<&str, _>("UserIdentity")? != format!("'{name}'@'%'")
    {
        return Err(crate::failure::fault(
            3,
            "other host identities exist for this name; no change made",
        ));
    }
    let row = matches[0];
    if row.try_get::<&str, _>("Comment")? != expected {
        return Err(crate::failure::fault(
            3,
            "identity provenance/database/cluster marker does not match; no change made",
        ));
    }
    no_unexpected_privileges(row, database)?;
    Ok(true)
}

pub async fn run(args: &Context, action: &Action) -> Result<()> {
    if matches!(
        action,
        Action::Retire {
            force: true,
            maintenance: false,
            ..
        }
    ) {
        return Err(crate::failure::fault(
            2,
            "identity retirement requires explicit maintenance",
        ));
    }
    if args.identity_file.is_some() {
        return Err(crate::failure::fault(
            2,
            "identity management requires an independent administrator URL; omit --identity-file",
        ));
    }
    if let Action::Status { name } | Action::Retire { name, .. } = action {
        identity_id(name).map_err(|_| crate::failure::fault(2, "invalid managed identity"))?;
    }
    let mut c = args.connect().await?;
    let (database, cluster) = environment(&mut c).await?;
    args.observation.data(
        "identity",
        serde_json::json!({
            "database": database,
            "cluster": cluster
        }),
    );
    match action {
        Action::Create { output } => {
            let files = PrivateFiles::new()?;
            let volume = &files.volume;
            let parent = output
                .parent()
                .filter(|v| !v.as_os_str().is_empty())
                .unwrap_or(Path::new("."));
            let parent = external(parent, volume)?;
            let path = parent.join(
                output
                    .file_name()
                    .ok_or("credential output must name a file")?,
            );
            let name = format!("{PREFIX}{}", uuid::Uuid::new_v4().simple());
            let password = uuid::Uuid::new_v4().simple().to_string();
            let base = args.url()?.parse::<DorisConnectOptions>()?;
            password_transport(&base)?;
            let url = base
                .username(&name)
                .password(&password)
                .to_url_lossy()
                .to_string();
            let data = serde_json::json!({
                "format": FORMAT,
                "name": name,
                "database": database,
                "database_url": url
            });
            args.observation.data(
                "identity",
                serde_json::json!({
                    "name": name,
                    "database": database,
                    "cluster": cluster,
                    "credential_file": path
                }),
            );
            args.observation
                .phase(crate::observation::Phase::Local, "credentials_writing");
            files.write_new(&path, serde_json::to_string_pretty(&data)?.as_bytes())?;
            // Keep a durable breadcrumb even if CREATE/GRANT is interrupted.
            // Neither statement is retried and the generated name is never reused.

            args.observation.data(
                "identity",
                serde_json::json!({
                    "name": name,
                    "database": database,
                    "cluster": cluster,
                    "credential_file": path
                }),
            );
            args.observation
                .phase(crate::observation::Phase::Local, "credentials_persisted");
            args.observation
                .event(crate::observation::Event::CredentialsPersisted {
                    name: name.clone(),
                    path: path.display().to_string(),
                })?;
            let comment = marker(&name, &database, &cluster)?;
            args.observation
                .phase(crate::observation::Phase::Submitted, "create_identity");
            if c.execute(AssertSqlSafe(format!(
                "CREATE USER '{name}'@'%' IDENTIFIED BY '{password}' COMMENT '{comment}'"
            )))
            .await
            .is_err()
            {
                return Err("identity creation outcome is unconfirmed; credential file retained; inspect identity status before any action; no automatic retry".into());
            }
            args.observation
                .phase(crate::observation::Phase::Acknowledged, "create_identity");
            args.observation
                .phase(crate::observation::Phase::Submitted, "grant_identity");
            c.execute(AssertSqlSafe(format!("GRANT {PRIVILEGES} ON `{database}`.* TO '{name}'@'%'"))).await
                .map_err(|_| "identity grant outcome is unconfirmed; credential file retained; inspect status; no automatic retry")?;
            args.observation
                .phase(crate::observation::Phase::Acknowledged, "grant_identity");
            if !inspect(&mut c, &name, &database, &cluster).await? {
                return Err("created identity disappeared".into());
            }
            args.observation
                .phase(crate::observation::Phase::Verified, "identity_created");
        }
        Action::Status { name } => {
            let present = inspect(&mut c, name, &database, &cluster).await?;
            args.observation.data(
                "identity",
                serde_json::json!({
                    "name": name,
                    "database": database,
                    "cluster": cluster,
                    "present": present,
                    "isolated": false
                }),
            );
        }
        Action::Retire { name, force, .. } => {
            args.observation.data(
                "identity",
                serde_json::json!({
                    "name": name,
                    "database": database,
                    "cluster": cluster,
                    "preview": !force
                }),
            );
            if !inspect(&mut c, name, &database, &cluster).await? {
                return Err(crate::failure::fault(
                    3,
                    "identity already absent; no DROP sent; do not automatically retry or recreate it",
                ));
            }

            if *force {
                // Recheck immediately, but do not pretend DROP USER is a CAS.
                // Unique, never-reused names and serialized administration are required.
                environment(&mut c).await?;
                if !inspect(&mut c, name, &database, &cluster).await? {
                    return Err(crate::failure::fault(
                        3,
                        "identity changed before DROP; no DROP sent",
                    ));
                }
                args.observation
                    .phase(crate::observation::Phase::Submitted, "retire_identity");
                c.execute(AssertSqlSafe(format!("DROP USER '{name}'@'%'"))).await
                    .map_err(|_| "identity DROP outcome is uncertain; inspect status on a new connection; never recreate or blindly retry this name")?;
                args.observation
                    .phase(crate::observation::Phase::Acknowledged, "retire_identity");
                let verification: Result<()> = async {
                    let observed = environment(&mut c).await?;
                    if observed != (database.clone(), cluster.clone())
                        || inspect(&mut c, name, &database, &cluster).await?
                    {
                        return Err("identity or environment changed during retirement".into());
                    }
                    Ok(())
                }
                .await;
                verification.map_err(|_| "identity DROP acknowledged; verification failed; inspect status on a new connection; do not retry or recreate the identity")?;
                args.observation.data(
                    "identity",
                    serde_json::json!({
                        "name": name, "database": database, "cluster": cluster,
                        "present": false, "retired": true
                    }),
                );
                args.observation
                    .phase(crate::observation::Phase::Verified, "identity_retired");
            }
        }
    }
    c.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_unmanaged_noncanonical_and_system_targets() {
        for name in [
            "root",
            "admin",
            "sqlx_mig_bad",
            "sqlx_mig_00000000-0000-4000-8000-000000000001",
            "sqlx_mig_0000000000004000800000000000000A",
        ] {
            assert!(identity_id(name).is_err(), "{name}");
        }
        for db in ["", "mysql", "information_schema", "db;other", "1db"] {
            assert!(valid_database(db).is_err());
        }
    }
    #[test]
    fn privileges_cannot_escape_the_expected_database_or_include_grant() {
        assert!(check_database_grants("internal.mysql: Select_priv; internal.information_schema: Select_priv; internal.app: Select_priv,Load_priv,Alter_priv,Create_priv,Drop_priv", "app").is_ok());
        for value in [
            "internal.app: Grant_priv",
            "internal.other: Select_priv",
            "*.*: Load_priv",
            "internal.mysql: Load_priv",
            "unknown",
        ] {
            assert!(check_database_grants(value, "app").is_err(), "{value}");
        }
    }
    #[test]
    fn isolation_cannot_be_attested_for_multiple_dead_remote_or_unknown_frontends() {
        assert!(
            supported_frontend(1, "true", "true", "Yes", "doris-4.1.5-release-example").is_ok()
        );
        for (count, alive, master, current, version) in [
            (0, "true", "true", "Yes", "doris-4.1.5-release-example"),
            (2, "true", "true", "Yes", "doris-4.1.5-release-example"),
            (1, "false", "true", "Yes", "doris-4.1.5-release-example"),
            (1, "true", "false", "Yes", "doris-4.1.5-release-example"),
            (1, "true", "true", "No", "doris-4.1.5-release-example"),
            (1, "true", "true", "Yes", "doris-4.1.6-release-example"),
        ] {
            assert!(supported_frontend(count, alive, master, current, version).is_err());
        }
    }
    #[cfg(unix)]
    #[test]
    fn a_scoped_file_attestation_rejects_device_changes_and_other_filesystems() {
        let mut files = PrivateFiles::new().unwrap();
        files.check(&files.volume).unwrap();
        assert!(files.check(Path::new("/")).is_err());
        files.device = files.device.wrapping_add(1);
        assert!(files.check(&files.volume).is_err());
        assert!(files.open(&files.volume).is_err());
    }
    #[test]
    fn administrative_client_certificates_cannot_be_inherited_by_runners() {
        for suffix in [
            "ssl-cert=administrator.pem",
            "ssl-key=administrator-key.pem",
        ] {
            let options = format!("doris://admin:admin-secret@localhost/app?{suffix}")
                .parse::<DorisConnectOptions>()
                .unwrap();
            let error = password_transport(&options).unwrap_err().to_string();
            assert!(!error.contains("admin-secret"));
        }
        assert!(
            password_transport(
                &"doris://user:runner-secret@localhost/app?ssl-mode=verify-identity&ssl-ca=ca.pem"
                    .parse::<DorisConnectOptions>()
                    .unwrap()
            )
            .is_ok()
        );
    }
}
