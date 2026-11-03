//! Cancel only a verified BROKER LOAD's registered transaction, never its label.
use super::{Action, Context, Observer, Result, number};
use reqwest::{Client, Method, Response, Url, header};
use sqlx_core::connection::ConnectOptions;
use sqlx_core::executor::Executor;
use sqlx_core::row::Row;
use sqlx_core::sql_str::AssertSqlSafe;
use sqlx_doris_core::{DorisConnectOptions, DorisConnection};
use std::time::Duration;

#[derive(Debug, Eq, PartialEq)]
struct Load {
    id: u64,
    transaction: u64,
    label: String,
    created: String,
    kind: String,
    state: String,
}
async fn inspect(c: &mut DorisConnection, database: &str, id: u64) -> Result<Load> {
    let mut found = None;
    for r in c
        .fetch_all(AssertSqlSafe(format!("SHOW LOAD FROM `{database}`")))
        .await?
    {
        if number(r.try_get("JobId")?)? == id {
            if found.is_some() {
                return Err("ambiguous LOAD JobId; no abort sent".into());
            }
            found = Some(Load {
                id,
                transaction: number(r.try_get("TransactionId")?)?,
                label: r.try_get("Label")?,
                created: r.try_get("CreateTime")?,
                kind: r.try_get("Type")?,
                state: r.try_get("State")?,
            });
        }
    }
    found.ok_or_else(|| "LOAD JobId not found; no label fallback or abort sent".into())
}
fn eligible(load: &Load, expected: u64) -> Result<()> {
    if load.kind != "BROKER"
        || !matches!(load.state.as_str(), "PENDING" | "LOADING")
        || expected == 0
        || load.transaction != expected
    {
        return Err("only an active BROKER LOAD with the exact positive registered TransactionId is eligible; no abort sent".into());
    }
    Ok(())
}
async fn transaction(c: &mut DorisConnection, database: &str, load: &Load) -> Result<String> {
    let rows = c
        .fetch_all(AssertSqlSafe(format!(
            "SHOW TRANSACTION FROM `{database}` WHERE id={}",
            load.transaction
        )))
        .await?;
    if rows.len() != 1
        || number(rows[0].try_get("TransactionId")?)? != load.transaction
        || number(rows[0].try_get("ListenerId")?)? != load.id
        || rows[0].try_get::<&str, _>("Label")? != load.label
        || rows[0].try_get::<&str, _>("LoadJobSourceType")? != "BATCH_LOAD_JOB"
    {
        return Err(
            "LOAD transaction/job binding is unavailable or mismatched; no label fallback".into(),
        );
    }
    Ok(rows[0].try_get("TransactionStatus")?)
}
fn origin(value: &str) -> Result<Url> {
    let url = Url::parse(value).map_err(|_| "invalid FE HTTP origin")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !matches!(url.path(), "" | "/")
    {
        return Err(
            "FE HTTP origin must be http(s)://host[:port], without credentials/path/query/fragment"
                .into(),
        );
    }
    Ok(url)
}
fn credentials(options: &DorisConnectOptions) -> Result<(String, String)> {
    let url = options.to_url_lossy();
    if url
        .query_pairs()
        .any(|(k, _)| matches!(k.as_ref(), "ssl-cert" | "ssl-key"))
    {
        return Err("LOAD abort requires password authentication; SQL client certificates/keys cannot be inherited by HTTP".into());
    }
    let password = percent_encoding::percent_decode_str(url.password().unwrap_or(""))
        .decode_utf8()
        .map_err(|_| "invalid password encoding")?
        .into_owned();
    Ok((options.get_username().to_owned(), password))
}
async fn json(mut response: Response) -> Result<serde_json::Value> {
    if !response.status().is_success() {
        return Err("HTTP response was not successful".into());
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "HTTP body read failed")?
    {
        if bytes.len() + chunk.len() > 1_048_576 {
            return Err("HTTP response exceeds metadata limit".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid HTTP JSON response".into())
}
fn fe_row(value: &serde_json::Value) -> Result<std::collections::BTreeMap<String, String>> {
    if value.get("code").and_then(|v| v.as_i64()) != Some(0) {
        return Err("FE HTTP metadata rejected".into());
    }
    let columns = value["data"]["columnNames"]
        .as_array()
        .ok_or("unknown FE HTTP metadata columns")?;
    let rows = value["data"]["rows"]
        .as_array()
        .ok_or("unknown FE HTTP metadata rows")?;
    if rows.len() != 1 {
        return Err("HTTP endpoint is not the verified single-FE cluster".into());
    }
    let row = rows[0].as_array().ok_or("unknown FE HTTP row")?;
    if row.len() != columns.len() {
        return Err("FE HTTP metadata width mismatch".into());
    }
    let mut result = std::collections::BTreeMap::new();
    for (key, value) in columns.iter().zip(row) {
        let (key, value) = (
            key.as_str().ok_or("unknown FE HTTP column")?,
            value.as_str().ok_or("unknown FE HTTP value")?,
        );
        if result.insert(key.to_owned(), value.to_owned()).is_some() {
            return Err("duplicate FE HTTP metadata column".into());
        }
    }
    Ok(result)
}
struct Http {
    client: Client,
    base: Url,
    user: String,
    password: String,
    frontend: std::collections::BTreeMap<String, String>,
    backend_host: String,
    backend_port: u16,
    backend_id: String,
    backend_started: String,
}
impl Http {
    fn request(&self, method: Method, url: Url) -> reqwest::RequestBuilder {
        self.client
            .request(method, url)
            .basic_auth(&self.user, Some(&self.password))
    }
    async fn new(args: &Context, c: &mut DorisConnection, http_url: &str) -> Result<Self> {
        let base = origin(http_url)?;
        let options = args.url()?.parse::<DorisConnectOptions>()?;
        let (user, password) = credentials(&options)?;
        let mut builder = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(Duration::from_secs(10));
        for (key, value) in options.to_url_lossy().query_pairs() {
            if key == "ssl-ca" {
                let cert = reqwest::Certificate::from_pem(&std::fs::read(value.as_ref())?)
                    .map_err(|_| "invalid HTTP CA certificate from ssl-ca")?;
                builder = builder.add_root_certificate(cert);
            }
            if key == "ssl-mode"
                && matches!(
                    value.as_ref(),
                    "required" | "require" | "verify-ca" | "verify-identity"
                )
                && base.scheme() != "https"
            {
                return Err("SQL TLS requirements forbid cleartext HTTP for LOAD abort".into());
            }
        }
        let client = builder
            .build()
            .map_err(|_| "cannot configure HTTP client")?;
        let mut frontend = std::collections::BTreeMap::new();
        let rows = c.fetch_all("SHOW FRONTENDS").await?;
        if rows.len() != 1 {
            return Err("LOAD abort requires one verified FE".into());
        }
        for key in [
            "Name",
            "Host",
            "HttpPort",
            "QueryPort",
            "RpcPort",
            "ClusterId",
            "Version",
            "LastStartTime",
            "Alive",
            "IsMaster",
            "CurrentConnected",
        ] {
            frontend.insert(key.to_owned(), rows[0].try_get::<String, _>(key)?);
        }
        let rows = c.fetch_all("SHOW BACKENDS").await?;
        // The HTTP cancellation path is currently certified on the dedicated
        // one-FE/one-BE fixture only. Never choose an unverified fallback node.
        if rows.len() != 1
            || rows[0].try_get::<&str, _>("Alive")? != "true"
            || !rows[0]
                .try_get::<&str, _>("Version")?
                .starts_with("doris-4.1.5-")
        {
            return Err("LOAD abort currently requires exactly one live Doris 4.1.5 BE".into());
        }
        let backend_host = rows[0].try_get::<String, _>("Host")?;
        let backend_port = rows[0]
            .try_get::<&str, _>("HttpPort")?
            .parse::<u16>()
            .map_err(|_| "invalid BE HTTP port")?;
        let backend_id = rows[0].try_get("BackendId")?;
        let backend_started = rows[0].try_get("LastStartTime")?;
        let http = Self {
            client,
            base,
            user,
            password,
            frontend,
            backend_host,
            backend_port,
            backend_id,
            backend_started,
        };
        http.verify(c).await?;
        Ok(http)
    }
    async fn verify(&self, c: &mut DorisConnection) -> Result<()> {
        let rows = c.fetch_all("SHOW FRONTENDS").await?;
        if rows.len() != 1 {
            return Err("FE topology changed; no BE abort sent".into());
        }
        for (key, value) in &self.frontend {
            if rows[0].try_get::<&str, _>(key.as_str())? != value {
                return Err("FE identity/epoch changed; no BE abort sent".into());
            }
        }
        let backends = c.fetch_all("SHOW BACKENDS").await?;
        if backends.len() != 1
            || backends[0].try_get::<&str, _>("BackendId")? != self.backend_id
            || backends[0].try_get::<&str, _>("Host")? != self.backend_host
            || backends[0].try_get::<&str, _>("HttpPort")?.parse::<u16>()? != self.backend_port
            || backends[0].try_get::<&str, _>("LastStartTime")? != self.backend_started
            || backends[0].try_get::<&str, _>("Alive")? != "true"
            || !backends[0]
                .try_get::<&str, _>("Version")?
                .starts_with("doris-4.1.5-")
        {
            return Err("BE identity/epoch changed; no BE abort sent".into());
        }
        let response = self
            .request(
                Method::GET,
                self.base.join("rest/v2/manager/node/frontends")?,
            )
            .header(header::CACHE_CONTROL, "no-cache")
            .send()
            .await
            .map_err(|_| "FE HTTP identity read failed")?;
        let actual = fe_row(&json(response).await?)?;
        for (key, value) in &self.frontend {
            if actual.get(key) != Some(value) {
                return Err("SQL/HTTP FE identity mismatch; no BE abort sent".into());
            }
        }
        Ok(())
    }
    fn target(&self, value: &str, database: &str) -> Result<Url> {
        let mut target = Url::parse(value).map_err(|_| "invalid abort redirect")?;
        // Doris 4.1.5 includes Basic credentials in Location. Never retain,
        // log or use redirect-supplied credentials; authenticate independently.
        target
            .set_username("")
            .map_err(|_| "invalid abort redirect")?;
        target
            .set_password(None)
            .map_err(|_| "invalid abort redirect")?;
        let host = target
            .host_str()
            .unwrap_or("")
            .trim_start_matches('[')
            .trim_end_matches(']');
        if !matches!(target.scheme(), "http" | "https")
            || self.base.scheme() == "https" && target.scheme() != "https"
            || !host.eq_ignore_ascii_case(&self.backend_host)
            || target.port_or_known_default() != Some(self.backend_port)
            || target.path() != format!("/api/{database}/_stream_load_2pc")
            || target.query().is_some()
            || target.fragment().is_some()
        {
            return Err("abort redirect is not the registered BE/path; no BE abort sent".into());
        }
        Ok(target)
    }
    async fn route(&self, database: &str, txn: u64) -> Result<Url> {
        let response = self
            .request(
                Method::PUT,
                self.base
                    .join(&format!("api/{database}/_stream_load_2pc"))?,
            )
            .header("txn_id", txn)
            .header("txn_operation", "abort")
            .send()
            .await
            .map_err(|_| "FE abort routing unconfirmed; no BE abort sent by CLI")?;
        if response.status() != reqwest::StatusCode::TEMPORARY_REDIRECT {
            return Err(
                "unexpected FE abort routing response; operation unconfirmed; never blindly retry"
                    .into(),
            );
        }
        let location = response
            .headers()
            .get(header::LOCATION)
            .ok_or("abort redirect missing")?
            .to_str()
            .map_err(|_| "invalid abort redirect header")?;
        self.target(location, database)
    }
}
pub async fn run(
    args: &Context,
    c: &mut DorisConnection,
    observer: &Observer,
    action: &Action,
    phase: &mut &'static str,
) -> Result<()> {
    let Action::CancelLoad {
        name,
        job_id,
        transaction_id,
        http_url,
        force,
        ..
    } = action
    else {
        return Err("invalid LOAD cancellation action".into());
    };
    observer.retired(c, Some(name)).await?;
    let load = inspect(c, &observer.database, *job_id).await?;
    args.observation.data(
        "target",
        serde_json::json!({
            "kind": "broker_load",
            "job_id": load.id,
            "transaction_id": load.transaction,
            "state": load.state,
            "preview": !force
        }),
    );

    if !force {
        if load.transaction > 0 {
            eligible(&load, load.transaction)?;
            let state = transaction(c, &observer.database, &load).await?;
            args.observation
                .data("transaction_state", serde_json::json!(state));
        }

        return Ok(());
    }
    eligible(
        &load,
        transaction_id.ok_or("execution requires --transaction-id")?,
    )?;
    let state = transaction(c, &observer.database, &load).await?;
    if !matches!(state.as_str(), "PREPARE" | "PRECOMMITTED") {
        return Err(
            "transaction is not abortable; no abort sent; committed/visible data cannot be undone"
                .into(),
        );
    }
    let http = Http::new(
        args,
        c,
        http_url
            .as_deref()
            .ok_or("execution requires --http-url or DORIS_HTTP_URL")?,
    )
    .await?;
    *phase = "FE abort routing submitted; operation unconfirmed";
    args.observation
        .phase(crate::observation::Phase::Submitted, "abort_load");
    let target = http.route(&observer.database, load.transaction).await?;
    observer.retired(c, Some(name)).await?;
    http.verify(c).await?;
    let fresh = inspect(c, &observer.database, *job_id).await?;
    eligible(&fresh, load.transaction)?;
    if fresh.label != load.label || fresh.created != load.created || fresh.kind != load.kind {
        return Err("LOAD identity changed; no BE abort sent".into());
    }
    if !matches!(
        transaction(c, &observer.database, &fresh).await?.as_str(),
        "PREPARE" | "PRECOMMITTED"
    ) {
        return Err("LOAD transaction changed; no BE abort sent".into());
    }
    *phase = "LOAD ABORT submitted; outcome uncertain";
    args.observation
        .phase(crate::observation::Phase::Submitted, "abort_load");
    let response = http
        .request(Method::PUT, target)
        .header("txn_id", load.transaction)
        .header("txn_operation", "abort")
        .send()
        .await
        .map_err(|_|
            "LOAD ABORT outcome is uncertain; inspect exact job/transaction on a new connection; never retry by label"
        )?;
    let reply = json(response)
        .await
        .map_err(|_| "LOAD ABORT response unconfirmed; no automatic retry or label fallback")?;
    if reply["status"].as_str() != Some("Success")
        || reply["msg"].as_str()
            != Some(&format!(
                "transaction [{}] abort successfully.",
                load.transaction
            ))
    {
        return Err("LOAD ABORT outcome unconfirmed/rejected; transaction may have committed; inspect exact IDs, never retry by label".into());
    }
    *phase = "LOAD ABORT acknowledged; terminal verification unconfirmed";
    args.observation
        .phase(crate::observation::Phase::Acknowledged, "abort_load");
    let verification: Result<()> = async {
        let after = inspect(c, &observer.database, load.id).await?;
        if after.transaction != load.transaction
            || after.label != load.label
            || after.created != load.created
            || after.kind != load.kind
            || after.state != "CANCELLED"
            || transaction(c, &observer.database, &after).await? != "ABORTED"
        {
            return Err("job/transaction terminal binding not confirmed".into());
        }
        observer.retired(c, Some(name)).await?;
        Ok(())
    }
    .await;
    verification.map_err(|_| "LOAD ABORT acknowledged; terminal verification failed; use a new connection, do not replay")?;
    args.observation.data(
        "completion",
        serde_json::json!({
            "kind": "broker_load", "job_id": load.id, "transaction_id": load.transaction,
            "state": "CANCELLED", "transaction_state": "ABORTED"
        }),
    );
    args.observation
        .phase(crate::observation::Phase::Verified, "abort_load");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unbound_terminal_wrong_type_and_changed_transactions_cannot_be_aborted() {
        let mut load = Load {
            id: 1,
            transaction: 2,
            label: "same".into(),
            created: "time".into(),
            kind: "BROKER".into(),
            state: "LOADING".into(),
        };
        assert!(eligible(&load, 2).is_ok());
        assert!(eligible(&load, 3).is_err());
        load.transaction = 0;
        assert!(eligible(&load, 0).is_err());
        load.transaction = 2;
        for state in ["FINISHED", "CANCELLED", "RETRY", "UNKNOWN"] {
            load.state = state.into();
            assert!(eligible(&load, 2).is_err());
        }
        load.state = "LOADING".into();
        for kind in ["INSERT", "SPARK", "ROUTINE_LOAD"] {
            load.kind = kind.into();
            assert!(eligible(&load, 2).is_err());
        }
    }
    #[test]
    fn http_origins_and_passwords_do_not_change_authentication_scope() {
        for value in [
            "http://user:secret@host",
            "http://host/path",
            "http://host/?q=x",
            "file:///tmp/data",
        ] {
            assert!(origin(value).is_err());
        }
        let options = "doris://user:p%40ss%2F%25%E4%B8%AD@localhost/app"
            .parse::<DorisConnectOptions>()
            .unwrap();
        assert_eq!(
            credentials(&options).unwrap(),
            ("user".into(), "p@ss/%中".into())
        );
        let options = "doris://user@localhost/app?ssl-key=administrator.pem"
            .parse::<DorisConnectOptions>()
            .unwrap();
        assert!(credentials(&options).is_err());
    }
    #[test]
    fn redirects_are_exact_registered_nodes_and_never_supply_credentials() {
        let http = Http {
            client: Client::new(),
            base: origin("http://fe:8030").unwrap(),
            user: "admin".into(),
            password: "secret".into(),
            frontend: Default::default(),
            backend_host: "be".into(),
            backend_port: 8040,
            backend_id: "1".into(),
            backend_started: "time".into(),
        };
        let target = http
            .target(
                "http://untrusted:ignored@be:8040/api/app/_stream_load_2pc",
                "app",
            )
            .unwrap();
        assert_eq!(target.as_str(), "http://be:8040/api/app/_stream_load_2pc");
        for value in [
            "http://other:8040/api/app/_stream_load_2pc",
            "http://be:8041/api/app/_stream_load_2pc",
            "http://be:8040/api/other/_stream_load_2pc",
            "http://be:8040/api/app/_stream_load_2pc?label=same",
            "http://be:8040/api/app/_stream_load_2pc#x",
        ] {
            assert!(http.target(value, "app").is_err());
        }
        let secure = Http {
            base: origin("https://fe:8030").unwrap(),
            ..http
        };
        assert!(secure.target(target.as_str(), "app").is_err());
    }
    #[test]
    fn malformed_or_ambiguous_http_identity_is_rejected() {
        for value in [
            serde_json::json!({"code":1}),
            serde_json::json!({
                "code": 0,
                "data": {"columnNames":["Name"],"rows":[]}
            }),
            serde_json::json!({
                "code": 0,
                "data": {"columnNames":["Name","Name"],"rows":[["one","two"]]}
            }),
            serde_json::json!({
                "code": 0,
                "data": {"columnNames":["Name"],"rows":[[1]]}
            }),
        ] {
            assert!(fe_row(&value).is_err());
        }
    }
}
