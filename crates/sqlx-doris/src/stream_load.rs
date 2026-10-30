//! Optional HTTP Stream Load. Uses the explicitly configured trusted Doris
//! cluster endpoints; follows FE redirects to BE with the same credentials.
//! No retry is made after a transport failure or an uncertain write result.

use crate::{
    Error, GroupCommit, Result,
    operations::{Column, DataType, TableDefinition, TableName},
};
use reqwest::{
    Body, Url,
    header::{HeaderMap, HeaderName, HeaderValue, LOCATION},
};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};

/// The load's acknowledgement and visibility state, separate from row counts.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoadState {
    /// The ordinary load succeeded and is query-visible.
    Visible,
    /// Async Group Commit accepted the request; visibility still needs checking.
    Accepted,
    /// The transaction committed but publication did not finish before timeout.
    Committed,
    /// A two-phase load has been prepared and still requires an explicit commit.
    Prepared,
    /// The label refers to an existing job; no new write was performed.
    AlreadyExists {
        /// The existing job reports FINISHED and is already visible.
        finished: bool,
    },
    /// A failed or unknown server status; send methods return Error::Load.
    Failed,
}

/// Doris's result fields. Unknown fields can be added server-side without
/// affecting decoding. A failed load is returned as Error::Load by send methods.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LoadReceipt {
    /// Unmodified server Status.
    pub status: String,
    /// Server label, or the submitted ordinary-load label if omitted by the reply.
    #[serde(default)]
    pub label: String,
    /// Server transaction ID; negative sentinels can occur before creation.
    #[serde(default)]
    pub txn_id: Option<i64>,
    /// Server diagnostic Message.
    #[serde(default)]
    pub message: String,
    /// Existing label's job state, such as RUNNING or FINISHED.
    #[serde(default)]
    pub existing_job_status: Option<String>,
    /// Rows examined by the load.
    #[serde(default)]
    pub number_total_rows: u64,
    /// Rows reported loaded; inspect state() before interpreting visibility.
    #[serde(default)]
    pub number_loaded_rows: u64,
    /// Rows rejected by data-quality checks, even in a tolerated successful load.
    #[serde(default)]
    pub number_filtered_rows: u64,
    /// Rows excluded by the request's WHERE condition.
    #[serde(default)]
    pub number_unselected_rows: u64,
    /// Bytes received by the server.
    #[serde(default)]
    pub load_bytes: u64,
    /// Server load duration in milliseconds.
    #[serde(default)]
    pub load_time_ms: u64,
    /// Optional server diagnostic URL for rejected rows; never fetched automatically.
    #[serde(default, rename = "ErrorURL")]
    pub error_url: Option<String>,
    /// This is a prepared two-phase load, which still requires a separate commit.
    #[serde(default, deserialize_with = "wire_bool")]
    pub two_phase_commit: bool,
    /// The server used Group Commit for this load.
    #[serde(default)]
    pub group_commit: bool,
    /// Actual server Group Commit mode, which determines acknowledgement semantics.
    #[serde(default)]
    pub group_commit_mode: Option<String>,
    /// Client-requested mode, retained separately when older servers omit the
    /// actual mode. An explicit server mode takes precedence.
    #[serde(default)]
    pub requested_group_commit_mode: Option<String>,
    /// Number of actual HTTP redirects followed for this request.
    #[serde(skip)]
    pub redirects: u8,
}
fn wire_bool<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<bool, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Value {
        Bool(bool),
        Text(String),
    }
    match Value::deserialize(d)? {
        Value::Bool(value) => Ok(value),
        Value::Text(value) if value == "true" => Ok(true),
        Value::Text(value) if value == "false" => Ok(false),
        _ => Err(serde::de::Error::custom("expected true or false")),
    }
}
impl fmt::Display for LoadReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "label {}: {}: {}", self.label, self.status, self.message)
    }
}
impl LoadReceipt {
    /// Classify acknowledgement, visibility, duplication and failure separately.
    pub fn state(&self) -> LoadState {
        let acceptance_only = self.group_commit_mode.as_deref().map_or_else(
            || {
                self.group_commit
                    && !self
                        .requested_group_commit_mode
                        .as_deref()
                        .is_some_and(|mode| mode.eq_ignore_ascii_case("sync_mode"))
            },
            |mode| mode.eq_ignore_ascii_case("async_mode"),
        );
        match self.status.as_str() {
            "Success" if self.two_phase_commit => LoadState::Prepared,
            "Success" if acceptance_only => LoadState::Accepted,
            "Success" => LoadState::Visible,
            "Publish Timeout" => LoadState::Committed,
            "Label Already Exists" => LoadState::AlreadyExists {
                finished: self.existing_job_status.as_deref() == Some("FINISHED"),
            },
            _ => LoadState::Failed,
        }
    }
    /// True only for ordinary Success or an existing FINISHED label. Filtered
    /// rows may still exist; inspect number_filtered_rows separately.
    pub fn is_visible(&self) -> bool {
        matches!(
            self.state(),
            LoadState::Visible | LoadState::AlreadyExists { finished: true }
        )
    }
}

/// Wire format of a raw byte/file upload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LoadFormat {
    /// JSON row array, using strip_outer_array=true.
    Json,
    /// One JSON row per line, using read_json_by_line=true.
    JsonLines,
    /// Delimited text; set column_separator and related headers as needed.
    Csv,
    /// Parquet file bytes; files are streamed without client-side decoding.
    Parquet,
    /// ORC file bytes; files are streamed without client-side decoding.
    Orc,
}

/// Reusable HTTP client for a trusted Doris cluster. Passwords are never printed
/// by Debug. Use with_client_builder to configure custom TLS roots or timeouts.
#[derive(Clone)]
pub struct StreamLoadClient {
    http: reqwest::Client,
    endpoint: Url,
    backend_override: Option<Url>,
    username: String,
    password: Option<String>,
}
impl fmt::Debug for StreamLoadClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamLoadClient")
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}
impl StreamLoadClient {
    /// Create a client with a 120-second request timeout and explicit Basic auth.
    /// Endpoint redirects are trusted cluster endpoints and retain credentials.
    pub fn new(
        endpoint: &str,
        username: impl Into<String>,
        password: Option<String>,
    ) -> Result<Self> {
        Self::with_client_builder(
            endpoint,
            username,
            password,
            reqwest::Client::builder().timeout(Duration::from_secs(120)),
        )
    }
    /// Customize TLS roots, proxy and timeouts. Automatic redirects/retries are
    /// disabled so the library handles redirects and never replays uncertain writes.
    pub fn with_client_builder(
        endpoint: &str,
        username: impl Into<String>,
        password: Option<String>,
        builder: reqwest::ClientBuilder,
    ) -> Result<Self> {
        let endpoint = Url::parse(endpoint).map_err(|e| Error::InvalidInput(e.to_string()))?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(Error::InvalidInput(
                "use an HTTP(S) endpoint and explicit credentials".into(),
            ));
        }
        let http = builder
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(|source| Error::Transport {
                label: None,
                source,
            })?;
        Ok(Self {
            http,
            endpoint,
            backend_override: None,
            username: username.into(),
            password,
        })
    }
    /// Map a BE redirect's authority to a reachable endpoint (for a tunnel or
    /// NAT). Its returned path and query remain unchanged.
    pub fn backend_endpoint(mut self, endpoint: &str) -> Result<Self> {
        let url = Url::parse(endpoint).map_err(|e| Error::InvalidInput(e.to_string()))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(Error::InvalidInput(
                "backend endpoint must be HTTP(S)".into(),
            ));
        }
        self.backend_override = Some(url);
        Ok(self)
    }
    /// Start an ordinary load with a generated label, strict_mode=true and
    /// max_filter_ratio=0. Use load_table for structured complex JSON columns.
    pub fn load<'a>(&'a self, table: &TableName) -> Result<LoadRequest<'a>> {
        table.validate()?;
        let database = table.database.as_deref().ok_or_else(|| {
            Error::InvalidInput("Stream Load requires a database-qualified table".into())
        })?;
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .map_err(|_| Error::InvalidInput("endpoint cannot contain path segments".into()))?
            .pop_if_empty()
            .extend(["api", database, &table.table, "_stream_load"]);
        Ok(LoadRequest {
            client: self,
            url,
            label: format!("sqlx_doris_{}", uuid::Uuid::new_v4().simple()),
            explicit_label: false,
            format: LoadFormat::Json,
            headers: HeaderMap::new(),
            schema: None,
        })
    }
    /// JSON row writes with schema-informed encoding for ARRAY/MAP/STRUCT.
    /// Doris 4.1.5's JSON loader otherwise interprets nested JSON escapes using
    /// its CAST grammar. The schema is used by send_json, not raw bytes/files.
    pub fn load_table<'a>(&'a self, table: &TableDefinition) -> Result<LoadRequest<'a>> {
        let mut request = self.load(&table.name)?;
        request.schema = Some(table.columns.clone());
        Ok(request)
    }
}

/// One load request. Sending consumes the builder; no automatic write retry occurs.
pub struct LoadRequest<'a> {
    client: &'a StreamLoadClient,
    url: Url,
    label: String,
    explicit_label: bool,
    format: LoadFormat,
    headers: HeaderMap,
    schema: Option<Vec<Column>>,
}
impl fmt::Debug for LoadRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoadRequest")
            .field("url", &self.url)
            .field("label", &self.label)
            .field("format", &self.format)
            .finish()
    }
}

enum Source {
    Bytes(Vec<u8>),
    File(PathBuf),
}
impl Source {
    async fn body(&self) -> std::io::Result<(Body, u64)> {
        match self {
            Self::Bytes(bytes) => Ok((Body::from(bytes.clone()), bytes.len() as u64)),
            Self::File(path) => {
                let file = tokio::fs::File::open(path).await?;
                let len = file.metadata().await?.len();
                Ok((
                    Body::wrap_stream(tokio_util::io::ReaderStream::new(file)),
                    len,
                ))
            }
        }
    }
}

impl LoadRequest<'_> {
    /// Use a stable label to identify this batch. Duplicate labels do not imply
    /// that a different payload was accepted; callers must keep batch identity.
    pub fn label(mut self, label: impl Into<String>) -> Self {
        self.label = label.into();
        self.explicit_label = true;
        self
    }
    /// Group Commit assigns its own server-side label and cannot use a user
    /// label. Async mode acknowledges acceptance before visibility.
    pub fn group_commit(mut self, mode: GroupCommit) -> Self {
        self.headers
            .insert("group_commit", HeaderValue::from_static(mode.as_str()));
        self
    }
    /// Select the raw bytes/file format. send_json always uses JSON row arrays.
    pub fn format(mut self, format: LoadFormat) -> Self {
        self.format = format;
        self
    }
    /// Set a Doris import option, e.g. columns, time_zone or partial-update mode.
    /// Overrides corresponding defaults; label is routed through label().
    pub fn header(mut self, name: &str, value: &str) -> Result<Self> {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        let parsed =
            HeaderValue::from_str(value).map_err(|e| Error::InvalidInput(e.to_string()))?;
        if name == "label" {
            return Ok(self.label(value));
        }
        self.headers.insert(name, parsed);
        Ok(self)
    }
    /// Serialize a JSON row array. The request sets strip_outer_array=true.
    pub async fn send_json<T: Serialize + ?Sized>(mut self, rows: &T) -> Result<LoadReceipt> {
        self.format = LoadFormat::Json;
        let payload = if let Some(schema) = &self.schema {
            let mut rows = crate::types::json::to_value(rows)?;
            let rows = rows
                .as_array_mut()
                .ok_or_else(|| Error::InvalidInput("send_json requires a JSON row array".into()))?;
            for row in rows.iter_mut() {
                let row = row
                    .as_object_mut()
                    .ok_or_else(|| Error::InvalidInput("each JSON row must be an object".into()))?;
                for column in schema {
                    if matches!(
                        column.data_type,
                        DataType::Array(_) | DataType::Map(_, _) | DataType::Struct(_)
                    ) && let Some(value) = row.get_mut(&column.name).filter(|v| !v.is_null())
                    {
                        let text = crate::types::text::to_doris(value)
                            .map_err(|e| Error::InvalidInput(e.to_string()))?;
                        *value = serde_json::Value::String(text);
                    }
                }
            }
            crate::types::json::to_vec(rows)?
        } else {
            crate::types::json::to_vec(rows)?
        };
        self.send(Source::Bytes(payload)).await
    }
    /// Send bytes unchanged in the selected format, without schema conversion.
    pub async fn send_bytes(self, payload: impl Into<Vec<u8>>) -> Result<LoadReceipt> {
        self.send(Source::Bytes(payload.into())).await
    }
    /// Reopens the file after an FE redirect; does not buffer the full file.
    pub async fn send_file(self, path: impl AsRef<Path>) -> Result<LoadReceipt> {
        self.send(Source::File(path.as_ref().to_owned())).await
    }

    async fn send(self, source: Source) -> Result<LoadReceipt> {
        let mut headers = HeaderMap::new();
        headers.insert("strict_mode", HeaderValue::from_static("true"));
        headers.insert("max_filter_ratio", HeaderValue::from_static("0"));
        headers.insert("expect", HeaderValue::from_static("100-continue"));
        let format = match self.format {
            LoadFormat::Json | LoadFormat::JsonLines => "json",
            LoadFormat::Csv => "csv",
            LoadFormat::Parquet => "parquet",
            LoadFormat::Orc => "orc",
        };
        headers.insert("format", HeaderValue::from_static(format));
        if self.format == LoadFormat::Json {
            headers.insert("strip_outer_array", HeaderValue::from_static("true"));
        }
        if self.format == LoadFormat::JsonLines {
            headers.insert("read_json_by_line", HeaderValue::from_static("true"));
        }
        headers.extend(self.headers);
        let requested_group_commit_mode = headers
            .get("group_commit")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let group_commit = headers
            .get("group_commit")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.eq_ignore_ascii_case("sync_mode") || v.eq_ignore_ascii_case("async_mode")
            });
        if group_commit && self.explicit_label {
            return Err(Error::InvalidInput(
                "Group Commit and a user Label cannot be combined".into(),
            ));
        }
        let label = if group_commit { None } else { Some(self.label) };
        if let Some(label) = &label {
            headers.insert(
                "label",
                HeaderValue::from_str(label).map_err(|e| Error::InvalidInput(e.to_string()))?,
            );
        }
        let mut url = self.url;
        for hop in 0..=3 {
            let (body, length) = source
                .body()
                .await
                .map_err(|e| Error::InvalidInput(format!("cannot read Stream Load input: {e}")))?;
            let response = self
                .client
                .http
                .put(url.clone())
                .basic_auth(&self.client.username, self.client.password.as_deref())
                .headers(headers.clone())
                .header("content-length", length)
                .body(body)
                .send()
                .await
                .map_err(|source| Error::Transport {
                    label: label.clone(),
                    source,
                })?;
            if matches!(response.status().as_u16(), 307 | 308) {
                if hop == 3 {
                    return Err(Error::InvalidInput("too many Doris load redirects".into()));
                }
                let location = response
                    .headers()
                    .get(LOCATION)
                    .ok_or_else(|| Error::InvalidInput("Doris redirect has no Location".into()))?
                    .to_str()
                    .map_err(|e| Error::InvalidInput(e.to_string()))?;
                let mut next = url
                    .join(location)
                    .map_err(|e| Error::InvalidInput(e.to_string()))?;
                if let Some(backend) = &self.client.backend_override {
                    next.set_scheme(backend.scheme())
                        .map_err(|_| Error::InvalidInput("invalid backend scheme".into()))?;
                    next.set_host(backend.host_str())
                        .map_err(|e| Error::InvalidInput(e.to_string()))?;
                    next.set_port(backend.port())
                        .map_err(|_| Error::InvalidInput("invalid backend port".into()))?;
                }
                if url.scheme() == "https" && next.scheme() != "https" {
                    return Err(Error::InvalidInput(
                        "Doris redirect would downgrade HTTPS".into(),
                    ));
                }
                url = next;
                continue;
            }
            let response = response
                .error_for_status()
                .map_err(|source| Error::Transport {
                    label: label.clone(),
                    source,
                })?;
            let mut receipt: LoadReceipt =
                response.json().await.map_err(|source| Error::Transport {
                    label: label.clone(),
                    source,
                })?;
            if receipt.label.is_empty() {
                receipt.label = label.unwrap_or_default();
            }
            receipt.requested_group_commit_mode = requested_group_commit_mode.clone();
            receipt.redirects = hop;
            if receipt.state() == LoadState::Failed {
                return Err(Error::Load {
                    receipt: Box::new(receipt),
                });
            }
            return Ok(receipt);
        }
        unreachable!("redirect loop always returns")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn result_states_preserve_uncertainty_and_duplicate_labels() {
        let mut receipt: LoadReceipt =
            serde_json::from_str(r#"{"Status":"Publish Timeout","Label":"x","TxnId":42}"#).unwrap();
        assert_eq!(receipt.state(), LoadState::Committed);
        assert!(!receipt.is_visible());
        receipt.status = "Success".into();
        receipt.group_commit_mode = Some("async_mode".into());
        assert_eq!(receipt.state(), LoadState::Accepted);
        receipt.status = "Label Already Exists".into();
        receipt.existing_job_status = Some("RUNNING".into());
        assert!(!receipt.is_visible());
        receipt.existing_job_status = Some("FINISHED".into());
        assert!(receipt.is_visible());
        let prepared: LoadReceipt =
            serde_json::from_str(r#"{"Status":"Success","TwoPhaseCommit":"true"}"#).unwrap();
        assert_eq!(prepared.state(), LoadState::Prepared);
        assert!(!prepared.is_visible());
    }
}
