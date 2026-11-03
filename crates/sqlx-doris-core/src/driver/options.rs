use super::*;
use sqlx_core::{
    connection::{ConnectOptions, LogSettings},
    net::tls::CertificateInput,
};
use std::{path::Path, str::FromStr, time::Duration};
use url::Url;

/// TLS negotiation and certificate verification policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DorisSslMode {
    /// Use an unencrypted connection.
    Disabled,
    /// Negotiate TLS when offered by the FE.
    #[default]
    Preferred,
    /// Require encryption without checking the server certificate.
    Required,
    /// Require encryption and a trusted certificate.
    VerifyCa,
    /// Require encryption, a trusted certificate and a matching hostname.
    VerifyIdentity,
}
/// Doris FE connection configuration. Defaults to localhost:9030, user root.
#[derive(Clone)]
pub struct DorisConnectOptions {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) username: String,
    pub(crate) password: Option<String>,
    pub(crate) database: Option<String>,
    pub(crate) ssl_mode: DorisSslMode,
    pub(crate) ca: Option<CertificateInput>,
    pub(crate) cert: Option<CertificateInput>,
    pub(crate) key: Option<CertificateInput>,
    pub(crate) cache_capacity: usize,
    pub(crate) max_packet_size: usize,
    pub(crate) group_commit: crate::GroupCommit,
    pub(crate) decimal256: bool,
    pub(crate) strict_cast: bool,
    pub(crate) log: LogSettings,
    #[cfg(feature = "migrate")]
    pub(crate) migration_replication: u16,
    #[cfg(feature = "migrate")]
    pub(crate) migration_timeout: Duration,
    #[cfg(feature = "migrate")]
    pub(crate) migration_lock_timeout: Duration,
}
impl std::fmt::Debug for DorisConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DorisConnectOptions")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username)
            .field("database", &self.database)
            .field("ssl_mode", &self.ssl_mode)
            .field("statement_cache_capacity", &self.cache_capacity)
            .finish_non_exhaustive()
    }
}
impl Default for DorisConnectOptions {
    fn default() -> Self {
        Self::new()
    }
}
impl DorisConnectOptions {
    /// Construct the default options.
    pub fn new() -> Self {
        Self {
            host: "localhost".into(),
            port: 9030,
            username: "root".into(),
            password: None,
            database: None,
            ssl_mode: DorisSslMode::Preferred,
            ca: None,
            cert: None,
            key: None,
            cache_capacity: 100,
            max_packet_size: 64 * 1024 * 1024,
            group_commit: crate::GroupCommit::Off,
            decimal256: true,
            strict_cast: true,
            log: LogSettings::default(),
            #[cfg(feature = "migrate")]
            migration_replication: 3,
            #[cfg(feature = "migrate")]
            migration_timeout: Duration::from_secs(300),
            #[cfg(feature = "migrate")]
            migration_lock_timeout: Duration::from_secs(30),
        }
    }
    /// Replicas for migration metadata and lock objects (default 3). Use 1 only
    /// for a dedicated single-BE development fixture.
    #[cfg(feature = "migrate")]
    pub fn migration_replication(mut self, value: u16) -> Self {
        self.migration_replication = value;
        self
    }
    /// Maximum wait for a submitted schema-change job (default 300 seconds).
    #[cfg(feature = "migrate")]
    pub fn migration_timeout(mut self, value: Duration) -> Self {
        self.migration_timeout = value;
        self
    }
    /// Wait for the durable migration lock (default 30 seconds). Stale locks are
    /// never automatically removed when this timeout expires.
    /// Lock acquisition returns a configuration error if the duration exceeds
    /// the clock range, before creating coordination metadata.
    #[cfg(feature = "migrate")]
    pub fn migration_lock_timeout(mut self, value: Duration) -> Self {
        self.migration_lock_timeout = value;
        self
    }
    /// FE hostname or address.
    pub fn host(mut self, v: &str) -> Self {
        self.host = v.into();
        self
    }
    /// FE query port (default 9030).
    pub fn port(mut self, v: u16) -> Self {
        self.port = v;
        self
    }
    /// Account name.
    pub fn username(mut self, v: &str) -> Self {
        self.username = v.into();
        self
    }
    /// Account password. Never included in Debug output.
    pub fn password(mut self, v: &str) -> Self {
        self.password = Some(v.into());
        self
    }
    /// Default database.
    pub fn database(mut self, v: &str) -> Self {
        self.database = Some(v.into());
        self
    }
    /// TLS policy.
    pub fn ssl_mode(mut self, v: DorisSslMode) -> Self {
        self.ssl_mode = v;
        self
    }
    /// Trusted CA bundle in PEM format.
    pub fn ssl_ca(mut self, v: impl AsRef<Path>) -> Self {
        self.ca = Some(CertificateInput::File(v.as_ref().into()));
        self
    }
    /// PEM client certificate for mutual TLS.
    pub fn ssl_client_cert(mut self, v: impl AsRef<Path>) -> Self {
        self.cert = Some(CertificateInput::File(v.as_ref().into()));
        self
    }
    /// PEM client private key for mutual TLS.
    pub fn ssl_client_key(mut self, v: impl AsRef<Path>) -> Self {
        self.key = Some(CertificateInput::File(v.as_ref().into()));
        self
    }
    /// Number of server statements cached per connection; zero disables caching.
    pub fn statement_cache_capacity(mut self, v: usize) -> Self {
        self.cache_capacity = v;
        self
    }
    /// Maximum assembled incoming or outgoing packet, in bytes (default 64 MiB).
    pub fn max_packet_size(mut self, v: usize) -> Self {
        self.max_packet_size = v;
        self
    }
    /// Group Commit mode installed once on each new connection.
    pub fn group_commit(mut self, v: crate::GroupCommit) -> Self {
        self.group_commit = v;
        self
    }
    /// Enable DECIMAL256 in each session (default true).
    pub fn decimal256(mut self, v: bool) -> Self {
        self.decimal256 = v;
        self
    }
    /// Reject invalid casts instead of receiving NULL (default true).
    pub fn strict_cast(mut self, v: bool) -> Self {
        self.strict_cast = v;
        self
    }
    /// Configured hostname.
    pub fn get_host(&self) -> &str {
        &self.host
    }
    /// Configured query port.
    pub fn get_port(&self) -> u16 {
        self.port
    }
    /// Configured username.
    pub fn get_username(&self) -> &str {
        &self.username
    }
    /// Configured database.
    pub fn get_database(&self) -> Option<&str> {
        self.database.as_deref()
    }
}
impl FromStr for DorisConnectOptions {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Error> {
        Self::from_url(&Url::parse(s).map_err(|e| Error::Configuration(e.into()))?)
    }
}
impl ConnectOptions for DorisConnectOptions {
    type Connection = DorisConnection;
    fn from_url(url: &Url) -> Result<Self, Error> {
        if url.scheme() != "doris" {
            return Err(Error::Configuration("use a doris:// URL".into()));
        }
        fn decode(s: &str) -> Result<String, Error> {
            percent_encoding::percent_decode_str(s)
                .decode_utf8()
                .map(|v| v.into_owned())
                .map_err(|e| Error::Configuration(e.into()))
        }
        let mut o = Self::new();
        o.host = url
            .host_str()
            .unwrap_or("localhost")
            .trim_matches(['[', ']'])
            .into();
        o.port = url.port().unwrap_or(9030);
        if !url.username().is_empty() {
            o.username = decode(url.username())?;
        }
        o.password = url.password().map(decode).transpose()?;
        if !url.path().trim_start_matches('/').is_empty() {
            o.database = Some(decode(url.path().trim_start_matches('/'))?);
        }
        for (k, v) in url.query_pairs() {
            match k.as_ref() {
                "ssl-mode" => {
                    o.ssl_mode = match v.to_ascii_lowercase().as_str() {
                        "disabled" => DorisSslMode::Disabled,
                        "preferred" => DorisSslMode::Preferred,
                        "required" => DorisSslMode::Required,
                        "verify-ca" => DorisSslMode::VerifyCa,
                        "verify-identity" => DorisSslMode::VerifyIdentity,
                        _ => return Err(Error::Configuration("unknown ssl-mode".into())),
                    }
                }
                "ssl-ca" => o = o.ssl_ca(v.as_ref()),
                "ssl-cert" => o = o.ssl_client_cert(v.as_ref()),
                "ssl-key" => o = o.ssl_client_key(v.as_ref()),
                "statement-cache-capacity" => {
                    o.cache_capacity = v.parse().map_err(|e| Error::Configuration(Box::new(e)))?
                }
                _ => {
                    return Err(Error::Configuration(
                        format!("unknown Doris URL option: {k}").into(),
                    ));
                }
            }
        }
        Ok(o)
    }
    fn to_url_lossy(&self) -> Url {
        let mut u = Url::parse("doris://localhost").unwrap();
        u.set_host(Some(&self.host)).unwrap();
        u.set_port(Some(self.port)).unwrap();
        u.set_username(&self.username).unwrap();
        u.set_password(self.password.as_deref()).unwrap();
        if let Some(db) = &self.database {
            u.path_segments_mut().unwrap().push(db);
        }
        let mode = match self.ssl_mode {
            DorisSslMode::Disabled => "disabled",
            DorisSslMode::Preferred => "preferred",
            DorisSslMode::Required => "required",
            DorisSslMode::VerifyCa => "verify-ca",
            DorisSslMode::VerifyIdentity => "verify-identity",
        };
        u.query_pairs_mut()
            .append_pair("ssl-mode", mode)
            .append_pair("statement-cache-capacity", &self.cache_capacity.to_string());
        for (k, v) in [
            ("ssl-ca", &self.ca),
            ("ssl-cert", &self.cert),
            ("ssl-key", &self.key),
        ] {
            if let Some(CertificateInput::File(p)) = v {
                u.query_pairs_mut().append_pair(k, &p.to_string_lossy());
            }
        }
        u
    }
    async fn connect(&self) -> Result<DorisConnection, Error> {
        DorisConnection::establish(self).await
    }
    fn log_statements(mut self, v: log::LevelFilter) -> Self {
        self.log.log_statements(v);
        self
    }
    fn log_slow_statements(mut self, v: log::LevelFilter, d: Duration) -> Self {
        self.log.log_slow_statements(v, d);
        self
    }
}

/// Doris Group Commit mode. Async acknowledgement precedes query visibility.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupCommit {
    /// Ordinary implicit write transactions.
    #[default]
    Off,
    /// Acknowledge after the synchronous group completes.
    Sync,
    /// Acknowledge acceptance before data is necessarily visible.
    Async,
}
impl GroupCommit {
    /// Session-variable and Stream Load header value.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off_mode",
            Self::Sync => "sync_mode",
            Self::Async => "async_mode",
        }
    }
    pub(crate) const fn statement(self) -> &'static str {
        match self {
            Self::Off => "SET group_commit = off_mode",
            Self::Sync => "SET group_commit = sync_mode",
            Self::Async => "SET group_commit = async_mode",
        }
    }
}
