use std::{
    fmt,
    future::{Future, poll_fn},
    pin::{Pin, pin},
    str::FromStr,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use crate::sqlx::{self,
    Connection, Executor, MySqlConnection, MySqlPool,
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
};

use crate::{Error, Result};

/// Selects a tested compatibility profile, independently of server version strings.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Backend {
    /// MySQL sessions without Doris settings. Managed pools select the shared
    /// TIME list domain by default; the builder can retain native coercion.
    MySql,
    /// Doris 4.1.5 behavior and explicit native extensions.
    Doris,
}

/// Doris Group Commit mode. Async acknowledges before data is necessarily visible.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum GroupCommit {
    #[default]
    /// One ordinary implicit transaction per write.
    Off,
    /// Batch writes and acknowledge after the synchronous group completes.
    Sync,
    /// Acknowledge acceptance before data is necessarily query-visible.
    Async,
}

impl GroupCommit {
    /// The Doris session variable / HTTP header value.
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

/// Known behavior of the selected profile. These are not automatic server probes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Capabilities {
    /// Native SQLx top-level begin/commit/rollback is supported by this profile.
    /// Doris is write-only, without savepoints, and requires the companion option.
    pub sqlx_transactions: bool,
    /// Server-side prepared commands can run inside an explicit transaction.
    /// Application `.bind()` support is reported separately below.
    pub prepared_in_explicit_transactions: bool,
    /// `.bind()` is supported in native transactions. Doris uses checked client
    /// binding for supported value types rather than server-side PREPARE.
    pub bound_parameters_in_explicit_transactions: bool,
    /// The profile accepts native byte bindings, including simple byte equality.
    /// Doris adapts SQL/arguments internally; its raw server BLOB encoding is
    /// still unsupported. String/binary column and SQL-shape limits apply.
    pub blob_parameters: bool,
    /// This profile exposes Doris logical types and operations.
    pub native_doris_types: bool,
}

impl Backend {
    /// Static behavior of the tested profile, without probing the server.
    pub const fn capabilities(self) -> Capabilities {
        match self {
            Self::MySql => Capabilities {
                sqlx_transactions: true,
                prepared_in_explicit_transactions: true,
                bound_parameters_in_explicit_transactions: true,
                blob_parameters: true,
                native_doris_types: false,
            },
            Self::Doris => Capabilities {
                sqlx_transactions: true,
                prepared_in_explicit_transactions: false,
                bound_parameters_in_explicit_transactions: true,
                blob_parameters: true,
                native_doris_types: true,
            },
        }
    }
}

/// A cloneable handle containing the unmodified SQLx MySQL pool.
#[derive(Clone)]
pub struct Client {
    backend: Backend,
    pool: MySqlPool,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("backend", &self.backend)
            .field("pool_size", &self.pool.size())
            .finish()
    }
}

impl Client {
    /// Wrap an application's existing pool without changing its session hooks
    /// or installing PoolBuilder's on-release readiness check.
    pub fn from_pool(backend: Backend, pool: MySqlPool) -> Self {
        Self { backend, pool }
    }
    /// `doris://` selects Doris; `mysql://` selects MySQL. Use PoolBuilder to
    /// explicitly select Doris when retaining an existing mysql:// URL.
    pub async fn connect(url: &str) -> Result<Self> {
        PoolBuilder::new(if url.starts_with("doris:") {
            Backend::Doris
        } else {
            Backend::MySql
        })
        .connect(url)
        .await
    }
    /// The explicitly selected backend profile.
    pub const fn backend(&self) -> Backend {
        self.backend
    }
    /// Selected profile plus the pool's configured native transaction option.
    /// This does not probe the server or retrofit options on an existing pool.
    pub fn capabilities(&self) -> Capabilities {
        let mut capabilities = self.backend.capabilities();
        if self.backend == Backend::Doris {
            capabilities.sqlx_transactions = self
                .pool
                .connect_options()
                .get_doris_transaction_compatibility();
            capabilities.bound_parameters_in_explicit_transactions = capabilities.sqlx_transactions;
            capabilities.blob_parameters = capabilities.sqlx_transactions;
        }
        capabilities
    }
    /// Borrow the original pool for ordinary SQLx query/executor APIs.
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
    /// Consume this handle and return the original pool.
    pub fn into_pool(self) -> MySqlPool {
        self.pool
    }
    /// Close the shared pool and wait for checked-out connections to return.
    pub async fn close(&self) {
        self.pool.close().await;
    }
}

/// Connection and pool configuration shared by both servers.
///
/// Doris-specific settings are applied once per new connection, never to an
/// arbitrary checked-out session. Existing SQLx connect options remain available.
/// On release, a connection with results that cannot be drained immediately is
/// closed. This prevents SQLx 0.9's parameter-count error from stranding a pool
/// slot; it can also replace a connection after an unfinished stream is dropped.
#[derive(Clone)]
pub struct PoolBuilder {
    backend: Backend,
    pool: MySqlPoolOptions,
    group_commit: GroupCommit,
    decimal256: bool,
    strict_cast: bool,
    mysql_time_membership_compatibility: bool,
    after_connect: Option<ConnectHook>,
}

/// An application's per-connection initialization future.
pub type ConnectFuture<'a> = Pin<Box<dyn Future<Output = sqlx::Result<()>> + Send + 'a>>;
type ConnectHook = Arc<dyn for<'a> Fn(&'a mut MySqlConnection) -> ConnectFuture<'a> + Send + Sync>;
impl fmt::Debug for PoolBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PoolBuilder")
            .field("backend", &self.backend)
            .field("pool", &self.pool)
            .field("group_commit", &self.group_commit)
            .field("decimal256", &self.decimal256)
            .field("strict_cast", &self.strict_cast)
            .field(
                "mysql_time_membership_compatibility",
                &self.mysql_time_membership_compatibility,
            )
            .finish_non_exhaustive()
    }
}

impl PoolBuilder {
    /// Use SQLx pool defaults with an immediate readiness check on release,
    /// Group Commit off, decimal256 and strict casts on Doris.
    pub fn new(backend: Backend) -> Self {
        Self {
            backend,
            pool: MySqlPoolOptions::new().after_release(|connection, _| {
                Box::pin(async move {
                    // SQLx can leave Waiting::Result behind after a local parameter
                    // count error, before COM_STMT_EXECUTE has even been sent. Its
                    // normal on-release ping would then wait for a nonexistent row.
                    // Healthy connections finish flush immediately, without an RTT.
                    let mut ready = pin!(connection.flush());
                    poll_fn(|cx| match ready.as_mut().poll(cx) {
                        Poll::Ready(result) => Poll::Ready(result.map(|()| true)),
                        Poll::Pending => Poll::Ready(Ok(false)),
                    })
                    .await
                })
            }),
            group_commit: GroupCommit::Off,
            decimal256: true,
            strict_cast: true,
            mysql_time_membership_compatibility: true,
            after_connect: None,
        }
    }
    /// Set SQLx's maximum pool size.
    pub fn max_connections(mut self, value: u32) -> Self {
        self.pool = self.pool.max_connections(value);
        self
    }
    /// Set SQLx's minimum pool size.
    pub fn min_connections(mut self, value: u32) -> Self {
        self.pool = self.pool.min_connections(value);
        self
    }
    /// Limit the time spent acquiring a connection from the pool.
    pub fn acquire_timeout(mut self, value: Duration) -> Self {
        self.pool = self.pool.acquire_timeout(value);
        self
    }
    /// Retire idle connections after this duration; None disables retirement.
    pub fn idle_timeout(mut self, value: Option<Duration>) -> Self {
        self.pool = self.pool.idle_timeout(value);
        self
    }
    /// Retire connections after this lifetime; None disables age-based retirement.
    pub fn max_lifetime(mut self, value: Option<Duration>) -> Self {
        self.pool = self.pool.max_lifetime(value);
        self
    }
    /// Run application initialization after the selected profile's settings.
    pub fn after_connect<F>(mut self, hook: F) -> Self
    where
        F: for<'a> Fn(&'a mut MySqlConnection) -> ConnectFuture<'a> + Send + Sync + 'static,
    {
        self.after_connect = Some(Arc::new(hook));
        self
    }
    /// Set Group Commit on every new Doris connection; ignored for MySQL.
    pub fn group_commit(mut self, value: GroupCommit) -> Self {
        self.group_commit = value;
        self
    }
    /// Explicitly enable/disable decimal256 per Doris connection; ignored for MySQL.
    pub fn decimal256(mut self, value: bool) -> Self {
        self.decimal256 = value;
        self
    }
    /// Reject invalid/overflowing Doris CASTs instead of producing NULL. Enabled
    /// by default; false explicitly selects the server's permissive conversion.
    /// Ignored for MySQL, whose SQL mode remains under SQLx/application control.
    pub fn strict_cast(mut self, value: bool) -> Self {
        self.strict_cast = value;
        self
    }

    /// Normalize simple MySQL TIME IN/NOT IN lists to the shared TIME(6) domain.
    /// Enabled for managed MySQL pools to preserve this profile's zero-value,
    /// mixed-string and NULL behavior. Set false to retain native MySQL coercion
    /// and errors. This does not change SQLx MySql type identity or parameter
    /// types, and is ignored for Doris, which uses its own parameter adaptation.
    pub fn mysql_time_membership_compatibility(mut self, enabled: bool) -> Self {
        self.mysql_time_membership_compatibility = enabled;
        self
    }

    /// Connect using a mysql:// URL, or doris:// with the Doris profile.
    pub async fn connect(self, url: &str) -> Result<Client> {
        let normalized = if let Some(rest) = url.strip_prefix("doris:") {
            if self.backend != Backend::Doris {
                return Err(Error::InvalidInput(
                    "doris:// requires the Doris profile".into(),
                ));
            }
            format!("mysql:{rest}")
        } else {
            url.to_owned()
        };
        self.connect_with(MySqlConnectOptions::from_str(&normalized)?)
            .await
    }

    /// Try an explicit ordered list of endpoints when creating a pool.
    /// Each endpoint gets the configured acquire timeout and normal session hooks.
    /// Hooks can run on multiple attempted connections, as with ordinary SQLx;
    /// use them for session initialization, not business writes.
    /// Returns the first connected pool, or the last connection error. This is
    /// connection setup only: an existing pool stays on its chosen endpoint,
    /// in-flight queries are never moved, and writes are never replayed.
    pub async fn connect_any_with(
        self,
        endpoints: impl IntoIterator<Item = MySqlConnectOptions>,
    ) -> Result<Client> {
        let mut error = Error::InvalidInput("at least one endpoint is required".into());
        for options in endpoints {
            match self.clone().connect_with(options).await {
                Ok(client) => return Ok(client),
                Err(cause) => error = cause,
            }
        }
        Err(error)
    }

    /// Preserve SQLx TLS, credentials and cache settings. The selected profile
    /// owns the Doris and MySQL TIME membership compatibility options.
    pub async fn connect_with(self, options: MySqlConnectOptions) -> Result<Client> {
        let backend = self.backend;
        let options = options
            .doris_transaction_compatibility(backend == Backend::Doris)
            .time_membership_compatibility(
                backend == Backend::MySql && self.mysql_time_membership_compatibility,
            );
        let mut pool = self.pool;
        if backend == Backend::Doris || self.after_connect.is_some() {
            let mode = self.group_commit;
            let decimal256 = self.decimal256;
            let strict_cast = self.strict_cast;
            let hook = self.after_connect;
            pool = pool.after_connect(move |conn, _| {
                let hook = hook.clone();
                Box::pin(async move {
                    if backend == Backend::Doris {
                        conn.execute(mode.statement()).await?;
                        conn.execute(if decimal256 {
                            "SET enable_decimal256 = true"
                        } else {
                            "SET enable_decimal256 = false"
                        })
                        .await?;
                        conn.execute(if strict_cast {
                            "SET enable_strict_cast = true"
                        } else {
                            "SET enable_strict_cast = false"
                        })
                        .await?;
                    }
                    if let Some(hook) = hook {
                        hook(conn).await?;
                    }
                    Ok(())
                })
            });
        }
        Ok(Client {
            backend,
            pool: pool.connect_with(options).await?,
        })
    }
}
