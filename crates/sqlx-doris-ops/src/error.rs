/// Errors from Doris operation and Stream Load helpers. Connections and ordinary
/// SQLx queries return `sqlx::Error` directly; wrapped SQLx causes are preserved.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A database/protocol error from SQLx, retaining its original cause.
    #[error(transparent)]
    Database(#[from] sqlx_core::Error),
    /// Invalid local configuration, identifiers or request data.
    #[error("{0}")]
    InvalidInput(String),
    /// A known unsupported operation or type/storage combination.
    #[error("operation is not supported: {0}")]
    Unsupported(&'static str),
    /// The visibility predicate did not succeed before its deadline.
    #[error("data did not become visible before the deadline")]
    VisibilityTimeout,
    /// A materialized-view task did not reach a terminal state before its deadline.
    #[error("materialized-view task did not finish before the deadline")]
    TaskTimeout,
    /// A JSON encoding or decoding error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    /// An HTTP/transport error. A submitted write may already have committed.
    #[cfg(feature = "stream-load")]
    #[error("Stream Load request for label {label:?} failed: {source}")]
    Transport {
        /// None for Group Commit, whose job label is assigned by the server.
        label: Option<String>,
        #[source]
        /// The original HTTP client error.
        source: reqwest::Error,
    },
    /// An upload source could not be opened or validated before an HTTP hop.
    /// Earlier request bytes may already have reached the server; never replay blindly.
    #[cfg(feature = "stream-load")]
    #[error("Stream Load input for label {label:?} failed: {source}")]
    LoadSource {
        /// Ordinary-load identity, absent for server-labeled Group Commit.
        label: Option<String>,
        /// Original file, metadata or asynchronous reader failure.
        #[source]
        source: std::io::Error,
    },
    /// A received redirect could not be followed within the trusted attempt.
    #[cfg(feature = "stream-load")]
    #[error("Stream Load redirect for label {label:?} failed: {message}")]
    LoadRedirect {
        /// Ordinary-load identity, absent for server-labeled Group Commit.
        label: Option<String>,
        /// Redirect validation failure; the original attempt must be reconciled.
        message: String,
    },
    /// A read-only label observation did not return a usable API response.
    #[cfg(feature = "stream-load")]
    #[error("Stream Load observation for label {label:?} unavailable: {message}")]
    LoadObservation {
        /// Requested label; its absence from history would still be inconclusive.
        label: String,
        /// Response validation failure, distinct from a known transaction state.
        message: &'static str,
    },
    /// A Doris business failure, even when the HTTP response was successful.
    #[cfg(feature = "stream-load")]
    #[error("Stream Load failed: {receipt}")]
    Load {
        /// Full server result, including filtered rows and ErrorURL.
        receipt: Box<crate::stream_load::LoadReceipt>,
    },
}

/// Result type for Doris operation helpers, rather than ordinary SQLx queries.
pub type Result<T> = std::result::Result<T, Error>;
