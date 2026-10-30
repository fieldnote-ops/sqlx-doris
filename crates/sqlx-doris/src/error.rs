/// Errors from connection setup and Doris operations. SQLx causes are preserved.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A database/protocol error from SQLx, retaining its original cause.
    #[error(transparent)]
    Database(#[from] crate::sqlx::Error),
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
    /// A Doris business failure, even when the HTTP response was successful.
    #[cfg(feature = "stream-load")]
    #[error("Stream Load failed: {receipt}")]
    Load {
        /// Full server result, including filtered rows and ErrorURL.
        receipt: Box<crate::stream_load::LoadReceipt>,
    },
}

/// Library result type.
pub type Result<T> = std::result::Result<T, Error>;
