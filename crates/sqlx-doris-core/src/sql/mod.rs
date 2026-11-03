//! Lexical policies shared by trusted migrations and checked-query metadata.
pub(crate) mod migration;
#[cfg(feature = "macro-support")]
pub(crate) mod projection;
