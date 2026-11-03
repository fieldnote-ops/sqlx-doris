use clap::Args as ClapArgs;
#[cfg(feature = "kafka-admin")]
use sqlx_doris_ops::kafka;
use std::path::PathBuf;
#[derive(ClapArgs)]
pub(crate) struct Options {
    /// Original managed Routine receipt; selects and verifies its database.
    #[arg(long)]
    pub receipt: Option<PathBuf>,
    /// Managed identity to attest absent, including its surviving sessions.
    #[arg(long)]
    pub retired_identity: Option<String>,
    /// Optional migration source for checksum/missing-source reconciliation.
    #[arg(long)]
    pub source: Option<PathBuf>,
    #[arg(long, default_value = "_sqlx_migrations")]
    pub table: String,
    #[cfg(feature = "kafka-admin")]
    /// Independent Kafka credentials; requires explicit topic, checkpoint and candidates.
    #[arg(long, requires_all = ["topic", "checkpoint", "offsets"])]
    pub connection_file: Option<PathBuf>,
    #[cfg(feature = "kafka-admin")]
    #[arg(long, requires = "connection_file")]
    pub topic: Option<String>,
    #[cfg(feature = "kafka-admin")]
    #[arg(long, requires = "connection_file")]
    pub checkpoint: Option<PathBuf>,
    #[cfg(feature = "kafka-admin")]
    #[arg(long = "offset", requires = "connection_file", value_parser = kafka::parse_offset)]
    pub offsets: Vec<kafka::Candidate>,
    /// Network-observation deadline; local input validation runs first.
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
    pub timeout: u64,
}
impl From<&Options> for sqlx_doris_ops::maintenance::diagnose::Options {
    fn from(value: &Options) -> Self {
        let options = Self::new(
            value.receipt.clone(),
            value.retired_identity.clone(),
            value.source.clone(),
            value.table.clone(),
            value.timeout,
        );
        #[cfg(feature = "kafka-admin")]
        let options = options.with_kafka(
            value.connection_file.clone(),
            value.topic.clone(),
            value.checkpoint.clone(),
            value.offsets.clone(),
        );
        options
    }
}
