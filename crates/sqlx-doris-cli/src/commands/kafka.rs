use Candidate as Offset;
use clap::Subcommand;
pub(crate) use sqlx_doris_ops::kafka::Candidate;
use std::path::PathBuf;
#[derive(Subcommand)]
pub(crate) enum Action {
    /// Save a new private historical baseline. Never overwrites or certifies earlier history.
    Snapshot {
        #[arg(long)]
        connection_file: PathBuf,
        #[arg(long)]
        topic: String,
        #[arg(long)]
        output: PathBuf,
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout: u64,
    },
    /// Check explicitly supplied next offsets. Never derives, transfers, resets or commits offsets.
    Preflight {
        #[arg(long)]
        connection_file: PathBuf,
        #[arg(long)]
        topic: String,
        /// Earlier trusted snapshot; omitting it always leaves history inconclusive.
        #[arg(long)]
        checkpoint: Option<PathBuf>,
        #[arg(long = "offset", required = true, value_parser = sqlx_doris_ops::kafka::parse_offset)]
        offsets: Vec<Offset>,
        #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u64).range(1..=3600))]
        timeout: u64,
    },
}
impl From<&Action> for sqlx_doris_ops::kafka::Action {
    fn from(value: &Action) -> Self {
        use Action as Input;
        match value {
            Input::Snapshot {
                connection_file,
                topic,
                output,
                timeout,
            } => sqlx_doris_ops::kafka::Action::Snapshot {
                connection_file: connection_file.clone(),
                topic: topic.clone(),
                output: output.clone(),
                timeout: *timeout,
            },
            Input::Preflight {
                connection_file,
                topic,
                checkpoint,
                offsets,
                timeout,
            } => sqlx_doris_ops::kafka::Action::Preflight {
                connection_file: connection_file.clone(),
                topic: topic.clone(),
                checkpoint: checkpoint.clone(),
                offsets: offsets.clone(),
                timeout: *timeout,
            },
        }
    }
}
