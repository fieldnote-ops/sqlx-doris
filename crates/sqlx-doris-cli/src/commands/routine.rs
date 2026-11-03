use clap::Subcommand;
use std::path::PathBuf;
#[derive(clap::Args)]
#[group(id = "routine_locator", required = true, multiple = false)]
pub(crate) struct Locator {
    /// Generated UUID name in the database selected by the administrator URL.
    #[arg(long)]
    pub(crate) job_name: Option<String>,
    /// Private external receipt; selects its database and verifies cluster/database IDs.
    #[arg(long)]
    pub(crate) receipt: Option<PathBuf>,
}
impl From<&Locator> for sqlx_doris_ops::maintenance::drain::routine::Locator {
    fn from(value: &Locator) -> Self {
        Self {
            job_name: value.job_name.clone(),
            receipt: value.receipt.clone(),
        }
    }
}
#[derive(Subcommand)]
pub(crate) enum Action {
    /// Create one job with a fresh UUID and permanent server reservation view.
    /// Never accepts a caller-chosen name or replays an uncertain CREATE.
    Create {
        /// Private credentials from identity create; the administrator URL stays separate.
        #[arg(long)]
        runner_identity_file: PathBuf,
        /// Current-database single target table (ASCII identifier).
        #[arg(long)]
        table: String,
        /// Native SQL suffix after ON TABLE, including FROM KAFKA. No semicolons/NUL.
        #[arg(long)]
        source: PathBuf,
        /// New private external receipt, persisted before any server mutation.
        #[arg(long)]
        output: PathBuf,
        /// Reserve sqlx_rl_ jobs and _sqlx_rl_ views exclusively for this protocol.
        /// All creators must use this CLI; preserve reservations/names forever.
        #[arg(long, required = true)]
        managed_namespace: bool,
    },
    /// Inspect the permanent reservation and exact retained job history. No mutation.
    Status {
        #[command(flatten)]
        locator: Locator,
    },
    /// Preview/stop a managed UUID job after its creator identity is retired.
    /// Safety requires the no-reuse protocol; Doris still resolves STOP by name.
    Stop {
        #[command(flatten)]
        locator: Locator,
        /// Exact Id from routine status; checked again before submitting STOP.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..=i64::MAX as u64))]
        job_id: Option<u64>,
        /// Stop old actors/admission, serialize administration and keep the cluster
        /// and metadata stable. No restoration, namespace bypass or name reuse.
        #[arg(long)]
        maintenance: bool,
        /// Confirm every creator uses this protocol and all reservations stay intact.
        #[arg(long)]
        managed_namespace: bool,
        /// Submit one STOP. Never retry after timeout, disconnect or cancellation.
        #[arg(long, requires_all = ["maintenance", "managed_namespace", "job_id"])]
        force: bool,
    },
}
impl From<&Action> for sqlx_doris_ops::maintenance::drain::routine::Action {
    fn from(value: &Action) -> Self {
        use Action as Input;
        match value {
            Input::Create {
                runner_identity_file,
                table,
                source,
                output,
                managed_namespace,
            } => sqlx_doris_ops::maintenance::drain::routine::Action::Create {
                runner_identity_file: runner_identity_file.clone(),
                table: table.clone(),
                source: source.clone(),
                output: output.clone(),
                managed_namespace: *managed_namespace,
            },
            Input::Status { locator } => {
                sqlx_doris_ops::maintenance::drain::routine::Action::Status {
                    locator: locator.into(),
                }
            }
            Input::Stop {
                locator,
                job_id,
                maintenance,
                managed_namespace,
                force,
            } => sqlx_doris_ops::maintenance::drain::routine::Action::Stop {
                locator: locator.into(),
                job_id: *job_id,
                maintenance: *maintenance,
                managed_namespace: *managed_namespace,
                force: *force,
            },
        }
    }
}
