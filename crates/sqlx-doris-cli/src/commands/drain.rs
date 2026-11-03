use clap::Subcommand;
#[derive(Subcommand)]
pub(crate) enum Action {
    /// Preview/cancel one BROKER LOAD through its exact registered transaction.
    /// Never sends label-based CANCEL; unregistered LOAD and other job types fail closed.
    CancelLoad {
        /// Retired managed identity; independently verify its attribution to this job.
        #[arg(long)]
        name: String,
        /// Exact LOAD JobId from drain status or a previous preview.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..=i64::MAX as u64))]
        job_id: u64,
        /// Exact TransactionId shown by preview; required when executing.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..=i64::MAX as u64))]
        transaction_id: Option<u64>,
        /// FE HTTP(S) origin of this SQL cluster. Redirects must identify a registered BE.
        #[arg(long, env = "DORIS_HTTP_URL", hide_env_values = true)]
        http_url: Option<String>,
        /// Confirm exclusive maintenance, job attribution and stable trusted
        /// cluster/endpoints. Never rebind/restore/reuse metadata during this operation.
        #[arg(long)]
        maintenance: bool,
        /// Submit one exact transaction abort after rechecking. Never automatically retry.
        #[arg(long, requires_all = ["maintenance", "transaction_id", "http_url"])]
        force: bool,
    },
    /// Read one snapshot. Exit nonzero if blocked; never changes locks or jobs.
    Status {
        #[arg(long)]
        name: String,
    },
    /// Wait for terminal work and inactive automatic producers in the documented scope.
    /// Stop old actors/admission and reconcile work outside the documented scope.
    Wait {
        #[arg(long)]
        name: String,
        /// Overall deadline, including metadata reads (seconds).
        #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..))]
        timeout: u64,
        /// Attest exclusive maintenance, retired-name provenance, stopped old
        /// actors/admission and reconciliation of other async job classes.
        #[arg(long, required = true)]
        maintenance: bool,
    },
    /// Preview cancellation of one exact ROLLUP JobId. COLUMN ignores JobId in
    /// Doris 4.1.5 and cannot be safely cancelled by this CLI.
    CancelRollup {
        #[arg(long)]
        name: String,
        #[arg(long)]
        table: String,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..=i64::MAX as u64))]
        job_id: u64,
        /// Confirm exclusive maintenance and attribution of this exact job.
        #[arg(long)]
        maintenance: bool,
        /// Send one CANCEL; never replay it after error or cancellation.
        #[arg(long, requires = "maintenance")]
        force: bool,
    },
    /// Preview/cancel one MV task by exact parent JobId and TaskId. Does not
    /// disable automatic refresh or prove that the whole database is drained.
    CancelMvTask {
        #[arg(long)]
        name: String,
        #[arg(long)]
        view: String,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..=i64::MAX as u64))]
        job_id: u64,
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..=i64::MAX as u64))]
        task_id: u64,
        /// Confirm exclusive maintenance, exact task attribution, no reuse of
        /// server-generated job names, and stopped refresh/DDL/admission actors.
        #[arg(long)]
        maintenance: bool,
        #[arg(long, requires = "maintenance")]
        force: bool,
    },
}
impl From<&Action> for sqlx_doris_ops::maintenance::drain::Action {
    fn from(value: &Action) -> Self {
        use Action as Input;
        match value {
            Input::CancelLoad {
                name,
                job_id,
                transaction_id,
                http_url,
                maintenance,
                force,
            } => sqlx_doris_ops::maintenance::drain::Action::CancelLoad {
                name: name.clone(),
                job_id: *job_id,
                transaction_id: *transaction_id,
                http_url: http_url.clone(),
                maintenance: *maintenance,
                force: *force,
            },
            Input::Status { name } => {
                sqlx_doris_ops::maintenance::drain::Action::Status { name: name.clone() }
            }
            Input::Wait {
                name,
                timeout,
                maintenance,
            } => sqlx_doris_ops::maintenance::drain::Action::Wait {
                name: name.clone(),
                timeout: *timeout,
                maintenance: *maintenance,
            },
            Input::CancelRollup {
                name,
                table,
                job_id,
                maintenance,
                force,
            } => sqlx_doris_ops::maintenance::drain::Action::CancelRollup {
                name: name.clone(),
                table: table.clone(),
                job_id: *job_id,
                maintenance: *maintenance,
                force: *force,
            },
            Input::CancelMvTask {
                name,
                view,
                job_id,
                task_id,
                maintenance,
                force,
            } => sqlx_doris_ops::maintenance::drain::Action::CancelMvTask {
                name: name.clone(),
                view: view.clone(),
                job_id: *job_id,
                task_id: *task_id,
                maintenance: *maintenance,
                force: *force,
            },
        }
    }
}
