use clap::Subcommand;
use std::path::PathBuf;
#[derive(Subcommand)]
pub(crate) enum Action {
    /// Create a fresh, dedicated identity and a private external credential file.
    Create {
        #[arg(long)]
        output: PathBuf,
    },
    /// Inspect one managed identity, without changing privileges or locks.
    Status {
        #[arg(long)]
        name: String,
    },
    /// Preview removal of one managed identity. Does not recover any lock.
    Retire {
        #[arg(long)]
        name: String,
        /// Confirm this identity belongs exclusively to the old runner, no
        /// administrator can recreate/regrant it, and this is the sole maintainer.
        #[arg(long)]
        maintenance: bool,
        /// DROP the identity after validation; without this flag, preview only.
        #[arg(long, requires = "maintenance")]
        force: bool,
    },
}
impl From<&Action> for sqlx_doris_ops::maintenance::identity::Action {
    fn from(value: &Action) -> Self {
        use Action as Input;
        match value {
            Input::Create { output } => sqlx_doris_ops::maintenance::identity::Action::Create {
                output: output.clone(),
            },
            Input::Status { name } => {
                sqlx_doris_ops::maintenance::identity::Action::Status { name: name.clone() }
            }
            Input::Retire {
                name,
                maintenance,
                force,
            } => sqlx_doris_ops::maintenance::identity::Action::Retire {
                name: name.clone(),
                maintenance: *maintenance,
                force: *force,
            },
        }
    }
}
