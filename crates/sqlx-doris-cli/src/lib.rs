//! Command-line interface for the public sqlx-doris package.
#![forbid(unsafe_code)]
use args::{Action, Args, DatabaseAction, MigrationAction};
#[cfg(feature = "admin")]
use args::{AdminAction, AdminMigrationAction, LockAction};
use clap::Parser;
mod args;
#[cfg(feature = "admin")]
mod lock;
mod migrate;
mod prepare;
use sqlx_core::migrate::MigrateDatabase;
use sqlx_doris_core::Doris;

#[cfg(feature = "kafka-admin")]
use commands::kafka;
#[cfg(feature = "admin")]
use commands::{drain, identity, routine};
use sqlx_doris_ops::{self as ops, failure::Result, observation::Observation};
#[cfg(all(test, feature = "admin"))]
mod command_tests;
#[cfg(feature = "admin")]
mod commands;
mod output;
mod render;
#[cfg(feature = "admin")]
mod render_diagnose;
#[cfg(feature = "kafka-admin")]
mod render_kafka;

struct Context {
    ops: ops::context::Context,
    json: bool,
}
impl std::ops::Deref for Context {
    type Target = ops::context::Context;
    fn deref(&self) -> &Self::Target {
        &self.ops
    }
}
impl Context {
    fn line(&self, arguments: std::fmt::Arguments<'_>) {
        if !self.json {
            println!("{arguments}");
        }
    }
}

#[tokio::main]
/// Execute one command from this process's arguments.
pub async fn main() {
    let mut input = std::env::args_os().collect::<Vec<_>>();
    if input.get(1).is_some_and(|value| value == "sqlx-doris") {
        input.remove(1);
    }
    let args = match Args::try_parse_from(&input) {
        Ok(args) => args,
        Err(error) => {
            if input.iter().any(|value| value == "--json") && error.exit_code() != 0 {
                output::finish(&Observation::default(), true, "arguments", Some(2));
                std::process::exit(2);
            }
            error.exit();
        }
    };
    let observation = if args.json {
        Observation::default()
    } else {
        Observation::default().with_listener(render::progress)
    };
    let context = Context {
        json: args.json,
        ops: ops::context::Context {
            connection: ops::context::ConnectionSettings {
                database_url: args.database_url.clone(),
                fallback_url: std::env::var("DORIS_TEST_URL").ok(),
                identity_file: {
                    #[cfg(feature = "admin")]
                    {
                        args.identity_file.clone()
                    }
                    #[cfg(not(feature = "admin"))]
                    {
                        None
                    }
                },
                replication_num: args.replication_num,
                lock_timeout: args.lock_timeout,
                ddl_timeout: args.ddl_timeout,
            },
            observation,
        },
    };
    let operation = operation(&args.command);
    let result = run(&context, &args.command).await;
    if let Err(error) = &result
        && let Some(failure) = error.downcast_ref::<output::Failure>()
    {
        context
            .observation
            .data("message", serde_json::json!(failure.message));
        if let Some(reason) = failure.reason {
            context.observation.failure_reason(reason, &failure.message);
        }
    }
    let requested = match &result {
        Ok(()) => Some(0),
        Err(error) => error
            .downcast_ref::<output::Failure>()
            .map(|failure| failure.code()),
    };
    if !args.json {
        render::command(&operation, &context.observation.snapshot().details);
    }
    let code = output::finish(&context.observation, args.json, &operation, requested);
    if !args.json
        && let Err(error) = result
    {
        eprintln!("{error}");
    }
    std::process::exit(i32::from(code));
}
fn operation(action: &Action) -> String {
    // Clap's canonical subcommand names are stable independently of Debug formatting.
    let (group, leaf) = match action {
        Action::Prepare { .. } => ("prepare", "metadata"),
        Action::Database { command } => (
            "database",
            match command {
                DatabaseAction::Create => "create",
                DatabaseAction::Exists => "exists",
                _ => "drop",
            },
        ),
        #[cfg(feature = "admin")]
        Action::Admin { command } => return admin_operation(command),
        Action::Migrate { command, .. } => (
            "migrate",
            match command {
                MigrationAction::Add { .. } => "add",
                MigrationAction::Run { .. } => "run",
                MigrationAction::Info => "info",
                MigrationAction::Revert { .. } => "revert",
            },
        ),
    };
    format!("{group}.{leaf}")
}
async fn run(args: &Context, action: &Action) -> Result<()> {
    match action {
        Action::Prepare {
            manifest_path,
            output,
            check,
            cargo_offline,
            cargo_args,
        } => prepare::run(
            args,
            manifest_path,
            output.as_deref(),
            *check,
            *cargo_offline,
            cargo_args,
        ),
        Action::Database { command } => {
            let url = args.url()?;
            match command {
                DatabaseAction::Create => {
                    args.observation
                        .phase(output::Phase::Submitted, "database_create");
                    Doris::create_database(&url).await?;
                    args.observation
                        .phase(output::Phase::Verified, "database_created");
                }
                DatabaseAction::Exists => {
                    let exists = Doris::database_exists(&url).await?;
                    args.observation.data("exists", serde_json::json!(exists));
                    args.line(format_args!("{exists}"));
                }
                DatabaseAction::Drop { .. } => {
                    args.observation
                        .phase(output::Phase::Submitted, "database_drop");
                    Doris::drop_database(&url).await?;
                    args.observation
                        .phase(output::Phase::Verified, "database_dropped");
                }
            }
            Ok(())
        }
        #[cfg(feature = "admin")]
        Action::Admin { command } => run_admin(args, command).await,
        Action::Migrate {
            source,
            table,
            no_lock,
            command,
        } => migrate::run(args, source, table, *no_lock, command).await,
    }
}

#[cfg(feature = "admin")]
fn admin_operation(action: &AdminAction) -> String {
    let (group, leaf) = match action {
        AdminAction::Diagnose { .. } => ("diagnose", "recovery"),
        #[cfg(feature = "kafka-admin")]
        AdminAction::Kafka { command } => (
            "kafka",
            match command {
                kafka::Action::Snapshot { .. } => "snapshot",
                _ => "preflight",
            },
        ),
        AdminAction::Routine { command } => (
            "routine",
            match command {
                routine::Action::Create { .. } => "create",
                routine::Action::Status { .. } => "status",
                _ => "stop",
            },
        ),
        AdminAction::Drain { command } => (
            "drain",
            match command {
                drain::Action::Status { .. } => "status",
                drain::Action::Wait { .. } => "wait",
                drain::Action::CancelLoad { .. } => "cancel-load",
                drain::Action::CancelRollup { .. } => "cancel-rollup",
                _ => "cancel-mv-task",
            },
        ),
        AdminAction::Identity { command } => (
            "identity",
            match command {
                identity::Action::Create { .. } => "create",
                identity::Action::Status { .. } => "status",
                _ => "retire",
            },
        ),
        AdminAction::Lock { command } => (
            "lock",
            match command {
                LockAction::Status => "status",
                LockAction::Recover { .. } => "recover",
                _ => "recover-transition",
            },
        ),
        AdminAction::Migrate { command, .. } => (
            "migrate",
            match command {
                AdminMigrationAction::Repair { .. } => "repair",
            },
        ),
    };
    format!("admin.{group}.{leaf}")
}

#[cfg(feature = "admin")]
async fn run_admin(args: &Context, action: &AdminAction) -> Result<()> {
    match action {
        AdminAction::Diagnose { options } => {
            ops::maintenance::diagnose::run(&args.ops, &options.into()).await
        }
        #[cfg(feature = "kafka-admin")]
        AdminAction::Kafka { command } => ops::kafka::run(&args.ops, &command.into()).await,
        AdminAction::Routine { command } => {
            ops::maintenance::drain::routine::run(&args.ops, &command.into()).await
        }
        AdminAction::Drain { command } => {
            ops::maintenance::drain::run(&args.ops, &command.into()).await
        }
        AdminAction::Identity { command } => {
            ops::maintenance::identity::run(&args.ops, &command.into()).await
        }
        AdminAction::Lock { command } => lock::run(args, command).await,
        AdminAction::Migrate {
            source,
            table,
            command,
        } => match command {
            AdminMigrationAction::Repair {
                version, action, ..
            } => migrate::repair(args, source, table, *version, action).await,
        },
    }
}
