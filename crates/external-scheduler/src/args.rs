use std::path::PathBuf;

use clap::{Parser, ValueEnum, ValueHint};

#[derive(Debug, Parser)]
#[command(version = toolbox::version!(), long_version = toolbox::long_version!())]
pub(crate) struct Args {
    /// Scheduler variant to use.
    #[clap(long, value_hint = ValueHint::Other)]
    pub(crate) scheduler: SchedulerVariant,
    /// Path to scheduler config.
    #[clap(long, value_hint = ValueHint::FilePath)]
    pub(crate) config: PathBuf,
    /// Path to scheduler bindings ipc server.
    #[clap(long, value_hint = ValueHint::FilePath)]
    pub(crate) bindings_ipc: PathBuf,
    /// If provided, will write hourly log files to this directory.
    #[arg(long, value_hint = ValueHint::DirPath)]
    pub(crate) logs: Option<PathBuf>,
    /// Emit metrics via NATS.
    #[arg(long)]
    pub(crate) metrics: bool,
}

#[derive(Debug, Clone, ValueEnum)]
pub(crate) enum SchedulerVariant {
    Fifo,
    Greedy,
}
