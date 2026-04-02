use std::path::PathBuf;

use clap::{Parser, ValueHint};

#[derive(Debug, Parser)]
#[command(version = toolbox::version!(), long_version = toolbox::long_version!())]
pub(crate) struct Args {
    /// Path to scheduler config.
    #[clap(long, value_hint = ValueHint::FilePath)]
    pub(crate) config: Option<PathBuf>,
    /// Emit metrics via NATS.
    #[arg(long)]
    pub(crate) metrics: bool,
}
