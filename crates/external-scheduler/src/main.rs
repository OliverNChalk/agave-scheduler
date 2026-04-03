mod args;
mod config;
mod control_thread;
mod events_thread;
mod scheduler_thread;

fn main() -> std::thread::Result<()> {
    use clap::Parser;
    use control_thread::ControlThread;
    use tracing::error;

    use crate::config::{Config, SchedulerConfig};

    // Parse command-line arguments.
    let args = crate::args::Args::parse();

    // Setup tracing.
    let _log_guard = toolbox::tracing::setup_tracing("rust-template", args.logs.as_deref());

    // Log build information (as soon as possible).
    toolbox::log_build_info!();

    // Setup standard panic handling.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        error!(?panic_info, "Application panic");

        default_panic(panic_info);
    }));

    // Load config (or use default).
    let config = args.config.as_ref().map_or_else(
        || Config {
            host_name: "dev".to_string(),
            nats_servers: vec![],
            scheduler: SchedulerConfig::GreedyThroughput,
        },
        |path| toml::from_str(&String::from_utf8(toolbox::fs::must_read(path)).unwrap()).unwrap(),
    );

    // Start server.
    ControlThread::run_in_place(args, config)
}
