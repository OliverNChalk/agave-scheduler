use std::collections::HashSet;

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

    // Load config (or use default).
    let config = args.config.as_ref().map_or_else(
        || Config {
            host_name: "dev".to_string(),
            nats_servers: vec![],
            filter_keys: HashSet::new(),
            logs: None,
            scheduler: SchedulerConfig::GreedyThroughput,
        },
        |path| serde_yaml::from_slice(&toolbox::fs::must_read(path)).unwrap(),
    );

    // Setup tracing.
    let _log_guard =
        toolbox::tracing::setup_tracing("agave-external-scheduler", config.logs.as_deref());

    // Log build information (as soon as possible).
    toolbox::log_build_info!();

    // Setup standard panic handling.
    let default_panic = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        error!(?panic_info, "Application panic");

        default_panic(panic_info);
    }));

    // Receive our shared memory configuration.
    //
    // SAFETY: FD 3 is assumed to have been setup by the orchestrator.
    let session = unsafe { agave_orchestrator::scheduler::recv_client_session() };

    // Start server.
    ControlThread::run_in_place(config, session)
}
