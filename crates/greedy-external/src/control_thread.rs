use std::path::PathBuf;
use std::time::Duration;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::runtime::Runtime;
use tokio::signal::unix::SignalKind;
use toolbox::shutdown::Shutdown;
use toolbox::tokio::NamedTask;
use tracing::{error, info};

use crate::config::Config;
use crate::scheduler_thread::SchedulerThread;

pub(crate) struct ControlThread {
    shutdown: Shutdown,
    threads: FuturesUnordered<NamedTask<std::thread::Result<()>>>,
}

impl ControlThread {
    pub(crate) fn run_in_place(config: Config, bindings_ipc: PathBuf) -> std::thread::Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = rt.block_on(ControlThread::setup(&rt, config, bindings_ipc));

        rt.block_on(server.run())
    }

    async fn setup(runtime: &Runtime, config: Config, bindings_ipc: PathBuf) -> Self {
        let shutdown = Shutdown::new();

        // Spawn metrics publisher.
        let mut threads = Vec::default();
        let nats_client = Box::leak(Box::new(
            metrics_nats_exporter::async_nats::connect(config.nats_servers)
                .await
                .expect("NATS Client Connect"),
        ));
        threads.push(
            metrics_nats_exporter::install(
                shutdown.token.clone(),
                metrics_nats_exporter::Config {
                    interval_min: Duration::from_millis(50),
                    interval_max: Duration::from_millis(1000),
                    metric_prefix: Some(format!("metric.greedy-external.{}", config.host_name)),
                },
                nats_client,
            )
            .unwrap(),
        );

        // Spawn scheduler.
        threads.push(SchedulerThread::spawn(shutdown.clone(), bindings_ipc));

        // Use tokio to listen on all thread exits concurrently.
        let threads = threads
            .into_iter()
            .map(|thread| {
                let name = thread.thread().name().unwrap().to_string();
                info!(name, "Thread spawned");

                NamedTask::new(runtime.spawn_blocking(move || thread.join()), name)
            })
            .collect();

        ControlThread { shutdown, threads }
    }

    async fn run(mut self) -> std::thread::Result<()> {
        let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate()).unwrap();
        let mut sigint = tokio::signal::unix::signal(SignalKind::interrupt()).unwrap();

        let mut exit = tokio::select! {
            () = self.shutdown.cancelled() => Ok(()),

            _ = sigterm.recv() => {
                info!("SIGTERM caught, stopping server");

                Ok(())
            },
            _ = sigint.recv() => {
                info!("SIGINT caught, stopping server");

                Ok(())
            },
            opt = self.threads.next() => {
                let (name, res) = opt.unwrap();
                error!(%name, ?res, "Thread exited unexpectedly");

                res.unwrap().and_then(|()| Err(Box::new("Thread exited unexpectedly")))
            }
        };

        // Trigger shutdown.
        self.shutdown.shutdown();

        // Wait for all threads to exit, reporting the first error as the ultimate
        // error.
        while let Some((name, res)) = self.threads.next().await {
            info!(%name, ?res, "Thread exited");
            exit = exit.and(res.unwrap());
        }

        exit
    }
}
