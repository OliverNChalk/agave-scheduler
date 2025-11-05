use std::path::PathBuf;

use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::runtime::Runtime;
use tokio::signal::unix::SignalKind;
use toolbox::shutdown::Shutdown;
use toolbox::tokio::NamedTask;
use tracing::{error, info};

use crate::scheduler_thread::SchedulerThread;

pub(crate) struct ControlThread {
    shutdown: Shutdown,
    threads: FuturesUnordered<NamedTask<std::thread::Result<()>>>,
}

impl ControlThread {
    pub(crate) fn run_in_place(bindings_ipc: PathBuf) -> std::thread::Result<()> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = ControlThread::setup(&runtime, bindings_ipc);

        runtime.block_on(server.run())
    }

    fn setup(runtime: &Runtime, bindings_ipc: PathBuf) -> Self {
        let shutdown = Shutdown::new();

        // Setup app threads.
        let threads = vec![SchedulerThread::spawn(shutdown.clone(), bindings_ipc)];

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
