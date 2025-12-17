use std::time::Duration;

use agave_schedulers::batch;
use agave_schedulers::events::{EventContext, EventEmitter};
use agave_schedulers::fifo::FifoScheduler;
use agave_schedulers::greedy::GreedyScheduler;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use solana_keypair::{EncodableKey, Keypair};
use tokio::runtime::Runtime;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;
use toolbox::shutdown::Shutdown;
use toolbox::tokio::NamedTask;
use tracing::{error, info};

use crate::args::Args;
use crate::config::{Config, SchedulerConfig};
use crate::events_thread::EventsThread;

pub(crate) struct ControlThread {
    shutdown: Shutdown,
    threads: FuturesUnordered<NamedTask<std::thread::Result<()>>>,
}

impl ControlThread {
    pub(crate) fn run_in_place(args: Args, config: Config) -> std::thread::Result<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = rt.block_on(ControlThread::setup(&rt, args, config));

        rt.block_on(server.run())
    }

    async fn setup(runtime: &Runtime, args: Args, config: Config) -> Self {
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
                    metric_prefix: Some(format!("metric.scheduler.{}", config.host_name)),
                },
                nats_client,
            )
            .unwrap(),
        );

        // Spawn events publisher.
        let event_ctx = EventContext::leak();
        let (event_tx, event_rx) = mpsc::channel(1024);
        let events = EventEmitter::new(event_ctx, event_tx);
        threads.push(EventsThread::spawn(event_rx, nats_client, &config.host_name));

        // Setup scheduler.
        match config.scheduler {
            SchedulerConfig::Batch(batch) => {
                let keypair =
                    Box::leak(Box::new(Keypair::read_from_file(batch.keypair_path).unwrap()));
                let (scheduler, workers) = batch::BatchScheduler::new(
                    Some(events),
                    batch::BatchSchedulerArgs {
                        tip: batch::TipDistributionArgs {
                            vote_account: batch.tip.vote_account,
                            merkle_authority: batch.tip.merkle_authority,
                            commission_bps: batch.tip.commission_bps,
                        },
                        jito: batch::JitoArgs {
                            http_rpc: batch.jito.http_rpc,
                            ws_rpc: batch.jito.ws_rpc,
                            block_engine: batch.jito.block_engine,
                        },
                        keypair,
                    },
                );

                threads.push(crate::scheduler_thread::spawn(
                    shutdown.clone(),
                    args.bindings_ipc,
                    scheduler,
                ));
                threads.extend(workers);
            }
            SchedulerConfig::Fifo => threads.push(crate::scheduler_thread::spawn::<FifoScheduler>(
                shutdown.clone(),
                args.bindings_ipc,
                FifoScheduler::new(),
            )),
            SchedulerConfig::Greedy => {
                threads.push(crate::scheduler_thread::spawn::<GreedyScheduler>(
                    shutdown.clone(),
                    args.bindings_ipc,
                    GreedyScheduler::new(Some(events)),
                ));
            }
        }

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
