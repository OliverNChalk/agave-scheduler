use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use agave_scheduler_batch::BatchScheduler;
use agave_scheduler_fifo::FifoScheduler;
use agave_scheduler_greedy_revenue::GreedyRevenueScheduler;
use agave_scheduler_greedy_throughput::GreedyThroughputScheduler;
use agave_schedulers::shared::PriorityId;
use agave_scheduling_utils::bridge::SchedulerBindingsBridge;
use agave_scheduling_utils::handshake::{ClientLogon, client as handshake_client};
use toolbox::shutdown::Shutdown;

pub(crate) fn spawn<S>(
    shutdown: Shutdown,
    bindings_ipc: PathBuf,
    mut scheduler: S,
    worker_count: usize,
) -> JoinHandle<()>
where
    S: Scheduler + Send,
{
    std::thread::Builder::new()
        .name("Scheduler".to_string())
        .spawn(move || {
            let session = handshake_client::connect(
                &bindings_ipc,
                ClientLogon {
                    worker_count,
                    // 2GB allocator size.
                    allocator_size: 2 * 1024 * 1024 * 1024,
                    allocator_handles: 1,
                    tpu_to_pack_capacity: 2usize.pow(16),
                    progress_tracker_capacity: 128,
                    pack_to_worker_capacity: 128,
                    worker_to_pack_capacity: 256,
                    flags: 0,
                },
                Duration::from_secs(1),
            )
            .unwrap();
            let mut bridge = SchedulerBindingsBridge::new(session);

            while !shutdown.is_shutdown() {
                scheduler.poll(&mut bridge);
            }
        })
        .unwrap()
}

pub(crate) trait Scheduler
where
    Self: Sized + 'static,
{
    type Meta: Copy;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>);
}

impl Scheduler for BatchScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for FifoScheduler {
    type Meta = ();

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for GreedyRevenueScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for GreedyThroughputScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}
