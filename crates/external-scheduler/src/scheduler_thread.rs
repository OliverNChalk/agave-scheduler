use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use agave_bridge::SchedulerBindings;
use agave_schedulers::batch::BatchScheduler;
use agave_schedulers::fifo::FifoScheduler;
use agave_schedulers::greedy::GreedyScheduler;
use agave_schedulers::shared::PriorityId;
use agave_scheduling_utils::handshake::{ClientLogon, client as handshake_client, logon_flags};
use toolbox::shutdown::Shutdown;

pub(crate) fn spawn<S>(
    shutdown: Shutdown,
    bindings_ipc: PathBuf,
    mut scheduler: S,
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
                    worker_count: 5,
                    // 2GB allocator size.
                    allocator_size: 2 * 1024 * 1024 * 1024,
                    allocator_handles: 1,
                    tpu_to_pack_capacity: 2usize.pow(16),
                    progress_tracker_capacity: 128,
                    pack_to_worker_capacity: 128,
                    worker_to_pack_capacity: 256,
                    flags: logon_flags::REROUTE_VOTES,
                },
                Duration::from_secs(1),
            )
            .unwrap();
            let mut bridge = SchedulerBindings::new(session);

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

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>);
}

impl Scheduler for BatchScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for FifoScheduler {
    type Meta = ();

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for GreedyScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}
