use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use agave_scheduling_utils::handshake::{ClientLogon, client as handshake_client};
use bridge::SchedulerBindings;
use greedy::GreedyScheduler;
use toolbox::shutdown::Shutdown;

pub(crate) struct SchedulerThread {
    shutdown: Shutdown,
    scheduler: GreedyScheduler,
    bridge: SchedulerBindings,
}

impl SchedulerThread {
    pub(crate) fn spawn(shutdown: Shutdown, bindings_ipc: PathBuf) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("Scheduler".to_string())
            .spawn(move || {
                let session = handshake_client::connect(
                    &bindings_ipc,
                    ClientLogon {
                        worker_count: 4,
                        allocator_size: 256 * 1024 * 1024,
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
                let bridge = SchedulerBindings::new(session);
                let scheduler = GreedyScheduler::new();

                SchedulerThread { shutdown, scheduler, bridge }.run();
            })
            .unwrap()
    }

    fn run(mut self) {
        while !self.shutdown.is_shutdown() {
            self.scheduler.poll(&mut self.bridge);
        }
    }
}
