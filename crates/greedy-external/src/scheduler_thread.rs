use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use agave_scheduler_bindings::ProgressMessage;
use agave_scheduling_utils::handshake::client::ClientSession;
use agave_scheduling_utils::handshake::{ClientLogon, client as handshake_client};
use toolbox::shutdown::Shutdown;

pub(crate) struct SchedulerThread {
    shutdown: Shutdown,
    session: ClientSession,
    progress: ProgressMessage,
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
                        worker_to_pack_capacity: 128,
                        flags: 0,
                    },
                    Duration::from_secs(1),
                )
                .unwrap();

                SchedulerThread {
                    shutdown,
                    session,
                    progress: ProgressMessage {
                        leader_state: 0,
                        current_slot: 0,
                        next_leader_slot: u64::MAX,
                        leader_range_end: u64::MAX,
                        remaining_cost_units: 0,
                        current_slot_progress: 0,
                    },
                }
                .run();
            })
            .unwrap()
    }

    fn run(mut self) {
        while !self.shutdown.is_shutdown() {
            // Drain the progress tracker so we know which slot we're on.
            self.drain_progress();
        }
    }

    fn drain_progress(&mut self) {
        self.session.progress_tracker.sync();
        while let Some(msg) = self.session.progress_tracker.try_read() {
            self.progress = *msg;
        }
    }
}
