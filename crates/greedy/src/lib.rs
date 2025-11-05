use agave_scheduler_bindings::{ProgressMessage, TpuToPackMessage};
use agave_scheduling_utils::handshake::client::{ClientSession, ClientWorkerSession};
use rts_alloc::Allocator;

pub struct GreedyScheduler {
    allocator: Allocator,
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    progress_tracker: shaq::Consumer<ProgressMessage>,
    workers: Vec<ClientWorkerSession>,
    progress: ProgressMessage,
}

impl GreedyScheduler {
    pub fn new(
        ClientSession { mut allocators, tpu_to_pack, progress_tracker, workers }: ClientSession,
    ) -> Self {
        assert_eq!(allocators.len(), 1, "invalid number of allocators");

        Self {
            allocator: allocators.remove(0),
            tpu_to_pack,
            progress_tracker,
            workers,
            progress: ProgressMessage {
                leader_state: 0,
                current_slot: 0,
                next_leader_slot: u64::MAX,
                leader_range_end: u64::MAX,
                remaining_cost_units: 0,
                current_slot_progress: 0,
            },
        }
    }

    pub fn poll(&mut self) {
        // Drain the progress tracker so we know which slot we're on.
        self.drain_progress();

        // Drain responses from workers.
        self.drain_worker_responses();
    }

    fn drain_progress(&mut self) {
        self.progress_tracker.sync();
        while let Some(msg) = self.progress_tracker.try_read() {
            self.progress = *msg;
        }
        self.progress_tracker.finalize();
    }

    fn drain_worker_responses(&mut self) {
        for worker in &mut self.workers {
            worker.worker_to_pack.sync();
            while let Some(msg) = worker.worker_to_pack.try_read() {
                todo!()
            }
        }
    }
}
