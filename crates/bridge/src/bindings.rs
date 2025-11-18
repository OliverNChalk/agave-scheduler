use agave_feature_set::FeatureSet;
use agave_scheduler_bindings::worker_message_types::{CheckResponse, ExecutionResponse};
use agave_scheduler_bindings::{ProgressMessage, TpuToPackMessage};
use agave_scheduling_utils::handshake::client::{ClientSession, ClientWorkerSession};
use agave_scheduling_utils::handshake::server::AgaveWorkerSession;
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use rts_alloc::Allocator;
use solana_fee::FeeFeatures;

use crate::{Bridge, RuntimeState, TpuDecision, TransactionId, Worker, WorkerId};

pub struct SchedulerBindings {
    allocator: Allocator,
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    progress_tracker: shaq::Consumer<ProgressMessage>,
    workers: Vec<ClientWorkerSession>,

    progress: ProgressMessage,
    runtime: RuntimeState,
}

impl SchedulerBindings {
    #[must_use]
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
            // TODO: Load this properly.
            runtime: RuntimeState {
                feature_set: FeatureSet::all_enabled(),
                fee_features: FeeFeatures { enable_secp256r1_precompile: true },
                lamports_per_signature: 5000,
                burn_percent: 50,
            },
        }
    }
}

impl Bridge for SchedulerBindings {
    type Worker = WorkerSession;

    fn progress(&self) -> &ProgressMessage {
        &self.progress
    }

    fn worker(&mut self, id: WorkerId) -> &mut Self::Worker {
        todo!()
    }

    fn drain_progress(&mut self) {
        todo!()
    }

    fn drain_tpu(
        &mut self,
        cb: impl FnMut((TransactionId, TransactionPtr)) -> TpuDecision,
        max_count: usize,
    ) {
        todo!()
    }

    fn pop_check(
        &mut self,
        cb: impl FnMut(TransactionId, &TransactionPtr, &CheckResponse) -> TpuDecision,
    ) -> bool {
        todo!()
    }

    fn pop_execute(
        &mut self,
        cb: impl FnMut(TransactionId, &TransactionPtr, &ExecutionResponse) -> TpuDecision,
    ) -> bool {
        todo!()
    }

    fn schedule_check(
        &mut self,
        worker: WorkerId,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    ) {
        todo!()
    }

    fn schedule_execute(
        &mut self,
        worker: WorkerId,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    ) {
        todo!()
    }
}

pub struct WorkerSession(AgaveWorkerSession);

impl Worker for WorkerSession {
    fn len(&mut self) -> usize {
        self.0.pack_to_worker.sync();

        self.0.pack_to_worker.len()
    }

    fn rem(&mut self) -> usize {
        self.0.pack_to_worker.sync();

        self.0.pack_to_worker.capacity() - self.0.pack_to_worker.len()
    }
}
