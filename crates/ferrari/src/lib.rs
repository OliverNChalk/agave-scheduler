use agave_scheduling_utils::responses_region::{CheckResponsesPtr, ExecutionResponsesPtr};
use agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch};

pub struct FerrariScheduler {
    core: SchedulerCore,
}

impl FerrariScheduler {
    #[must_use]
    pub fn new(core: SchedulerCore) -> Self {
        Self { core }
    }

    pub fn poll(&mut self) {
        todo!()
    }
}

struct SchedulerCore;

impl SchedulerCore {
    fn drain_progress(&mut self) {
        todo!()
    }

    fn drain_tpu(&mut self, on_tpu: impl FnMut(TransactionPtr) -> TpuDecision, max_count: usize) {
        todo!()
    }

    fn pop_check(&mut self) -> Option<(TransactionPtrBatch, CheckResponsesPtr)> {
        todo!()
    }

    fn pop_execute(&mut self) -> Option<(TransactionPtrBatch, ExecutionResponsesPtr)> {
        todo!()
    }

    fn schedule_batch(
        &mut self,
        worker: usize,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    ) {
        todo!()
    }
}

struct TransactionId;

enum TpuDecision {
    Store,
    Drop,
}
