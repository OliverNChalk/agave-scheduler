use agave_scheduler_bindings::{IS_LEADER, ProgressMessage};
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
        // Drain the progress tracker so we know which slot we're on.
        self.core.drain_progress();
        let is_leader = self.core.progress().leader_state == IS_LEADER;

        // Drain check responses.
        while let Some((batch, rep)) = self.core.pop_check() {
            todo!("free stuff");
        }

        // Drain execute responses.
        while let Some((batch, rep)) = self.core.pop_execute() {
            todo!("free stuff");
        }

        // Ingest a bounded amount of new transactions.
        match is_leader {
            true => self.core.drain_tpu(|tx| todo!(), 128),
            false => self.core.drain_tpu(|tx| todo!(), 1024),
        }

        todo!("Do our thing and schedule");
    }
}

struct SchedulerCore;

impl SchedulerCore {
    fn progress(&self) -> &ProgressMessage {
        todo!()
    }

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
