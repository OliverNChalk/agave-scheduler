use agave_scheduler_bindings::ProgressMessage;
use agave_scheduler_bindings::worker_message_types::{CheckResponse, ExecutionResponse};
use agave_scheduling_utils::transaction_ptr::TransactionPtr;

use crate::{Bridge, TpuDecision, TransactionId, Worker, WorkerId};

pub struct SchedulerBindings;

impl Bridge for SchedulerBindings {
    fn progress(&self) -> &ProgressMessage {
        todo!()
    }

    fn worker(&self, id: WorkerId) -> &Worker {
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
