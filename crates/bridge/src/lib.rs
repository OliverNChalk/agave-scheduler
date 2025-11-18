use std::collections::VecDeque;

use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{
    CheckResponse, ExecutionResponse, parsing_and_sanitization_flags, status_check_flags,
};
use agave_scheduler_bindings::{
    IS_LEADER, MAX_TRANSACTIONS_PER_MESSAGE, ProgressMessage, pack_message_flags,
};
use agave_scheduling_utils::transaction_ptr::TransactionPtr;

pub trait Bridge {
    fn progress(&self) -> &ProgressMessage;
    fn worker(&self, id: WorkerId) -> &Worker;
    fn drain_progress(&mut self);
    fn drain_tpu(
        &mut self,
        cb: impl FnMut((TransactionId, TransactionPtr)) -> TpuDecision,
        max_count: usize,
    );
    fn pop_check(
        &mut self,
        cb: impl FnMut(TransactionId, &TransactionPtr, &CheckResponse) -> TpuDecision,
    ) -> bool;
    fn pop_execute(
        &mut self,
        cb: impl FnMut(TransactionId, &TransactionPtr, &ExecutionResponse) -> TpuDecision,
    ) -> bool;
    fn schedule_check(
        &mut self,
        worker: WorkerId,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    );
    fn schedule_execute(
        &mut self,
        worker: WorkerId,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    );
}

#[derive(Debug, Clone, Copy)]
pub struct WorkerId;

pub struct Worker;

impl Worker {
    pub fn len(&self) -> usize {
        todo!()
    }

    pub fn rem(&self) -> usize {
        todo!()
    }
}

pub struct TransactionId;

pub enum TpuDecision {
    Keep,
    Drop,
}
