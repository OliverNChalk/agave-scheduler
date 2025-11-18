use agave_feature_set::FeatureSet;
use agave_scheduler_bindings::ProgressMessage;
use agave_scheduler_bindings::worker_message_types::{CheckResponse, ExecutionResponse};
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use solana_fee::FeeFeatures;

pub trait Bridge {
    type Worker: Worker;

    fn progress(&self) -> &ProgressMessage;

    fn worker(&mut self, id: usize) -> &mut Self::Worker;

    fn drain_progress(&mut self);

    fn drain_tpu(
        &mut self,
        cb: impl FnMut((TransactionId, &TransactionPtr)) -> TxDecision,
        max_count: usize,
    );

    fn pop_worker(
        &mut self,
        worker: usize,
        cb: impl FnMut((TransactionId, &TransactionPtr, WorkerResponse)) -> TxDecision,
    ) -> bool;

    fn schedule_check(
        &mut self,
        worker: usize,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    );

    fn schedule_execute(
        &mut self,
        worker: usize,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    );
}

pub struct RuntimeState {
    pub feature_set: FeatureSet,
    pub fee_features: FeeFeatures,
    pub lamports_per_signature: u64,
    pub burn_percent: u64,
}

pub trait Worker {
    fn is_empty(&mut self) -> bool {
        self.len() == 0
    }

    fn len(&mut self) -> usize;
    fn rem(&mut self) -> usize;
}

pub enum WorkerResponse<'a> {
    Check(&'a CheckResponse),
    Execute(&'a ExecutionResponse),
}

slotmap::new_key_type! {
    pub struct TransactionId;
}

#[derive(Debug, PartialEq, Eq)]
pub enum TxDecision {
    Keep,
    Drop,
}
