use agave_feature_set::FeatureSet;
use agave_scheduler_bindings::ProgressMessage;
use agave_scheduler_bindings::worker_message_types::{CheckResponse, ExecutionResponse};
use agave_scheduling_utils::pubkeys_ptr::PubkeysPtr;
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use solana_fee::FeeFeatures;
use solana_pubkey::Pubkey;

pub trait Bridge {
    type Worker: Worker;
    type Meta;

    fn runtime(&self) -> &RuntimeState;

    fn progress(&self) -> &ProgressMessage;

    fn worker_len(&self) -> usize;

    fn worker(&mut self, id: usize) -> &mut Self::Worker;

    fn tx(&self, key: TransactionId) -> &TransactionState;

    fn tx_drop(&mut self, key: TransactionId);

    fn drain_progress(&mut self);

    fn tpu_len(&mut self) -> usize;

    fn tpu_drain(
        &mut self,
        cb: impl FnMut(&mut Self, TransactionId) -> TxDecision,
        max_count: usize,
    );

    fn pop_worker(
        &mut self,
        worker: usize,
        cb: impl FnMut(&mut Self, WorkerResponse<'_, Self::Meta>) -> TxDecision,
    ) -> bool;

    fn schedule(
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

pub struct WorkerResponse<'a, M> {
    pub key: TransactionId,
    pub data: &'a TransactionPtr,
    pub meta: M,
    pub response: WorkerAction<'a>,
}

pub enum WorkerAction<'a> {
    Unprocessed,
    Check(CheckResponse, Option<&'a PubkeysPtr>),
    Execute(ExecutionResponse),
}

slotmap::new_key_type! {
    pub struct TransactionId;
}

pub struct TransactionState {
    pub data: SanitizedTransactionView<TransactionPtr>,
    pub keys: Option<PubkeysPtr>,
}

impl TransactionState {
    pub fn write_locks(&self) -> impl Iterator<Item = &Pubkey> {
        self.data
            .static_account_keys()
            .iter()
            .chain(self.keys.iter().flat_map(|keys| keys.as_slice().iter()))
            .enumerate()
            .filter(|(i, _)| self.is_writable(*i as u8))
            .map(|(_, key)| key)
    }

    pub fn read_locks(&self) -> impl Iterator<Item = &Pubkey> {
        self.data
            .static_account_keys()
            .iter()
            .chain(self.keys.iter().flat_map(|keys| keys.as_slice().iter()))
            .enumerate()
            .filter(|(i, _)| !self.is_writable(*i as u8))
            .map(|(_, key)| key)
    }

    fn is_writable(&self, index: u8) -> bool {
        if index >= self.data.num_static_account_keys() {
            let loaded_address_index = index.wrapping_sub(self.data.num_static_account_keys());
            loaded_address_index < self.data.total_writable_lookup_accounts() as u8
        } else {
            index
                < self
                    .data
                    .num_signatures()
                    .wrapping_sub(self.data.num_readonly_signed_static_accounts())
                || (index >= self.data.num_signatures()
                    && index
                        < (self.data.static_account_keys().len() as u8)
                            .wrapping_sub(self.data.num_readonly_unsigned_static_accounts()))
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum TxDecision {
    Keep,
    Drop,
}
