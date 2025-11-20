use std::collections::VecDeque;
use std::marker::PhantomData;
use std::ptr::NonNull;

use agave_feature_set::FeatureSet;
use agave_scheduler_bindings::ProgressMessage;
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use agave_transaction_view::transaction_data::TransactionData;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use slotmap::SlotMap;
use solana_fee::FeeFeatures;
use solana_transaction::versioned::VersionedTransaction;

use crate::{
    Bridge, KeyedTransactionMeta, RuntimeState, ScheduleBatch, TransactionId, TransactionState,
    TxDecision, Worker, WorkerResponse,
};

pub struct TestBridge<M> {
    progress_queue: VecDeque<ProgressMessage>,
    tpu_queue: VecDeque<TransactionId>,
    worker_queues: Vec<VecDeque<WorkerResponse<'static, M>>>,
    workers: Vec<TestWorker>,
    scheduled: VecDeque<ScheduleBatch<Vec<KeyedTransactionMeta<M>>>>,

    progress: ProgressMessage,
    runtime: RuntimeState,
    state: SlotMap<TransactionId, TransactionState>,

    _meta: PhantomData<M>,
}

impl<M> TestBridge<M>
where
    M: Copy,
{
    #[must_use]
    pub fn new(worker_count: usize, worker_req_cap: usize) -> Self {
        Self {
            progress_queue: VecDeque::default(),
            tpu_queue: VecDeque::default(),
            worker_queues: vec![VecDeque::default(); worker_count],
            workers: vec![TestWorker { len: 0, cap: worker_req_cap }; worker_count],
            scheduled: VecDeque::default(),

            progress: ProgressMessage {
                leader_state: 0,
                current_slot: 0,
                next_leader_slot: u64::MAX,
                leader_range_end: u64::MAX,
                remaining_cost_units: 0,
                current_slot_progress: 0,
            },
            runtime: RuntimeState {
                feature_set: FeatureSet::all_enabled(),
                fee_features: FeeFeatures { enable_secp256r1_precompile: true },
                lamports_per_signature: 5000,
                burn_percent: 50,
            },
            state: SlotMap::default(),

            _meta: PhantomData,
        }
    }

    pub fn queue_progress(&mut self, progress: ProgressMessage) {
        self.progress_queue.push_back(progress);
    }

    pub fn queue_tpu(&mut self, tx: &VersionedTransaction) {
        // Serialize the transaction & get a raw pointer.
        let mut serialized = bincode::serialize(tx).unwrap();
        let len = serialized.len();
        let data = NonNull::new(serialized.as_mut_ptr()).unwrap();
        core::mem::forget(serialized);

        // Construct our TransactionPtr & sanitized view.
        //
        // SAFETY
        // - We own this allocation exclusively & len is accurate.
        let data = unsafe { TransactionPtr::from_raw_parts(data, len) };
        let data = SanitizedTransactionView::try_new_sanitized(data, true).unwrap();

        // Insert into state & store the key in the tpu queue.
        let key = self.state.insert(TransactionState { data, keys: None });
        self.tpu_queue.push_back(key);
    }

    pub fn queue_response(
        &mut self,
        batch: &ScheduleBatch<Vec<KeyedTransactionMeta<M>>>,
        response: WorkerResponse<'static, M>,
    ) {
        self.worker_queues[batch.worker].push_back(response);
    }

    pub fn pop_schedule(&mut self) -> Option<ScheduleBatch<Vec<KeyedTransactionMeta<M>>>> {
        self.scheduled.pop_front()
    }
}

impl<M> Bridge for TestBridge<M>
where
    M: Copy,
{
    type Worker = TestWorker;
    type Meta = M;

    fn runtime(&self) -> &RuntimeState {
        &self.runtime
    }

    fn progress(&self) -> &ProgressMessage {
        &self.progress
    }

    fn worker_count(&self) -> usize {
        self.workers.len()
    }

    fn worker(&mut self, id: usize) -> &mut Self::Worker {
        &mut self.workers[id]
    }

    fn tx(&self, key: TransactionId) -> &TransactionState {
        &self.state[key]
    }

    fn tx_drop(&mut self, key: TransactionId) {
        self.state.remove(key).unwrap();
    }

    fn drain_progress(&mut self) {
        if let Some(progress) = self.progress_queue.back() {
            self.progress = *progress;
        }

        self.progress_queue.clear();
    }

    fn tpu_len(&mut self) -> usize {
        self.tpu_queue.len()
    }

    fn tpu_drain(
        &mut self,
        mut cb: impl FnMut(&mut Self, TransactionId) -> TxDecision,
        max_count: usize,
    ) {
        for _ in 0..max_count {
            let Some(tx) = self.tpu_queue.pop_front() else {
                return;
            };

            cb(self, tx);
        }
    }

    fn pop_worker(
        &mut self,
        worker: usize,
        mut cb: impl FnMut(&mut Self, WorkerResponse<'_, Self::Meta>) -> TxDecision,
    ) -> bool {
        let Some(WorkerResponse { key, meta, response }) = self.worker_queues[worker].pop_front()
        else {
            return false;
        };

        if cb(self, WorkerResponse { key, meta, response }) == TxDecision::Drop {
            let state = self.state.remove(key).unwrap();

            // Extract the raw parts of the allocation.
            let data = state.data.inner_data().data();
            let len = data.len();
            let ptr = data.as_ptr();
            drop(state);

            // Recover the underlying allocation so we can drop it.
            //
            // SAFETY
            // - We original allocated this and exclusively own it, so it's safe for us to
            //   deallocate.
            unsafe {
                let allocation = core::slice::from_raw_parts_mut(ptr.cast_mut(), len);
                core::ptr::drop_in_place(allocation);
            }
        }

        true
    }

    fn schedule(
        &mut self,
        ScheduleBatch { worker, transactions, max_working_slot, flags }: ScheduleBatch<
            &[KeyedTransactionMeta<M>],
        >,
    ) {
        self.scheduled.push_back(ScheduleBatch {
            worker,
            transactions: transactions.to_vec(),
            max_working_slot,
            flags,
        });
    }
}

#[derive(Debug, Clone)]
pub struct TestWorker {
    len: usize,
    cap: usize,
}

impl Worker for TestWorker {
    fn len(&mut self) -> usize {
        self.len
    }

    fn rem(&mut self) -> usize {
        self.cap - self.len
    }
}
