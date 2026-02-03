use std::cmp::Ordering;
use std::ptr::NonNull;

use agave_feature_set::FeatureSet;
use agave_scheduler_bindings::worker_message_types::{CheckResponse, ExecutionResponse};
use agave_scheduler_bindings::{
    MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, ProgressMessage,
    SharableTransactionBatchRegion, SharableTransactionRegion, TpuToPackMessage, processed_codes,
    worker_message_types,
};
use agave_scheduling_utils::handshake::client::{ClientSession, ClientWorkerSession};
use agave_scheduling_utils::pubkeys_ptr::PubkeysPtr;
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use agave_transaction_view::result::TransactionViewError;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use metrics::{Gauge, gauge};
use rts_alloc::Allocator;
use slotmap::SlotMap;
use solana_fee::FeeFeatures;
use solana_packet::PACKET_DATA_SIZE;

use crate::{
    Bridge, KeyedTransactionMeta, RuntimeState, ScheduleBatch, TransactionKey, TransactionState,
    TxDecision, Worker, WorkerAction, WorkerResponse,
};

pub struct SchedulerBindings<M> {
    allocator: Allocator,
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    progress_tracker: shaq::Consumer<ProgressMessage>,
    workers: Vec<SchedulerWorker>,

    progress: ProgressMessage,
    runtime: RuntimeState,
    state: SlotMap<TransactionKey, TransactionState>,
    worker_response: Option<WorkerResponsePointers<M>>,

    metrics: SchedulerBindingsMetrics,
}

impl<M> SchedulerBindings<M>
where
    M: Copy,
{
    // TODO: Duplicated from scheduling_utils::transaction_ptr.
    const TX_CORE_SIZE: usize = std::mem::size_of::<SharableTransactionRegion>();
    const TX_TOTAL_SIZE: usize =
        Self::TX_CORE_SIZE + std::mem::size_of::<KeyedTransactionMeta<M>>();
    const TX_BATCH_META_OFFSET: usize = Self::TX_CORE_SIZE * MAX_TRANSACTIONS_PER_MESSAGE;
    const TX_BATCH_SIZE: usize = Self::TX_TOTAL_SIZE * MAX_TRANSACTIONS_PER_MESSAGE;
    const _TX_BATCH_SIZE_ASSERT: () = assert!(Self::TX_BATCH_SIZE < 4096);

    #[must_use]
    pub fn new(
        ClientSession { mut allocators, tpu_to_pack, progress_tracker, workers }: ClientSession,
    ) -> Self {
        assert_eq!(allocators.len(), 1, "invalid number of allocators");

        Self {
            allocator: allocators.remove(0),
            tpu_to_pack,
            progress_tracker,
            workers: workers.into_iter().map(SchedulerWorker).collect(),

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
            state: SlotMap::default(),
            worker_response: None,

            metrics: SchedulerBindingsMetrics::new(),
        }
    }

    fn collect_batch(
        allocator: &Allocator,
        state: &mut SlotMap<TransactionKey, TransactionState>,
        batch: &[KeyedTransactionMeta<M>],
    ) -> SharableTransactionBatchRegion {
        assert!(batch.len() <= MAX_TRANSACTIONS_PER_MESSAGE);

        // Allocate a batch that can hold all our transaction pointers.
        let transactions = allocator.allocate(Self::TX_BATCH_SIZE as u32).unwrap();
        let transactions_offset = unsafe { allocator.offset(transactions) };

        // Get our two pointers to the TX region & meta region.
        let tx_ptr = unsafe {
            allocator
                .ptr_from_offset(transactions_offset)
                .cast::<SharableTransactionRegion>()
        };
        // SAFETY
        // - Pointer is guaranteed to not overrun the allocation as we just created it
        //   with a sufficient size.
        let meta_ptr = unsafe {
            allocator
                .ptr_from_offset(transactions_offset)
                .byte_add(Self::TX_BATCH_META_OFFSET)
                .cast::<KeyedTransactionMeta<M>>()
        };

        // Fill in the batch with transaction pointers.
        for (i, meta) in batch.iter().copied().enumerate() {
            let tx = &mut state[meta.key];
            assert!(!tx.dead);

            // We are sending a copy to Agave, we track this as a new borrow.
            tx.borrows += 1;

            // SAFETY
            // - We have allocated the transaction batch to support at least
            //   `MAX_TRANSACTIONS_PER_MESSAGE`, we terminate the loop before we overrun the
            //   region.
            unsafe {
                tx_ptr.add(i).write(
                    tx.data
                        .inner_data()
                        .to_sharable_transaction_region(allocator),
                );
                meta_ptr.add(i).write(meta);
            };
        }

        SharableTransactionBatchRegion {
            num_transactions: batch.len().try_into().unwrap(),
            transactions_offset,
        }
    }
}

impl<M> Bridge for SchedulerBindings<M>
where
    M: Copy,
{
    type Worker = SchedulerWorker;
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

    fn tx(&self, key: TransactionKey) -> &crate::TransactionState {
        &self.state[key]
    }

    fn tx_insert(&mut self, tx: &[u8]) -> Result<TransactionKey, TransactionViewError> {
        assert!(tx.len() <= PACKET_DATA_SIZE);

        let ptr = self
            .allocator
            .allocate(tx.len().try_into().unwrap())
            .unwrap();
        // SAFETY:
        // - We own this pointer exclusively.
        // - The allocated region is at least `tx.len()` bytes.
        unsafe {
            std::ptr::copy_nonoverlapping(tx.as_ptr(), ptr.as_ptr(), tx.len());
        }
        // SAFETY:
        // - We own this pointer and the size is correct.
        let tx = unsafe { TransactionPtr::from_raw_parts(ptr, tx.len()) };

        // Sanitize the transaction, drop it immediately if it fails sanitization.
        //
        // TODO: Don't hardcode true, true.
        match SanitizedTransactionView::try_new_sanitized(tx, true, true) {
            Ok(tx) => {
                let key = self.state.insert(TransactionState {
                    dead: false,
                    borrows: 0,
                    flags: 0,
                    data: tx,
                    keys: None,
                });
                self.metrics.state_len.set(self.state.len() as f64);

                Ok(key)
            }
            Err(err) => {
                // SAFETY:
                // - We own `tx` exclusively.
                // - The previous `TransactionPtr` has been dropped by `try_new_sanitized`.
                unsafe {
                    self.allocator.free(ptr);
                }

                Err(err)
            }
        }
    }

    fn tx_drop(&mut self, key: TransactionKey) {
        // If we have requests that have borrowed this shared transaction region, then
        // we can't immediately clean up and must instead flag it as dead.
        match self.state[key].borrows {
            0 => {
                let state = self.state.remove(key).unwrap();
                self.metrics.state_len.set(self.state.len() as f64);

                if let Some(keys) = state.keys {
                    // SAFETY
                    // - We own these pointers/allocations exclusively.
                    unsafe {
                        keys.free(&self.allocator);
                    }
                }

                // SAFETY
                // - We own the allocation exclusively.
                unsafe {
                    state.data.into_inner_data().free(&self.allocator);
                }
            }
            _ => self.state[key].dead = true,
        }
    }

    fn drain_progress(&mut self) -> Option<ProgressMessage> {
        self.progress_tracker.sync();

        let mut received = false;
        while let Some(msg) = self.progress_tracker.try_read() {
            self.progress = *msg;
            received = true;
        }
        self.progress_tracker.finalize();

        received.then_some(self.progress)
    }

    fn tpu_len(&mut self) -> usize {
        self.tpu_to_pack.sync();

        self.tpu_to_pack.len()
    }

    fn tpu_drain(
        &mut self,
        mut cb: impl FnMut(&mut Self, TransactionKey) -> TxDecision,
        max_count: usize,
    ) {
        self.tpu_to_pack.sync();

        let additional = std::cmp::min(self.tpu_to_pack.len(), max_count);
        for _ in 0..additional {
            let msg = self.tpu_to_pack.try_read().unwrap();

            // SAFETY:
            // - Trust Agave to have properly transferred ownership to use & not to
            //   free/access this.
            // - We are only creating a single exclusive pointer.
            let tx = unsafe {
                TransactionPtr::from_sharable_transaction_region(&msg.transaction, &self.allocator)
            };

            // Sanitize the transaction, drop it immediately if it fails sanitization.
            //
            // TODO: Don't hardcode true, true,
            let Ok(tx) = SanitizedTransactionView::try_new_sanitized(tx, true, true) else {
                // SAFETY:
                // - We own `tx` exclusively.
                // - The previous `TransactionPtr` has been dropped by `try_new_sanitized`.
                unsafe {
                    self.allocator.free_offset(msg.transaction.offset);
                }

                // TODO: Metrics.

                continue;
            };

            // Get the ID so the caller can store it for later use.
            let key = self.state.insert(TransactionState {
                dead: false,
                borrows: 0,
                flags: msg.flags,
                data: tx,
                keys: None,
            });
            self.metrics.state_len.set(self.state.len() as f64);

            // Remove & free the TX if the scheduler doesn't want it.
            if cb(self, key) == TxDecision::Drop {
                let state = self.state.remove(key).unwrap();
                assert!(state.keys.is_none());
                assert_eq!(state.borrows, 0);
                self.metrics.state_len.set(self.state.len() as f64);

                // SAFETY:
                // - We own `tx` exclusively.
                unsafe { state.data.into_inner_data().free(&self.allocator) };
            }
        }

        self.tpu_to_pack.finalize();
    }

    fn pop_worker(
        &mut self,
        worker: usize,
        mut cb: impl FnMut(&mut Self, WorkerResponse<'_, Self::Meta>) -> TxDecision,
    ) -> bool {
        let ptrs = match &mut self.worker_response {
            Some(in_progress) => in_progress,
            None => {
                self.workers[worker].0.worker_to_pack.sync();
                let Some(rep) = self.workers[worker].0.worker_to_pack.try_read().copied() else {
                    return false;
                };
                self.workers[worker].0.worker_to_pack.finalize();

                // Get transaction & meta pointers.
                let transactions = unsafe {
                    self.allocator
                        .ptr_from_offset(rep.batch.transactions_offset)
                        .cast::<SharableTransactionRegion>()
                };
                // SAFETY:
                // - We ensured that this batch was originally allocated to support M.
                let metas = unsafe { transactions.byte_add(Self::TX_BATCH_META_OFFSET).cast() };

                let responses = match (rep.processed_code, rep.responses.tag) {
                    (processed_codes::PROCESSED, worker_message_types::EXECUTION_RESPONSE) => {
                        assert_eq!(
                            rep.batch.num_transactions,
                            rep.responses.num_transaction_responses
                        );
                        WorkerResponseBatch::Execution(unsafe {
                            self.allocator
                                .ptr_from_offset(rep.responses.transaction_responses_offset)
                                .cast()
                        })
                    }
                    (processed_codes::PROCESSED, worker_message_types::CHECK_RESPONSE) => {
                        assert_eq!(
                            rep.batch.num_transactions,
                            rep.responses.num_transaction_responses
                        );
                        WorkerResponseBatch::Check(unsafe {
                            self.allocator
                                .ptr_from_offset(rep.responses.transaction_responses_offset)
                                .cast()
                        })
                    }
                    (processed_codes::MAX_WORKING_SLOT_EXCEEDED, _) => {
                        WorkerResponseBatch::Unprocessed
                    }
                    _ => panic!("Unexpected response; rep={rep:?}"),
                };

                self.worker_response.insert(WorkerResponsePointers {
                    index: 0,
                    len: usize::from(rep.batch.num_transactions),
                    batch_offset: rep.batch.transactions_offset,
                    metas,
                    responses,
                })
            }
        };

        // SAFETY
        // - We took care to allocate these correctly originally.
        let KeyedTransactionMeta { key, meta } = unsafe { ptrs.metas.add(ptrs.index).read() };

        // Decrease the borrow counter as Agave has returned ownership to us.
        let state = &mut self.state[key];
        state.borrows -= 1;

        // Only callback if this state is not already dead (scheduler requested drop).
        let decision = match (state.dead, &ptrs.responses) {
            (true, _) => TxDecision::Drop,
            (false, WorkerResponseBatch::Unprocessed) => {
                let rep = WorkerResponse { key, meta, response: WorkerAction::Unprocessed };

                cb(self, rep)
            }
            (false, WorkerResponseBatch::Execution(rep)) => {
                // SAFETY
                // - We trust Agave to have correctly allocated the responses.
                let rep = unsafe { rep.add(ptrs.index).read() };
                let rep = WorkerResponse { key, meta, response: WorkerAction::Execute(rep) };

                cb(self, rep)
            }
            (false, WorkerResponseBatch::Check(rep)) => {
                // SAFETY
                // - We trust Agave to have correctly allocated the responses.
                let rep = unsafe { rep.add(ptrs.index).read() };

                // Load shared pubkeys if there are any.
                let keys = (rep.resolved_pubkeys.num_pubkeys > 0).then(|| unsafe {
                    // SAFETY
                    // - Region exists as `num_pubkeys > 0`.
                    // - Trust Agave to have allocated this region correctly.
                    PubkeysPtr::from_sharable_pubkeys(&rep.resolved_pubkeys, &self.allocator)
                });

                // Callback holding keys ref, defer storing keys on state.
                let decision = cb(
                    self,
                    WorkerResponse { key, meta, response: WorkerAction::Check(rep, keys.as_ref()) },
                );

                // Free old keys if present before storing new keys.
                if let Some(old_keys) = self.state[key].keys.take() {
                    // SAFETY
                    // - We own this allocation exclusively.
                    unsafe { old_keys.free(&self.allocator) }
                }

                // Store the keys on state.
                self.state[key].keys = keys;

                decision
            }
        };

        // Remove the tx from state & drop the allocation if requested.
        if decision == TxDecision::Drop {
            self.tx_drop(key);
        }

        // Increment the index & clear if we have exhausted all responses.
        let ptrs = self.worker_response.as_mut().unwrap();
        ptrs.index += 1;
        match ptrs.index.cmp(&ptrs.len) {
            Ordering::Greater => unreachable!(),
            Ordering::Equal => {
                let ptrs = self.worker_response.take().unwrap();

                // SAFETY:
                // - It is our responsibility to free the response pointers. The transaction
                //   lifetimes we are already managing separately via Keep/Drop.
                unsafe {
                    self.allocator.free_offset(ptrs.batch_offset);
                    match ptrs.responses {
                        WorkerResponseBatch::Unprocessed => {}
                        WorkerResponseBatch::Check(ptr) => self.allocator.free(ptr.cast()),
                        WorkerResponseBatch::Execution(ptr) => self.allocator.free(ptr.cast()),
                    }
                }
            }
            Ordering::Less => {}
        }

        true
    }

    fn schedule(
        &mut self,
        ScheduleBatch { worker, transactions: batch, max_working_slot, flags }: ScheduleBatch<
            &[KeyedTransactionMeta<M>],
        >,
    ) {
        let queue = &mut self.workers[worker].0.pack_to_worker;

        queue.sync();
        queue
            .try_write(PackToWorkerMessage {
                flags,
                max_working_slot,
                batch: Self::collect_batch(&self.allocator, &mut self.state, batch),
            })
            .unwrap();
        queue.commit();
    }
}

struct SchedulerBindingsMetrics {
    state_len: Gauge,
}

impl SchedulerBindingsMetrics {
    fn new() -> Self {
        Self { state_len: gauge!("container_len", "label" => "state") }
    }
}

pub struct SchedulerWorker(ClientWorkerSession);

impl Worker for SchedulerWorker {
    fn len(&mut self) -> usize {
        self.0.pack_to_worker.sync();

        self.0.pack_to_worker.len()
    }

    fn rem(&mut self) -> usize {
        self.0.pack_to_worker.sync();

        self.0.pack_to_worker.capacity() - self.0.pack_to_worker.len()
    }
}

struct WorkerResponsePointers<M> {
    index: usize,
    len: usize,
    batch_offset: usize,
    metas: NonNull<KeyedTransactionMeta<M>>,
    responses: WorkerResponseBatch,
}

enum WorkerResponseBatch {
    Unprocessed,
    Execution(NonNull<ExecutionResponse>),
    Check(NonNull<CheckResponse>),
}
