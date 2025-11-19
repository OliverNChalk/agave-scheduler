use std::ptr::NonNull;

use agave_feature_set::FeatureSet;
use agave_scheduler_bindings::worker_message_types::{CheckResponse, ExecutionResponse};
use agave_scheduler_bindings::{
    MAX_TRANSACTIONS_PER_MESSAGE, ProgressMessage, SharableTransactionRegion, TpuToPackMessage,
    worker_message_types,
};
use agave_scheduling_utils::handshake::client::{ClientSession, ClientWorkerSession};
use agave_scheduling_utils::pubkeys_ptr::PubkeysPtr;
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use rts_alloc::Allocator;
use slotmap::SlotMap;
use solana_fee::FeeFeatures;

use crate::{Bridge, RuntimeState, TransactionId, TxDecision, Worker, WorkerResponse};

pub struct SchedulerBindings {
    allocator: Allocator,
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    progress_tracker: shaq::Consumer<ProgressMessage>,
    workers: Vec<SchedulerWorker>,

    progress: ProgressMessage,
    runtime: RuntimeState,
    transactions: SlotMap<TransactionId, ()>,
    worker_response: Option<WorkerResponsePointers>,
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
            transactions: SlotMap::default(),
            worker_response: None,
        }
    }
}

impl SchedulerBindings {
    // TODO: Duplicated from scheduling_utils::transaction_ptr.
    const TX_CORE_SIZE: usize = std::mem::size_of::<SharableTransactionRegion>();
    const TX_TOTAL_SIZE: usize = Self::TX_CORE_SIZE + std::mem::size_of::<TransactionId>();
    #[allow(dead_code, reason = "Invariant assertion")]
    const TX_BATCH_SIZE_ASSERT: () =
        assert!(Self::TX_TOTAL_SIZE * MAX_TRANSACTIONS_PER_MESSAGE < 4096);
    const TX_BATCH_META_OFFSET: usize = Self::TX_CORE_SIZE * MAX_TRANSACTIONS_PER_MESSAGE;
}

impl Bridge for SchedulerBindings {
    type Worker = SchedulerWorker;

    fn progress(&self) -> &ProgressMessage {
        &self.progress
    }

    fn worker(&mut self, id: usize) -> &mut Self::Worker {
        &mut self.workers[id]
    }

    fn drain_progress(&mut self) {
        self.progress_tracker.sync();
        while let Some(msg) = self.progress_tracker.try_read() {
            self.progress = *msg;
        }
        self.progress_tracker.finalize();
    }

    fn drain_tpu(
        &mut self,
        mut cb: impl FnMut((TransactionId, &TransactionPtr)) -> TxDecision,
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
            let id = self.transactions.insert(());

            // Remove & free the TX if the scheduler doesn't want it.
            if cb((id, &tx)) == TxDecision::Drop {
                self.transactions.remove(id).unwrap();
                // SAFETY:
                // - We own `tx` exclusively.
                unsafe { tx.free(&self.allocator) };
            }
        }

        self.tpu_to_pack.finalize();
    }

    fn pop_worker(
        &mut self,
        worker: usize,
        mut cb: impl FnMut((TransactionId, &TransactionPtr, crate::WorkerResponse)) -> TxDecision,
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
                assert_eq!(rep.batch.num_transactions, rep.responses.num_transaction_responses);
                let transactions = self
                    .allocator
                    .ptr_from_offset(rep.batch.transactions_offset)
                    .cast::<SharableTransactionRegion>();
                // SAFETY:
                // - We ensured that this batch was originally allocated to support M.
                let metas = unsafe { transactions.byte_add(Self::TX_BATCH_META_OFFSET).cast() };

                let responses = match rep.responses.tag {
                    worker_message_types::EXECUTION_RESPONSE => WorkerResponseBatch::Execution(
                        self.allocator
                            .ptr_from_offset(rep.responses.transaction_responses_offset)
                            .cast(),
                    ),
                    worker_message_types::CHECK_RESPONSE => WorkerResponseBatch::Check(
                        self.allocator
                            .ptr_from_offset(rep.responses.transaction_responses_offset)
                            .cast(),
                    ),
                    _ => panic!(),
                };

                self.worker_response.insert(WorkerResponsePointers {
                    index: 0,
                    transactions,
                    metas,
                    responses,
                })
            }
        };

        match ptrs.responses {
            WorkerResponseBatch::Execution(rep) => {
                // SAFETY
                // - For tx & meta, we took care to allocate these correctly originally.
                // - For responses we trust Agave to have correctly allocated the responses.
                let (tx, meta, rep) = unsafe {
                    let region = ptrs.transactions.add(ptrs.index).read();
                    let tx =
                        TransactionPtr::from_sharable_transaction_region(&region, &self.allocator);
                    let meta = ptrs.metas.add(ptrs.index).read();
                    let rep = rep.add(ptrs.index).read();

                    (tx, meta, rep)
                };

                if cb((meta, &tx, WorkerResponse::Execute(rep))) == TxDecision::Drop {
                    self.transactions.remove(meta).unwrap();
                    // SAFETY
                    // - We own `tx` exclusively.
                    unsafe {
                        tx.free(&self.allocator);
                    };
                }

                true
            }
            WorkerResponseBatch::Check(rep) => {
                // SAFETY
                // - For tx & meta, we took care to allocate these correctly originally.
                // - For responses we trust Agave to have correctly allocated the responses.
                let (tx, meta, rep) = unsafe {
                    let region = ptrs.transactions.add(ptrs.index).read();
                    let tx =
                        TransactionPtr::from_sharable_transaction_region(&region, &self.allocator);
                    let meta = ptrs.metas.add(ptrs.index).read();
                    let rep = rep.add(ptrs.index).read();

                    (tx, meta, rep)
                };

                // Load shared pubkeys if there are any.
                let keys = (rep.resolved_pubkeys.num_pubkeys > 0).then(|| unsafe {
                    // SAFETY
                    // - Region exists as `num_pubkeys > 0`.
                    // - Trust Agave to have allocated this region correctly.
                    PubkeysPtr::from_sharable_pubkeys(&rep.resolved_pubkeys, &self.allocator)
                });

                if cb((meta, &tx, WorkerResponse::Check(rep, keys.as_ref()))) == TxDecision::Drop {
                    // SAFETY
                    // - We own these pointers/allocations exclusively.
                    unsafe {
                        tx.free(&self.allocator);
                        if let Some(keys) = keys {
                            keys.free(&self.allocator);
                        }
                    }
                }

                true
            }
        }
    }

    fn schedule_check(
        &mut self,
        worker: usize,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    ) {
        todo!()
    }

    fn schedule_execute(
        &mut self,
        worker: usize,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    ) {
        todo!()
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

struct WorkerResponsePointers {
    index: usize,
    transactions: NonNull<SharableTransactionRegion>,
    metas: NonNull<TransactionId>,
    responses: WorkerResponseBatch,
}

enum WorkerResponseBatch {
    Execution(NonNull<ExecutionResponse>),
    Check(NonNull<CheckResponse>),
}
