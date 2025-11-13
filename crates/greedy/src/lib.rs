#[macro_use]
extern crate static_assertions;

use agave_feature_set::FeatureSet;
use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{
    self, fee_payer_balance_flags, parsing_and_sanitization_flags, resolve_flags,
    status_check_flags,
};
use agave_scheduler_bindings::{
    IS_LEADER, MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, ProgressMessage,
    SharableTransactionBatchRegion, SharableTransactionRegion, TpuToPackMessage,
    pack_message_flags, processed_codes,
};
use agave_scheduling_utils::handshake::client::{ClientSession, ClientWorkerSession};
use agave_scheduling_utils::responses_region::CheckResponsesPtr;
use agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch};
use agave_transaction_view::transaction_view::{SanitizedTransactionView, TransactionView};
use min_max_heap::MinMaxHeap;
use rts_alloc::Allocator;
use slotmap::SlotMap;
use solana_compute_budget_instruction::compute_budget_instruction_details;
use solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS_SIMD_0256;
use solana_cost_model::cost_model::CostModel;
use solana_fee::FeeFeatures;
use solana_fee_structure::FeeBudgetLimits;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_svm_transaction::svm_message::SVMStaticMessage;
use solana_transaction::sanitized::MessageHash;

const UNCHECKED_CAPACITY: usize = 64 * 1024;
const CHECKED_CAPACITY: usize = 64 * 1024;
const STATE_CAPACITY: usize = UNCHECKED_CAPACITY + CHECKED_CAPACITY;

const TX_REGION_SIZE: usize = std::mem::size_of::<SharableTransactionRegion>();
const TX_BATCH_PER_MESSAGE: usize = TX_REGION_SIZE + std::mem::size_of::<PriorityId>();
const TX_BATCH_SIZE: usize = TX_BATCH_PER_MESSAGE * MAX_TRANSACTIONS_PER_MESSAGE;
const_assert!(TX_BATCH_SIZE < 4096);
const TX_BATCH_META_OFFSET: usize = TX_REGION_SIZE * MAX_TRANSACTIONS_PER_MESSAGE;

/// How many percentage points before the end should we aim to fill the block.
const BLOCK_FILL_CUTOFF: u8 = 20;

pub struct GreedyScheduler {
    allocator: Allocator,
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    progress_tracker: shaq::Consumer<ProgressMessage>,
    workers: Vec<ClientWorkerSession>,

    progress: ProgressMessage,
    runtime: RuntimeState,
    unchecked: MinMaxHeap<PriorityId>,
    checked: MinMaxHeap<PriorityId>,
    state: SlotMap<TransactionStateKey, SharableTransactionRegion>,
    in_flight_cost: u32,
}

impl GreedyScheduler {
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
            runtime: RuntimeState {
                feature_set: FeatureSet::all_enabled(),
                fee_features: FeeFeatures { enable_secp256r1_precompile: true },
                lamports_per_signature: 5000,
                burn_percent: 50,
            },
            unchecked: MinMaxHeap::with_capacity(UNCHECKED_CAPACITY),
            checked: MinMaxHeap::with_capacity(UNCHECKED_CAPACITY),
            state: SlotMap::with_capacity_and_key(STATE_CAPACITY),
            in_flight_cost: 0,
        }
    }

    pub fn poll(&mut self) {
        // Drain the progress tracker so we know which slot we're on.
        self.drain_progress();
        let is_leader = self.progress.leader_state == IS_LEADER;

        // Drain responses from workers.
        self.drain_worker_responses();

        // Ingest a bounded amount of new transactions.
        match is_leader {
            true => self.drain_tpu(128),
            false => self.drain_tpu(1024),
        }

        // Drain pending checks.
        self.schedule_checks();

        // Schedule if we're currently the leader.
        if is_leader {
            self.schedule_execute();
        }

        // TODO: Think about re-checking all TXs on slot roll (or at least
        // expired TXs). If we do this we should use a dense slotmap to make
        // iteration fast.
    }

    fn drain_progress(&mut self) {
        self.progress_tracker.sync();
        while let Some(msg) = self.progress_tracker.try_read() {
            self.progress = *msg;
        }
        self.progress_tracker.finalize();
    }

    fn drain_worker_responses(&mut self) {
        for worker in &mut self.workers {
            worker.worker_to_pack.sync();
            while let Some(msg) = worker.worker_to_pack.try_read() {
                match msg.processed_code {
                    processed_codes::MAX_WORKING_SLOT_EXCEEDED => {
                        // SAFETY
                        // - Trust Agave to not have modified/freed this pointer.
                        unsafe {
                            self.allocator
                                .free_offset(msg.responses.transaction_responses_offset);
                        }

                        continue;
                    }
                    processed_codes::PROCESSED => {}
                    _ => panic!(),
                }

                match msg.responses.tag {
                    worker_message_types::CHECK_RESPONSE => {
                        // SAFETY:
                        // - Trust Agave to have allocated the batch/responses properly & told us
                        //   the correct size.
                        // - Use the correct wrapper type (check response ptr).
                        // - Don't duplicate wrappers so we cannot double free.
                        let (batch, responses) = unsafe {
                            (
                                TransactionPtrBatch::from_sharable_transaction_batch_region(
                                    &msg.batch,
                                    &self.allocator,
                                ),
                                CheckResponsesPtr::from_transaction_response_region(
                                    &msg.responses,
                                    &self.allocator,
                                ),
                            )
                        };

                        for (_, id) in Self::on_check_response(&self.allocator, &batch, &responses)
                        {
                            // Evict lowest priority if at capacity.
                            if self.checked.len() == CHECKED_CAPACITY {
                                let evicted = self.checked.pop_min().unwrap();
                                let tx = self.state.remove(evicted.key).unwrap();
                                // SAFETY
                                // - We own this transaction and do not duplicate it so it will not
                                //   get double freed.
                                unsafe { self.allocator.free_offset(tx.offset) };
                            }

                            // Insert the new transaction (yes this may be lower priority then what
                            // we just evicted but that's fine).
                            self.checked.push(id);
                        }

                        // Free both containers.
                        batch.free();
                        responses.free();
                    }
                    worker_message_types::EXECUTION_RESPONSE => {
                        // SAFETY:
                        // - Trust Agave to have allocated the batch/responses properly & told us
                        //   the correct size.
                        // - Don't duplicate wrapper so we cannot double free.
                        let batch: TransactionPtrBatch<PriorityId> = unsafe {
                            TransactionPtrBatch::from_sharable_transaction_batch_region(
                                &msg.batch,
                                &self.allocator,
                            )
                        };

                        // Remove in-flight costs and free all transactions.
                        for (tx, id) in batch.iter() {
                            self.in_flight_cost -= id.cost;

                            // SAFETY:
                            // - Trust Agave not to have already freed this transaction as we are
                            //   the owner.
                            unsafe { tx.free(&self.allocator) };
                        }

                        // Free the containers.
                        batch.free();
                        // SAFETY:
                        // - Trust Agave to have allocated these responses properly.
                        unsafe {
                            self.allocator
                                .free_offset(msg.responses.transaction_responses_offset);
                        }
                    }
                    _ => panic!(),
                }
            }
        }
    }

    fn drain_tpu(&mut self, max: usize) {
        self.tpu_to_pack.sync();

        let additional = std::cmp::min(self.tpu_to_pack.len(), max);
        let shortfall = (self.checked.len() + additional).saturating_sub(UNCHECKED_CAPACITY);

        // NB: Technically we are evicting more than we need to because not all of
        // `additional` will parse correctly & thus have a priority.
        for _ in 0..shortfall {
            let id = self.unchecked.pop_min().unwrap();
            let tx = self.state.remove(id.key).unwrap();

            // SAFETY:
            // - Trust Agave to behave correctly and not free these transactions.
            // - We have not previously freed this transaction as each region has a single
            //   unique entry in `self.state`.
            unsafe {
                self.allocator.free_offset(tx.offset);
            }
        }

        // TODO: Need to dedupe already seen transactions.

        for _ in 0..additional {
            let msg = self.tpu_to_pack.try_read().unwrap();

            match Self::calculate_priority(&self.runtime, &self.allocator, msg) {
                Some((priority, cost)) => {
                    let key = self.state.insert(msg.transaction);
                    self.unchecked.push(PriorityId { priority, cost, key });
                }
                // SAFETY:
                // - Trust Agave to have correctly allocated & trenferred ownership of this
                //   transaction region to us.
                None => unsafe {
                    self.allocator.free_offset(msg.transaction.offset);
                },
            }
        }
    }

    fn schedule_checks(&mut self) {
        let worker = &mut self.workers[0];
        worker.pack_to_worker.sync();

        // Loop until worker queue is filled or backlog is empty.
        let worker_capacity = worker.pack_to_worker.capacity();
        for _ in 0..worker_capacity {
            if self.unchecked.is_empty() {
                break;
            }

            worker
                .pack_to_worker
                .try_write(PackToWorkerMessage {
                    flags: pack_message_flags::CHECK
                        | check_flags::STATUS_CHECKS
                        | check_flags::LOAD_FEE_PAYER_BALANCE
                        | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
                    max_working_slot: self.progress.current_slot + 1,
                    batch: Self::collect_batch(&self.allocator, || {
                        self.unchecked.pop_max().map(|id| (id, self.state[id.key]))
                    }),
                })
                .unwrap();
        }

        worker.pack_to_worker.commit();
    }

    fn schedule_execute(&mut self) {
        debug_assert_eq!(self.progress.leader_state, IS_LEADER);
        let budget_percentage =
            std::cmp::min(self.progress.current_slot_progress + BLOCK_FILL_CUTOFF, 100);
        // TODO: Would be ideal for the scheduler protocol to tell us the max block
        // units.
        let budget_limit = MAX_BLOCK_UNITS_SIMD_0256 * u64::from(budget_percentage) / 100;
        let cost_used = self.progress.remaining_cost_units + u64::from(self.in_flight_cost);
        let mut budget_remaining = budget_limit.saturating_sub(cost_used);
        for worker in &mut self.workers[1..] {
            if budget_remaining == 0 || self.checked.is_empty() {
                continue;
            }

            let batch = Self::collect_batch(&self.allocator, || {
                self.checked
                    .pop_max()
                    .filter(|id| {
                        let exceeded = u64::from(id.cost) > budget_remaining;
                        budget_remaining = budget_remaining.saturating_sub(u64::from(id.cost));

                        // Re-queue the transaction if we can't schedule it due to cost limits.
                        if exceeded {
                            self.checked.push(*id);
                        }

                        !exceeded
                    })
                    .map(|id| {
                        self.in_flight_cost += id.cost;

                        (id, self.state[id.key])
                    })
            });

            worker.pack_to_worker.sync();
            // TODO: Figure out back pressure with workers to ensure they are all keeping
            // up.
            worker
                .pack_to_worker
                .try_write(PackToWorkerMessage {
                    flags: pack_message_flags::EXECUTE,
                    max_working_slot: self.progress.current_slot + 1,
                    batch,
                })
                .unwrap();
            worker.pack_to_worker.commit();
        }
    }

    fn on_check_response(
        allocator: &Allocator,
        batch: &TransactionPtrBatch<PriorityId>,
        responses: &CheckResponsesPtr,
    ) -> impl Iterator<Item = (SharableTransactionRegion, PriorityId)> {
        assert_eq!(batch.len(), responses.len());

        batch
            .iter()
            .zip(responses.iter().copied())
            .filter_map(|((tx, meta), rep)| {
                let parsing_failed =
                    rep.parsing_and_sanitization_flags == parsing_and_sanitization_flags::FAILED;
                let status_failed = rep.status_check_flags
                    & !(status_check_flags::REQUESTED | status_check_flags::PERFORMED)
                    != 0;
                if parsing_failed || status_failed {
                    // SAFETY:
                    // - TX was previously allocated with this allocator.
                    // - Trust Agave to not free this TX while returning it to us.
                    unsafe { tx.free(allocator) }

                    return None;
                }

                // Sanity check the flags.
                assert_ne!(rep.status_check_flags & status_check_flags::REQUESTED, 0);
                assert_ne!(rep.status_check_flags & status_check_flags::PERFORMED, 0);
                assert_eq!(rep.resolve_flags, resolve_flags::REQUESTED | resolve_flags::PERFORMED);
                assert_eq!(
                    rep.fee_payer_balance_flags,
                    fee_payer_balance_flags::REQUESTED | fee_payer_balance_flags::PERFORMED
                );

                // SAFETY
                // - TX was validly constructed as our code controls this.
                Some((unsafe { tx.to_sharable_transaction_region(allocator) }, meta))
            })
    }

    fn collect_batch(
        allocator: &Allocator,
        mut pop: impl FnMut() -> Option<(PriorityId, SharableTransactionRegion)>,
    ) -> SharableTransactionBatchRegion {
        // Allocate a batch that can hold all our transaction pointers.
        let transactions = allocator.allocate(TX_BATCH_SIZE as u32).unwrap();
        let transactions_offset = unsafe { allocator.offset(transactions) };

        // Get our two pointers to the TX region & meta region.
        let tx_ptr = allocator
            .ptr_from_offset(transactions_offset)
            .cast::<SharableTransactionRegion>();
        // SAFETY:
        // - Pointer is guaranteed to not overrun the allocation as we just created it
        //   with a sufficient size.
        let meta_ptr = unsafe {
            allocator
                .ptr_from_offset(transactions_offset)
                .byte_add(TX_BATCH_META_OFFSET)
                .cast::<PriorityId>()
        };

        // Fill in the batch with transaction pointers.
        let mut num_transactions = 0;
        while num_transactions < MAX_TRANSACTIONS_PER_MESSAGE {
            let Some((id, tx)) = pop() else {
                break;
            };

            // SAFETY:
            // - We have allocated the transaction batch to support at least
            //   `MAX_TRANSACTIONS_PER_MESSAGE`, we terminate the loop before we overrun the
            //   region.
            unsafe {
                tx_ptr.add(num_transactions).write(tx);
                meta_ptr.add(num_transactions).write(id);
            };

            num_transactions += 1;
        }

        SharableTransactionBatchRegion {
            num_transactions: num_transactions.try_into().unwrap(),
            transactions_offset,
        }
    }

    fn calculate_priority(
        runtime: &RuntimeState,
        allocator: &Allocator,
        msg: &TpuToPackMessage,
    ) -> Option<(u64, u32)> {
        let tx = SanitizedTransactionView::try_new_sanitized(
            // SAFETY:
            // - Trust Agave to have allocated the shared transactin region correctly.
            // - `SanitizedTransactionView` does not free the allocation on drop.
            unsafe {
                TransactionPtr::from_sharable_transaction_region(&msg.transaction, allocator)
            },
            true,
        )
        .ok()?;
        let tx = RuntimeTransaction::<TransactionView<true, TransactionPtr>>::try_from(
            tx,
            MessageHash::Compute,
            None,
        )
        .ok()?;

        // Compute transaction cost.
        let compute_budget_limits =
            compute_budget_instruction_details::ComputeBudgetInstructionDetails::try_from(
                tx.program_instructions_iter(),
            )
            .ok()?
            .sanitize_and_convert_to_compute_budget_limits(&runtime.feature_set)
            .ok()?;
        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let cost = CostModel::calculate_cost(&tx, &runtime.feature_set).sum();

        // Compute transaction reward.
        let fee_details = solana_fee::calculate_fee_details(
            &tx,
            false,
            runtime.lamports_per_signature,
            fee_budget_limits.prioritization_fee,
            runtime.fee_features,
        );
        let burn = fee_details
            .transaction_fee()
            .checked_mul(runtime.burn_percent)?
            / 100;
        let base_fee = fee_details.transaction_fee() - burn;
        let reward = base_fee.saturating_add(fee_details.prioritization_fee());

        // Compute priority.
        Some((
            reward
                .saturating_mul(1_000_000)
                .saturating_div(cost.saturating_add(1)),
            // TODO: Is it possible to craft a TX that passes sanitization with a cost > u32::MAX?
            cost.try_into().unwrap(),
        ))
    }
}

struct RuntimeState {
    feature_set: FeatureSet,
    fee_features: FeeFeatures,
    lamports_per_signature: u64,
    burn_percent: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
struct PriorityId {
    priority: u64,
    cost: u32,
    key: TransactionStateKey,
}

impl PartialOrd for PriorityId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.cost.cmp(&other.cost))
            .then_with(|| self.key.cmp(&other.key))
    }
}

slotmap::new_key_type! {
    struct TransactionStateKey;
}

#[cfg(test)]
mod tests {
    use std::os::fd::IntoRawFd;
    use std::ptr::NonNull;

    use agave_scheduler_bindings::worker_message_types::CheckResponse;
    use agave_scheduler_bindings::{
        SharablePubkeys, SharableTransactionRegion, WorkerToPackMessage, processed_codes,
    };
    use agave_scheduling_utils::handshake::server::AgaveSession;
    use agave_scheduling_utils::handshake::{self, ClientLogon};
    use agave_scheduling_utils::responses_region::allocate_check_response_region;
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Pubkey};
    use solana_transaction::Transaction;

    use super::*;

    #[test]
    fn check_no_schedule() {
        let mut harness = Harness::setup();

        // Ingest a simple transfer.
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        harness.send_tx(&solana_system_transaction::transfer(&from, &to, 1, Hash::new_unique()));

        // Poll the greedy scheduler.
        harness.poll_scheduler();

        // Assert - A single request (to check the TX) is sent.
        let mut worker_requests = harness.drain_pack_to_workers();
        assert_eq!(worker_requests.len(), 1);
        let (worker_index, message) = worker_requests.remove(0);
        assert_eq!(message.flags & 1, pack_message_flags::CHECK);

        // Respond with OK.
        harness.send_check_ok(worker_index, message);
        harness.poll_scheduler();

        // Assert - Scheduler does not schedule the valid TX.
        assert!(harness.session.workers.iter_mut().all(|worker| {
            worker.pack_to_worker.sync();

            worker.pack_to_worker.is_empty()
        }));
    }

    #[test]
    fn check_then_schedule() {
        let mut harness = Harness::setup();

        // Notify the scheduler that node is now leader.
        harness.send_progress(ProgressMessage {
            leader_state: IS_LEADER,
            current_slot: 10,
            next_leader_slot: 11,
            leader_range_end: 11,
            remaining_cost_units: 10_000_000,
            current_slot_progress: 25,
        });

        // Ingest a simple transfer.
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        harness.send_tx(&solana_system_transaction::transfer(&from, &to, 1, Hash::new_unique()));

        // Poll the greedy scheduler.
        harness.poll_scheduler();

        // Assert - A single request (to check the TX) is sent.
        let mut worker_requests = harness.drain_pack_to_workers();
        assert_eq!(worker_requests.len(), 1);
        let (worker_index, message) = worker_requests.remove(0);
        assert_eq!(message.flags & 1, pack_message_flags::CHECK);

        // Respond with OK.
        harness.send_check_ok(worker_index, message);
        harness.poll_scheduler();

        // Assert - A single request (to execute the TX) is sent.
        let mut worker_requests = harness.drain_pack_to_workers();
        assert_eq!(worker_requests.len(), 1);
        let (_, message) = worker_requests.remove(0);
        assert_eq!(message.flags & 1, pack_message_flags::EXECUTE);
        assert_eq!(message.batch.num_transactions, 1);
    }

    struct Harness {
        slot: u64,
        session: AgaveSession,
        scheduler: GreedyScheduler,
    }

    impl Harness {
        fn setup() -> Self {
            let logon = ClientLogon {
                worker_count: 4,
                allocator_size: 16 * 1024 * 1024,
                allocator_handles: 1,
                tpu_to_pack_capacity: 16,
                progress_tracker_capacity: 16,
                pack_to_worker_capacity: 16,
                worker_to_pack_capacity: 16,
                flags: 0,
            };
            let (agave_session, files) = handshake::server::Server::setup_session(logon).unwrap();
            let client_session = handshake::client::setup_session(
                &logon,
                files.into_iter().map(IntoRawFd::into_raw_fd).collect(),
            )
            .unwrap();

            Self {
                slot: 0,
                session: agave_session,
                scheduler: GreedyScheduler::new(client_session),
            }
        }

        fn allocator(&self) -> &Allocator {
            &self.session.tpu_to_pack.allocator
        }

        fn poll_scheduler(&mut self) {
            self.scheduler.poll();
        }

        fn drain_pack_to_workers(&mut self) -> Vec<(usize, PackToWorkerMessage)> {
            self.session
                .workers
                .iter_mut()
                .enumerate()
                .flat_map(|(i, worker)| {
                    worker.pack_to_worker.sync();

                    std::iter::from_fn(move || {
                        worker.pack_to_worker.try_read().map(|msg| (i, *msg))
                    })
                })
                .collect()
        }

        fn send_progress(&mut self, progress: ProgressMessage) {
            self.slot = progress.current_slot;

            self.session.progress_tracker.sync();
            self.session.progress_tracker.try_write(progress).unwrap();
            self.session.progress_tracker.commit();
        }

        fn send_tx(&mut self, tx: &Transaction) {
            // Serialize & copy the pointer to shared memory.
            let mut packet = bincode::serialize(&tx).unwrap();
            let packet_len = packet.len().try_into().unwrap();
            let pointer = self.allocator().allocate(packet_len).unwrap();
            unsafe {
                pointer.copy_from_nonoverlapping(
                    NonNull::new(packet.as_mut_ptr()).unwrap(),
                    packet.len(),
                );
            }
            let offset = unsafe { self.allocator().offset(pointer) };
            let tx = SharableTransactionRegion { offset, length: packet_len };

            self.session.tpu_to_pack.producer.sync();
            self.session
                .tpu_to_pack
                .producer
                .try_write(TpuToPackMessage { transaction: tx, flags: 0, src_addr: [0; 16] })
                .unwrap();
            self.session.tpu_to_pack.producer.commit();
        }

        fn send_check_ok(&mut self, worker_index: usize, msg: PackToWorkerMessage) {
            assert_eq!(msg.batch.num_transactions, 1);

            let (mut response_ptr, responses) =
                allocate_check_response_region(self.allocator(), 1).unwrap();
            unsafe {
                *response_ptr.as_mut() = CheckResponse {
                    parsing_and_sanitization_flags: 0,
                    status_check_flags: status_check_flags::REQUESTED
                        | status_check_flags::PERFORMED,
                    fee_payer_balance_flags: fee_payer_balance_flags::REQUESTED
                        | fee_payer_balance_flags::PERFORMED,
                    resolve_flags: resolve_flags::REQUESTED | resolve_flags::PERFORMED,
                    included_slot: self.slot,
                    balance_slot: self.slot,
                    fee_payer_balance: u64::from(u32::MAX),
                    resolution_slot: self.slot,
                    min_alt_deactivation_slot: u64::MAX,
                    resolved_pubkeys: SharablePubkeys { offset: 0, num_pubkeys: 0 },
                };
            }

            let queue = &mut self.session.workers[worker_index].worker_to_pack;
            queue.sync();
            queue
                .try_write(WorkerToPackMessage {
                    batch: msg.batch,
                    processed_code: processed_codes::PROCESSED,
                    responses,
                })
                .unwrap();
            queue.commit();
        }
    }
}
