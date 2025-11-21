use agave_bridge::{
    Bridge, KeyedTransactionMeta, RuntimeState, ScheduleBatch, TransactionId, TxDecision, Worker,
    WorkerAction, WorkerResponse,
};
use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{
    CheckResponse, ExecutionResponse, fee_payer_balance_flags, not_included_reasons,
    parsing_and_sanitization_flags, resolve_flags, status_check_flags,
};
use agave_scheduler_bindings::{
    IS_LEADER, MAX_TRANSACTIONS_PER_MESSAGE, SharableTransactionRegion, pack_message_flags,
};
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use hashbrown::HashMap;
use metrics::{Counter, Gauge, counter, gauge};
use min_max_heap::MinMaxHeap;
use solana_compute_budget_instruction::compute_budget_instruction_details;
use solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS_SIMD_0256;
use solana_cost_model::cost_model::CostModel;
use solana_fee_structure::FeeBudgetLimits;
use solana_pubkey::Pubkey;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_svm_transaction::svm_message::SVMStaticMessage;
use solana_transaction::sanitized::MessageHash;

const UNCHECKED_CAPACITY: usize = 64 * 1024;
const CHECKED_CAPACITY: usize = 64 * 1024;

const TX_REGION_SIZE: usize = std::mem::size_of::<SharableTransactionRegion>();
const TX_BATCH_PER_MESSAGE: usize = TX_REGION_SIZE + std::mem::size_of::<PriorityId>();
const TX_BATCH_SIZE: usize = TX_BATCH_PER_MESSAGE * MAX_TRANSACTIONS_PER_MESSAGE;
const_assert!(TX_BATCH_SIZE < 4096);

const CHECK_WORKER: usize = 0;
/// How many percentage points before the end should we aim to fill the block.
const BLOCK_FILL_CUTOFF: u8 = 20;

pub struct GreedyScheduler {
    unchecked: MinMaxHeap<PriorityId>,
    checked: MinMaxHeap<PriorityId>,
    cu_in_flight: u32,
    schedule_locks: HashMap<Pubkey, bool>,
    schedule_batch: Vec<KeyedTransactionMeta<PriorityId>>,

    metrics: GreedyMetrics,
}

impl GreedyScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self {
            unchecked: MinMaxHeap::with_capacity(UNCHECKED_CAPACITY),
            checked: MinMaxHeap::with_capacity(UNCHECKED_CAPACITY),
            cu_in_flight: 0,
            schedule_locks: HashMap::default(),
            schedule_batch: Vec::default(),

            metrics: GreedyMetrics::new(),
        }
    }

    pub fn poll<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        // Drain the progress tracker so we know which slot we're on.
        bridge.drain_progress();
        let is_leader = bridge.progress().leader_state == IS_LEADER;

        // TODO: Think about re-checking all TXs on slot roll (or at least
        // expired TXs). If we do this we should use a dense slotmap to make
        // iteration fast.

        // Drain responses from workers.
        self.drain_worker_responses(bridge);

        // Ingest a bounded amount of new transactions.
        match is_leader {
            true => self.drain_tpu(bridge, 128),
            false => self.drain_tpu(bridge, 1024),
        }

        // Queue additional checks.
        self.schedule_checks(bridge);

        // Schedule if we're currently the leader.
        if is_leader {
            self.schedule_execute(bridge);
        }

        // Update metrics.
        self.metrics.slot.set(bridge.progress().current_slot as f64);
        self.metrics
            .next_leader_slot
            .set(bridge.progress().next_leader_slot as f64);
        self.metrics.unchecked_len.set(self.unchecked.len() as f64);
        self.metrics.checked_len.set(self.checked.len() as f64);
        self.metrics.cu_in_flight.set(f64::from(self.cu_in_flight));
    }

    fn drain_worker_responses<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        for worker in 0..5 {
            while bridge.pop_worker(worker, |bridge, WorkerResponse { meta, response, .. }| {
                match response {
                    WorkerAction::Unprocessed => TxDecision::Keep,
                    WorkerAction::Check(rep, _) => self.on_check(bridge, meta, rep),
                    WorkerAction::Execute(rep) => self.on_execute(meta, rep),
                }
            }) {}
        }
    }

    fn drain_tpu<B>(&mut self, bridge: &mut B, max_count: usize)
    where
        B: Bridge<Meta = PriorityId>,
    {
        let additional = std::cmp::min(bridge.tpu_len(), max_count);
        let shortfall = (self.checked.len() + additional).saturating_sub(UNCHECKED_CAPACITY);

        // NB: Technically we are evicting more than we need to because not all of
        // `additional` will parse correctly & thus have a priority.
        for _ in 0..shortfall {
            let id = self.unchecked.pop_min().unwrap();

            bridge.tx_drop(id.key);
        }
        self.metrics.recv_evict.increment(shortfall as u64);

        // TODO: Need to dedupe already seen transactions?

        bridge.tpu_drain(
            |bridge, key| match Self::calculate_priority(bridge.runtime(), &bridge.tx(key).data) {
                Some((priority, cost)) => {
                    self.unchecked.push(PriorityId { priority, cost, key });
                    self.metrics.recv_ok.increment(1);

                    TxDecision::Keep
                }
                None => {
                    self.metrics.recv_err.increment(1);

                    TxDecision::Drop
                }
            },
            max_count,
        );
    }

    fn schedule_checks<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        // Loop until worker queue is filled or backlog is empty.
        let start_len = self.unchecked.len();
        while bridge.worker(0).rem() > 0 {
            if self.unchecked.is_empty() {
                break;
            }

            self.schedule_batch.clear();
            self.schedule_batch.extend(std::iter::from_fn(|| {
                self.unchecked
                    .pop_max()
                    .map(|id| KeyedTransactionMeta { key: id.key, meta: id })
            }));
            bridge.schedule(ScheduleBatch {
                worker: CHECK_WORKER,
                transactions: &self.schedule_batch,
                max_working_slot: u64::MAX,
                flags: pack_message_flags::CHECK
                    | check_flags::STATUS_CHECKS
                    | check_flags::LOAD_FEE_PAYER_BALANCE
                    | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
            });
        }

        // Update metrics with our scheduled amount.
        self.metrics
            .check_requested
            .increment((start_len - self.unchecked.len()) as u64);
    }

    fn schedule_execute<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        self.schedule_locks.clear();

        debug_assert_eq!(bridge.progress().leader_state, IS_LEADER);
        let budget_percentage =
            std::cmp::min(bridge.progress().current_slot_progress + BLOCK_FILL_CUTOFF, 100);
        // TODO: Would be ideal for the scheduler protocol to tell us the max block
        // units.
        let budget_limit = MAX_BLOCK_UNITS_SIMD_0256 * u64::from(budget_percentage) / 100;
        let cost_used = MAX_BLOCK_UNITS_SIMD_0256
            .saturating_sub(bridge.progress().remaining_cost_units)
            + u64::from(self.cu_in_flight);
        let mut budget_remaining = budget_limit.saturating_sub(cost_used);
        for worker in 1..bridge.worker_count() {
            if budget_remaining == 0 || self.checked.is_empty() {
                break;
            }

            // If the worker already has a pending job, don't give it any more.
            if bridge.worker(worker).len() > 0 {
                continue;
            }

            let pop_next = || {
                self.checked
                    .pop_max()
                    .filter(|id| {
                        // Check if we can fit the TX within our budget.
                        if u64::from(id.cost) > budget_remaining {
                            self.checked.push(*id);

                            return false;
                        }

                        // Check if this transaction's read/write locks conflict with any
                        // pre-existing read/write locks.
                        let tx = bridge.tx(id.key);
                        if tx
                            .write_locks()
                            .any(|key| self.schedule_locks.insert(*key, true).is_some())
                            || tx.read_locks().any(|key| {
                                self.schedule_locks
                                    .insert(*key, false)
                                    .is_some_and(|writable| writable)
                            })
                        {
                            self.checked.push(*id);
                            budget_remaining = 0;

                            return false;
                        }

                        // Update the budget as we are scheduling this TX.
                        budget_remaining = budget_remaining.saturating_sub(u64::from(id.cost));
                        self.cu_in_flight += id.cost;

                        true
                    })
                    .map(|id| KeyedTransactionMeta { key: id.key, meta: id })
            };

            self.schedule_batch.clear();
            self.schedule_batch.extend(std::iter::from_fn(pop_next));

            // If we failed to schedule anything, don't send the batch.
            if self.schedule_batch.is_empty() {
                break;
            }

            // Update metrics.
            self.metrics
                .execute_requested
                .increment(self.schedule_batch.len() as u64);

            // Write the next batch for the worker.
            bridge.schedule(ScheduleBatch {
                worker,
                transactions: &self.schedule_batch,
                max_working_slot: bridge.progress().current_slot + 1,
                flags: pack_message_flags::EXECUTE,
            });
        }
    }

    fn on_check<B>(&mut self, bridge: &mut B, meta: PriorityId, rep: CheckResponse) -> TxDecision
    where
        B: Bridge<Meta = PriorityId>,
    {
        let parsing_failed =
            rep.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED != 0;
        let resolve_failed = rep.resolve_flags & resolve_flags::FAILED != 0;
        let status_ok = status_check_flags::REQUESTED | status_check_flags::PERFORMED;
        let status_failed = rep.status_check_flags & !status_ok != 0;
        if parsing_failed || resolve_failed || status_failed {
            self.metrics.check_err.increment(1);

            return TxDecision::Drop;
        }

        // Sanity check the flags.
        assert_eq!(
            rep.fee_payer_balance_flags,
            fee_payer_balance_flags::REQUESTED | fee_payer_balance_flags::PERFORMED,
            "{rep:?}"
        );
        assert_eq!(
            rep.resolve_flags,
            resolve_flags::REQUESTED | resolve_flags::PERFORMED,
            "{rep:?}"
        );
        assert_ne!(rep.status_check_flags & status_check_flags::REQUESTED, 0, "{rep:?}");
        assert_ne!(rep.status_check_flags & status_check_flags::PERFORMED, 0, "{rep:?}");

        // Evict lowest priority if at capacity.
        if self.checked.len() == CHECKED_CAPACITY {
            let id = self.checked.pop_min().unwrap();
            bridge.tx_drop(id.key);

            self.metrics.check_evict.increment(1);
        }

        // Insert the new transaction (yes this may be lower priority then what
        // we just evicted but that's fine).
        self.checked.push(meta);

        // Update ok metric.
        self.metrics.check_ok.increment(1);

        TxDecision::Keep
    }

    fn on_execute(&mut self, meta: PriorityId, rep: ExecutionResponse) -> TxDecision {
        // Remove in-flight costs.
        self.cu_in_flight -= meta.cost;

        // Update metrics.
        match rep.not_included_reason == not_included_reasons::NONE {
            true => self.metrics.execute_ok.increment(1),
            false => self.metrics.execute_err.increment(1),
        }

        TxDecision::Drop
    }

    fn calculate_priority(
        runtime: &RuntimeState,
        tx: &SanitizedTransactionView<TransactionPtr>,
    ) -> Option<(u64, u32)> {
        // Construct runtime transaction.
        let tx = RuntimeTransaction::<&SanitizedTransactionView<TransactionPtr>>::try_new(
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

struct GreedyMetrics {
    slot: Gauge,
    next_leader_slot: Gauge,
    unchecked_len: Gauge,
    checked_len: Gauge,
    cu_in_flight: Gauge,
    recv_ok: Counter,
    recv_err: Counter,
    recv_evict: Counter,
    check_requested: Counter,
    check_ok: Counter,
    check_err: Counter,
    check_evict: Counter,
    execute_requested: Counter,
    execute_ok: Counter,
    execute_err: Counter,
    // TODO: Inspect execute responses to determine ok/err.
}

impl GreedyMetrics {
    fn new() -> Self {
        Self {
            slot: gauge!("slot"),
            next_leader_slot: gauge!("next_leader_slot"),
            unchecked_len: gauge!("unchecked_len"),
            checked_len: gauge!("checked_len"),
            recv_ok: counter!("recv_ok"),
            recv_err: counter!("recv_err"),
            recv_evict: counter!("recv_evict"),
            cu_in_flight: gauge!("cu_in_flight"),
            check_requested: counter!("check_requested"),
            check_ok: counter!("check_ok"),
            check_err: counter!("check_err"),
            check_evict: counter!("check_evict"),
            execute_requested: counter!("execute_requested"),
            execute_ok: counter!("execute_ok"),
            execute_err: counter!("execute_err"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PriorityId {
    priority: u64,
    cost: u32,
    key: TransactionId,
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

#[cfg(test)]
mod tests {
    use agave_bridge::TestBridge;
    use agave_scheduler_bindings::{IS_NOT_LEADER, ProgressMessage};
    use solana_compute_budget_interface::ComputeBudgetInstruction;
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Pubkey, Signer};
    use solana_message::{AddressLookupTableAccount, v0};
    use solana_transaction::versioned::VersionedTransaction;
    use solana_transaction::{AccountMeta, Instruction, Transaction, VersionedMessage};

    use super::*;

    const MOCK_PROGRESS: ProgressMessage = ProgressMessage {
        leader_state: IS_NOT_LEADER,
        current_slot: 10,
        next_leader_slot: 11,
        leader_range_end: 11,
        remaining_cost_units: 50_000_000,
        current_slot_progress: 25,
    };

    #[test]
    fn check_no_schedule() {
        let mut bridge = TestBridge::new(5, 4);
        let mut scheduler = GreedyScheduler::new();

        // Ingest a simple transfer.
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        bridge.queue_tpu(
            &solana_system_transaction::transfer(&from, &to, 1, Hash::new_unique()).into(),
        );

        // Poll the greedy scheduler.
        scheduler.poll(&mut bridge);

        // Assert - A single request (to check the TX) is sent.
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);
        assert_eq!(batch.flags & 1, pack_message_flags::CHECK);
        assert_eq!(batch.transactions.len(), 1);

        // Respond with OK.
        bridge.queue_check_response(&batch, 0, None);
        scheduler.poll(&mut bridge);

        // Assert - Scheduler does not schedule the valid TX as we are not leader.
        assert_eq!(bridge.pop_schedule(), None);
    }

    #[test]
    fn check_then_schedule() {
        let mut bridge = TestBridge::new(5, 4);
        let mut scheduler = GreedyScheduler::new();

        // Notify the scheduler that node is now leader.
        bridge.queue_progress(ProgressMessage { leader_state: IS_LEADER, ..MOCK_PROGRESS });

        // Ingest a simple transfer.
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        bridge.queue_tpu(
            &solana_system_transaction::transfer(&from, &to, 1, Hash::new_unique()).into(),
        );

        // Poll the greedy scheduler.
        scheduler.poll(&mut bridge);

        // Assert - A single request (to check the TX) is sent.
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);
        assert_eq!(batch.flags & 1, pack_message_flags::CHECK);
        assert_eq!(batch.transactions.len(), 1);

        // Respond with OK.
        bridge.queue_check_response(&batch, 0, None);
        scheduler.poll(&mut bridge);

        // Assert - A single request (to execute the TX) is sent.
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);
        assert_eq!(batch.flags & 1, pack_message_flags::EXECUTE);
        assert_eq!(batch.transactions.len(), 1);
    }

    #[test]
    fn schedule_by_priority_static_non_conflicting() {
        let mut bridge = TestBridge::new(5, 4);
        let mut scheduler = GreedyScheduler::new();

        // Ingest a simple transfer (with low priority).
        let payer0 = Keypair::new();
        let tx0 = noop_with_budget(&payer0, 25_000, 100);
        bridge.queue_tpu(&tx0);
        scheduler.poll(&mut bridge);
        bridge.queue_all_checks_ok();
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Ingest a simple transfer (with high priority).
        let payer1 = Keypair::new();
        let tx1 = noop_with_budget(&payer1, 25_000, 500);
        bridge.queue_tpu(&tx1);
        scheduler.poll(&mut bridge);
        bridge.queue_all_checks_ok();
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Become the leader of a slot that is 50% done with a lot of remaining cost
        // units.
        bridge.queue_progress(ProgressMessage {
            leader_state: IS_LEADER,
            current_slot_progress: 50,
            remaining_cost_units: 50_000_000,
            ..MOCK_PROGRESS
        });

        // Assert - Scheduler has scheduled both.
        scheduler.poll(&mut bridge);
        let batch0 = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);

        let [ex0, ex1] = batch0.transactions[..] else {
            panic!();
        };
        assert_eq!(bridge.tx(ex0.key).data.signatures()[0], tx1.signatures[0]);
        assert_eq!(bridge.tx(ex1.key).data.signatures()[0], tx0.signatures[0]);
    }

    #[test]
    fn schedule_by_priority_static_conflicting() {
        let mut bridge = TestBridge::new(5, 4);
        let mut scheduler = GreedyScheduler::new();

        // Ingest a simple transfer (with low priority).
        let payer = Keypair::new();
        let tx0 = noop_with_budget(&payer, 25_000, 100);
        bridge.queue_tpu(&tx0);
        scheduler.poll(&mut bridge);
        bridge.queue_all_checks_ok();
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Ingest a simple transfer (with high priority).
        let tx1 = noop_with_budget(&payer, 25_000, 500);
        bridge.queue_tpu(&tx1);
        scheduler.poll(&mut bridge);
        bridge.queue_all_checks_ok();
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Become the leader of a slot that is 50% done with a lot of remaining cost
        // units.
        bridge.queue_progress(ProgressMessage {
            leader_state: IS_LEADER,
            current_slot_progress: 50,
            remaining_cost_units: 50_000_000,
            ..MOCK_PROGRESS
        });

        // Assert - Scheduler has scheduled tx1.
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);
        let [ex0] = &batch.transactions[..] else {
            panic!();
        };
        assert_eq!(bridge.tx(ex0.key).data.signatures()[0], tx1.signatures[0]);

        // Assert - Scheduler has scheduled tx0.
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);
        let [ex1] = &batch.transactions[..] else {
            panic!();
        };
        assert_eq!(bridge.tx(ex1.key).data.signatures()[0], tx0.signatures[0]);
    }

    #[test]
    fn schedule_by_priority_alt_non_conflicting() {
        let mut bridge = TestBridge::new(5, 4);
        let mut scheduler = GreedyScheduler::new();
        let resolved_pubkeys = vec![Pubkey::new_from_array([1; 32])];

        // Ingest a simple transfer (with low priority).
        let payer0 = Keypair::new();
        let read_lock = Pubkey::new_unique();
        let tx0 = noop_with_alt_locks(&payer0, &[], &[read_lock], 25_000, 100);
        bridge.queue_tpu(&tx0);
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        bridge.queue_check_response(&batch, 0, Some(resolved_pubkeys.clone()));
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Ingest a simple transfer (with high priority).
        let payer1 = Keypair::new();
        let tx1 = noop_with_alt_locks(&payer1, &[], &[read_lock], 25_000, 500);
        bridge.queue_tpu(&tx1);
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        bridge.queue_check_response(&batch, 0, Some(resolved_pubkeys));
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Become the leader of a slot that is 50% done with a lot of remaining cost
        // units.
        bridge.queue_progress(ProgressMessage {
            leader_state: IS_LEADER,
            current_slot_progress: 50,
            remaining_cost_units: 50_000_000,
            ..MOCK_PROGRESS
        });

        // Assert - Scheduler has scheduled both.
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);
        let [ex0, ex1] = batch.transactions[..] else {
            panic!();
        };
        assert_eq!(bridge.tx(ex0.key).data.signatures()[0], tx1.signatures[0]);
        assert_eq!(bridge.tx(ex1.key).data.signatures()[0], tx0.signatures[0]);
    }

    #[test]
    fn schedule_by_priority_alt_conflicting() {
        let mut bridge = TestBridge::new(5, 4);
        let mut scheduler = GreedyScheduler::new();

        // Ingest a simple transfer (with low priority).
        let payer0 = Keypair::new();
        let write_lock = Pubkey::new_unique();
        let resolved_pubkeys = vec![write_lock];
        let tx0 = noop_with_alt_locks(&payer0, &[write_lock], &[], 25_000, 100);
        bridge.queue_tpu(&tx0);
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        bridge.queue_check_response(&batch, 0, Some(resolved_pubkeys.clone()));
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Ingest a simple transfer (with high priority).
        let payer1 = Keypair::new();
        let tx1 = noop_with_alt_locks(&payer1, &[write_lock], &[], 25_000, 500);
        bridge.queue_tpu(&tx1);
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        bridge.queue_check_response(&batch, 0, Some(resolved_pubkeys));
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Become the leader of a slot that is 50% done with a lot of remaining cost
        // units.
        bridge.queue_progress(ProgressMessage {
            leader_state: IS_LEADER,
            current_slot_progress: 50,
            remaining_cost_units: 50_000_000,
            ..MOCK_PROGRESS
        });

        // Assert - Scheduler has scheduled tx1.
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);
        let [ex0] = batch.transactions[..] else {
            panic!();
        };
        assert_eq!(bridge.tx(ex0.key).data.signatures()[0], tx1.signatures[0]);

        // Assert - Scheduler has scheduled tx0.
        scheduler.poll(&mut bridge);
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(bridge.pop_schedule(), None);
        let [ex1] = batch.transactions[..] else {
            panic!();
        };
        assert_eq!(bridge.tx(ex1.key).data.signatures()[0], tx0.signatures[0]);
    }

    fn noop_with_budget(payer: &Keypair, cu_limit: u32, cu_price: u64) -> VersionedTransaction {
        Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
                ComputeBudgetInstruction::set_compute_unit_price(cu_price),
            ],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::new_from_array([1; 32]),
        )
        .into()
    }

    fn noop_with_alt_locks(
        payer: &Keypair,
        write: &[Pubkey],
        read: &[Pubkey],
        cu_limit: u32,
        cu_price: u64,
    ) -> VersionedTransaction {
        VersionedTransaction::try_new(
            VersionedMessage::V0(
                v0::Message::try_compile(
                    &payer.pubkey(),
                    &[
                        ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
                        ComputeBudgetInstruction::set_compute_unit_price(cu_price),
                        Instruction {
                            program_id: Pubkey::default(),
                            accounts: write
                                .iter()
                                .map(|key| (*key, true))
                                .chain(read.iter().map(|key| (*key, false)))
                                .map(|(key, is_writable)| AccountMeta {
                                    pubkey: key,
                                    is_signer: false,
                                    is_writable,
                                })
                                .collect(),
                            data: vec![],
                        },
                    ],
                    &[AddressLookupTableAccount {
                        key: Pubkey::new_unique(),
                        addresses: [write, read].concat(),
                    }],
                    Hash::new_from_array([1; 32]),
                )
                .unwrap(),
            ),
            &[payer],
        )
        .unwrap()
    }
}
