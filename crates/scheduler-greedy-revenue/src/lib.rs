#[macro_use]
extern crate static_assertions;

use std::collections::BTreeSet;
use std::ops::Bound;
use std::time::{Duration, Instant};

use agave_bridge::{
    Bridge, KeyedTransactionMeta, RuntimeState, ScheduleBatch, TransactionKey, TxDecision, Worker,
    WorkerAction, WorkerResponse,
};
use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{
    CheckResponse, ExecutionResponse, fee_payer_balance_flags, not_included_reasons,
    parsing_and_sanitization_flags, resolve_flags, status_check_flags,
};
use agave_scheduler_bindings::{
    LEADER_READY, MAX_TRANSACTIONS_PER_MESSAGE, SharableTransactionRegion, pack_message_flags,
};
use agave_schedulers::events::{
    CheckFailure, Event, EventEmitter, EvictReason, SlotStatsEvent, TransactionAction,
    TransactionEvent, TransactionSource,
};
use agave_schedulers::shared::PriorityId;
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use hashbrown::hash_map::EntryRef;
use hashbrown::{HashMap, HashSet};
use indexmap::IndexSet;
use metrics::{Counter, Gauge, counter, gauge};
use min_max_heap::MinMaxHeap;
use serde::Deserialize;
use solana_clock::Slot;
use solana_compute_budget_instruction::compute_budget_instruction_details;
use solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS_SIMD_0256;
use solana_cost_model::cost_model::CostModel;
use solana_fee_structure::FeeBudgetLimits;
use solana_pubkey::Pubkey;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_svm_transaction::svm_message::SVMStaticMessage;
use solana_transaction::sanitized::MessageHash;

const PRIORITY_MULTIPLIER: u64 = 1_000_000;
const BUNDLE_MARKER: u64 = u64::MAX;

const TX_REGION_SIZE: usize = std::mem::size_of::<SharableTransactionRegion>();
const TX_BATCH_PER_MESSAGE: usize = TX_REGION_SIZE + std::mem::size_of::<PriorityId>();
const TX_BATCH_SIZE: usize = TX_BATCH_PER_MESSAGE * MAX_TRANSACTIONS_PER_MESSAGE;
const_assert!(TX_BATCH_SIZE < 4096);

const CHECK_WORKER: usize = 0;
const EXECUTE_WORKER_START: usize = 1;
const MAX_CHECK_BATCHES: usize = 4;
/// How many percentage points before the end should we aim to fill the block.
const BLOCK_FILL_CUTOFF: u8 = 20;
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct GreedyRevenueArgs {
    pub workers: usize,
    pub unchecked_capacity: usize,
    pub checked_capacity: usize,
}

pub struct GreedyRevenueScheduler {
    args: GreedyRevenueArgs,

    unchecked_tx: MinMaxHeap<PriorityId>,
    checked_tx: BTreeSet<PriorityId>,
    executing_tx: HashSet<TransactionKey>,
    deferred_tx: IndexSet<PriorityId>,
    next_recheck: Option<PriorityId>,
    in_flight_cus: u64,
    in_flight_locks: HashMap<Pubkey, AccountLockers>,
    schedule_batch: Vec<KeyedTransactionMeta<PriorityId>>,
    last_progress_time: Instant,

    events: Option<EventEmitter>,
    slot: Slot,
    slot_stats: SlotStatsEvent,
    metrics: BatchMetrics,
}

impl GreedyRevenueScheduler {
    /// Create a new greedy scheduler.
    ///
    /// # Panics
    ///
    /// - If [`GreedyRevenueArgs::workers`] is < 2.
    #[must_use]
    pub fn new(events: Option<EventEmitter>, args: GreedyRevenueArgs) -> Self {
        assert!(args.workers >= 2, "need at least 2 workers");

        Self {
            args,

            unchecked_tx: MinMaxHeap::with_capacity(args.unchecked_capacity),
            checked_tx: BTreeSet::new(),
            executing_tx: HashSet::with_capacity(args.checked_capacity),
            deferred_tx: IndexSet::with_capacity(args.checked_capacity),
            next_recheck: None,
            in_flight_cus: 0,
            in_flight_locks: HashMap::new(),
            schedule_batch: Vec::new(),
            last_progress_time: Instant::now(),

            events,
            slot: 0,
            slot_stats: SlotStatsEvent::default(),
            metrics: BatchMetrics::new(),
        }
    }

    pub fn poll<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        // Drain the progress tracker & check for roll.
        self.check_slot_roll(bridge);

        // Drain responses from workers.
        self.drain_worker_responses(bridge);

        // Ingest a bounded amount of new transactions.
        let is_leader = bridge.progress().leader_state == LEADER_READY;
        match is_leader {
            true => self.drain_tpu(bridge, 128),
            false => self.drain_tpu(bridge, 1024),
        }

        // Queue additional checks.
        self.schedule_checks(bridge);

        // Schedule if we're currently the leader.
        if is_leader {
            self.schedule_execute(bridge);

            // Start another recheck if we are not currently performing one.
            self.next_recheck = self
                .next_recheck
                .or_else(|| self.checked_tx.last().copied());
        }

        // Update metrics.
        self.metrics
            .current_slot
            .set(bridge.progress().current_slot as f64);
        self.metrics
            .next_leader_slot
            .set(bridge.progress().next_leader_slot as f64);
        self.metrics
            .tpu_unchecked_len
            .set(self.unchecked_tx.len() as f64);
        self.metrics
            .tpu_checked_len
            .set(self.checked_tx.len() as f64);
        self.metrics
            .executing_len
            .set(self.executing_tx.len() as f64);
        self.metrics
            .tpu_deferred_len
            .set(self.deferred_tx.len() as f64);
        self.metrics
            .locks_len
            .set(self.in_flight_locks.len() as f64);
        self.metrics.in_flight_cus.set(self.in_flight_cus as f64);
    }

    fn check_slot_roll<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        // Drain progress and check for disconnect.
        match bridge.drain_progress() {
            Some(_) => self.last_progress_time = Instant::now(),
            None => assert!(
                self.last_progress_time.elapsed() < PROGRESS_TIMEOUT,
                "Agave disconnected; elapsed={:?}; slot={}",
                self.last_progress_time.elapsed(),
                self.slot,
            ),
        }

        // Check for slot roll.
        let was_leader_ready = self.slot_stats.was_leader_ready;
        let progress = *bridge.progress();

        // Slot has changed.
        if progress.current_slot != self.slot {
            if let Some(events) = &self.events {
                // Emit SlotStats for the slot that just ended.
                if self.slot != 0 {
                    let stats = core::mem::take(&mut self.slot_stats);
                    events.emit(Event::SlotStats(stats));
                }

                // Update context for new slot events.
                events.ctx().set(progress.current_slot);

                // Emit SlotStart for the new slot.
                events.emit(Event::SlotStart);
            }

            // Update our local state.
            self.slot = progress.current_slot;
            self.slot_stats.was_leader_ready = false;

            // Drain deferred transactions back to checked.
            for meta in self.deferred_tx.drain(..) {
                assert!(self.checked_tx.insert(meta));
            }

            // Start another recheck if we are not currently performing one.
            self.next_recheck = self
                .next_recheck
                .or_else(|| self.checked_tx.last().copied());
        }

        // If we have just become the leader, emit an event & configure tip accounts.
        if progress.leader_state == LEADER_READY && !was_leader_ready {
            if let Some(events) = &self.events {
                events.emit(Event::LeaderReady);
            }

            self.slot_stats.was_leader_ready = true;
        }
    }

    fn drain_worker_responses<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        for worker in 0..self.args.workers {
            bridge.worker_drain(
                worker,
                |bridge, WorkerResponse { meta, response, .. }| {
                    match response {
                        WorkerAction::Unprocessed => {
                            // Release locks if this was an execute request.
                            if self.executing_tx.remove(&meta.key) {
                                Self::unlock(&mut self.in_flight_locks, bridge, meta.key);
                                self.in_flight_cus -= meta.cost;

                                self.emit_tx_event(
                                    bridge,
                                    meta.key,
                                    meta.priority,
                                    TransactionAction::ExecuteUnprocessed,
                                );
                                self.metrics.execute_unprocessed.increment(1);
                                self.checked_tx.insert(meta);
                            }

                            TxDecision::Keep
                        }
                        WorkerAction::Check(rep, _) => self.on_check(bridge, meta, rep),
                        WorkerAction::Execute(rep) => self.on_execute(bridge, meta, rep),
                    }
                },
                usize::MAX,
            );
        }
    }

    fn drain_tpu<B>(&mut self, bridge: &mut B, max_count: usize)
    where
        B: Bridge<Meta = PriorityId>,
    {
        let additional = std::cmp::min(bridge.tpu_len(), max_count);
        let shortfall =
            (self.unchecked_tx.len() + additional).saturating_sub(self.args.unchecked_capacity);

        // NB: Technically we are evicting more than we need to because not all of
        // `additional` will parse correctly & thus have a priority.
        for _ in 0..shortfall {
            let id = self.unchecked_tx.pop_min().unwrap();
            self.emit_tx_event(
                bridge,
                id.key,
                id.priority,
                TransactionAction::Evict { reason: EvictReason::UncheckedCapacity },
            );
            bridge.tx_drop(id.key);
        }
        self.metrics.recv_tpu_evict.increment(shortfall as u64);

        // TODO: Need to dedupe already seen transactions?

        bridge.tpu_drain(
            |bridge, key| match Self::calculate_priority(bridge.runtime(), &bridge.tx(key).data) {
                Some((priority, cost)) => {
                    self.unchecked_tx.push(PriorityId { priority, cost, key });
                    self.emit_tx_event(
                        bridge,
                        key,
                        priority,
                        TransactionAction::Ingest { source: TransactionSource::Tpu, bundle: None },
                    );
                    self.metrics.recv_tpu_ok.increment(1);

                    TxDecision::Keep
                }
                None => {
                    self.metrics.recv_tpu_err.increment(1);

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
        let start_len = self.unchecked_tx.len();
        while bridge.worker(CHECK_WORKER).len() < MAX_CHECK_BATCHES
            && bridge.worker(CHECK_WORKER).rem() > 0
        {
            let pop_next = || {
                // Prioritize unchecked transactions.
                if let Some(id) = self.unchecked_tx.pop_max() {
                    return Some(KeyedTransactionMeta { key: id.key, meta: id });
                }

                // Re-check already checked transactions if we have remaining.
                while let Some(curr) = self.next_recheck.take() {
                    self.next_recheck = self
                        .checked_tx
                        .range((Bound::Unbounded, Bound::Excluded(curr)))
                        .next_back()
                        .copied();

                    // Skip if transaction was removed from checked_tx (e.g., scheduled for
                    // execution) or is currently executing.
                    if !self.checked_tx.contains(&curr) || self.executing_tx.contains(&curr.key) {
                        continue;
                    }

                    return Some(KeyedTransactionMeta { key: curr.key, meta: curr });
                }

                None
            };

            // Build the next batch.
            self.schedule_batch.clear();
            self.schedule_batch
                .extend(std::iter::from_fn(pop_next).take(64));

            // If we built an empty batch we are done.
            if self.schedule_batch.is_empty() {
                break;
            }

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
            .increment((start_len - self.unchecked_tx.len()) as u64);
    }

    fn schedule_execute<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        debug_assert_eq!(bridge.progress().leader_state, LEADER_READY);
        let budget_percentage =
            std::cmp::min(bridge.progress().current_slot_progress + BLOCK_FILL_CUTOFF, 100);
        // TODO: Would be ideal for the scheduler protocol to tell us the max block
        // units.
        let budget_limit = MAX_BLOCK_UNITS_SIMD_0256 * u64::from(budget_percentage) / 100;
        let cost_used = MAX_BLOCK_UNITS_SIMD_0256
            .saturating_sub(bridge.progress().remaining_cost_units)
            + self.in_flight_cus;
        let mut budget = budget_limit.saturating_sub(cost_used);
        for worker in EXECUTE_WORKER_START..bridge.worker_count() {
            // If we are packing too fast, slow down.
            if budget == 0 {
                break;
            }

            // If the worker already has a pending job, don't give it any more.
            if bridge.worker(worker).len() > 0 {
                continue;
            }

            // If nothing is checked we are done.
            if self.checked_tx.is_empty() {
                break;
            }

            // Schedule the next best TX.
            self.schedule_batch.clear();
            self.try_schedule_transaction(&mut budget, bridge, worker);

            // If we failed to schedule anything, don't send the batch.
            if self.schedule_batch.is_empty() {
                break;
            }

            // For each TX we need to:
            // - Add to executing_tx.
            // - Emit an event.
            for tx in &self.schedule_batch {
                assert!(self.executing_tx.insert(tx.key));
                self.emit_tx_event(bridge, tx.key, tx.meta.priority, TransactionAction::ExecuteReq);
            }

            // Update metrics.
            self.metrics
                .execute_requested
                .increment(self.schedule_batch.len() as u64);
        }
    }

    fn on_check<B>(&mut self, bridge: &mut B, meta: PriorityId, rep: CheckResponse) -> TxDecision
    where
        B: Bridge<Meta = PriorityId>,
    {
        // If transaction is currently executing (or deferred), ignore the recheck
        // result.
        if self.executing_tx.contains(&meta.key) || self.deferred_tx.contains(&meta) {
            return TxDecision::Keep;
        }

        let parsing_failed =
            rep.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED != 0;
        let resolve_failed = rep.resolve_flags & resolve_flags::FAILED != 0;
        let status_ok = status_check_flags::REQUESTED | status_check_flags::PERFORMED;
        let status_failed = rep.status_check_flags & !status_ok != 0;
        if parsing_failed || resolve_failed || status_failed {
            let reason = match (parsing_failed, resolve_failed, status_failed) {
                (true, false, false) => CheckFailure::ParseOrSanitize,
                (false, true, false) => CheckFailure::AccountResolution,
                (false, false, true) => CheckFailure::StatusCheck,
                _ => unreachable!(),
            };
            self.emit_tx_event(
                bridge,
                meta.key,
                meta.priority,
                TransactionAction::CheckErr { reason },
            );
            self.metrics.check_err.increment(1);

            // NB: If we are re-checking then we must remove here, else we can just silently
            // ignore the None returned by `remove()`.
            self.checked_tx.remove(&meta);

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

        // If already in checked_tx, this is a recheck completing - nothing to do.
        if self.checked_tx.contains(&meta) {
            self.metrics.check_ok.increment(1);

            return TxDecision::Keep;
        }

        // First check. Evict lowest priority if at capacity.
        if self.pending_len() >= self.args.checked_capacity {
            let id = self.checked_tx.pop_first().unwrap();
            self.emit_tx_event(
                bridge,
                id.key,
                id.priority,
                TransactionAction::Evict { reason: EvictReason::CheckedCapacity },
            );
            bridge.tx_drop(id.key);

            self.metrics.check_evict.increment(1);
        }

        // Insert the new transaction (yes this may be lower priority than what
        // we just evicted but that's fine).
        self.checked_tx.insert(meta);
        self.emit_tx_event(bridge, meta.key, meta.priority, TransactionAction::CheckOk);

        // Update ok metric.
        self.metrics.check_ok.increment(1);

        TxDecision::Keep
    }

    fn on_execute<B>(
        &mut self,
        bridge: &mut B,
        meta: PriorityId,
        rep: ExecutionResponse,
    ) -> TxDecision
    where
        B: Bridge<Meta = PriorityId>,
    {
        // Remove from executing set now that execution is complete.
        assert!(self.executing_tx.remove(&meta.key));

        // Remove in-flight costs.
        self.in_flight_cus -= meta.cost;

        // Remove in flight locks.
        Self::unlock(&mut self.in_flight_locks, bridge, meta.key);

        // Emit event and update metrics.
        let (action, metric) = match rep.not_included_reason {
            not_included_reasons::NONE => (TransactionAction::ExecuteOk, &self.metrics.execute_ok),
            reason => (
                TransactionAction::ExecuteErr { reason: u32::from(reason) },
                &self.metrics.execute_err,
            ),
        };
        self.emit_tx_event(bridge, meta.key, meta.priority, action);
        metric.increment(1);

        // If non retryable or a bundle, just drop immediately.
        let is_bundle = meta.priority == BUNDLE_MARKER;
        let is_retryable = Self::is_retryable(rep.not_included_reason);
        if is_bundle || !is_retryable {
            return TxDecision::Drop;
        }

        // If we attempted on this slot already, defer to next slot. Unless this was a
        // lock conflict, then we can immediately retry.
        match rep.execution_slot == self.slot
            && rep.not_included_reason != not_included_reasons::ACCOUNT_IN_USE
        {
            true => assert!(self.deferred_tx.insert(meta)),
            false => assert!(self.checked_tx.insert(meta)),
        }

        // Evict from checked_tx if over capacity.
        if self.pending_len() > self.args.checked_capacity
            && let Some(evicted) = self.checked_tx.pop_first()
        {
            self.emit_tx_event(
                bridge,
                evicted.key,
                evicted.priority,
                TransactionAction::Evict { reason: EvictReason::CheckedCapacity },
            );
            bridge.tx_drop(evicted.key);
            self.metrics.execute_evict.increment(1);
        }

        TxDecision::Keep
    }

    fn pending_len(&self) -> usize {
        self.checked_tx.len() + self.executing_tx.len() + self.deferred_tx.len()
    }

    const fn is_retryable(reason: u8) -> bool {
        // TODO: Enable
        // assert_ne!(reason, not_included_reasons::ACCOUNT_IN_USE);

        matches!(
            reason,
            not_included_reasons::ACCOUNT_IN_USE
                | not_included_reasons::BANK_NOT_AVAILABLE
                | not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT
                | not_included_reasons::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT
                | not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT
                | not_included_reasons::WOULD_EXCEED_MAX_VOTE_COST_LIMIT
                | not_included_reasons::WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT
        )
    }

    /// Trys to schedule a transaction.
    ///
    /// # Return
    ///
    /// Places scheduled transactions in `self.schedule_batch`.
    fn try_schedule_transaction<B>(&mut self, budget: &mut u64, bridge: &mut B, worker: usize)
    where
        B: Bridge<Meta = PriorityId>,
    {
        let tx = self.checked_tx.last().unwrap();

        // Check if this fits in the budget.
        if tx.cost > *budget {
            return;
        }

        // Check if this transaction's read/write locks conflict with any
        // pre-existing read/write locks.
        if !Self::can_lock(&self.in_flight_locks, bridge, tx.key) {
            return;
        }

        // Insert all the locks.
        Self::lock(&mut self.in_flight_locks, bridge, tx.key);

        // Build the 1TX batch.
        self.schedule_batch
            .push(KeyedTransactionMeta { key: tx.key, meta: *tx });

        // Schedule the batch.
        bridge.schedule(ScheduleBatch {
            worker,
            transactions: &self.schedule_batch,
            max_working_slot: bridge.progress().current_slot + 1,
            flags: pack_message_flags::EXECUTE,
        });

        // Update state.
        *budget -= tx.cost;
        self.in_flight_cus += tx.cost;
        self.checked_tx.pop_last().unwrap();
    }

    /// Checks a TX for lock conflicts without inserting locks.
    fn can_lock<B>(
        in_flight_locks: &HashMap<Pubkey, AccountLockers>,
        bridge: &mut B,
        tx_key: TransactionKey,
    ) -> bool
    where
        B: Bridge,
    {
        // Check if this transaction's read/write locks conflict with any
        // pre-existing read/write locks.
        bridge.tx(tx_key).locks().all(|(addr, writable)| {
            in_flight_locks
                .get(addr)
                .is_none_or(|lockers| lockers.can_lock(writable))
        })
    }

    /// Locks a transaction without checking for conflicts.
    fn lock<B>(
        in_flight_locks: &mut HashMap<Pubkey, AccountLockers>,
        bridge: &mut B,
        tx_key: TransactionKey,
    ) where
        B: Bridge,
    {
        for (addr, writable) in bridge.tx(tx_key).locks() {
            in_flight_locks
                .entry_ref(addr)
                .or_default()
                .insert(tx_key, writable);
        }
    }

    /// Unlocks a transaction, releasing all its locks.
    ///
    /// Panics if the transaction doesn't hold the expected locks.
    fn unlock<B>(
        in_flight_locks: &mut HashMap<Pubkey, AccountLockers>,
        bridge: &B,
        tx_key: TransactionKey,
    ) where
        B: Bridge,
    {
        for (addr, writable) in bridge.tx(tx_key).locks() {
            let EntryRef::Occupied(mut entry) = in_flight_locks.entry_ref(addr) else {
                panic!();
            };
            entry.get_mut().remove(tx_key, writable);
            if entry.get().is_empty() {
                entry.remove();
            }
        }
    }

    fn calculate_cost_and_reward(
        runtime: &RuntimeState,
        tx: &SanitizedTransactionView<TransactionPtr>,
    ) -> Option<(u64, u64)> {
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

        Some((cost, reward))
    }

    fn calculate_priority(
        runtime: &RuntimeState,
        tx: &SanitizedTransactionView<TransactionPtr>,
    ) -> Option<(u64, u64)> {
        let (cost, reward) = Self::calculate_cost_and_reward(runtime, tx)?;
        let priority = reward
            .saturating_mul(PRIORITY_MULTIPLIER)
            .saturating_div(cost.saturating_add(1));
        // NB: We use `u64::MAX` as sentinel value for bundles.
        let priority = core::cmp::min(priority, BUNDLE_MARKER - 1);

        Some((priority, cost))
    }

    fn emit_tx_event<B>(
        &self,
        bridge: &B,
        key: TransactionKey,
        priority: u64,
        action: TransactionAction,
    ) where
        B: Bridge,
    {
        let Some(events) = &self.events else { return };

        // Don't emit for vote TXs (save my disk/familia).
        let tx = bridge.tx(key);
        if tx.is_simple_vote() {
            return;
        }

        events.emit(Event::Transaction(TransactionEvent {
            signature: tx.data.signatures()[0],
            slot: self.slot,
            priority,
            action,
        }));
    }
}

struct BatchMetrics {
    current_slot: Gauge,
    next_leader_slot: Gauge,

    tpu_unchecked_len: Gauge,
    tpu_checked_len: Gauge,
    tpu_deferred_len: Gauge,
    locks_len: Gauge,
    executing_len: Gauge,

    in_flight_cus: Gauge,

    recv_tpu_ok: Counter,
    recv_tpu_err: Counter,
    recv_tpu_evict: Counter,

    check_requested: Counter,
    check_ok: Counter,
    check_err: Counter,
    check_evict: Counter,

    execute_requested: Counter,
    execute_ok: Counter,
    execute_err: Counter,
    execute_unprocessed: Counter,
    execute_evict: Counter,
}

impl BatchMetrics {
    fn new() -> Self {
        Self {
            current_slot: gauge!("slot", "label" => "current"),
            next_leader_slot: gauge!("slot", "label" => "next_leader"),

            tpu_unchecked_len: gauge!("container_len", "label" => "tpu_unchecked"),
            tpu_checked_len: gauge!("container_len", "label" => "tpu_checked"),
            tpu_deferred_len: gauge!("container_len", "label" => "tpu_deferred"),
            locks_len: gauge!("container_len", "label" => "locks"),
            executing_len: gauge!("container_len", "label" => "executing"),

            recv_tpu_ok: counter!("recv_tpu", "label" => "ok"),
            recv_tpu_err: counter!("recv_tpu", "label" => "err"),
            recv_tpu_evict: counter!("recv_tpu", "label" => "evict"),

            in_flight_cus: gauge!("in_flight_cus"),

            check_requested: counter!("check", "label" => "requested"),
            check_ok: counter!("check", "label" => "ok"),
            check_err: counter!("check", "label" => "err"),
            check_evict: counter!("check", "label" => "evict"),

            execute_requested: counter!("execute", "label" => "requested"),
            execute_ok: counter!("execute", "label" => "ok"),
            execute_err: counter!("execute", "label" => "err"),
            execute_unprocessed: counter!("execute", "label" => "unprocessed"),
            execute_evict: counter!("execute", "label" => "evict"),
        }
    }
}

#[derive(Debug, Default)]
struct AccountLockers {
    writers: HashSet<TransactionKey>,
    readers: HashSet<TransactionKey>,
}

impl AccountLockers {
    fn is_empty(&self) -> bool {
        self.writers.is_empty() && self.readers.is_empty()
    }

    fn can_lock(&self, writable: bool) -> bool {
        match writable {
            true => self.is_empty(),
            false => self.writers.is_empty(),
        }
    }

    fn insert(&mut self, tx_key: TransactionKey, writable: bool) {
        let set = match writable {
            true => &mut self.writers,
            false => &mut self.readers,
        };
        assert!(set.insert(tx_key));
    }

    fn remove(&mut self, tx_key: TransactionKey, writable: bool) {
        let set = match writable {
            true => &mut self.writers,
            false => &mut self.readers,
        };
        assert!(set.remove(&tx_key));
    }
}

#[cfg(test)]
mod tests {
    use agave_bridge::TestBridge;
    use agave_scheduler_bindings::worker_message_types::{
        CheckResponse, parsing_and_sanitization_flags, resolve_flags, status_check_flags,
    };
    use agave_scheduler_bindings::{NOT_LEADER, ProgressMessage, pack_message_flags};
    use solana_compute_budget_interface::ComputeBudgetInstruction;
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Signer};
    use solana_transaction::versioned::VersionedTransaction;
    use solana_transaction::{Instruction, Transaction};

    use super::*;

    //////////
    // Helpers

    const MOCK_PROGRESS: ProgressMessage = ProgressMessage {
        leader_state: NOT_LEADER,
        current_slot: 10,
        next_leader_slot: 11,
        leader_range_end: 11,
        remaining_cost_units: 50_000_000,
        current_slot_progress: 25,
    };

    fn test_scheduler() -> GreedyRevenueScheduler {
        GreedyRevenueScheduler::new(
            None,
            GreedyRevenueArgs { workers: 5, unchecked_capacity: 64, checked_capacity: 64 },
        )
    }

    fn noop_with_budget(payer: &Keypair, cu_limit: u32, cu_price: u64) -> VersionedTransaction {
        Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
                ComputeBudgetInstruction::set_compute_unit_price(cu_price),
            ],
            Some(&payer.pubkey()),
            &[payer],
            Hash::new_from_array([1; 32]),
        )
        .into()
    }

    type SetupExecuting = (
        GreedyRevenueScheduler,
        TestBridge<PriorityId>,
        ScheduleBatch<Vec<KeyedTransactionMeta<PriorityId>>>,
    );

    fn setup_executing_tx(cu_limit: u32, cu_price: u64) -> SetupExecuting {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Ingest a TX.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, cu_limit, cu_price);
        bridge.queue_tpu(&tx);

        // Poll - ingest & schedule checks.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Complete checks.
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 1);

        // Transition to leader.
        bridge.queue_progress(ProgressMessage { leader_state: LEADER_READY, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);

        // Pop the user TX execute batch.
        let exec_batch = bridge.pop_schedule().unwrap();
        assert_eq!(exec_batch.flags, pack_message_flags::EXECUTE);
        assert_eq!(exec_batch.transactions.len(), 1);
        assert_eq!(scheduler.checked_tx.len(), 0);
        assert!(
            scheduler
                .executing_tx
                .contains(&exec_batch.transactions[0].key)
        );

        (scheduler, bridge, exec_batch)
    }

    //////
    // TPU

    #[test]
    fn tpu_recv_schedules_check() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Ingest a transaction via TPU.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, 100);
        bridge.queue_tpu(&tx);

        // Poll the scheduler.
        scheduler.poll(&mut bridge);

        // Assert - A single check request was scheduled.
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(batch.flags & 1, pack_message_flags::CHECK);
        assert_eq!(batch.transactions.len(), 1);
        assert_eq!(bridge.pop_schedule(), None);
    }

    ///////////////////
    // Validation flow

    #[test]
    fn check_ok_moves_to_checked() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Ingest a transaction via TPU.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, 100);
        bridge.queue_tpu(&tx);

        // Poll - TX ingested into unchecked, CHECK batch scheduled.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.unchecked_tx.len(), 0); // drained to check worker
        let check_batch = bridge.pop_schedule().unwrap();
        assert_eq!(check_batch.flags & 1, pack_message_flags::CHECK);
        assert_eq!(check_batch.transactions.len(), 1);

        // Queue a successful check response.
        bridge.queue_check_response(&check_batch, 0, None);

        // Poll - check response drained, TX moves to checked.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 1);
        assert_eq!(bridge.tx_count(), 1);

        // Transition to leader - TX should be scheduled for execution.
        bridge.queue_progress(ProgressMessage { leader_state: LEADER_READY, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);

        // Next scheduled batch should be our checked TX.
        let exec_batch = bridge.pop_schedule().unwrap();
        assert_eq!(exec_batch.flags, pack_message_flags::EXECUTE);
        assert_eq!(exec_batch.transactions.len(), 1);
        assert_eq!(exec_batch.transactions[0].key, check_batch.transactions[0].key);

        // TX moved from checked to executing.
        assert_eq!(scheduler.checked_tx.len(), 0);
        assert!(
            scheduler
                .executing_tx
                .contains(&check_batch.transactions[0].key)
        );
    }

    #[test]
    fn check_err_drops_transaction() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Ingest three TXs so we can test each failure mode.
        let payers: Vec<_> = (0..3).map(|_| Keypair::new()).collect();
        for payer in &payers {
            let tx = noop_with_budget(payer, 25_000, 100);
            bridge.queue_tpu(&tx);
        }

        // Poll - all three are checked.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        let check_batch = bridge.pop_schedule().unwrap();
        assert_eq!(check_batch.transactions.len(), 3);

        // Queue failures: parse error, resolve error, status error.
        let parse_fail = CheckResponse {
            parsing_and_sanitization_flags: parsing_and_sanitization_flags::FAILED,
            ..bridge.check_ok()
        };
        let resolve_fail = CheckResponse {
            resolve_flags: resolve_flags::REQUESTED
                | resolve_flags::PERFORMED
                | resolve_flags::FAILED,
            ..bridge.check_ok()
        };
        let status_fail = CheckResponse {
            status_check_flags: status_check_flags::REQUESTED
                | status_check_flags::PERFORMED
                | status_check_flags::ALREADY_PROCESSED,
            ..bridge.check_ok()
        };
        bridge.queue_check_response_with(&check_batch, 0, None, parse_fail);
        bridge.queue_check_response_with(&check_batch, 1, None, resolve_fail);
        bridge.queue_check_response_with(&check_batch, 2, None, status_fail);

        // Poll.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Asset - all 3 are dropped.
        assert_eq!(scheduler.unchecked_tx.len(), 0);
        assert_eq!(scheduler.checked_tx.len(), 0);
        assert_eq!(bridge.tx_count(), 0);
    }

    #[test]
    fn recheck_during_leader_slot() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Zero remaining CUs so can't execute (allows us to just re-check).
        let leader_no_budget = ProgressMessage {
            leader_state: LEADER_READY,
            remaining_cost_units: 0,
            ..MOCK_PROGRESS
        };

        // TPU ingest.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, 100);
        bridge.queue_tpu(&tx);

        // Scheduler picks up TX from tpu queue.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        bridge.queue_all_checks_ok();

        // Scheduler picks up check result from worker queue.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Asser - TX moves to check.
        assert_eq!(scheduler.checked_tx.len(), 1);
        let checked_meta = *scheduler.checked_tx.last().unwrap();

        // First leader poll we become leader & next_recheck is set.
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);
        assert!(scheduler.checked_tx.contains(&checked_meta));
        while bridge.pop_schedule().is_some() {}

        // Second leader poll, we queue the recheck to the worker.
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);

        // Assert - the check should contain our checked TX (recheck).
        let recheck_batch = bridge.pop_schedule().unwrap();
        assert_eq!(recheck_batch.flags & 1, pack_message_flags::CHECK);
        assert!(
            recheck_batch
                .transactions
                .iter()
                .any(|t| t.key == checked_meta.key)
        );

        // Queue a successful recheck response.
        let idx = recheck_batch
            .transactions
            .iter()
            .position(|t| t.key == checked_meta.key)
            .unwrap();
        bridge.queue_check_response(&recheck_batch, idx, None);

        // Poll - recheck OK is a no-op; TX stays in checked.
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);
        assert!(scheduler.checked_tx.contains(&checked_meta));
        assert!(bridge.contains_tx(checked_meta.key));
    }

    #[test]
    fn recheck_failure_removes_from_checked() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Use zero remaining CUs so the TX stays in checked during recheck.
        let leader_no_budget = ProgressMessage {
            leader_state: LEADER_READY,
            remaining_cost_units: 0,
            ..MOCK_PROGRESS
        };

        // Poll - Ingest and check a TX so it lands in checked.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, 100);
        bridge.queue_tpu(&tx);
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Poll - Checks ok.
        bridge.queue_all_checks_ok();
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 1);
        let checked_meta = *scheduler.checked_tx.last().unwrap();

        // Poll - TX stays in checked (no budget), next_recheck set.
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);
        while bridge.pop_schedule().is_some() {} // Could be 1 batch in future.

        // Poll - schedule_checks fires the recheck.
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);

        // Assert - One batch is scheduled (recheck).
        let recheck_batch = bridge.pop_schedule().unwrap();
        assert_eq!(recheck_batch.flags & 1, pack_message_flags::CHECK);
        let idx = recheck_batch
            .transactions
            .iter()
            .position(|t| t.key == checked_meta.key)
            .unwrap();
        assert!(bridge.pop_schedule().is_none());

        // Poll - Recheck fails.
        let status_fail = CheckResponse {
            status_check_flags: status_check_flags::REQUESTED
                | status_check_flags::PERFORMED
                | status_check_flags::ALREADY_PROCESSED,
            ..bridge.check_ok()
        };
        bridge.queue_check_response_with(&recheck_batch, idx, None, status_fail);
        scheduler.poll(&mut bridge);

        // Assert - Checked TX dropped.
        assert!(!scheduler.checked_tx.contains(&checked_meta));
        assert!(!bridge.contains_tx(checked_meta.key));
    }

    ///////////////////
    // Execution flow

    #[test]
    fn execute_schedules_by_priority() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Ingest two TXs with different priorities.
        let payer_low = Keypair::new();
        let payer_high = Keypair::new();
        let tx_low = noop_with_budget(&payer_low, 25_000, 100);
        let tx_high = noop_with_budget(&payer_high, 25_000, 200);
        bridge.queue_tpu(&tx_low);
        bridge.queue_tpu(&tx_high);

        // Poll - ingest & schedule checks.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Poll - Complete checks successfully.
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 2);

        // Poll - Transition to leader.
        bridge.queue_progress(ProgressMessage { leader_state: LEADER_READY, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);

        // First execute batch should be the higher priority TX.
        let exec_high = bridge.pop_schedule().unwrap();
        assert_eq!(exec_high.flags, pack_message_flags::EXECUTE);
        assert_eq!(exec_high.transactions.len(), 1);

        // Second execute batch should be the lower priority TX.
        let exec_low = bridge.pop_schedule().unwrap();
        assert_eq!(exec_low.flags, pack_message_flags::EXECUTE);
        assert_eq!(exec_low.transactions.len(), 1);

        // Higher priority TX was scheduled first (higher meta.priority).
        assert!(exec_high.transactions[0].meta.priority > exec_low.transactions[0].meta.priority);

        // Both user TXs moved from checked to executing.
        assert_eq!(scheduler.checked_tx.len(), 0);
        assert!(
            scheduler
                .executing_tx
                .contains(&exec_high.transactions[0].key)
        );
        assert!(
            scheduler
                .executing_tx
                .contains(&exec_low.transactions[0].key)
        );
    }

    #[test]
    fn execute_respects_budget() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Ingest a TX.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, 100);
        bridge.queue_tpu(&tx);

        // Poll - ingest & schedule checks.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Poll - Complete checks successfully.
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 1);

        // Transition to leader with zero remaining CUs (no budget).
        bridge.queue_progress(ProgressMessage {
            leader_state: LEADER_READY,
            remaining_cost_units: 0,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);

        // Assert - No user TX execute batch scheduled (budget exhausted).
        assert_eq!(bridge.pop_schedule(), None);

        // Assert - User TX stays in checked (not moved to executing).
        assert_eq!(scheduler.checked_tx.len(), 1);
    }

    #[test]
    fn execute_respects_lock_conflicts() {
        use solana_pubkey::Pubkey;
        use solana_transaction::AccountMeta;

        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Create a shared writable account.
        let shared_account = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();

        // Build two TXs that both write to the shared account.
        let payer_a = Keypair::new();
        let tx_a: VersionedTransaction = Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(25_000),
                ComputeBudgetInstruction::set_compute_unit_price(200),
                Instruction {
                    program_id,
                    accounts: vec![AccountMeta::new(shared_account, false)],
                    data: vec![],
                },
            ],
            Some(&payer_a.pubkey()),
            &[&payer_a],
            Hash::new_from_array([1; 32]),
        )
        .into();

        let payer_b = Keypair::new();
        let tx_b: VersionedTransaction = Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(25_000),
                ComputeBudgetInstruction::set_compute_unit_price(100),
                Instruction {
                    program_id,
                    accounts: vec![AccountMeta::new(shared_account, false)],
                    data: vec![],
                },
            ],
            Some(&payer_b.pubkey()),
            &[&payer_b],
            Hash::new_from_array([1; 32]),
        )
        .into();

        bridge.queue_tpu(&tx_a);
        bridge.queue_tpu(&tx_b);

        // Poll - ingest & schedule checks.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Poll - Complete checks successfully.
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 2);

        // Poll - Transition to leader.
        bridge.queue_progress(ProgressMessage { leader_state: LEADER_READY, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);

        // Only one user TX should be scheduled (the other conflicts on shared_account).
        let exec = bridge.pop_schedule().unwrap();
        assert_eq!(exec.flags, pack_message_flags::EXECUTE);
        assert_eq!(exec.transactions.len(), 1);

        // No second execute batch (lock conflict blocks the second TX).
        assert_eq!(bridge.pop_schedule(), None);

        // One user TX executing, one still in checked.
        assert!(scheduler.executing_tx.contains(&exec.transactions[0].key));
        assert_eq!(scheduler.checked_tx.len(), 1);
    }

    #[test]
    fn execute_only_when_leader() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Ingest a TX.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, 100);
        bridge.queue_tpu(&tx);

        // Poll - ingest & schedule checks.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Poll - Complete checks successfully.
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 1);

        // Poll again as NOT_LEADER (MOCK_PROGRESS has leader_state = NOT_LEADER).
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Assert - Only check batches (rechecks), no execute batches.
        while let Some(batch) = bridge.pop_schedule() {
            assert_eq!(
                batch.flags & pack_message_flags::EXECUTE,
                0,
                "Expected no EXECUTE batches when not leader, got flags: {}",
                batch.flags,
            );
        }

        // Assert - TX stays in checked, nothing executing.
        assert_eq!(scheduler.checked_tx.len(), 1);
        assert_eq!(scheduler.executing_tx.len(), 0);
    }

    //////////////////////
    // Execution responses

    #[test]
    fn execute_ok_drops_transaction() {
        let (mut scheduler, mut bridge, exec_batch) = setup_executing_tx(25_000, 100);
        let tx_key = exec_batch.transactions[0].key;

        // Queue a successful execution response.
        bridge.queue_execute_response(&exec_batch, 0, bridge.execute_ok());

        // Poll to drain the response.
        bridge.queue_progress(ProgressMessage { leader_state: LEADER_READY, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);

        // TX dropped from bridge, removed from executing, not in checked or deferred.
        assert!(!bridge.contains_tx(tx_key));
        assert!(!scheduler.executing_tx.contains(&tx_key));
        assert_eq!(scheduler.checked_tx.len(), 0);
        assert_eq!(scheduler.deferred_tx.len(), 0);
    }

    #[test]
    fn execute_retryable_account_in_use_retries_same_slot() {
        let (mut scheduler, mut bridge, exec_batch) = setup_executing_tx(25_000, 100);
        let tx_key = exec_batch.transactions[0].key;

        // Queue ACCOUNT_IN_USE response (retryable, same slot).
        bridge.queue_execute_response(
            &exec_batch,
            0,
            bridge.execute_err(not_included_reasons::ACCOUNT_IN_USE),
        );

        // Poll as NOT_LEADER so schedule_execute doesn't immediately re-schedule.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // TX goes back to checked (immediate retry), not deferred.
        assert!(bridge.contains_tx(tx_key));
        assert!(!scheduler.executing_tx.contains(&tx_key));
        assert_eq!(scheduler.deferred_tx.len(), 0);
        assert!(scheduler.checked_tx.iter().any(|id| id.key == tx_key));
    }

    #[test]
    fn execute_retryable_other_defers_to_next_slot() {
        let (mut scheduler, mut bridge, exec_batch) = setup_executing_tx(25_000, 100);
        let tx_key = exec_batch.transactions[0].key;

        // Queue WOULD_EXCEED_MAX_BLOCK_COST_LIMIT response (retryable, same slot).
        bridge.queue_execute_response(
            &exec_batch,
            0,
            bridge.execute_err(not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT),
        );

        // Poll to drain the response.
        bridge.queue_progress(ProgressMessage { leader_state: LEADER_READY, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);

        // TX goes to deferred (not checked), will retry next slot.
        assert!(bridge.contains_tx(tx_key));
        assert!(!scheduler.executing_tx.contains(&tx_key));
        assert_eq!(scheduler.checked_tx.len(), 0);
        assert!(scheduler.deferred_tx.iter().any(|id| id.key == tx_key));
    }

    #[test]
    fn deferred_tx_drained_on_slot_roll() {
        let (mut scheduler, mut bridge, exec_batch) = setup_executing_tx(25_000, 100);
        let tx_key = exec_batch.transactions[0].key;

        // Queue a retryable error that defers the TX.
        bridge.queue_execute_response(
            &exec_batch,
            0,
            bridge.execute_err(not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT),
        );

        // Poll to drain the response - TX moves to deferred.
        bridge.queue_progress(ProgressMessage { leader_state: LEADER_READY, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);
        assert!(scheduler.deferred_tx.iter().any(|id| id.key == tx_key));

        // Roll to the next slot.
        bridge.queue_progress(ProgressMessage {
            current_slot: MOCK_PROGRESS.current_slot + 1,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);

        // Deferred TX drained back to checked.
        assert_eq!(scheduler.deferred_tx.len(), 0);
        assert!(scheduler.checked_tx.iter().any(|id| id.key == tx_key));
        assert!(bridge.contains_tx(tx_key));
    }

    #[test]
    fn execute_non_retryable_drops_transaction() {
        let (mut scheduler, mut bridge, exec_batch) = setup_executing_tx(25_000, 100);
        let tx_key = exec_batch.transactions[0].key;

        // Queue ALREADY_PROCESSED response (non-retryable).
        bridge.queue_execute_response(
            &exec_batch,
            0,
            bridge.execute_err(not_included_reasons::ALREADY_PROCESSED),
        );

        // Poll to drain the response.
        bridge.queue_progress(ProgressMessage { leader_state: LEADER_READY, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);

        // TX dropped entirely.
        assert!(!bridge.contains_tx(tx_key));
        assert!(!scheduler.executing_tx.contains(&tx_key));
        assert_eq!(scheduler.checked_tx.len(), 0);
        assert_eq!(scheduler.deferred_tx.len(), 0);
    }

    #[test]
    fn unprocessed_execute_returns_to_checked() {
        let (mut scheduler, mut bridge, exec_batch) = setup_executing_tx(25_000, 100);
        let tx_key = exec_batch.transactions[0].key;

        // Queue an unprocessed response.
        bridge.queue_unprocessed_response(&exec_batch, 0);

        // Poll as NOT_LEADER so schedule_execute doesn't immediately re-schedule.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // TX returns to checked, not dropped.
        assert!(bridge.contains_tx(tx_key));
        assert!(!scheduler.executing_tx.contains(&tx_key));
        assert!(scheduler.checked_tx.iter().any(|id| id.key == tx_key));
        assert_eq!(scheduler.deferred_tx.len(), 0);
    }

    ////////////////////////////
    // Slot/leader transitions

    #[test]
    fn slot_roll_clears_deferred_to_checked() {
        let (mut scheduler, mut bridge, exec_batch) = setup_executing_tx(25_000, 100);
        let tx_key = exec_batch.transactions[0].key;

        // Queue a retryable error that defers the TX.
        bridge.queue_execute_response(
            &exec_batch,
            0,
            bridge.execute_err(not_included_reasons::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT),
        );

        // Poll - TX moves to deferred.
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert!(scheduler.deferred_tx.iter().any(|id| id.key == tx_key));
        assert_eq!(scheduler.checked_tx.len(), 0);

        // Roll to next slot.
        bridge.queue_progress(ProgressMessage {
            current_slot: MOCK_PROGRESS.current_slot + 1,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);

        // Deferred TX drained back to checked.
        assert_eq!(scheduler.deferred_tx.len(), 0);
        assert!(
            scheduler.checked_tx.iter().any(|id| id.key == tx_key),
            "Deferred TX should move to checked_tx on slot roll",
        );
        assert!(bridge.contains_tx(tx_key));
    }

    #[test]
    fn slot_roll_resets_recheck_cursor() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Zero remaining CUs so TX stays in checked (no execution budget).
        let leader_no_budget = ProgressMessage {
            leader_state: LEADER_READY,
            remaining_cost_units: 0,
            ..MOCK_PROGRESS
        };

        // Ingest & check a TX.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, 100);
        bridge.queue_tpu(&tx);
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 1);
        let checked_meta = *scheduler.checked_tx.last().unwrap();

        // First leader poll - become_receiver fires, next_recheck set.
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);
        while bridge.pop_schedule().is_some() {}

        // Second leader poll - recheck is scheduled.
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);
        let recheck_batch = bridge.pop_schedule().unwrap();
        assert_eq!(recheck_batch.flags & 1, pack_message_flags::CHECK);
        assert!(
            recheck_batch
                .transactions
                .iter()
                .any(|t| t.key == checked_meta.key),
        );
        bridge.queue_check_response(&recheck_batch, 0, None);

        // Third leader poll - recheck response drained, cursor exhausted (only 1 TX).
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);
        assert!(scheduler.next_recheck.is_some()); // Re-initialized by poll.

        // Exhaust remaining rechecks so cursor is fully consumed.
        while let Some(batch) = bridge.pop_schedule() {
            if batch.flags & 1 == pack_message_flags::CHECK {
                for i in 0..batch.transactions.len() {
                    bridge.queue_check_response(&batch, i, None);
                }
            }
        }
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);
        // After exhausting all rechecks, cursor should be None.
        while let Some(batch) = bridge.pop_schedule() {
            if batch.flags & 1 == pack_message_flags::CHECK {
                for i in 0..batch.transactions.len() {
                    bridge.queue_check_response(&batch, i, None);
                }
            }
        }
        bridge.queue_progress(leader_no_budget);
        scheduler.poll(&mut bridge);
        while bridge.pop_schedule().is_some() {}

        // Now roll to a new slot - cursor should reset.
        let new_slot = MOCK_PROGRESS.current_slot + 1;
        bridge.queue_progress(ProgressMessage {
            leader_state: LEADER_READY,
            current_slot: new_slot,
            remaining_cost_units: 0,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);
        while bridge.pop_schedule().is_some() {} // Drain any become_receiver batches.

        // Poll again - recheck should be scheduled (cursor was reset by slot roll).
        bridge.queue_progress(ProgressMessage {
            leader_state: LEADER_READY,
            current_slot: new_slot,
            remaining_cost_units: 0,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);

        // Assert - a check batch containing our TX is scheduled (recheck restarted).
        let mut found_recheck = false;
        while let Some(batch) = bridge.pop_schedule() {
            if batch.flags & 1 == pack_message_flags::CHECK
                && batch.transactions.iter().any(|t| t.key == checked_meta.key)
            {
                found_recheck = true;
            }
        }
        assert!(found_recheck, "Recheck should restart after slot roll");
        assert!(scheduler.checked_tx.contains(&checked_meta));
    }

    //////////////
    // Edge cases

    #[test]
    fn tpu_recv_evicts_lowest_priority() {
        let mut scheduler = test_scheduler();
        // Worker capacity 0 prevents schedule_checks from draining unchecked_tx.
        let mut bridge = TestBridge::new(5, 0);

        // Fill unchecked_tx to capacity (64) with TXs of increasing priority.
        // Use large cu_price values to ensure distinct priorities (avoid integer
        // truncation in fee calculation).
        let payers: Vec<Keypair> = (0..64).map(|_| Keypair::new()).collect();
        for (i, payer) in payers.iter().enumerate() {
            let cu_price = ((i + 1) as u64) * 1_000;
            let tx = noop_with_budget(payer, 25_000, cu_price);
            bridge.queue_tpu(&tx);
        }
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.unchecked_tx.len(), 64);

        // Remember the lowest priority TX's key.
        let lowest = *scheduler.unchecked_tx.peek_min().unwrap();

        // Ingest one more TX with higher priority than the lowest.
        let new_payer = Keypair::new();
        let new_tx = noop_with_budget(&new_payer, 25_000, 100_000);
        bridge.queue_tpu(&new_tx);
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Assert - still at capacity (the lowest was evicted, new one added).
        assert_eq!(scheduler.unchecked_tx.len(), 64);

        // Assert - the old lowest priority TX was dropped from the bridge.
        assert!(!bridge.contains_tx(lowest.key));

        // Assert - new minimum has higher priority than the evicted TX.
        let new_min = scheduler.unchecked_tx.peek_min().unwrap();
        assert!(new_min.priority > lowest.priority);
    }

    #[test]
    fn checked_capacity_eviction() {
        let mut scheduler = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Fill checked_tx to capacity (64) by ingesting and checking 64 TXs.
        // Use large cu_price values to ensure distinct priorities.
        let payers: Vec<Keypair> = (0..64).map(|_| Keypair::new()).collect();
        for (i, payer) in payers.iter().enumerate() {
            let cu_price = ((i + 1) as u64) * 1_000;
            let tx = noop_with_budget(payer, 25_000, cu_price);
            bridge.queue_tpu(&tx);
        }
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Complete all checks.
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);
        assert_eq!(scheduler.checked_tx.len(), 64);
        assert_eq!(scheduler.unchecked_tx.len(), 0);

        // Remember the lowest priority checked TX.
        let lowest = *scheduler.checked_tx.first().unwrap();

        // Ingest one more TX and complete its check → should evict lowest checked.
        let new_payer = Keypair::new();
        let new_tx = noop_with_budget(&new_payer, 25_000, 100_000);
        bridge.queue_tpu(&new_tx);
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Complete the new TX's check.
        bridge.queue_all_checks_ok();
        bridge.queue_progress(MOCK_PROGRESS);
        scheduler.poll(&mut bridge);

        // Assert - checked_tx is still at capacity (lowest was evicted, new one
        // inserted).
        assert_eq!(scheduler.checked_tx.len(), 64);

        // Assert - the old lowest priority TX was evicted and dropped from bridge.
        assert!(!scheduler.checked_tx.contains(&lowest));
        assert!(!bridge.contains_tx(lowest.key));

        // Assert - new minimum has higher priority than the evicted TX.
        let new_min = scheduler.checked_tx.first().unwrap();
        assert!(new_min.priority > lowest.priority);
    }
}
