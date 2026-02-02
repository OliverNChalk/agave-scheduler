use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use agave_bridge::{
    Bridge, KeyedTransactionMeta, RuntimeState, ScheduleBatch, TransactionKey, TransactionState,
    TxDecision, Worker, WorkerAction, WorkerResponse,
};
use agave_scheduler_bindings::pack_message_flags::{check_flags, execution_flags};
use agave_scheduler_bindings::worker_message_types::{
    CheckResponse, ExecutionResponse, fee_payer_balance_flags, not_included_reasons,
    parsing_and_sanitization_flags, resolve_flags, status_check_flags,
};
use agave_scheduler_bindings::{
    LEADER_READY, MAX_TRANSACTIONS_PER_MESSAGE, SharableTransactionRegion, pack_message_flags,
};
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use agave_transaction_view::transaction_view::SanitizedTransactionView;
use crossbeam_channel::TryRecvError;
use hashbrown::hash_map::EntryRef;
use hashbrown::{HashMap, HashSet};
use indexmap::IndexSet;
use metrics::{Counter, Gauge, counter, gauge};
use min_max_heap::MinMaxHeap;
use solana_clock::{DEFAULT_SLOTS_PER_EPOCH, Slot};
use solana_compute_budget_instruction::compute_budget_instruction_details;
use solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS_SIMD_0256;
use solana_cost_model::cost_model::CostModel;
use solana_fee_structure::FeeBudgetLimits;
use solana_hash::Hash;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_svm_transaction::svm_message::SVMStaticMessage;
use solana_transaction::sanitized::MessageHash;
use toolbox::shutdown::Shutdown;
use tracing::{info, warn};

use crate::batch::jito_thread::{BuilderConfig, JitoArgs, JitoThread, JitoUpdate, TipConfig};
use crate::batch::tip_program::{
    ChangeTipReceiverArgs, TIP_ACCOUNTS, TIP_PAYMENT_PROGRAM, TipDistributionArgs,
    change_tip_receiver, init_tip_distribution,
};
use crate::events::{
    CheckFailure, Event, EventEmitter, EvictReason, SlotStatsEvent, TransactionAction,
    TransactionEvent, TransactionSource,
};
use crate::shared::PriorityId;

const PRIORITY_MULTIPLIER: u64 = 1_000_000;
const BUNDLE_MARKER: u64 = u64::MAX;

const UNCHECKED_CAPACITY: usize = 64 * 1024;
const CHECKED_CAPACITY: usize = 64 * 1024;
const BUNDLE_CAPACITY: usize = 1024;

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
const BUNDLE_EXPIRY: Duration = Duration::from_millis(200);

#[derive(Debug)]
pub struct BatchSchedulerArgs {
    pub tip: TipDistributionArgs,
    pub jito: JitoArgs,
    pub keypair: &'static Keypair,
}

pub struct BatchScheduler {
    shutdown: Shutdown,
    jito_rx: crossbeam_channel::Receiver<JitoUpdate>,
    tip_distribution_config: TipDistributionArgs,
    keypair: &'static Keypair,

    builder_config: BuilderConfig,
    tip_config: Option<TipConfig>,
    recent_blockhash: Hash,
    bundles: BTreeSet<BundleId>,
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

impl BatchScheduler {
    #[must_use]
    pub fn new(
        shutdown: Shutdown,
        events: Option<EventEmitter>,
        args: BatchSchedulerArgs,
    ) -> (Self, JoinHandle<()>) {
        let (jito_tx, jito_rx) = crossbeam_channel::bounded(1024);
        let jito_thread =
            JitoThread::spawn(shutdown.clone(), jito_tx, args.jito.clone(), args.keypair);

        (Self::new_with_jito(shutdown, events, args, jito_rx), jito_thread)
    }

    #[must_use]
    fn new_with_jito(
        shutdown: Shutdown,
        events: Option<EventEmitter>,
        BatchSchedulerArgs { tip, jito: _, keypair }: BatchSchedulerArgs,
        jito_rx: crossbeam_channel::Receiver<JitoUpdate>,
    ) -> Self {
        let JitoUpdate::BuilderConfig(builder_config) =
            jito_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        else {
            panic!();
        };

        Self {
            shutdown,
            jito_rx,
            tip_distribution_config: tip,
            keypair,

            builder_config,
            tip_config: None,
            recent_blockhash: Hash::default(),
            bundles: BTreeSet::new(),
            unchecked_tx: MinMaxHeap::with_capacity(UNCHECKED_CAPACITY),
            checked_tx: BTreeSet::new(),
            executing_tx: HashSet::with_capacity(CHECKED_CAPACITY),
            deferred_tx: IndexSet::with_capacity(CHECKED_CAPACITY),
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

        // Drop expired bundles.
        self.drop_expired_bundles(bridge);

        // Drain pending jito messages.
        self.drain_jito(bridge);

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
        self.metrics.bundles_len.set(self.bundles.len() as f64);
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
            self.become_tip_receiver(bridge);
        }
    }

    fn become_tip_receiver<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        info!("Becoming tip receiver");

        let (tip_distribution_key, init_tip_distribution) = init_tip_distribution(
            self.keypair,
            self.tip_distribution_config,
            self.slot / DEFAULT_SLOTS_PER_EPOCH,
            self.recent_blockhash,
        );
        let init_tip_distribution = bridge.tx_insert(&init_tip_distribution).unwrap();

        let tip_config = self.tip_config.as_ref().unwrap();
        let change_tip_receiver = change_tip_receiver(
            self.keypair,
            ChangeTipReceiverArgs {
                old_tip_receiver: tip_config.tip_receiver,
                new_tip_receiver: tip_distribution_key,
                old_block_builder: tip_config.block_builder,
                new_block_builder: self.builder_config.key,
                block_builder_commission: self.builder_config.commission,
            },
            self.recent_blockhash,
        );
        let change_tip_receiver = bridge.tx_insert(&change_tip_receiver).unwrap();

        // Check if our batch can be locked.
        if !Self::can_lock(&self.in_flight_locks, bridge, init_tip_distribution)
            || !Self::can_lock(&self.in_flight_locks, bridge, change_tip_receiver)
        {
            warn!("Failed to grab locks for change tip receiver");
            bridge.tx_drop(init_tip_distribution);
            bridge.tx_drop(change_tip_receiver);

            return;
        }

        // Lock our batch (Self::lock allows us to create overlapping write locks).
        Self::lock(&mut self.in_flight_locks, bridge, init_tip_distribution);
        Self::lock(&mut self.in_flight_locks, bridge, change_tip_receiver);

        // Set these transactions as executing.
        assert!(self.executing_tx.insert(init_tip_distribution));
        assert!(self.executing_tx.insert(change_tip_receiver));

        // TODO: Schedule as a single batch once we have SIMD83 live.
        bridge.schedule(ScheduleBatch {
            worker: EXECUTE_WORKER_START,
            transactions: &[KeyedTransactionMeta {
                key: init_tip_distribution,
                meta: PriorityId { priority: BUNDLE_MARKER, cost: 0, key: init_tip_distribution },
            }],
            max_working_slot: self.slot + 4,
            flags: pack_message_flags::EXECUTE | execution_flags::DROP_ON_FAILURE,
        });
        bridge.schedule(ScheduleBatch {
            worker: EXECUTE_WORKER_START,
            transactions: &[KeyedTransactionMeta {
                key: change_tip_receiver,
                meta: PriorityId { priority: BUNDLE_MARKER, cost: 0, key: change_tip_receiver },
            }],
            max_working_slot: self.slot + 4,
            flags: pack_message_flags::EXECUTE | execution_flags::DROP_ON_FAILURE,
        });
    }

    fn drain_worker_responses<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        for worker in 0..5 {
            while bridge.pop_worker(worker, |bridge, WorkerResponse { meta, response, .. }| {
                match response {
                    WorkerAction::Unprocessed => {
                        // Release locks if this was an execute request.
                        if self.executing_tx.remove(&meta.key) {
                            Self::unlock(&mut self.in_flight_locks, bridge, meta.key);
                            self.in_flight_cus -= meta.cost;

                            // TODO: What is the most appropriate event for a bundle unprocessed.
                            if meta.priority == BUNDLE_MARKER {
                                return TxDecision::Drop;
                            }

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
            }) {}
        }
    }

    fn drain_tpu<B>(&mut self, bridge: &mut B, max_count: usize)
    where
        B: Bridge<Meta = PriorityId>,
    {
        let additional = std::cmp::min(bridge.tpu_len(), max_count);
        let shortfall = (self.unchecked_tx.len() + additional).saturating_sub(UNCHECKED_CAPACITY);

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
                    // Ban using the tip payment program as it could be used to steal tips.
                    if Self::should_filter(bridge.tx(key)) {
                        self.metrics.recv_tpu_filtered.increment(1);

                        return TxDecision::Drop;
                    }

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

    fn drain_jito<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        loop {
            match self.jito_rx.try_recv() {
                Ok(JitoUpdate::BuilderConfig { .. }) => {}
                Ok(JitoUpdate::TipConfig(config)) => self.tip_config = Some(config),
                Ok(JitoUpdate::RecentBlockhash(hash)) => self.recent_blockhash = hash,
                Ok(JitoUpdate::Packet(packet)) => self.on_packet(bridge, &packet),
                Ok(JitoUpdate::Bundle(bundle)) => self.on_bundle(bridge, bundle),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => assert!(self.shutdown.is_shutdown()),
            }
        }
    }

    fn on_packet<B>(&mut self, bridge: &mut B, packet: &[u8])
    where
        B: Bridge<Meta = PriorityId>,
    {
        let Ok(key) = bridge.tx_insert(packet) else {
            return;
        };

        match Self::calculate_priority(bridge.runtime(), &bridge.tx(key).data) {
            Some((priority, cost)) => {
                // Ban using the tip payment program as it could be used to steal tips.
                if Self::should_filter(bridge.tx(key)) {
                    self.metrics.recv_packet_filtered.increment(1);
                    bridge.tx_drop(key);

                    return;
                }

                // Evict lowest if we're at capacity.
                if self.unchecked_tx.len() == UNCHECKED_CAPACITY {
                    let id = self.unchecked_tx.pop_min().unwrap();
                    self.emit_tx_event(
                        bridge,
                        id.key,
                        id.priority,
                        TransactionAction::Evict { reason: EvictReason::UncheckedCapacity },
                    );
                    bridge.tx_drop(id.key);
                    self.metrics.recv_packet_evict.increment(1);
                }

                // Store the new packet.
                self.unchecked_tx.push(PriorityId { priority, cost, key });
                self.emit_tx_event(
                    bridge,
                    key,
                    priority,
                    TransactionAction::Ingest { source: TransactionSource::Jito, bundle: None },
                );
                self.metrics.recv_packet_ok.increment(1);
            }
            None => {
                self.metrics.recv_packet_err.increment(1);

                bridge.tx_drop(key);
            }
        }
    }

    fn on_bundle<B>(&mut self, bridge: &mut B, bundle: Vec<Vec<u8>>)
    where
        B: Bridge<Meta = PriorityId>,
    {
        let mut keys = Vec::with_capacity(bundle.len());
        let mut total_cost: u64 = 0;
        let mut total_reward: u64 = 0;

        for packet in bundle {
            let Ok(key) = bridge.tx_insert(&packet) else {
                for key in keys {
                    bridge.tx_drop(key);
                }
                self.metrics.recv_bundle_err.increment(1);

                return;
            };

            // Add to our bundle keys.
            keys.push(key);

            // Calculate cost and reward for this transaction.
            let Some((cost, reward)) =
                Self::calculate_cost_and_reward(bridge.runtime(), &bridge.tx(key).data)
            else {
                for key in keys {
                    bridge.tx_drop(key);
                }
                self.metrics.recv_bundle_err.increment(1);

                return;
            };

            // Extract tip from this transaction.
            let tip = Self::extract_tip(&bridge.tx(key).data);

            // Accumulate totals.
            total_cost += cost;
            total_reward += reward + tip;
        }

        // Filter bundles containing transactions that write to tip accounts.
        if keys.iter().any(|key| Self::should_filter(bridge.tx(*key))) {
            self.metrics.recv_bundle_filtered.increment(1);
            for key in keys {
                bridge.tx_drop(key);
            }

            return;
        }

        // Calculate bundle priority.
        let priority = total_reward
            .saturating_mul(PRIORITY_MULTIPLIER)
            .checked_div(std::cmp::max(total_cost, 1))
            .unwrap_or(0);

        // Emit ingest events for bundle transactions.
        let bundle_sig = bridge.tx(keys[0]).data.signatures()[0];
        let bundle_id = Arc::new(bundle_sig.to_string());
        for &key in &keys {
            self.emit_tx_event(
                bridge,
                key,
                priority,
                TransactionAction::Ingest {
                    source: TransactionSource::Jito,
                    bundle: Some(bundle_id.clone()),
                },
            );
        }

        // Evict lowest priority bundle if at capacity.
        if self.bundles.len() == BUNDLE_CAPACITY {
            let evicted = self.bundles.pop_first().unwrap();
            for key in evicted.keys {
                bridge.tx_drop(key);
            }
            self.metrics.recv_bundle_evict.increment(1);
        }

        self.metrics.recv_bundle_ok.increment(1);
        // TODO: If Jito sends us a transaction (not a bundle) with overlapping
        // read/write keys we will panic as normally CHECK prevents this.
        self.bundles.insert(BundleId {
            priority,
            cost: total_cost,
            received_at: Instant::now(),
            keys,
        });
    }

    fn drop_expired_bundles<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        let now = Instant::now();
        // Retain only non-expired bundles, dropping expired ones.
        let expired: Vec<_> = self
            .bundles
            .iter()
            .filter(|b| now.duration_since(b.received_at) > BUNDLE_EXPIRY)
            .cloned()
            // TODO: Need an ExtractIf to avoid this BS alloc.
            .collect();

        for bundle in expired {
            self.bundles.remove(&bundle);
            self.metrics.recv_bundle_expired.increment(1);
            for key in bundle.keys {
                bridge.tx_drop(key);
            }
        }
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

            // Find the best tx & bundle, if both are empty we're done.
            let tx = self.checked_tx.last().map(|tx| tx.priority);
            let bundle = self.bundles.last().map(|bundle| bundle.priority);

            // Pick & schedule the best.
            self.schedule_batch.clear();
            match (tx, bundle) {
                (Some(tx), Some(bundle)) => match tx.cmp(&bundle) {
                    Ordering::Greater | Ordering::Equal => {
                        self.try_schedule_transaction(&mut budget, bridge, worker);
                    }
                    Ordering::Less => self.try_schedule_bundle(&mut budget, bridge, worker),
                },
                (Some(_), None) => self.try_schedule_transaction(&mut budget, bridge, worker),
                (None, Some(_)) => self.try_schedule_bundle(&mut budget, bridge, worker),
                (None, None) => break,
            }

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
        if self.pending_len() >= CHECKED_CAPACITY {
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
        if self.pending_len() > CHECKED_CAPACITY
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

    /// Trys to schedule a bundle.
    ///
    /// # Return
    ///
    /// Places scheduled transactions in `self.schedule_batch`.
    fn try_schedule_bundle<B>(&mut self, budget: &mut u64, bridge: &mut B, worker: usize)
    where
        B: Bridge<Meta = PriorityId>,
    {
        let bundle = self.bundles.last().unwrap();

        // Check this fits in budget.
        if bundle.cost > *budget {
            return;
        }

        // See if the bundle can be scheduled without conflicts.
        if !bundle
            .keys
            .iter()
            .all(|tx_key| Self::can_lock(&self.in_flight_locks, bridge, *tx_key))
        {
            return;
        }

        // Take all the locks & declare the TXs as executing.
        for tx_key in &bundle.keys {
            Self::lock(&mut self.in_flight_locks, bridge, *tx_key);
        }

        // Build the 1 bundle batch.
        self.schedule_batch
            .extend(
                bundle
                    .keys
                    .iter()
                    .enumerate()
                    .map(|(i, key)| KeyedTransactionMeta {
                        key: *key,
                        meta: PriorityId {
                            // TODO: This is a hacky way to identify bundles.
                            priority: BUNDLE_MARKER,
                            cost: match i {
                                0 => bundle.cost,
                                1.. => 0,
                            },
                            key: *key,
                        },
                    }),
            );

        // Schedule 1 bundle as 1 batch.
        bridge.schedule(ScheduleBatch {
            worker,
            transactions: &self.schedule_batch,
            max_working_slot: bridge.progress().current_slot + 1,
            flags: pack_message_flags::EXECUTE
                | execution_flags::DROP_ON_FAILURE
                | execution_flags::ALL_OR_NOTHING,
        });

        // Update state.
        *budget -= bundle.cost;
        self.in_flight_cus += bundle.cost;
        self.bundles.pop_last().unwrap();
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

    fn should_filter(tx: &TransactionState) -> bool {
        tx.write_locks()
            .chain(tx.read_locks())
            .any(|lock| lock == &TIP_PAYMENT_PROGRAM)
    }

    fn extract_tip(tx: &SanitizedTransactionView<TransactionPtr>) -> u64 {
        let account_keys = tx.static_account_keys();

        tx.program_instructions_iter()
            .filter_map(|(program_id, ix)| {
                // Check for system program transfer (discriminator = 2).
                if program_id != &solana_sdk_ids::system_program::ID
                    || ix.data.len() < 12
                    || u32::from_le_bytes(*arrayref::array_ref![ix.data, 0, 4]) != 2
                {
                    return None;
                }

                let dest_idx = *ix.accounts.get(1)? as usize;
                let dest = account_keys.get(dest_idx)?;
                let amount = u64::from_le_bytes(*arrayref::array_ref![ix.data, 4, 8]);

                TIP_ACCOUNTS.contains(dest).then_some(amount)
            })
            .sum()
    }
}

struct BatchMetrics {
    current_slot: Gauge,
    next_leader_slot: Gauge,

    tpu_unchecked_len: Gauge,
    tpu_checked_len: Gauge,
    tpu_deferred_len: Gauge,
    bundles_len: Gauge,
    locks_len: Gauge,
    executing_len: Gauge,

    in_flight_cus: Gauge,

    recv_tpu_ok: Counter,
    recv_tpu_err: Counter,
    recv_tpu_evict: Counter,
    recv_tpu_filtered: Counter,

    recv_packet_ok: Counter,
    recv_packet_err: Counter,
    recv_packet_evict: Counter,
    recv_packet_filtered: Counter,

    recv_bundle_ok: Counter,
    recv_bundle_err: Counter,
    recv_bundle_filtered: Counter,
    recv_bundle_expired: Counter,
    recv_bundle_evict: Counter,

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
            bundles_len: gauge!("container_len", "label" => "bundles"),
            locks_len: gauge!("container_len", "label" => "locks"),
            executing_len: gauge!("container_len", "label" => "executing"),

            recv_tpu_ok: counter!("recv_tpu", "label" => "ok"),
            recv_tpu_err: counter!("recv_tpu", "label" => "err"),
            recv_tpu_evict: counter!("recv_tpu", "label" => "evict"),
            recv_tpu_filtered: counter!("recv_tpu", "label" => "filtered"),

            recv_packet_ok: counter!("recv_packet", "label" => "ok"),
            recv_packet_err: counter!("recv_packet", "label" => "err"),
            recv_packet_evict: counter!("recv_packet", "label" => "evict"),
            recv_packet_filtered: counter!("recv_packet", "label" => "filtered"),

            recv_bundle_ok: counter!("recv_bundle", "label" => "ok"),
            recv_bundle_err: counter!("recv_bundle", "label" => "err"),
            recv_bundle_filtered: counter!("recv_bundle", "label" => "filtered"),
            recv_bundle_expired: counter!("recv_bundle", "label" => "expired"),
            recv_bundle_evict: counter!("recv_bundle", "label" => "evict"),

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

// TODO: Consider custom Ord to prioritize fifo as priority tie breaker?
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct BundleId {
    priority: u64,
    cost: u64,
    received_at: Instant,
    keys: Vec<TransactionKey>,
}

#[cfg(test)]
mod tests {
    use agave_bridge::TestBridge;
    use agave_scheduler_bindings::{NOT_LEADER, ProgressMessage, pack_message_flags};
    use solana_compute_budget_interface::ComputeBudgetInstruction;
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Signer};
    use solana_pubkey::Pubkey;
    use solana_transaction::versioned::VersionedTransaction;
    use solana_transaction::{Instruction, Transaction};
    use toolbox::shutdown::Shutdown;

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

    fn test_scheduler() -> (BatchScheduler, crossbeam_channel::Sender<JitoUpdate>) {
        let (jito_tx, jito_rx) = crossbeam_channel::bounded(1024);

        // Scheduler blocks until we give it an initial builder config.
        jito_tx
            .send(JitoUpdate::BuilderConfig(BuilderConfig {
                key: Pubkey::new_unique(),
                commission: 0,
            }))
            .unwrap();

        let args = BatchSchedulerArgs {
            tip: TipDistributionArgs {
                vote_account: Pubkey::new_unique(),
                merkle_authority: Pubkey::new_unique(),
                commission_bps: 0,
            },
            jito: JitoArgs {
                http_rpc: String::new(),
                ws_rpc: String::new(),
                block_engine: String::new(),
            },
            keypair: Box::leak(Box::new(Keypair::new())),
        };
        let scheduler = BatchScheduler::new_with_jito(Shutdown::new(), None, args, jito_rx);

        (scheduler, jito_tx)
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

    ////////////////////////
    // Misc

    #[test]
    fn leader_ready_triggers_become_receiver() {
        let (mut scheduler, jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Initial jito config & slot status indicating leader not ready.
        jito_tx
            .send(JitoUpdate::TipConfig(TipConfig {
                tip_receiver: Pubkey::new_unique(),
                block_builder: Pubkey::new_unique(),
            }))
            .unwrap();
        bridge.queue_progress(ProgressMessage { current_slot: 1, ..MOCK_PROGRESS });
        scheduler.poll(&mut bridge);
        assert_eq!(bridge.pop_schedule(), None);

        // Transition to leader & poll.
        bridge.queue_progress(ProgressMessage {
            leader_state: LEADER_READY,
            current_slot: 1,
            ..MOCK_PROGRESS
        });
        scheduler.poll(&mut bridge);

        // Assert - our become receiver batches scheduled.
        let expected_flags = pack_message_flags::EXECUTE | execution_flags::DROP_ON_FAILURE;

        let batch0 = bridge.pop_schedule().unwrap();
        assert_eq!(batch0.flags, expected_flags);
        assert_eq!(batch0.transactions.len(), 1);
        assert_eq!(batch0.worker, EXECUTE_WORKER_START);

        let batch1 = bridge.pop_schedule().unwrap();
        assert_eq!(batch1.flags, expected_flags);
        assert_eq!(batch1.transactions.len(), 1);
        assert_eq!(batch1.worker, EXECUTE_WORKER_START);

        // Assert - Nothing else scheduled.
        assert_eq!(bridge.pop_schedule(), None);
    }

    //////
    // TPU

    #[test]
    fn tpu_recv_schedules_check() {
        let (mut scheduler, _jito_tx) = test_scheduler();
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

    #[test]
    fn tpu_recv_filters_tip_program() {
        let (mut scheduler, _jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Build a TX that invokes the tip payment program (should be filtered).
        let payer = Keypair::new();
        let tx: VersionedTransaction = Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(25_000),
                ComputeBudgetInstruction::set_compute_unit_price(100),
                Instruction { program_id: TIP_PAYMENT_PROGRAM, accounts: vec![], data: vec![] },
            ],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::new_from_array([1; 32]),
        )
        .into();
        bridge.queue_tpu(&tx);

        // Poll the scheduler.
        scheduler.poll(&mut bridge);

        // Assert - No check scheduled (TX was filtered).
        assert_eq!(bridge.pop_schedule(), None);

        // Assert - TX was dropped from bridge.
        assert_eq!(bridge.tx_count(), 0);
    }

    ///////////////
    // Jito Packets

    #[test]
    fn jito_packet_schedules_check() {
        let (mut scheduler, jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Send a packet via jito channel.
        let payer = Keypair::new();
        let tx = noop_with_budget(&payer, 25_000, 100);
        jito_tx
            .send(JitoUpdate::Packet(bincode::serialize(&tx).unwrap()))
            .unwrap();

        // Poll to drain jito messages and schedule checks.
        scheduler.poll(&mut bridge);

        // Assert - A single check request was scheduled.
        let batch = bridge.pop_schedule().unwrap();
        assert_eq!(batch.flags & 1, pack_message_flags::CHECK);
        assert_eq!(batch.transactions.len(), 1);
        assert_eq!(bridge.pop_schedule(), None);
    }

    #[test]
    fn jito_packet_filters_tip_program() {
        let (mut scheduler, jito_tx) = test_scheduler();
        let mut bridge = TestBridge::new(5, 4);

        // Send a packet that invokes the tip payment program.
        let payer = Keypair::new();
        let tx: VersionedTransaction = Transaction::new_signed_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(25_000),
                ComputeBudgetInstruction::set_compute_unit_price(100),
                Instruction {
                    program_id: TIP_PAYMENT_PROGRAM,
                    accounts: vec![],
                    data: vec![],
                },
            ],
            Some(&payer.pubkey()),
            &[&payer],
            Hash::new_from_array([1; 32]),
        )
        .into();
        jito_tx
            .send(JitoUpdate::Packet(bincode::serialize(&tx).unwrap()))
            .unwrap();

        // Poll to drain jito messages.
        scheduler.poll(&mut bridge);

        // Assert - No check scheduled and TX dropped from bridge.
        assert_eq!(bridge.pop_schedule(), None);
        assert_eq!(bridge.tx_count(), 0);
    }
}
