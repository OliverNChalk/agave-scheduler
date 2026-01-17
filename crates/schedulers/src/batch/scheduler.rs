use std::collections::{BTreeSet, VecDeque};
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
    ChangeTipReceiverArgs, TIP_PAYMENT_PROGRAM, TipDistributionArgs, change_tip_receiver,
    init_tip_distribution,
};
use crate::events::{
    CheckFailure, Event, EventEmitter, EvictReason, SlotStatsEvent, TransactionAction,
    TransactionEvent, TransactionSource,
};
use crate::shared::{PriorityId, TARGET_BATCH_SIZE};

const UNCHECKED_CAPACITY: usize = 64 * 1024;
const CHECKED_CAPACITY: usize = 64 * 1024;

const TX_REGION_SIZE: usize = std::mem::size_of::<SharableTransactionRegion>();
const TX_BATCH_PER_MESSAGE: usize = TX_REGION_SIZE + std::mem::size_of::<PriorityId>();
const TX_BATCH_SIZE: usize = TX_BATCH_PER_MESSAGE * MAX_TRANSACTIONS_PER_MESSAGE;
const_assert!(TX_BATCH_SIZE < 4096);

const CHECK_WORKER: usize = 0;
const BUNDLE_WORKER: usize = 1;
const TPU_WORKER_START: usize = 2;
const MAX_CHECK_BATCHES: usize = 8;
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
    // TODO: Bundles should be sorted against transactions.
    bundles: VecDeque<(Instant, Vec<TransactionKey>)>,
    unchecked_tx: MinMaxHeap<PriorityId>,
    checked_tx: BTreeSet<PriorityId>,
    executing_tx: HashSet<TransactionKey>,
    next_recheck: Option<PriorityId>,
    in_flight_cus: u32,
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
        BatchSchedulerArgs { tip, jito, keypair }: BatchSchedulerArgs,
    ) -> (Self, Vec<JoinHandle<()>>) {
        let (jito_tx, jito_rx) = crossbeam_channel::bounded(1024);
        let jito_thread = JitoThread::spawn(shutdown.clone(), jito_tx, jito, keypair);

        let JitoUpdate::BuilderConfig(builder_config) =
            jito_rx.recv_timeout(Duration::from_secs(5)).unwrap()
        else {
            panic!();
        };

        (
            Self {
                shutdown,
                jito_rx,
                tip_distribution_config: tip,
                keypair,

                builder_config,
                tip_config: None,
                recent_blockhash: Hash::default(),
                bundles: VecDeque::new(),
                unchecked_tx: MinMaxHeap::with_capacity(UNCHECKED_CAPACITY),
                checked_tx: BTreeSet::new(),
                executing_tx: HashSet::new(),
                next_recheck: None,
                in_flight_cus: 0,
                in_flight_locks: HashMap::new(),
                schedule_batch: Vec::new(),
                last_progress_time: Instant::now(),

                events,
                slot: 0,
                slot_stats: SlotStatsEvent::default(),
                metrics: BatchMetrics::new(),
            },
            vec![jito_thread],
        )
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
        self.metrics.bundles_len.set(self.bundles.len() as f64);
        self.metrics
            .locks_len
            .set(self.in_flight_locks.len() as f64);
        self.metrics
            .in_flight_cus
            .set(f64::from(self.in_flight_cus));
        self.metrics
            .in_flight_locks
            .set(self.in_flight_locks.len() as f64);
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

        // TODO: Schedule as a single batch once we have SIMD83 live.
        bridge.schedule(ScheduleBatch {
            worker: BUNDLE_WORKER,
            transactions: &[KeyedTransactionMeta {
                key: init_tip_distribution,
                meta: PriorityId { priority: u64::MAX, cost: 0, key: init_tip_distribution },
            }],
            max_working_slot: self.slot + 4,
            flags: pack_message_flags::EXECUTE | execution_flags::DROP_ON_FAILURE,
        });
        bridge.schedule(ScheduleBatch {
            worker: BUNDLE_WORKER,
            transactions: &[KeyedTransactionMeta {
                key: change_tip_receiver,
                meta: PriorityId { priority: u64::MAX, cost: 0, key: change_tip_receiver },
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
                    WorkerAction::Unprocessed => TxDecision::Keep,
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
        let shortfall = (self.checked_tx.len() + additional).saturating_sub(UNCHECKED_CAPACITY);

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
        for packet in bundle {
            let Ok(key) = bridge.tx_insert(&packet) else {
                for key in keys {
                    bridge.tx_drop(key);
                }

                self.metrics.recv_bundle_err.increment(1);

                return;
            };

            keys.push(key);
        }

        // Filter bundles containing transactions that write to tip accounts.
        if keys.iter().any(|key| Self::should_filter(bridge.tx(*key))) {
            self.metrics.recv_bundle_filtered.increment(1);
            for key in keys {
                bridge.tx_drop(key);
            }

            return;
        }

        // Emit ingest events for bundle transactions.
        let bundle_sig = bridge.tx(keys[0]).data.signatures()[0];
        let bundle_id = Arc::new(bundle_sig.to_string());
        for &key in &keys {
            self.emit_tx_event(
                bridge,
                key,
                u64::MAX,
                TransactionAction::Ingest {
                    source: TransactionSource::Jito,
                    bundle: Some(bundle_id.clone()),
                },
            );
        }

        self.metrics.recv_bundle_ok.increment(1);
        // TODO: If Jito sends us a transaction (not a bundle) with overlapping
        // read/write keys we will panic as normally CHECK prevents this.
        self.bundles.push_back((Instant::now(), keys));
    }

    fn drop_expired_bundles<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        let now = Instant::now();
        while let Some((received_at, _)) = self.bundles.front() {
            if now.duration_since(*received_at) <= BUNDLE_EXPIRY {
                break;
            }

            let (_, bundle) = self.bundles.pop_front().unwrap();
            self.metrics.recv_bundle_expired.increment(1);
            for key in bundle {
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
                .extend(std::iter::from_fn(pop_next).take(TARGET_BATCH_SIZE));

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
        // TEMP: Schedule all bundles.
        for _ in 0..bridge.worker(1).rem() {
            let Some((_, bundle)) = self.bundles.front() else {
                break;
            };

            // See if the bundle can be scheduled without conflicts.
            if !bundle
                .iter()
                .all(|tx_key| Self::can_lock(&self.in_flight_locks, bridge, *tx_key))
            {
                break;
            }

            // Take all the locks necessary for the bundle.
            for tx_key in bundle {
                Self::lock(&mut self.in_flight_locks, bridge, *tx_key);
            }

            // Clear old data & build the batch.
            self.schedule_batch.clear();
            self.schedule_batch
                .extend(bundle.iter().map(|key| KeyedTransactionMeta {
                    key: *key,
                    meta: PriorityId { priority: u64::MAX, cost: 0, key: *key },
                }));

            // Schedule 1 bundle as 1 batch.
            bridge.schedule(ScheduleBatch {
                worker: BUNDLE_WORKER,
                transactions: &self.schedule_batch,
                max_working_slot: bridge.progress().current_slot + 1,
                flags: pack_message_flags::EXECUTE
                    | execution_flags::DROP_ON_FAILURE
                    | execution_flags::ALL_OR_NOTHING,
            });

            // Update metrics.
            self.metrics
                .execute_requested
                .increment(bundle.len() as u64);

            // Drop the bundle.
            self.bundles.pop_front().unwrap();
        }

        debug_assert_eq!(bridge.progress().leader_state, LEADER_READY);
        let budget_percentage =
            std::cmp::min(bridge.progress().current_slot_progress + BLOCK_FILL_CUTOFF, 100);
        // TODO: Would be ideal for the scheduler protocol to tell us the max block
        // units.
        let budget_limit = MAX_BLOCK_UNITS_SIMD_0256 * u64::from(budget_percentage) / 100;
        let cost_used = MAX_BLOCK_UNITS_SIMD_0256
            .saturating_sub(bridge.progress().remaining_cost_units)
            + u64::from(self.in_flight_cus);
        let mut budget_remaining = budget_limit.saturating_sub(cost_used);
        for worker in TPU_WORKER_START..bridge.worker_count() {
            if budget_remaining == 0 || self.checked_tx.is_empty() {
                break;
            }

            // If the worker already has a pending job, don't give it any more.
            if bridge.worker(worker).len() > 0 {
                continue;
            }

            // Our logic for selecting the next TX to schedule.
            let pop_next = || {
                self.checked_tx
                    .pop_last()
                    .filter(|id| {
                        // Check if we can fit the TX within our budget.
                        if u64::from(id.cost) > budget_remaining {
                            self.checked_tx.insert(*id);

                            return false;
                        }

                        // Check if this transaction's read/write locks conflict with any
                        // pre-existing read/write locks.
                        if !Self::can_lock(&self.in_flight_locks, bridge, id.key) {
                            self.checked_tx.insert(*id);
                            budget_remaining = 0;

                            return false;
                        }

                        // Insert all the locks.
                        Self::lock(&mut self.in_flight_locks, bridge, id.key);

                        // Update the budget as we are scheduling this TX.
                        budget_remaining = budget_remaining.saturating_sub(u64::from(id.cost));
                        self.in_flight_cus += id.cost;

                        // Track that this transaction is now executing.
                        self.executing_tx.insert(id.key);

                        true
                    })
                    .map(|id| KeyedTransactionMeta { key: id.key, meta: id })
            };

            // Clear any old data & build the batch.
            self.schedule_batch.clear();
            self.schedule_batch
                .extend(std::iter::from_fn(pop_next).take(TARGET_BATCH_SIZE));

            // If we failed to schedule anything, don't send the batch.
            if self.schedule_batch.is_empty() {
                break;
            }

            // Emit ExecuteReq events.
            for tx in &self.schedule_batch {
                self.emit_tx_event(bridge, tx.key, tx.meta.priority, TransactionAction::ExecuteReq);
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

        // Don't insert if transaction is currently executing. This can happen when a
        // recheck returns after execution was scheduled but before it completed.
        // Execution will handle cleanup.
        if self.executing_tx.contains(&meta.key) {
            self.metrics.check_ok.increment(1);

            return TxDecision::Keep;
        }

        // If already in checked_tx, this is a recheck completing - nothing to do.
        if self.checked_tx.contains(&meta) {
            self.metrics.check_ok.increment(1);

            return TxDecision::Keep;
        }

        // First check. Evict lowest priority if at capacity.
        if self.checked_tx.len() == CHECKED_CAPACITY {
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

    fn on_execute<B>(&mut self, bridge: &B, meta: PriorityId, rep: ExecutionResponse) -> TxDecision
    where
        B: Bridge<Meta = PriorityId>,
    {
        // Remove from executing set now that execution is complete.
        self.executing_tx.remove(&meta.key);

        // Remove in-flight costs.
        self.in_flight_cus -= meta.cost;

        // Remove in flight locks.
        let tx = bridge.tx(meta.key);
        for (lock, writable) in tx.locks() {
            let EntryRef::Occupied(mut entry) = self.in_flight_locks.entry_ref(lock) else {
                panic!();
            };

            // Remove the TX from the lock set.
            entry.get_mut().remove(meta.key, writable);

            // If the set is empty now, remove this write lock.
            if entry.get().is_empty() {
                entry.remove();
            }
        }

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

        TxDecision::Drop
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

    fn emit_tx_event<B>(
        &self,
        bridge: &B,
        key: TransactionKey,
        priority: u64,
        action: TransactionAction,
    ) where
        B: Bridge<Meta = PriorityId>,
    {
        let Some(events) = &self.events else { return };

        events.emit(Event::Transaction(TransactionEvent {
            signature: bridge.tx(key).data.signatures()[0],
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
}

struct BatchMetrics {
    current_slot: Gauge,
    next_leader_slot: Gauge,

    tpu_unchecked_len: Gauge,
    tpu_checked_len: Gauge,
    bundles_len: Gauge,
    locks_len: Gauge,

    in_flight_cus: Gauge,
    in_flight_locks: Gauge,

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

    check_requested: Counter,
    check_ok: Counter,
    check_err: Counter,
    check_evict: Counter,

    execute_requested: Counter,
    execute_ok: Counter,
    execute_err: Counter,
}

impl BatchMetrics {
    fn new() -> Self {
        Self {
            current_slot: gauge!("slot", "label" => "current"),
            next_leader_slot: gauge!("slot", "label" => "next_leader"),

            tpu_unchecked_len: gauge!("container_len", "label" => "tpu_unchecked"),
            tpu_checked_len: gauge!("container_len", "label" => "tpu_checked"),
            bundles_len: gauge!("container_len", "label" => "bundles"),
            locks_len: gauge!("container_len", "label" => "locks"),

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

            in_flight_cus: gauge!("in_flight_cus"),
            in_flight_locks: gauge!("in_flight_locks"),

            check_requested: counter!("check", "label" => "requested"),
            check_ok: counter!("check", "label" => "ok"),
            check_err: counter!("check", "label" => "err"),
            check_evict: counter!("check", "label" => "evict"),

            execute_requested: counter!("execute", "label" => "requested"),
            execute_ok: counter!("execute", "label" => "ok"),
            execute_err: counter!("execute", "label" => "err"),
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
