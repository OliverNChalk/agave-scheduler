mod jito_thread;

use std::collections::VecDeque;
use std::thread::JoinHandle;

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
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_svm_transaction::svm_message::SVMStaticMessage;
use solana_transaction::sanitized::MessageHash;

use crate::batch::jito_thread::{JitoConfig, JitoThread};
use crate::shared::PriorityId;

const UNCHECKED_CAPACITY: usize = 64 * 1024;
const CHECKED_CAPACITY: usize = 64 * 1024;

const TX_REGION_SIZE: usize = std::mem::size_of::<SharableTransactionRegion>();
const TX_BATCH_PER_MESSAGE: usize = TX_REGION_SIZE + std::mem::size_of::<PriorityId>();
const TX_BATCH_SIZE: usize = TX_BATCH_PER_MESSAGE * MAX_TRANSACTIONS_PER_MESSAGE;
const_assert!(TX_BATCH_SIZE < 4096);

const CHECK_WORKER: usize = 0;
/// How many percentage points before the end should we aim to fill the block.
const BLOCK_FILL_CUTOFF: u8 = 20;

pub struct BatchScheduler {
    bundle_rx: crossbeam_channel::Receiver<Vec<Vec<u8>>>,

    // TODO: Bundles should be sorted against transactions.
    bundles: VecDeque<Vec<TransactionKey>>,
    unchecked_tx: MinMaxHeap<PriorityId>,
    checked_tx: MinMaxHeap<PriorityId>,
    cu_in_flight: u32,
    schedule_locks: HashMap<Pubkey, bool>,
    schedule_batch: Vec<KeyedTransactionMeta<PriorityId>>,

    metrics: BatchMetrics,
}

impl BatchScheduler {
    #[must_use]
    pub fn new() -> (Self, Vec<JoinHandle<()>>) {
        let (bundle_tx, bundle_rx) = crossbeam_channel::bounded(128);
        let keypair = Keypair::new();
        let jito_thread = JitoThread::spawn(
            bundle_tx,
            JitoConfig { url: "https://mainnet.block-engine.jito.wtf".to_string() },
            keypair,
        );

        (
            Self {
                bundle_rx,

                bundles: VecDeque::new(),
                unchecked_tx: MinMaxHeap::with_capacity(UNCHECKED_CAPACITY),
                checked_tx: MinMaxHeap::with_capacity(CHECKED_CAPACITY),
                cu_in_flight: 0,
                schedule_locks: HashMap::new(),
                schedule_batch: Vec::new(),

                metrics: BatchMetrics::new(),
            },
            vec![jito_thread],
        )
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

        // Drain pending bundles.
        self.drain_bundles(bridge);

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
        self.metrics
            .unchecked_len
            .set(self.unchecked_tx.len() as f64);
        self.metrics.checked_len.set(self.checked_tx.len() as f64);
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
        let shortfall = (self.checked_tx.len() + additional).saturating_sub(UNCHECKED_CAPACITY);

        // NB: Technically we are evicting more than we need to because not all of
        // `additional` will parse correctly & thus have a priority.
        for _ in 0..shortfall {
            let id = self.unchecked_tx.pop_min().unwrap();

            bridge.tx_drop(id.key);
        }
        self.metrics.recv_evict.increment(shortfall as u64);

        // TODO: Need to dedupe already seen transactions?

        bridge.tpu_drain(
            |bridge, key| match Self::calculate_priority(bridge.runtime(), &bridge.tx(key).data) {
                Some((priority, cost)) => {
                    self.unchecked_tx.push(PriorityId { priority, cost, key });
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

    fn drain_bundles<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        'outer: while let Ok(bundle) = self.bundle_rx.try_recv() {
            let mut keys = Vec::with_capacity(bundle.len());
            for packet in bundle {
                let Ok(key) = bridge.tx_insert(&packet) else {
                    for key in keys {
                        bridge.tx_drop(key);
                    }

                    continue 'outer;
                };

                keys.push(key);
            }

            self.bundles.push_back(keys);
        }
    }

    fn schedule_checks<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = PriorityId>,
    {
        // Loop until worker queue is filled or backlog is empty.
        let start_len = self.unchecked_tx.len();
        while bridge.worker(0).rem() > 0 {
            if self.unchecked_tx.is_empty() {
                break;
            }

            self.schedule_batch.clear();
            self.schedule_batch.extend(std::iter::from_fn(|| {
                self.unchecked_tx
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
            .increment((start_len - self.unchecked_tx.len()) as u64);
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
            if budget_remaining == 0 || self.checked_tx.is_empty() {
                break;
            }

            // If the worker already has a pending job, don't give it any more.
            if bridge.worker(worker).len() > 0 {
                continue;
            }

            let pop_next = || {
                self.checked_tx
                    .pop_max()
                    .filter(|id| {
                        // Check if we can fit the TX within our budget.
                        if u64::from(id.cost) > budget_remaining {
                            self.checked_tx.push(*id);

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
                            self.checked_tx.push(*id);
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
        if self.checked_tx.len() == CHECKED_CAPACITY {
            let id = self.checked_tx.pop_min().unwrap();
            bridge.tx_drop(id.key);

            self.metrics.check_evict.increment(1);
        }

        // Insert the new transaction (yes this may be lower priority then what
        // we just evicted but that's fine).
        self.checked_tx.push(meta);

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

struct BatchMetrics {
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

impl BatchMetrics {
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
