use std::collections::VecDeque;

use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::worker_message_types::{
    parsing_and_sanitization_flags, status_check_flags,
};
use agave_scheduler_bindings::{IS_LEADER, MAX_TRANSACTIONS_PER_MESSAGE, pack_message_flags};
use bridge::{Bridge, TransactionId, TxDecision, Worker, WorkerResponse};

// TODO:
//
// - Implement dead simple fifo scheduler using mock interface.
// - Fill in all the methods behind the mock interface by duplicating code from
//   greedy.
// - Duplicate basic greedy tests & confirm they still work (not the ordering
//   ones just the simple ones).
// - Move core to shared location & dedupe code.
// - Confirm all tests still work.
// - PR it.

const CHECK_WORKER: usize = 0;
const EXECUTE_WORKER: usize = 1;

pub struct FifoScheduler<B> {
    bridge: B,
    check_queue: VecDeque<TransactionId>,
    execute_queue: VecDeque<TransactionId>,
    batch: Vec<TransactionId>,
}

impl<B> FifoScheduler<B>
where
    B: Bridge,
{
    #[must_use]
    pub fn new(bridge: B) -> Self {
        Self {
            bridge,
            check_queue: VecDeque::default(),
            execute_queue: VecDeque::default(),
            batch: Vec::with_capacity(MAX_TRANSACTIONS_PER_MESSAGE),
        }
    }

    pub fn poll(&mut self) {
        // Drain the progress tracker so we know which slot we're on.
        self.bridge.drain_progress();

        // Drain check responses.
        while self.bridge.pop_worker(CHECK_WORKER, |(id, _, rep)| {
            let WorkerResponse::Check(rep, _) = rep else {
                panic!();
            };

            // TODO: Dedupe with greedy & make this friendlier.
            let parsing_failed =
                rep.parsing_and_sanitization_flags == parsing_and_sanitization_flags::FAILED;
            let status_failed = rep.status_check_flags
                & !(status_check_flags::REQUESTED | status_check_flags::PERFORMED)
                != 0;

            match parsing_failed || status_failed {
                true => TxDecision::Drop,
                false => {
                    self.execute_queue.push_back(id);

                    TxDecision::Keep
                }
            }
        }) {}

        // Drain execute responses.
        while self
            .bridge
            .pop_worker(EXECUTE_WORKER, |(..)| TxDecision::Drop)
        {}

        // Ingest a bounded amount of new transactions.
        let max_count = match self.bridge.progress().leader_state == IS_LEADER {
            true => 128,
            false => 1024,
        };
        self.bridge.drain_tpu(
            |(id, _)| {
                self.check_queue.push_back(id);

                TxDecision::Keep
            },
            max_count,
        );

        // Schedule checks & execution (if we're the leader).
        self.schedule();
    }

    fn schedule(&mut self) {
        // Schedule additional checks.
        while !self.bridge.worker(CHECK_WORKER).is_empty() {
            self.batch.clear();
            self.batch.extend(
                std::iter::from_fn(|| self.check_queue.pop_front())
                    .take(MAX_TRANSACTIONS_PER_MESSAGE),
            );
            self.bridge.schedule_check(
                CHECK_WORKER,
                &self.batch,
                u64::MAX,
                pack_message_flags::CHECK
                    | check_flags::STATUS_CHECKS
                    | check_flags::LOAD_FEE_PAYER_BALANCE
                    | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
            );
        }

        // If we are the leader, schedule executes.
        if self.bridge.progress().leader_state == IS_LEADER
            && self.bridge.worker(EXECUTE_WORKER).len() == 0
        {
            self.batch.clear();
            self.batch.extend(
                std::iter::from_fn(|| self.execute_queue.pop_front())
                    .take(MAX_TRANSACTIONS_PER_MESSAGE),
            );
            self.bridge.schedule_execute(
                EXECUTE_WORKER,
                &self.batch,
                self.bridge.progress().current_slot + 1,
                pack_message_flags::EXECUTE,
            );
        }
    }
}
