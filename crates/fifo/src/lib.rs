use std::collections::VecDeque;

use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::{
    IS_LEADER, MAX_TRANSACTIONS_PER_MESSAGE, ProgressMessage, pack_message_flags,
};
use agave_scheduling_utils::responses_region::{CheckResponsesPtr, ExecutionResponsesPtr};
use agave_scheduling_utils::transaction_ptr::{TransactionPtr, TransactionPtrBatch};

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

const CHECK_WORKER: WorkerId = WorkerId;
const EXECUTE_WORKER: WorkerId = WorkerId;

pub struct FifoScheduler {
    core: SchedulerCore,
    check_queue: VecDeque<TransactionId>,
    execute_queue: VecDeque<TransactionId>,
    batch: Vec<TransactionId>,
}

impl FifoScheduler {
    #[must_use]
    pub fn new(core: SchedulerCore) -> Self {
        Self {
            core,
            check_queue: VecDeque::default(),
            execute_queue: VecDeque::default(),
            batch: Vec::with_capacity(MAX_TRANSACTIONS_PER_MESSAGE),
        }
    }

    pub fn poll(&mut self) {
        // Drain the progress tracker so we know which slot we're on.
        self.core.drain_progress();
        let is_leader = self.core.progress().leader_state == IS_LEADER;

        // Drain check responses.
        while let Some((batch, rep)) = self.core.pop_check() {
            todo!("free stuff");
        }

        // Drain execute responses.
        while let Some((batch, rep)) = self.core.pop_execute() {
            todo!("free stuff");
        }

        // Ingest a bounded amount of new transactions.
        let handle_tx = |(id, _)| {
            self.check_queue.push_back(id);

            TpuDecision::Keep
        };
        match is_leader {
            true => self.core.drain_tpu(handle_tx, 128),
            false => self.core.drain_tpu(handle_tx, 1024),
        }

        self.schedule();
    }

    fn schedule(&mut self) {
        // Schedule additional checks.
        while self.core.worker(CHECK_WORKER).rem() > 0 {
            self.batch.clear();
            self.batch.extend(
                std::iter::from_fn(|| self.check_queue.pop_front())
                    .take(MAX_TRANSACTIONS_PER_MESSAGE),
            );
            self.core.schedule_check(
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
        if self.core.progress().leader_state == IS_LEADER {
            self.batch.clear();
            self.batch.extend(
                std::iter::from_fn(|| self.execute_queue.pop_front())
                    .take(MAX_TRANSACTIONS_PER_MESSAGE),
            );
            self.core.schedule_execute(
                EXECUTE_WORKER,
                &self.batch,
                self.core.progress().current_slot + 1,
                pack_message_flags::EXECUTE,
            );
        }
    }
}

struct SchedulerCore;

impl SchedulerCore {
    fn progress(&self) -> &ProgressMessage {
        todo!()
    }

    fn worker(&self, id: WorkerId) -> &Worker {
        todo!()
    }

    fn drain_progress(&mut self) {
        todo!()
    }

    fn drain_tpu(
        &mut self,
        on_tpu: impl FnMut((TransactionId, TransactionPtr)) -> TpuDecision,
        max_count: usize,
    ) {
        todo!()
    }

    fn pop_check(&mut self) -> Option<(TransactionPtrBatch, CheckResponsesPtr)> {
        todo!()
    }

    fn pop_execute(&mut self) -> Option<(TransactionPtrBatch, ExecutionResponsesPtr)> {
        todo!()
    }

    fn schedule_check(
        &mut self,
        worker: WorkerId,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    ) {
        todo!()
    }

    fn schedule_execute(
        &mut self,
        worker: WorkerId,
        batch: &[TransactionId],
        max_working_slot: u64,
        flags: u16,
    ) {
        todo!()
    }
}

#[derive(Debug, Clone, Copy)]
struct WorkerId;

struct Worker;

impl Worker {
    fn rem(&self) -> usize {
        todo!()
    }
}

struct TransactionId;

enum TpuDecision {
    Keep,
    Drop,
}
