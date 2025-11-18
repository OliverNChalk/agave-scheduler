use agave_scheduler_bindings::pack_message_flags::check_flags;
use agave_scheduler_bindings::{IS_LEADER, ProgressMessage, pack_message_flags};
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

pub struct FerrariScheduler {
    core: SchedulerCore,
}

impl FerrariScheduler {
    #[must_use]
    pub fn new(core: SchedulerCore) -> Self {
        Self { core }
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
        match is_leader {
            true => self.core.drain_tpu(|tx| todo!(), 128),
            false => self.core.drain_tpu(|tx| todo!(), 1024),
        }

        self.schedule();
    }

    fn schedule(&mut self) {
        // Schedule additional checks.
        while self.core.worker(CHECK_WORKER).rem() > 0 {
            self.core.schedule_check(
                CHECK_WORKER,
                // TODO: Construct batch.
                &[],
                u64::MAX,
                pack_message_flags::CHECK
                    | check_flags::STATUS_CHECKS
                    | check_flags::LOAD_FEE_PAYER_BALANCE
                    | check_flags::LOAD_ADDRESS_LOOKUP_TABLES,
            );
        }

        // If we are the leader, schedule executes.
        if self.core.progress().leader_state == IS_LEADER {
            self.core.schedule_execute(
                EXECUTE_WORKER,
                // TODO: Construct batch.
                &[],
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

    fn drain_tpu(&mut self, on_tpu: impl FnMut(TransactionPtr) -> TpuDecision, max_count: usize) {
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
    Store,
    Drop,
}
