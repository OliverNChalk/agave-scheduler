use std::collections::VecDeque;
use std::ptr::NonNull;

use agave_scheduler_bindings::worker_message_types::{
    self, CheckResponse, fee_payer_balance_flags, parsing_and_sanitization_flags, resolve_flags,
    status_check_flags,
};
use agave_scheduler_bindings::{
    IS_LEADER, MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, ProgressMessage,
    SharableTransactionBatchRegion, SharableTransactionRegion, TpuToPackMessage,
    pack_message_flags, processed_codes,
};
use agave_scheduling_utils::check_responses_ptr::CheckResponsesPtr;
use agave_scheduling_utils::handshake::client::{ClientSession, ClientWorkerSession};
use agave_scheduling_utils::transaction_ptr::TransactionPtrBatch;
use rts_alloc::Allocator;

pub struct GreedyScheduler {
    allocator: Allocator,
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    progress_tracker: shaq::Consumer<ProgressMessage>,
    workers: Vec<ClientWorkerSession>,

    progress: ProgressMessage,
    queue_unchecked: VecDeque<SharableTransactionRegion>,
    queue_checked: VecDeque<SharableTransactionRegion>,
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
            queue_unchecked: VecDeque::default(),
            queue_checked: VecDeque::default(),
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
            println!("scheduling execute");
            self.schedule_execute();
        }

        // TODO: Think about re-checking all TXs on slot roll (or at least
        // expired TXs).
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
                            let ptr = self
                                .allocator
                                .ptr_from_offset(msg.responses.transaction_responses_offset);
                            self.allocator.free(ptr);
                        }

                        continue;
                    }
                    processed_codes::PROCESSED => {}
                    _ => panic!(),
                };

                match msg.responses.tag {
                    worker_message_types::CHECK_RESPONSE => {
                        let ptr = self
                            .allocator
                            .ptr_from_offset(msg.responses.transaction_responses_offset)
                            .cast::<CheckResponse>();

                        self.on_check_response(ptr, msg.responses.num_transaction_responses);
                    }
                    worker_message_types::EXECUTION_RESPONSE => todo!(),
                    _ => panic!(),
                }

                println!("{msg:?}");
            }
        }
    }

    fn drain_tpu(&mut self, max: usize) {
        self.tpu_to_pack.sync();
        for _ in 0..max {
            let Some(msg) = self.tpu_to_pack.try_read() else {
                return;
            };

            self.queue_unchecked.push_back(msg.transaction);
        }
    }

    fn schedule_checks(&mut self) {
        // TODO: Need to figure out how back pressure works when the check worker is not
        // keeping up.
        while !self.queue_unchecked.is_empty() {
            let worker = &mut self.workers[0];
            worker.pack_to_worker.sync();
            worker
                .pack_to_worker
                .try_write(PackToWorkerMessage {
                    flags: pack_message_flags::CHECK,
                    max_working_slot: self.progress.current_slot + 1,
                    batch: Self::collect_batch(&self.allocator, || {
                        self.queue_unchecked.pop_front()
                    }),
                })
                .unwrap();
            worker.pack_to_worker.commit();
        }
    }

    fn schedule_execute(&mut self) {
        for worker in &mut self.workers[1..] {
            worker.pack_to_worker.sync();

            if self.queue_checked.is_empty() {
                continue;
            }

            let batch = Self::collect_batch(&self.allocator, || self.queue_checked.pop_front());
            println!("scheduled: {}", batch.num_transactions);

            worker.pack_to_worker.sync();
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

    fn on_check_response(&self, batch: TransactionPtrBatch, responses: CheckResponsesPtr) {
        assert_eq!(batch.len(), responses.len());

        for (tx, rep) in batch.iter().zip(responses.iter().copied()) {
            let parsing_failed =
                rep.parsing_and_sanitization_flags == parsing_and_sanitization_flags::FAILED;
            let status_failed = rep.status_check_flags
                & !(status_check_flags::REQUESTED | status_check_flags::PERFORMED)
                != 0;
            if parsing_failed || status_failed {
                // TODO: Should this match the API of the other ptr types.
                unsafe {
                    tx.free(&self.allocator);
                }

                continue;
            }

            // Sanity check the flags.
            assert_ne!(rep.status_check_flags & status_check_flags::REQUESTED, 0);
            assert_ne!(rep.status_check_flags & status_check_flags::PERFORMED, 0);
            assert_eq!(rep.resolve_flags, resolve_flags::REQUESTED | resolve_flags::PERFORMED);
            assert_eq!(
                rep.fee_payer_balance_flags,
                fee_payer_balance_flags::REQUESTED | fee_payer_balance_flags::PERFORMED
            );

            todo!()
        }

        todo!()
    }

    fn collect_batch(
        allocator: &Allocator,
        mut pop: impl FnMut() -> Option<SharableTransactionRegion>,
    ) -> SharableTransactionBatchRegion {
        // Allocate a batch that can hold all our transaction pointers.
        let transactions = allocator
            .allocate(
                (core::mem::size_of::<SharableTransactionRegion>() * MAX_TRANSACTIONS_PER_MESSAGE)
                    as u32,
            )
            .unwrap();
        let transactions_offset = unsafe { allocator.offset(transactions) };

        // Fill in the batch with transaction pointers.
        let mut num_transactions = 0;
        while num_transactions < MAX_TRANSACTIONS_PER_MESSAGE {
            let Some(tx) = pop() else {
                break;
            };

            // SAFETY:
            // - We have allocated the transaction batch to support at least
            //   `MAX_TRANSACTIONS_PER_MESSAGE`, we terminate the loop before we overrun the
            //   region.
            unsafe {
                allocator
                    .ptr_from_offset(transactions_offset)
                    .cast::<SharableTransactionRegion>()
                    .add(num_transactions)
                    .write(tx);
            };

            num_transactions += 1;
        }

        SharableTransactionBatchRegion {
            num_transactions: num_transactions.try_into().unwrap(),
            transactions_offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::IntoRawFd;
    use std::ptr::NonNull;

    use agave_scheduler_bindings::{
        SharableTransactionRegion, TransactionResponseRegion, WorkerToPackMessage, processed_codes,
        worker_message_types,
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
        let mut worker_requests: Vec<_> = harness
            .session
            .workers
            .iter_mut()
            .enumerate()
            .flat_map(|(i, worker)| {
                worker.pack_to_worker.sync();

                std::iter::from_fn(move || worker.pack_to_worker.try_read().map(|msg| (i, *msg)))
            })
            .collect();
        assert_eq!(worker_requests.len(), 1);
        let (worker_index, message) = worker_requests.remove(0);
        assert_eq!(message.flags & 1, pack_message_flags::CHECK);

        // Response with OK.
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
        let mut worker_requests: Vec<_> = harness
            .session
            .workers
            .iter_mut()
            .enumerate()
            .flat_map(|(i, worker)| {
                worker.pack_to_worker.sync();

                std::iter::from_fn(move || worker.pack_to_worker.try_read().map(|msg| (i, *msg)))
            })
            .collect();
        assert_eq!(worker_requests.len(), 1);
        let (worker_index, message) = worker_requests.remove(0);
        assert_eq!(message.flags & 1, pack_message_flags::CHECK);

        // Response with OK.
        harness.send_check_ok(worker_index, message);
        harness.poll_scheduler();

        // Assert - Scheduler does not schedule the valid TX.
        assert!(harness.session.workers.iter_mut().all(|worker| {
            worker.pack_to_worker.sync();

            worker.pack_to_worker.is_empty()
        }));
    }

    fn check_response() -> TransactionResponseRegion {
        todo!()
    }

    struct Harness {
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

            Self { session: agave_session, scheduler: GreedyScheduler::new(client_session) }
        }

        fn allocator(&self) -> &Allocator {
            &self.session.tpu_to_pack.allocator
        }

        fn poll_scheduler(&mut self) {
            self.scheduler.poll();
        }

        fn send_progress(&mut self, progress: ProgressMessage) {
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
            let (_, responses) = allocate_check_response_region(self.allocator(), 1).unwrap();

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
