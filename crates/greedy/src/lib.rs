use std::collections::VecDeque;

use agave_scheduler_bindings::{
    IS_LEADER, MAX_TRANSACTIONS_PER_MESSAGE, PackToWorkerMessage, ProgressMessage,
    SharableTransactionBatchRegion, SharableTransactionRegion, TpuToPackMessage,
    pack_message_flags,
};
use agave_scheduling_utils::handshake::client::{ClientSession, ClientWorkerSession};
use rts_alloc::Allocator;

pub struct GreedyScheduler {
    allocator: Allocator,
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    progress_tracker: shaq::Consumer<ProgressMessage>,
    workers: Vec<ClientWorkerSession>,

    progress: ProgressMessage,
    queue: VecDeque<SharableTransactionRegion>,
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
            queue: VecDeque::default(),
        }
    }

    pub fn poll(&mut self) {
        // Drain the progress tracker so we know which slot we're on.
        self.drain_progress();

        // Drain responses from workers.
        self.drain_worker_responses();

        // Ingest a bounded amount of new transactions.
        self.drain_tpu(128);

        // Schedule if we're currently the leader.
        if self.progress.leader_state == IS_LEADER {
            self.schedule();
        }
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
                println!("{msg:?}");
            }
        }
    }

    fn drain_tpu(&mut self, max_count: usize) {
        self.tpu_to_pack.sync();
        for _ in 0..max_count {
            let Some(msg) = self.tpu_to_pack.try_read() else {
                return;
            };
            println!("{msg:?}");

            self.queue.push_back(msg.transaction);
        }
    }

    fn schedule(&mut self) {
        for worker in &mut self.workers {
            worker.pack_to_worker.sync();

            if self.queue.is_empty() {
                continue;
            }

            // Allocate a batch that can hold all our transaction pointers.
            let transactions = self
                .allocator
                .allocate(
                    (core::mem::size_of::<SharableTransactionRegion>()
                        * MAX_TRANSACTIONS_PER_MESSAGE) as u32,
                )
                .unwrap();
            let transactions_offset = unsafe { self.allocator.offset(transactions) };

            // Fill in the batch with transaction pointers.
            let mut num_transactions = 0;
            while num_transactions < MAX_TRANSACTIONS_PER_MESSAGE {
                let Some(tx) = self.queue.pop_front() else {
                    break;
                };

                // SAFETY:
                // - We have allocated the transaction batch to support at least
                //   `MAX_TRANSACTIONS_PER_MESSAGE`, we terminate the loop before we overrun the
                //   region.
                unsafe {
                    self.allocator
                        .ptr_from_offset(transactions_offset)
                        .cast::<SharableTransactionRegion>()
                        .add(num_transactions)
                        .write(tx);
                };

                num_transactions += 1;
            }

            println!("created batch with {num_transactions} txs");

            worker
                .pack_to_worker
                .try_write(PackToWorkerMessage {
                    flags: pack_message_flags::EXECUTE,
                    max_working_slot: self.progress.current_slot + 1,
                    batch: SharableTransactionBatchRegion {
                        num_transactions: num_transactions.try_into().unwrap(),
                        transactions_offset,
                    },
                })
                .unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::IntoRawFd;
    use std::ptr::NonNull;

    use agave_scheduler_bindings::{
        SharableTransactionRegion, TransactionResponseRegion, WorkerToPackMessage, processed_codes,
    };
    use agave_scheduling_utils::handshake::server::AgaveSession;
    use agave_scheduling_utils::handshake::{self, ClientLogon};
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Pubkey};
    use solana_transaction::Transaction;

    use super::*;

    #[test]
    fn check_on_ingest() {
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

        // Assert - One worker is requested to check the transaction.
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
        assert_eq!(message.flags & 1, 0);

        // Queue the mock worker response.
        harness.session.workers[worker_index]
            .worker_to_pack
            .try_write(WorkerToPackMessage {
                batch: message.batch,
                processed_code: processed_codes::PROCESSED,
                responses: check_response(),
            })
            .unwrap();

        // Assert - Scheduler does not schedule the valid TX.
        assert!(harness.session.workers.iter_mut().all(|worker| {
            worker.pack_to_worker.sync();

            worker.pack_to_worker.is_empty()
        }));
    }

    fn setup() -> (AgaveSession, GreedyScheduler) {
        todo!()
    }

    fn simple_transfer(allocator: &Allocator) -> SharableTransactionRegion {
        todo!()
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

        fn allocator(&self) -> &Allocator {
            &self.session.tpu_to_pack.allocator
        }
    }
}
