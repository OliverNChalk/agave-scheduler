use agave_scheduler_bindings::{ProgressMessage, TpuToPackMessage};
use agave_scheduling_utils::handshake::client::{ClientSession, ClientWorkerSession};
use rts_alloc::Allocator;

pub struct GreedyScheduler {
    allocator: Allocator,
    tpu_to_pack: shaq::Consumer<TpuToPackMessage>,
    progress_tracker: shaq::Consumer<ProgressMessage>,
    workers: Vec<ClientWorkerSession>,
    progress: ProgressMessage,
}

impl GreedyScheduler {
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
        }
    }

    pub fn poll(&mut self) {
        // Drain the progress tracker so we know which slot we're on.
        self.drain_progress();

        // Drain responses from workers.
        self.drain_worker_responses();
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

        // Ingest a simple transfer.
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        harness.ingest_transaction(&solana_system_transaction::transfer(
            &from,
            &to,
            1,
            Hash::new_unique(),
        ));

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
                allocator_size: 1024 * 1024,
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

        fn ingest_transaction(&mut self, tx: &Transaction) {
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
