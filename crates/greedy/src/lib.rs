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

    use agave_scheduler_bindings::{
        SharableTransactionRegion, TransactionResponseRegion, WorkerToPackMessage,
        pack_message_flags, processed_codes,
    };
    use agave_scheduling_utils::handshake::server::{AgaveSession, AgaveTpuToPackSession};
    use agave_scheduling_utils::handshake::{self, ClientLogon};

    use super::*;

    #[test]
    fn check_on_ingest() {
        let (mut session, mut scheduler) = setup();

        // Write a simple transfer.
        session
            .tpu_to_pack
            .producer
            .try_write(TpuToPackMessage {
                transaction: simple_transfer(&session.tpu_to_pack.allocator),
                flags: 0,
                src_addr: [0; 16],
            })
            .unwrap();
        session.tpu_to_pack.producer.commit();

        // Poll the greedy scheduler.
        scheduler.poll();

        // Assert - One worker is requested to check the transaction.
        let mut worker_requests: Vec<_> = session
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
        session.workers[worker_index]
            .worker_to_pack
            .try_write(WorkerToPackMessage {
                batch: message.batch,
                processed_code: processed_codes::PROCESSED,
                responses: check_response(),
            })
            .unwrap();

        // Assert - Scheduler does not schedule the valid TX.
        assert!(session.workers.iter_mut().all(|worker| {
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
            let (server_session, files) = handshake::server::Server::setup_session(logon).unwrap();
            let client_session = handshake::client::setup_session(
                &logon,
                files.into_iter().map(|file| file.into_raw_fd()).collect(),
            )
            .unwrap();

            Self { session: (), scheduler: () }
        }
    }
}
