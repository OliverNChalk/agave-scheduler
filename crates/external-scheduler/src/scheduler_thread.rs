use std::path::PathBuf;
use std::thread::JoinHandle;
use std::time::Duration;

use agave_bridge::SchedulerBindings;
use agave_scheduling_utils::handshake::{ClientLogon, client as handshake_client};
use toolbox::shutdown::Shutdown;

use crate::schedulers::Scheduler;

pub(crate) fn spawn<S>(shutdown: Shutdown, bindings_ipc: PathBuf) -> Vec<JoinHandle<()>>
where
    S: Scheduler + Send,
{
    let (mut scheduler, mut threads) = S::new();
    let scheduler_thread = std::thread::Builder::new()
        .name("Scheduler".to_string())
        .spawn(move || {
            let session = handshake_client::connect(
                &bindings_ipc,
                ClientLogon {
                    worker_count: 5,
                    allocator_size: 256 * 1024 * 1024,
                    allocator_handles: 1,
                    tpu_to_pack_capacity: 2usize.pow(16),
                    progress_tracker_capacity: 128,
                    pack_to_worker_capacity: 128,
                    worker_to_pack_capacity: 256,
                    flags: 0,
                },
                Duration::from_secs(1),
            )
            .unwrap();
            let mut bridge = SchedulerBindings::new(session);

            while !shutdown.is_shutdown() {
                scheduler.poll(&mut bridge);
            }
        })
        .unwrap();
    threads.insert(0, scheduler_thread);

    threads
}
