use std::thread::JoinHandle;

use toolbox::shutdown::Shutdown;

pub(crate) struct WorkerThread {
    shutdown: Shutdown,
}

impl WorkerThread {
    pub(crate) fn spawn(shutdown: Shutdown) -> JoinHandle<()> {
        std::thread::Builder::new()
            .name("Worker".to_string())
            .spawn(|| {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(WorkerThread { shutdown }.run());
            })
            .unwrap()
    }

    async fn run(self) {
        self.shutdown.cancelled().await;
    }
}
