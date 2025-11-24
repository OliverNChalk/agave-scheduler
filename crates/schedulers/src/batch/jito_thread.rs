use std::thread::JoinHandle;

pub(crate) struct JitoThread {}

impl JitoThread {
    pub(crate) fn spawn(bundle_tx: crossbeam_channel::Sender<()>) -> JoinHandle<()> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        std::thread::Builder::new()
            .name("Jito".to_string())
            .spawn(move || rt.block_on(JitoThread {}.run()))
            .unwrap()
    }

    async fn run(self) {
        todo!()
    }
}
