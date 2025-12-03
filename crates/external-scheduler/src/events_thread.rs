use agave_schedulers::events::{EventDiscriminants, StampedEvent};
use metrics_nats_exporter::async_nats::Client;
use tokio::sync::mpsc;

pub(crate) struct EventsThread {
    event_rx: mpsc::Receiver<StampedEvent>,

    client: &'static Client,
    subject_base: &'static str,
}

impl EventsThread {
    pub(crate) fn spawn(
        event_rx: mpsc::Receiver<StampedEvent>,
        client: &'static Client,
        host_name: &str,
    ) -> std::thread::JoinHandle<()> {
        let subject_base = format!("event.scheduler.{host_name}").leak();

        std::thread::Builder::new()
            .name("Events".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let thread = {
                    let _guard = rt.enter();

                    EventsThread { event_rx, client, subject_base }
                };

                rt.block_on(thread.run());
            })
            .unwrap()
    }

    async fn run(mut self) {
        while let Some(event) = self.event_rx.recv().await {
            self.on_event(event).await;
        }
    }

    async fn on_event(&self, event: StampedEvent) {
        self.client
            .publish(
                format!(
                    "{}.{}",
                    self.subject_base,
                    <&str>::from(EventDiscriminants::from(&event.event))
                ),
                serde_json::to_string(&event).unwrap().into(),
            )
            .await
            .unwrap();
    }
}
