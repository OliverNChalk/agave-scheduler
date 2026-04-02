use std::thread::JoinHandle;

use agave_scheduler_batch::BatchScheduler;
use agave_scheduler_fifo::FifoScheduler;
use agave_scheduler_greedy_revenue::GreedyRevenueScheduler;
use agave_scheduler_greedy_throughput::GreedyThroughputScheduler;
use agave_schedulers::shared::PriorityId;
use agave_scheduling_utils::bridge::SchedulerBindingsBridge;
use toolbox::shutdown::Shutdown;

pub(crate) fn spawn<S>(
    shutdown: Shutdown,
    session: agave_orchestrator::scheduler::ClientSession,
    mut scheduler: S,
) -> JoinHandle<()>
where
    S: Scheduler + Send,
{
    std::thread::Builder::new()
        .name("Scheduler".to_string())
        .spawn(move || {
            let mut bridge = SchedulerBindingsBridge::new(session);

            while !shutdown.is_shutdown() {
                scheduler.poll(&mut bridge);
            }
        })
        .unwrap()
}

pub(crate) trait Scheduler
where
    Self: Sized + 'static,
{
    type Meta: Copy;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>);
}

impl Scheduler for BatchScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for FifoScheduler {
    type Meta = ();

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for GreedyRevenueScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for GreedyThroughputScheduler {
    type Meta = PriorityId;

    fn poll(&mut self, bridge: &mut SchedulerBindingsBridge<Self::Meta>) {
        self.poll(bridge);
    }
}
