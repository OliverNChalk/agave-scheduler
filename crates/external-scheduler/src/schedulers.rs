use std::thread::JoinHandle;

use agave_bridge::SchedulerBindings;
use agave_schedulers::batch::BatchScheduler;
use agave_schedulers::fifo::FifoScheduler;
use agave_schedulers::greedy::GreedyScheduler;
use agave_schedulers::shared::PriorityId;

pub(crate) trait Scheduler
where
    Self: Sized + 'static,
{
    type Meta: Copy;

    fn new() -> (Self, Vec<JoinHandle<()>>);
    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>);
}

impl Scheduler for BatchScheduler {
    type Meta = PriorityId;

    fn new() -> (Self, Vec<JoinHandle<()>>) {
        BatchScheduler::new()
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for FifoScheduler {
    type Meta = ();

    fn new() -> (Self, Vec<JoinHandle<()>>) {
        (FifoScheduler::new(), vec![])
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for GreedyScheduler {
    type Meta = PriorityId;

    fn new() -> (Self, Vec<JoinHandle<()>>) {
        (GreedyScheduler::new(), vec![])
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}
