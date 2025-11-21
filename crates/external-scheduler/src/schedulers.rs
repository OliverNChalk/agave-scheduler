use agave_bridge::SchedulerBindings;
use agave_schedulers::batch::BatchScheduler;
use agave_schedulers::fifo::FifoScheduler;
use agave_schedulers::greedy::GreedyScheduler;
use agave_schedulers::shared::PriorityId;

pub(crate) trait Scheduler {
    type Meta: Copy;

    fn new() -> Self;
    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>);
}

impl Scheduler for BatchScheduler {
    type Meta = PriorityId;

    fn new() -> Self {
        BatchScheduler::new()
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for FifoScheduler {
    type Meta = ();

    fn new() -> Self {
        FifoScheduler::new()
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for GreedyScheduler {
    type Meta = PriorityId;

    fn new() -> Self {
        GreedyScheduler::new()
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}
