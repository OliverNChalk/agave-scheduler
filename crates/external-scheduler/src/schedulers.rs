use std::thread::JoinHandle;

use agave_bridge::SchedulerBindings;
use agave_schedulers::batch::BatchScheduler;
use agave_schedulers::events::EventEmitter;
use agave_schedulers::fifo::FifoScheduler;
use agave_schedulers::greedy::GreedyScheduler;
use agave_schedulers::shared::PriorityId;

pub(crate) trait Scheduler
where
    Self: Sized + 'static,
{
    type Meta: Copy;

    fn new(events: EventEmitter) -> (Self, Vec<JoinHandle<()>>);
    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>);
}

impl Scheduler for BatchScheduler {
    type Meta = PriorityId;

    fn new(events: EventEmitter) -> (Self, Vec<JoinHandle<()>>) {
        // OLI: We should throw out this trait and just have a SchedulerConfig enum that
        // inside of it has BatchConfig (other schedulers can be unit). We should just
        // have the keypair injected into the batch config from env.
        BatchScheduler::new(Some(events), todo!(), todo!())
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for FifoScheduler {
    type Meta = ();

    fn new(_: EventEmitter) -> (Self, Vec<JoinHandle<()>>) {
        (FifoScheduler::new(), vec![])
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}

impl Scheduler for GreedyScheduler {
    type Meta = PriorityId;

    fn new(events: EventEmitter) -> (Self, Vec<JoinHandle<()>>) {
        (GreedyScheduler::new(Some(events)), vec![])
    }

    fn poll(&mut self, bridge: &mut SchedulerBindings<Self::Meta>) {
        self.poll(bridge);
    }
}
