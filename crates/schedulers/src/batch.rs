use agave_bridge::Bridge;

pub struct BatchScheduler {}

impl BatchScheduler {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    pub fn poll<B>(&mut self, bridge: &mut B)
    where
        B: Bridge<Meta = ()>,
    {
    }
}
