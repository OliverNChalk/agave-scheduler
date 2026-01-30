use agave_bridge::TransactionKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct PriorityId {
    pub(crate) priority: u64,
    pub(crate) cost: u64,
    pub(crate) key: TransactionKey,
}

impl PartialOrd for PriorityId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriorityId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority
            .cmp(&other.priority)
            .then_with(|| self.cost.cmp(&other.cost))
            .then_with(|| self.key.cmp(&other.key))
    }
}
