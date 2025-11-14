use std::ops::Deref;

use agave_scheduler_bindings::{SharablePubkeys, SharableTransactionRegion};
use rts_alloc::Allocator;
use slotmap::SlotMap;

#[derive(Debug, Default)]
pub(crate) struct TransactionMap(SlotMap<TransactionStateKey, TransactionState>);

impl TransactionMap {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self(SlotMap::with_capacity_and_key(cap))
    }

    pub(crate) fn insert(&mut self, shared: SharableTransactionRegion) -> TransactionStateKey {
        self.0.insert(TransactionState { shared, resolved: None })
    }

    /// Removes the transaction from the map & frees the underlying objects.
    ///
    /// # Safety
    ///
    /// - Caller must have passed exclusive ownership of objects on insert.
    /// - Caller must not have previously freed the underlying objects.
    ///
    /// # Panics
    ///
    /// - If the key has already been removed.
    pub(crate) unsafe fn remove(&mut self, allocator: &Allocator, key: TransactionStateKey) {
        let state = self.0.remove(key).unwrap();

        // SAFETY
        // - Caller must not have freed the offsets prior.
        unsafe {
            allocator.free_offset(state.shared.offset);
            if let Some(resolved) = state.resolved {
                allocator.free_offset(resolved.offset);
            }
        }
    }
}

impl Deref for TransactionMap {
    type Target = SlotMap<TransactionStateKey, TransactionState>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

slotmap::new_key_type! {
    pub(crate) struct TransactionStateKey;
}

#[derive(Debug)]
pub(crate) struct TransactionState {
    pub(crate) shared: SharableTransactionRegion,
    pub(crate) resolved: Option<SharablePubkeys>,
}
