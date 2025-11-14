use std::ops::{Index, IndexMut};

use agave_scheduler_bindings::SharableTransactionRegion;
use agave_scheduling_utils::pubkeys_ptr::PubkeysPtr;
use agave_scheduling_utils::transaction_ptr::TransactionPtr;
use agave_transaction_view::transaction_view::TransactionView;
use rts_alloc::Allocator;
use slotmap::SlotMap;
use solana_pubkey::Pubkey;

#[derive(Default)]
pub(crate) struct TransactionMap(SlotMap<TransactionStateKey, TransactionState>);

impl TransactionMap {
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self(SlotMap::with_capacity_and_key(cap))
    }

    pub(crate) fn insert(
        &mut self,
        shared: SharableTransactionRegion,
        view: TransactionView<true, TransactionPtr>,
    ) -> TransactionStateKey {
        self.0
            .insert(TransactionState { shared, view, resolved: None })
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
        // - Caller must not have freed the objects prior.
        unsafe {
            state.view.into_inner_data().free(allocator);
            if let Some(resolved) = state.resolved {
                resolved.free(allocator);
            }
        }
    }
}

impl Index<TransactionStateKey> for TransactionMap {
    type Output = TransactionState;

    fn index(&self, index: TransactionStateKey) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<TransactionStateKey> for TransactionMap {
    fn index_mut(&mut self, index: TransactionStateKey) -> &mut Self::Output {
        &mut self.0[index]
    }
}

slotmap::new_key_type! {
    pub(crate) struct TransactionStateKey;
}

pub(crate) struct TransactionState {
    pub(crate) shared: SharableTransactionRegion,
    pub(crate) view: TransactionView<true, TransactionPtr>,
    pub(crate) resolved: Option<PubkeysPtr>,
}

impl TransactionState {
    pub(crate) fn write_locks(&self) -> impl Iterator<Item = &Pubkey> {
        self.view
            .static_account_keys()
            .iter()
            .chain(self.resolved.as_ref().unwrap().as_slice().iter())
            .enumerate()
            .filter(|(i, _)| self.is_writable(*i as u8))
            .map(|(_, key)| key)
    }

    pub(crate) fn read_locks(&self) -> impl Iterator<Item = &Pubkey> {
        self.view
            .static_account_keys()
            .iter()
            .chain(self.resolved.as_ref().unwrap().as_slice().iter())
            .enumerate()
            .filter(|(i, _)| !self.is_writable(*i as u8))
            .map(|(_, key)| key)
    }

    fn is_writable(&self, index: u8) -> bool {
        if index >= self.view.num_static_account_keys() {
            let loaded_address_index = index.wrapping_sub(self.view.num_static_account_keys());
            loaded_address_index < self.view.total_writable_lookup_accounts() as u8
        } else {
            index
                < self
                    .view
                    .num_signatures()
                    .wrapping_sub(self.view.num_readonly_signed_static_accounts())
                || (index >= self.view.num_signatures()
                    && index
                        < (self.view.static_account_keys().len() as u8)
                            .wrapping_sub(self.view.num_readonly_unsigned_static_accounts()))
        }
    }
}
