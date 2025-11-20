use std::marker::PhantomData;

use agave_scheduler_bindings::ProgressMessage;
use solana_transaction::versioned::VersionedTransaction;

use crate::{
    Bridge, KeyedTransactionMeta, RuntimeState, ScheduleBatch, TransactionId, TransactionState,
    TxDecision, Worker, WorkerResponse,
};

#[derive(Debug)]
pub struct TestBridge<M>(PhantomData<M>);

impl<M> TestBridge<M> {
    pub fn queue_tpu(&mut self, tx: &VersionedTransaction) {
        todo!()
    }

    pub fn queue_response(
        &mut self,
        batch: ScheduleBatch<Vec<KeyedTransactionMeta<M>>>,
        response: WorkerResponse<'_, M>,
    ) {
        todo!()
    }

    pub fn pop_schedule(&mut self) -> Option<ScheduleBatch<Vec<KeyedTransactionMeta<M>>>> {
        todo!()
    }
}

impl<M> Default for TestBridge<M> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<M> Bridge for TestBridge<M> {
    type Worker = TestWorker;
    type Meta = M;

    fn runtime(&self) -> &RuntimeState {
        todo!()
    }

    fn progress(&self) -> &ProgressMessage {
        todo!()
    }

    fn worker_count(&self) -> usize {
        todo!()
    }

    fn worker(&mut self, id: usize) -> &mut Self::Worker {
        todo!()
    }

    fn tx(&self, key: TransactionId) -> &TransactionState {
        todo!()
    }

    fn tx_drop(&mut self, key: TransactionId) {
        todo!()
    }

    fn drain_progress(&mut self) {
        todo!()
    }

    fn tpu_len(&mut self) -> usize {
        todo!()
    }

    fn tpu_drain(
        &mut self,
        cb: impl FnMut(&mut Self, TransactionId) -> TxDecision,
        max_count: usize,
    ) {
        todo!()
    }

    fn pop_worker(
        &mut self,
        worker: usize,
        cb: impl FnMut(&mut Self, WorkerResponse<'_, Self::Meta>) -> TxDecision,
    ) -> bool {
        todo!()
    }

    fn schedule(&mut self, batch: ScheduleBatch<&[KeyedTransactionMeta<M>]>) {
        todo!()
    }
}

pub struct TestWorker;

impl Worker for TestWorker {
    fn len(&mut self) -> usize {
        todo!()
    }

    fn rem(&mut self) -> usize {
        todo!()
    }
}
