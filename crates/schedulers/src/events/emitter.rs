use std::sync::atomic::{AtomicU64, Ordering};

use solana_clock::Slot;
use tokio::sync::mpsc;

use crate::events::{Event, StampedEvent};

#[derive(Debug, Clone)]
pub struct EventEmitter {
    ctx: &'static EventContext,
    tx: mpsc::Sender<StampedEvent>,
}

impl EventEmitter {
    pub const fn new(ctx: &'static EventContext, tx: mpsc::Sender<StampedEvent>) -> Self {
        EventEmitter { ctx, tx }
    }

    #[must_use]
    pub const fn ctx(&self) -> &'static EventContext {
        self.ctx
    }

    pub fn emit(&self, event: Event) {
        let timestamp = chrono::Utc::now();
        let slot = self.ctx.slot.load(Ordering::Relaxed);

        self.tx
            .try_send(StampedEvent { timestamp, slot, event })
            .unwrap();
    }
}

#[derive(Debug)]
pub struct EventContext {
    pub slot: AtomicU64,
}

impl EventContext {
    #[must_use]
    pub fn leak() -> &'static Self {
        Box::leak(Box::new(Self { slot: AtomicU64::new(0) }))
    }

    pub fn set(&self, slot: Slot) {
        self.slot.store(slot, Ordering::Relaxed);
    }
}
