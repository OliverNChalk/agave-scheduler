use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_with::serde_as;
use solana_clock::Slot;
use solana_signature::Signature;
use strum::EnumDiscriminants;

#[derive(Debug, Serialize)]
pub struct StampedEvent {
    pub timestamp: DateTime<Utc>,
    pub slot: Slot,
    #[serde(flatten)]
    pub event: Event,
}

#[derive(Debug, Serialize, EnumDiscriminants)]
#[strum_discriminants(derive(strum::IntoStaticStr))]
#[strum_discriminants(strum(serialize_all = "kebab-case"))]
#[serde(tag = "type")]
pub enum Event {
    SlotStart,
    LeaderReady,
    SlotStats(SlotStatsEvent),
    Transaction(TransactionEvent),
}

#[serde_as]
#[derive(Debug, Serialize)]
pub struct TransactionEvent {
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub signature: Signature,
    #[serde(rename = "tx_slot")]
    pub slot: Slot,
    pub priority: u64,
    #[serde(flatten)]
    pub action: TransactionAction,
}

#[derive(Debug, Serialize)]
#[serde(tag = "action")]
pub enum TransactionAction {
    Ingest { source: TransactionSource, bundle: Option<Arc<String>> },
    CheckOk,
    CheckErr { reason: CheckFailure },
    ExecuteReq,
    ExecuteOk,
    ExecuteErr { reason: u32 },
    Evict { reason: EvictReason },
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum TransactionSource {
    Tpu,
    Jito,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum CheckFailure {
    ParseOrSanitize,
    AccountResolution,
    StatusCheck,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum EvictReason {
    UncheckedCapacity,
    CheckedCapacity,
}

#[derive(Debug, Default, Serialize)]
pub struct SlotStatsEvent {
    pub was_leader: bool,
    pub ingest_tpu_ok: u64,
    pub ingest_tpu_err: u64,
    pub ingest_tpu_evict: u64,
    pub ingest_custom_ok: u64,
    pub ingest_custom_err: u64,
    pub worker_unprocessed: u64,
    pub check_requested: u64,
    pub check_ok: u64,
    pub check_err: u64,
    pub check_evict: u64,
    pub execute_requested: u64,
    pub execute_ok: u64,
    pub execute_err: u64,
}

#[cfg(test)]
mod tests {
    use expect_test::expect;

    use super::*;

    #[test]
    fn slot_start_subject() {
        let event = Event::SlotStart;

        expect!["slot-start"].assert_eq(EventDiscriminants::from(&event).into());
    }

    #[test]
    fn slot_start_event() {
        let event =
            StampedEvent { timestamp: DateTime::UNIX_EPOCH, slot: 100, event: Event::SlotStart };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "SlotStart"
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn leader_ready_subject() {
        let event = Event::LeaderReady;

        expect!["leader-ready"].assert_eq(EventDiscriminants::from(&event).into());
    }

    #[test]
    fn leader_ready_event() {
        let event =
            StampedEvent { timestamp: DateTime::UNIX_EPOCH, slot: 105, event: Event::LeaderReady };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 105,
              "type": "LeaderReady"
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn slot_stats_subject() {
        let event = Event::SlotStats(SlotStatsEvent {
            was_leader: true,
            ingest_tpu_ok: 7,
            ingest_tpu_err: 3,
            ingest_tpu_evict: 2,
            ingest_custom_ok: 5,
            ingest_custom_err: 1,
            worker_unprocessed: 0,
            check_requested: 16,
            check_ok: 12,
            check_err: 4,
            check_evict: 1,
            execute_requested: 7,
            execute_ok: 5,
            execute_err: 2,
        });

        expect!["slot-stats"].assert_eq(EventDiscriminants::from(&event).into());
    }

    #[test]
    fn slot_stats_event() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 25,
            event: Event::SlotStats(SlotStatsEvent {
                was_leader: true,
                ingest_tpu_ok: 7,
                ingest_tpu_err: 3,
                ingest_tpu_evict: 2,
                ingest_custom_ok: 5,
                ingest_custom_err: 1,
                worker_unprocessed: 0,
                check_requested: 16,
                check_ok: 12,
                check_err: 4,
                check_evict: 1,
                execute_requested: 7,
                execute_ok: 5,
                execute_err: 2,
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 25,
              "type": "SlotStats",
              "was_leader": true,
              "ingest_tpu_ok": 7,
              "ingest_tpu_err": 3,
              "ingest_tpu_evict": 2,
              "ingest_custom_ok": 5,
              "ingest_custom_err": 1,
              "worker_unprocessed": 0,
              "check_requested": 16,
              "check_ok": 12,
              "check_err": 4,
              "check_evict": 1,
              "execute_requested": 7,
              "execute_ok": 5,
              "execute_err": 2
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn transaction_subject() {
        let event = Event::Transaction(TransactionEvent {
            signature: Signature::from([1; 64]),
            slot: 100,
            priority: 5000,
            action: TransactionAction::Ingest { source: TransactionSource::Tpu, bundle: None },
        });

        expect!["transaction"].assert_eq(EventDiscriminants::from(&event).into());
    }

    #[test]
    fn transaction_ingest_tpu() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 100,
            event: Event::Transaction(TransactionEvent {
                signature: Signature::from([1; 64]),
                slot: 100,
                priority: 5000,
                action: TransactionAction::Ingest { source: TransactionSource::Tpu, bundle: None },
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "Transaction",
              "signature": "2AXDGYSE4f2sz7tvMMzyHvUfcoJmxudvdhBcmiUSo6ijwfYmfZYsKRxboQMPh3R4kUhXRVdtSXFXMheka4Rc4P2",
              "tx_slot": 100,
              "priority": 5000,
              "action": "Ingest",
              "source": "Tpu",
              "bundle": null
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn transaction_ingest_jito_bundle() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 100,
            event: Event::Transaction(TransactionEvent {
                signature: Signature::from([2; 64]),
                slot: 100,
                priority: 10000,
                action: TransactionAction::Ingest {
                    source: TransactionSource::Jito,
                    bundle: Some(Arc::new("bundle-abc123".to_string())),
                },
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "Transaction",
              "signature": "3L3RY5sT8K4kyEnqhizwaqxLEbcYvpGrGPNEYRwtbCSUtL6YL86jdrvCbohnP5q8VxQ3qzGmt3W3iQJW97rD7m3",
              "tx_slot": 100,
              "priority": 10000,
              "action": "Ingest",
              "source": "Jito",
              "bundle": "bundle-abc123"
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn transaction_check_ok() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 100,
            event: Event::Transaction(TransactionEvent {
                signature: Signature::from([1; 64]),
                slot: 100,
                priority: 5000,
                action: TransactionAction::CheckOk,
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "Transaction",
              "signature": "2AXDGYSE4f2sz7tvMMzyHvUfcoJmxudvdhBcmiUSo6ijwfYmfZYsKRxboQMPh3R4kUhXRVdtSXFXMheka4Rc4P2",
              "tx_slot": 100,
              "priority": 5000,
              "action": "CheckOk"
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn transaction_check_err() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 100,
            event: Event::Transaction(TransactionEvent {
                signature: Signature::from([1; 64]),
                slot: 100,
                priority: 5000,
                action: TransactionAction::CheckErr { reason: CheckFailure::ParseOrSanitize },
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "Transaction",
              "signature": "2AXDGYSE4f2sz7tvMMzyHvUfcoJmxudvdhBcmiUSo6ijwfYmfZYsKRxboQMPh3R4kUhXRVdtSXFXMheka4Rc4P2",
              "tx_slot": 100,
              "priority": 5000,
              "action": "CheckErr",
              "reason": "ParseOrSanitize"
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn transaction_execute_req() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 100,
            event: Event::Transaction(TransactionEvent {
                signature: Signature::from([1; 64]),
                slot: 100,
                priority: 5000,
                action: TransactionAction::ExecuteReq,
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "Transaction",
              "signature": "2AXDGYSE4f2sz7tvMMzyHvUfcoJmxudvdhBcmiUSo6ijwfYmfZYsKRxboQMPh3R4kUhXRVdtSXFXMheka4Rc4P2",
              "tx_slot": 100,
              "priority": 5000,
              "action": "ExecuteReq"
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn transaction_execute_ok() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 100,
            event: Event::Transaction(TransactionEvent {
                signature: Signature::from([1; 64]),
                slot: 100,
                priority: 5000,
                action: TransactionAction::ExecuteOk,
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "Transaction",
              "signature": "2AXDGYSE4f2sz7tvMMzyHvUfcoJmxudvdhBcmiUSo6ijwfYmfZYsKRxboQMPh3R4kUhXRVdtSXFXMheka4Rc4P2",
              "tx_slot": 100,
              "priority": 5000,
              "action": "ExecuteOk"
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn transaction_execute_err() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 100,
            event: Event::Transaction(TransactionEvent {
                signature: Signature::from([1; 64]),
                slot: 100,
                priority: 5000,
                action: TransactionAction::ExecuteErr { reason: 42 },
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "Transaction",
              "signature": "2AXDGYSE4f2sz7tvMMzyHvUfcoJmxudvdhBcmiUSo6ijwfYmfZYsKRxboQMPh3R4kUhXRVdtSXFXMheka4Rc4P2",
              "tx_slot": 100,
              "priority": 5000,
              "action": "ExecuteErr",
              "reason": 42
            }"#]]
        .assert_eq(&event);
    }

    #[test]
    fn transaction_evict() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 100,
            event: Event::Transaction(TransactionEvent {
                signature: Signature::from([1; 64]),
                slot: 100,
                priority: 5000,
                action: TransactionAction::Evict { reason: EvictReason::CheckedCapacity },
            }),
        };
        let event = serde_json::to_string_pretty(&event).unwrap();

        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 100,
              "type": "Transaction",
              "signature": "2AXDGYSE4f2sz7tvMMzyHvUfcoJmxudvdhBcmiUSo6ijwfYmfZYsKRxboQMPh3R4kUhXRVdtSXFXMheka4Rc4P2",
              "tx_slot": 100,
              "priority": 5000,
              "action": "Evict",
              "reason": "CheckedCapacity"
            }"#]]
        .assert_eq(&event);
    }
}
