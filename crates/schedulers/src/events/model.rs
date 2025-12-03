use chrono::{DateTime, Utc};
use serde::Serialize;
use solana_clock::Slot;
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
    Slot(SlotEvent),
}

#[derive(Debug, Default, Serialize)]
pub struct SlotEvent {
    pub is_leader: bool,
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
    fn slot_subject() {
        let event = Event::Slot(SlotEvent {
            is_leader: true,
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

        expect!["slot"].assert_eq(EventDiscriminants::from(&event).into());
    }

    #[test]
    fn slot_started() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 25,
            event: Event::Slot(SlotEvent {
                is_leader: true,
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

        // Assert.
        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 25,
              "type": "Slot",
              "is_leader": true,
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
    fn slot_finished() {
        let event = StampedEvent {
            timestamp: DateTime::UNIX_EPOCH,
            slot: 25,
            event: Event::Slot(SlotEvent {
                is_leader: true,
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

        // Assert.
        expect![[r#"
            {
              "timestamp": "1970-01-01T00:00:00Z",
              "slot": 25,
              "type": "Slot",
              "is_leader": true,
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
}
