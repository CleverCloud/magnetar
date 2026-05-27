// SPDX-License-Identifier: Apache-2.0

// Chaos scenarios value a single readable step-by-step `fn`. Splitting
// these into sub-helpers would obscure the synthetic frame sequence the
// test pins. We accept the line count.
#![allow(clippy::too_many_lines)]

//! Chaos scenario: PIP-188 broker migration where the broker tells us to
//! reconnect to broker-B; on the freshly-handshaked session, broker-B
//! immediately sends a second `CommandTopicMigrated` redirecting us back
//! to broker-A. The state machine must honour both migration commands,
//! the supervised reconnect path must dial both targets in order, and
//! the consumer must end up live on broker-A (the final target).
//!
//! Why this is moonpool territory: `testcontainers` can simulate one
//! `TOPIC_MIGRATED` via the broker's namespace policy machinery, but
//! sequencing two back-to-back migrations across distinct sockets requires
//! a scripted broker — exactly what synthetic frame injection provides.
//!
//! ## Shape
//!
//! 1. Complete the handshake on broker-A.
//! 2. Open a producer; ack the open round-trip.
//! 3. Feed back `CommandTopicMigrated` redirecting the producer to broker-B. Confirm the state
//!    machine emits a [`ConnectionEvent::TopicMigrated`] event with broker-B's URL.
//! 4. Simulate the supervisor: `reset` the connection, `begin_handshake` on the new socket, accept
//!    broker-B's `CommandConnected`. The pending-rebuild flag is set so the rebuild path reissues
//!    `CommandProducer` on the new session (the assertion is that `rebuild_producers` returns the
//!    live handle).
//! 5. Feed back a second `CommandTopicMigrated` from broker-B telling us to migrate to broker-A.
//!    Same flow — confirm the event surfaces and rebuild fires again.
//! 6. Final handshake against the (returned-to) broker-A; producer is live on the original target
//!    with the same handle.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, CreateProducerRequest, encode_command, pb,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::{handshake_response_bytes, topic_migrated_bytes};

const BROKER_A: &str = "pulsar://broker-a:6650";
const BROKER_B: &str = "pulsar://broker-b:6650";

#[test]
fn back_to_back_topic_migrations_settle() {
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());

    // === Session 1: handshake on broker-A, open a producer.
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake A");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on A");
        // Drain the Connected event so the next poll_event surfaces the
        // first migration cleanly.
        let _ = conn.poll_event();
    }
    let req = CreateProducerRequest {
        topic: "persistent://public/default/migrate-twice".to_owned(),
        ..Default::default()
    };
    let (handle, open_request_id) = {
        let mut conn = shared.inner.lock();
        let id = conn.peek_next_request_id_for_test();
        let handle = conn.create_producer(req);
        (handle, id)
    };
    {
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: open_request_id,
                producer_name: "magnetar-test-migrate".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &success).expect("encode success");
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &buf).expect("ProducerSuccess on A");
        let _ = conn.poll_event();
    }

    // === Migration #1: broker-A → broker-B.
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &topic_migrated_bytes(handle, Some(BROKER_B), None))
            .expect("apply migration A→B");
        let evt = conn.poll_event().expect("migration event A→B");
        match evt {
            ConnectionEvent::TopicMigrated {
                producer,
                broker_service_url,
                ..
            } => {
                assert_eq!(producer, Some(handle));
                assert_eq!(broker_service_url.as_deref(), Some(BROKER_B));
            }
            other => panic!("expected TopicMigrated A→B, got {other:?}"),
        }
    }

    // Supervisor's recovery for migration #1: reset + re-handshake on
    // broker-B's socket. After Connected lands, the rebuild path fires
    // (the supervisor sets `pending_rebuild` before the new socket
    // handshakes; the driver loop trips it on the first Connected).
    {
        let mut conn = shared.inner.lock();
        conn.reset();
        conn.begin_handshake().expect("re-handshake on B");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on B");
        let _ = conn.poll_event();
    }
    {
        // Rebuild the producer on the new session. The proto layer reissues
        // a fresh `CommandProducer` on the wire.
        let mut conn = shared.inner.lock();
        let rebuilt_request_ids = conn.rebuild_producers();
        assert_eq!(
            rebuilt_request_ids.len(),
            1,
            "rebuild_producers must reissue exactly one CommandProducer on the new session"
        );
        let new_open_request_id = rebuilt_request_ids[0];
        // Drain the rebuilt outbound bytes so the next assertion isolates
        // the second migration's wire activity.
        let _ = conn.poll_transmit();
        // Ack the new producer open.
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: new_open_request_id.0,
                producer_name: "magnetar-test-migrate".to_owned(),
                last_sequence_id: Some(-1),
                schema_version: None,
                topic_epoch: None,
                producer_ready: Some(true),
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_command(&mut buf, &success).expect("encode success B");
        conn.handle_bytes(t0, &buf).expect("ProducerSuccess on B");
        let _ = conn.poll_event();
    }

    // === Migration #2: broker-B → broker-A.
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(t0, &topic_migrated_bytes(handle, Some(BROKER_A), None))
            .expect("apply migration B→A");
        let evt = conn.poll_event().expect("migration event B→A");
        match evt {
            ConnectionEvent::TopicMigrated {
                producer,
                broker_service_url,
                ..
            } => {
                assert_eq!(producer, Some(handle));
                assert_eq!(broker_service_url.as_deref(), Some(BROKER_A));
            }
            other => panic!("expected TopicMigrated B→A, got {other:?}"),
        }
    }

    // Supervisor's recovery for migration #2: dial broker-A again and
    // settle. The producer's handle survives across both migrations —
    // that's the user-visible contract: `Producer` handles do not
    // reshuffle, even across multiple back-to-back migrations.
    let epoch_pre_b_to_a = shared.inner.lock().session_epoch();
    {
        let mut conn = shared.inner.lock();
        conn.reset();
        conn.begin_handshake().expect("re-handshake on A (return)");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected on A again");
        let _ = conn.poll_event();
    }
    let epoch_post_b_to_a = shared.inner.lock().session_epoch();
    assert_eq!(
        epoch_post_b_to_a,
        epoch_pre_b_to_a.wrapping_add(1),
        "the second reset must bump the session epoch one more time",
    );

    // Final rebuild on broker-A's socket.
    {
        let mut conn = shared.inner.lock();
        let rebuilt = conn.rebuild_producers();
        assert_eq!(
            rebuilt.len(),
            1,
            "the producer must survive two migrations and still rebuild on the final session"
        );
    }

    // Sanity: the connection is live (Connected, not Failed/Closed) on
    // the final broker-A session.
    let conn = shared.inner.lock();
    assert!(
        conn.is_connected(),
        "the connection must end up Connected after two back-to-back migrations"
    );
}
