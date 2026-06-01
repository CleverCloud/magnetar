// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer 3 — moonpool integration test for the PIP-188
//! redirect-URL allow-list (MEDIUM-1 from the lookup multi-agent review).
//! Mirror of
//! `magnetar-runtime-tokio/tests/topic_migrated_allow_list.rs`.
//!
//! Threat-model + design rationale are documented in
//! [`RedirectUrlAllowList`](magnetar_proto::RedirectUrlAllowList) and in
//! ADR-0018's "Redirect URL allow-list (2026-06-01)" section.
//!
//! ## Strategy
//!
//! Drive the proto state machine through the moonpool engine's
//! `ConnectionShared` wrapper via synthetic frame injection (mirrors the
//! `pip_188_migrate_then_migrate_again` test).
//!
//! 1. Set `redirect_url_allow_list = Some(Hosts(["broker.example.com"]))`.
//! 2. Handshake + open producer.
//! 3. Feed a `CommandTopicMigrated` whose URL is **outside** the set.
//! 4. Assert the state machine surfaces `RedirectUrlRejected` instead of `TopicMigrated`. The
//!    supervised reconnect arm in the moonpool driver loop swallows the rejection without raising
//!    an `EngineError`, so the proto layer's `is_connected()` invariant stays `true` after the
//!    rejection.
//!
//! The companion `default_permissive` test pins the regression contract:
//! with `redirect_url_allow_list = None` the state machine still emits
//! `TopicMigrated` exactly as ADR-0018 documents.

mod common;

use std::time::Instant;

use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, CreateProducerRequest, RedirectUrlAllowList,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::{handshake_response_bytes, topic_migrated_bytes};

#[test]
fn topic_migrated_to_disallowed_url_surfaces_rejection_event() {
    let t0 = Instant::now();

    let config = ConnectionConfig {
        redirect_url_allow_list: Some(RedirectUrlAllowList::Hosts(vec![
            "broker.example.com".to_owned(),
        ])),
        ..ConnectionConfig::default()
    };
    let shared = ConnectionShared::new(config);

    // Handshake.
    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected");
        let _ = conn.poll_event();
    }

    // Open a producer and ack it so the next event we observe is the
    // migration (or its rejection).
    let req = CreateProducerRequest {
        topic: "persistent://public/default/redirect-allowlist-moonpool".to_owned(),
        ..Default::default()
    };
    let (handle, open_request_id) = {
        let mut conn = shared.inner.lock();
        let id = conn.peek_next_request_id_for_test();
        let handle = conn.create_producer(req);
        (handle, id)
    };
    {
        use bytes::BytesMut;
        use magnetar_proto::{encode_command, pb};
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: open_request_id,
                producer_name: "moonpool-allowlist-test".to_owned(),
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
        conn.handle_bytes(t0, &buf).expect("ProducerSuccess");
        let _ = conn.poll_event();
    }

    // Feed a `CommandTopicMigrated` advertising the attacker host.
    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(
            t0,
            &topic_migrated_bytes(handle, Some("pulsar://attacker.example.com:6650"), None),
        )
        .expect("apply migration");
    }

    // The proto state machine must surface the rejection (no
    // `TopicMigrated`) so the supervised reconnect arm in the driver
    // stays asleep.
    let evt = shared.inner.lock().poll_event().expect("must have event");
    match evt {
        ConnectionEvent::RedirectUrlRejected {
            source,
            broker_service_url,
            broker_service_url_tls,
        } => {
            assert_eq!(source, "CommandTopicMigrated");
            assert_eq!(
                broker_service_url.as_deref(),
                Some("pulsar://attacker.example.com:6650")
            );
            assert!(broker_service_url_tls.is_none());
        }
        ConnectionEvent::TopicMigrated { .. } => panic!(
            "RedirectUrlAllowList::Hosts(['broker.example.com']) must REJECT \
             pulsar://attacker.example.com:6650 — surfaced TopicMigrated instead",
        ),
        other => panic!("unexpected event: {other:?}"),
    }

    // The connection is still live — no reconnect, no auth replay.
    assert!(
        shared.inner.lock().is_connected(),
        "rejection must not tear the connection down",
    );
}

#[test]
fn topic_migrated_with_no_allow_list_preserves_pre_existing_behaviour() {
    // Mirror of the tokio `topic_migrated_with_no_allow_list_*`
    // companion: with the default config (no allow-list), the state
    // machine still emits `TopicMigrated`. Locks in the
    // default-permissive contract ADR-0018 documents.
    let t0 = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());

    {
        let mut conn = shared.inner.lock();
        conn.begin_handshake().expect("handshake");
        conn.handle_bytes(t0, &handshake_response_bytes())
            .expect("Connected");
        let _ = conn.poll_event();
    }

    let req = CreateProducerRequest {
        topic: "persistent://public/default/redirect-no-allowlist-moonpool".to_owned(),
        ..Default::default()
    };
    let (handle, open_request_id) = {
        let mut conn = shared.inner.lock();
        let id = conn.peek_next_request_id_for_test();
        let handle = conn.create_producer(req);
        (handle, id)
    };
    {
        use bytes::BytesMut;
        use magnetar_proto::{encode_command, pb};
        let success = pb::BaseCommand {
            r#type: pb::base_command::Type::ProducerSuccess as i32,
            producer_success: Some(pb::CommandProducerSuccess {
                request_id: open_request_id,
                producer_name: "moonpool-default-test".to_owned(),
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
        conn.handle_bytes(t0, &buf).expect("ProducerSuccess");
        let _ = conn.poll_event();
    }

    {
        let mut conn = shared.inner.lock();
        conn.handle_bytes(
            t0,
            &topic_migrated_bytes(handle, Some("pulsar://new-broker:6650"), None),
        )
        .expect("apply migration");
    }

    let evt = shared.inner.lock().poll_event().expect("must have event");
    match evt {
        ConnectionEvent::TopicMigrated {
            producer,
            broker_service_url,
            ..
        } => {
            assert_eq!(producer, Some(handle));
            assert_eq!(
                broker_service_url.as_deref(),
                Some("pulsar://new-broker:6650"),
            );
        }
        ConnectionEvent::RedirectUrlRejected { .. } => panic!(
            "default `redirect_url_allow_list = None` must NOT reject any URL — \
             ADR-0018 default-permissive contract"
        ),
        other => panic!("unexpected event: {other:?}"),
    }
}
