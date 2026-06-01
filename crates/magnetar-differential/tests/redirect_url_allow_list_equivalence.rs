// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer 4 — tokio ↔ moonpool equivalence for the PIP-188
//! redirect-URL allow-list (MEDIUM-1 from the lookup multi-agent review).
//!
//! The rejection decision lives in `magnetar-proto::Connection`, which
//! both engines wrap behind a `Mutex<Connection>` via their respective
//! `ConnectionShared`. The user-visible event stream
//! (`Connection::poll_event`) must therefore be bit-identical between
//! the two engines step-by-step:
//!
//! 1. Both `Connection`s start `Uninitialized`.
//! 2. Both go through `begin_handshake` → `CommandConnected`.
//! 3. Both create a producer and observe `ProducerSuccess`.
//! 4. Both receive a `CommandTopicMigrated` whose advertised URL is outside the configured
//!    allow-list — both must surface `RedirectUrlRejected` with the same `source` + URLs and the
//!    same `is_connected()` posture afterwards.
//! 5. With no allow-list configured, both must surface `TopicMigrated` (pre-allow-list contract).
//!
//! No I/O on either side — the test drives the proto state machine
//! directly. The differential value is structural: a regression in
//! either engine's `Connection` wrapper that diverged the proto contract
//! would surface here.

#![forbid(unsafe_code)]

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, ConnectionEvent, CreateProducerRequest, ProducerHandle, RedirectUrlAllowList,
    encode_command, pb,
};

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-diff-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

fn topic_migrated_bytes(producer_handle: ProducerHandle, new_url: &str) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::TopicMigrated as i32,
        topic_migrated: Some(pb::CommandTopicMigrated {
            resource_id: producer_handle.0,
            resource_type: pb::command_topic_migrated::ResourceType::Producer as i32,
            broker_service_url: Some(new_url.to_owned()),
            broker_service_url_tls: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandTopicMigrated");
    buf
}

fn producer_success_bytes(request_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "diff-allowlist".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: None,
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode ProducerSuccess");
    buf
}

/// Walk both engines through identical wire input with the allow-list
/// blocking `attacker.example.com`. Assert step-by-step event parity.
#[test]
fn rejection_event_stream_is_byte_identical_across_engines() {
    let t0 = Instant::now();

    let config = ConnectionConfig {
        redirect_url_allow_list: Some(RedirectUrlAllowList::Hosts(vec![
            "broker.example.com".to_owned(),
        ])),
        ..ConnectionConfig::default()
    };

    let tokio_shared = magnetar_runtime_tokio::ConnectionShared::new(config.clone());
    let moonpool_shared = magnetar_runtime_moonpool::ConnectionShared::new(config);

    // (1) Both: begin handshake and drive to Connected.
    {
        let mut t = tokio_shared.inner.lock();
        let mut m = moonpool_shared.inner.lock();
        t.begin_handshake().expect("tokio handshake");
        m.begin_handshake().expect("moonpool handshake");
        t.handle_bytes(t0, &handshake_response_bytes())
            .expect("tokio Connected");
        m.handle_bytes(t0, &handshake_response_bytes())
            .expect("moonpool Connected");
        // Drain the Connected event on both engines.
        match (t.poll_event(), m.poll_event()) {
            (Some(ConnectionEvent::Connected { .. }), Some(ConnectionEvent::Connected { .. })) => {}
            (t_evt, m_evt) => panic!(
                "engines diverged on the first event after handshake — \
                 tokio={t_evt:?} moonpool={m_evt:?}"
            ),
        }
    }

    // (2) Both: open a producer.
    let req = CreateProducerRequest {
        topic: "persistent://public/default/diff-allowlist".to_owned(),
        ..Default::default()
    };
    let (t_handle, m_handle, t_rid, m_rid) = {
        let mut t = tokio_shared.inner.lock();
        let mut m = moonpool_shared.inner.lock();
        let t_rid = t.peek_next_request_id_for_test();
        let m_rid = m.peek_next_request_id_for_test();
        let t_handle = t.create_producer(req.clone());
        let m_handle = m.create_producer(req);
        (t_handle, m_handle, t_rid, m_rid)
    };
    assert_eq!(t_handle, m_handle, "handle allocation must agree");
    assert_eq!(t_rid, m_rid, "request id allocation must agree");

    // Ack the producer-open on both sides.
    {
        let mut t = tokio_shared.inner.lock();
        let mut m = moonpool_shared.inner.lock();
        t.handle_bytes(t0, &producer_success_bytes(t_rid))
            .expect("tokio ProducerSuccess");
        m.handle_bytes(t0, &producer_success_bytes(m_rid))
            .expect("moonpool ProducerSuccess");
        // Drain the ProducerReady event so the next event is the
        // migration's rejection.
        let _ = t.poll_event();
        let _ = m.poll_event();
    }

    // (3) Both: feed the malicious migration command.
    {
        let mut t = tokio_shared.inner.lock();
        let mut m = moonpool_shared.inner.lock();
        let frame = topic_migrated_bytes(t_handle, "pulsar://attacker.example.com:6650");
        t.handle_bytes(t0, &frame).expect("tokio apply migration");
        m.handle_bytes(t0, &frame)
            .expect("moonpool apply migration");
    }

    // (4) Both engines must surface the same `RedirectUrlRejected`.
    let t_evt = tokio_shared.inner.lock().poll_event();
    let m_evt = moonpool_shared.inner.lock().poll_event();
    match (&t_evt, &m_evt) {
        (
            Some(ConnectionEvent::RedirectUrlRejected {
                source: t_source,
                broker_service_url: t_url,
                broker_service_url_tls: t_tls,
            }),
            Some(ConnectionEvent::RedirectUrlRejected {
                source: m_source,
                broker_service_url: m_url,
                broker_service_url_tls: m_tls,
            }),
        ) => {
            assert_eq!(t_source, m_source, "source label must match across engines");
            assert_eq!(t_url, m_url, "rejected URL must match across engines");
            assert_eq!(t_tls, m_tls, "rejected TLS URL must match across engines");
            assert_eq!(*t_source, "CommandTopicMigrated");
            assert_eq!(t_url.as_deref(), Some("pulsar://attacker.example.com:6650"));
        }
        (t_evt, m_evt) => {
            panic!("engines diverged on the rejection event — tokio={t_evt:?} moonpool={m_evt:?}")
        }
    }

    // (5) Both connections must still be live (no reconnect fired, no
    // credentials were replayed against the attacker host).
    assert_eq!(
        tokio_shared.inner.lock().is_connected(),
        moonpool_shared.inner.lock().is_connected(),
        "is_connected() must agree post-rejection",
    );
    assert!(
        tokio_shared.inner.lock().is_connected(),
        "tokio connection must stay live after rejection",
    );
}

/// Companion case: with no allow-list, both engines must surface
/// `TopicMigrated` (the pre-existing behaviour ADR-0018 documents).
#[test]
fn default_permissive_event_stream_is_byte_identical_across_engines() {
    let t0 = Instant::now();
    let config = ConnectionConfig::default();
    assert!(config.redirect_url_allow_list.is_none());

    let tokio_shared = magnetar_runtime_tokio::ConnectionShared::new(config.clone());
    let moonpool_shared = magnetar_runtime_moonpool::ConnectionShared::new(config);

    {
        let mut t = tokio_shared.inner.lock();
        let mut m = moonpool_shared.inner.lock();
        t.begin_handshake().expect("tokio handshake");
        m.begin_handshake().expect("moonpool handshake");
        t.handle_bytes(t0, &handshake_response_bytes())
            .expect("tokio Connected");
        m.handle_bytes(t0, &handshake_response_bytes())
            .expect("moonpool Connected");
        let _ = t.poll_event();
        let _ = m.poll_event();
    }

    let req = CreateProducerRequest {
        topic: "persistent://public/default/diff-no-allowlist".to_owned(),
        ..Default::default()
    };
    let (t_handle, m_handle, t_rid, m_rid) = {
        let mut t = tokio_shared.inner.lock();
        let mut m = moonpool_shared.inner.lock();
        let t_rid = t.peek_next_request_id_for_test();
        let m_rid = m.peek_next_request_id_for_test();
        (
            t.create_producer(req.clone()),
            m.create_producer(req),
            t_rid,
            m_rid,
        )
    };
    assert_eq!(t_handle, m_handle);
    assert_eq!(t_rid, m_rid);

    {
        let mut t = tokio_shared.inner.lock();
        let mut m = moonpool_shared.inner.lock();
        t.handle_bytes(t0, &producer_success_bytes(t_rid))
            .expect("tokio ack");
        m.handle_bytes(t0, &producer_success_bytes(m_rid))
            .expect("moonpool ack");
        let _ = t.poll_event();
        let _ = m.poll_event();
    }

    {
        let mut t = tokio_shared.inner.lock();
        let mut m = moonpool_shared.inner.lock();
        let frame = topic_migrated_bytes(t_handle, "pulsar://new-broker:6650");
        t.handle_bytes(t0, &frame).expect("tokio migration");
        m.handle_bytes(t0, &frame).expect("moonpool migration");
    }

    let t_evt = tokio_shared.inner.lock().poll_event();
    let m_evt = moonpool_shared.inner.lock().poll_event();
    match (&t_evt, &m_evt) {
        (
            Some(ConnectionEvent::TopicMigrated {
                producer: t_prod,
                broker_service_url: t_url,
                ..
            }),
            Some(ConnectionEvent::TopicMigrated {
                producer: m_prod,
                broker_service_url: m_url,
                ..
            }),
        ) => {
            assert_eq!(t_prod, m_prod);
            assert_eq!(t_url, m_url);
            assert_eq!(t_url.as_deref(), Some("pulsar://new-broker:6650"));
        }
        (t_evt, m_evt) => panic!(
            "engines diverged on the default-permissive migration event — \
             tokio={t_evt:?} moonpool={m_evt:?}"
        ),
    }
}
