// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0093 scalable-topic integration — moonpool engine.
//!
//! **Experimental.** Drives `magnetar_proto::Connection` directly with
//! synthetic broker frames so the same wire trace exercises both engines
//! (the tokio mirror at
//! `magnetar-runtime-tokio/tests/scalable_topic.rs` runs the identical
//! script). The engine-level `Client::scalable_topic_lookup` /
//! `close_scalable_topic_session` / `next_scalable_event` are thin delegates
//! over the sans-io `Connection` entries these tests touch — no real socket, no
//! provider plumbing required, matching the `shadow_topic.rs` pattern.
//!
//! The frames here are ordinary `BaseCommand`s: since ADR-0093 the client
//! speaks the upstream PIP-460 surface vendored from Pulsar 5.0.0-M1, so these
//! tests encode exactly what a real broker sends.
//!
//! Parity required by ADR-0024: the test count must match the tokio side
//! 1:1 (`cargo xtask check-runtime-test-parity`).

#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]
#![cfg(feature = "scalable-topics")]

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig, ConnectionEvent, SegmentId, pb};

/// A `Connected` handshake frame, optionally advertising the PIP-460
/// capability. `false` models a Pulsar 4.x broker.
fn connected_frame(scalable: bool) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags {
                supports_scalable_topics: scalable.then_some(true),
                ..pb::FeatureFlags::default()
            }),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode Connected");
    buf
}

fn connected_conn_with(scalable: bool) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(Instant::now(), &connected_frame(scalable))
        .expect("connected");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    conn
}

fn connected_conn() -> Connection {
    connected_conn_with(true)
}

fn seg(id: u64, start: u32, end: u32, parents: &[u64]) -> pb::SegmentInfoProto {
    pb::SegmentInfoProto {
        segment_id: id,
        hash_start: start,
        hash_end: end,
        state: pb::SegmentState::Active as i32,
        parent_ids: parents.to_vec(),
        child_ids: Vec::new(),
        created_at_epoch: 0,
        sealed_at_epoch: None,
        created_at_ms: 0,
        sealed_at_ms: None,
        legacy_topic_name: None,
    }
}

/// Encode a broker→client `CommandScalableTopicUpdate` carrying a whole layout.
fn layout_frame(session_id: u64, epoch: u64, segments: Vec<pb::SegmentInfoProto>) -> BytesMut {
    let segment_brokers = segments
        .iter()
        .map(|s| pb::SegmentBrokerAddress {
            segment_id: s.segment_id,
            broker_url: format!("pulsar://seg{}:6650", s.segment_id),
            broker_url_tls: None,
        })
        .collect();
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: Some(pb::ScalableTopicDag {
                epoch,
                segments,
                segment_brokers,
                controller_broker_url: Some("pulsar://controller:6650".to_owned()),
                controller_broker_url_tls: None,
            }),
            error: None,
            message: None,
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode update");
    buf
}

/// (c) #1 — `topic://` URL parsing parity with the sibling engine.
#[test]
fn scalable_topic_url_parsing() {
    assert!(magnetar_runtime_moonpool::is_scalable_topic_url(
        "topic://public/default/scaled"
    ));
    assert!(!magnetar_runtime_moonpool::is_scalable_topic_url(
        "persistent://public/default/regular"
    ));
    assert!(!magnetar_runtime_moonpool::is_scalable_topic_url(
        "non-persistent://public/default/np"
    ));
}

/// (c) #2 — happy path: the lookup opens the session and the first layout
/// resolves it, carrying the canonical topic identity and the placement joined
/// from the parallel address list.
#[test]
fn stream_consumer_happy_path_against_fake_broker() {
    let mut conn = connected_conn();
    let session_id = conn
        .open_scalable_topic_session("topic://public/default/scaled")
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();

    let buf = layout_frame(
        session_id,
        1,
        vec![seg(1, 0, 32_768, &[]), seg(2, 32_768, 65_536, &[])],
    );
    conn.handle_bytes(Instant::now(), &buf).expect("layout");

    let mut resolved = None;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ScalableTopicLookupResolved {
            segments,
            controller_broker_url,
            resolved_topic_name,
            epoch,
            ..
        } = ev
        {
            resolved = Some((segments, controller_broker_url, resolved_topic_name, epoch));
        }
    }
    let (segments, url, resolved_name, epoch) = resolved.expect("lookup resolved");
    assert_eq!(segments.len(), 2);
    assert_eq!(epoch, 1);
    assert_eq!(url.as_deref(), Some("pulsar://controller:6650"));
    assert_eq!(
        resolved_name.as_deref(),
        Some("topic://public/default/scaled")
    );
    assert_eq!(
        segments[0].broker_url.as_deref(),
        Some("pulsar://seg1:6650")
    );

    let snap = conn.dag_snapshot(session_id).expect("session open");
    assert_eq!(snap.len(), 2);
    assert!(snap.iter().any(|d| d.segment_id == SegmentId(1)));
}

/// (c) #3 — drop-on-DAG-change: a second layout splits segment 1 into
/// 3 + 4, derived from the children's `parent_ids`.
#[test]
fn stream_consumer_drops_on_dag_change() {
    let mut conn = connected_conn();
    let session_id = conn
        .open_scalable_topic_session("topic://public/default/scaled")
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();

    // First layout resolves the session.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(session_id, 1, vec![seg(1, 0, 65_536, &[])]),
    )
    .expect("initial layout");
    while conn.poll_event().is_some() {}

    // Second layout: 1 splits into 3 and 4.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(
            session_id,
            2,
            vec![seg(3, 0, 32_768, &[1]), seg(4, 32_768, 65_536, &[1])],
        ),
    )
    .expect("split layout");

    let mut saw_updated = false;
    let mut saw_changed = false;
    while let Some(ev) = conn.poll_event() {
        match ev {
            ConnectionEvent::SegmentDagUpdated { delta, .. } => {
                assert_eq!(delta.epoch, 2);
                assert_eq!(delta.split_events.len(), 1);
                assert_eq!(delta.split_events[0].parent_segment_id, SegmentId(1));
                saw_updated = true;
            }
            ConnectionEvent::DagChangedDuringConsume { reason, .. } => {
                assert_eq!(reason, magnetar_proto::DagChangeReason::Split);
                saw_changed = true;
            }
            _ => {}
        }
    }
    assert!(saw_updated && saw_changed, "split surfaces both events");
    let snap = conn.dag_snapshot(session_id).expect("session open");
    assert!(
        !snap.iter().any(|d| d.segment_id == SegmentId(1)),
        "parent gone"
    );
    assert_eq!(snap.len(), 2, "two children present");
}

/// (c) #4 — **v4 compatibility**. A broker that did not advertise
/// `supports_scalable_topics` gets no scalable-topic command at all: the client
/// refuses locally and the outbound buffer stays empty. This is what keeps a
/// `scalable-topics` build usable against Pulsar 4.x.
#[test]
fn scalable_lookup_refused_against_v4_broker() {
    let mut conn = connected_conn_with(false);
    assert!(!conn.broker_supports_scalable_topics());

    let err = conn
        .open_scalable_topic_session("topic://public/default/scaled")
        .expect_err("v4 broker refuses the scalable surface");
    assert_eq!(err, magnetar_proto::ScalableTopicError::BrokerUnsupported);
    assert!(
        conn.poll_transmit().is_empty(),
        "no scalable command may reach a broker that cannot parse it"
    );
}

/// (c) #5 — the client advertises the capability on `CommandConnect`, and
/// does **not** claim a protocol version above the v4 ceiling: upstream gates
/// PIP-460 on a feature flag, and `ProtocolVersion` still tops at v21 in
/// Pulsar 5.0.0-M1.
#[test]
fn scalable_topics_advertised_by_feature_flag_not_protocol_version() {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("handshake");
    let mut out = conn.poll_transmit();
    let frame = magnetar_proto::decode_one(&mut out).expect("decodes CommandConnect");
    let connect = frame.command.connect.expect("connect payload");
    assert_eq!(
        connect
            .feature_flags
            .and_then(|f| f.supports_scalable_topics),
        Some(true)
    );
    assert_eq!(
        connect.protocol_version,
        Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION)
    );
    assert!(magnetar_runtime_moonpool::is_scalable_topic_url(
        "topic://x"
    ));
}
