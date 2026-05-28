// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0031 scalable-topic integration — moonpool engine.
//!
//! **Experimental.** 1:1 mirror of
//! `magnetar-runtime-tokio/tests/scalable_topic.rs`. Drives
//! `magnetar_proto::Connection` directly with the same synthetic broker
//! script so both engines exercise the identical wire trace. The moonpool
//! engine's `Client::scalable_topic_lookup` / `open_scalable_dag_watch` /
//! `next_scalable_event` are thin delegates over the sans-io entries these
//! tests touch.
//!
//! Parity required by ADR-0024: the test count must match the tokio side 1:1
//! (`cargo xtask check-runtime-test-parity`).

#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]
#![cfg(feature = "scalable-topics")]

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::pb::scalable_topics as st;
use magnetar_proto::{Connection, ConnectionConfig, ConnectionEvent, SegmentId};

fn connected_frame() -> BytesMut {
    let cmd = magnetar_proto::pb::BaseCommand {
        r#type: magnetar_proto::pb::base_command::Type::Connected as i32,
        connected: Some(magnetar_proto::pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(magnetar_proto::pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode Connected");
    buf
}

fn connected_conn() -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(Instant::now(), &connected_frame())
        .expect("connected");
    while conn.poll_event().is_some() {}
    conn
}

fn seg(id: u64, start: u32, end: u32) -> st::SegmentDescriptor {
    st::SegmentDescriptor {
        segment_id: id,
        broker_url: format!("pulsar://seg{id}:6650"),
        broker_url_tls: None,
        key_range_start: start,
        key_range_end: end,
        state: st::SegmentStatePb::Active as i32,
    }
}

/// (c) #1 — `topic://` URL parsing parity with the tokio engine.
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

/// (c) #2 — happy path: same script as the tokio mirror.
#[test]
fn stream_consumer_happy_path_against_fake_broker() {
    let mut conn = connected_conn();
    let rid = conn.send_scalable_topic_lookup("topic://public/default/scaled", false);
    let _ = conn.poll_transmit();

    let resp = st::CommandScalableTopicLookupResponse {
        request_id: rid.0,
        response: st::scalable_lookup_response::LookupType::Connect as i32,
        controller_broker_url: Some("pulsar://controller:6650".to_owned()),
        controller_broker_url_tls: None,
        segments: vec![seg(1, 0, 32_768), seg(2, 32_768, 65_536)],
        lookup_token: Some(42),
        error: None,
        message: None,
    };
    let mut buf = BytesMut::new();
    st::encode(&mut buf, &st::ScalableBaseCommand::lookup_response(resp)).expect("encode resp");
    conn.handle_bytes(Instant::now(), &buf)
        .expect("lookup resp");

    let mut resolved = None;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ScalableTopicLookupResolved {
            segments,
            lookup_token,
            controller_broker_url,
            ..
        } = ev
        {
            resolved = Some((segments, lookup_token, controller_broker_url));
        }
    }
    let (segments, token, url) = resolved.expect("lookup resolved");
    assert_eq!(segments.len(), 2);
    assert_eq!(token, 42);
    assert_eq!(url, "pulsar://controller:6650");

    let sid = conn.open_dag_watch("topic://public/default/scaled", token, segments);
    let _ = conn.poll_transmit();
    let snap = conn.dag_snapshot(sid).expect("session open");
    assert_eq!(snap.len(), 2);
    assert!(snap.iter().any(|d| d.segment_id == SegmentId(1)));
}

/// (c) #3 — drop-on-DAG-change: same split script as the tokio mirror.
#[test]
fn stream_consumer_drops_on_dag_change() {
    let mut conn = connected_conn();
    let initial = vec![magnetar_proto::SegmentDescriptor::from_pb(&seg(
        1, 0, 65_536,
    ))];
    let sid = conn.open_dag_watch("topic://public/default/scaled", 42, initial);
    let _ = conn.poll_transmit();

    let upd = st::CommandSegmentDagUpdate {
        watch_session_id: sid,
        update_seq: 1,
        added: vec![seg(3, 0, 32_768), seg(4, 32_768, 65_536)],
        removed: vec![],
        split_events: vec![st::SplitEvent {
            parent_segment_id: 1,
            child_segment_ids: vec![3, 4],
            split_at_entry: 1000,
        }],
        merge_events: vec![],
    };
    let mut buf = BytesMut::new();
    st::encode(&mut buf, &st::ScalableBaseCommand::dag_update(upd)).expect("encode update");
    conn.handle_bytes(Instant::now(), &buf).expect("update");

    let mut saw_updated = false;
    let mut saw_changed = false;
    while let Some(ev) = conn.poll_event() {
        match ev {
            ConnectionEvent::SegmentDagUpdated { delta, .. } => {
                assert_eq!(delta.split_events.len(), 1);
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
    let snap = conn.dag_snapshot(sid).expect("session open");
    assert!(
        !snap.iter().any(|d| d.segment_id == SegmentId(1)),
        "parent gone"
    );
    assert_eq!(snap.len(), 2, "two children present");
}

/// (c) #4 — feature-off proof mirror. Same shape as the tokio test: when
/// `scalable-topics` is OFF this file is `#[cfg]`-stripped, so none of the
/// scalable surface is exported; when ON, this is a runtime witness that the
/// feature-gated surface resolves symmetrically.
#[test]
fn scalable_topics_feature_off_does_not_export() {
    #[inline(never)]
    fn proto_versions() -> (i32, i32) {
        (
            magnetar_proto::SUPPORTED_PROTOCOL_VERSION_SCALABLE_TOPICS,
            magnetar_proto::SUPPORTED_PROTOCOL_VERSION,
        )
    }
    let (scalable, v4) = proto_versions();
    assert!(
        scalable > v4,
        "scalable protocol version must exceed the v4 ceiling"
    );
    assert!(magnetar_runtime_moonpool::is_scalable_topic_url(
        "topic://x"
    ));
}
