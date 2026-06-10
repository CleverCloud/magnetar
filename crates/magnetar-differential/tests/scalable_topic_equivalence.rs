// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0031 differential equivalence — the tokio and moonpool
//! engines MUST produce identical `ConnectionEvent` streams for the
//! scalable-topic surface.
//!
//! **Experimental.** The scalable-topic state machine lives entirely in the
//! shared sans-io `magnetar_proto::Connection` (lookup registry, `DagWatch`
//! session, event emission). Both engines drive the *same* `Connection`; the
//! only engine-varying input is the injected wall-clock provider (tokio plugs
//! in host `SystemTime::now`; moonpool plugs in a fixed-base atomic clock).
//! These tests run the identical scripted-broker transcript through a
//! `Connection` constructed the way each engine constructs it and assert the
//! emitted event sequences match — the differential equivalence guarantee at
//! the layer the scalable surface actually lives in.
//!
//! A golden trace lives at `tests/golden/scalable_topic_drop_on_split.json` —
//! human-reviewable, regenerated via `MAGNETAR_REGENERATE_GOLDEN=1`.
//!
//! Two tests, mirroring the `(d)` plan in the proposal:
//! 1. `scalable_topic_lookup_event_stream_parity`
//! 2. `dag_change_event_stream_parity`

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::pb::scalable_topics as st;
use magnetar_proto::{Connection, ConnectionConfig, ConnectionEvent};

/// A tokio-engine-shaped wall clock (host `SystemTime::now`).
fn tokio_wall_clock() -> Arc<dyn Fn() -> SystemTime + Send + Sync> {
    Arc::new(SystemTime::now)
}

/// A moonpool-engine-shaped wall clock (fixed-base atomic, as
/// `magnetar_runtime_moonpool::ConnectionShared` installs it).
fn moonpool_wall_clock() -> Arc<dyn Fn() -> SystemTime + Send + Sync> {
    let base = Arc::new(AtomicU64::new(1_700_000_000_000));
    Arc::new(move || {
        SystemTime::UNIX_EPOCH + std::time::Duration::from_millis(base.load(Ordering::Relaxed))
    })
}

fn connected(conn: &mut Connection) {
    conn.begin_handshake().expect("handshake");
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
    conn.handle_bytes(Instant::now(), &buf).expect("connected");
    while conn.poll_event().is_some() {}
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

/// Drive a `Connection` (built with the given wall clock) through the scripted
/// lookup transcript and return a normalised list of event tags.
fn run_lookup_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
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
    st::encode(&mut buf, &st::ScalableBaseCommand::lookup_response(resp)).expect("encode");
    conn.handle_bytes(Instant::now(), &buf).expect("resp");
    drain_event_tags(&mut conn)
}

/// Drive a `Connection` through the scripted DAG-watch + split transcript.
fn run_split_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
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
    st::encode(&mut buf, &st::ScalableBaseCommand::dag_update(upd)).expect("encode");
    conn.handle_bytes(Instant::now(), &buf).expect("update");
    drain_event_tags(&mut conn)
}

/// Drain the connection's event queue into stable string tags (timestamp-free,
/// matching the differential harness convention of ignoring `Instant` fields).
fn drain_event_tags(conn: &mut Connection) -> Vec<String> {
    let mut tags = Vec::new();
    while let Some(ev) = conn.poll_event() {
        let tag = match ev {
            ConnectionEvent::ScalableTopicLookupResolved {
                segments,
                lookup_token,
                controller_broker_url,
                ..
            } => format!(
                "LookupResolved(url={controller_broker_url},token={lookup_token},segs={})",
                segments.len()
            ),
            ConnectionEvent::SegmentDagUpdated { delta, .. } => format!(
                "DagUpdated(added={},removed={},splits={},merges={})",
                delta.added.len(),
                delta.removed.len(),
                delta.split_events.len(),
                delta.merge_events.len()
            ),
            ConnectionEvent::DagChangedDuringConsume { reason, .. } => {
                format!("DagChanged({reason:?})")
            }
            ConnectionEvent::DagWatchClosed { reason, .. } => {
                format!("DagWatchClosed({reason:?})")
            }
            other => format!("Other({other:?})"),
        };
        tags.push(tag);
    }
    tags
}

#[test]
fn scalable_topic_lookup_event_stream_parity() {
    let tokio_tags = run_lookup_transcript(tokio_wall_clock());
    let moonpool_tags = run_lookup_transcript(moonpool_wall_clock());
    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the scalable lookup transcript"
    );
    assert_eq!(tokio_tags.len(), 1);
    assert_eq!(
        tokio_tags[0],
        "LookupResolved(url=pulsar://controller:6650,token=42,segs=2)"
    );
}

#[test]
fn dag_change_event_stream_parity() {
    let tokio_tags = run_split_transcript(tokio_wall_clock());
    let moonpool_tags = run_split_transcript(moonpool_wall_clock());
    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the scalable split transcript"
    );

    // Golden trace — human-reviewable, regenerated via MAGNETAR_REGENERATE_GOLDEN=1.
    let golden_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/scalable_topic_drop_on_split.json");
    let expected = "[\
\n  \"DagUpdated(added=2,removed=1,splits=1,merges=0)\",\
\n  \"DagChanged(Split)\"\
\n]\n";
    if std::env::var_os("MAGNETAR_REGENERATE_GOLDEN").is_some() {
        std::fs::create_dir_all(golden_path.parent().unwrap()).unwrap();
        std::fs::write(&golden_path, expected).unwrap();
    }
    let actual = std::fs::read_to_string(&golden_path)
        .unwrap_or_else(|_| panic!("golden file missing at {golden_path:?}"));
    assert_eq!(
        actual.trim(),
        expected.trim(),
        "PIP-460 golden trace drift — regenerate via MAGNETAR_REGENERATE_GOLDEN=1"
    );
    // Sanity: the recorded stream matches the golden.
    assert_eq!(
        tokio_tags,
        vec![
            "DagUpdated(added=2,removed=1,splits=1,merges=0)".to_owned(),
            "DagChanged(Split)".to_owned(),
        ]
    );
}
