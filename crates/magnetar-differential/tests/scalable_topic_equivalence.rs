// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0093 differential equivalence — the tokio and moonpool
//! engines MUST produce identical `ConnectionEvent` streams for the
//! scalable-topic surface.
//!
//! **Experimental.** The scalable-topic state machine lives entirely in the
//! shared sans-io `magnetar_proto::Connection` (session registry, layout-epoch
//! tracking, event emission). Both engines drive the *same* `Connection`; the
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
//! Mirrors the `(d)` plan in the proposal, with one transcript per
//! client-visible outcome: the initial layout, a split, a merge, a broker
//! rejection, a bodyless update, a legacy (unmigrated-topic) layout, the
//! consumer registration and its rebalance, and the namespace watch.
//!
//! The rejection and merge transcripts matter as much as the happy path: both
//! close the session or reclassify the topology on **both** engines, and a
//! divergence there would surface to the caller as a different `ConsumerEvent`.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime};

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig, ConnectionEvent, pb};

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
            protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags {
                supports_scalable_topics: Some(true),
                supports_tc_metadata_discovery: Some(true),
                ..pb::FeatureFlags::default()
            }),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode Connected");
    conn.handle_bytes(Instant::now(), &buf).expect("connected");
    while conn.poll_event().is_some() {}
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

/// Drive a `Connection` (built with the given wall clock) through the scripted
/// lookup transcript and return a normalised list of event tags.
fn run_lookup_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
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
    drain_event_tags(&mut conn)
}

/// Drive a `Connection` through the scripted DAG-watch + split transcript.
fn run_split_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
    let session_id = conn
        .open_scalable_topic_session("topic://public/default/scaled")
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    // First layout resolves the session; drop its events so the transcript
    // records only the split.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(session_id, 1, vec![seg(1, 0, 65_536, &[])]),
    )
    .expect("initial layout");
    while conn.poll_event().is_some() {}
    // Second layout splits segment 1 into 3 + 4.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(
            session_id,
            2,
            vec![seg(3, 0, 32_768, &[1]), seg(4, 32_768, 65_536, &[1])],
        ),
    )
    .expect("split layout");
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
                controller_broker_url,
                resolved_topic_name,
                epoch,
                ..
            } => format!(
                "LookupResolved(url={},resolved={},epoch={epoch},segs={})",
                controller_broker_url.unwrap_or_default(),
                resolved_topic_name.unwrap_or_default(),
                segments.len()
            ),
            ConnectionEvent::SegmentDagUpdated { delta, .. } => format!(
                "DagUpdated(epoch={},added={},removed={},splits={},merges={})",
                delta.epoch,
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
            ConnectionEvent::ScalableConsumerAssigned {
                consumer_id,
                assignment,
            } => format!(
                "ConsumerAssigned(id={consumer_id},epoch={},topics={})",
                assignment.layout_epoch,
                assignment.segment_topics().join("|")
            ),
            ConnectionEvent::ScalableAssignmentChanged { consumer_id, delta } => format!(
                "AssignmentChanged(id={consumer_id},epoch={},gained={},lost={})",
                delta.layout_epoch,
                delta.gained.len(),
                delta.lost.len()
            ),
            ConnectionEvent::ScalableTopicsChanged { watch_id, change } => match change {
                magnetar_proto::TopicsChange::Snapshot { topics } => {
                    format!(
                        "TopicsSnapshot(watch={watch_id},topics={})",
                        topics.join("|")
                    )
                }
                magnetar_proto::TopicsChange::Diff { added, removed } => format!(
                    "TopicsDiff(watch={watch_id},added={},removed={})",
                    added.join("|"),
                    removed.join("|")
                ),
            },
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
        "LookupResolved(url=pulsar://controller:6650,resolved=topic://public/default/scaled,epoch=1,segs=2)"
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
\n  \"DagUpdated(epoch=2,added=2,removed=1,splits=1,merges=0)\",\
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
            "DagUpdated(epoch=2,added=2,removed=1,splits=1,merges=0)".to_owned(),
            "DagChanged(Split)".to_owned(),
        ]
    );
}

/// Encode a `CommandScalableTopicSubscribeResponse` frame.
fn subscribe_response_frame(request_id: u64, epoch: u64, segs: &[u64]) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicSubscribeResponse as i32,
        scalable_topic_subscribe_response: Some(pb::CommandScalableTopicSubscribeResponse {
            request_id,
            error: None,
            message: None,
            assignment: Some(consumer_assignment(epoch, segs)),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode subscribe response");
    buf
}

/// Encode a `CommandScalableTopicAssignmentUpdate` frame.
fn assignment_update_frame(consumer_id: u64, epoch: u64, segs: &[u64]) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicAssignmentUpdate as i32,
        scalable_topic_assignment_update: Some(pb::CommandScalableTopicAssignmentUpdate {
            consumer_id,
            assignment: consumer_assignment(epoch, segs),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode assignment update");
    buf
}

fn consumer_assignment(epoch: u64, segs: &[u64]) -> pb::ScalableConsumerAssignment {
    pb::ScalableConsumerAssignment {
        layout_epoch: epoch,
        segments: segs
            .iter()
            .map(|&id| pb::ScalableAssignedSegment {
                segment_id: id,
                hash_start: 0,
                hash_end: 32_768,
                segment_topic: format!("segment://public/default/scaled/{id}"),
            })
            .collect(),
    }
}

/// Encode a namespace-watch update carrying `event`.
fn topics_update_frame(
    watch_id: u64,
    event: pb::command_watch_scalable_topics_update::Event,
) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::WatchScalableTopicsUpdate as i32,
        watch_scalable_topics_update: Some(pb::CommandWatchScalableTopicsUpdate {
            watch_id,
            error: None,
            message: None,
            event: Some(event),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode topics update");
    buf
}

/// Drive a `Connection` through the scripted consumer-registration transcript.
fn run_assignment_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
    let request_id = conn
        .scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            42,
            magnetar_proto::ScalableConsumerType::Stream,
        )
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    conn.handle_bytes(
        Instant::now(),
        &subscribe_response_frame(request_id.0, 1, &[1]),
    )
    .expect("subscribe response");
    conn.handle_bytes(Instant::now(), &assignment_update_frame(42, 2, &[2]))
        .expect("rebalance");
    drain_event_tags(&mut conn)
}

/// Drive a `Connection` through the scripted namespace-watch transcript.
fn run_topics_watch_transcript(
    wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>,
) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
    let watch_id = conn
        .watch_scalable_topics("public/default", vec![])
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    conn.handle_bytes(
        Instant::now(),
        &topics_update_frame(
            watch_id,
            pb::command_watch_scalable_topics_update::Event::Snapshot(pb::ScalableTopicsSnapshot {
                topics: vec!["topic://public/default/a".to_owned()],
            }),
        ),
    )
    .expect("snapshot");
    conn.handle_bytes(
        Instant::now(),
        &topics_update_frame(
            watch_id,
            pb::command_watch_scalable_topics_update::Event::Diff(pb::ScalableTopicsDiff {
                added: vec!["topic://public/default/c".to_owned()],
                removed: vec!["topic://public/default/a".to_owned()],
            }),
        ),
    )
    .expect("diff");
    drain_event_tags(&mut conn)
}

#[test]
fn consumer_assignment_event_stream_parity() {
    let tokio_tags = run_assignment_transcript(tokio_wall_clock());
    let moonpool_tags = run_assignment_transcript(moonpool_wall_clock());
    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the consumer-registration transcript"
    );
    assert_eq!(
        tokio_tags,
        vec![
            "ConsumerAssigned(id=42,epoch=1,topics=segment://public/default/scaled/1)".to_owned(),
            "AssignmentChanged(id=42,epoch=2,gained=1,lost=1)".to_owned(),
        ]
    );
}

#[test]
fn namespace_topics_watch_event_stream_parity() {
    let tokio_tags = run_topics_watch_transcript(tokio_wall_clock());
    let moonpool_tags = run_topics_watch_transcript(moonpool_wall_clock());
    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the namespace-watch transcript"
    );
    assert_eq!(
        tokio_tags,
        vec![
            "TopicsSnapshot(watch=1,topics=topic://public/default/a)".to_owned(),
            "TopicsDiff(watch=1,added=topic://public/default/c,removed=topic://public/default/a)"
                .to_owned(),
        ]
    );
}

/// Drive a `Connection` through a **merge** transcript: two segments fold into
/// one child that names both in its `parent_ids`.
fn run_merge_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
    let session_id = conn
        .open_scalable_topic_session("topic://public/default/scaled")
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(
            session_id,
            1,
            vec![seg(5, 0, 32_768, &[]), seg(6, 32_768, 65_536, &[])],
        ),
    )
    .expect("initial layout");
    while conn.poll_event().is_some() {}
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(session_id, 2, vec![seg(7, 0, 65_536, &[5, 6])]),
    )
    .expect("merge layout");
    drain_event_tags(&mut conn)
}

/// Encode a `CommandScalableTopicUpdate` carrying an error instead of a layout.
fn error_update_frame(session_id: u64, code: i32, message: &str) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: None,
            error: Some(code),
            message: Some(message.to_owned()),
            resolved_topic_name: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode error update");
    buf
}

/// Encode a `CommandScalableTopicUpdate` carrying neither a layout nor an error.
fn bodyless_update_frame(session_id: u64) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: None,
            error: None,
            message: None,
            resolved_topic_name: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode bodyless update");
    buf
}

/// Drive a `Connection` through a broker-rejection transcript.
fn run_rejection_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
    let session_id = conn
        .open_scalable_topic_session("topic://public/default/missing")
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    conn.handle_bytes(
        Instant::now(),
        &error_update_frame(
            session_id,
            pb::ServerError::TopicNotFound as i32,
            "no such topic",
        ),
    )
    .expect("rejection");
    drain_event_tags(&mut conn)
}

/// Drive a `Connection` through a bodyless-update transcript — a protocol-shape
/// surprise the session refuses rather than treating as an empty layout.
fn run_bodyless_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
    let session_id = conn
        .open_scalable_topic_session("topic://public/default/scaled")
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    conn.handle_bytes(Instant::now(), &bodyless_update_frame(session_id))
        .expect("bodyless update");
    drain_event_tags(&mut conn)
}

/// Drive a `Connection` through the broker's **synthetic** layout for a regular,
/// unmigrated topic: one sealed legacy segment wrapping a `persistent://` name.
fn run_legacy_transcript(wall_clock: Arc<dyn Fn() -> SystemTime + Send + Sync>) -> Vec<String> {
    let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
    connected(&mut conn);
    let session_id = conn
        .open_scalable_topic_session("persistent://public/default/plain")
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    let mut legacy = seg(0, 0, 65_536, &[]);
    legacy.legacy_topic_name = Some("persistent://public/default/plain".to_owned());
    legacy.state = pb::SegmentState::Sealed as i32;
    conn.handle_bytes(Instant::now(), &layout_frame(session_id, 0, vec![legacy]))
        .expect("legacy layout");
    let tags = drain_event_tags(&mut conn);
    // The legacy marker and the sealed state must survive the decode, or a
    // consumer would look for a `segment://` topic that does not exist.
    let snap = conn.dag_snapshot(session_id).expect("session open");
    assert!(snap[0].is_legacy(), "legacy marker survives decode");
    assert_eq!(snap[0].state, magnetar_proto::SegmentState::Sealed);
    tags
}

#[test]
fn merge_event_stream_parity() {
    let tokio_tags = run_merge_transcript(tokio_wall_clock());
    let moonpool_tags = run_merge_transcript(moonpool_wall_clock());
    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the merge transcript"
    );
    assert_eq!(
        tokio_tags,
        vec![
            "DagUpdated(epoch=2,added=1,removed=2,splits=0,merges=1)".to_owned(),
            "DagChanged(Merge)".to_owned(),
        ]
    );
}

#[test]
fn broker_rejection_event_stream_parity() {
    let tokio_tags = run_rejection_transcript(tokio_wall_clock());
    let moonpool_tags = run_rejection_transcript(moonpool_wall_clock());
    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the rejection transcript"
    );
    assert_eq!(tokio_tags.len(), 1);
    assert!(
        tokio_tags[0].starts_with("DagWatchClosed("),
        "a rejected session closes: {}",
        tokio_tags[0]
    );
    assert!(
        tokio_tags[0].contains("no such topic"),
        "the broker's message reaches the caller: {}",
        tokio_tags[0]
    );
}

#[test]
fn bodyless_update_event_stream_parity() {
    let tokio_tags = run_bodyless_transcript(tokio_wall_clock());
    let moonpool_tags = run_bodyless_transcript(moonpool_wall_clock());
    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the bodyless-update transcript"
    );
    assert_eq!(tokio_tags.len(), 1);
    assert!(
        tokio_tags[0].contains("carried neither a DAG nor an error"),
        "a bodyless update is refused, not treated as an empty layout: {}",
        tokio_tags[0]
    );
}

#[test]
fn legacy_layout_event_stream_parity() {
    let tokio_tags = run_legacy_transcript(tokio_wall_clock());
    let moonpool_tags = run_legacy_transcript(moonpool_wall_clock());
    assert_eq!(
        tokio_tags, moonpool_tags,
        "engine event streams diverged for the legacy-layout transcript"
    );
    assert_eq!(
        tokio_tags,
        vec![
            "LookupResolved(url=pulsar://controller:6650,resolved=topic://public/default/scaled,epoch=0,segs=1)"
                .to_owned()
        ]
    );
}

/// The descriptor and assignment encode sides round-trip. Both engines hand
/// these to the broker fakes and to the CLI, so a lossy encode would diverge
/// the two legs the moment either replays a layout it decoded.
#[test]
fn scalable_wire_types_roundtrip() {
    let mut info = seg(3, 16_384, 32_768, &[1, 2]);
    info.legacy_topic_name = Some("persistent://public/default/plain".to_owned());
    info.sealed_at_epoch = Some(4);
    info.state = pb::SegmentState::Sealed as i32;
    let address = pb::SegmentBrokerAddress {
        segment_id: 3,
        broker_url: "pulsar://seg3:6650".to_owned(),
        broker_url_tls: Some("pulsar+ssl://seg3:6651".to_owned()),
    };

    let descriptor = magnetar_proto::SegmentDescriptor::from_pb(&info, Some(&address));
    let (back_info, back_address) = descriptor.to_pb();
    assert_eq!(back_info.segment_id, info.segment_id);
    assert_eq!(back_info.hash_start, info.hash_start);
    assert_eq!(back_info.hash_end, info.hash_end);
    assert_eq!(back_info.state, info.state);
    assert_eq!(back_info.parent_ids, info.parent_ids);
    assert_eq!(back_info.child_ids, info.child_ids);
    assert_eq!(back_info.sealed_at_epoch, info.sealed_at_epoch);
    assert_eq!(back_info.legacy_topic_name, info.legacy_topic_name);
    assert_eq!(back_address, Some(address));

    // A descriptor with no placement encodes no address half — a sealed segment
    // the broker no longer serves has none.
    let placeless = magnetar_proto::SegmentDescriptor::from_pb(&info, None);
    assert_eq!(placeless.to_pb().1, None);

    // Segment state and consumer type survive the enum round-trip.
    assert_eq!(
        magnetar_proto::SegmentState::from_pb_i32(magnetar_proto::SegmentState::Sealed.to_pb_i32()),
        magnetar_proto::SegmentState::Sealed
    );
    assert_eq!(
        magnetar_proto::SegmentState::from_pb_i32(magnetar_proto::SegmentState::Active.to_pb_i32()),
        magnetar_proto::SegmentState::Active
    );
    assert_eq!(
        magnetar_proto::SegmentState::from_pb_i32(99),
        magnetar_proto::SegmentState::Active,
        "an unknown wire state saturates rather than breaking the decode"
    );

    let assignment = magnetar_proto::ConsumerAssignment::from_pb(&consumer_assignment(7, &[2, 1]));
    assert_eq!(assignment.layout_epoch, 7);
    let back = assignment.to_pb();
    assert_eq!(back.layout_epoch, 7);
    assert_eq!(back.segments.len(), 2);
    assert_eq!(
        back.segments[0].segment_id, 1,
        "segments are ordered by id so the two engines compare equal"
    );
    assert_eq!(
        magnetar_proto::ScalableConsumerType::from_pb_i32(
            magnetar_proto::ScalableConsumerType::Checkpoint.to_pb_i32()
        ),
        magnetar_proto::ScalableConsumerType::Checkpoint
    );
}

/// The session's own guards — mismatched session id, non-advancing epoch — and
/// its accessors behave identically whichever engine's clock is installed.
/// A mismatched or replayed frame must close the session, never mutate it.
#[test]
fn session_guards_and_accessors_parity() {
    for wall_clock in [tokio_wall_clock(), moonpool_wall_clock()] {
        let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
        connected(&mut conn);
        // The negotiated flags are readable, and carry what the broker sent.
        assert!(
            conn.feature_flags()
                .supports_scalable_topics
                .unwrap_or(false)
        );

        let session_id = conn
            .open_scalable_topic_session("topic://public/default/scaled")
            .expect("broker supports scalable topics");
        let _ = conn.poll_transmit();
        conn.handle_bytes(
            Instant::now(),
            &layout_frame(session_id, 5, vec![seg(1, 0, 65_536, &[])]),
        )
        .expect("initial layout");
        while conn.poll_event().is_some() {}

        // A replayed layout at the same epoch closes the session rather than
        // being applied — the broker's ordering guarantee, enforced client-side.
        conn.handle_bytes(
            Instant::now(),
            &layout_frame(session_id, 5, vec![seg(9, 0, 65_536, &[])]),
        )
        .expect("stale layout");
        let tags = drain_event_tags(&mut conn);
        assert_eq!(tags.len(), 1);
        assert!(
            tags[0].contains("non-monotonic layout epoch"),
            "a replayed layout closes the session: {}",
            tags[0]
        );
        assert!(
            conn.dag_snapshot(session_id).is_none(),
            "the session is dropped, not left holding a stale layout"
        );

        // Closing an already-closed session writes nothing.
        conn.close_scalable_topic_session(session_id);
        assert!(conn.poll_transmit().is_empty());

        // An update naming a session this connection does not track is ignored.
        let mut session = magnetar_proto::DagWatchSession::new(1234);
        assert!(!session.is_resolved(), "a fresh session holds no layout");
        let err = session
            .handle_update(&pb::CommandScalableTopicUpdate {
                session_id: 4321,
                dag: None,
                error: None,
                message: None,
                resolved_topic_name: None,
            })
            .expect_err("session mismatch rejected");
        assert_eq!(
            err,
            magnetar_proto::DagError::SessionMismatch {
                got: 4321,
                expected: 1234
            }
        );
    }
}

/// Stale-frame guards and read-only accessors, driven from the differential
/// layer because `magnetar-proto`'s own unit tests never run under the
/// sim-coverage runner (ADR-0024 execution scope).
///
/// Every arm here is a *drop* path: a frame naming a session, consumer or watch
/// this connection does not track must be ignored rather than acted on. Those
/// arrive routinely — a broker push racing a client-side close — so a
/// regression would surface as a panic or a spurious event on a live consumer,
/// not as a test failure elsewhere.
#[test]
fn scalable_stale_frames_are_dropped_and_accessors_read() {
    for wall_clock in [tokio_wall_clock(), moonpool_wall_clock()] {
        let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
        connected(&mut conn);

        let session_id = conn
            .open_scalable_topic_session("persistent://public/default/scaled")
            .expect("broker supports scalable topics");
        let _ = conn.poll_transmit();
        assert_eq!(
            conn.scalable_resolved_topic_name(session_id),
            None,
            "no canonical identity before the first layout"
        );
        conn.handle_bytes(
            Instant::now(),
            &layout_frame(session_id, 1, vec![seg(1, 0, 65_536, &[])]),
        )
        .expect("initial layout");
        while conn.poll_event().is_some() {}
        assert_eq!(
            conn.scalable_resolved_topic_name(session_id),
            Some("topic://public/default/scaled"),
            "the broker's canonical identity is readable off the session"
        );
        assert_eq!(conn.scalable_resolved_topic_name(9_999), None);

        // A layout for a session this connection never opened is dropped.
        conn.handle_bytes(
            Instant::now(),
            &layout_frame(9_999, 1, vec![seg(1, 0, 65_536, &[])]),
        )
        .expect("stale layout tolerated");
        assert!(
            conn.poll_event().is_none(),
            "a layout for an unknown session emits nothing"
        );
        assert!(conn.dag_snapshot(9_999).is_none());

        // A subscribe response for a request this connection never issued.
        conn.handle_bytes(Instant::now(), &subscribe_response_frame(4_242, 1, &[1]))
            .expect("stale subscribe response tolerated");
        assert!(conn.poll_event().is_none());

        // An assignment update for a consumer that never registered.
        conn.handle_bytes(Instant::now(), &assignment_update_frame(4_242, 2, &[1]))
            .expect("stale assignment tolerated");
        assert!(conn.poll_event().is_none());
        assert!(conn.scalable_consumer_assignment(4_242).is_none());

        // A namespace-watch update for a watch that was never opened.
        conn.handle_bytes(
            Instant::now(),
            &topics_update_frame(
                7_777,
                pb::command_watch_scalable_topics_update::Event::Snapshot(
                    pb::ScalableTopicsSnapshot {
                        topics: vec!["topic://public/default/x".to_owned()],
                    },
                ),
            ),
        )
        .expect("stale watch update tolerated");
        assert!(conn.poll_event().is_none());
        assert!(conn.scalable_topics_snapshot(7_777).is_none());

        // A TC-assignment update for a watch that was never opened.
        conn.handle_bytes(Instant::now(), &tc_update_frame(7_777, 2))
            .expect("stale tc update tolerated");
        assert!(conn.poll_event().is_none());

        // Closing ids this connection does not track writes nothing.
        conn.close_scalable_topic_session(9_999);
        conn.close_scalable_topics_watch(7_777);
        conn.close_tc_assignments_watch(7_777);
        assert!(
            conn.poll_transmit().is_empty(),
            "closing an untracked id emits no frame"
        );

        // Opening a TC watch without the broker flag is refused; the scripted
        // handshake advertises it, so this one succeeds and then closes.
        assert!(conn.broker_supports_tc_metadata_discovery());
        let tc_watch = conn.watch_tc_assignments().expect("tc watch opens");
        let _ = conn.poll_transmit();
        conn.close_tc_assignments_watch(tc_watch);
        assert!(
            !conn.poll_transmit().is_empty(),
            "the close reaches the wire"
        );
    }
}

/// The sans-io session and watch types expose read-only state the engines and
/// the CLI surface; exercised here for the same execution-scope reason.
#[test]
fn scalable_consumer_session_and_watch_accessors() {
    let mut session = magnetar_proto::ScalableConsumerSession::new(
        7,
        "topic://public/default/scaled".to_owned(),
        "sub".to_owned(),
        "consumer-a".to_owned(),
        magnetar_proto::ScalableConsumerType::Stream,
    );
    assert_eq!(session.topic(), "topic://public/default/scaled");
    assert_eq!(session.subscription(), "sub");
    assert_eq!(session.consumer_name(), "consumer-a");
    assert_eq!(
        session.consumer_type(),
        magnetar_proto::ScalableConsumerType::Stream
    );
    assert!(!session.is_registered());
    assert!(session.assignment().is_none());

    let assignment = session
        .handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
            request_id: 1,
            error: None,
            message: None,
            assignment: Some(consumer_assignment(3, &[1])),
        })
        .expect("subscribe resolves");
    assert!(session.is_registered());
    assert_eq!(session.assignment().map(|a| a.layout_epoch), Some(3));
    assert_eq!(
        assignment.segment_topics(),
        vec!["segment://public/default/scaled/1"]
    );

    // A success response carrying no assignment is refused rather than read as
    // an empty share.
    let mut empty = magnetar_proto::ScalableConsumerSession::new(
        8,
        "t".to_owned(),
        "s".to_owned(),
        "c".to_owned(),
        magnetar_proto::ScalableConsumerType::Checkpoint,
    );
    assert_eq!(
        empty.consumer_type(),
        magnetar_proto::ScalableConsumerType::Checkpoint
    );
    let err = empty
        .handle_subscribe_response(&pb::CommandScalableTopicSubscribeResponse {
            request_id: 9,
            error: None,
            message: None,
            assignment: None,
        })
        .expect_err("bodyless subscribe response refused");
    assert_eq!(
        err,
        magnetar_proto::AssignmentError::Empty { request_id: 9 }
    );

    // `Stream` is the saturating default for an unrecognised wire value, and
    // the round-trip holds for the variant the enum defaults to.
    assert_eq!(
        magnetar_proto::ScalableConsumerType::from_pb_i32(
            magnetar_proto::ScalableConsumerType::Stream.to_pb_i32()
        ),
        magnetar_proto::ScalableConsumerType::Stream
    );

    let mut watch = magnetar_proto::ScalableTopicsWatch::new(3, "public/default".to_owned());
    assert_eq!(watch.namespace(), "public/default");
    assert!(!watch.is_resolved());
    assert!(watch.topics().is_empty());
    watch
        .handle_update(&pb::CommandWatchScalableTopicsUpdate {
            watch_id: 3,
            error: None,
            message: None,
            event: Some(pb::command_watch_scalable_topics_update::Event::Snapshot(
                pb::ScalableTopicsSnapshot {
                    topics: vec!["topic://public/default/a".to_owned()],
                },
            )),
        })
        .expect("snapshot applies");
    assert!(watch.is_resolved());
    assert_eq!(watch.topics(), vec!["topic://public/default/a".to_owned()]);

    // An assignment round-trips through the wire pair, and the assigned-segment
    // encode side is what the fakes hand back.
    let seg = magnetar_proto::AssignedSegment::from_pb(&pb::ScalableAssignedSegment {
        segment_id: 4,
        hash_start: 0,
        hash_end: 128,
        segment_topic: "segment://public/default/scaled/4".to_owned(),
    });
    assert_eq!(seg.to_pb().segment_id, 4);
    assert!(
        magnetar_proto::AssignmentDelta {
            layout_epoch: 1,
            gained: vec![],
            lost: vec![],
        }
        .is_empty()
    );
}

/// Encode a `CommandWatchTcAssignmentsUpdate` carrying `parallelism` coordinators.
fn tc_update_frame(watch_id: u64, parallelism: u32) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTcAssignmentsUpdate as i32,
        watch_tc_assignments_update: Some(pb::CommandWatchTcAssignmentsUpdate {
            watch_id,
            snapshot: Some(pb::TcAssignmentsSnapshot {
                parallelism,
                assignments: (0..u64::from(parallelism))
                    .map(|tc_id| pb::TcAssignment {
                        tc_id,
                        broker_service_url: Some(format!("pulsar://tc{tc_id}:6650")),
                        broker_service_url_tls: None,
                    })
                    .collect(),
            }),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode tc update");
    buf
}

/// Rejection paths on the two watch families, plus the malformed-frame guards
/// on the dispatch arms.
///
/// A broker that refuses a namespace watch or a coordinator-discovery watch
/// must close that watch and say why, not leave the caller holding an empty set
/// it will read as "no matching topics". The dispatch guards cover the
/// complementary case: a frame whose type says one thing and whose payload is
/// absent must be refused rather than unwrapped.
#[test]
fn scalable_watch_rejections_and_malformed_frames() {
    for wall_clock in [tokio_wall_clock(), moonpool_wall_clock()] {
        let mut conn = Connection::new(ConnectionConfig::default(), wall_clock);
        connected(&mut conn);

        // A rejected namespace watch closes it and carries the broker's reason.
        let watch_id = conn
            .watch_scalable_topics("public/default", vec![])
            .expect("watch opens");
        let _ = conn.poll_transmit();
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::WatchScalableTopicsUpdate as i32,
            watch_scalable_topics_update: Some(pb::CommandWatchScalableTopicsUpdate {
                watch_id,
                error: Some(pb::ServerError::AuthorizationError as i32),
                message: Some("not permitted on this namespace".to_owned()),
                event: None,
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        magnetar_proto::encode_command(&mut buf, &cmd).expect("encode watch rejection");
        conn.handle_bytes(Instant::now(), &buf).expect("rejection");

        let tags = drain_event_tags(&mut conn);
        assert_eq!(tags.len(), 1);
        assert!(
            tags[0].contains("not permitted on this namespace"),
            "the broker's reason reaches the caller: {}",
            tags[0]
        );
        assert!(
            conn.scalable_topics_snapshot(watch_id).is_none(),
            "a rejected watch is dropped, not left holding an empty set"
        );

        // A coordinator-discovery update carrying no snapshot closes that watch.
        let tc_watch = conn.watch_tc_assignments().expect("tc watch opens");
        let _ = conn.poll_transmit();
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTcAssignmentsUpdate as i32,
            watch_tc_assignments_update: Some(pb::CommandWatchTcAssignmentsUpdate {
                watch_id: tc_watch,
                snapshot: None,
                error: Some(pb::ServerError::ServiceNotReady as i32),
                message: Some("coordinators not assigned yet".to_owned()),
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        magnetar_proto::encode_command(&mut buf, &cmd).expect("encode tc rejection");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("tc rejection");
        let tags = drain_event_tags(&mut conn);
        assert_eq!(tags.len(), 1);
        assert!(
            tags[0].contains("coordinators not assigned yet"),
            "the broker's reason reaches the caller: {}",
            tags[0]
        );

        // A bodyless coordinator update — neither snapshot nor error — also
        // closes rather than being read as "zero coordinators".
        let tc_watch = conn.watch_tc_assignments().expect("second tc watch opens");
        let _ = conn.poll_transmit();
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTcAssignmentsUpdate as i32,
            watch_tc_assignments_update: Some(pb::CommandWatchTcAssignmentsUpdate {
                watch_id: tc_watch,
                snapshot: None,
                error: None,
                message: None,
            }),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        magnetar_proto::encode_command(&mut buf, &cmd).expect("encode bodyless tc");
        conn.handle_bytes(Instant::now(), &buf)
            .expect("bodyless tc");
        assert_eq!(drain_event_tags(&mut conn).len(), 1);

        // Malformed frames: the type says a scalable command, the payload is
        // absent. Each dispatch arm must refuse rather than unwrap.
        for cmd_type in [
            pb::base_command::Type::ScalableTopicUpdate,
            pb::base_command::Type::ScalableTopicSubscribeResponse,
            pb::base_command::Type::ScalableTopicAssignmentUpdate,
            pb::base_command::Type::WatchScalableTopicsUpdate,
            pb::base_command::Type::WatchTcAssignmentsUpdate,
        ] {
            let cmd = pb::BaseCommand {
                r#type: cmd_type as i32,
                ..Default::default()
            };
            let mut buf = BytesMut::new();
            magnetar_proto::encode_command(&mut buf, &cmd).expect("encode payloadless");
            let err = conn
                .handle_bytes(Instant::now(), &buf)
                .expect_err("a payloadless scalable frame is refused");
            assert!(
                matches!(err, magnetar_proto::ProtocolError::InvariantViolation(_)),
                "{cmd_type:?} refused as an invariant violation, got {err:?}"
            );
        }
    }
}

/// A consumer session refuses an assignment addressed to a different consumer,
/// and a namespace watch refuses an update addressed to a different watch.
#[test]
fn scalable_session_and_watch_reject_foreign_updates() {
    let mut session = magnetar_proto::ScalableConsumerSession::new(
        7,
        "topic://public/default/scaled".to_owned(),
        "sub".to_owned(),
        "consumer-a".to_owned(),
        magnetar_proto::ScalableConsumerType::Stream,
    );
    let err = session
        .handle_assignment_update(&pb::CommandScalableTopicAssignmentUpdate {
            consumer_id: 999,
            assignment: consumer_assignment(1, &[1]),
        })
        .expect_err("assignment for another consumer refused");
    assert_eq!(
        err,
        magnetar_proto::AssignmentError::ConsumerMismatch {
            got: 999,
            expected: 7
        }
    );

    let mut watch = magnetar_proto::ScalableTopicsWatch::new(3, "public/default".to_owned());
    let err = watch
        .handle_update(&pb::CommandWatchScalableTopicsUpdate {
            watch_id: 99,
            error: None,
            message: None,
            event: None,
        })
        .expect_err("update for another watch refused");
    assert_eq!(
        err,
        magnetar_proto::TopicsWatchError::WatchMismatch {
            got: 99,
            expected: 3
        }
    );

    // A bodyless update for the right watch is refused too, rather than read as
    // an empty matching set.
    let err = watch
        .handle_update(&pb::CommandWatchScalableTopicsUpdate {
            watch_id: 3,
            error: None,
            message: None,
            event: None,
        })
        .expect_err("bodyless watch update refused");
    assert_eq!(err, magnetar_proto::TopicsWatchError::Empty { watch_id: 3 });
}
