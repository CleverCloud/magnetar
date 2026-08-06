// SPDX-License-Identifier: Apache-2.0

//! PIP-460 / ADR-0093 scalable-topic integration — moonpool engine.
//!
//! **Experimental.** The paired protocol cases drive
//! `magnetar_proto::Connection` directly with identical synthetic frames. The
//! provider-native case additionally opens a real runtime DAG session over a
//! `SimProviders` network, task provider, and virtual clock.
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

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{BufMut as _, Bytes, BytesMut};
use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use magnetar_fakes::m1::{Endpoint, EndpointAuthorities, M1FakeCluster, M1FakeConfig};
use magnetar_proto::{Connection, ConnectionConfig, ConnectionEvent, FrameError, SegmentId, pb};
use magnetar_runtime_moonpool::{Client, MoonpoolEngine, StreamConsumerOptions};
use moonpool_core::{NetworkProvider, Providers, TaskProvider, TcpListenerTrait, TimeProvider};
use moonpool_sim::network::sim::SimTcpListener;
use moonpool_sim::providers::SimProviders;
use moonpool_sim::{SimContext, SimulationBuilder, SimulationError, SimulationResult, Workload};
use parking_lot::Mutex;
use prost::Message as _;
use tokio_util::sync::CancellationToken;

mod common;
use common::sweep_seeds;

/// A `Connected` handshake frame, optionally advertising the PIP-460 and
/// PIP-473 capabilities. `false` for both models a Pulsar 4.x broker.
fn connected_frame(scalable: bool, tc_discovery: bool) -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags {
                supports_scalable_topics: scalable.then_some(true),
                supports_tc_metadata_discovery: tc_discovery.then_some(true),
                ..pb::FeatureFlags::default()
            }),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode Connected");
    buf
}

fn connected_conn_with_flags(scalable: bool, tc_discovery: bool) -> Connection {
    let mut conn = Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    );
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(Instant::now(), &connected_frame(scalable, tc_discovery))
        .expect("connected");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();
    conn
}

fn connected_conn_with(scalable: bool) -> Connection {
    connected_conn_with_flags(scalable, false)
}

fn connected_conn() -> Connection {
    connected_conn_with(true)
}

fn seg(
    id: u64,
    start: u32,
    end: u32,
    parents: &[u64],
    children: &[u64],
    state: pb::SegmentState,
) -> pb::SegmentInfoProto {
    pb::SegmentInfoProto {
        segment_id: id,
        hash_start: start,
        hash_end: end,
        state: state as i32,
        parent_ids: parents.to_vec(),
        child_ids: children.to_vec(),
        created_at_epoch: if parents.is_empty() { 0 } else { 2 },
        sealed_at_epoch: (state == pb::SegmentState::Sealed).then_some(2),
        created_at_ms: 0,
        sealed_at_ms: None,
        legacy_topic_name: None,
    }
}

/// Encode a broker→client `CommandScalableTopicUpdate` carrying a whole layout.
fn layout_frame(session_id: u64, epoch: u64, segments: Vec<pb::SegmentInfoProto>) -> BytesMut {
    let segment_brokers = segments
        .iter()
        .filter(|segment| segment.state == pb::SegmentState::Active as i32)
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
        vec![
            seg(1, 0, 32_767, &[], &[], pb::SegmentState::Active),
            seg(2, 32_768, 65_535, &[], &[], pb::SegmentState::Active),
        ],
    );
    conn.handle_bytes(Instant::now(), &buf).expect("layout");

    let mut resolved = None;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ScalableTopicLookupResolved {
            snapshot,
            controller_broker_url,
            resolved_topic_name,
            ..
        } = ev
        {
            resolved = Some((
                snapshot.segments(),
                controller_broker_url,
                resolved_topic_name,
                snapshot.epoch(),
            ));
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

#[test]
#[allow(clippy::too_many_lines)]
fn partial_batch_ack_seek_and_flow_wire_contract() {
    let mut conn = connected_conn();
    let handle = conn.subscribe(magnetar_proto::SubscribeRequest {
        topic: "segment://public/default/scaled/0000-ffff-1".to_owned(),
        subscription: "aggregate".to_owned(),
        receiver_queue_size: 0,
        defer_payload_processing: true,
        ..Default::default()
    });
    let _ = conn.poll_transmit();
    let mut body = BytesMut::new();
    for payload in [b"first".as_ref(), b"omitted".as_ref(), b"third".as_ref()] {
        let metadata = pb::SingleMessageMetadata {
            payload_size: i32::try_from(payload.len()).expect("payload fits i32"),
            ..Default::default()
        }
        .encode_to_vec();
        body.put_u32(u32::try_from(metadata.len()).expect("metadata fits u32"));
        body.extend_from_slice(&metadata);
        body.extend_from_slice(payload);
    }
    let base_id = pb::MessageIdData {
        ledger_id: 7,
        entry_id: 11,
        partition: Some(-1),
        ..Default::default()
    };
    let command = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: handle.0,
            message_id: base_id.clone(),
            redelivery_count: Some(0),
            ack_set: vec![0b101],
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let metadata = pb::MessageMetadata {
        producer_name: "producer".to_owned(),
        sequence_id: 1,
        publish_time: 1,
        num_messages_in_batch: Some(3),
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    magnetar_proto::encode_payload(&mut frame, &command, &metadata, &body)
        .expect("encode partial batch");
    conn.handle_bytes(Instant::now(), &frame)
        .expect("accept partial batch");
    let deferred = conn
        .pop_deferred_message(handle, Instant::now())
        .expect("deferred batch");
    assert_eq!(deferred.ack_set, vec![0b101]);
    assert_eq!(deferred.dispatch_permits, 2);

    let cumulative = pb::MessageIdData {
        batch_index: Some(0),
        ack_set: vec![0b101],
        batch_size: Some(3),
        ..base_id.clone()
    };
    let _ = conn.ack_with_message_id_data(
        handle,
        magnetar_proto::AckRequest {
            message_ids: vec![magnetar_proto::MessageId::from_pb(&cumulative)],
            ack_type: pb::command_ack::AckType::Cumulative,
            properties: Vec::new(),
            txn_id: None,
        },
        vec![cumulative],
        Instant::now(),
    );
    let mut outbound = conn.poll_transmit();
    let ack = magnetar_proto::decode_one(&mut outbound)
        .expect("decode cumulative ack")
        .command
        .ack
        .expect("CommandAck");
    assert_eq!(ack.message_id[0].ack_set, vec![0b100]);

    let _ = conn.seek(
        handle,
        magnetar_proto::SeekTarget::MessageIdData(pb::MessageIdData {
            batch_index: Some(2),
            ack_set: vec![0b101],
            batch_size: Some(3),
            ..base_id
        }),
    );
    let mut outbound = conn.poll_transmit();
    let seek = magnetar_proto::decode_one(&mut outbound)
        .expect("decode seek")
        .command
        .seek
        .expect("CommandSeek");
    let seek_id = seek.message_id.expect("seek id");
    assert_eq!(seek_id.batch_index, None);
    assert_eq!(seek_id.batch_size, None);
    assert_eq!(seek_id.ack_set, vec![0b100]);

    conn.flow_for_aggregate(handle, 1, 3);
    let mut outbound = conn.poll_transmit();
    let flow = magnetar_proto::decode_one(&mut outbound)
        .expect("decode aggregate flow")
        .command
        .flow
        .expect("CommandFlow");
    assert_eq!(flow.message_permits, 4);
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
        &layout_frame(
            session_id,
            1,
            vec![seg(1, 0, 65_535, &[], &[], pb::SegmentState::Active)],
        ),
    )
    .expect("initial layout");
    while conn.poll_event().is_some() {}

    // Second layout: 1 splits into 3 and 4.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(
            session_id,
            2,
            vec![
                seg(1, 0, 65_535, &[], &[3, 4], pb::SegmentState::Sealed),
                seg(3, 0, 32_767, &[1], &[], pb::SegmentState::Active),
                seg(4, 32_768, 65_535, &[1], &[], pb::SegmentState::Active),
            ],
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
        snap.iter().any(|d| {
            d.segment_id == SegmentId(1) && d.state == magnetar_proto::SegmentState::Sealed
        }),
        "sealed parent remains as an ordering barrier"
    );
    assert_eq!(snap.len(), 3, "sealed parent and two children present");
}

/// A re-sent layout at the epoch already applied is ignored, and the session
/// **survives** it.
///
/// Pulsar 5.0.0-M1 answers the lookup with the current layout and then pushes
/// that same layout again, at the same epoch, on the watch the lookup opened.
/// Until 2026-08-04 that was `DagError::NonMonotonic`, and since any error
/// closes the session, the duplicate blinded the client to every later epoch —
/// including the one carrying the split. Observed only against a real broker:
/// every scripted test here advanced the epoch on each frame, so none of them
/// ever sent the duplicate.
#[test]
fn duplicate_layout_epoch_is_ignored_and_session_survives() {
    let mut conn = connected_conn();
    let session_id = conn
        .open_scalable_topic_session("topic://public/default/scaled")
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();

    // First layout resolves the session at epoch 1.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(
            session_id,
            1,
            vec![seg(1, 0, 65_535, &[], &[], pb::SegmentState::Active)],
        ),
    )
    .expect("initial layout");
    while conn.poll_event().is_some() {}

    // The broker re-sends the very same epoch.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(
            session_id,
            1,
            vec![seg(1, 0, 65_535, &[], &[], pb::SegmentState::Active)],
        ),
    )
    .expect("a duplicate layout is accepted at the frame level");
    let mut emitted = 0;
    while let Some(ev) = conn.poll_event() {
        assert!(
            !matches!(ev, ConnectionEvent::DagWatchClosed { .. }),
            "a duplicate epoch must not close the session"
        );
        emitted += 1;
    }
    assert_eq!(
        emitted, 0,
        "a duplicate epoch changes nothing and says nothing"
    );
    assert!(
        conn.dag_snapshot(session_id).is_some(),
        "the session is still open after the duplicate"
    );

    // The next genuine advance still lands, which is what the duplicate used to
    // cost us.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(
            session_id,
            2,
            vec![
                seg(1, 0, 65_535, &[], &[3, 4], pb::SegmentState::Sealed),
                seg(3, 0, 32_767, &[1], &[], pb::SegmentState::Active),
                seg(4, 32_768, 65_535, &[1], &[], pb::SegmentState::Active),
            ],
        ),
    )
    .expect("split layout");
    let mut saw_split = false;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::SegmentDagUpdated { delta, .. } = ev {
            assert_eq!(delta.epoch, 2);
            assert_eq!(delta.split_events.len(), 1);
            saw_split = true;
        }
    }
    assert!(saw_split, "the advance after a duplicate is applied");
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

/// Encode a `CommandScalableTopicSubscribeResponse` frame.
fn subscribe_response_frame(request_id: u64, epoch: u64, segs: &[(u64, u32, u32)]) -> BytesMut {
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
fn assignment_update_frame(consumer_id: u64, epoch: u64, segs: &[(u64, u32, u32)]) -> BytesMut {
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

fn consumer_assignment(epoch: u64, segs: &[(u64, u32, u32)]) -> pb::ScalableConsumerAssignment {
    pb::ScalableConsumerAssignment {
        layout_epoch: epoch,
        segments: segs
            .iter()
            .map(|&(id, start, end)| pb::ScalableAssignedSegment {
                segment_id: id,
                hash_start: start,
                hash_end: end,
                segment_topic: magnetar_proto::canonical_segment_topic(
                    "topic://public/default/scaled",
                    magnetar_proto::KeyRange::new(start, end).expect("valid inclusive range"),
                    SegmentId(id),
                )
                .expect("canonical segment topic"),
            })
            .collect(),
    }
}

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

fn topics_snapshot_frame(watch_id: u64, topics: &[&str]) -> BytesMut {
    topics_update_frame(
        watch_id,
        pb::command_watch_scalable_topics_update::Event::Snapshot(pb::ScalableTopicsSnapshot {
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
        }),
    )
}

fn topics_diff_frame(watch_id: u64, added: &[&str], removed: &[&str]) -> BytesMut {
    topics_update_frame(
        watch_id,
        pb::command_watch_scalable_topics_update::Event::Diff(pb::ScalableTopicsDiff {
            added: added.iter().map(|t| (*t).to_owned()).collect(),
            removed: removed.iter().map(|t| (*t).to_owned()).collect(),
        }),
    )
}

/// Encode a `CommandWatchTcAssignmentsUpdate` carrying `parallelism` coordinators.
fn tc_assignments_frame(watch_id: u64, parallelism: u32) -> BytesMut {
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
    magnetar_proto::encode_command(&mut buf, &cmd).expect("encode tc assignments");
    buf
}

/// (c) #6 — consumer registration: the subscribe response resolves the
/// consumer's share, naming the `segment://` topics it owns. A layout says what
/// segments exist; an assignment says which of them are this consumer's.
#[test]
fn scalable_consumer_subscribe_resolves_assignment() {
    let mut conn = connected_conn();
    let request_id = conn
        .scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            42,
            magnetar_proto::ScalableConsumerType::Stream,
            magnetar_proto::ControllerIncarnation(1),
        )
        .expect("broker supports scalable topics");

    // The subscribe rides ordinary framing and carries what the controller needs.
    let mut out = conn.poll_transmit();
    let frame = magnetar_proto::decode_one(&mut out).expect("decodes subscribe");
    let sub = frame
        .command
        .scalable_topic_subscribe
        .expect("subscribe payload");
    assert_eq!(sub.request_id, request_id.0);
    assert_eq!(sub.consumer_id, 42);
    assert_eq!(sub.subscription, "sub");
    assert_eq!(sub.consumer_type, pb::ScalableConsumerType::Stream as i32);

    conn.handle_bytes(
        Instant::now(),
        &subscribe_response_frame(request_id.0, 1, &[(1, 0, 32_767)]),
    )
    .expect("subscribe response");

    let mut assigned = None;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ScalableConsumerAssigned {
            consumer_id,
            assignment,
            ..
        } = ev
        {
            assigned = Some((consumer_id, assignment));
        }
    }
    let (consumer_id, assignment) = assigned.expect("ScalableConsumerAssigned emitted");
    assert_eq!(consumer_id, 42);
    assert_eq!(assignment.layout_epoch(), 1);
    assert_eq!(
        assignment.segment_topics(),
        vec!["segment://public/default/scaled/0000-7fff-1"]
    );
}

/// (c) #7 — a rebalance reports exactly what to attach to and detach from,
/// and a lower-epoch push fails the registration closed.
#[test]
fn scalable_consumer_rebalance_reports_delta_and_rejects_stale() {
    let mut conn = connected_conn();
    let request_id = conn
        .scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            42,
            magnetar_proto::ScalableConsumerType::Stream,
            magnetar_proto::ControllerIncarnation(1),
        )
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    conn.handle_bytes(
        Instant::now(),
        &subscribe_response_frame(request_id.0, 1, &[(1, 0, 32_767)]),
    )
    .expect("subscribe response");
    while conn.poll_event().is_some() {}

    // Rebalance: segment 1 goes away, segment 2 arrives.
    conn.handle_bytes(
        Instant::now(),
        &assignment_update_frame(42, 2, &[(2, 32_768, 65_535)]),
    )
    .expect("rebalance");

    let mut delta = None;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ScalableAssignmentChanged { delta: d, .. } = ev {
            delta = Some(d);
        }
    }
    let delta = delta.expect("ScalableAssignmentChanged emitted");
    assert_eq!(delta.layout_epoch, 2);
    assert_eq!(delta.gained.len(), 1);
    assert_eq!(delta.gained[0].segment_id(), SegmentId(2));
    assert_eq!(delta.lost, vec![SegmentId(1)]);

    // A lower epoch fails closed: applying it would hand the consumer segments
    // that no longer exist.
    conn.handle_bytes(
        Instant::now(),
        &assignment_update_frame(42, 1, &[(9, 0, 65_535)]),
    )
    .expect("stale rebalance");
    let rejected = conn.poll_event().expect("stale registration rejected");
    assert!(matches!(
        rejected,
        ConnectionEvent::ScalableConsumerRejected {
            consumer_id: 42,
            reason,
            ..
        } if reason.contains("regressed")
    ));
    assert!(
        conn.scalable_consumer_assignment(42).is_none(),
        "stale assignment removes the registration"
    );
}

/// (c) #8 — the namespace watch applies `removed` before `added`, so a
/// topic named in both lists survives the diff.
#[test]
fn scalable_topics_watch_applies_removed_before_added() {
    let mut conn = connected_conn();
    let watch_id = conn
        .watch_scalable_topics("public/default", vec![])
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();

    conn.handle_bytes(
        Instant::now(),
        &topics_snapshot_frame(watch_id, &["topic://public/default/a"]),
    )
    .expect("snapshot");
    while conn.poll_event().is_some() {}

    conn.handle_bytes(
        Instant::now(),
        &topics_diff_frame(
            watch_id,
            &["topic://public/default/a", "topic://public/default/c"],
            &["topic://public/default/a"],
        ),
    )
    .expect("diff");

    let mut change = None;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ScalableTopicsChanged { change: c, .. } = ev {
            change = Some(c);
        }
    }
    assert!(matches!(
        change.expect("ScalableTopicsChanged emitted"),
        magnetar_proto::TopicsChange::Diff { .. }
    ));
    assert_eq!(
        conn.scalable_topics_snapshot(watch_id).expect("watch open"),
        vec![
            "topic://public/default/a".to_owned(),
            "topic://public/default/c".to_owned()
        ],
        "a topic removed and re-added in one diff survives"
    );
}

/// (c) #9 — **v4 compatibility** for the V5 additions. Neither the
/// consumer registration nor the namespace watch reaches a broker that did not
/// advertise the capability.
#[test]
fn scalable_v5_surface_refused_against_v4_broker() {
    let mut conn = connected_conn_with(false);

    let err = conn
        .scalable_topic_subscribe(
            "topic://public/default/scaled",
            "sub",
            "consumer-a",
            42,
            magnetar_proto::ScalableConsumerType::Stream,
            magnetar_proto::ControllerIncarnation(1),
        )
        .expect_err("v4 broker refuses the registration");
    assert_eq!(err, magnetar_proto::ScalableTopicError::BrokerUnsupported);

    let err = conn
        .watch_scalable_topics("public/default", vec![])
        .expect_err("v4 broker refuses the namespace watch");
    assert_eq!(err, magnetar_proto::ScalableTopicError::BrokerUnsupported);

    assert!(
        conn.poll_transmit().is_empty(),
        "no V5 command may reach a broker that cannot parse it"
    );
}

/// (c) #10 — transaction-coordinator discovery is gated on its **own**
/// feature flag: a broker may serve scalable topics without it, so
/// `supports_scalable_topics` alone must not unlock the watch.
#[test]
fn tc_assignment_discovery_gated_on_its_own_flag() {
    let mut conn = connected_conn();
    assert!(conn.broker_supports_scalable_topics());
    assert!(!conn.broker_supports_tc_metadata_discovery());
    assert_eq!(
        conn.watch_tc_assignments()
            .expect_err("scalable support alone does not unlock TC discovery"),
        magnetar_proto::ScalableTopicError::BrokerUnsupported
    );

    let mut conn = connected_conn_with_flags(true, true);
    let watch_id = conn
        .watch_tc_assignments()
        .expect("broker advertises TC discovery");
    let _ = conn.poll_transmit();

    conn.handle_bytes(Instant::now(), &tc_assignments_frame(watch_id, 2))
        .expect("tc snapshot");
    let mut seen = None;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::TcAssignmentsChanged {
            parallelism,
            assignments,
            ..
        } = ev
        {
            seen = Some((parallelism, assignments));
        }
    }
    let (parallelism, assignments) = seen.expect("TcAssignmentsChanged emitted");
    assert_eq!(parallelism, 2);
    assert_eq!(assignments.len(), 2);
    assert_eq!(assignments[0].tc_id, 0);
}

const PROVIDER_CONTROLLER_PORT: u16 = 6650;
const PROVIDER_SEGMENT_ONE_PORT: u16 = 6651;
const PROVIDER_SEGMENT_TWO_PORT: u16 = 6652;
const PROVIDER_RUN_BUDGET: Duration = Duration::from_secs(30);
const PROVIDER_SEED_COUNT: usize = 4;
const PROVIDER_TOPIC: &str = "topic://public/default/provider-native";

async fn provider_read_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buffer: &mut BytesMut,
) -> std::io::Result<usize> {
    let mut bytes = vec![0; 64 * 1024];
    let read = stream.read(&mut bytes).await?;
    buffer.extend_from_slice(&bytes[..read]);
    Ok(read)
}

async fn provider_fake_session<S>(
    mut stream: S,
    fake: Arc<Mutex<M1FakeCluster>>,
    connection: magnetar_fakes::m1::ConnectionId,
    endpoint: Endpoint,
    published: Arc<Mutex<BTreeSet<u64>>>,
) -> Result<(), String>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let mut input = BytesMut::with_capacity(64 * 1024);
    loop {
        match provider_read_into(&mut stream, &mut input).await {
            Ok(0) => {
                let _ = fake.lock().disconnect_connection(connection);
                return Ok(());
            }
            Ok(_) => {}
            Err(error) => {
                let _ = fake.lock().disconnect_connection(connection);
                return Err(format!("provider fake read failed: {error}"));
            }
        }

        loop {
            let mut framed = input.clone().freeze();
            let before = framed.len();
            let command = match magnetar_proto::decode_one(&mut framed) {
                Ok(frame) => frame.command.r#type,
                Err(FrameError::Incomplete { .. }) => break,
                Err(error) => return Err(format!("provider fake decode failed: {error}")),
            };
            let consumed = before - framed.len();
            let mut frame = input.split_to(consumed).freeze();
            fake.lock()
                .handle_bytes(connection, &mut frame)
                .map_err(|error| format!("provider fake rejected client frame: {error}"))?;
            if command == pb::base_command::Type::Flow as i32
                && let Endpoint::Segment(segment) = endpoint
                && published.lock().insert(segment)
            {
                let payload = match segment {
                    1 => Bytes::from_static(b"segment-one"),
                    2 => Bytes::from_static(b"segment-two"),
                    _ => return Err(format!("unexpected provider segment {segment}")),
                };
                fake.lock()
                    .enqueue_message(segment, payload)
                    .map_err(|error| format!("provider fake enqueue failed: {error}"))?;
            }
        }

        let output = fake
            .lock()
            .take_output(connection)
            .map_err(|error| format!("provider fake output failed: {error}"))?;
        for frame in output {
            stream
                .write_all(&frame)
                .await
                .map_err(|error| format!("provider fake write failed: {error}"))?;
        }
        stream
            .flush()
            .await
            .map_err(|error| format!("provider fake flush failed: {error}"))?;
    }
}

fn provider_authorities(ip: &str, port: u16) -> EndpointAuthorities {
    EndpointAuthorities::new(
        format!("pulsar://{ip}:{port}"),
        format!("pulsar+ssl://{ip}:{}", port.saturating_add(1000)),
    )
}

fn provider_failure(slot: &Mutex<Option<String>>, error: impl Into<String>) {
    let error = error.into();
    eprintln!("provider aggregate broker failed: {error}");
    let mut slot = slot.lock();
    if slot.is_none() {
        *slot = Some(error);
    }
}

fn provider_invalid(error: impl Into<String>) -> SimulationError {
    let error = error.into();
    eprintln!("provider aggregate client failed: {error}");
    SimulationError::InvalidState(error)
}

struct ProviderBroker {
    failure: Arc<Mutex<Option<String>>>,
    finished: Arc<Mutex<CancellationToken>>,
    listeners: Option<Vec<(SimTcpListener, Endpoint)>>,
}

impl ProviderBroker {
    fn new(finished: Arc<Mutex<CancellationToken>>) -> Self {
        Self {
            failure: Arc::new(Mutex::new(None)),
            finished,
            listeners: None,
        }
    }
}

#[async_trait]
impl Workload for ProviderBroker {
    fn name(&self) -> &str {
        "broker"
    }

    async fn setup(&mut self, context: &SimContext) -> SimulationResult<()> {
        *self.finished.lock() = CancellationToken::new();
        let ip = context.my_ip().to_string();
        let controller = context
            .network()
            .bind(&format!("{ip}:{PROVIDER_CONTROLLER_PORT}"))
            .await
            .map_err(|error| SimulationError::InvalidState(format!("controller bind: {error}")))?;
        let segment_one = context
            .network()
            .bind(&format!("{ip}:{PROVIDER_SEGMENT_ONE_PORT}"))
            .await
            .map_err(|error| SimulationError::InvalidState(format!("segment 1 bind: {error}")))?;
        let segment_two = context
            .network()
            .bind(&format!("{ip}:{PROVIDER_SEGMENT_TWO_PORT}"))
            .await
            .map_err(|error| SimulationError::InvalidState(format!("segment 2 bind: {error}")))?;
        self.listeners = Some(vec![
            (controller, Endpoint::Controller),
            (segment_one, Endpoint::Segment(1)),
            (segment_two, Endpoint::Segment(2)),
        ]);
        Ok(())
    }

    async fn run(&mut self, context: &SimContext) -> SimulationResult<()> {
        *self.failure.lock() = None;
        let ip = context.my_ip().to_string();
        let finished = self.finished.lock().clone();
        let listeners = self.listeners.take().ok_or_else(|| {
            SimulationError::InvalidState("provider listeners missing after setup".to_owned())
        })?;
        let config = M1FakeConfig::new(PROVIDER_TOPIC)
            .map_err(|error| SimulationError::InvalidState(error.to_string()))?
            .with_endpoint_authorities(
                Endpoint::Controller,
                provider_authorities(&ip, PROVIDER_CONTROLLER_PORT),
            )
            .with_endpoint_authorities(
                Endpoint::Segment(1),
                provider_authorities(&ip, PROVIDER_SEGMENT_ONE_PORT),
            )
            .with_endpoint_authorities(
                Endpoint::Segment(2),
                provider_authorities(&ip, PROVIDER_SEGMENT_TWO_PORT),
            );
        let fake = M1FakeCluster::from_config(config)
            .map_err(|error| SimulationError::InvalidState(error.to_string()))?;
        let fake = Arc::new(Mutex::new(fake));
        let published = Arc::new(Mutex::new(BTreeSet::new()));
        let tasks = context.providers().task().clone();
        for (listener, endpoint) in listeners {
            let fake = fake.clone();
            let failure = self.failure.clone();
            let published = published.clone();
            let shutdown = context.shutdown().clone();
            let session_tasks = tasks.clone();
            std::mem::drop(tasks.spawn_task("scalable-aggregate-accept", async move {
                loop {
                    moonpool_sim::select! {
                        () = shutdown.cancelled() => return,
                        accepted = listener.accept() => match accepted {
                            Ok((stream, _)) => {
                                let connection = match fake.lock().open_connection(endpoint) {
                                    Ok(connection) => connection,
                                    Err(error) => {
                                        provider_failure(&failure, error.to_string());
                                        return;
                                    }
                                };
                                let fake = fake.clone();
                                let failure = failure.clone();
                                let session_published = published.clone();
                                std::mem::drop(session_tasks.spawn_task(
                                    "scalable-aggregate-session",
                                    async move {
                                        if let Err(error) = provider_fake_session(
                                            stream,
                                            fake.clone(),
                                            connection,
                                            endpoint,
                                            session_published,
                                        )
                                        .await
                                        {
                                            provider_failure(&failure, error);
                                            let _ = fake.lock().disconnect_connection(connection);
                                        }
                                    },
                                ));
                            }
                            Err(error) => {
                                provider_failure(&failure, format!("accept failed: {error}"));
                                return;
                            }
                        }
                    }
                }
            }));
        }
        moonpool_sim::select! {
            () = context.shutdown().cancelled() => {}
            () = finished.cancelled() => {}
        }
        if let Some(error) = self.failure.lock().clone() {
            return Err(SimulationError::InvalidState(error));
        }
        let counts = fake.lock().resource_counts();
        if counts.ledger_messages != 2 || counts.unacked_messages != 0 {
            return Err(SimulationError::InvalidState(format!(
                "provider fake delivery state invalid: {counts:?}"
            )));
        }
        Ok(())
    }
}

struct ProviderAggregateClient {
    finished: Arc<Mutex<CancellationToken>>,
}

impl ProviderAggregateClient {
    fn new(finished: Arc<Mutex<CancellationToken>>) -> Self {
        Self { finished }
    }
}

#[async_trait]
impl Workload for ProviderAggregateClient {
    fn name(&self) -> &str {
        "client"
    }

    #[allow(clippy::too_many_lines)]
    async fn run(&mut self, context: &SimContext) -> SimulationResult<()> {
        let _finished = self.finished.lock().clone().drop_guard();
        let broker = context
            .peer("broker")
            .ok_or_else(|| SimulationError::InvalidState("broker peer missing".to_owned()))?;
        let address = format!("{broker}:{PROVIDER_CONTROLLER_PORT}");
        let providers = context.providers().clone();
        let time = providers.time().clone();
        let engine: MoonpoolEngine<SimProviders> = MoonpoolEngine::new(providers);
        let config = ConnectionConfig {
            connect_timeout: Duration::from_millis(100),
            operation_timeout: Duration::from_secs(5),
            supervisor: Some(magnetar_proto::SupervisorConfig::default()),
            ..ConnectionConfig::default()
        };
        let client = match time
            .timeout(
                Duration::from_secs(10),
                Client::connect_plain_supervised(&engine, &address, config, None, None),
            )
            .await
        {
            Ok(Ok(client)) => client,
            other => {
                return Err(provider_invalid(format!(
                    "runtime connect failed: {other:?}"
                )));
            }
        };
        let subscriber = match client.segment_subscriber() {
            Ok(subscriber) => subscriber,
            Err(error) => {
                client.close().await;
                return Err(provider_invalid(error.to_string()));
            }
        };
        let receiver_budget = magnetar_proto::ReceiverBudget::bytes(16 * 1024 * 1024)
            .map_err(|error| SimulationError::InvalidState(error.to_string()))?;
        let consumer = match time
            .timeout(
                Duration::from_secs(10),
                subscriber.subscribe_stream_consumer(StreamConsumerOptions {
                    topic: PROVIDER_TOPIC.to_owned(),
                    subscription: "provider-native-sub".to_owned(),
                    consumer_name: "provider-native-member".to_owned(),
                    schema: pb::Schema::default(),
                    receiver_budget,
                    ordering_mode: magnetar_proto::OrderingMode::BrokerManaged,
                }),
            )
            .await
        {
            Ok(Ok(consumer)) => consumer,
            other => {
                client.close().await;
                return Err(provider_invalid(format!(
                    "aggregate subscribe failed: {other:?}"
                )));
            }
        };

        let mut messages = Vec::new();
        for _ in 0..2 {
            match time
                .timeout(Duration::from_secs(10), consumer.receive())
                .await
            {
                Ok(Ok(message)) => messages.push(message),
                other => {
                    let status = consumer.status();
                    let mut events = Vec::new();
                    for _ in 0..16 {
                        match time
                            .timeout(Duration::from_millis(1), consumer.next_event())
                            .await
                        {
                            Ok(Ok(Some(event))) => events.push(event),
                            Ok(Ok(None) | Err(_)) | Err(_) => break,
                        }
                    }
                    consumer.close_best_effort();
                    client.close().await;
                    return Err(provider_invalid(format!(
                        "aggregate receive failed: {other:?}; status={status:?}; events={events:?}"
                    )));
                }
            }
        }

        let mut payloads: Vec<Vec<u8>> = messages
            .iter()
            .map(|message| message.message.payload.to_vec())
            .collect();
        payloads.sort();
        if payloads != vec![b"segment-one".to_vec(), b"segment-two".to_vec()] {
            return Err(provider_invalid(format!(
                "aggregate payloads diverged: {payloads:?}"
            )));
        }
        let mut segments: Vec<u64> = messages
            .iter()
            .map(|message| message.token.stream_message_id().source().segment_id().0)
            .collect();
        segments.sort_unstable();
        if segments != vec![1, 2] {
            return Err(provider_invalid(format!(
                "aggregate sources diverged: {segments:?}"
            )));
        }
        let status = consumer.status();
        if status.assigned_segments() != 2
            || status.attached_segments() != 2
            || status.receiver_budget_used() == 0
            || status.receiver_budget_used() > status.receiver_budget_limit()
        {
            return Err(provider_invalid(format!(
                "aggregate status invalid: {status:?}"
            )));
        }
        if consumer.delivered_position().len() != 2 {
            return Err(provider_invalid(
                "both simulated sources must contribute to the delivered position".to_owned(),
            ));
        }
        for message in &messages {
            match time
                .timeout(
                    Duration::from_secs(10),
                    consumer.acknowledge(&message.token),
                )
                .await
            {
                Ok(Ok(())) => {}
                other => {
                    return Err(provider_invalid(format!(
                        "aggregate acknowledgement failed: {other:?}"
                    )));
                }
            }
        }
        match time
            .timeout(Duration::from_secs(10), consumer.close())
            .await
        {
            Ok(Ok(())) => {}
            other => {
                client.close().await;
                return Err(provider_invalid(format!(
                    "aggregate close failed: {other:?}"
                )));
            }
        }
        client.close().await;
        Ok(())
    }
}

/// Run the complete Moonpool runtime aggregate over SimProviders controller and
/// segment sockets. `sweep_seeds` derives every explored schedule from
/// `MOONPOOL_SEED`, so a reported daily-sweep seed replays the aggregate's task,
/// network, receive, acknowledgement, and close interleavings.
#[test]
fn scalable_aggregate_runs_on_sim_providers_across_seeded_schedules() {
    let finished = Arc::new(Mutex::new(CancellationToken::new()));
    let report = SimulationBuilder::new()
        .workload(ProviderBroker::new(finished.clone()))
        .workload(ProviderAggregateClient::new(finished))
        .run_time_budget(PROVIDER_RUN_BUDGET)
        .set_debug_seeds(sweep_seeds(PROVIDER_SEED_COUNT))
        .set_iterations(PROVIDER_SEED_COUNT)
        .run();
    assert_eq!(report.failed_runs, 0, "report: {report:?}");
    assert_eq!(
        report.successful_runs, PROVIDER_SEED_COUNT,
        "every seeded aggregate schedule must complete: {report:?}"
    );
}
