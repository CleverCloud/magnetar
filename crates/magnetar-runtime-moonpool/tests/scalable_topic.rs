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
        &layout_frame(session_id, 1, vec![seg(1, 0, 65_536, &[])]),
    )
    .expect("initial layout");
    while conn.poll_event().is_some() {}

    // The broker re-sends the very same epoch.
    conn.handle_bytes(
        Instant::now(),
        &layout_frame(session_id, 1, vec![seg(1, 0, 65_536, &[])]),
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
            vec![seg(3, 0, 32_768, &[1]), seg(4, 32_768, 65_536, &[1])],
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
                segment_topic: format!("segment://public/default/scaled/{id}"),
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
        &subscribe_response_frame(request_id.0, 1, &[(1, 0, 32_768)]),
    )
    .expect("subscribe response");

    let mut assigned = None;
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::ScalableConsumerAssigned {
            consumer_id,
            assignment,
        } = ev
        {
            assigned = Some((consumer_id, assignment));
        }
    }
    let (consumer_id, assignment) = assigned.expect("ScalableConsumerAssigned emitted");
    assert_eq!(consumer_id, 42);
    assert_eq!(assignment.layout_epoch, 1);
    assert_eq!(
        assignment.segment_topics(),
        vec!["segment://public/default/scaled/1"]
    );
}

/// (c) #7 — a rebalance reports exactly what to attach to and detach from,
/// and a stale push is rejected rather than applied.
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
        )
        .expect("broker supports scalable topics");
    let _ = conn.poll_transmit();
    conn.handle_bytes(
        Instant::now(),
        &subscribe_response_frame(request_id.0, 1, &[(1, 0, 32_768)]),
    )
    .expect("subscribe response");
    while conn.poll_event().is_some() {}

    // Rebalance: segment 1 goes away, segment 2 arrives.
    conn.handle_bytes(
        Instant::now(),
        &assignment_update_frame(42, 2, &[(2, 32_768, 65_536)]),
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
    assert_eq!(delta.gained[0].segment_id, SegmentId(2));
    assert_eq!(delta.lost, vec![SegmentId(1)]);

    // A stale push is dropped: the broker recomputes assignments per layout, so
    // applying it would hand the consumer segments that no longer exist.
    conn.handle_bytes(
        Instant::now(),
        &assignment_update_frame(42, 1, &[(9, 0, 65_536)]),
    )
    .expect("stale rebalance");
    assert!(
        !conn
            .poll_event()
            .is_some_and(|ev| matches!(ev, ConnectionEvent::ScalableAssignmentChanged { .. })),
        "a stale assignment emits no change"
    );
    let held = conn
        .scalable_consumer_assignment(42)
        .expect("still registered");
    assert_eq!(held.layout_epoch, 2, "session kept the newer assignment");
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
