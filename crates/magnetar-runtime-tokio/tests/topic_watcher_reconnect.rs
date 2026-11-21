// SPDX-License-Identifier: Apache-2.0

//! A PIP-145 topic-list watcher survives a supervised reconnect, exercised through the tokio
//! engine's [`ConnectionShared`] state. Mirrors the matching `magnetar-runtime-moonpool` test, its
//! `magnetar-proto` unit test (`crates/magnetar-proto/tests/topic_watcher_reconnect.rs`) and the
//! `magnetar-differential` equivalence scenario.
//!
//! The broker keeps a topic-list watcher per connection and a new session pushes no
//! `CommandWatchTopicUpdate` until the client sends `CommandWatchTopicList` again.
//! `Connection::reset()` replaces the whole watcher registry and the supervisor re-attaches only
//! producers and consumers, so a `PatternConsumer` built before the reconnect never hears of a new
//! or deleted topic again. If the fix adds a separate rebuild entry point for watchers, call it
//! here beside the two existing ones.
//!
//! Layer (b) of the ADR-0024 four-layer test policy.

mod common;

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, decode_one, encode_command, pb};
use magnetar_runtime_tokio::ConnectionShared;

use crate::common::handshake_response_bytes;

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

/// Decode every frame staged on the connection's outbound buffer.
fn drain_commands(conn: &mut magnetar_proto::Connection) -> Vec<pb::BaseCommand> {
    let mut bytes = conn.poll_transmit();
    let mut commands = Vec::new();
    while !bytes.is_empty() {
        commands.push(
            decode_one(&mut bytes)
                .expect("staged frame must decode")
                .command,
        );
    }
    commands
}

/// The `CommandWatchTopicList` frames among `commands`, as `(namespace, pattern)`.
fn watch_requests(commands: &[pb::BaseCommand]) -> Vec<(String, String)> {
    commands
        .iter()
        .filter_map(|command| command.watch_topic_list.as_ref())
        .map(|watch| (watch.namespace.clone(), watch.topics_pattern.clone()))
        .collect()
}

#[test]
fn acknowledged_topic_watcher_is_registered_again_after_a_reconnect() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

    // Register the watcher and let the broker acknowledge it. The pre-reset frame proves that
    // the helper decodes a `CommandWatchTopicList` and that the assertion below can match one.
    let request_id = conn.watch_topic_list(NAMESPACE, PATTERN);
    let first_session = drain_commands(&mut conn);
    assert_eq!(
        watch_requests(&first_session),
        vec![(NAMESPACE.to_owned(), PATTERN.to_owned())],
        "the first session must carry exactly the watcher's registration"
    );
    let watcher_id = first_session
        .iter()
        .find_map(|command| command.watch_topic_list.as_ref())
        .map(|watch| watch.watcher_id)
        .expect("the registration frame names a watcher id");
    let mut ack = BytesMut::new();
    encode_command(
        &mut ack,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::WatchTopicListSuccess as i32,
            watch_topic_list_success: Some(pb::CommandWatchTopicListSuccess {
                request_id: request_id.0,
                watcher_id,
                topic: vec!["persistent://public/default/orders-1".to_owned()],
                topics_hash: "hash-1".to_owned(),
            }),
            ..Default::default()
        },
    )
    .expect("encode CommandWatchTopicListSuccess");
    conn.handle_bytes(now, &ack)
        .expect("apply CommandWatchTopicListSuccess");
    while conn.poll_event().is_some() {}

    // The transport drops: the supervisor resets, redials, re-handshakes, then re-attaches.
    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("reconnect Connected");
    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();
    let second_session = drain_commands(&mut conn);

    assert_eq!(
        watch_requests(&second_session),
        vec![(NAMESPACE.to_owned(), PATTERN.to_owned())],
        "the new session must register the same topic-list watcher again; the frames it carries \
         are {:?}",
        second_session
            .iter()
            .map(|command| command.r#type)
            .collect::<Vec<_>>()
    );
}
