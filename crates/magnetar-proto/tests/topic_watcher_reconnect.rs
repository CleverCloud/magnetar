// SPDX-License-Identifier: Apache-2.0

//! A PIP-145 topic-list watcher survives a supervised reconnect.
//!
//! The broker keeps a topic-list watcher per connection: it dies with the TCP session that
//! registered it, and a new session pushes no `CommandWatchTopicUpdate` until the client sends
//! `CommandWatchTopicList` again. `Connection::reset()` replaces the whole watcher registry, and
//! the supervisor re-attaches only producers (`rebuild_producers`) and consumers
//! (`rebuild_consumers`) once the handshake completes. A `PatternConsumer` built before the
//! reconnect therefore never hears of a new or deleted topic again, and nothing reports the loss.
//!
//! The test drives the supervisor's exact sequence (`reset`, `begin_handshake`, `CONNECTED`,
//! `rebuild_producers`, `rebuild_consumers`) and collects every frame the connection emits on the
//! new session. If the fix adds a separate rebuild entry point for watchers, call it here beside
//! the two existing ones.
//!
//! Layer (a) of the ADR-0024 four-layer test policy.

use std::time::Instant;

use magnetar_proto::{Connection, ConnectionConfig, decode_one, encode_command, pb};

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

fn new_connection() -> Connection {
    Connection::new(
        ConnectionConfig::default(),
        std::sync::Arc::new(std::time::SystemTime::now),
    )
}

fn connected_frame() -> bytes::BytesMut {
    let connected = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    encode_command(&mut buf, &connected).expect("encode connected");
    buf
}

/// Decode every frame staged on the connection's outbound buffer.
fn drain_commands(conn: &mut Connection) -> Vec<pb::BaseCommand> {
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

/// A watcher the broker has acknowledged (the shape a `PatternConsumer` holds) must be registered
/// again on the new session: without a fresh `CommandWatchTopicList` the broker pushes no
/// update for it, and the pattern consumer silently stops discovering topics.
#[test]
fn acknowledged_topic_watcher_is_registered_again_after_a_reconnect() {
    let now = Instant::now();
    let mut conn = new_connection();
    conn.begin_handshake().expect("begin_handshake");
    conn.handle_bytes(now, &connected_frame())
        .expect("apply CONNECTED");
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
    let mut ack = bytes::BytesMut::new();
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
    conn.handle_bytes(now, &connected_frame())
        .expect("apply reconnect CONNECTED");
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
