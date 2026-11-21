// SPDX-License-Identifier: Apache-2.0

//! A PIP-145 topic-list watcher survives a supervised reconnect, identically on both engines
//! (ADR-0024 layer (d)).
//!
//! The broker keeps a topic-list watcher per connection, and a new session pushes no
//! `CommandWatchTopicUpdate` until the client sends `CommandWatchTopicList` again.
//! `Connection::reset()` replaces the whole watcher registry and the supervisor re-attaches only
//! producers and consumers, so a `PatternConsumer` built before the reconnect never hears of a new
//! or deleted topic again. Both engines wrap the same `magnetar-proto` state machine, so they must
//! agree, and the new session must carry the watcher's registration. If the fix adds a separate
//! rebuild entry point for watchers, call it here beside the two existing ones.

use std::time::Instant;

use bytes::BytesMut;
use magnetar_proto::{Connection, ConnectionConfig, decode_one, encode_command, pb};

const NAMESPACE: &str = "public/default";
const PATTERN: &str = "persistent://public/default/orders-.*";

/// The `(namespace, pattern)` of each `CommandWatchTopicList` a session carries.
type Registrations = Vec<(String, String)>;

fn connected_frame() -> BytesMut {
    let mut frame = BytesMut::new();
    encode_command(
        &mut frame,
        &pb::BaseCommand {
            r#type: pb::base_command::Type::Connected as i32,
            connected: Some(pb::CommandConnected {
                server_version: "magnetar-differential".to_owned(),
                protocol_version: Some(magnetar_proto::SUPPORTED_PROTOCOL_VERSION),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: Some(pb::FeatureFlags::default()),
            }),
            ..Default::default()
        },
    )
    .expect("encode Connected");
    frame
}

fn tokio_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
}

fn moonpool_projection<T>(run: impl FnOnce(&mut Connection, Instant) -> T) -> T {
    let at = Instant::now();
    let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();
    run(&mut conn, at)
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
fn watch_requests(commands: &[pb::BaseCommand]) -> Registrations {
    commands
        .iter()
        .filter_map(|command| command.watch_topic_list.as_ref())
        .map(|watch| (watch.namespace.clone(), watch.topics_pattern.clone()))
        .collect()
}

/// Registers an acknowledged watcher, drops the transport, and returns the watcher registrations
/// the connection emits on the first session and on the new one, in that order.
fn registrations_per_session(conn: &mut Connection, at: Instant) -> (Registrations, Registrations) {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(at, &connected_frame())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

    let request_id = conn.watch_topic_list(NAMESPACE, PATTERN);
    let first_session = drain_commands(conn);
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
    conn.handle_bytes(at, &ack)
        .expect("CommandWatchTopicListSuccess");
    while conn.poll_event().is_some() {}

    conn.reset();
    conn.begin_handshake().expect("re-handshake");
    conn.handle_bytes(at, &connected_frame())
        .expect("reconnect Connected");
    let _ = conn.rebuild_producers();
    let _ = conn.rebuild_consumers();
    let second_session = drain_commands(conn);

    (
        watch_requests(&first_session),
        watch_requests(&second_session),
    )
}

#[test]
fn engines_register_an_acknowledged_topic_watcher_again_after_a_reconnect() {
    let tokio = tokio_projection(registrations_per_session);
    let moonpool = moonpool_projection(registrations_per_session);
    assert_eq!(
        tokio, moonpool,
        "engines must agree on the watcher registrations of each session"
    );

    let registration = vec![(NAMESPACE.to_owned(), PATTERN.to_owned())];
    let (first, second) = tokio;
    assert_eq!(
        first, registration,
        "the first session must carry exactly the watcher's registration"
    );
    assert_eq!(
        second, registration,
        "the new session must register the same topic-list watcher again"
    );
}
