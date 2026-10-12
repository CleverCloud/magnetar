// SPDX-License-Identifier: Apache-2.0

//! PIP-460 scalable-topic waiter lost-wakeup regression — tokio engine.
//!
//! `Client::scalable_topic_lookup`, `Client::scalable_topic_subscribe` and
//! `Client::next_scalable_event` used to drain `scalable_events` and re-check
//! `is_closed()` *before* creating the `Notified` they park on. The driver's
//! `emit_scalable` pushes the event and then fires `scalable_notify.notify_waiters()`,
//! which stores **no permit**: an event pushed between the drain and the
//! (too-late) `notified()` was buffered, the waiter parked, and nothing woke
//! it again — the scripted broker sends each of these events exactly once, so
//! the `scalable_assignment_rebalance_parity` differential test hung until the
//! CI job timeout. The fix arms the `Notified` *before* the drain (the
//! enroll-before-drain idiom of `next_replicated_subscription_marker`),
//! mirrored 1:1 across both engines.
//!
//! The window is three synchronous statements wide (buffer unlock, `inner`
//! lock for `is_closed()`, `notified()` creation), so no poll order, paused
//! clock or simulator schedule can land an event inside it: it needs a second
//! thread. These tests widen it instead of racing it. Each holds the two locks
//! the waiter takes on its way through the window and hands them over in a
//! fixed order:
//!
//! 1. the test holds the buffer lock, so the waiter parks on its first drain;
//! 2. the test also takes the connection lock, then releases the buffer lock *fairly*, which hands
//!    it straight to the parked waiter — the waiter drains an empty buffer and moves on to
//!    `is_closed()`, where it blocks on the connection lock the test still holds;
//! 3. the test's next buffer-lock acquisition queues behind that hand-off, so it returns only once
//!    the waiter's drain is over: the waiter is now provably inside the window;
//! 4. the test does exactly what `emit_scalable` does — push, then `notify_waiters()` — and
//!    releases the connection lock.
//!
//! A waiter that enrolled before draining is woken by step 4 and returns the
//! event; one that enrols after it parks forever and the `HANG_GUARD` fires.
//! The only timing assumption is step 1: a short settle gives the freshly
//! spawned waiter time to reach its first lock. It errs in one direction only —
//! a waiter that is still late finds the event in its drain, so the test can
//! pass spuriously against a regression but can never fail against the fix.
//!
//! ADR-0024: this is the tokio integration twin; the moonpool twin is
//! `scalable_lost_wakeup.rs` (1:1, `check-runtime-test-parity`).

#![cfg(feature = "scalable-topics")]
#![forbid(unsafe_code)]
#![allow(clippy::expect_used)]
#![allow(clippy::doc_markdown)]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    ConnectionConfig, FrameError, ScalableConsumerType, decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::{Client, ConnectionShared, ScalableEvent};
use parking_lot::MutexGuard;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
mod common;
use common::HANG_GUARD;

/// How long the freshly spawned waiter gets to reach its first lock before the
/// test takes the connection lock. See the module docs: too short can only make
/// a regression pass spuriously, never make the fix fail.
const SETTLE: Duration = Duration::from_millis(250);

/// Minimal broker: `CONNECT` -> `CONNECTED` advertising PIP-460, `PING` ->
/// `PONG`, and silence for everything else. The silence is the point: the
/// scalable commands the waiters emit are never answered, so the only event a
/// waiter can see is the one the test delivers.
async fn serve_scalable_broker(mut stream: TcpStream) {
    let mut read_buf = BytesMut::with_capacity(8 * 1024);
    let mut out = BytesMut::with_capacity(8 * 1024);
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            match pb::base_command::Type::try_from(frame.command.r#type) {
                Ok(pb::base_command::Type::Connect) => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Connected as i32,
                        connected: Some(pb::CommandConnected {
                            server_version: "scalable-silent-broker".to_owned(),
                            protocol_version: Some(21),
                            max_message_size: Some(5 * 1024 * 1024),
                            feature_flags: Some(pb::FeatureFlags {
                                supports_scalable_topics: Some(true),
                                ..pb::FeatureFlags::default()
                            }),
                        }),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out, &cmd);
                }
                Ok(pb::base_command::Type::Ping) => {
                    let cmd = pb::BaseCommand {
                        r#type: pb::base_command::Type::Pong as i32,
                        pong: Some(pb::CommandPong {}),
                        ..Default::default()
                    };
                    let _ = encode_command(&mut out, &cmd);
                }
                _ => {}
            }
        }
        if !out.is_empty() && stream.write_all(&out.split()).await.is_err() {
            return;
        }
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => read_buf.extend_from_slice(&chunk[..n]),
        }
    }
}

/// A handshaked client whose driver is idle: the broker never volunteers a
/// scalable event.
async fn connect_to_silent_broker() -> Arc<Client> {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind broker");
    let port = listener.local_addr().expect("local_addr").port();
    tokio::spawn(async move {
        if let Ok((stream, _peer)) = listener.accept().await {
            serve_scalable_broker(stream).await;
        }
    });
    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("client connect");
    let client = tokio::time::timeout(
        HANG_GUARD,
        Client::from_socket(stream, ConnectionConfig::default()),
    )
    .await
    .expect("handshake did not time out")
    .expect("handshake");
    assert!(
        client.broker_supports_scalable_topics(),
        "the silent broker must advertise PIP-460 or the client refuses the scalable commands"
    );
    Arc::new(client)
}

/// The waiter thread has been joined, so the test holds the only reference.
async fn close_client(client: Arc<Client>) {
    Arc::into_inner(client)
        .expect("the joined waiter released its client reference")
        .close()
        .await;
}

/// Run `fut` to completion on the calling (waiter) thread, bounded by the
/// anti-hang backstop.
fn bounded_on_this_thread<F: Future>(fut: F) -> Result<F::Output, tokio::time::error::Elapsed> {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("waiter runtime")
        .block_on(async { tokio::time::timeout(HANG_GUARD, fut).await })
}

/// Run `waiter` on its own thread, drive it into the window between its buffer
/// drain and its park, and deliver `event` there exactly as the driver's
/// `emit_scalable` does (push, then `notify_waiters()`). Returns what the waiter
/// returned. See the module docs for the lock hand-off that pins the waiter.
fn deliver_in_the_race_window<T: Send + 'static>(
    shared: &ConnectionShared,
    waiter: impl FnOnce() -> T + Send + 'static,
    event: ScalableEvent,
) -> T {
    let drain_gate = shared.scalable_events.lock();

    let started = Arc::new(AtomicBool::new(false));
    let waiter = {
        let started = Arc::clone(&started);
        std::thread::spawn(move || {
            started.store(true, Ordering::SeqCst);
            waiter()
        })
    };
    while !started.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    std::thread::sleep(SETTLE);

    // The waiter is parked on `drain_gate`. Take the lock its `is_closed()`
    // re-check needs, then hand the buffer lock to it.
    let check_gate = shared.inner.lock();
    MutexGuard::unlock_fair(drain_gate);

    // Queued behind the hand-off: returns only once the waiter's drain is over.
    let mut drain_gate = shared.scalable_events.lock();
    drain_gate.push_back(event);
    drop(drain_gate);
    shared.scalable_notify.notify_waiters();
    drop(check_gate);

    waiter.join().expect("waiter thread panicked")
}

/// `next_scalable_event` must return an event the driver pushed after its
/// drain found the buffer empty and before it parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn next_scalable_event_catches_event_pushed_between_drain_and_park() {
    let client = connect_to_silent_broker().await;

    let waiter_client = Arc::clone(&client);
    let outcome = deliver_in_the_race_window(
        client.shared(),
        move || bounded_on_this_thread(waiter_client.next_scalable_event()),
        ScalableEvent::DagWatchClosed {
            session_id: 9,
            reason: Some("scripted".to_owned()),
        },
    );

    let event = outcome
        .expect("next_scalable_event returned rather than hanging (a lost wakeup would hang here)")
        .expect("connection still open");
    assert!(
        matches!(event, ScalableEvent::DagWatchClosed { session_id: 9, .. }),
        "the delivered event reaches the waiter: {event:?}"
    );

    close_client(client).await;
}

/// `scalable_topic_lookup` must resolve on a layout the driver pushed after its
/// drain found the buffer empty and before it parked. A fresh connection
/// allocates scalable session id 1, which is the id the delivered event names.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_lookup_catches_resolution_pushed_between_drain_and_park() {
    let client = connect_to_silent_broker().await;

    let waiter_client = Arc::clone(&client);
    let outcome = deliver_in_the_race_window(
        client.shared(),
        move || {
            bounded_on_this_thread(
                waiter_client.scalable_topic_lookup("topic://public/default/lost-wakeup"),
            )
        },
        ScalableEvent::LookupResolved {
            session_id: 1,
            resolved_topic_name: Some("topic://public/default/lost-wakeup".to_owned()),
            controller_broker_url: None,
            segments: Vec::new(),
            epoch: 3,
        },
    );

    let lookup = outcome
        .expect(
            "scalable_topic_lookup returned rather than hanging (a lost wakeup would hang here)",
        )
        .expect("lookup resolved");
    assert_eq!(lookup.session_id, 1);
    assert_eq!(lookup.epoch, 3);
    assert_eq!(
        lookup.resolved_topic_name.as_deref(),
        Some("topic://public/default/lost-wakeup")
    );

    close_client(client).await;
}

/// `scalable_topic_subscribe` must surface a rejection the driver pushed after
/// its drain found the buffer empty and before it parked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scalable_subscribe_catches_rejection_pushed_between_drain_and_park() {
    let client = connect_to_silent_broker().await;

    let waiter_client = Arc::clone(&client);
    let outcome = deliver_in_the_race_window(
        client.shared(),
        move || {
            bounded_on_this_thread(waiter_client.scalable_topic_subscribe(
                "topic://public/default/lost-wakeup",
                "sub",
                "consumer-a",
                77,
                ScalableConsumerType::Stream,
            ))
        },
        ScalableEvent::ConsumerRejected {
            consumer_id: 77,
            reason: "scripted rejection".to_owned(),
        },
    );

    let err = outcome
        .expect(
            "scalable_topic_subscribe returned rather than hanging (a lost wakeup would hang here)",
        )
        .expect_err("the delivered rejection is an error")
        .to_string();
    assert!(
        err.contains("scripted rejection"),
        "the delivered reason reaches the caller: {err}"
    );

    close_client(client).await;
}
