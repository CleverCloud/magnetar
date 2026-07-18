// SPDX-License-Identifier: Apache-2.0

//! Cancelling a lookup-style future before the broker response lands must
//! NOT leave a [`std::task::Waker`] orphaned in
//! [`magnetar_proto::Connection`]'s `wakers` slab.
//!
//! Moonpool mirror of
//! `crates/magnetar-runtime-tokio/tests/lookup_drop_unregister.rs`. See
//! that file for the rationale; this one exists to satisfy ADR-0024
//! four-layer parity for the lookup multi-agent review MEDIUM-4 finding.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_moonpool::{Client, ClientError, MoonpoolEngine};
use moonpool_core::TokioProviders;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
mod common;
use common::HANG_GUARD;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordedKind(i32);

async fn spawn_silent_lookup_broker() -> (String, Arc<Mutex<Vec<RecordedKind>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let log: Arc<Mutex<Vec<RecordedKind>>> = Arc::new(Mutex::new(Vec::new()));
    let log_clone = log.clone();
    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut read_buf = BytesMut::with_capacity(8 * 1024);
        let mut out_buf = BytesMut::with_capacity(8 * 1024);
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
                log_clone.lock().push(RecordedKind(frame.command.r#type));
                handle_frame(&frame, &mut out_buf);
            }

            if !out_buf.is_empty() {
                if stream.write_all(&out_buf).await.is_err() {
                    return;
                }
                if stream.flush().await.is_err() {
                    return;
                }
                out_buf.clear();
            }

            match stream.read_buf(&mut read_buf).await {
                Ok(0) | Err(_) => return,
                Ok(_) => {}
            }
        }
    });
    (addr.to_string(), log)
}

fn handle_frame(frame: &magnetar_proto::Frame, out: &mut BytesMut) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Connected as i32,
                connected: Some(pb::CommandConnected {
                    server_version: "magnetar-lookup-drop-test".to_owned(),
                    protocol_version: Some(21),
                    max_message_size: Some(5 * 1024 * 1024),
                    feature_flags: Some(pb::FeatureFlags::default()),
                }),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        pb::base_command::Type::Ping => {
            let cmd = pb::BaseCommand {
                r#type: pb::base_command::Type::Pong as i32,
                pong: Some(pb::CommandPong {}),
                ..Default::default()
            };
            let _ = encode_command(out, &cmd);
        }
        // PartitionedMetadata / Lookup intentionally unanswered — we want
        // the user-facing future to stay Pending until the caller drops it.
        _ => {}
    }
}

async fn assert_lookup_deadline(client: &Client<TokioProviders>) {
    let mut deadline = Box::pin(tokio::time::sleep(Duration::from_millis(20)));
    let mut last_broker_error = None;
    let error = client
        .lookup_topic_with_operation_deadline(
            "persistent://public/default/lookup-deadline",
            false,
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
        .expect_err("silent lookup must resolve on its explicit deadline");
    assert!(
        matches!(error, ClientError::Other(ref message)
            if message == "topic lookup exceeded operation_timeout"),
        "unexpected lookup deadline error: {error:?}"
    );
}

async fn assert_metadata_deadlines(client: &Client<TokioProviders>) {
    let mut expired = Box::pin(async {});
    let mut last_broker_error = None;
    let preflight_error = client
        .partitioned_topic_metadata_with_operation_deadline(
            "persistent://public/default/metadata-preflight",
            expired.as_mut(),
            &mut last_broker_error,
        )
        .await
        .expect_err("ready metadata deadline must fail before enqueue");
    assert!(
        matches!(preflight_error, ClientError::Other(ref message)
            if message == "partitioned topic metadata exceeded operation_timeout"),
        "unexpected metadata preflight error: {preflight_error:?}"
    );

    let mut deadline = Box::pin(tokio::time::sleep(Duration::from_millis(20)));
    let mut last_broker_error = None;
    let error = client
        .partitioned_topic_metadata_with_operation_deadline(
            "persistent://public/default/metadata-deadline",
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
        .expect_err("silent metadata request must resolve on its explicit deadline");
    assert!(
        matches!(error, ClientError::Other(ref message)
            if message == "partitioned topic metadata exceeded operation_timeout"),
        "unexpected metadata deadline error: {error:?}"
    );
}

async fn assert_watcher_deadlines_and_cancellation(client: &Client<TokioProviders>) {
    let mut expired = Box::pin(async {});
    let mut last_broker_error = None;
    let preflight_error = client
        .watch_topic_list_with_operation_deadline(
            "public/default",
            "preflight-.*",
            expired.as_mut(),
            &mut last_broker_error,
        )
        .await
        .expect_err("ready watcher deadline must fail before enqueue");
    assert!(
        matches!(preflight_error, ClientError::Other(ref message)
            if message == "topic-list snapshot exceeded operation_timeout"),
        "unexpected watcher preflight error: {preflight_error:?}"
    );

    let mut deadline = Box::pin(tokio::time::sleep(Duration::from_millis(20)));
    let mut last_broker_error = None;
    let error = client
        .watch_topic_list_with_operation_deadline(
            "public/default",
            "deadline-.*",
            deadline.as_mut(),
            &mut last_broker_error,
        )
        .await
        .expect_err("silent watcher request must resolve on its explicit deadline");
    assert!(
        matches!(error, ClientError::Other(ref message)
            if message == "topic-list snapshot exceeded operation_timeout"),
        "unexpected watcher deadline error: {error:?}"
    );

    let watcher = tokio::time::timeout(
        Duration::from_millis(20),
        client.watch_topic_list("public/default", "cancelled-.*"),
    )
    .await;
    assert!(
        watcher.is_err(),
        "public watcher wrapper must remain pending against the silent broker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_partitioned_metadata_unregisters_waker() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (host_port, log) = spawn_silent_lookup_broker().await;
            let engine = MoonpoolEngine::new(TokioProviders::new());
            let client = tokio::time::timeout(
                HANG_GUARD,
                Client::connect_plain(&engine, &host_port, ConnectionConfig::default()),
            )
            .await
            .expect("connect did not time out")
            .expect("connect ok");

            let baseline_wakers = client.shared().inner.lock().pending_waker_count();
            assert_lookup_deadline(&client).await;
            assert_metadata_deadlines(&client).await;
            assert_watcher_deadlines_and_cancellation(&client).await;

            let res = tokio::time::timeout(
                Duration::from_millis(200),
                client.partitioned_topic_metadata(
                    "persistent://public/default/lookup-drop-unregister",
                ),
            )
            .await;
            assert!(
                res.is_err(),
                "expected partitioned_topic_metadata to time out (broker is silent)"
            );

            tokio::task::yield_now().await;

            let post_drop_wakers = client.shared().inner.lock().pending_waker_count();
            assert_eq!(
                post_drop_wakers, baseline_wakers,
                "dropping the RequestFut must restore the waker slab \
                 to its pre-call size (baseline={baseline_wakers}, observed={post_drop_wakers})",
            );

            let saw_partition = log
                .lock()
                .iter()
                .any(|r| r.0 == pb::base_command::Type::PartitionedMetadata as i32);
            assert!(
                saw_partition,
                "expected the broker stub to have received a CommandPartitionedMetadata"
            );

            client.close().await;
        })
        .await;
}
