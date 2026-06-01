// SPDX-License-Identifier: Apache-2.0

//! Cancelling a lookup-style future before the broker response lands must
//! NOT leave a [`std::task::Waker`] orphaned in
//! [`magnetar_proto::Connection`]'s `wakers` slab.
//!
//! ADR-0024 four-layer coverage of the lookup multi-agent review MEDIUM-4
//! finding. The companion layers are:
//!
//! - `crates/magnetar-proto/src/conn.
//!   rs::unregister_waker_drops_request_entry_without_disturbing_siblings` (proto unit test)
//! - `crates/magnetar-runtime-moonpool/tests/lookup_drop_unregister.rs` (moonpool integration test
//!   — same shape as this file)
//! - `crates/magnetar-differential/tests/lookup_drop_unregister.rs` (differential equivalence test)
//! - `crates/magnetar/tests/e2e_lookup_drop_unregister.rs` (engine-agnostic façade smoke test)
//!
//! Strategy: stand up a TCP broker stub that answers `Connect` and `Ping`
//! but parks every `PartitionedMetadata` request without replying. Drive
//! the tokio engine to issue `partitioned_topic_metadata`, race it
//! against a short [`tokio::time::timeout`] so the future is dropped
//! before the (never-arriving) response, then re-poll the proto state
//! machine and assert
//! [`magnetar_proto::Connection::pending_waker_count`] is back to zero.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{ConnectionConfig, FrameError, decode_one, encode_command, pb};
use magnetar_runtime_tokio::Client;
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordedKind(i32);

/// Spawn a TCP broker that responds to `Connect` + `Ping` but silently
/// swallows every other command (in particular `PartitionedMetadata`).
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
    (format!("pulsar://{addr}"), log)
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
        // PartitionedMetadata and Lookup are intentionally dropped on the
        // floor: we want the user-facing future to stay Pending until the
        // caller drops it.
        _ => {}
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_partitioned_metadata_unregisters_waker() {
    let (url, log) = spawn_silent_lookup_broker().await;
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect ok");

    // Snapshot the waker-slab size right after handshake. The runtime
    // never registers a request-keyed waker during the handshake itself,
    // so this should be zero, but we capture it to make the assertion
    // robust against future driver changes.
    let baseline_wakers = client.shared().inner.lock().pending_waker_count();

    // Race the call against a short timeout to force a drop. The broker
    // will never respond to PartitionedMetadata, so the only way out is
    // the timeout — which drops the future. Without the Drop impl on
    // RequestFut the waker entry leaks here.
    let res = tokio::time::timeout(
        Duration::from_millis(200),
        client.partitioned_topic_metadata("persistent://public/default/lookup-drop-unregister"),
    )
    .await;
    assert!(
        res.is_err(),
        "expected the partitioned_topic_metadata call to time out (broker is silent)"
    );

    // Give the drop a moment to land + the driver to settle.
    tokio::task::yield_now().await;

    let post_drop_wakers = client.shared().inner.lock().pending_waker_count();
    assert_eq!(
        post_drop_wakers, baseline_wakers,
        "dropping the RequestFut must restore the waker slab \
         to its pre-call size (baseline={baseline_wakers}, observed={post_drop_wakers})",
    );

    // Sanity check: the broker actually saw the PartitionedMetadata command,
    // so the test really exercises the leak path and not some short-circuit.
    let saw_partition = log
        .lock()
        .iter()
        .any(|r| r.0 == pb::base_command::Type::PartitionedMetadata as i32);
    assert!(
        saw_partition,
        "expected the broker stub to have received a CommandPartitionedMetadata"
    );

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
}
