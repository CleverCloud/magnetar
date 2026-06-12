// SPDX-License-Identifier: Apache-2.0

//! Smoke test for the scripted broker: confirm a tokio engine can
//! connect and handshake without timing out.

use std::time::Duration;

use bytes::Bytes;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Op, Trace, runner_tokio};
use magnetar_proto::producer::OutgoingMessage;
use magnetar_proto::{ConnectionConfig, CreateProducerRequest, SupervisorConfig};
use magnetar_runtime_tokio::Client;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tokio_client_handshakes() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let url = broker.pulsar_url();
    eprintln!("[test] broker @ {url}");

    let client = tokio::time::timeout(
        Duration::from_secs(5),
        Client::connect(&url, ConnectionConfig::default()),
    )
    .await
    .expect("connect did not time out")
    .expect("connect succeeded");

    eprintln!("[test] connected, is_connected={}", client.is_connected());
    assert!(client.is_connected());

    // Try opening a producer.
    eprintln!("[test] opening producer");
    let producer = tokio::time::timeout(
        Duration::from_secs(3),
        client.open_producer_with(
            CreateProducerRequest {
                topic: "persistent://public/default/smoke".to_owned(),
                ..Default::default()
            },
            None,
        ),
    )
    .await
    .expect("producer open did not time out")
    .expect("producer opened");
    eprintln!("[test] producer ready, name={:?}", producer.name());

    // Send a message.
    eprintln!("[test] sending");
    let mid = tokio::time::timeout(
        Duration::from_secs(3),
        producer.send(OutgoingMessage {
            payload: Bytes::from_static(b"smoke"),
            metadata: magnetar_proto::pb::MessageMetadata::default(),
            uncompressed_size: 5,
            num_messages: 1,
            txn_id: None,
            source_message_id: None,
        }),
    )
    .await
    .expect("send did not time out")
    .expect("send ok");
    eprintln!("[test] sent, mid={mid:?}");

    // Don't call close — just drop and let the broker session task be reaped.
    drop(producer);
    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);
    broker.shutdown().await;
    eprintln!("[test] done");
}

/// Regression guard for the cross-session resume state (docs/follow-ups.md
/// §4.2). The drop knob promotes the ledger + durable cursor out of the
/// per-session state into a broker-level [`ScriptedBroker`] store shared by
/// every session of one broker. The differential runner drives BOTH engine
/// legs against ONE broker, so a leaked ledger would silently corrupt the
/// second leg's parity comparison.
///
/// This test pins the isolation contract LOUDLY:
///
/// 1. a fresh broker starts with an EMPTY cross-session ledger;
/// 2. an armed drop + redial leg populates it (resume mode is on);
/// 3. [`ScriptedBroker::clear_cross_session_state`] empties it again, so a second back-to-back leg
///    on the same broker starts from an EMPTY ledger.
///
/// If a future change forgets the reset (or wires resume mode to always
/// persist), step 3's assertion fails here rather than surfacing as a
/// confusing false-parity pass in a downstream differential test.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cross_session_state_is_isolated_between_legs() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");

    // (1) A fresh broker holds no cross-session state.
    assert_eq!(
        broker.cross_session_ledger_len(),
        0,
        "a fresh broker must start with an empty cross-session ledger",
    );

    // (2) Arm the drop knob and run one supervised leg: a send + recv + ack +
    // redial-resumed send. Resume mode persists the ledger to the broker-level
    // store.
    broker.drop_connection_after(8);
    let supervisor = SupervisorConfig {
        initial_backoff: Duration::from_millis(20),
        max_backoff: Duration::from_millis(200),
        ..SupervisorConfig::default()
    };
    // Same deterministic shape as `reconnect_replay_gating_equivalence`: the
    // in-flight second send fully resolves (Recv B + Ack B) BEFORE `Close`, so
    // the supervised teardown never races an unresolved replay. A truncated
    // `Send B → Close` trace can hang the close on certain seeds while B is
    // still replaying across the redial.
    let m = |entry_id: u64| magnetar_proto::MessageId {
        ledger_id: 1,
        entry_id,
        partition: -1,
        batch_index: -1,
        batch_size: 0,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };
    let trace = Trace::new(
        "persistent://public/default/smoke-cross-session",
        "sub-smoke-cross-session",
        vec![
            Op::Send {
                payload: b"a".to_vec(),
            },
            Op::Recv {
                timeout: Duration::from_secs(5),
            },
            Op::Ack { message_id: m(0) },
            Op::Send {
                payload: b"b".to_vec(),
            },
            Op::Recv {
                timeout: Duration::from_secs(5),
            },
            Op::Ack { message_id: m(1) },
            Op::Close,
        ],
    );
    let _ = tokio::time::timeout(
        Duration::from_secs(30),
        runner_tokio::run_supervised(&broker.pulsar_url(), &trace, supervisor),
    )
    .await
    .expect("supervised leg must not hang")
    .expect("supervised runner");
    assert!(
        broker.cross_session_ledger_len() >= 1,
        "an armed drop + redial leg must persist the ledger to the cross-session store",
    );

    // (3) The reset empties it, so the next back-to-back leg starts clean.
    broker.clear_cross_session_state();
    assert_eq!(
        broker.cross_session_ledger_len(),
        0,
        "clear_cross_session_state must empty the ledger so a second leg on this \
         broker starts from scratch — a missing reset would leak leg-1 state into leg-2",
    );

    broker.shutdown().await;
}
