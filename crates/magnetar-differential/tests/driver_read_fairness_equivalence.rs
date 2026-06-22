// SPDX-License-Identifier: Apache-2.0

//! Issue #303 driver read-fairness — differential equivalence (ADR-0024 layer
//! (d)). The localized `select!` reorder (poll the inbound read arm before the
//! `driver_waker` arm, keeping `biased;`) is applied IDENTICALLY to both
//! engines, so a high-volume send burst — where each driver tick reads back a
//! `CommandSendReceipt` while more sends are staged — MUST still produce
//! byte-identical `EventStream`s on tokio and moonpool.
//!
//! If one engine received the reorder and the other did not, or if the reorder
//! changed the order in which `CommandSendReceipt`s are correlated to
//! `SendFut`s, the resulting `Event::Sent` sequences (with their broker-assigned
//! message ids) would diverge and this test would fail. Pins that the fairness
//! fix is a pure scheduling reorder with no observable wire/event effect.

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{Op, Trace, runner_moonpool, runner_tokio};

#[tokio::test(flavor = "current_thread")]
async fn send_burst_under_read_fairness_event_stream_parity() {
    // A large back-to-back send burst: many more sends than a single driver
    // tick drains, so the loop repeatedly interleaves "stage + write sends" with
    // "read back receipts" — exactly the path the read-fairness reorder touches.
    // Both engines must emit the same 128 `Event::Sent` entries in the same
    // order with the same broker-assigned message ids.
    let ops: Vec<Op> = (0..128_u16)
        .map(|i| Op::Send {
            payload: vec![u8::try_from(i % 256).unwrap_or(0); 32],
        })
        .collect();
    let trace = Trace::new(
        "persistent://public/default/read-fairness-equiv",
        "sub-fairness",
        ops,
    );

    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();

    let tokio_stream = runner_tokio::run(&pulsar_url, &trace)
        .await
        .expect("tokio runner");
    let moonpool_stream = runner_moonpool::run(&host_port, &trace)
        .await
        .expect("moonpool runner");

    assert_eq!(
        tokio_stream, moonpool_stream,
        "engine event streams diverged for the read-fairness send burst — the \
         select! reorder must be identical on both engines and observably inert",
    );

    broker.shutdown().await;
}
