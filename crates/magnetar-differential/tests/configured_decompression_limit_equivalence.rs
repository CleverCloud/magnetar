// SPDX-License-Identifier: Apache-2.0

//! The configured per-consumer ceiling is observable as identical plaintext delivery by both
//! runtime engines, including a payload larger than the unchanged wire-frame limit.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{BatchLayout, Event, Op, Trace, runner_moonpool, runner_tokio};
use magnetar_proto::types::CompressionKind;

const TOPIC: &str = "persistent://public/default/configured-decompression-limit";
const SUBSCRIPTION: &str = "configured-decompression-limit";

fn trace(payload: &[u8]) -> Trace {
    Trace::new(
        TOPIC,
        SUBSCRIPTION,
        vec![
            Op::SendCompressedBatch {
                payloads: vec![payload.to_vec()],
                codec: CompressionKind::Zstd,
                layout: BatchLayout::Java,
            },
            Op::OpenSharedConsumer {
                name: "large-limit".to_owned(),
                receiver_queue_size: 1,
                max_redeliver_count: 0,
                max_uncompressed_message_size: Some(8 * 1024 * 1024),
            },
            Op::RecvShared {
                name: "large-limit".to_owned(),
                timeout: Duration::from_secs(5),
            },
            Op::AckLastReceivedShared {
                name: "large-limit".to_owned(),
            },
            Op::Close,
        ],
    )
}

#[tokio::test(flavor = "current_thread")]
async fn configured_large_payload_event_streams_match_byte_for_byte() {
    let payload = vec![0x6a; 6 * 1024 * 1024];
    let trace = trace(&payload);
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let tokio_events = runner_tokio::run(&broker.pulsar_url(), &trace)
        .await
        .expect("tokio trace");
    broker.clear_frame_log();
    let moonpool_events = runner_moonpool::run(&broker.host_port(), &trace)
        .await
        .expect("moonpool trace");

    assert_eq!(tokio_events, moonpool_events);
    assert!(tokio_events.events.iter().any(|event| matches!(
        event,
        Event::Received { payload: received, .. } if received == &payload
    )));
    assert!(tokio_events.events.contains(&Event::Acked));
}
