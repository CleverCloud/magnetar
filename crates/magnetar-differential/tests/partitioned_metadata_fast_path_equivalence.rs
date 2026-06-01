// SPDX-License-Identifier: Apache-2.0

//! F11 partitioned-topic-metadata fast-path — tokio ↔ moonpool engine
//! equivalence (ADR-0024 layer (d)).
//!
//! When the caller asks for partitioned-topic metadata on a topic whose
//! name already encodes a partition index (`<base>-partition-<N>`), both
//! engines MUST short-circuit identically:
//!
//! * the call resolves to `Ok(0)` synchronously without any broker round-trip,
//! * the `ScriptedBroker` sees ZERO `CommandPartitionedTopicMetadata` frames (the only frames it
//!   sees are the handshake `CommandConnect` followed by an eventual close on disconnect).
//!
//! If either engine accidentally regressed to "always send the
//! lookup frame", the broker would see a `PartitionedMetadata` entry in
//! its frame log on the affected leg and the equivalence would diverge.

use std::time::Duration;

use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::pb;
use magnetar_runtime_moonpool::MoonpoolEngine;
use moonpool_core::TokioProviders;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partition_suffix_fast_path_event_stream_parity() {
    let broker = ScriptedBroker::bind().await.expect("broker bind");
    let pulsar_url = broker.pulsar_url();
    let host_port = broker.host_port();
    let topic = "persistent://public/default/diff-fast-path-partition-0";

    // ----- tokio leg -----
    let tokio_count = {
        let client = tokio::time::timeout(
            Duration::from_secs(5),
            magnetar_runtime_tokio::Client::connect(
                &pulsar_url,
                magnetar_proto::ConnectionConfig::default(),
            ),
        )
        .await
        .expect("tokio connect did not time out")
        .expect("tokio connect ok");
        let count = tokio::time::timeout(
            Duration::from_secs(2),
            client.partitioned_topic_metadata(topic),
        )
        .await
        .expect("tokio fast-path did not time out")
        .expect("tokio fast-path Ok");
        if let Some(d) = client.take_driver() {
            d.abort();
        }
        drop(client);
        count
    };

    // Let the recording broker drain in-flight reads, then snapshot the
    // wire history and clear it so the moonpool leg starts fresh.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let tokio_frames = broker.frame_log_snapshot();
    broker.clear_frame_log();

    // ----- moonpool leg -----
    let moonpool_count = {
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let engine = MoonpoolEngine::new(TokioProviders::new());
                let client = tokio::time::timeout(
                    Duration::from_secs(5),
                    magnetar_runtime_moonpool::Client::connect_plain(
                        &engine,
                        &host_port,
                        magnetar_proto::ConnectionConfig::default(),
                    ),
                )
                .await
                .expect("moonpool connect did not time out")
                .expect("moonpool connect ok");
                let count = tokio::time::timeout(
                    Duration::from_secs(2),
                    client.partitioned_topic_metadata(topic),
                )
                .await
                .expect("moonpool fast-path did not time out")
                .expect("moonpool fast-path Ok");
                client.close().await;
                count
            })
            .await
    };

    tokio::time::sleep(Duration::from_millis(50)).await;
    let moonpool_frames = broker.frame_log_snapshot();
    broker.shutdown().await;

    // Equivalence claim 1: both engines return `Ok(0)`.
    assert_eq!(tokio_count, 0, "tokio fast-path must report 0 partitions");
    assert_eq!(
        moonpool_count, 0,
        "moonpool fast-path must report 0 partitions"
    );
    assert_eq!(tokio_count, moonpool_count);

    // Equivalence claim 2: neither engine emitted a
    // `CommandPartitionedTopicMetadata` frame to the broker. The only
    // frames the broker should see are handshake `CommandConnect`s and
    // possibly a Ping/Pong if the driver had time to tick.
    let pm = pb::base_command::Type::PartitionedMetadata as i32;
    assert!(
        !tokio_frames.contains(&pm),
        "tokio leg must not emit PartitionedMetadata; got {tokio_frames:?}"
    );
    assert!(
        !moonpool_frames.contains(&pm),
        "moonpool leg must not emit PartitionedMetadata; got {moonpool_frames:?}"
    );
}
