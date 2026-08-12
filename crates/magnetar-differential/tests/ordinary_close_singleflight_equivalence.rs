// SPDX-License-Identifier: Apache-2.0

use magnetar_differential::broker::ScriptedBroker;
use magnetar_differential::{HANG_GUARD, Op, Trace, runner_moonpool, runner_tokio};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_ordinary_close_is_equivalent() {
    let trace = Trace::new(
        "persistent://public/default/ordinary-close-singleflight",
        "ordinary-close-singleflight",
        vec![
            Op::Recv {
                timeout: std::time::Duration::from_millis(1),
            },
            Op::ConcurrentClose,
        ],
    );
    let broker = ScriptedBroker::bind().await.expect("scripted broker");
    let tokio_trace =
        tokio::time::timeout(HANG_GUARD, runner_tokio::run(&broker.pulsar_url(), &trace))
            .await
            .expect("Tokio close trace must not hang")
            .expect("Tokio close trace");
    let tokio_closes = broker.consumer_close_log_snapshot();
    assert_eq!(tokio_closes.len(), 1, "Tokio must emit one close command");
    assert_ne!(tokio_closes[0].request_id, 0);
    assert_eq!(tokio_closes[0].topic.as_deref(), Some(trace.topic.as_str()));
    assert_eq!(
        tokio_closes[0].subscription.as_deref(),
        Some(trace.subscription.as_str())
    );
    broker.clear_cross_session_state();
    broker.clear_consumer_close_log();
    let moonpool_trace = tokio::time::timeout(
        HANG_GUARD,
        runner_moonpool::run(&broker.host_port(), &trace),
    )
    .await
    .expect("Moonpool close trace must not hang")
    .expect("Moonpool close trace");
    let moonpool_closes = broker.consumer_close_log_snapshot();
    assert_eq!(
        moonpool_closes.len(),
        1,
        "Moonpool must emit one close command"
    );
    assert_ne!(moonpool_closes[0].request_id, 0);
    assert_eq!(
        moonpool_closes[0].topic.as_deref(),
        Some(trace.topic.as_str())
    );
    assert_eq!(
        moonpool_closes[0].subscription.as_deref(),
        Some(trace.subscription.as_str())
    );
    assert_eq!(tokio_trace, moonpool_trace);
    assert_eq!(tokio_closes, moonpool_closes);
    broker.shutdown().await;
}
