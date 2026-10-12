// SPDX-License-Identifier: Apache-2.0

//! End-to-end tests for the cancelled producer open (issue #406, ADR-0100).
//!
//! Production repro: an `open_producer` lost client-side while completing
//! broker-side. The broker keeps the `(topic, producer_name)` registration,
//! so every later open under that name fails with:
//!
//! ```text
//! NamingException: Producer with name 'X' is already connected to topic 'T'
//! ```
//!
//! `ProducerBusy` (code 16) is retryable for `ProducerOpen` (ADR-0080), so
//! the retry loop re-hits the zombie with a fresh producer id until the
//! budget runs out; only `topics unload` recovered the name.
//!
//! Two user-visible contracts are covered here:
//!
//! 1. **Recovery, whatever the broker-side interleaving (ADR-0100).** A producer open abandoned on
//!    its deadline pushes a best-effort `CommandCloseProducer` for the abandoned producer id. That
//!    close can still lose its race with the broker's own creation — Pulsar completes a
//!    close-before-creation by failing the pending creation future and acking Success, and CI
//!    reproduced a registration surviving exactly that. So a subsequent `ProducerBusy` under the
//!    name is *recovered from* rather than merely retried: the abandoned id is re-closed while the
//!    broker can address it, and if the name is still busy the next attempt re-attaches under that
//!    id as a strict successor. The claim this test makes is the user-visible one — **the name is
//!    reusable on the same connection, with no `topics unload`** — inside the client's own retry
//!    budget. The interleaving-by-interleaving proof lives in the in-process layers
//!    (`crates/magnetar-proto/src/conn.rs` unit tests, the two engines'
//!    `producer_open_cancel_close.rs`, and
//!    `crates/magnetar-differential/tests/producer_open_cancel_close_equivalence.rs`), which script
//!    each interleaving exactly. Against a real broker it cannot be scripted, so this test sweeps
//!    tight deadlines to land inside the window. It is one-sided: with the recovery the final open
//!    always succeeds; without it, any attempt that stranded a registration wedges the name.
//! 2. **Opt-in unique name suffix.** `ProducerBuilder::unique_name_suffix(true)` makes each open
//!    claim its own name, so a registration stranded by a dead client — or by a proxy-mediated
//!    connection that outlived it, where no close can ever be sent — cannot collide with the next
//!    one. Default off: a pinned name stays pinned, and a second live open under it is still
//!    rejected.
//!
//! Runs as a regular test under `cargo test` (ADR-0046). Run with:
//!
//! ```sh
//! cargo test -p magnetar-driver --test e2e_producer_open_cancel -- --nocapture
//! ```
//!
//! Requires Docker on the host. See `e2e_pulsar.rs` for the broker
//! container plumbing; this file uses the same image/wait strategy via a
//! local helper.

use std::time::Duration;

use magnetar::PulsarClient;

mod common;

use common::pulsar::start_pulsar;

/// Deadlines swept by [`e2e_timed_out_producer_open_does_not_poison_the_name`].
/// A warm topic answers a producer open in single-digit milliseconds, so this
/// range brackets the window where the `CommandProducer` is on the wire and
/// the `CommandProducerSuccess` is not back yet.
///
/// Kept short on purpose: every abandoned attempt is one more candidate the
/// recovery may have to rotate through, and the recovery has to finish inside
/// the final open's retry budget.
const TIGHT_DEADLINES_MS: [u64; 3] = [2, 5, 9];

/// Retry policy for the recovering open. Recovery costs at most one extra
/// round trip per abandoned candidate, and the default 2 s to 8 s schedule
/// would spend the whole operation budget waiting between them. Shortening the
/// WAIT does not weaken the property under test — the final open must still
/// succeed on its own, with no `topics unload` and no broker-side help.
fn recovery_retry() -> magnetar_proto::OperationRetryConfig {
    magnetar_proto::OperationRetryConfig {
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(250),
        max_retries: Some(12),
    }
}

fn unique_topic(prefix: &str) -> String {
    format!(
        "persistent://public/default/{prefix}-{}",
        uuid::Uuid::new_v4().simple()
    )
}

/// Issue #406: a producer open abandoned on its deadline must not poison the
/// pinned name. Each abandoned attempt pushes a `CommandCloseProducer` for
/// its producer id, and the close precedes the next open on the same
/// connection (FIFO), so the final open observes a free name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_timed_out_producer_open_does_not_poison_the_name()
-> Result<(), Box<dyn std::error::Error>> {
    const PRODUCER_NAME: &str = "open-cancel-hostname";
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .operation_retry(recovery_retry())
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-producer-open-cancel");

    // Warm the topic so the namespace bundle is loaded and the lookup is a
    // cheap round-trip: the swept deadlines below must expire during the
    // producer open, not during bundle assignment.
    let warmup = client.producer(&topic).create().await?;
    warmup.close().await?;

    for budget_ms in TIGHT_DEADLINES_MS {
        // Dropping the open future is what arms the engine's cancellation
        // guard — exactly the path a lost `open_producer` takes.
        let attempt = tokio::time::timeout(
            Duration::from_millis(budget_ms),
            client.producer(&topic).name(PRODUCER_NAME).create(),
        )
        .await;
        // An attempt that beat its deadline is released the same way the
        // production repro releases it: no explicit close (ADR-0057's
        // last-clone drop guard frees the name for the next attempt).
        if let Ok(Ok(producer)) = attempt {
            drop(producer);
        }
    }

    // The contract: whatever the sweep did to the broker — a registration
    // reaped by the cancel-time close, or one that outlived it — the pinned
    // name is reusable on the same connection, inside this open's own retry
    // budget and with no `topics unload`.
    let recreated = client.producer(&topic).name(PRODUCER_NAME).create().await?;
    recreated
        .send_bytes(b"after-cancelled-opens".to_vec())
        .await?;
    recreated.close().await?;
    client.close().await;
    Ok(())
}

/// The opt-in escape hatch for a registration nothing can close — a dead
/// client, or a proxy-mediated connection that outlives it.
/// `unique_name_suffix(true)` gives every open its own name, so two live
/// producers built from the same caller-supplied name coexist. Default off:
/// the same pair without the suffix collides on `ProducerBusy`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e2e_unique_name_suffix_avoids_a_pinned_name_collision()
-> Result<(), Box<dyn std::error::Error>> {
    const PRODUCER_NAME: &str = "unique-suffix-hostname";
    let (service_url, _admin_url, _container) = start_pulsar().await?;
    let client = PulsarClient::builder()
        .service_url(service_url)
        .build()
        .await?;
    let topic = unique_topic("magnetar-e2e-producer-unique-suffix");

    // Default (suffix off): the name is pinned, so a second live open under
    // it is rejected by the broker.
    let pinned = client.producer(&topic).name(PRODUCER_NAME).create().await?;
    let collision = client.producer(&topic).name(PRODUCER_NAME).create().await;
    assert!(
        collision.is_err(),
        "a pinned name held by a live producer must not open a second time"
    );
    pinned.close().await?;

    // Suffix on: both opens claim distinct names and coexist.
    let first = client
        .producer(&topic)
        .name(PRODUCER_NAME)
        .unique_name_suffix(true)
        .create()
        .await?;
    let second = client
        .producer(&topic)
        .name(PRODUCER_NAME)
        .unique_name_suffix(true)
        .create()
        .await?;
    first.send_bytes(b"first".to_vec()).await?;
    second.send_bytes(b"second".to_vec()).await?;
    first.close().await?;
    second.close().await?;
    client.close().await;
    Ok(())
}
