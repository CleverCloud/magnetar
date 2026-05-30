// SPDX-License-Identifier: Apache-2.0

//! Shared background partition-watcher used by
//! [`crate::PartitionedProducer`], [`crate::MultiTopicsConsumer`], and
//! [`crate::TableView`].
//!
//! Java parity: `*Builder#autoUpdatePartitionsInterval` across producer /
//! consumer / table-view. The spawned task is a pure timer that signals
//! [`AutoUpdateTask::changed`] every `interval`; the actual
//! `PulsarClient::partitions_for_topic` call is driven by user code via
//! the per-surface `refresh_partitions` method (the crate-wide
//! `#![forbid(unsafe_code)]` rules out punning the `&PulsarClient`
//! lifetime into a `'static` spawn).
//!
//! Lifetime is bounded by the surface that owns the
//! `Arc<AutoUpdateTask>`: dropping every clone of that surface drops the
//! `Arc`, which runs [`Drop`] for [`AutoUpdateTask`] and aborts the
//! spawned tokio task. No channels — coordination is `Arc<Mutex<...>>` +
//! [`tokio::sync::Notify`] + [`tokio::time::interval`] (per the
//! project's "no channels in Rust async code" policy).
//!
//! Private — no public API surface.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Background partition-watcher timer task. See module docs.
#[derive(Debug)]
pub(crate) struct AutoUpdateTask {
    /// Topic the watcher polls — typically the base topic the owning
    /// surface was built against (without the `-partition-N` suffix).
    pub(crate) topic: String,
    /// Last partition count observed by the watcher. Seeded with the
    /// `initial_partitions` value passed to [`spawn_auto_update_task`];
    /// callers bump it via [`std::sync::atomic::Ordering`] stores when
    /// `refresh_partitions` detects a change.
    pub(crate) observed_partitions: Arc<AtomicU32>,
    /// Monotonic counter of "partition count changed" events. Useful
    /// for tests and "did anything change since I last looked?" probes.
    /// Bumped by the owning surface's `refresh_partitions` when a
    /// different count is observed.
    pub(crate) change_count: Arc<AtomicU64>,
    /// Signalled every time the internal timer fires, and every time
    /// the owning surface's `refresh_partitions` detects a real
    /// partition-count change.
    pub(crate) changed: Arc<Notify>,
    /// Signalled on drop to cooperatively wake the loop sleeping on
    /// [`Notify`] so it can notice it has been aborted promptly. The
    /// `handle.abort()` is the source of truth; the notify is only there
    /// to short-circuit a long `tick().await`.
    pub(crate) shutdown: Arc<Notify>,
    /// The spawned task. Held in a [`tokio::sync::Mutex`] so [`Drop`]
    /// can take it on the best-effort path without blocking; some
    /// owners (`TableView::close`) drain it explicitly via the lock.
    pub(crate) handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl Drop for AutoUpdateTask {
    fn drop(&mut self) {
        // Best-effort wake of the loop, then abort. If the lock is contended the abort
        // still happens once `Mutex<Option<JoinHandle>>` is dropped via the inner Option,
        // but the JoinHandle's own `Drop` does not abort by itself — only the explicit
        // `abort()` here does — so this `try_lock` matters for prompt teardown.
        self.shutdown.notify_waiters();
        if let Ok(mut g) = self.handle.try_lock()
            && let Some(h) = g.take()
        {
            h.abort();
        }
    }
}

/// Spawn the partition-watcher *timer* task.
///
/// The task is intentionally minimal: it ticks every `interval` and signals the
/// `Notify` returned via the owning surface's `partitions_changed_notify()`. It
/// does **not** itself call into the [`crate::PulsarClient`] — that requires a
/// `'static` clone of the client which the current `PulsarClient` API does not
/// yet expose, and going via `unsafe` would break the crate-wide
/// `#![forbid(unsafe_code)]` invariant.
///
/// `initial_partitions` is the partition count observed at spawn time and seeds
/// [`AutoUpdateTask::observed_partitions`]. Callers that don't have a count at
/// spawn time (e.g. multi-topic consumer with an explicit topic list, or
/// [`crate::TableView`]) pass `0` so the first observed refresh always logs a
/// change.
///
/// The `Arc<AutoUpdateTask>` returned wraps a [`Drop`] that aborts the spawned
/// task, so the timer is bounded by the owning surface's lifetime.
pub(crate) fn spawn_auto_update_task(
    topic: String,
    interval: Duration,
    initial_partitions: u32,
) -> Arc<AutoUpdateTask> {
    let observed_partitions = Arc::new(AtomicU32::new(initial_partitions));
    let change_count = Arc::new(AtomicU64::new(0));
    let changed = Arc::new(Notify::new());
    let shutdown = Arc::new(Notify::new());

    let changed_task = changed.clone();
    let shutdown_task = shutdown.clone();

    let handle = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Consume the immediate tick so the first real signal happens after one interval.
        ticker.tick().await;
        loop {
            tokio::select! {
                biased;
                () = shutdown_task.notified() => break,
                _ = ticker.tick() => {}
            }
            changed_task.notify_waiters();
        }
    });

    Arc::new(AutoUpdateTask {
        topic,
        observed_partitions,
        change_count,
        changed,
        shutdown,
        handle: tokio::sync::Mutex::new(Some(handle)),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;

    /// Spawn the auto-update timer and confirm the [`Drop`] impl aborts the
    /// spawned task. Uses `tokio::time::pause()` for deterministic timing.
    #[tokio::test(start_paused = true)]
    async fn drop_aborts_spawned_task() {
        let task = spawn_auto_update_task(
            "persistent://public/default/drop-abort-test".to_owned(),
            Duration::from_millis(100),
            0,
        );
        // Capture the JoinHandle's abort handle so we can verify it was aborted
        // after the `Arc` drops.
        let abort_handle = {
            let g = task.handle.lock().await;
            g.as_ref()
                .expect("handle is Some until Drop runs")
                .abort_handle()
        };
        assert!(!abort_handle.is_finished());
        // Advance virtual time and let the spawned task make progress before drop.
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;

        // Dropping the only Arc clone runs `Drop` → `handle.abort()`.
        drop(task);
        // Give the runtime a turn to observe the abort.
        tokio::task::yield_now().await;
        assert!(abort_handle.is_finished());
    }

    /// `initial_partitions` seeds the observed counter so the owning surface
    /// can detect the first real change.
    #[tokio::test(start_paused = true)]
    async fn initial_partitions_seeds_observed_counter() {
        let task = spawn_auto_update_task(
            "persistent://public/default/seed-test".to_owned(),
            Duration::from_secs(60),
            7,
        );
        assert_eq!(task.observed_partitions.load(Ordering::Relaxed), 7);
        assert_eq!(task.change_count.load(Ordering::Relaxed), 0);
        assert_eq!(task.topic, "persistent://public/default/seed-test");
        drop(task);
    }
}
