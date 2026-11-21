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
//! `Arc`, which drops the [`BackgroundTask`] and stops the spawned task. The task
//! is spawned and paced through the owning client's [`BackgroundApi`], so it runs on
//! the executor and clock of the provider bundle that client was built over: Tokio's
//! for `TokioEngine`, the simulation's for `MoonpoolEngine<SimProviders>` (ADR-0078).
//! No channels — coordination is [`tokio::sync::Notify`] plus the client's sleep (per
//! the project's "no channels in Rust async code" policy).
//!
//! Private — no public API surface.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::time::Duration;

use tokio::sync::Notify;

use crate::BackgroundApi;
use crate::background_task::BackgroundTask;

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
    /// The spawned timer task. Dropping the `AutoUpdateTask` stops it; some owners
    /// (`TableView::close`) stop it earlier and wait for it to end.
    pub(crate) task: BackgroundTask,
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
/// The `Arc<AutoUpdateTask>` returned owns the spawned task and stops it when dropped, so the
/// timer is bounded by the owning surface's lifetime. `api` is the owning surface's client: the
/// task is spawned, and sleeps, where that client runs.
pub(crate) fn spawn_auto_update_task<A: BackgroundApi>(
    api: &A,
    topic: String,
    interval: Duration,
    initial_partitions: u32,
) -> Arc<AutoUpdateTask> {
    let observed_partitions = Arc::new(AtomicU32::new(initial_partitions));
    let change_count = Arc::new(AtomicU64::new(0));
    let changed = Arc::new(Notify::new());

    let changed_task = changed.clone();
    let sleep = api.background_sleeper();
    let task = BackgroundTask::spawn(api, "magnetar-auto-update-partitions", async move {
        // The first signal comes one interval after the spawn, and each next one an interval
        // after the previous signal (the old `MissedTickBehavior::Delay`). The sleep resolves
        // `false` once the client's clock is gone, which ends the task instead of spinning.
        while sleep(interval).await {
            changed_task.notify_waiters();
        }
    });

    Arc::new(AutoUpdateTask {
        topic,
        observed_partitions,
        change_count,
        changed,
        task,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::background_task::TokioBackground;

    /// Spawn the auto-update timer and confirm dropping the owner stops the
    /// spawned task. Uses `tokio::time::pause()` for deterministic timing.
    #[tokio::test(start_paused = true)]
    async fn drop_aborts_spawned_task() {
        let api = TokioBackground::default();
        let task = spawn_auto_update_task(
            &api,
            "persistent://public/default/drop-abort-test".to_owned(),
            Duration::from_millis(100),
            0,
        );
        // Keep the spawned task's abort handle so we can verify it finished
        // after the `Arc` drops.
        let abort_handle = api.spawned.lock()[0].clone();
        assert!(!abort_handle.is_finished());
        // Advance virtual time and let the spawned task make progress before drop.
        tokio::time::advance(Duration::from_millis(150)).await;
        tokio::task::yield_now().await;
        assert!(
            !abort_handle.is_finished(),
            "the timer loop runs until it is stopped"
        );

        // Dropping the only Arc clone drops the owned `BackgroundTask` → stop.
        drop(task);
        // Give the runtime a turn to observe the stop.
        tokio::task::yield_now().await;
        assert!(abort_handle.is_finished());
    }

    /// A client whose clock is gone: every sleep resolves `false` at once, as a time provider
    /// does once its simulation has ended.
    struct ClockGone(TokioBackground);

    impl BackgroundApi for ClockGone {
        fn spawn_background(
            &self,
            name: &str,
            task: crate::BackgroundFuture,
        ) -> crate::BackgroundFuture {
            self.0.spawn_background(name, task)
        }

        fn background_sleeper(&self) -> crate::BackgroundSleep {
            Arc::new(|_| Box::pin(async { false }))
        }
    }

    /// The ticker ends on its own when the clock is gone, instead of looping without ever
    /// yielding (which no stop signal could interrupt). Nothing stops it here: the test waits for
    /// the task to finish by itself.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ticker_ends_when_the_clock_is_gone() {
        let api = ClockGone(TokioBackground::default());
        let task = spawn_auto_update_task(
            &api,
            "persistent://public/default/clock-gone".to_owned(),
            Duration::from_millis(100),
            0,
        );
        let abort_handle = api.0.spawned.lock()[0].clone();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !abort_handle.is_finished() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the ticker must end by itself once the clock is gone");
        drop(task);
    }

    /// `initial_partitions` seeds the observed counter so the owning surface
    /// can detect the first real change.
    #[tokio::test(start_paused = true)]
    async fn initial_partitions_seeds_observed_counter() {
        let api = TokioBackground::default();
        let task = spawn_auto_update_task(
            &api,
            "persistent://public/default/seed-test".to_owned(),
            Duration::from_mins(1),
            7,
        );
        assert_eq!(task.observed_partitions.load(Ordering::Relaxed), 7);
        assert_eq!(task.change_count.load(Ordering::Relaxed), 0);
        assert_eq!(task.topic, "persistent://public/default/seed-test");
        drop(task);
    }
}
