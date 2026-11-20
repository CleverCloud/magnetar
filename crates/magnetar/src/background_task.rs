// SPDX-License-Identifier: Apache-2.0

//! A helper task spawned on a client's own executor and stopped cooperatively.
//!
//! Shared by the surfaces that own one: the [`crate::TableView`] drain and the auto-update
//! partition ticker of [`crate::PartitionedProducer`], [`crate::MultiTopicsConsumer`] and
//! [`crate::TableView`].
//!
//! The task is spawned through [`BackgroundApi`], so it runs where the client it serves runs: on
//! the ambient Tokio runtime for the Tokio client, on the client's `TaskProvider` for the
//! Moonpool client (ADR-0078). A `TaskProvider` join handle cannot cancel its task, and
//! `tokio::task::JoinHandle::abort` is not available through that seam, so the stop is a signal
//! the task races against: [`BackgroundTask::stop`] stores a permit on a [`Notify`], and the
//! wrapper future drops the work the next time it is polled. That is the same point a Tokio abort
//! takes effect at, on either engine. No channels (`GUIDELINES.md` "no channels"): a `Notify`
//! and a boxed join future.
//!
//! Private — no public API surface.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::Notify;

use crate::{BackgroundApi, BackgroundFuture};

/// A spawned helper task. Dropping the last owner stops it.
pub(crate) struct BackgroundTask {
    /// Stop signal. `notify_one` stores a permit, so a stop that lands before the task is first
    /// polled, or while it is between two awaits, is still seen.
    stop: Arc<Notify>,
    /// Resolves once the task has ended. Taken by [`Self::shutdown`], so a second call does not
    /// wait twice.
    join: tokio::sync::Mutex<Option<BackgroundFuture>>,
}

impl std::fmt::Debug for BackgroundTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackgroundTask").finish_non_exhaustive()
    }
}

impl BackgroundTask {
    /// Spawn `work` through `api` under `name`. It runs until it returns or the task is stopped.
    pub(crate) fn spawn<A, W>(api: &A, name: &str, work: W) -> Self
    where
        A: BackgroundApi,
        W: Future<Output = ()> + Send + 'static,
    {
        let stop = Arc::new(Notify::new());
        let stopped = stop.clone();
        let join = api.spawn_background(
            name,
            Box::pin(async move {
                tokio::select! {
                    biased;
                    () = stopped.notified() => {}
                    () = work => {}
                }
            }),
        );
        Self {
            stop,
            join: tokio::sync::Mutex::new(Some(join)),
        }
    }

    /// Ask the task to stop. Idempotent and never blocks; the task ends the next time it is polled.
    pub(crate) fn stop(&self) {
        self.stop.notify_one();
    }

    /// Stop the task and wait until it has ended.
    pub(crate) async fn shutdown(&self) {
        self.stop();
        if let Some(join) = self.join.lock().await.take() {
            join.await;
        }
    }
}

impl Drop for BackgroundTask {
    fn drop(&mut self) {
        self.stop();
    }
}

/// A [`BackgroundApi`] over the ambient Tokio runtime, for unit tests of the surfaces that own a
/// [`BackgroundTask`]. It also records the abort handle of every task it spawns so a test can
/// assert the task really finished.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct TokioBackground {
    pub(crate) spawned: parking_lot::Mutex<Vec<tokio::task::AbortHandle>>,
}

#[cfg(test)]
impl BackgroundApi for TokioBackground {
    fn spawn_background(&self, _name: &str, task: BackgroundFuture) -> BackgroundFuture {
        let join = tokio::spawn(task);
        self.spawned.lock().push(join.abort_handle());
        Box::pin(async move {
            let _ended = join.await;
        })
    }

    fn background_sleeper(&self) -> crate::BackgroundSleep {
        Arc::new(|duration| {
            Box::pin(async move {
                tokio::time::sleep(duration).await;
                true
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    /// Flips a flag when dropped: proves the work future itself was dropped, not just left parked.
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    /// Dropping the owner stops a task parked forever on its first await: the task ends and the
    /// work future it held is dropped.
    #[tokio::test]
    async fn dropping_the_owner_ends_a_parked_task() {
        let api = TokioBackground::default();
        let dropped = Arc::new(AtomicBool::new(false));
        let flag = DropFlag(dropped.clone());
        let task = BackgroundTask::spawn(&api, "parked", async move {
            let _flag = flag;
            std::future::pending::<()>().await;
        });
        let abort = api.spawned.lock()[0].clone();
        tokio::task::yield_now().await;
        assert!(!abort.is_finished(), "the task is parked, not finished");

        drop(task);
        tokio::task::yield_now().await;

        assert!(abort.is_finished(), "dropping the owner must end the task");
        assert!(
            dropped.load(Ordering::SeqCst),
            "the work future must be dropped with it"
        );
    }

    /// A stop that lands before the task is first polled is not lost: `notify_one` keeps a permit.
    #[tokio::test]
    async fn a_stop_before_the_first_poll_is_kept() {
        let api = TokioBackground::default();
        let task = BackgroundTask::spawn(&api, "early", std::future::pending::<()>());
        task.stop();
        let abort = api.spawned.lock()[0].clone();
        task.shutdown().await;
        assert!(abort.is_finished());
    }

    /// `shutdown` waits for the task to end, and a second call returns at once.
    #[tokio::test]
    async fn shutdown_waits_for_the_task_and_is_idempotent() {
        let api = TokioBackground::default();
        let task = BackgroundTask::spawn(&api, "joined", std::future::pending::<()>());
        task.shutdown().await;
        assert!(api.spawned.lock()[0].is_finished());
        task.shutdown().await;
    }
}
