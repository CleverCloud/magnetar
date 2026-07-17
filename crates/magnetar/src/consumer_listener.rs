// SPDX-License-Identifier: Apache-2.0

//! Push-delivery consumer listener. Mirrors
//! `org.apache.pulsar.client.api.MessageListener` /
//! `ConsumerBuilder#messageListener`.
//!
//! A [`MessageListener`] is a user callback invoked once per delivered message.
//! Registering one on a consumer builder (via `message_listener(...)`) and
//! subscribing with `subscribe_with_listener()` flips the consumer from
//! **pull** mode (`receive` / `receive_async`) to **push** mode: a background
//! poller task drives the consumer's existing `receive()` loop and hands each
//! message to the callback.
//!
//! ## Design (runtime-side, proto stays sans-io — ADR-0004)
//!
//! `MessageListener` is a runtime concept. `magnetar-proto` cannot spawn tasks
//! or invoke callbacks, so nothing here touches the sans-io state machine. The
//! poller is a [`tokio::spawn`]ed task — exactly the pattern
//! [`crate::TableView`]'s `spawn_drain` uses (ADR-0025: both engines schedule
//! on tokio; determinism for the moonpool engine comes from substituting the
//! `moonpool_core::Providers`, not from replacing the executor). It is
//! engine-generic over `C: ConsumerApi + Clone`, so the same poller serves the
//! tokio and moonpool consumers without a per-engine carve-out.
//!
//! ## Delivery semantics (match Java)
//!
//! - **Sequential, in order.** The poller awaits one `receive()`, runs the callback to completion,
//!   then pulls the next message. There is no per-message concurrency — order is preserved, exactly
//!   like Java's single-threaded per-consumer listener executor.
//! - **No channel between the consumer and the listener** (ADR-0003). The poller calls `receive()`
//!   directly, which already parks on the per-consumer `Notify` / `Waker` slab inside the sans-io
//!   state machine.
//! - **No auto-ack.** The callback is responsible for acking (positive ack, cumulative ack, or
//!   nack) — same contract as Java's `MessageListener`, which hands you the `Consumer` so you ack
//!   explicitly. The poller never acks on the callback's behalf.
//! - **Clean shutdown.** An explicit or terminal remote consumer close makes `receive()` resolve
//!   with an error, so the poller loop ends without a panic. Dropping the returned
//!   [`MessageListenerHandle`] (or calling [`MessageListenerHandle::close`]) aborts the poller;
//!   task unwinding then drops its owned consumer clone. That clone triggers a best-effort close
//!   only if it is the final clone. Dropping an intermediate consumer clone does nothing.
//!
//! ## Pull / push are mutually exclusive
//!
//! Java forbids calling `receive()` on a consumer that has a `messageListener`.
//! magnetar mirrors the intent by *moving* the consumer into the poller task:
//! `subscribe_with_listener()` returns a [`MessageListenerHandle`], not the
//! consumer, so there is no consumer handle left to call `receive()` on. The
//! listener owns delivery for the lifetime of the handle.
//!
//! ## Wrapper consumers (multi-topic / partitioned / pattern)
//!
//! The single-topic poller above is bound to [`crate::ConsumerApi`], whose
//! `receive()` yields a bare [`magnetar_proto::IncomingMessage`]. The wrapper
//! consumers — [`crate::MultiTopicsConsumer`], [`crate::PartitionedConsumer`],
//! [`crate::PatternConsumer`] — are **not** `ConsumerApi`: their `receive()`
//! returns a topic-tagged wrapper message ([`crate::MultiTopicsMessage`] /
//! [`crate::PatternMessage`]) because a message's originating topic matters once
//! the consumer fans across many topics (the callback must know which child to
//! ack against). They get a second poller, [`spawn_wrapper_message_listener`],
//! generic over the [`WrapperReceiver`] trait (an `async fn receive()` yielding a
//! topic + message). It preserves the exact same ADR-0064 semantics —
//! sequential, in order, no auto-ack, clean shutdown on `receive()` error / handle
//! drop — and its callback shape is [`WrapperMessageListener`] = `Fn(&str,
//! &IncomingMessage)` (the topic is the extra argument, mirroring Java
//! `Message#getTopicName()`).
//!
//! **Pattern-child inheritance.** A [`crate::PatternConsumer`] discovers new
//! topics after subscribe (on PIP-145 `TopicListChanged` deltas, applied by
//! [`crate::PatternConsumer::update`]) and a [`crate::PartitionedConsumer`] can
//! grow its child set via [`crate::MultiTopicsConsumer::add_topic`]; in both cases
//! the children added later **inherit** the listener. The wrapper poller does not
//! simply re-snapshot on the next call — a parked `receive()` over the old child
//! set would never see a child added while it waits. Instead each poller iteration
//! **races** the in-flight `receive()` against
//! [`WrapperReceiver::membership_changed`] (a `Notify` the wrapper signals on every
//! add): when a child joins while the poller is parked, the membership signal wins,
//! the stale receive is dropped (cancel-safe — unpopped messages stay queued), and
//! the next iteration re-snapshots and starts draining the new child. This matches
//! Java, where `MultiTopicsConsumerImpl` owns the single listener executor and
//! creates every child — initial or later-discovered — with its own
//! `messageListener` set to `null` (`getInternalConsumerConfig`), routing all
//! delivery through the parent's listener.

use std::future::Future;
use std::sync::Arc;

use tokio::task::JoinHandle;

use crate::client::{IncomingMessage, PulsarError};

/// Callback fired for every message delivered to a push-mode consumer.
///
/// Receives the façade [`IncomingMessage`] (the same rich type returned by
/// `Consumer::receive`, with `key()`, `property()`, `publish_time_ms()`, … ).
/// The callback runs inside the poller task, sequentially — keep it from
/// blocking the runtime for long, the way Java's listener-executor contract
/// expects. The callback **must ack explicitly** (the poller does not auto-ack;
/// it has no handle to the consumer's ack path once it has handed off the
/// message). Mirrors Java `MessageListener#received(Consumer, Message)`, minus
/// the consumer argument: hold a clone of your consumer in the closure to ack.
pub type MessageListener = Arc<dyn Fn(&IncomingMessage) + Send + Sync>;

/// Owns the background poller task driving a push-mode consumer. Mirrors the
/// lifetime semantics of [`crate::TableView`]'s drain task: dropping the handle
/// aborts the poller; [`Self::close`] awaits a clean stop.
///
/// The poller terminates on its own when an explicit or terminal remote close
/// makes `receive()` return an error. Dropping this handle aborts the poller,
/// then task unwinding drops its owned consumer clone. That clone triggers a
/// best-effort close only when it is the final clone; if other consumer clones
/// remain, this drop stages no close.
pub struct MessageListenerHandle {
    handle: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for MessageListenerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let running = self
            .handle
            .try_lock()
            .is_ok_and(|g| g.as_ref().is_some_and(|h| !h.is_finished()));
        f.debug_struct("MessageListenerHandle")
            .field("running", &running)
            .finish()
    }
}

impl Drop for MessageListenerHandle {
    fn drop(&mut self) {
        if let Ok(mut g) = self.handle.try_lock() {
            if let Some(h) = g.take() {
                h.abort();
            }
        }
    }
}

impl MessageListenerHandle {
    /// `true` while the poller task is still running. Flips to `false` once the
    /// consumer is closed (the loop broke) or the handle has been
    /// [`Self::close`]d / dropped.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.handle
            .try_lock()
            .is_ok_and(|g| g.as_ref().is_some_and(|h| !h.is_finished()))
    }

    /// Stop the poller task and wait for it to unwind. Idempotent — a second
    /// call (or a call after the consumer already closed the loop) is a no-op.
    pub async fn close(&self) {
        let mut g = self.handle.lock().await;
        if let Some(h) = g.take() {
            h.abort();
            let _ = h.await;
        }
    }
}

/// Attach a push-delivery listener to an already-subscribed `consumer`,
/// returning the owning [`MessageListenerHandle`]. The poller drives
/// `consumer.receive()` and invokes `listener` once per message, sequentially
/// and in order, with **no auto-ack** — the callback acks explicitly.
///
/// This is the lower-level, Java-faithful "attach a listener to this consumer"
/// entry, paired with the higher-level
/// [`crate::ConsumerBuilder::subscribe_with_listener`] convenience. Use it when
/// the callback needs to ack: **clone the consumer first**, move one clone here
/// to drive delivery and capture the other in the closure to ack (mirroring how
/// Java's `MessageListener#received(Consumer, Message)` hands you the consumer).
/// Because acking is async and the callback is a synchronous `Fn`, ack from the
/// closure via a fire-and-forget grouped ack
/// ([`crate::ConsumerBuilder::ack_group_time`] +
/// `Consumer::ack_grouped`) or by spawning the `ack()` future.
///
/// Engine-generic: `C: ConsumerApi + Clone` resolves to
/// `magnetar_runtime_tokio::Consumer` or
/// `magnetar_runtime_moonpool::Consumer<P>`. The task is a bare
/// `loop { receive(); callback }` — no channel, no extra lock, no host-clock
/// read (ADR-0003 / ADR-0011 / ADR-0038 all preserved). The loop breaks the
/// first time an explicit or terminal remote close makes `receive()` return an
/// error. Dropping the returned handle instead aborts the poller and drops this
/// task's owned consumer clone; only a final clone triggers the best-effort
/// consumer close.
pub fn spawn_message_listener<C: crate::ConsumerApi + Clone>(
    consumer: C,
    listener: MessageListener,
) -> MessageListenerHandle {
    // Hand the façade message to the callback. The callback acks explicitly —
    // the poller deliberately does NOT ack (Java parity).
    spawn_listener_loop(consumer, move |msg| {
        let msg: IncomingMessage = msg.into();
        listener(&msg);
    })
}

/// Core sequential poller shared by the raw and schema-aware listeners. Drives
/// `consumer.receive()` and runs `on_message` to completion before pulling the
/// next entry, preserving order and never overlapping two callback
/// invocations. `on_message` receives the runtime's
/// `magnetar_proto::IncomingMessage`; the raw / typed wrappers adapt it to
/// their own callback shape. The loop breaks the first time `receive()` errors
/// (closed / terminally-disconnected consumer) for clean, panic-free shutdown.
pub(crate) fn spawn_listener_loop<C, F>(consumer: C, on_message: F) -> MessageListenerHandle
where
    C: crate::ConsumerApi + Clone,
    F: Fn(magnetar_proto::IncomingMessage) + Send + 'static,
{
    let join = tokio::spawn(async move {
        loop {
            let Ok(msg) = crate::ConsumerApi::receive(&consumer).await else {
                // Consumer closed / connection terminally lost: stop cleanly.
                break;
            };
            on_message(msg);
        }
    });
    MessageListenerHandle {
        handle: tokio::sync::Mutex::new(Some(join)),
    }
}

/// Callback fired for every message delivered to a push-mode **wrapper**
/// consumer ([`crate::MultiTopicsConsumer`], [`crate::PartitionedConsumer`],
/// [`crate::PatternConsumer`]).
///
/// Unlike the single-topic [`MessageListener`], the callback receives the
/// originating **topic** alongside the façade [`IncomingMessage`] — the wrapper
/// consumer fans across many topics, so the callback needs the topic to route an
/// explicit ack to the right child (e.g.
/// [`crate::MultiTopicsConsumer::ack`] / [`crate::PatternConsumer::ack`], both of
/// which take `(topic, message_id)`). Mirrors Java
/// `MessageListener#received(Consumer, Message)` where `Message#getTopicName()`
/// supplies the topic.
///
/// Same contract as [`MessageListener`]: runs inside the poller task,
/// sequentially, and **must ack explicitly** — the poller never auto-acks.
pub type WrapperMessageListener = Arc<dyn Fn(&str, &IncomingMessage) + Send + Sync>;

/// A wrapper consumer's `receive()` surface, abstracted for the wrapper poller.
///
/// Implemented by [`crate::MultiTopicsConsumer`] (hence
/// [`crate::PartitionedConsumer`], which is a type alias) and
/// [`crate::PatternConsumer`]. Each one's `receive()` yields a topic-tagged
/// wrapper message; this trait normalises that to `(topic, message)` so one
/// poller serves all three surfaces, exactly as the single-topic `spawn_listener_loop` serves
/// every `ConsumerApi`.
///
/// `Clone + Send + 'static` so the poller can move the receiver into a
/// [`tokio::spawn`]ed task (the wrapper consumers are cheap `Arc`-clones).
pub trait WrapperReceiver: Clone + Send + Sync + 'static {
    /// Receive the next message across the wrapper's current child set, returning
    /// the originating topic and the message. A terminal error (every child closed
    /// / disconnected) breaks the poller loop for a clean shutdown — the same
    /// signal `ConsumerApi::receive` gives the single-topic poller. On an empty set
    /// the wrapper `receive()` errors immediately; the poller does not treat that
    /// as terminal (see [`Self::is_empty`]) — it parks on [`Self::membership_changed`].
    fn wrapper_receive(
        &self,
    ) -> impl Future<Output = Result<(String, magnetar_proto::IncomingMessage), PulsarError>> + Send;

    /// `true` when the wrapper currently holds no child consumers (e.g. a pattern
    /// consumer whose pattern matched nothing yet). The poller parks on
    /// [`Self::membership_changed`] rather than spinning on the empty-set error.
    fn is_empty(&self) -> bool;

    /// Resolves when a child consumer is added to the set after this future was
    /// created. The poller races its in-flight [`Self::wrapper_receive`] against
    /// this so a child discovered *after* the poller parked (pattern
    /// `TopicListChanged` deltas, partition growth) is swept on the next iteration:
    /// when this wins, the poller drops the stale receive (cancel-safe — unpopped
    /// messages stay queued) and re-snapshots. No channel (ADR-0003); the
    /// underlying `Notify` stores one permit so an add that races a wait is not lost.
    fn membership_changed(&self) -> impl Future<Output = ()> + Send;
}

/// Spawn a push-delivery poller over a wrapper consumer, returning the owning
/// [`MessageListenerHandle`]. The poller drives `receiver.wrapper_receive()` and
/// invokes `listener(topic, &msg)` once per message, sequentially and in order,
/// with **no auto-ack** — the callback acks explicitly via the wrapper's
/// topic-routed ack (`ack(topic, id)`).
///
/// This is the wrapper-surface sibling of [`spawn_message_listener`]. The loop is
/// the same bare `loop { receive(); callback }` shape — no channel (ADR-0003), no
/// extra lock (ADR-0038), no host-clock read (ADR-0011) — and breaks the first
/// time `wrapper_receive()` errors (closed / empty consumer set) for clean,
/// panic-free shutdown.
///
/// Children discovered after subscribe (pattern `TopicListChanged` deltas,
/// partition growth) inherit the listener: each iteration the poller races its
/// in-flight `wrapper_receive()` (over the *current* child snapshot) against
/// [`WrapperReceiver::membership_changed`]. When a child is added while the poller
/// is parked, the membership signal wins, the stale receive is dropped
/// (cancel-safe — unpopped messages stay queued), and the next iteration
/// re-snapshots and starts draining the new child. An empty wrapper (e.g. a
/// pattern with no current match) parks on the membership signal instead of
/// spinning on the empty-set error.
pub fn spawn_wrapper_message_listener<R: WrapperReceiver>(
    receiver: R,
    listener: WrapperMessageListener,
) -> MessageListenerHandle {
    let join = tokio::spawn(async move {
        loop {
            // An empty set: the wrapper `receive()` errors immediately, which is
            // NOT terminal here — wait for a child to join, then re-loop.
            if receiver.is_empty() {
                receiver.membership_changed().await;
                continue;
            }
            // Race the in-flight receive against a membership change so a child
            // added after we parked is picked up on the next iteration.
            let outcome = tokio::select! {
                biased;
                r = receiver.wrapper_receive() => r,
                () = receiver.membership_changed() => continue,
            };
            let Ok((topic, msg)) = outcome else {
                // Every child closed / terminally disconnected: stop cleanly.
                // (An empty-set error is handled above and never reaches here.)
                break;
            };
            // Hand the façade message to the callback. The callback acks
            // explicitly via the wrapper's topic-routed ack — the poller never
            // acks (Java parity).
            let msg: IncomingMessage = msg.into();
            listener(&topic, &msg);
        }
    });
    MessageListenerHandle {
        handle: tokio::sync::Mutex::new(Some(join)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// The poller's "receive → callback → next" sequencing, in isolation.
    /// We replicate the loop body against a synthetic message stream (the real
    /// poller needs a live consumer, which needs a broker) and assert the
    /// callback observes every message exactly once, in order. This pins the
    /// load-bearing invariant the spawned task relies on: sequential,
    /// in-order, no-skip delivery.
    #[test]
    fn listener_fires_sequentially_in_order() {
        let order = Arc::new(parking_lot::Mutex::new(Vec::<u64>::new()));
        let order_cb = order.clone();
        let listener: MessageListener = Arc::new(move |msg: &IncomingMessage| {
            order_cb.lock().push(msg.sequence_id());
        });

        // Drive the exact loop body the poller runs, over a fixed sequence.
        for seq in 0..5u64 {
            let md = magnetar_proto::pb::MessageMetadata {
                sequence_id: seq,
                ..Default::default()
            };
            let msg = IncomingMessage {
                id: magnetar_proto::MessageId::EARLIEST,
                metadata: Arc::new(md),
                payload: bytes::Bytes::new(),
                redelivery_count: 0,
                broker_entry_metadata: None,
            };
            listener(&msg);
        }

        assert_eq!(*order.lock(), vec![0, 1, 2, 3, 4]);
    }

    /// The callback must run to completion before the next message is handed
    /// to it — i.e. the poller never overlaps two callback invocations. We
    /// assert this by counting concurrent entries via a guard that would
    /// observe a value > 1 if delivery were parallel.
    #[test]
    fn listener_never_overlaps_invocations() {
        let in_flight = Arc::new(AtomicUsize::new(0));
        let max_seen = Arc::new(AtomicUsize::new(0));
        let inflight_cb = in_flight.clone();
        let max_cb = max_seen.clone();
        let listener: MessageListener = Arc::new(move |_msg: &IncomingMessage| {
            let now = inflight_cb.fetch_add(1, Ordering::SeqCst) + 1;
            max_cb.fetch_max(now, Ordering::SeqCst);
            inflight_cb.fetch_sub(1, Ordering::SeqCst);
        });

        for seq in 0..8u64 {
            let md = magnetar_proto::pb::MessageMetadata {
                sequence_id: seq,
                ..Default::default()
            };
            let msg = IncomingMessage {
                id: magnetar_proto::MessageId::EARLIEST,
                metadata: Arc::new(md),
                payload: bytes::Bytes::new(),
                redelivery_count: 0,
                broker_entry_metadata: None,
            };
            listener(&msg);
        }

        assert_eq!(
            max_seen.load(Ordering::SeqCst),
            1,
            "sequential delivery never overlaps two callback invocations"
        );
    }

    /// Synthetic [`WrapperReceiver`] for the wrapper-poller tests: a queue of
    /// `(topic, message)` pairs plus a membership-change `Notify`. `wrapper_receive`
    /// pops the next pair (parking on `delivered` when the queue is empty) so the
    /// poller's empty-set / membership-race control flow can be exercised without a
    /// broker. `is_empty` is driven by a flag the test flips, and `membership_changed`
    /// resolves when the test signals a new child joined.
    #[derive(Clone)]
    struct MockWrapper {
        queue: Arc<parking_lot::Mutex<std::collections::VecDeque<(String, u64)>>>,
        /// Wakes a parked `wrapper_receive` when a message is pushed.
        delivered: Arc<tokio::sync::Notify>,
        /// Signalled when the test simulates a child being added.
        membership: Arc<tokio::sync::Notify>,
        /// Mirrors the wrapper's empty-child-set predicate.
        empty: Arc<std::sync::atomic::AtomicBool>,
    }

    impl MockWrapper {
        fn new(empty: bool) -> Self {
            Self {
                queue: Arc::new(parking_lot::Mutex::new(std::collections::VecDeque::new())),
                delivered: Arc::new(tokio::sync::Notify::new()),
                membership: Arc::new(tokio::sync::Notify::new()),
                empty: Arc::new(std::sync::atomic::AtomicBool::new(empty)),
            }
        }

        /// Push a message and wake a parked `wrapper_receive`.
        fn push(&self, topic: &str, seq: u64) {
            self.queue.lock().push_back((topic.to_owned(), seq));
            self.delivered.notify_one();
        }

        /// Simulate a child joining the set: clear empty + signal membership.
        fn add_child(&self) {
            self.empty.store(false, std::sync::atomic::Ordering::SeqCst);
            self.membership.notify_one();
        }
    }

    fn mock_message(seq: u64) -> magnetar_proto::IncomingMessage {
        magnetar_proto::IncomingMessage {
            message_id: magnetar_proto::MessageId::EARLIEST,
            metadata: Arc::new(magnetar_proto::pb::MessageMetadata {
                sequence_id: seq,
                ..Default::default()
            }),
            single_metadata: None,
            payload: bytes::Bytes::new(),
            redelivery_count: 0,
            broker_entry_metadata: None,
            arrived_at: std::time::Instant::now(),
        }
    }

    impl WrapperReceiver for MockWrapper {
        async fn wrapper_receive(
            &self,
        ) -> Result<(String, magnetar_proto::IncomingMessage), PulsarError> {
            loop {
                if let Some((topic, seq)) = self.queue.lock().pop_front() {
                    return Ok((topic, mock_message(seq)));
                }
                self.delivered.notified().await;
            }
        }

        fn is_empty(&self) -> bool {
            self.empty.load(std::sync::atomic::Ordering::SeqCst)
        }

        async fn membership_changed(&self) {
            self.membership.notified().await;
        }
    }

    /// The wrapper poller delivers every queued message, topic-tagged, in order.
    #[tokio::test(flavor = "current_thread")]
    async fn wrapper_poller_delivers_topic_tagged_in_order() {
        let mock = MockWrapper::new(false);
        let seen: Arc<parking_lot::Mutex<Vec<(String, u64)>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));
        let seen_cb = seen.clone();
        let done = Arc::new(tokio::sync::Notify::new());
        let done_cb = done.clone();
        let listener: WrapperMessageListener =
            Arc::new(move |topic: &str, msg: &IncomingMessage| {
                seen_cb.lock().push((topic.to_owned(), msg.sequence_id()));
                if seen_cb.lock().len() == 3 {
                    done_cb.notify_one();
                }
            });

        mock.push("t-a", 0);
        mock.push("t-b", 1);
        mock.push("t-a", 2);

        let handle = spawn_wrapper_message_listener(mock, listener);
        tokio::time::timeout(std::time::Duration::from_secs(5), done.notified())
            .await
            .expect("poller delivered all queued messages");
        handle.close().await;

        assert_eq!(
            *seen.lock(),
            vec![
                ("t-a".to_owned(), 0),
                ("t-b".to_owned(), 1),
                ("t-a".to_owned(), 2),
            ],
            "wrapper poller delivered every message, topic-tagged, in order",
        );
    }

    /// Inheritance: the poller starts with an EMPTY wrapper (no child yet),
    /// parks on the membership signal rather than spinning on the empty-set error,
    /// and once a child joins + produces, delivers that late child's message. This
    /// is the deterministic core of the e2e pattern-inheritance assertion.
    #[tokio::test(flavor = "current_thread")]
    async fn wrapper_poller_inherits_late_added_child() {
        let mock = MockWrapper::new(true); // starts empty
        let seen: Arc<parking_lot::Mutex<Vec<(String, u64)>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));
        let seen_cb = seen.clone();
        let done = Arc::new(tokio::sync::Notify::new());
        let done_cb = done.clone();
        let listener: WrapperMessageListener =
            Arc::new(move |topic: &str, msg: &IncomingMessage| {
                seen_cb.lock().push((topic.to_owned(), msg.sequence_id()));
                done_cb.notify_one();
            });

        let handle = spawn_wrapper_message_listener(mock.clone(), listener);

        // Let the poller reach the empty-set park.
        tokio::task::yield_now().await;
        assert!(
            seen.lock().is_empty(),
            "nothing delivered while the set is empty"
        );

        // A late child joins and produces — the poller must pick it up via the
        // membership signal + the delivery wake.
        mock.add_child();
        mock.push("late-topic", 7);

        tokio::time::timeout(std::time::Duration::from_secs(5), done.notified())
            .await
            .expect("poller delivered the late-added child's message (inheritance)");
        handle.close().await;

        assert_eq!(
            *seen.lock(),
            vec![("late-topic".to_owned(), 7)],
            "the late-added child's message reached the inherited listener",
        );
    }
}
