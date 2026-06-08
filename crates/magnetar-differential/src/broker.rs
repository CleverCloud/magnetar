// SPDX-License-Identifier: Apache-2.0

//! Scripted in-process Pulsar broker for the differential harness.
//!
//! A real loopback TCP listener that speaks the tight subset of the
//! Pulsar binary protocol the [`crate::trace`] ops exercise:
//!
//! - `CONNECT` → `CONNECTED`
//! - `PRODUCER` → `PRODUCER_SUCCESS`
//! - `SEND` (payload frame) → `SEND_RECEIPT`
//! - `SUBSCRIBE` → `SUCCESS`
//! - pushed `MESSAGE` frames (one per outstanding flow permit + queued payload)
//! - `ACK` → `ACK_RESPONSE`
//! - `SEEK` → `SUCCESS`
//! - `FLOW` (no response — just counted)
//! - `CLOSE_PRODUCER` / `CLOSE_CONSUMER` → `SUCCESS`
//! - `PING` → `PONG`
//!
//! The broker keeps a per-consumer queue of pending pushes plus a
//! per-(producer-id) ledger of received sends so seeks / redeliveries
//! can replay. Both engines connect to the same broker over real TCP
//! loopback; the broker has no engine-specific knowledge.
//!
//! ## Injection knobs
//!
//! Three opt-in knobs script faults for the survivability differential
//! scenarios; all default off, so the golden traces and every other test see
//! a fault-free broker:
//!
//! - [`ScriptedBroker::inject_corrupted_frame_after_connected`] — one CRC32C-corrupted frame behind
//!   the handshake (recoverable; ADR-0054).
//! - [`ScriptedBroker::inject_decode_fatal_frame_on_send`] — one unparseable command frame in place
//!   of the first send receipt, then close (terminal; ADR-0055 §1).
//! - [`ScriptedBroker::drop_connection_after`] — the first session closes after writing N frames,
//!   forcing a supervised client to redial. This one also turns on **resume mode**: the ledger,
//!   per-topic entry-id sequence, and durable per-subscription cursor move into a cross-session
//!   store (`CrossSession`) so the redialled session resumes from the acked position
//!   (docs/follow-ups.md §4.2; ADR-0055 §3 shape). Reset both the knob and the persisted state with
//!   [`ScriptedBroker::clear_cross_session_state`] between legs that share one broker.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use magnetar_proto::{FrameError, MAX_FRAME_SIZE, decode_one, encode_command, encode_payload, pb};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// A single delivered message the broker has queued for a consumer.
#[derive(Debug, Clone)]
struct StoredMessage {
    ledger_id: u64,
    entry_id: u64,
    payload: Bytes,
    /// PIP-4 producer-stamped encryption metadata, preserved so the broker
    /// echoes `encryption_keys` / `encryption_algo` / `encryption_param` back
    /// on the pushed `CommandMessage`. A real broker is opaque to PIP-4 (it is
    /// a client-side concern) and round-trips the metadata verbatim; the
    /// scripted broker mirrors that so the consumer-side decrypt path is
    /// reachable in differential traces. `None` for plaintext sends.
    encryption_keys: Vec<pb::EncryptionKeys>,
    encryption_algo: Option<String>,
    encryption_param: Option<Bytes>,
}

#[derive(Debug, Default, Clone)]
struct ConsumerState {
    /// Outstanding flow permits (incremented by `CommandFlow`).
    permits: u32,
    /// Index of the next message in `ledger` to deliver (this session's
    /// **delivery position**). In resume mode it is seeded from the durable
    /// per-subscription ack cursor at subscribe time, so the un-acked tail is
    /// redelivered on a redial.
    cursor: usize,
    /// Pending redeliveries (negative-ack'd messages queued ahead of the
    /// normal cursor).
    nacked: Vec<StoredMessage>,
    /// Subscription name this consumer is bound to. Only populated in resume
    /// mode, where it is the key into [`CrossSession::cursors`] so a
    /// `CommandAck` advances the right durable cursor.
    subscription: String,
}

#[derive(Debug, Default)]
struct ProducerState {
    /// Next entry id to assign on this producer.
    next_entry_id: u64,
}

/// Cross-session broker state that survives a redial (ADR-0055 §3 shape,
/// mirrored from `magnetar-runtime-moonpool/tests/sim_chaos.rs`'s
/// `SharedBroker`).
///
/// Per-session [`SessionState`] is re-created on every accept, so its ledger
/// and per-consumer cursor vanish the instant a connection drops — fine for
/// the single-session golden traces, useless for a drop + redial scenario
/// where a replayed producer send and a re-subscribe must resume from where
/// the previous session left off. This struct persists exactly the
/// resume-relevant state, keyed by **stable identity** (NOT by the
/// per-session producer / consumer id the client re-allocates on reconnect):
///
/// - **ledger** + **next entry id** are keyed by **topic** (a producer re-opened on the same topic
///   resumes the same entry-id sequence);
/// - the durable per-subscription **ack cursor** is keyed by **subscription NAME** (a re-subscribe
///   under the same name resumes from the acked position — the un-acked tail is redelivered);
/// - the **send dedup** map is keyed by `(topic, sequence_id)` so an at-least-once replay of an
///   in-flight publish re-emits the *existing* receipt instead of double-appending.
///
/// It is shared behind an `Arc<Mutex<…>>` by every session of one
/// [`ScriptedBroker`], but is **only consulted when the drop knob is armed**
/// ([`ScriptedBroker::drop_connection_after`]). When the knob is disarmed —
/// the default for every other differential trace — each session stays fully
/// isolated on its own [`SessionState`], so two back-to-back legs on one
/// broker each start from an empty ledger (asserted by `broker_smoke`).
#[derive(Debug, Default)]
struct CrossSession {
    /// Per-topic append-only ledger. Survives the client's per-reconnect
    /// producer-id churn.
    ledger: HashMap<String, Vec<StoredMessage>>,
    /// Next entry id to assign per topic. Survives reconnect so a producer
    /// re-opened on the same topic resumes its entry-id sequence.
    next_entry_id: HashMap<String, u64>,
    /// Durable per-subscription ack cursor: the next entry index to deliver
    /// on this subscription. Keyed by subscription NAME, advanced only by a
    /// real `CommandAck`. A re-subscribe seeds its delivery position from
    /// here, so the un-acked tail is redelivered.
    cursors: HashMap<String, usize>,
    /// Send dedup: `(topic, sequence_id)` → the `(ledger_id, entry_id)` the
    /// broker already assigned. A replayed in-flight publish re-emits the
    /// existing receipt rather than appending a duplicate ledger entry.
    dedup: HashMap<(String, u64), (u64, u64)>,
}

/// Shared mutable state for the scripted broker. Each connection has
/// its own [`SessionState`] (this struct); resume-relevant state that must
/// survive a redial lives in the cross-session `CrossSession` store on the
/// parent [`ScriptedBroker`] handle (consulted only when the drop knob is
/// armed).
///
/// **Partition awareness.** Pulsar encodes partition identity in the
/// topic name itself via the `-partition-N` suffix (Java's
/// `TopicName.getPartitionIndex` convention); the broker therefore
/// reuses the existing per-topic `ledger`/`consumers` maps for
/// per-partition isolation (each `-partition-N` topic gets its own
/// ledger and cursor). The `per_partition` map adds an observability
/// view keyed by partition index (with `-1` for non-partitioned
/// topics): every broker-assigned message id is appended to its
/// partition's bucket as the broker stores it, and every seek that
/// targets a partitioned topic records the partition idx in
/// `seeked_partitions`. Both views let golden traces assert
/// per-partition dispatch without crawling the raw frame log.
#[derive(Debug, Default)]
struct SessionState {
    /// Per-topic message ledger (append-only).
    ledger: HashMap<String, Vec<StoredMessage>>,
    /// Per producer id (assigned by the client).
    producers: HashMap<u64, (String, ProducerState)>,
    /// Per consumer id (assigned by the client).
    consumers: HashMap<u64, (String, ConsumerState)>,
    /// Observability view of every stored message id grouped by
    /// partition index (parsed from the topic's `-partition-N`
    /// suffix; `-1` when the topic is non-partitioned).
    per_partition: HashMap<i32, Vec<(u64, u64)>>,
    /// Append-only log of partition indices touched by `CommandSeek`
    /// against partitioned topics. Lets traces assert that a seek on
    /// partition `K` did not move any other partition's cursor.
    seeked_partitions: Vec<i32>,
    /// Next txn id slot the broker allocates on `CommandNewTxn`.
    /// Mirrors what a real TC's `TransactionMetadataStore` does — gives
    /// each open transaction a monotonically-increasing low-bit pair so
    /// the client can correlate responses. We pin the high bits at 0
    /// because magnetar pins the TC id at 0 (see
    /// `TxnClient::new(0)`).
    next_txn_least_bits: u64,
    /// Per-txn ack ledger keyed by `(txnid_most_bits, txnid_least_bits)`.
    /// PIP-31: `CommandAck` carrying a txn id stages the ack against the
    /// txn; the broker only durably applies them on
    /// `CommandEndTxn(commit)` (drains the entry; `abort` would drop it).
    /// The differential trace asserts the drained-on-commit count.
    txn_ack_ledger: HashMap<(u64, u64), Vec<TxnStagedAck>>,
}

/// One acknowledgement staged against an open transaction. Drained on
/// `CommandEndTxn(commit)`; dropped on `CommandEndTxn(abort)`.
/// Fields are retained for completeness (a real broker would replay
/// them into the durable cursor on commit); the differential
/// assertion only inspects the entry count today.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TxnStagedAck {
    consumer_id: u64,
    message_ids: Vec<(u64, u64)>,
}

/// One observable txn-end event surfaced via [`ScriptedBroker::txn_drain_log_snapshot`].
///
/// Pushed by the `CommandEndTxn` arm of the broker's per-frame
/// dispatcher whenever a transaction is closed. `ack_count` is the
/// number of staged-ack
/// entries the broker had accumulated under `(most, least)` at the
/// moment of end; `drained == true` means the transaction was
/// committed (a real broker would apply the staged acks to the
/// durable cursor here); `drained == false` means it was aborted (the
/// staged acks were dropped without applying).
///
/// Lets the `txn_send_ack_then_commit` / `txn_send_ack_then_abort`
/// golden traces assert the drain count and the commit/abort flag
/// without crawling the raw frame log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnDrainEvent {
    /// `txnid_most_bits` carried by the closing `CommandEndTxn`.
    pub most: u64,
    /// `txnid_least_bits` carried by the closing `CommandEndTxn`.
    pub least: u64,
    /// `true` → committed (staged acks would be applied);
    /// `false` → aborted (staged acks were dropped).
    pub drained: bool,
    /// Number of staged-ack entries the broker held for
    /// `(most, least)` at the moment of `CommandEndTxn`. One per
    /// `CommandAck` carrying a `txn_id` observed since the matching
    /// `CommandNewTxn`.
    pub ack_count: usize,
}

/// Cross-session log of received `BaseCommand` kinds, in arrival order.
/// Mutated by every session task that the broker accepts; the equivalence
/// harness reads it after each engine run to assert ordering invariants
/// (e.g. lookup-before-producer-open).
pub type FrameLog = Arc<Mutex<Vec<i32>>>;

/// Cross-session, append-only log of partition indices touched by
/// `CommandSeek` against partitioned topics. The partition index is
/// parsed from the consumer's bound topic via the `-partition-N`
/// suffix (Java's `TopicName.getPartitionIndex` convention); `-1`
/// when the consumer is bound to a non-partitioned topic. Lets the
/// `seek-per-partition` golden trace assert that exactly one
/// partition's cursor was moved by a `SeekPartition` op.
pub type SeekedPartitionLog = Arc<Mutex<Vec<i32>>>;

/// Cross-session, append-only log of every `CommandEndTxn` the broker
/// observed, in arrival order. Each entry records the txn id halves,
/// whether the end was a commit (`drained: true`) or an abort
/// (`drained: false`), and how many staged acks the broker held for
/// the txn at end time. Lets the `txn_send_ack_then_commit` /
/// `txn_send_ack_then_abort` golden traces assert the drain count
/// directly.
pub type TxnDrainLog = Arc<Mutex<Vec<TxnDrainEvent>>>;

/// The broker-wide shared handles a single session task needs: the
/// cross-session logs the harness reads back, and the injection knobs that
/// arm the corrupted-frame / decode-fatal / drop-redial scenarios. Bundled
/// into one struct (cheaply `Clone`-able — every field is an `Arc`) so the
/// accept loop hands the session ONE value instead of a pile of arguments.
#[derive(Clone)]
struct SessionDeps {
    frame_log: FrameLog,
    seeked_partitions: SeekedPartitionLog,
    txn_drain_log: TxnDrainLog,
    corrupt_after_connected: Arc<Mutex<bool>>,
    decode_fatal_on_send: Arc<Mutex<bool>>,
    drop_after: Arc<Mutex<Option<usize>>>,
    dropped_once: Arc<AtomicBool>,
    cross_session: Arc<Mutex<CrossSession>>,
}

/// Handle to a running scripted broker. Drop to shut down.
pub struct ScriptedBroker {
    /// `host:port` the broker is bound to.
    addr: SocketAddr,
    shutdown: Arc<Notify>,
    accept_task: Option<JoinHandle<()>>,
    /// Shared, append-only log of every `BaseCommand` kind (as the
    /// `pb::base_command::Type` integer tag) seen across every session.
    frame_log: FrameLog,
    /// Shared, append-only log of partition indices that received a
    /// `CommandSeek`.
    seeked_partitions: SeekedPartitionLog,
    /// Shared, append-only log of every `CommandEndTxn` and its drain
    /// count. Surfaces the per-txn ack ledger's drain/drop side-effect
    /// to the golden-trace assertion path.
    txn_drain_log: TxnDrainLog,
    /// When `true`, every session writes ONE CRC32C-corrupted frame
    /// immediately after answering `CommandConnect` with
    /// `CommandConnected`. Armed by
    /// [`Self::inject_corrupted_frame_after_connected`] for the
    /// corrupted-frame differential scenario (ADR-0054 / decision Q2):
    /// the receiving proto layer must log + drop the frame and both
    /// engines must keep the connection alive.
    corrupt_after_connected: Arc<Mutex<bool>>,
    /// When `true`, the session answers the first `CommandSend` with ONE
    /// **decode-fatal** command frame (a corrupt length prefix whose
    /// command bytes are not valid protobuf) *instead of* a
    /// `CommandSendReceipt`, then closes the session. Armed by
    /// [`Self::inject_decode_fatal_frame_on_send`] for the terminal-error
    /// differential scenario (ADR-0055 §1).
    ///
    /// Unlike [`Self::corrupt_after_connected`] (a CRC32C payload mismatch
    /// the proto layer drops and recovers from), a decode-fatal command
    /// frame is unparseable from that byte on: the proto decode loop
    /// surfaces a fatal `Frame(Decode(..))` error, the plain driver exits,
    /// and `fail_all_pending` resolves the in-flight send future with
    /// `OpOutcome::Terminal` → `ClientError::PeerClosed`. Both engines must
    /// surface that terminal outcome identically.
    decode_fatal_on_send: Arc<Mutex<bool>>,
    /// When `Some(n)`, the FIRST session closes its socket after writing
    /// exactly `n` frames, forcing a supervised client to redial; every
    /// redialled session then serves normally (the [`Self::dropped_once`]
    /// latch gates the drop to one occurrence so the scenario is a single,
    /// deterministic drop + redial rather than a redial storm). Armed by
    /// [`Self::drop_connection_after`] for the drop + redial differential
    /// scenario (`reconnect_replay_gating_equivalence`). Arming this knob also
    /// switches every session into **resume mode**: the ledger + per-topic
    /// entry-id sequence + durable per-subscription cursor live in the
    /// cross-session `CrossSession` store so the redialled session resumes
    /// from the acked position instead of starting fresh. `None` (the
    /// default) keeps each session fully isolated and never drops — the shape
    /// every other differential trace relies on.
    drop_after: Arc<Mutex<Option<usize>>>,
    /// Latch ensuring [`Self::drop_after`] fires on exactly one session. The
    /// first session to reach the frame budget sets it; later sessions stay in
    /// resume mode but do not drop. Reset by [`Self::clear_cross_session_state`].
    dropped_once: Arc<AtomicBool>,
    /// Cross-session ledger + durable cursors, consulted only when
    /// [`Self::drop_after`] is armed. Shared by every session of this broker
    /// so resume-relevant state survives the client's per-reconnect id churn
    /// (ADR-0055 §3 shape).
    cross_session: Arc<Mutex<CrossSession>>,
}

impl std::fmt::Debug for ScriptedBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedBroker")
            .field("addr", &self.addr)
            .finish_non_exhaustive()
    }
}

impl ScriptedBroker {
    /// Bind to `127.0.0.1:0` (auto-assigned port) and start accepting
    /// connections.
    ///
    /// # Errors
    /// Surfaces the underlying [`TcpListener::bind`] failure.
    pub async fn bind() -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = shutdown.clone();
        let frame_log: FrameLog = Arc::new(Mutex::new(Vec::new()));
        let frame_log_clone = frame_log.clone();
        let seeked_partitions: SeekedPartitionLog = Arc::new(Mutex::new(Vec::new()));
        let seeked_partitions_clone = seeked_partitions.clone();
        let txn_drain_log: TxnDrainLog = Arc::new(Mutex::new(Vec::new()));
        let txn_drain_log_clone = txn_drain_log.clone();
        let corrupt_after_connected = Arc::new(Mutex::new(false));
        let corrupt_after_connected_clone = corrupt_after_connected.clone();
        let decode_fatal_on_send = Arc::new(Mutex::new(false));
        let decode_fatal_on_send_clone = decode_fatal_on_send.clone();
        let drop_after = Arc::new(Mutex::new(None));
        let drop_after_clone = drop_after.clone();
        let dropped_once = Arc::new(AtomicBool::new(false));
        let dropped_once_clone = dropped_once.clone();
        let cross_session = Arc::new(Mutex::new(CrossSession::default()));
        let cross_session_clone = cross_session.clone();
        let deps = SessionDeps {
            frame_log: frame_log_clone,
            seeked_partitions: seeked_partitions_clone,
            txn_drain_log: txn_drain_log_clone,
            corrupt_after_connected: corrupt_after_connected_clone,
            decode_fatal_on_send: decode_fatal_on_send_clone,
            drop_after: drop_after_clone,
            dropped_once: dropped_once_clone,
            cross_session: cross_session_clone,
        };
        let accept_task = tokio::spawn(async move {
            loop {
                let accept = listener.accept();
                tokio::select! {
                    res = accept => {
                        match res {
                            Ok((stream, _)) => {
                                tokio::spawn(handle_session(stream, deps.clone()));
                            }
                            Err(_) => break,
                        }
                    }
                    () = shutdown_clone.notified() => break,
                }
            }
        });
        Ok(Self {
            addr,
            shutdown,
            accept_task: Some(accept_task),
            frame_log,
            seeked_partitions,
            txn_drain_log,
            corrupt_after_connected,
            decode_fatal_on_send,
            drop_after,
            dropped_once,
            cross_session,
        })
    }

    /// Arm the corrupted-frame injection: every subsequent session writes
    /// ONE CRC32C-corrupted frame immediately after answering
    /// `CommandConnect` with `CommandConnected` (construction mirrors the
    /// proto unit test `frame::tests::detects_crc32c_mismatch`). Used by
    /// the corrupted-frame differential scenario (ADR-0054 / decision Q2)
    /// to prove both engines drop the frame at the proto layer and keep
    /// the connection — and the subsequent trace traffic — flowing.
    pub fn inject_corrupted_frame_after_connected(&self) {
        *self.corrupt_after_connected.lock() = true;
    }

    /// Arm the decode-fatal injection: the session answers the first
    /// `CommandSend` with ONE **decode-fatal** command frame (a corrupt
    /// length prefix whose command bytes are not valid protobuf) instead of
    /// a `CommandSendReceipt`, then ends the session. Used by the
    /// terminal-error differential scenario (ADR-0055 §1) to prove both
    /// engines surface the same terminal outcome
    /// (`OpOutcome::Terminal` → `ClientError::PeerClosed`) on the in-flight
    /// send rather than hanging on a connection that is gone.
    ///
    /// Contrast with [`Self::inject_corrupted_frame_after_connected`], whose
    /// CRC32C payload mismatch is *recoverable* (the proto layer drops the
    /// frame and the connection survives). A decode-fatal command frame is
    /// terminal: the byte stream is unparseable from that point on.
    pub fn inject_decode_fatal_frame_on_send(&self) {
        *self.decode_fatal_on_send.lock() = true;
    }

    /// Arm the drop + redial injection: the FIRST session closes its socket
    /// immediately after writing exactly `n` frames, forcing a supervised
    /// client to redial; every redialled session then serves to completion.
    /// The one-shot latch keeps the scenario a single, deterministic drop +
    /// redial rather than a redial storm. The per-session frame counter is
    /// deterministic (it counts encoded `BaseCommand` replies in the order the
    /// broker writes them), so the drop lands at the same wire position on
    /// every engine leg.
    ///
    /// Arming this knob also switches every session into **resume mode**: the
    /// ledger, per-topic entry-id sequence, and durable per-subscription
    /// cursor move out of the volatile per-session state into the
    /// cross-session `CrossSession` store, so the replayed in-flight publish
    /// and the re-subscribe after the redial resume from the acked position
    /// instead of starting fresh (ADR-0055 §3 shape). A replayed publish is
    /// de-duplicated by `(topic, sequence_id)` so it re-emits the existing
    /// receipt rather than double-appending.
    ///
    /// **Reset rule.** Disarming with `drop_connection_after(0)` is *not* the
    /// reset — passing `0` would close the first session before its handshake
    /// reply, which no scenario wants. Instead the disarm + state reset is
    /// [`Self::clear_cross_session_state`], which clears the persisted ledger /
    /// cursors, re-arms the one-shot latch, and re-disarms the knob, mirroring
    /// [`Self::clear_frame_log`] for between-leg isolation.
    pub fn drop_connection_after(&self, n: usize) {
        *self.drop_after.lock() = Some(n);
    }

    /// Number of `(topic, message)` entries persisted in the cross-session
    /// ledger. `0` whenever the drop knob has never been armed (every other
    /// differential trace stays on per-session isolation). Used by
    /// `broker_smoke` to assert that two back-to-back legs on one broker each
    /// start from an EMPTY ledger, so a missing
    /// [`Self::clear_cross_session_state`] reset fails loudly.
    #[must_use]
    pub fn cross_session_ledger_len(&self) -> usize {
        self.cross_session
            .lock()
            .ledger
            .values()
            .map(Vec::len)
            .sum()
    }

    /// Disarm the drop knob and clear all cross-session ledger / cursor /
    /// dedup state. Call between two legs that share one broker so the second
    /// leg starts from an empty ledger (mirrors [`Self::clear_frame_log`]).
    ///
    /// This is the deterministic reset rule for
    /// [`Self::drop_connection_after`]: it re-disarms the knob (sessions go
    /// back to per-session isolation and never drop) and wipes the persisted
    /// resume state in one call, so a missing reset between legs fails loudly
    /// (the second leg would observe the first leg's ledger entries).
    pub fn clear_cross_session_state(&self) {
        *self.drop_after.lock() = None;
        self.dropped_once.store(false, Ordering::SeqCst);
        let mut cross = self.cross_session.lock();
        cross.ledger.clear();
        cross.next_entry_id.clear();
        cross.cursors.clear();
        cross.dedup.clear();
    }

    /// Snapshot the frame log: every `BaseCommand` kind seen so far,
    /// in arrival order, across all sessions.
    #[must_use]
    pub fn frame_log_snapshot(&self) -> Vec<i32> {
        self.frame_log.lock().clone()
    }

    /// Clear the frame log. Useful between engine runs so the second
    /// engine's snapshot doesn't include the first engine's frames.
    pub fn clear_frame_log(&self) {
        self.frame_log.lock().clear();
    }

    /// Snapshot the partition indices touched by every `CommandSeek`
    /// received so far, in arrival order. Used by the seek-per-partition
    /// golden trace to assert that a seek on partition `K` did not bleed
    /// into any other partition's cursor.
    #[must_use]
    pub fn seeked_partitions_snapshot(&self) -> Vec<i32> {
        self.seeked_partitions.lock().clone()
    }

    /// Clear the seeked-partitions log. Mirrors [`Self::clear_frame_log`]
    /// for isolating per-engine snapshots when running both legs against
    /// the same broker instance.
    pub fn clear_seeked_partitions(&self) {
        self.seeked_partitions.lock().clear();
    }

    /// Snapshot every txn-drain event observed so far, in arrival order
    /// across all sessions. Each [`TxnDrainEvent`] records the
    /// `(most, least)` txn-id halves, whether the end was a commit
    /// (`drained: true`) or an abort (`drained: false`), and the
    /// staged-ack count at end time. Used by the `txn_send_ack_*` golden
    /// traces to assert the drain count without crawling the raw frame
    /// log.
    #[must_use]
    pub fn txn_drain_log_snapshot(&self) -> Vec<TxnDrainEvent> {
        self.txn_drain_log.lock().clone()
    }

    /// Clear the txn-drain log. Mirrors [`Self::clear_frame_log`] for
    /// isolating per-engine snapshots when running both legs against the
    /// same broker instance.
    pub fn clear_txn_drain_log(&self) {
        self.txn_drain_log.lock().clear();
    }

    /// `pulsar://127.0.0.1:<port>` URL the engines should connect to.
    #[must_use]
    pub fn pulsar_url(&self) -> String {
        format!("pulsar://{}", self.addr)
    }

    /// `host:port` the moonpool engine wants directly.
    #[must_use]
    pub fn host_port(&self) -> String {
        self.addr.to_string()
    }

    /// Wait for the broker to finish in-flight work and shut down. The
    /// internal accept loop terminates on next iteration; outstanding
    /// session tasks are detached.
    pub async fn shutdown(mut self) {
        self.shutdown.notify_waiters();
        if let Some(t) = self.accept_task.take() {
            // Best-effort: ignore JoinError.
            let _ = tokio::time::timeout(Duration::from_millis(500), t).await;
        }
    }
}

impl Drop for ScriptedBroker {
    fn drop(&mut self) {
        self.shutdown.notify_waiters();
        if let Some(t) = self.accept_task.take() {
            t.abort();
        }
    }
}

/// Parse the partition index from a Pulsar topic name. Mirrors Java's
/// `TopicName.getPartitionIndex`: returns the trailing integer from a
/// `-partition-N` suffix, or `-1` when the topic is non-partitioned.
///
/// Used by the scripted broker so traces can address partitions by
/// integer index (the wire protocol carries partition identity in the
/// topic-name suffix, not in a dedicated field on `CommandSubscribe`).
fn partition_index_of(topic: &str) -> i32 {
    if let Some(idx) = topic.rfind("-partition-") {
        topic[idx + "-partition-".len()..]
            .parse::<i32>()
            .unwrap_or(-1)
    } else {
        -1
    }
}

/// Count the number of complete frames at the head of `buf`. Used by the
/// drop-after-N knob to know how many frames a pending flush would carry, so
/// the session can stop at exactly the Nth frame. Every byte the broker
/// writes is a frame it itself encoded, so the buffer always parses cleanly
/// here; a non-frame tail (impossible in practice) is ignored.
fn count_frames(buf: &[u8]) -> usize {
    let mut cursor = Bytes::copy_from_slice(buf);
    let mut count = 0;
    while !cursor.is_empty() {
        match decode_one(&mut cursor) {
            Ok(_) => count += 1,
            Err(_) => break,
        }
    }
    count
}

/// Byte length of the first `n` complete frames at the head of `buf`. Used by
/// the drop-after-N knob to truncate a pending flush to exactly `n` frames.
/// If `buf` holds fewer than `n` frames, returns the full parsed length.
fn frame_prefix_len(buf: &[u8], n: usize) -> usize {
    let mut cursor = Bytes::copy_from_slice(buf);
    let total = cursor.len();
    let mut taken = 0;
    while taken < n && !cursor.is_empty() {
        match decode_one(&mut cursor) {
            Ok(_) => taken += 1,
            Err(_) => break,
        }
    }
    total - cursor.len()
}

async fn handle_session(mut stream: TcpStream, deps: SessionDeps) {
    // Only the handles this session loop touches directly are destructured;
    // the per-frame logs (`seeked_partitions`, `txn_drain_log`) are read
    // through `deps` inside `handle_frame`.
    let SessionDeps {
        frame_log,
        corrupt_after_connected,
        decode_fatal_on_send,
        drop_after,
        dropped_once,
        cross_session,
        ..
    } = &deps;
    let state = Arc::new(Mutex::new(SessionState::default()));
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    // Set by the Send arm once it has written the decode-fatal frame: the
    // session must flush that frame and then close (the byte stream is
    // unparseable from there on, so there is nothing more to do).
    let mut terminate_after_flush = false;
    // Resume mode: the drop knob is armed, so the ledger + durable cursors
    // live in the cross-session store. Snapshot the knob ONCE at session start
    // so a `clear_cross_session_state` mid-flight does not change this
    // session's behaviour. `None` → never drop, per-session isolation.
    let armed = *drop_after.lock();
    let resume = armed.map(|_| cross_session);
    // This session drops only if the knob is armed AND it wins the one-shot
    // drop latch (no earlier session has claimed it) — so the scenario is a
    // single, deterministic drop + redial, and every redialled session then
    // serves to completion (resuming from the durable cursor). The CAS claims
    // the latch atomically: `Ok` means this session is the one that drops.
    let drop_at = armed.filter(|_| {
        dropped_once
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    });
    // Count of frames the broker has written on this session, used only when
    // `drop_at` is `Some`.
    let mut frames_written: usize = 0;
    eprintln!("[broker] session opened");

    loop {
        // Decode every complete frame currently in the buffer, then
        // read more bytes if nothing decoded (or after we handled what
        // we had). We drain on every iteration to avoid wedging when
        // the client pipelined multiple frames into one packet.
        loop {
            // Snapshot the buffer as Bytes, decode advancing the
            // snapshot, then split_to on the BytesMut by however many
            // bytes were consumed.
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(e) => {
                    eprintln!("[broker] decode error: {e:?}");
                    return;
                }
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            eprintln!("[broker] decoded frame type={}", frame.command.r#type);
            frame_log.lock().push(frame.command.r#type);
            let corrupt = *corrupt_after_connected.lock();
            let fatal_on_send = *decode_fatal_on_send.lock();
            let keep_going = handle_frame(
                &state,
                &frame,
                &mut out_buf,
                &deps,
                corrupt,
                fatal_on_send,
                resume,
            );
            if !keep_going {
                // The decode-fatal frame is already staged in `out_buf`;
                // flush it below, then close the session.
                terminate_after_flush = true;
                break;
            }
        }

        // Push any queued messages to consumers with outstanding permits.
        push_pending(&state, &mut out_buf, resume);

        if !out_buf.is_empty() {
            // Drop-after-N: when the knob is armed, truncate `out_buf` to the
            // first `drop_at - frames_written` complete frames, write those,
            // and close the session so a supervised client redials. The
            // counter is per-session and deterministic (frames are emitted in
            // a fixed order), so the drop lands at the same wire position on
            // every engine leg.
            let mut close_after_write = false;
            if let Some(limit) = drop_at {
                let remaining = limit.saturating_sub(frames_written);
                let available = count_frames(&out_buf);
                if available >= remaining {
                    let cut = frame_prefix_len(&out_buf, remaining);
                    out_buf.truncate(cut);
                    frames_written = limit;
                    close_after_write = true;
                } else {
                    frames_written += available;
                }
            }
            eprintln!("[broker] writing {} bytes", out_buf.len());
            if stream.write_all(&out_buf).await.is_err() {
                eprintln!("[broker] write failed");
                return;
            }
            if stream.flush().await.is_err() {
                eprintln!("[broker] flush failed");
                return;
            }
            out_buf.clear();
            if close_after_write {
                eprintln!(
                    "[broker] drop-after-{} reached; closing session",
                    drop_at.unwrap_or(0)
                );
                return;
            }
        }

        if terminate_after_flush {
            eprintln!("[broker] decode-fatal frame flushed; closing session");
            return;
        }

        // Read more bytes.
        eprintln!("[broker] about to read; buf has {} bytes", read_buf.len());
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => {
                eprintln!("[broker] read returned 0/err");
                return;
            }
            Ok(n) => eprintln!("[broker] read {n} bytes; buf now has {}", read_buf.len()),
        }
    }
}

/// Handle one decoded frame, writing any replies into `out`.
///
/// Returns `false` when the session must close after the current `out`
/// buffer is flushed — used by the decode-fatal-on-send injection
/// (ADR-0055 §1), which writes ONE unparseable command frame in place of a
/// `CommandSendReceipt` and then ends the session. Every other arm returns
/// `true` (keep serving).
fn handle_frame(
    state: &Arc<Mutex<SessionState>>,
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
    deps: &SessionDeps,
    corrupt_after_connected: bool,
    decode_fatal_on_send: bool,
    resume: Option<&Arc<Mutex<CrossSession>>>,
) -> bool {
    let seeked_partitions = &deps.seeked_partitions;
    let txn_drain_log = &deps.txn_drain_log;
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return true;
    };
    match kind {
        pb::base_command::Type::Connect => {
            emit_connected(out);
            // Corrupted-frame differential scenario (ADR-0054 / decision
            // Q2): when armed, follow the handshake reply with ONE
            // CRC32C-corrupted frame so both engine legs observe the
            // corruption at the same wire position — right behind
            // `CommandConnected`, ahead of any lookup traffic.
            if corrupt_after_connected {
                emit_corrupted_frame(out);
            }
        }
        pb::base_command::Type::Ping => emit_pong(out),
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                emit_lookup_response(out, l.request_id);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                let mut g = state.lock();
                g.producers
                    .insert(p.producer_id, (p.topic.clone(), ProducerState::default()));
                emit_producer_success(out, p.request_id, &p.topic);
            }
        }
        pb::base_command::Type::Send => {
            // Terminal-error differential scenario (ADR-0055 §1): when armed,
            // answer the in-flight send with ONE decode-fatal command frame
            // instead of a `CommandSendReceipt`, then close the session. The
            // proto decode loop surfaces a fatal `Frame(Decode(..))`, the
            // plain driver exits, and `fail_all_pending` resolves the
            // pending `SendFut` with `OpOutcome::Terminal` →
            // `ClientError::PeerClosed`. Both engines must behave identically.
            if decode_fatal_on_send {
                emit_decode_fatal_frame(out);
                return false;
            }
            if let (Some(s), Some(payload)) = (&frame.command.send, &frame.payload) {
                let topic = state
                    .lock()
                    .producers
                    .get(&s.producer_id)
                    .map(|(t, _)| t.clone());
                if let Some(topic) = topic {
                    let stored_partial = |ledger_id: u64, entry_id: u64| StoredMessage {
                        ledger_id,
                        entry_id,
                        payload: payload.body.clone(),
                        // Preserve the producer's PIP-4 encryption metadata so
                        // the pushed `CommandMessage` round-trips it verbatim.
                        encryption_keys: payload.metadata.encryption_keys.clone(),
                        encryption_algo: payload.metadata.encryption_algo.clone(),
                        encryption_param: payload.metadata.encryption_param.clone(),
                    };
                    let partition = partition_index_of(&topic);
                    // PIP-180 / ADR-0033: if the client asserted a source-topic
                    // `MessageId` via `CommandSend.message_id`, echo it back on
                    // the receipt verbatim (mirrors the upstream broker's
                    // shadow-topic replicator handling — the broker preserves
                    // the source id chain). Without this round-trip, the
                    // engine's `SendFut` would resolve to the broker-allocated
                    // `(1, next_entry_id)` and shadow-side dedup would break.
                    let asserted = s.message_id.as_ref().map(|m| (m.ledger_id, m.entry_id));
                    let (ledger_id, entry_id) = if let Some(cross) = resume {
                        // Resume mode: assign the entry id from the durable
                        // per-topic sequence and de-duplicate a replayed
                        // in-flight publish by `(topic, sequence_id)` — an
                        // at-least-once replay after a redial must re-emit the
                        // *existing* receipt, not append a second ledger entry.
                        let mut c = cross.lock();
                        let key = (topic.clone(), s.sequence_id);
                        if let Some(&(lid, eid)) = c.dedup.get(&key) {
                            (lid, eid)
                        } else {
                            let (ledger_id, entry_id) = if let Some((lid, eid)) = asserted {
                                (lid, eid)
                            } else {
                                let next = c.next_entry_id.entry(topic.clone()).or_insert(0);
                                let eid = *next;
                                *next += 1;
                                (1u64, eid)
                            };
                            c.dedup.insert(key, (ledger_id, entry_id));
                            c.ledger
                                .entry(topic.clone())
                                .or_default()
                                .push(stored_partial(ledger_id, entry_id));
                            (ledger_id, entry_id)
                        }
                    } else if let Some((lid, eid)) = asserted {
                        // Round-trip preservation — use the client's id.
                        let mut g = state.lock();
                        g.ledger
                            .entry(topic.clone())
                            .or_default()
                            .push(stored_partial(lid, eid));
                        (lid, eid)
                    } else {
                        let mut g = state.lock();
                        let entry_id = {
                            let prod = g
                                .producers
                                .get_mut(&s.producer_id)
                                .expect("producer registered above");
                            let entry_id = prod.1.next_entry_id;
                            prod.1.next_entry_id += 1;
                            entry_id
                        };
                        g.ledger
                            .entry(topic.clone())
                            .or_default()
                            .push(stored_partial(1, entry_id));
                        (1u64, entry_id)
                    };
                    state
                        .lock()
                        .per_partition
                        .entry(partition)
                        .or_default()
                        .push((ledger_id, entry_id));
                    emit_send_receipt(out, s.producer_id, s.sequence_id, ledger_id, entry_id);
                }
            }
        }
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                // Resume mode: seed this session's delivery position from the
                // durable per-subscription ack cursor so a re-subscribe after
                // a redial redelivers the un-acked tail (the ack cursor only
                // advances on a real `CommandAck`; see the Ack arm). In
                // isolated mode the cursor starts at 0 as before.
                let cursor = if let Some(cross) = resume {
                    *cross.lock().cursors.get(&s.subscription).unwrap_or(&0)
                } else {
                    0
                };
                state.lock().consumers.insert(
                    s.consumer_id,
                    (
                        s.topic.clone(),
                        ConsumerState {
                            cursor,
                            subscription: s.subscription.clone(),
                            ..ConsumerState::default()
                        },
                    ),
                );
                emit_success(out, s.request_id);
            }
        }
        pb::base_command::Type::Flow => {
            if let Some(f) = &frame.command.flow {
                let mut g = state.lock();
                if let Some((_, c)) = g.consumers.get_mut(&f.consumer_id) {
                    c.permits = c.permits.saturating_add(f.message_permits);
                }
            }
        }
        pb::base_command::Type::Ack => {
            if let Some(a) = &frame.command.ack {
                // Resume mode: advance the durable per-subscription cursor past
                // the highest acked entry so a later re-subscribe resumes from
                // the acked position (the un-acked tail is redelivered). A
                // non-txn ack is the only one that advances the durable cursor;
                // a txn-staged ack stays pending until `CommandEndTxn(commit)`.
                if let Some(cross) = resume {
                    if a.txnid_most_bits.is_none() && a.txnid_least_bits.is_none() {
                        let sub = state
                            .lock()
                            .consumers
                            .get(&a.consumer_id)
                            .map(|(_, c)| c.subscription.clone());
                        if let Some(sub) = sub {
                            // `entry_id` is the 0-based ledger index; acking
                            // entry E means "delivered + acked through E", so
                            // the next entry to deliver is E + 1.
                            let acked_through = a
                                .message_id
                                .iter()
                                .map(|m| m.entry_id)
                                .max()
                                .map(|e| usize::try_from(e).unwrap_or(usize::MAX));
                            if let Some(next) = acked_through.map(|e| e.saturating_add(1)) {
                                let mut c = cross.lock();
                                let cur = c.cursors.entry(sub).or_insert(0);
                                *cur = (*cur).max(next);
                            }
                        }
                    }
                }
                // PIP-31: if the ack carries a txn id, stage it against
                // the txn ledger; the broker only durably applies the
                // staged acks on `CommandEndTxn(commit)`.
                if let (Some(most), Some(least)) = (a.txnid_most_bits, a.txnid_least_bits) {
                    let staged = TxnStagedAck {
                        consumer_id: a.consumer_id,
                        message_ids: a
                            .message_id
                            .iter()
                            .map(|m| (m.ledger_id, m.entry_id))
                            .collect(),
                    };
                    state
                        .lock()
                        .txn_ack_ledger
                        .entry((most, least))
                        .or_default()
                        .push(staged);
                }
                // ACK_RESPONSE is required only when the client included
                // a request id (PIP-72). The state machine always sets
                // one; we mirror that back.
                if let Some(rid) = a.request_id {
                    emit_ack_response(out, a.consumer_id, rid);
                }
            }
        }
        pb::base_command::Type::TcClientConnectRequest => {
            // PIP-31 / magnetar `ensure_txn_bootstrapped`: the client
            // hand-shakes the TC (tc_id pinned to 0 by magnetar) and
            // expects a `TcClientConnectResponse` carrying back the
            // request_id. The real Pulsar broker only responds once the
            // TC metadata store is loaded; our scripted broker is
            // synchronously "ready" so we ack immediately.
            if let Some(req) = &frame.command.tc_client_connect_request {
                emit_tc_client_connect_response(out, req.request_id);
            }
        }
        pb::base_command::Type::NewTxn => {
            if let Some(req) = &frame.command.new_txn {
                let least = {
                    let mut g = state.lock();
                    let least = g.next_txn_least_bits;
                    g.next_txn_least_bits = g.next_txn_least_bits.saturating_add(1);
                    least
                };
                emit_new_txn_response(out, req.request_id, 0, least);
            }
        }
        pb::base_command::Type::AddPartitionToTxn => {
            if let Some(req) = &frame.command.add_partition_to_txn {
                emit_add_partition_to_txn_response(
                    out,
                    req.request_id,
                    req.txnid_most_bits.unwrap_or(0),
                    req.txnid_least_bits.unwrap_or(0),
                );
            }
        }
        pb::base_command::Type::AddSubscriptionToTxn => {
            if let Some(req) = &frame.command.add_subscription_to_txn {
                emit_add_subscription_to_txn_response(
                    out,
                    req.request_id,
                    req.txnid_most_bits.unwrap_or(0),
                    req.txnid_least_bits.unwrap_or(0),
                );
            }
        }
        pb::base_command::Type::EndTxn => {
            if let Some(req) = &frame.command.end_txn {
                let most = req.txnid_most_bits.unwrap_or(0);
                let least = req.txnid_least_bits.unwrap_or(0);
                // PIP-31: drain the per-txn ack ledger on commit;
                // drop it (without applying) on abort. Either way the
                // entry is removed from the broker's open-txn map.
                // The `action` (commit vs abort) is encoded as a
                // `TxnAction` enum on the wire (`Commit = 0`, `Abort = 1`).
                let drained = state.lock().txn_ack_ledger.remove(&(most, least));
                let ack_count = drained.as_ref().map_or(0, Vec::len);
                // `txn_action` is `Option<i32>` mapping to `pb::TxnAction`
                // (`Commit = 0`, `Abort = 1`). Magnetar's `Op::EndTxn`
                // always sets it; treat `None` as commit defensively.
                let committed = req
                    .txn_action
                    .is_none_or(|a| a == pb::TxnAction::Commit as i32);
                // `drained.unwrap_or_default()` would be applied to the
                // durable cursor in a real broker on commit; the
                // scripted broker surfaces the (drain/drop, count) pair
                // through the cross-session `TxnDrainLog` instead so the
                // golden traces can assert the per-txn ack ledger's
                // commit/abort side-effect directly.
                txn_drain_log.lock().push(TxnDrainEvent {
                    most,
                    least,
                    drained: committed,
                    ack_count,
                });
                emit_end_txn_response(out, req.request_id, most, least);
            }
        }
        pb::base_command::Type::Seek => {
            if let Some(s) = &frame.command.seek {
                let mut g = state.lock();
                if let Some((topic, c)) = g.consumers.get_mut(&s.consumer_id) {
                    // Seek to the first message at-or-after the given
                    // message id; if no message id was provided, reset
                    // to the beginning. Each `-partition-N` topic has
                    // its OWN ledger + cursor, so this naturally only
                    // moves the cursor on the partition addressed by
                    // this consumer — other partitions' consumers are
                    // untouched.
                    let topic_owned = topic.clone();
                    if let Some(mid) = &s.message_id {
                        let ledger = g.ledger.get(&topic_owned).cloned().unwrap_or_default();
                        let new_cursor = ledger
                            .iter()
                            .position(|m| {
                                m.ledger_id > mid.ledger_id
                                    || (m.ledger_id == mid.ledger_id && m.entry_id >= mid.entry_id)
                            })
                            .unwrap_or(0);
                        // Need to re-acquire mut borrow to update cursor.
                        let (_, c) = g.consumers.get_mut(&s.consumer_id).expect("present above");
                        c.cursor = new_cursor;
                        c.nacked.clear();
                    } else {
                        c.cursor = 0;
                        c.nacked.clear();
                    }
                    let partition = partition_index_of(&topic_owned);
                    g.seeked_partitions.push(partition);
                    seeked_partitions.lock().push(partition);
                    emit_success(out, s.request_id);
                }
            }
        }
        pb::base_command::Type::RedeliverUnacknowledgedMessages => {
            // Nack path: the state machine wraps `negative_ack` into a
            // RedeliverUnacknowledgedMessages with explicit message ids.
            if let Some(r) = &frame.command.redeliver_unacknowledged_messages {
                let mut g = state.lock();
                if let Some((topic, _c)) = g.consumers.get(&r.consumer_id).cloned() {
                    // Pull the matching stored messages and queue them
                    // for redelivery (front-loaded, ahead of cursor).
                    let ledger = g.ledger.get(&topic).cloned().unwrap_or_default();
                    let mut found: Vec<StoredMessage> = Vec::new();
                    for mid in &r.message_ids {
                        if let Some(m) = ledger
                            .iter()
                            .find(|m| m.ledger_id == mid.ledger_id && m.entry_id == mid.entry_id)
                        {
                            found.push(m.clone());
                        }
                    }
                    if let Some((_, c)) = g.consumers.get_mut(&r.consumer_id) {
                        c.nacked.extend(found);
                    }
                }
            }
        }
        pb::base_command::Type::CloseProducer => {
            if let Some(c) = &frame.command.close_producer {
                state.lock().producers.remove(&c.producer_id);
                emit_success(out, c.request_id);
            }
        }
        pb::base_command::Type::CloseConsumer => {
            if let Some(c) = &frame.command.close_consumer {
                state.lock().consumers.remove(&c.consumer_id);
                emit_success(out, c.request_id);
            }
        }
        _ => {}
    }
    // Default: keep serving. The decode-fatal-on-send arm is the only one
    // that returns `false` (above), to close the session after writing its
    // unparseable frame.
    true
}

fn push_pending(
    state: &Arc<Mutex<SessionState>>,
    out: &mut BytesMut,
    resume: Option<&Arc<Mutex<CrossSession>>>,
) {
    // Build a snapshot of which consumer is owed how many sends, then
    // emit; this avoids holding the lock across the encode loop.
    let mut to_push: Vec<(u64, Vec<StoredMessage>)> = Vec::new();
    {
        let mut g = state.lock();
        // Avoid `clone_into_iter`-style traps: collect ids first.
        let ids: Vec<u64> = g.consumers.keys().copied().collect();
        for cid in ids {
            let Some((topic, c)) = g.consumers.get_mut(&cid) else {
                continue;
            };
            if c.permits == 0 {
                continue;
            }
            let topic = topic.clone();
            let mut batch = Vec::new();
            // Drain nacked redeliveries first.
            while c.permits > 0 && !c.nacked.is_empty() {
                let m = c.nacked.remove(0);
                batch.push(m);
                c.permits -= 1;
            }
            // Then deliver from the cursor. In resume mode the topic's
            // messages live in the cross-session ledger (so they survive a
            // redial); in isolated mode they live on this session.
            let ledger = match resume {
                Some(cross) => cross.lock().ledger.get(&topic).cloned().unwrap_or_default(),
                None => g.ledger.get(&topic).cloned().unwrap_or_default(),
            };
            let (_, c) = g.consumers.get_mut(&cid).expect("present");
            while c.permits > 0 && c.cursor < ledger.len() {
                batch.push(ledger[c.cursor].clone());
                c.cursor += 1;
                c.permits -= 1;
            }
            if !batch.is_empty() {
                to_push.push((cid, batch));
            }
        }
    }
    for (cid, batch) in to_push {
        for m in batch {
            emit_message(out, cid, &m);
        }
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-differential-broker".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Encode one deliberately CRC32C-corrupted payload frame: a broker-push
/// `CommandMessage` whose last payload byte is flipped after encoding so the
/// CRC32C in the frame no longer matches the carried bytes (construction
/// mirrors the proto unit test `frame::tests::detects_crc32c_mismatch`).
///
/// The receiving proto layer must log the mismatch at the point of
/// detection, push `ConnectionEvent::ChecksumMismatch`, drop the frame, and
/// keep the connection alive (workspace invariant 4, "CRC32C verify or
/// drop") — the corrupted-frame differential trace asserts both engines do
/// so identically.
fn emit_corrupted_frame(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id: u64::MAX,
            message_id: pb::MessageIdData {
                ledger_id: u64::MAX,
                entry_id: u64::MAX,
                partition: Some(-1),
                batch_index: Some(-1),
                ack_set: Vec::new(),
                batch_size: Some(0),
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let meta = pb::MessageMetadata {
        producer_name: "diff-broker-corrupt".to_owned(),
        sequence_id: 0,
        publish_time: 1_700_000_000,
        ..Default::default()
    };
    let mut frame = BytesMut::new();
    encode_payload(&mut frame, &cmd, &meta, b"corrupt-me")
        .expect("static corrupted-frame fixture must encode");
    let last = frame.len() - 1;
    frame[last] ^= 0xff;
    out.extend_from_slice(&frame);
}

/// Encode one deliberately **decode-fatal** command frame: a plausible
/// length prefix (`total_size` within `MAX_FRAME_SIZE`, fully present in the
/// buffer) wrapping a command region whose bytes are NOT valid protobuf, so
/// the receiving proto decode loop surfaces a fatal `Frame(Decode(..))` and
/// terminates the connection.
///
/// Wire layout written here:
///
/// ```text
/// [total_size = 5 u32 BE][cmd_size = 1 u32 BE][0xFF]
/// ```
///
/// `0xFF` is protobuf wire-type 7 (reserved / invalid), so
/// `pb::BaseCommand::decode` rejects it. The frame passes
/// `peek_full_frame_len` (a valid, in-bounds `total_size`) but fails inside
/// `decode_one`, exercising the fatal-decode arm of
/// `Connection::handle_bytes_decode_loop`. Used by the terminal-error
/// differential scenario (ADR-0055 §1).
fn emit_decode_fatal_frame(out: &mut BytesMut) {
    use bytes::BufMut;
    // total_size = cmd_size field (4) + 1 command byte = 5.
    out.put_u32(5);
    // cmd_size = 1: exactly one command byte follows.
    out.put_u32(1);
    // 0xFF: protobuf wire-type 7 (reserved) — guarantees a decode error.
    out.put_u8(0xFF);
}

fn emit_pong(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Pong as i32,
        pong: Some(pb::CommandPong {}),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64, _topic: &str) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "diff-broker".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
    // Scripted broker speaks the single-broker contract: every lookup resolves
    // to "use the current connection". `broker_service_url=None` mirrors what
    // standalone Pulsar returns when the lookup target IS the current broker —
    // the proto layer treats that as `LookupOutcome::Connect` with no rebind
    // needed.
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::LookupResponse as i32,
        lookup_topic_response: Some(pb::CommandLookupTopicResponse {
            broker_service_url: None,
            broker_service_url_tls: None,
            response: Some(pb::command_lookup_topic_response::LookupType::Connect as i32),
            request_id,
            authoritative: Some(true),
            error: None,
            message: None,
            proxy_through_service_url: Some(false),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_send_receipt(
    out: &mut BytesMut,
    producer_id: u64,
    sequence_id: u64,
    ledger_id: u64,
    entry_id: u64,
) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::SendReceipt as i32,
        send_receipt: Some(pb::CommandSendReceipt {
            producer_id,
            sequence_id,
            message_id: Some(pb::MessageIdData {
                ledger_id,
                entry_id,
                partition: Some(-1),
                batch_index: Some(-1),
                ack_set: Vec::new(),
                batch_size: Some(0),
                first_chunk_message_id: None,
            }),
            highest_sequence_id: Some(0),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_tc_client_connect_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::TcClientConnectResponse as i32,
        tc_client_connect_response: Some(pb::CommandTcClientConnectResponse {
            request_id,
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_new_txn_response(out: &mut BytesMut, request_id: u64, most: u64, least: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::NewTxnResponse as i32,
        new_txn_response: Some(pb::CommandNewTxnResponse {
            request_id,
            txnid_least_bits: Some(least),
            txnid_most_bits: Some(most),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_add_partition_to_txn_response(out: &mut BytesMut, request_id: u64, most: u64, least: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AddPartitionToTxnResponse as i32,
        add_partition_to_txn_response: Some(pb::CommandAddPartitionToTxnResponse {
            request_id,
            txnid_least_bits: Some(least),
            txnid_most_bits: Some(most),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_add_subscription_to_txn_response(
    out: &mut BytesMut,
    request_id: u64,
    most: u64,
    least: u64,
) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AddSubscriptionToTxnResponse as i32,
        add_subscription_to_txn_response: Some(pb::CommandAddSubscriptionToTxnResponse {
            request_id,
            txnid_least_bits: Some(least),
            txnid_most_bits: Some(most),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_end_txn_response(out: &mut BytesMut, request_id: u64, most: u64, least: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::EndTxnResponse as i32,
        end_txn_response: Some(pb::CommandEndTxnResponse {
            request_id,
            txnid_least_bits: Some(least),
            txnid_most_bits: Some(most),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_ack_response(out: &mut BytesMut, consumer_id: u64, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::AckResponse as i32,
        ack_response: Some(pb::CommandAckResponse {
            consumer_id,
            txnid_least_bits: None,
            txnid_most_bits: None,
            error: None,
            message: None,
            request_id: Some(request_id),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_message(out: &mut BytesMut, consumer_id: u64, stored: &StoredMessage) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id: stored.ledger_id,
                entry_id: stored.entry_id,
                partition: Some(-1),
                batch_index: Some(-1),
                ack_set: Vec::new(),
                batch_size: Some(0),
                first_chunk_message_id: None,
            },
            redelivery_count: Some(0),
            ack_set: Vec::new(),
            consumer_epoch: None,
        }),
        ..Default::default()
    };
    let meta = pb::MessageMetadata {
        producer_name: "diff-broker".to_owned(),
        sequence_id: stored.entry_id,
        publish_time: 1_700_000_000,
        // Round-trip the producer's PIP-4 encryption metadata so the consumer
        // sees `encryption_keys` set and runs its decrypt path.
        encryption_keys: stored.encryption_keys.clone(),
        encryption_algo: stored.encryption_algo.clone(),
        encryption_param: stored.encryption_param.clone(),
        ..Default::default()
    };
    // payload encoding will compute the CRC over [meta_size][meta][payload].
    if encode_payload(out, &cmd, &meta, &stored.payload).is_err() {
        // Encoding shouldn't fail under MAX_FRAME_SIZE; we sanity check
        // and drop on overflow.
        debug_assert!(stored.payload.len() < MAX_FRAME_SIZE);
    }
}
