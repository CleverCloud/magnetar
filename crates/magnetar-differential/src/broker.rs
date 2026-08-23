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
//! - [`ScriptedBroker::drop_connection_after_first_ack`] — the first session closes right after it
//!   flushes the first ack-response, forcing a supervised client to redial. This one also turns on
//!   **resume mode**: the ledger, per-topic entry-id sequence, and durable per-subscription cursor
//!   move into a cross-session store (`CrossSession`) so the redialled session resumes from the
//!   acked position (ADR-0055 §3 shape). Reset both the knob and the persisted state with
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
    /// `Some((topic, subscription))` when this consumer subscribed with
    /// `SubType::Shared` and is therefore served by the cross-consumer
    /// [`SharedDispatcher`] under that key rather than by its own `cursor`
    /// (issue #414). `None` for every other subscription type, which keeps the
    /// per-consumer walk every existing golden trace depends on byte-identical.
    shared_key: Option<(String, String)>,
}

/// One Pulsar **Shared** subscription's dispatcher (issue #414).
///
/// The scripted broker used to give every consumer its own ledger cursor, so
/// two consumers on one Shared subscription each walked the whole ledger — a
/// shape the real broker never produces, and one in which the issue #414
/// failure mode is not even expressible. This models what a Shared dispatcher
/// actually is: ONE cursor over the ledger, shared by every attached consumer,
/// handed out round-robin to whichever attached consumers hold permits.
///
/// Consumer churn is the whole point of the model. When a consumer detaches
/// (`CommandCloseConsumer`, or the last-clone drop guard), the entries it was
/// holding un-acked are not lost: they go back into [`Self::redelivery`] and
/// the surviving consumers get them ahead of the cursor — which is exactly the
/// broker behaviour the #414 scale-down / recycle window exercises.
///
/// Scoped to one session, keyed by `(topic, subscription)`, following the same
/// stable-identity-key precedent the issue #406 `(topic, producer_name)`
/// registry set.
#[derive(Debug, Default)]
struct SharedDispatcher {
    /// Next index into the topic's ledger to hand out. ONE cursor for the whole
    /// subscription — the difference between a real Shared dispatcher and the
    /// per-consumer full-ledger walk this replaces.
    cursor: usize,
    /// Attached consumer ids, in attach order — the round-robin ring.
    attached: Vec<u64>,
    /// Index into [`Self::attached`] the next dispatch round starts scanning
    /// from, so a consumer that is out of permits does not monopolise the ring.
    next: usize,
    /// Entries handed to a consumer and not yet acked, keyed by consumer id.
    /// A detach returns this consumer's entries to [`Self::redelivery`].
    in_flight: HashMap<u64, Vec<StoredMessage>>,
    /// Entries returned by a detaching consumer, redelivered to the survivors
    /// ahead of the shared cursor (FIFO, oldest first).
    redelivery: std::collections::VecDeque<StoredMessage>,
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
/// ([`ScriptedBroker::drop_connection_after_first_ack`]). When the knob is disarmed —
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
    /// Live `(topic, producer_name) → (producer_id, epoch)` registrations, the
    /// broker resource issue #406 leaks. A `CommandProducer` naming a key
    /// already present is answered with `ProducerBusy` (the broker's
    /// `NamingException`) unless it is a SUCCESSOR of the current owner — same
    /// producer id, strictly higher epoch — which overwrites it, mirroring
    /// `AbstractTopic#tryOverwriteOldProducer` / `Producer#isSuccessorTo`.
    /// A `CommandCloseProducer` releases every key the closed id holds. Only
    /// user-provided, non-empty names register — an unnamed open is assigned a
    /// unique name broker-side and never collides.
    producer_names: HashMap<(String, String), (u64, u64)>,
    /// Producer ids this connection no longer maps. A close for one is acked
    /// and does nothing: Pulsar drops its record of a producer id when it
    /// completes a close-before-creation, and the registration that pending
    /// creation goes on to make is then unreachable by any close.
    unmapped_producer_ids: std::collections::HashSet<u64>,
    /// One withheld `(producer_id, request_id, producer_name)` whose
    /// `CommandProducerSuccess` the session is deliberately sitting on, so a
    /// client open races its `operation_timeout` while the registration
    /// exists broker-side. Released — and finally flushed as a LATE success —
    /// when the client closes that producer id. Armed by
    /// [`ScriptedBroker::withhold_first_producer_success_for_name`].
    withheld_producer_success: Option<(u64, u64, String)>,
    /// One-shot latch for [`Self::withheld_producer_success`]: only the FIRST
    /// matching open on a session is withheld, so the client's retry is
    /// served normally.
    withhold_fired: bool,
    /// Per consumer id (assigned by the client).
    consumers: HashMap<u64, (String, ConsumerState)>,
    /// Live Shared-subscription dispatchers keyed by `(topic, subscription)`
    /// (issue #414). Empty for every trace that opens no `SubType::Shared`
    /// consumer, which is every pre-#414 golden trace.
    shared_dispatchers: HashMap<(String, String), SharedDispatcher>,
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

/// Cross-session, append-only log of every `CommandFlow` the broker received,
/// as `(consumer_id, message_permits)` in arrival order. The per-consumer
/// `permits` balance is spent by dispatch, so it cannot answer "how many
/// permits was this consumer ever granted"; this log can, and it survives the
/// redial that resets the per-session `consumers` map. Issue #426 needs exactly
/// that: one `receiver_queue_size` grant per attach, on the fresh subscribe and
/// again on the post-reconnect `rebuild_consumers` re-attach.
pub type FlowGrantLog = Arc<Mutex<Vec<(u64, u32)>>>;

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
    flow_grants: FlowGrantLog,
    txn_drain_log: TxnDrainLog,
    corrupt_after_connected: Arc<Mutex<bool>>,
    decode_fatal_on_send: Arc<Mutex<bool>>,
    drop_after: Arc<Mutex<bool>>,
    dropped_once: Arc<AtomicBool>,
    /// When `true`, the FIRST `CommandProducer` on a REDIAL session (a session
    /// that opened after [`Self::dropped_once`] latched) is answered with a
    /// TRANSIENT `ServiceNotReady` `CommandError` instead of a
    /// `CommandProducerSuccess`, forcing the engine's lookup-then-retry
    /// leg; the retry's open is acked. A one-shot latch
    /// ([`Self::transient_reject_fired`]) gates it to a single rejection so the
    /// scenario is one deterministic transient → retry → recovery cycle.
    transient_reject_on_redial: Arc<Mutex<bool>>,
    /// One-shot latch for [`Self::transient_reject_on_redial`]: set once the
    /// first redial producer-open has been transiently rejected, so the retry's
    /// open (and every later open) is acked.
    transient_reject_fired: Arc<AtomicBool>,
    /// Producer name whose FIRST open on each session has its
    /// `CommandProducerSuccess` withheld until the client closes the
    /// registered producer id (issue #406). `None` — the default — serves
    /// every open normally.
    withhold_producer_success_for_name: Arc<Mutex<Option<String>>>,
    /// When `true`, the close that releases the withheld success does NOT
    /// release the registration: the broker acks it, stops mapping that
    /// producer id, and lets the creation complete anyway (issue #406's CI
    /// reproduction). Only a successor re-attach reclaims the name.
    withhold_registration_survives_close: Arc<Mutex<bool>>,
    /// When `true`, every `CommandSubscribe` is answered with `CommandSuccess`
    /// AND, immediately behind it in the same write,
    /// `CommandActiveConsumerChange { is_active: true }` — what a real broker
    /// does for an `Exclusive` / `Failover` subscription (issue #427).
    announce_active_consumer: Arc<Mutex<bool>>,
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
    /// Shared, append-only log of every `CommandFlow` as
    /// `(consumer_id, message_permits)`, across every session.
    flow_grants: FlowGrantLog,
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
    /// When `true`, the FIRST session closes its socket immediately after it
    /// writes the `CommandAckResponse` for the first durable ack, forcing a
    /// supervised client to redial; every redialled session then serves
    /// normally (the [`Self::dropped_once`] latch gates the drop to one
    /// occurrence so the scenario is a single, deterministic drop + redial
    /// rather than a redial storm). Armed by [`Self::drop_connection_after_first_ack`]
    /// for the drop + redial differential scenario
    /// (`reconnect_replay_gating_equivalence`).
    ///
    /// The drop point is keyed to a **protocol-semantic marker** (the first
    /// ack-response) rather than a raw broker-write count, so it lands at the
    /// same logical position on every engine leg regardless of timing-driven
    /// frames. A keepalive `PING` used to slip a `PONG` into a frame-count
    /// window and shift the drop ahead of the ack-response on one leg only,
    /// desyncing the durable cursor and diverging the two `EventStream`s
    /// (issue #286).
    ///
    /// Arming this knob also switches every session into **resume mode**: the
    /// ledger + per-topic entry-id sequence + durable per-subscription cursor
    /// live in the cross-session `CrossSession` store so the redialled session
    /// resumes from the acked position instead of starting fresh. `false` (the
    /// default) keeps each session fully isolated and never drops — the shape
    /// every other differential trace relies on.
    drop_after: Arc<Mutex<bool>>,
    /// Latch ensuring [`Self::drop_after`] fires on exactly one session. The
    /// first session to write the first ack-response sets it; later sessions
    /// stay in resume mode but do not drop. Reset by [`Self::clear_cross_session_state`].
    dropped_once: Arc<AtomicBool>,
    /// When `true`, the FIRST `CommandProducer` on a REDIAL session is answered
    /// with a transient `ServiceNotReady` `CommandError`, exercising the
    /// lookup-then-retry leg on both engines. Armed by
    /// [`Self::transient_reject_first_redial_producer_open`] for the
    /// transient-retry equivalence scenario; the
    /// [`Self::transient_reject_fired`] one-shot latch gates it to a single
    /// rejection.
    transient_reject_on_redial: Arc<Mutex<bool>>,
    /// One-shot latch for [`Self::transient_reject_on_redial`]. Reset by
    /// [`Self::clear_cross_session_state`].
    transient_reject_fired: Arc<AtomicBool>,
    /// Producer name whose FIRST open on each session has its
    /// `CommandProducerSuccess` withheld (issue #406). Armed by
    /// [`Self::withhold_first_producer_success_for_name`]; the latch lives in
    /// the per-session state, so both differential legs — each on its own
    /// session — see the same one-shot behaviour with no reset between them.
    withhold_producer_success_for_name: Arc<Mutex<Option<String>>>,
    /// Companion to [`Self::withhold_producer_success_for_name`]: when `true`
    /// the withheld open's registration OUTLIVES its close (issue #406).
    withhold_registration_survives_close: Arc<Mutex<bool>>,
    /// When `true`, every `CommandSubscribe` is answered with `CommandSuccess`
    /// AND `CommandActiveConsumerChange { is_active: true }` right behind it —
    /// a real broker's `Exclusive` / `Failover` announcement. Armed by
    /// [`Self::announce_active_consumer_on_subscribe`] for the issue #427
    /// initial-grant scenario.
    announce_active_consumer: Arc<Mutex<bool>>,
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
        let flow_grants: FlowGrantLog = Arc::new(Mutex::new(Vec::new()));
        let flow_grants_clone = flow_grants.clone();
        let txn_drain_log: TxnDrainLog = Arc::new(Mutex::new(Vec::new()));
        let txn_drain_log_clone = txn_drain_log.clone();
        let corrupt_after_connected = Arc::new(Mutex::new(false));
        let corrupt_after_connected_clone = corrupt_after_connected.clone();
        let decode_fatal_on_send = Arc::new(Mutex::new(false));
        let decode_fatal_on_send_clone = decode_fatal_on_send.clone();
        let drop_after = Arc::new(Mutex::new(false));
        let drop_after_clone = drop_after.clone();
        let dropped_once = Arc::new(AtomicBool::new(false));
        let dropped_once_clone = dropped_once.clone();
        let transient_reject_on_redial = Arc::new(Mutex::new(false));
        let transient_reject_on_redial_clone = transient_reject_on_redial.clone();
        let transient_reject_fired = Arc::new(AtomicBool::new(false));
        let transient_reject_fired_clone = transient_reject_fired.clone();
        let withhold_producer_success_for_name = Arc::new(Mutex::new(None));
        let withhold_producer_success_for_name_clone = withhold_producer_success_for_name.clone();
        let withhold_registration_survives_close = Arc::new(Mutex::new(false));
        let withhold_registration_survives_close_clone =
            withhold_registration_survives_close.clone();
        let announce_active_consumer = Arc::new(Mutex::new(false));
        let announce_active_consumer_clone = announce_active_consumer.clone();
        let cross_session = Arc::new(Mutex::new(CrossSession::default()));
        let cross_session_clone = cross_session.clone();
        let deps = SessionDeps {
            frame_log: frame_log_clone,
            seeked_partitions: seeked_partitions_clone,
            flow_grants: flow_grants_clone,
            txn_drain_log: txn_drain_log_clone,
            corrupt_after_connected: corrupt_after_connected_clone,
            decode_fatal_on_send: decode_fatal_on_send_clone,
            drop_after: drop_after_clone,
            dropped_once: dropped_once_clone,
            transient_reject_on_redial: transient_reject_on_redial_clone,
            transient_reject_fired: transient_reject_fired_clone,
            withhold_producer_success_for_name: withhold_producer_success_for_name_clone,
            withhold_registration_survives_close: withhold_registration_survives_close_clone,
            announce_active_consumer: announce_active_consumer_clone,
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
            flow_grants,
            txn_drain_log,
            corrupt_after_connected,
            decode_fatal_on_send,
            drop_after,
            dropped_once,
            transient_reject_on_redial,
            transient_reject_fired,
            withhold_producer_success_for_name,
            withhold_registration_survives_close,
            announce_active_consumer,
            cross_session,
        })
    }

    /// Arm the active-consumer announcement: every `CommandSubscribe` is answered with
    /// `CommandSuccess` and, immediately behind it in the same write,
    /// `CommandActiveConsumerChange { is_active: true }`.
    ///
    /// That is what a real broker does for an `Exclusive` / `Failover` subscription, and
    /// what makes issue #427 observable: both frames reach the client in one read, so the
    /// sans-io issue #307 promotion re-arm runs inside `handle_bytes` while the engine's
    /// own post-ack `initial_flow` is still parked on the resolving subscribe future. Both
    /// used to grant, and [`Self::flow_grant_log_snapshot`] recorded `2 ×
    /// receiver_queue_size` for one attach.
    ///
    /// Off by default, which is the shape every other differential trace relies on.
    pub fn announce_active_consumer_on_subscribe(&self) {
        *self.announce_active_consumer.lock() = true;
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
    /// immediately after it writes the `CommandAckResponse` for the first
    /// durable ack, forcing a supervised client to redial; every redialled
    /// session then serves to completion. The one-shot latch keeps the scenario
    /// a single, deterministic drop + redial rather than a redial storm.
    ///
    /// The drop point is keyed to a **protocol-semantic marker** — the first
    /// ack-response the broker writes — not a raw count of broker writes. That
    /// makes the drop land at the same logical position on every engine leg
    /// even when timing-driven frames interleave. (A keepalive `PING` fires on
    /// the wall clock, so under load it lands on one leg but not the other; the
    /// `PONG` it provokes used to shift an `n`-frame drop window ahead of the
    /// ack-response on that leg only, leaving its durable cursor un-advanced and
    /// diverging the two `EventStream`s — issue #286.)
    ///
    /// The drop fires *after* the ack-response is flushed, so the client
    /// observes the ack durably (an `Acked` event) and then redials — exactly
    /// the post-ack drop the resume traces assert.
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
    /// **Reset rule.** The disarm + state reset is
    /// [`Self::clear_cross_session_state`], which clears the persisted ledger /
    /// cursors, re-arms the one-shot latch, and re-disarms the knob, mirroring
    /// [`Self::clear_frame_log`] for between-leg isolation.
    pub fn drop_connection_after_first_ack(&self) {
        *self.drop_after.lock() = true;
    }

    /// Arm the transient-retry injection: the FIRST
    /// `CommandProducer` on a REDIAL session (any session that opens after the
    /// [`Self::drop_connection_after_first_ack`] drop has latched) is answered with a
    /// transient `ServiceNotReady` `CommandError` ("Please redo the lookup")
    /// instead of a `CommandProducerSuccess`. This is exactly the post-restart
    /// bundle-not-served window: the proto layer RETAINS the producer state and
    /// emits `ProducerOpenFailedTransient`, and the engine's lookup-then-retry
    /// leg re-issues a lookup + a fresh `CommandProducer` that the broker then
    /// acks. A one-shot latch gates the rejection to a single occurrence so the
    /// scenario is one deterministic transient → retry → recovery cycle on BOTH
    /// engines — and the resulting `EventStream` must stay identical in ORDER
    /// (the differential parity claim).
    ///
    /// Pair with [`Self::drop_connection_after_first_ack`]: the drop opens the redial
    /// window this knob then perturbs. Reset by
    /// [`Self::clear_cross_session_state`].
    pub fn transient_reject_first_redial_producer_open(&self) {
        *self.transient_reject_on_redial.lock() = true;
    }

    /// Arm the withheld-`ProducerSuccess` injection for issue #406: on every
    /// session, the FIRST `CommandProducer` carrying `producer_name` registers
    /// its `(topic, name)` key as usual but its `CommandProducerSuccess` is
    /// **withheld**, so the client's open races its `operation_timeout` while
    /// the broker-side registration exists. Any later open naming the same
    /// `(topic, name)` is rejected with `ProducerBusy` — the broker's
    /// `NamingException` — until a `CommandCloseProducer` for the registered
    /// producer id releases the key, at which point the withheld success is
    /// finally flushed as the LATE ack the client must discard.
    ///
    /// A client that abandons a timed-out open without closing it therefore
    /// can never reopen the name; one that emits the close (ADR-0100) reopens
    /// it on the next try. The one-shot latch lives in the per-session state,
    /// so the tokio and moonpool legs — each on its own session — observe the
    /// same script with no reset in between.
    pub fn withhold_first_producer_success_for_name(&self, producer_name: &str) {
        *self.withhold_producer_success_for_name.lock() = Some(producer_name.to_owned());
    }

    /// Arm the harder issue #406 interleaving, the one that reproduced against
    /// a real Pulsar 4.0.4 broker in CI after the cancel-time close landed.
    ///
    /// Same withheld `CommandProducerSuccess` as
    /// [`Self::withhold_first_producer_success_for_name`], but the close that
    /// releases it is consumed while the creation is still pending: the broker
    /// acks it, stops mapping that producer id, and completes the registration
    /// anyway. No close can address that registration afterwards, so a client
    /// that only re-closes stays wedged — reclaiming the name requires
    /// re-attaching under the abandoned id as a strict successor.
    pub fn withhold_first_producer_success_surviving_close(&self, producer_name: &str) {
        *self.withhold_producer_success_for_name.lock() = Some(producer_name.to_owned());
        *self.withhold_registration_survives_close.lock() = true;
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
    /// [`Self::drop_connection_after_first_ack`]: it re-disarms the knob (sessions go
    /// back to per-session isolation and never drop) and wipes the persisted
    /// resume state in one call, so a missing reset between legs fails loudly
    /// (the second leg would observe the first leg's ledger entries).
    pub fn clear_cross_session_state(&self) {
        *self.drop_after.lock() = false;
        self.dropped_once.store(false, Ordering::SeqCst);
        *self.transient_reject_on_redial.lock() = false;
        self.transient_reject_fired.store(false, Ordering::SeqCst);
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

    /// Snapshot every `CommandFlow` the broker received, as
    /// `(consumer_id, message_permits)` in arrival order across all sessions.
    /// The per-consumer `permits` balance is spent by dispatch and is reset by
    /// the redial that replaces the session state, so this log is what answers
    /// "how many permits was this consumer granted, per attach" (issue #426).
    #[must_use]
    pub fn flow_grant_log_snapshot(&self) -> Vec<(u64, u32)> {
        self.flow_grants.lock().clone()
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
    // Set once the dropping session has staged the first ack-response: the
    // session flushes that frame (so the client observes the ack durably) and
    // then closes, forcing a supervised client to redial.
    let mut drop_after_flush = false;
    // Resume mode: the drop knob is armed, so the ledger + durable cursors
    // live in the cross-session store. Snapshot the knob ONCE at session start
    // so a `clear_cross_session_state` mid-flight does not change this
    // session's behaviour. `false` → never drop, per-session isolation.
    let armed = *drop_after.lock();
    let resume = armed.then_some(cross_session);
    // A session is a REDIAL session when the drop has ALREADY latched before
    // this session opened — snapshot the latch BEFORE the drop CAS below
    // claims it for the dropping session. The transient-retry knob only
    // perturbs redial sessions.
    let is_redial = dropped_once.load(Ordering::SeqCst);
    let transient_reject_armed = is_redial && *deps.transient_reject_on_redial.lock();
    // This session drops only if the knob is armed AND it wins the one-shot
    // drop latch (no earlier session has claimed it) — so the scenario is a
    // single, deterministic drop + redial, and every redialled session then
    // serves to completion (resuming from the durable cursor). The CAS claims
    // the latch atomically: `Ok` means this session is the one that drops.
    let drop_this_session = armed
        && dropped_once
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();

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
                Err(_) => return,
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            frame_log.lock().push(frame.command.r#type);
            let inject = FrameInjections {
                corrupt_after_connected: *corrupt_after_connected.lock(),
                decode_fatal_on_send: *decode_fatal_on_send.lock(),
                transient_reject_armed,
            };
            let keep_going = handle_frame(&state, &frame, &mut out_buf, &deps, inject, resume);
            // Semantic drop marker: the dropping session closes right after it
            // flushes the FIRST ack-response — i.e. a `CommandAck` carrying a
            // `request_id`, which `handle_frame` answers with a
            // `CommandAckResponse`. Keying the drop to this protocol event,
            // rather than a raw count of broker writes, keeps the drop point
            // invariant to timing-driven frames: a keepalive PING used to slip
            // a PONG into an `n`-frame window and shift the drop ahead of the
            // ack-response on one leg only, diverging the streams (issue #286).
            if drop_this_session
                && frame
                    .command
                    .ack
                    .as_ref()
                    .is_some_and(|a| a.request_id.is_some())
            {
                drop_after_flush = true;
            }
            if !keep_going {
                // The decode-fatal frame is already staged in `out_buf`;
                // flush it below, then close the session.
                terminate_after_flush = true;
                break;
            }
            if drop_after_flush {
                // Stop draining further client frames; flush what we have
                // (through the ack-response) and close below.
                break;
            }
        }

        // Push any queued messages to consumers with outstanding permits.
        push_pending(&state, &mut out_buf, resume);

        if !out_buf.is_empty() {
            if stream.write_all(&out_buf).await.is_err() {
                return;
            }
            if stream.flush().await.is_err() {
                return;
            }
            out_buf.clear();
        }

        if drop_after_flush {
            // The ack-response is on the wire; close so the supervised client
            // redials and resumes from the now-durable cursor.
            return;
        }

        if terminate_after_flush {
            return;
        }

        // Read more bytes.
        match stream.read_buf(&mut read_buf).await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

/// Per-frame injection flags snapshotted at the start of each session-loop
/// iteration. Bundled into one struct so the per-frame dispatcher
/// ([`handle_frame`]) stays under the `clippy::too_many_arguments` limit as
/// new injection knobs land.
#[derive(Clone, Copy)]
struct FrameInjections {
    /// Follow the handshake reply with one CRC32C-corrupted frame (ADR-0054).
    corrupt_after_connected: bool,
    /// Answer the first `CommandSend` with a decode-fatal frame (ADR-0055 §1).
    decode_fatal_on_send: bool,
    /// Transiently reject the first redial producer-open.
    transient_reject_armed: bool,
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
    inject: FrameInjections,
    resume: Option<&Arc<Mutex<CrossSession>>>,
) -> bool {
    let FrameInjections {
        corrupt_after_connected,
        decode_fatal_on_send,
        transient_reject_armed,
    } = inject;
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
        // PIP-460 (ADR-0093). The lookup doubles as the layout subscribe, so a
        // single `CommandScalableTopicUpdate` answers it and the session stays
        // open. Two segments at epoch 1, each with a placement entry.
        pb::base_command::Type::ScalableTopicLookup => {
            if let Some(l) = &frame.command.scalable_topic_lookup {
                // A topic whose name ends in `-missing` is the scripted
                // rejection: the broker answers with an error and no layout, so
                // both engines exercise the lookup-refused path.
                if l.topic.ends_with("-missing") {
                    emit_scalable_layout_rejection(out, l.session_id, true);
                } else if l.topic.ends_with("-terse") {
                    // Rejection with no `message`, so the client's fallback
                    // wording is the one the caller sees.
                    emit_scalable_layout_rejection(out, l.session_id, false);
                } else {
                    emit_scalable_layout(out, l.session_id);
                    if l.topic.ends_with("-split") {
                        // A second layout on the same session: segment 1 splits
                        // into 3 + 4, so the client's pushed-update and
                        // drop-on-change paths run end to end.
                        emit_scalable_split_layout(out, l.session_id);
                    }
                }
            }
        }
        pb::base_command::Type::ScalableTopicSubscribe => {
            if let Some(sub) = &frame.command.scalable_topic_subscribe {
                // Consumer id 99 is the scripted rejection: the broker answers
                // with an authorization error and no assignment, so both engines
                // exercise the registration-refused path.
                if sub.consumer_id == 99 {
                    emit_scalable_subscribe_rejection(out, sub.request_id);
                } else {
                    emit_scalable_subscribe_response(out, sub.request_id);
                    // Follow the registration with one rebalance so the
                    // assignment delta path is exercised on both engines.
                    emit_scalable_assignment_update(out, sub.consumer_id);
                }
            }
        }
        pb::base_command::Type::WatchScalableTopics => {
            if let Some(w) = &frame.command.watch_scalable_topics {
                // A namespace ending `-deny` is the scripted refusal, so the
                // watch-closed path runs end to end through the driver.
                if w.namespace.ends_with("-deny") {
                    emit_scalable_topics_rejection(out, w.watch_id);
                } else {
                    emit_scalable_topics_snapshot(out, w.watch_id);
                    emit_scalable_topics_diff(out, w.watch_id);
                }
            }
        }
        pb::base_command::Type::WatchTcAssignments => {
            if let Some(w) = &frame.command.watch_tc_assignments {
                // An even watch id is the scripted refusal. The client allocates
                // watch ids sequentially from 1 across both watch families, so a
                // test opens one watch to reach an even id deterministically.
                if w.watch_id % 2 == 0 {
                    emit_tc_rejection(out, w.watch_id);
                } else {
                    emit_tc_assignments(out, w.watch_id);
                }
            }
        }
        pb::base_command::Type::Lookup => {
            if let Some(l) = &frame.command.lookup_topic {
                emit_lookup_response(out, l.request_id);
            }
        }
        pb::base_command::Type::Producer => {
            if let Some(p) = &frame.command.producer {
                // Transient-retry injection: on a redial
                // session, transiently reject the FIRST producer-open with
                // `ServiceNotReady` ("Please redo the lookup") so the engine's
                // lookup-then-retry leg fires; the `transient_reject_fired`
                // one-shot latch lets the retry's open (and every later open)
                // through to a normal ack. The producer state is registered
                // ONLY on the ack path so the broker's view matches the proto
                // layer (which keeps the handle but does not consider it
                // attached until `ProducerSuccess`).
                let reject = transient_reject_armed
                    && deps
                        .transient_reject_fired
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok();
                if reject {
                    emit_transient_producer_error(out, p.request_id);
                } else {
                    answer_producer_open(state, p, deps, out);
                }
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
                // Issue #414: a `SubType::Shared` consumer joins the ONE
                // dispatcher for its `(topic, subscription)` instead of walking
                // the ledger on its own cursor. Every other subscription type
                // keeps the historical per-consumer walk verbatim.
                let shared_key = (s.sub_type == pb::command_subscribe::SubType::Shared as i32)
                    .then(|| (s.topic.clone(), s.subscription.clone()));
                {
                    let mut g = state.lock();
                    g.consumers.insert(
                        s.consumer_id,
                        (
                            s.topic.clone(),
                            ConsumerState {
                                cursor,
                                subscription: s.subscription.clone(),
                                shared_key: shared_key.clone(),
                                ..ConsumerState::default()
                            },
                        ),
                    );
                    if let Some(key) = shared_key {
                        let dispatcher = g.shared_dispatchers.entry(key).or_default();
                        // A re-subscribe of the SAME consumer id (issue #307's
                        // same-broker re-attach, issue #414's caller-driven
                        // recovery) must not double-register it in the ring.
                        // Its in-flight entries stay with it: the client id is
                        // unchanged, so the broker has not detached anything.
                        if !dispatcher.attached.contains(&s.consumer_id) {
                            dispatcher.attached.push(s.consumer_id);
                        }
                        // A re-attach recreates the dispatcher slot at zero
                        // permits broker-side — mirrors the real broker, and it
                        // is what makes the client's own permit zeroing correct.
                        if let Some((_, c)) = g.consumers.get_mut(&s.consumer_id) {
                            c.permits = 0;
                        }
                    }
                }
                emit_success(out, s.request_id);
                // Issue #427: a real broker follows the subscribe `Success` with
                // `CommandActiveConsumerChange { is_active: true }` for an
                // `Exclusive` / `Failover` subscription, in the same write. Both frames
                // then reach the client in one read, which is what races the sans-io
                // issue #307 promotion re-arm against the engine's own post-ack
                // `initial_flow`.
                if *deps.announce_active_consumer.lock() {
                    emit_active_consumer_change(out, s.consumer_id, true);
                }
            }
        }
        pb::base_command::Type::Flow => {
            if let Some(f) = &frame.command.flow {
                deps.flow_grants
                    .lock()
                    .push((f.consumer_id, f.message_permits));
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
                // Issue #414: a Shared dispatcher only redelivers what is still
                // un-acked, so an ack retires the entry from this consumer's
                // in-flight set. Without this a consumer that acked everything
                // and then detached would still hand its whole history back to
                // the survivors.
                {
                    let mut g = state.lock();
                    let key = g
                        .consumers
                        .get(&a.consumer_id)
                        .and_then(|(_, c)| c.shared_key.clone());
                    if let Some(key) = key
                        && let Some(dispatcher) = g.shared_dispatchers.get_mut(&key)
                        && let Some(in_flight) = dispatcher.in_flight.get_mut(&a.consumer_id)
                    {
                        in_flight.retain(|m| {
                            !a.message_id
                                .iter()
                                .any(|id| id.ledger_id == m.ledger_id && id.entry_id == m.entry_id)
                        });
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
                // Issue #406: releasing the `(topic, producer_name)` keys the
                // closed id holds is the whole point of the close — that is
                // what frees the name for the next open. A withheld success
                // for the same id is flushed here as the LATE ack the client
                // must discard without resurrecting anything.
                let survives_close = *deps.withhold_registration_survives_close.lock();
                let released = {
                    let mut g = state.lock();
                    if g.unmapped_producer_ids.contains(&c.producer_id) {
                        None
                    } else {
                        g.producers.remove(&c.producer_id);
                        let creation_pending = g
                            .withheld_producer_success
                            .as_ref()
                            .is_some_and(|(producer_id, _, _)| *producer_id == c.producer_id);
                        if creation_pending && survives_close {
                            g.unmapped_producer_ids.insert(c.producer_id);
                        } else {
                            g.producer_names.retain(|_, (id, _)| *id != c.producer_id);
                        }
                        match g.withheld_producer_success.as_ref() {
                            Some((producer_id, _, _)) if *producer_id == c.producer_id => {
                                g.withheld_producer_success.take()
                            }
                            _ => None,
                        }
                    }
                };
                if let Some((_, request_id, producer_name)) = released {
                    emit_named_producer_success(out, request_id, &producer_name);
                }
                emit_success(out, c.request_id);
            }
        }
        pb::base_command::Type::CloseConsumer => {
            if let Some(c) = &frame.command.close_consumer {
                let mut g = state.lock();
                let removed = g.consumers.remove(&c.consumer_id);
                // Issue #414: detaching a Shared consumer returns everything it
                // was holding un-acked to the subscription's redelivery pool, so
                // a survivor picks it up. This is the broker half of the
                // scale-down / mid-drain-recycle window the issue reports.
                if let Some((_, state)) = removed
                    && let Some(key) = state.shared_key
                    && let Some(dispatcher) = g.shared_dispatchers.get_mut(&key)
                {
                    dispatcher.attached.retain(|id| *id != c.consumer_id);
                    // `unwrap_or_default` rather than `if let Some`: a consumer that never
                    // received anything simply has nothing to hand back, and that is not a
                    // separate case worth branching on — a dispatcher that redelivered on
                    // an empty detach would duplicate the backlog on every scale-down.
                    for m in dispatcher
                        .in_flight
                        .remove(&c.consumer_id)
                        .unwrap_or_default()
                    {
                        dispatcher.redelivery.push_back(m);
                    }
                    // Keep the round-robin start inside the shrunken ring.
                    if dispatcher.attached.is_empty() {
                        dispatcher.next = 0;
                    } else {
                        dispatcher.next %= dispatcher.attached.len();
                    }
                }
                drop(g);
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

/// The ledger this session serves for `topic`.
///
/// In **resume mode** (the drop + redial knob armed) a topic's messages live in
/// the cross-session store so they survive the redial; in **isolated mode** —
/// the default for every other trace — they live on this session alone. Both
/// [`push_pending`]'s per-consumer walk and [`push_pending_shared`]'s Shared
/// dispatcher make the identical choice, so it is made once here.
fn topic_ledger(
    state: &SessionState,
    resume: Option<&Arc<Mutex<CrossSession>>>,
    topic: &str,
) -> Vec<StoredMessage> {
    match resume {
        Some(cross) => cross.lock().ledger.get(topic).cloned().unwrap_or_default(),
        None => state.ledger.get(topic).cloned().unwrap_or_default(),
    }
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
        // Issue #414: Shared subscriptions are served by their own dispatcher
        // (one cursor, round-robin over attached consumers) before the
        // per-consumer walk below, which then skips them.
        push_pending_shared(&mut g, resume, &mut to_push);
        // Avoid `clone_into_iter`-style traps: collect ids first.
        let ids: Vec<u64> = g.consumers.keys().copied().collect();
        for cid in ids {
            let Some((topic, c)) = g.consumers.get_mut(&cid) else {
                continue;
            };
            if c.permits == 0 || c.shared_key.is_some() {
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
            // Then deliver from the cursor, out of whichever ledger this
            // session is serving (see `topic_ledger`).
            let ledger = topic_ledger(&g, resume, &topic);
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

/// Dispatch one round for every live Shared subscription (issue #414).
///
/// One cursor per `(topic, subscription)`, handed out round-robin to whichever
/// attached consumers still hold permits, with a detaching consumer's un-acked
/// entries redelivered ahead of the cursor. Loops until either no attached
/// consumer has a permit left or the ledger and the redelivery pool are both
/// exhausted, so one call drains as much as the granted permits allow — exactly
/// what the per-consumer walk below it does for the other subscription types.
///
/// Appends into the caller's `to_push` list so the encode loop stays outside the
/// state lock, matching [`push_pending`]'s existing shape.
fn push_pending_shared(
    g: &mut SessionState,
    resume: Option<&Arc<Mutex<CrossSession>>>,
    to_push: &mut Vec<(u64, Vec<StoredMessage>)>,
) {
    let keys: Vec<(String, String)> = g.shared_dispatchers.keys().cloned().collect();
    for key in keys {
        let ledger = topic_ledger(g, resume, &key.0);
        // Pick the next attached consumer holding a permit, starting from where
        // the previous round stopped so no consumer monopolises the ring.
        while let Some(dispatcher) = g.shared_dispatchers.get(&key) {
            if dispatcher.attached.is_empty() {
                break;
            }
            let ring = dispatcher.attached.clone();
            let start = dispatcher.next % ring.len();
            let chosen = (0..ring.len()).find_map(|offset| {
                let slot = (start + offset) % ring.len();
                let cid = ring[slot];
                g.consumers
                    .get(&cid)
                    .is_some_and(|(_, c)| c.permits > 0)
                    .then_some(((slot + 1) % ring.len(), cid))
            });
            let Some((next_start, cid)) = chosen else {
                break;
            };
            // Redeliveries (returned by a detached consumer) go out ahead of the
            // shared cursor; a real broker replays the un-acked backlog first.
            let entry = {
                let dispatcher = g
                    .shared_dispatchers
                    .get_mut(&key)
                    .expect("checked present above");
                dispatcher.next = next_start;
                if let Some(m) = dispatcher.redelivery.pop_front() {
                    Some(m)
                } else if dispatcher.cursor < ledger.len() {
                    let m = ledger[dispatcher.cursor].clone();
                    dispatcher.cursor += 1;
                    Some(m)
                } else {
                    None
                }
            };
            let Some(entry) = entry else {
                break;
            };
            if let Some((_, c)) = g.consumers.get_mut(&cid) {
                c.permits = c.permits.saturating_sub(1);
            }
            g.shared_dispatchers
                .get_mut(&key)
                .expect("checked present above")
                .in_flight
                .entry(cid)
                .or_default()
                .push(entry.clone());
            match to_push.iter_mut().find(|(id, _)| *id == cid) {
                Some((_, batch)) => batch.push(entry),
                None => to_push.push((cid, vec![entry])),
            }
        }
    }
}

/// Build a `SegmentInfoProto` with the given hash range and parent edges.
fn scalable_segment(id: u64, start: u32, end: u32, parents: &[u64]) -> pb::SegmentInfoProto {
    pb::SegmentInfoProto {
        segment_id: id,
        hash_start: start,
        hash_end: end,
        state: pb::SegmentState::Active as i32,
        parent_ids: parents.to_vec(),
        child_ids: Vec::new(),
        created_at_epoch: 0,
        sealed_at_epoch: None,
        created_at_ms: 0,
        sealed_at_ms: None,
        legacy_topic_name: None,
    }
}

/// Emit a rejected scalable-topic session: an error and no layout.
fn emit_scalable_layout_rejection(out: &mut BytesMut, session_id: u64, with_message: bool) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: None,
            error: Some(pb::ServerError::TopicNotFound as i32),
            message: with_message.then(|| "scripted: topic does not exist".to_owned()),
            resolved_topic_name: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Emit the initial two-segment layout for a scalable-topic session.
fn emit_scalable_layout(out: &mut BytesMut, session_id: u64) {
    let segments = vec![
        scalable_segment(1, 0, 32_768, &[]),
        scalable_segment(2, 32_768, 65_536, &[]),
    ];
    let segment_brokers = segments
        .iter()
        .map(|s| pb::SegmentBrokerAddress {
            segment_id: s.segment_id,
            broker_url: format!("pulsar://seg{}:6650", s.segment_id),
            broker_url_tls: None,
        })
        .collect();
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: Some(pb::ScalableTopicDag {
                epoch: 1,
                segments,
                segment_brokers,
                controller_broker_url: Some("pulsar://controller:6650".to_owned()),
                controller_broker_url_tls: None,
            }),
            error: None,
            message: None,
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Emit a second layout in which segment 1 splits into 3 + 4.
fn emit_scalable_split_layout(out: &mut BytesMut, session_id: u64) {
    let segments = vec![
        scalable_segment(2, 32_768, 65_536, &[]),
        scalable_segment(3, 0, 16_384, &[1]),
        scalable_segment(4, 16_384, 32_768, &[1]),
    ];
    let segment_brokers = segments
        .iter()
        .map(|s| pb::SegmentBrokerAddress {
            segment_id: s.segment_id,
            broker_url: format!("pulsar://seg{}:6650", s.segment_id),
            broker_url_tls: None,
        })
        .collect();
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicUpdate as i32,
        scalable_topic_update: Some(pb::CommandScalableTopicUpdate {
            session_id,
            dag: Some(pb::ScalableTopicDag {
                epoch: 2,
                segments,
                segment_brokers,
                controller_broker_url: Some("pulsar://controller:6650".to_owned()),
                controller_broker_url_tls: None,
            }),
            error: None,
            message: None,
            resolved_topic_name: Some("topic://public/default/scaled".to_owned()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn scalable_assignment(epoch: u64, segs: &[u64]) -> pb::ScalableConsumerAssignment {
    pb::ScalableConsumerAssignment {
        layout_epoch: epoch,
        segments: segs
            .iter()
            .map(|&id| pb::ScalableAssignedSegment {
                segment_id: id,
                hash_start: 0,
                hash_end: 32_768,
                segment_topic: format!("segment://public/default/scaled/{id}"),
            })
            .collect(),
    }
}

/// Emit the response that resolves a consumer registration.
fn emit_scalable_subscribe_response(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicSubscribeResponse as i32,
        scalable_topic_subscribe_response: Some(pb::CommandScalableTopicSubscribeResponse {
            request_id,
            error: None,
            message: None,
            assignment: Some(scalable_assignment(1, &[1])),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Emit a rejected consumer registration: an error and no assignment.
fn emit_scalable_subscribe_rejection(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicSubscribeResponse as i32,
        scalable_topic_subscribe_response: Some(pb::CommandScalableTopicSubscribeResponse {
            request_id,
            error: Some(pb::ServerError::AuthorizationError as i32),
            message: Some("not permitted on this subscription".to_owned()),
            assignment: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Emit one rebalance: segment 1 is replaced by segment 2 at epoch 2.
fn emit_scalable_assignment_update(out: &mut BytesMut, consumer_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicAssignmentUpdate as i32,
        scalable_topic_assignment_update: Some(pb::CommandScalableTopicAssignmentUpdate {
            consumer_id,
            assignment: scalable_assignment(2, &[2]),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_scalable_topics_update(
    out: &mut BytesMut,
    watch_id: u64,
    event: pb::command_watch_scalable_topics_update::Event,
) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::WatchScalableTopicsUpdate as i32,
        watch_scalable_topics_update: Some(pb::CommandWatchScalableTopicsUpdate {
            watch_id,
            error: None,
            message: None,
            event: Some(event),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Emit a refused namespace watch: an error and no event.
fn emit_scalable_topics_rejection(out: &mut BytesMut, watch_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::WatchScalableTopicsUpdate as i32,
        watch_scalable_topics_update: Some(pb::CommandWatchScalableTopicsUpdate {
            watch_id,
            error: Some(pb::ServerError::AuthorizationError as i32),
            message: Some("scripted: namespace watch refused".to_owned()),
            event: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Emit a refused coordinator-discovery watch: an error and no snapshot.
fn emit_tc_rejection(out: &mut BytesMut, watch_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTcAssignmentsUpdate as i32,
        watch_tc_assignments_update: Some(pb::CommandWatchTcAssignmentsUpdate {
            watch_id,
            snapshot: None,
            error: Some(pb::ServerError::ServiceNotReady as i32),
            message: Some("scripted: coordinators unavailable".to_owned()),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Emit the initial namespace-watch snapshot.
fn emit_scalable_topics_snapshot(out: &mut BytesMut, watch_id: u64) {
    emit_scalable_topics_update(
        out,
        watch_id,
        pb::command_watch_scalable_topics_update::Event::Snapshot(pb::ScalableTopicsSnapshot {
            topics: vec!["topic://public/default/a".to_owned()],
        }),
    );
}

/// Emit an incremental namespace-watch diff.
fn emit_scalable_topics_diff(out: &mut BytesMut, watch_id: u64) {
    emit_scalable_topics_update(
        out,
        watch_id,
        pb::command_watch_scalable_topics_update::Event::Diff(pb::ScalableTopicsDiff {
            added: vec!["topic://public/default/c".to_owned()],
            removed: vec!["topic://public/default/a".to_owned()],
        }),
    );
}

/// Emit a two-coordinator transaction-coordinator assignment snapshot.
fn emit_tc_assignments(out: &mut BytesMut, watch_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::WatchTcAssignmentsUpdate as i32,
        watch_tc_assignments_update: Some(pb::CommandWatchTcAssignmentsUpdate {
            watch_id,
            snapshot: Some(pb::TcAssignmentsSnapshot {
                parallelism: 2,
                assignments: (0..2)
                    .map(|tc_id| pb::TcAssignment {
                        tc_id,
                        broker_service_url: Some(format!("pulsar://tc{tc_id}:6650")),
                        broker_service_url_tls: None,
                    })
                    .collect(),
            }),
            error: None,
            message: None,
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-differential-broker".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            // PIP-460 / PIP-473 (ADR-0093): the client refuses to emit any
            // scalable-topic command unless the broker advertises the
            // capability, so the scripted broker must claim both for the
            // scalable transcripts to reach the wire at all.
            feature_flags: Some(pb::FeatureFlags {
                supports_scalable_topics: Some(true),
                supports_tc_metadata_discovery: Some(true),
                ..pb::FeatureFlags::default()
            }),
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
    emit_named_producer_success(out, request_id, "diff-broker");
}

fn emit_named_producer_success(out: &mut BytesMut, request_id: u64, producer_name: &str) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: producer_name.to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Register a producer open, enforcing `(topic, producer_name)` exclusivity
/// the way a real broker's `Topic#addProducer` does, and honouring the
/// withheld-success injection (issue #406).
///
/// An unnamed open (`producer_name` absent or empty) is never registered by
/// name: the broker assigns a unique one, so it cannot collide. A named open
/// whose key is already held is rejected with `ProducerBusy` and registers
/// nothing.
fn answer_producer_open(
    state: &Arc<Mutex<SessionState>>,
    p: &pb::CommandProducer,
    deps: &SessionDeps,
    out: &mut BytesMut,
) {
    let name = p
        .producer_name
        .clone()
        .filter(|candidate| !candidate.is_empty());
    let epoch = p.epoch.unwrap_or(0);
    let withhold_name = deps.withhold_producer_success_for_name.lock().clone();
    let mut g = state.lock();
    if let Some(name) = name.as_ref() {
        let key = (p.topic.clone(), name.clone());
        if let Some(&(owner_id, owner_epoch)) = g.producer_names.get(&key) {
            if owner_id != p.producer_id || epoch <= owner_epoch {
                drop(g);
                emit_producer_busy(out, p.request_id, name);
                return;
            }
            // Successor re-attach: same producer id, strictly higher epoch.
            // The owner is overwritten in place and the connection maps the id
            // again.
            g.producer_names.insert(key, (p.producer_id, epoch));
            g.unmapped_producer_ids.remove(&p.producer_id);
            g.producers
                .insert(p.producer_id, (p.topic.clone(), ProducerState::default()));
            drop(g);
            emit_named_producer_success(out, p.request_id, name);
            return;
        }
        g.producer_names.insert(key, (p.producer_id, epoch));
    }
    g.producers
        .insert(p.producer_id, (p.topic.clone(), ProducerState::default()));
    if withhold_name.is_some() && withhold_name == name && !g.withhold_fired {
        g.withhold_fired = true;
        let effective = name.unwrap_or_default();
        g.withheld_producer_success = Some((p.producer_id, p.request_id, effective));
        return;
    }
    drop(g);
    match name {
        Some(name) => emit_named_producer_success(out, p.request_id, &name),
        None => emit_producer_success(out, p.request_id, &p.topic),
    }
}

/// Encode the broker's `NamingException` — `ProducerBusy` correlated with a
/// producer-open `request_id`. ADR-0080 classifies it as retryable for
/// `ProducerOpen`, so an engine that never frees the name burns its retry
/// budget against a registration that outlives every attempt (issue #406).
fn emit_producer_busy(out: &mut BytesMut, request_id: u64, producer_name: &str) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: pb::ServerError::ProducerBusy as i32,
            message: format!("Producer with name '{producer_name}' is already connected to topic"),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

/// Encode a TRANSIENT `CommandError` (`ServiceNotReady` "Please redo the
/// lookup") correlated with a producer-open `request_id`. The proto layer
/// classifies `ServiceNotReady` as transient, RETAINS the producer state, and
/// emits `ProducerOpenFailedTransient` so the engine's lookup-then-retry
/// leg fires instead of failing the open. Used by
/// [`ScriptedBroker::transient_reject_first_redial_producer_open`].
fn emit_transient_producer_error(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Error as i32,
        error: Some(pb::CommandError {
            request_id,
            error: pb::ServerError::ServiceNotReady as i32,
            message: "Namespace bundle not served by this instance. Please redo the lookup."
                .to_owned(),
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

/// Emit `CommandActiveConsumerChange` for `consumer_id` — the broker's
/// active/standby election report for an `Exclusive` / `Failover` subscription
/// (issue #427).
fn emit_active_consumer_change(out: &mut BytesMut, consumer_id: u64, is_active: bool) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ActiveConsumerChange as i32,
        active_consumer_change: Some(pb::CommandActiveConsumerChange {
            consumer_id,
            is_active: Some(is_active),
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
