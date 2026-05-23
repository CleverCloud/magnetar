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

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
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
}

#[derive(Debug, Default, Clone)]
struct ConsumerState {
    /// Outstanding flow permits (incremented by `CommandFlow`).
    permits: u32,
    /// Index of the next message in `ledger` to deliver.
    cursor: usize,
    /// Pending redeliveries (negative-ack'd messages queued ahead of the
    /// normal cursor).
    nacked: Vec<StoredMessage>,
}

#[derive(Debug, Default)]
struct ProducerState {
    /// Next entry id to assign on this producer.
    next_entry_id: u64,
}

/// Shared mutable state for the scripted broker. Each connection has
/// its own [`SessionState`] (this struct); cross-session state would
/// belong on a parent broker handle if the harness ever needs it.
#[derive(Debug, Default)]
struct SessionState {
    /// Per-topic message ledger (append-only).
    ledger: HashMap<String, Vec<StoredMessage>>,
    /// Per producer id (assigned by the client).
    producers: HashMap<u64, (String, ProducerState)>,
    /// Per consumer id (assigned by the client).
    consumers: HashMap<u64, (String, ConsumerState)>,
}

/// Cross-session log of received `BaseCommand` kinds, in arrival order.
/// Mutated by every session task that the broker accepts; the equivalence
/// harness reads it after each engine run to assert ordering invariants
/// (e.g. lookup-before-producer-open).
pub type FrameLog = Arc<Mutex<Vec<i32>>>;

/// Handle to a running scripted broker. Drop to shut down.
pub struct ScriptedBroker {
    /// `host:port` the broker is bound to.
    addr: SocketAddr,
    shutdown: Arc<Notify>,
    accept_task: Option<JoinHandle<()>>,
    /// Shared, append-only log of every `BaseCommand` kind (as the
    /// `pb::base_command::Type` integer tag) seen across every session.
    frame_log: FrameLog,
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
        let accept_task = tokio::spawn(async move {
            loop {
                let accept = listener.accept();
                tokio::select! {
                    res = accept => {
                        match res {
                            Ok((stream, _)) => {
                                let log = frame_log_clone.clone();
                                tokio::spawn(handle_session(stream, log));
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
        })
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

async fn handle_session(mut stream: TcpStream, frame_log: FrameLog) {
    let state = Arc::new(Mutex::new(SessionState::default()));
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
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
            handle_frame(&state, &frame, &mut out_buf);
        }

        // Push any queued messages to consumers with outstanding permits.
        push_pending(&state, &mut out_buf);

        if !out_buf.is_empty() {
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

fn handle_frame(
    state: &Arc<Mutex<SessionState>>,
    frame: &magnetar_proto::Frame,
    out: &mut BytesMut,
) {
    let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
        return;
    };
    match kind {
        pb::base_command::Type::Connect => emit_connected(out),
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
            if let (Some(s), Some(payload)) = (&frame.command.send, &frame.payload) {
                let topic = state
                    .lock()
                    .producers
                    .get(&s.producer_id)
                    .map(|(t, _)| t.clone());
                if let Some(topic) = topic {
                    let (ledger_id, entry_id) = {
                        let mut g = state.lock();
                        let prod = g
                            .producers
                            .get_mut(&s.producer_id)
                            .expect("producer registered above");
                        let entry_id = prod.1.next_entry_id;
                        prod.1.next_entry_id += 1;
                        (1u64, entry_id)
                    };
                    let stored = StoredMessage {
                        ledger_id,
                        entry_id,
                        payload: payload.body.clone(),
                    };
                    state.lock().ledger.entry(topic).or_default().push(stored);
                    emit_send_receipt(out, s.producer_id, s.sequence_id, ledger_id, entry_id);
                }
            }
        }
        pb::base_command::Type::Subscribe => {
            if let Some(s) = &frame.command.subscribe {
                state
                    .lock()
                    .consumers
                    .insert(s.consumer_id, (s.topic.clone(), ConsumerState::default()));
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
                // ACK_RESPONSE is required only when the client included
                // a request id (PIP-72). The state machine always sets
                // one; we mirror that back.
                if let Some(rid) = a.request_id {
                    emit_ack_response(out, a.consumer_id, rid);
                }
            }
        }
        pb::base_command::Type::Seek => {
            if let Some(s) = &frame.command.seek {
                let mut g = state.lock();
                if let Some((topic, c)) = g.consumers.get_mut(&s.consumer_id) {
                    // Seek to the first message at-or-after the given
                    // message id; if no message id was provided, reset
                    // to the beginning.
                    if let Some(mid) = &s.message_id {
                        let topic = topic.clone();
                        let ledger = g.ledger.get(&topic).cloned().unwrap_or_default();
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
}

fn push_pending(state: &Arc<Mutex<SessionState>>, out: &mut BytesMut) {
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
            // Then deliver from cursor.
            let ledger = g.ledger.get(&topic).cloned().unwrap_or_default();
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
            emit_message(out, cid, m.ledger_id, m.entry_id, &m.payload);
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

fn emit_message(
    out: &mut BytesMut,
    consumer_id: u64,
    ledger_id: u64,
    entry_id: u64,
    payload: &Bytes,
) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Message as i32,
        message: Some(pb::CommandMessage {
            consumer_id,
            message_id: pb::MessageIdData {
                ledger_id,
                entry_id,
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
        sequence_id: entry_id,
        publish_time: 1_700_000_000,
        ..Default::default()
    };
    // payload encoding will compute the CRC over [meta_size][meta][payload].
    if encode_payload(out, &cmd, &meta, payload).is_err() {
        // Encoding shouldn't fail under MAX_FRAME_SIZE; we sanity check
        // and drop on overflow.
        debug_assert!(payload.len() < MAX_FRAME_SIZE);
    }
}
