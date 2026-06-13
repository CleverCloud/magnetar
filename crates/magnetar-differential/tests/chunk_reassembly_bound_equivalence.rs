// SPDX-License-Identifier: Apache-2.0

//! ADR-0024 layer (d): tokio ↔ moonpool `EventStream` parity for the bounded
//! chunk-reassembly fix. A consumer with `max_pending_chunked_message = 2`
//! evicts the oldest incomplete buffer when a third distinct chunked message
//! arrives, and the expiry sweep drops a stale buffer past
//! `expire_time_of_incomplete_chunked_message`. The two engines must agree on
//! the resulting `Message` stream bit-for-bit.
//!
//! Why this drives both engines' `ConnectionShared` directly: the bound is a
//! virtual-clock + buffer interaction (distinct-UUID first chunks +
//! `handle_timeout` past the expiry). Both engines wrap the SAME
//! `magnetar_proto::Connection`, so feeding an identical synthetic frame
//! sequence and comparing the surfaced reassembled messages is exactly the
//! `EventStream`-parity claim the harness exists to make — with a synthetic
//! [`Instant`] both engines can pin.

use std::time::{Duration, Instant};

use bytes::BytesMut;
use magnetar_proto::{
    Connection, ConnectionConfig, ConnectionEvent, ConsumerHandle, SubscribeRequest,
    encode_command, encode_payload, pb,
};

const EXPIRE: Duration = Duration::from_secs(30);

/// One surfaced reassembled message: the `(ledger, entry)` of its broker id and
/// its payload. This is the user-visible `EventStream` slice the two engines
/// must agree on for the bounded-chunk scenario.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Surfaced {
    ledger: u64,
    entry: u64,
    payload: Vec<u8>,
}

fn handshake_response_bytes() -> BytesMut {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-test".to_owned(),
            protocol_version: Some(21),
            max_message_size: Some(5 * 1024 * 1024),
            feature_flags: Some(pb::FeatureFlags::default()),
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &cmd).expect("encode CommandConnected");
    buf
}

fn message_cmd(consumer_id: u64, entry_id: u64) -> pb::CommandMessage {
    pb::CommandMessage {
        consumer_id,
        message_id: pb::MessageIdData {
            ledger_id: 1,
            entry_id,
            ..Default::default()
        },
        redelivery_count: Some(0),
        ack_set: vec![],
        consumer_epoch: None,
    }
}

fn chunk_meta(uuid: &str, total: i32, chunk_id: i32) -> pb::MessageMetadata {
    pb::MessageMetadata {
        producer_name: "p".to_owned(),
        sequence_id: 1,
        publish_time: 1_700_000_000,
        uuid: Some(uuid.to_owned()),
        num_chunks_from_msg: Some(total),
        chunk_id: Some(chunk_id),
        total_chunk_msg_size: Some(0),
        ..Default::default()
    }
}

/// Drive the shared bounded-chunk scenario over one engine's locked
/// `Connection` and collect the reassembled `Message`s it surfaces.
/// Engine-agnostic: the caller hands in a `&mut Connection` from either engine's
/// `ConnectionShared`, so the two runs are byte-for-byte comparable.
fn lock_and_run(conn: &mut Connection, t0: Instant) -> Vec<Surfaced> {
    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(t0, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();

    // Cap 2 + a 30s expiry window.
    let req = SubscribeRequest {
        topic: "persistent://public/default/chunk-bound-equiv".to_owned(),
        subscription: "sub-chunk-bound-equiv".to_owned(),
        sub_type: pb::command_subscribe::SubType::Exclusive,
        max_pending_chunked_message: 2,
        expire_time_of_incomplete_chunked_message: Some(EXPIRE),
        ..Default::default()
    };
    let subscribe_request_id = conn.peek_next_request_id_for_test();
    let handle: ConsumerHandle = conn.subscribe(req);

    let success = pb::BaseCommand {
        r#type: pb::base_command::Type::Success as i32,
        success: Some(pb::CommandSuccess {
            request_id: subscribe_request_id,
            schema: None,
        }),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_command(&mut buf, &success).expect("encode CommandSuccess");
    conn.handle_bytes(t0, &buf).expect("Success");
    while conn.poll_event().is_some() {}
    let _ = conn.poll_transmit();

    let mut surfaced = Vec::new();
    let deliver = |conn: &mut Connection,
                   now: Instant,
                   uuid: &str,
                   entry: u64,
                   total: i32,
                   chunk_id: i32,
                   body: &'static [u8],
                   out: &mut Vec<Surfaced>| {
        let cmd = pb::BaseCommand {
            r#type: pb::base_command::Type::Message as i32,
            message: Some(message_cmd(handle.0, entry)),
            ..Default::default()
        };
        let meta = chunk_meta(uuid, total, chunk_id);
        let mut frame = BytesMut::new();
        encode_payload(&mut frame, &cmd, &meta, body).expect("encode chunk frame");
        conn.handle_bytes(now, &frame).expect("deliver chunk");
        while let Some(ev) = conn.poll_event() {
            if let ConnectionEvent::Message { message, .. } = ev {
                out.push(Surfaced {
                    ledger: message.message_id.ledger_id,
                    entry: message.message_id.entry_id,
                    payload: message.payload.to_vec(),
                });
            }
        }
        let _ = conn.poll_transmit();
    };

    // Three distinct 2-chunk messages; the third (u-2) evicts the oldest (u-0).
    deliver(conn, t0, "u-0", 0, 2, 0, b"a0", &mut surfaced);
    deliver(conn, t0, "u-1", 1, 2, 0, b"b0", &mut surfaced);
    deliver(conn, t0, "u-2", 2, 2, 0, b"c0", &mut surfaced);

    // Completing u-0 (evicted) surfaces nothing; completing u-2 (retained) does.
    deliver(conn, t0, "u-0", 10, 2, 1, b"a1", &mut surfaced);
    deliver(conn, t0, "u-2", 12, 2, 1, b"c1", &mut surfaced);

    // u-1 is still incomplete; sweep past the expiry window removes it.
    // (poll_timeout must surface its deadline so the driver would have woken.)
    let deadline = conn
        .poll_timeout()
        .expect("an incomplete chunk buffer must surface a timeout deadline");
    assert!(
        deadline <= t0 + EXPIRE,
        "the chunk-expiry deadline must be surfaced through poll_timeout"
    );
    conn.handle_timeout(t0 + EXPIRE + Duration::from_secs(1));
    let _ = conn.poll_transmit();
    while let Some(ev) = conn.poll_event() {
        if let ConnectionEvent::Message { message, .. } = ev {
            surfaced.push(Surfaced {
                ledger: message.message_id.ledger_id,
                entry: message.message_id.entry_id,
                payload: message.payload.to_vec(),
            });
        }
    }

    surfaced
}

#[test]
fn bounded_chunk_reassembly_event_streams_agree() {
    let t0 = Instant::now();

    let tokio_stream = {
        let shared = magnetar_runtime_tokio::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    let moonpool_stream = {
        let shared = magnetar_runtime_moonpool::ConnectionShared::new(ConnectionConfig::default());
        let mut conn = shared.inner.lock();
        lock_and_run(&mut conn, t0)
    };

    assert_eq!(
        tokio_stream, moonpool_stream,
        "tokio and moonpool engines diverged on the bounded-chunk-reassembly message stream"
    );
    // Exactly one reassembled message (u-2): u-0 was evicted, u-1 expired.
    assert_eq!(
        tokio_stream.len(),
        1,
        "exactly one message must reassemble on both engines, got {tokio_stream:?}"
    );
    assert_eq!(tokio_stream[0].payload, b"c0c1");
}
