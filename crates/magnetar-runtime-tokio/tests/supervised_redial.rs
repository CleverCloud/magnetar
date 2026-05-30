// SPDX-License-Identifier: Apache-2.0

//! Supervised-redial coverage — tokio engine, real loopback broker.
//!
//! Mirror of the moonpool deterministic-simulation fixture
//! `crates/magnetar-runtime-moonpool/tests/supervised_redial.
//! rs::supervised_loop_redials_under_drop_accept_cycle_sweep_8_seeds` (`DropAcceptCycleBroker` +
//! `SupervisedRedialClientWorkload`). Maintains the tokio ↔ moonpool 1:1 test count required by
//! ADR-0024.
//!
//! Both sides drive the production `supervised_driver_loop`'s reconnect body
//! — the anti-thrash cooldown sleep, the persisted-backoff reset gate, the
//! multi-attempt redial loop, and the `reset()` + `begin_handshake()` +
//! resume tail — through a broker that accepts → CONNECT/CONNECTED → LOOKUP →
//! `PRODUCER_SUCCESS` → drops the socket, then **re-accepts** the supervisor's
//! redial for several cycles (drop → accept → drop → accept).
//!
//! The moonpool engine is the canonical place for the deterministic *timing*
//! assertion (virtual `time.sleep` makes the backoff schedule reproducible);
//! this tokio mirror drives the same production reconnect path over a real
//! `127.0.0.1` socket and asserts the supervisor genuinely redialed
//! (`>= 2` accepted sessions) and torn down more than one socket
//! (`>= 2` drops). Wall-clock timing of the schedule is intentionally NOT
//! asserted here (it is flaky over loopback); the re-accept count is the
//! authoritative proof the reconnect body ran.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use bytes::BytesMut;
use magnetar_proto::{
    AntiThrashThreshold, ConnectionConfig, CreateProducerRequest, FrameError, SupervisorConfig,
    decode_one, encode_command, pb,
};
use magnetar_runtime_tokio::Client;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Spawn a fake broker on `127.0.0.1:0` that runs the canonical
/// create-then-drop session on every inbound connection and re-accepts the
/// next one. Returns the bound `pulsar://...` URL plus two cross-session
/// counters: how many connections it accepted and how many it tore down.
async fn spawn_drop_accept_cycle_broker() -> (String, Arc<AtomicU32>, Arc<AtomicU32>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("broker bind");
    let addr = listener.local_addr().expect("local_addr");
    let sessions_accepted = Arc::new(AtomicU32::new(0));
    let drops_performed = Arc::new(AtomicU32::new(0));

    let accepted = sessions_accepted.clone();
    let drops = drops_performed.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                return;
            };
            accepted.fetch_add(1, Ordering::SeqCst);
            let drops_for_session = drops.clone();
            tokio::spawn(async move {
                let _ = handle_drop_after_create_session(stream, drops_for_session).await;
            });
        }
    });

    (
        format!("pulsar://{addr}"),
        sessions_accepted,
        drops_performed,
    )
}

/// One session: answer CONNECT / PING / LOOKUP, ack the first `CommandProducer`
/// with `ProducerSuccess`, then drop the socket a few ms later. Mirrors the
/// moonpool `handle_drop_after_create_session` helper.
async fn handle_drop_after_create_session(
    mut stream: tokio::net::TcpStream,
    drops_performed: Arc<AtomicU32>,
) -> std::io::Result<()> {
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let mut out_buf = BytesMut::with_capacity(64 * 1024);
    let mut sent_producer_success = false;
    loop {
        loop {
            let mut framed = read_buf.clone().freeze();
            let before = framed.len();
            let frame = match decode_one(&mut framed) {
                Ok(f) => f,
                Err(FrameError::Incomplete { .. }) => break,
                Err(_) => return Ok(()),
            };
            let consumed = before - framed.len();
            let _ = read_buf.split_to(consumed);
            let Ok(kind) = pb::base_command::Type::try_from(frame.command.r#type) else {
                continue;
            };
            match kind {
                pb::base_command::Type::Connect => emit_connected(&mut out_buf),
                pb::base_command::Type::Ping => emit_pong(&mut out_buf),
                pb::base_command::Type::Lookup => {
                    if let Some(l) = &frame.command.lookup_topic {
                        emit_lookup_response(&mut out_buf, l.request_id);
                    }
                }
                pb::base_command::Type::Producer => {
                    if let Some(p) = &frame.command.producer {
                        emit_producer_success(&mut out_buf, p.request_id);
                        sent_producer_success = true;
                    }
                }
                _ => {}
            }
        }

        if !out_buf.is_empty() {
            stream.write_all(&out_buf).await?;
            stream.flush().await?;
            out_buf.clear();
        }

        // Canonical ADR-0028 thrash: once we've acked the producer, sleep
        // briefly (well inside the supervisor's `drop_grace`) and tear the
        // socket down. The supervisor observes the drop and redials against
        // the listener, which re-accepts on the next loop turn.
        if sent_producer_success {
            tokio::time::sleep(Duration::from_millis(5)).await;
            drops_performed.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }

        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }
}

fn emit_connected(out: &mut BytesMut) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::Connected as i32,
        connected: Some(pb::CommandConnected {
            server_version: "magnetar-supervised-redial-test".to_owned(),
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

fn emit_lookup_response(out: &mut BytesMut, request_id: u64) {
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
            // No proxy redirect — keep the data plane on the bootstrap socket.
            proxy_through_service_url: Some(false),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn emit_producer_success(out: &mut BytesMut, request_id: u64) {
    let cmd = pb::BaseCommand {
        r#type: pb::base_command::Type::ProducerSuccess as i32,
        producer_success: Some(pb::CommandProducerSuccess {
            request_id,
            producer_name: "supervised-redial-test".to_owned(),
            last_sequence_id: Some(-1),
            schema_version: None,
            topic_epoch: Some(0),
            producer_ready: Some(true),
        }),
        ..Default::default()
    };
    let _ = encode_command(out, &cmd);
}

fn supervisor_with_anti_thrash() -> SupervisorConfig {
    SupervisorConfig {
        // Tiny schedule so the test runs in real time without slowing the
        // suite, yet long enough that the redial body dominates.
        initial_backoff: Duration::from_millis(5),
        max_backoff: Duration::from_millis(40),
        mandatory_stop: Duration::from_secs(60),
        max_attempts: Some(64),
        anti_thrash_threshold: Some(AntiThrashThreshold {
            successful_attaches: 3,
            window: Duration::from_secs(5),
            drop_within: Duration::from_millis(200),
        }),
        drop_grace: Duration::from_millis(500),
        max_backoff_after_thrash: Duration::from_millis(60),
    }
}

/// Drive the tokio engine's production `supervised_driver_loop` through
/// several drop → accept → drop → accept cycles and assert the supervisor
/// genuinely redialed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn supervised_loop_redials_under_drop_accept_cycle() {
    let (url, sessions_accepted, drops_performed) = spawn_drop_accept_cycle_broker().await;

    let cfg = ConnectionConfig {
        supervisor: Some(supervisor_with_anti_thrash()),
        ..ConnectionConfig::default()
    };

    // Supervised connect — `config.supervisor = Some` wires
    // `spawn_supervised_driver` (the reconnect body) on the tokio engine.
    let client = tokio::time::timeout(Duration::from_secs(5), Client::connect(&url, cfg))
        .await
        .expect("connect did not time out")
        .expect("connect ok");

    // Drive a sequence of opens. Every successful open is followed by the
    // broker dropping the socket, so the supervisor redials and resumes; the
    // next open rides a freshly-reconnected session.
    for _ in 0..12u32 {
        let _ = tokio::time::timeout(
            Duration::from_millis(800),
            client.open_producer(CreateProducerRequest {
                topic: "persistent://public/default/supervised-redial".to_owned(),
                ..Default::default()
            }),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    let accepts = sessions_accepted.load(Ordering::SeqCst);
    let drops = drops_performed.load(Ordering::SeqCst);

    if let Some(d) = client.take_driver() {
        d.abort();
    }
    drop(client);

    assert!(
        accepts >= 2,
        "supervisor must redial at least once (accepts={accepts})"
    );
    assert!(
        drops >= 2,
        "multi-cycle drop pattern must fire (drops={drops})"
    );
}
