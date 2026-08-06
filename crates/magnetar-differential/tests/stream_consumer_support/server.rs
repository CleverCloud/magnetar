// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::{Bytes, BytesMut};
use magnetar_fakes::m1::{
    Endpoint, EndpointAuthorities, M1FakeCluster, M1FakeConfig, M1FakeError, TransportSecurity,
};
use magnetar_proto::{FrameError, decode_one, encode_command};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

struct Shared {
    fake: Mutex<M1FakeCluster>,
    output_ready: Notify,
    changed: Notify,
    failure: Mutex<Option<String>>,
    sessions: Mutex<Vec<JoinHandle<()>>>,
    held_message_endpoints: Mutex<BTreeSet<Endpoint>>,
    held_output_commands: Mutex<BTreeSet<(Endpoint, i32)>>,
    held_messages: Mutex<BTreeMap<magnetar_fakes::m1::ConnectionId, VecDeque<Bytes>>>,
    retain_sealed_placements: AtomicBool,
}

/// Real-socket bridge around the sans-I/O M1 cluster.
pub(crate) struct M1SocketCluster {
    shared: Arc<Shared>,
    controller_url: String,
    accept_tasks: Vec<JoinHandle<()>>,
}

impl M1SocketCluster {
    /// Bind one controller and two segment listeners before constructing the
    /// fake, so every advertised authority names the real listener selected by
    /// the runtime pools.
    pub(crate) async fn bind() -> Self {
        let controller = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind M1 controller");
        let segment_one = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind M1 segment 1");
        let segment_two = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind M1 segment 2");

        let controller_address = controller.local_addr().expect("controller address");
        let segment_one_address = segment_one.local_addr().expect("segment 1 address");
        let segment_two_address = segment_two.local_addr().expect("segment 2 address");
        let authorities = |address: std::net::SocketAddr| {
            EndpointAuthorities::new(
                format!("pulsar://{address}"),
                format!("pulsar+ssl://{address}"),
            )
        };
        let config = M1FakeConfig::new("topic://public/default/scaled")
            .expect("valid M1 topic")
            .with_endpoint_authorities(Endpoint::Controller, authorities(controller_address))
            .with_endpoint_authorities(Endpoint::Segment(1), authorities(segment_one_address))
            .with_endpoint_authorities(Endpoint::Segment(2), authorities(segment_two_address));
        let fake = M1FakeCluster::from_config(config).expect("valid socket-backed M1 config");
        let shared = Arc::new(Shared {
            fake: Mutex::new(fake),
            output_ready: Notify::new(),
            changed: Notify::new(),
            failure: Mutex::new(None),
            sessions: Mutex::new(Vec::new()),
            held_message_endpoints: Mutex::new(BTreeSet::new()),
            held_output_commands: Mutex::new(BTreeSet::new()),
            held_messages: Mutex::new(BTreeMap::new()),
            retain_sealed_placements: AtomicBool::new(false),
        });
        let accept_tasks = vec![
            spawn_accept_loop(shared.clone(), Endpoint::Controller, controller),
            spawn_accept_loop(shared.clone(), Endpoint::Segment(1), segment_one),
            spawn_accept_loop(shared.clone(), Endpoint::Segment(2), segment_two),
        ];
        Self {
            shared,
            controller_url: format!("pulsar://{controller_address}"),
            accept_tasks,
        }
    }

    pub(crate) fn controller_url(&self) -> &str {
        &self.controller_url
    }

    /// Retain each sealed segment's last authority in generated layout frames.
    /// Exact M1 omits it; ordering tests use this routeable projection to drive
    /// descendant release independently from that known runtime limitation.
    #[allow(dead_code)]
    pub(crate) fn retain_sealed_placements(&self) {
        self.shared
            .retain_sealed_placements
            .store(true, Ordering::Release);
    }

    /// Apply one broker-side control operation and wake every socket that may
    /// now have generated output queued for it.
    pub(crate) fn update<T>(
        &self,
        update: impl FnOnce(&mut M1FakeCluster) -> Result<T, M1FakeError>,
    ) -> Result<T, M1FakeError> {
        let result = update(&mut self.shared.fake.lock());
        self.shared.output_ready.notify_waiters();
        self.shared.changed.notify_waiters();
        result
    }

    pub(crate) fn inspect<T>(&self, inspect: impl FnOnce(&M1FakeCluster) -> T) -> T {
        inspect(&self.shared.fake.lock())
    }

    /// Wait for a semantic fake predicate without a sleep-poll loop. The
    /// notification is enrolled before inspection, matching the runtime's own
    /// lost-wakeup discipline.
    pub(crate) async fn wait_for(
        &self,
        description: &str,
        predicate: impl Fn(&M1FakeCluster) -> bool,
    ) {
        let wait = async {
            loop {
                self.assert_healthy();
                let changed = self.shared.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if predicate(&self.shared.fake.lock()) {
                    return;
                }
                changed.await;
            }
        };
        if tokio::time::timeout(magnetar_differential::HANG_GUARD, wait)
            .await
            .is_err()
        {
            let (resources, routes) =
                self.inspect(|fake| (fake.resource_counts(), fake.routes().to_vec()));
            panic!(
                "timed out waiting for {description}: resources={resources:?}, routes={routes:?}"
            );
        }
    }

    pub(crate) fn assert_healthy(&self) {
        if let Some(failure) = self.shared.failure.lock().clone() {
            let (resources, routes) =
                self.inspect(|fake| (fake.resource_counts(), fake.routes().to_vec()));
            panic!(
                "M1 socket bridge failed: {failure}; resources={resources:?}, routes={routes:?}"
            );
        }
    }

    /// Hold generated `CommandMessage` frames for one endpoint while allowing
    /// control and request/response frames on the same socket to proceed.
    #[allow(dead_code)]
    pub(crate) fn hold_messages(&self, endpoint: Endpoint) {
        self.shared.held_message_endpoints.lock().insert(endpoint);
    }

    /// Release every held message in original fake-output order.
    #[allow(dead_code)]
    pub(crate) fn release_messages(&self, endpoint: Endpoint) {
        self.shared.held_message_endpoints.lock().remove(&endpoint);
        self.shared.output_ready.notify_waiters();
    }

    /// Hold one generated response command and every frame behind it on the
    /// same connection, preserving wire order until explicitly released.
    #[allow(dead_code)]
    pub(crate) fn hold_command(
        &self,
        endpoint: Endpoint,
        command: magnetar_proto::pb::base_command::Type,
    ) {
        self.shared
            .held_output_commands
            .lock()
            .insert((endpoint, command as i32));
    }

    #[allow(dead_code)]
    pub(crate) fn release_command(
        &self,
        endpoint: Endpoint,
        command: magnetar_proto::pb::base_command::Type,
    ) {
        self.shared
            .held_output_commands
            .lock()
            .remove(&(endpoint, command as i32));
        self.shared.output_ready.notify_waiters();
    }
}

impl Drop for M1SocketCluster {
    fn drop(&mut self) {
        for task in &self.accept_tasks {
            task.abort();
        }
        for task in self.shared.sessions.lock().drain(..) {
            task.abort();
        }
    }
}

fn spawn_accept_loop(
    shared: Arc<Shared>,
    endpoint: Endpoint,
    listener: TcpListener,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    record_failure(&shared, format!("{endpoint:?} accept failed: {error}"));
                    return;
                }
            };
            let connection = match shared.fake.lock().open_connection(endpoint) {
                Ok(connection) => connection,
                Err(error) => {
                    record_failure(
                        &shared,
                        format!("{endpoint:?} fake connection failed: {error}"),
                    );
                    return;
                }
            };
            shared.changed.notify_waiters();
            let session_shared = shared.clone();
            let task = tokio::spawn(async move {
                run_session(session_shared.clone(), endpoint, connection, stream).await;
                session_shared.changed.notify_waiters();
            });
            shared.sessions.lock().push(task);
        }
    })
}

async fn run_session(
    shared: Arc<Shared>,
    endpoint: Endpoint,
    connection: magnetar_fakes::m1::ConnectionId,
    mut stream: TcpStream,
) {
    let mut input = BytesMut::with_capacity(64 * 1024);
    loop {
        let output_ready = shared.output_ready.notified();
        tokio::pin!(output_ready);
        output_ready.as_mut().enable();

        let output = match shared.fake.lock().take_output(connection) {
            Ok(output) => output,
            Err(M1FakeError::Disconnected(_)) => return,
            Err(error) => {
                record_failure(&shared, format!("fake output drain failed: {error}"));
                return;
            }
        };
        let output = match prepare_output(&shared, endpoint, connection, output) {
            Ok(output) => output,
            Err(error) => {
                record_failure(&shared, error);
                return;
            }
        };
        if write_output(&mut stream, output).await.is_err() {
            let _ = shared.fake.lock().disconnect_connection(connection);
            shared.held_messages.lock().remove(&connection);
            shared.changed.notify_waiters();
            return;
        }

        tokio::select! {
            read = stream.read_buf(&mut input) => match read {
                Ok(0) | Err(_) => {
                    let _ = shared.fake.lock().disconnect_connection(connection);
                    shared.held_messages.lock().remove(&connection);
                    shared.changed.notify_waiters();
                    return;
                }
                Ok(_) => {
                    match forward_complete_frames(&shared, connection, &mut input) {
                        Ok(true) => {}
                        Ok(false) => return,
                        Err(error) => {
                            record_failure(&shared, error);
                            return;
                        }
                    }
                    shared.output_ready.notify_waiters();
                    shared.changed.notify_waiters();
                }
            },
            () = &mut output_ready => {}
        }
    }
}

fn prepare_output(
    shared: &Shared,
    endpoint: Endpoint,
    connection: magnetar_fakes::m1::ConnectionId,
    output: Vec<Bytes>,
) -> Result<Vec<Bytes>, String> {
    let holding_messages = shared.held_message_endpoints.lock().contains(&endpoint);
    let holding_command = shared
        .held_output_commands
        .lock()
        .iter()
        .any(|(held_endpoint, _)| *held_endpoint == endpoint);
    let mut held = shared.held_messages.lock();
    let mut ready = if holding_messages || holding_command {
        Vec::new()
    } else {
        held.remove(&connection)
            .map_or_else(Vec::new, |messages| messages.into_iter().collect())
    };
    let mut blocked = held
        .get(&connection)
        .is_some_and(|messages| !messages.is_empty());
    for bytes in output {
        let bytes = retain_sealed_placements(shared, bytes)?;
        let mut candidate = bytes.clone();
        let frame = decode_one(&mut candidate)
            .map_err(|error| format!("generated fake frame decode failed: {error}"))?;
        let command_is_held = shared
            .held_output_commands
            .lock()
            .contains(&(endpoint, frame.command.r#type));
        if blocked
            || (holding_messages
                && frame.command.r#type == magnetar_proto::pb::base_command::Type::Message as i32)
            || command_is_held
        {
            held.entry(connection).or_default().push_back(bytes);
            blocked = true;
        } else {
            ready.push(bytes);
        }
    }
    Ok(ready)
}

fn retain_sealed_placements(shared: &Shared, bytes: Bytes) -> Result<Bytes, String> {
    if !shared.retain_sealed_placements.load(Ordering::Acquire) {
        return Ok(bytes);
    }
    let mut candidate = bytes.clone();
    let frame = decode_one(&mut candidate)
        .map_err(|error| format!("generated fake frame decode failed: {error}"))?;
    if frame.command.r#type != magnetar_proto::pb::base_command::Type::ScalableTopicUpdate as i32 {
        return Ok(bytes);
    }
    let mut command = frame.command;
    let Some(update) = command.scalable_topic_update.as_mut() else {
        return Ok(bytes);
    };
    let Some(dag) = update.dag.as_mut() else {
        return Ok(bytes);
    };
    let fake = shared.fake.lock();
    for segment in &dag.segments {
        if segment.state != magnetar_proto::pb::SegmentState::Sealed as i32
            || dag
                .segment_brokers
                .iter()
                .any(|placement| placement.segment_id == segment.segment_id)
        {
            continue;
        }
        let endpoint = fake.segment_endpoint(segment.segment_id).ok_or_else(|| {
            format!(
                "sealed segment {} has no retained fake endpoint",
                segment.segment_id
            )
        })?;
        let broker_url = fake
            .endpoint_url_for(endpoint, TransportSecurity::Plaintext)
            .ok_or_else(|| {
                format!(
                    "sealed segment {} has no plaintext authority",
                    segment.segment_id
                )
            })?
            .to_owned();
        let broker_url_tls = fake
            .endpoint_url_for(endpoint, TransportSecurity::Tls)
            .map(str::to_owned);
        dag.segment_brokers
            .push(magnetar_proto::pb::SegmentBrokerAddress {
                segment_id: segment.segment_id,
                broker_url,
                broker_url_tls,
            });
    }
    drop(fake);
    let mut encoded = BytesMut::new();
    encode_command(&mut encoded, &command)
        .map_err(|error| format!("sealed-placement frame encode failed: {error}"))?;
    Ok(encoded.freeze())
}

fn forward_complete_frames(
    shared: &Shared,
    connection: magnetar_fakes::m1::ConnectionId,
    input: &mut BytesMut,
) -> Result<bool, String> {
    loop {
        let mut candidate = input.clone().freeze();
        let before = candidate.len();
        match decode_one(&mut candidate) {
            Ok(_) => {
                let consumed = before - candidate.len();
                let mut frame: Bytes = input.split_to(consumed).freeze();
                match shared.fake.lock().handle_bytes(connection, &mut frame) {
                    Ok(()) => {}
                    Err(M1FakeError::Disconnected(_)) => return Ok(false),
                    Err(error) => return Err(error.to_string()),
                }
            }
            Err(FrameError::Incomplete { .. }) => return Ok(true),
            Err(error) => return Err(format!("client frame decode failed: {error}")),
        }
    }
}

async fn write_output(stream: &mut TcpStream, frames: Vec<Bytes>) -> Result<(), std::io::Error> {
    let has_output = !frames.is_empty();
    for frame in frames {
        stream.write_all(&frame).await?;
    }
    if has_output {
        stream.flush().await?;
    }
    Ok(())
}

fn record_failure(shared: &Shared, failure: String) {
    let mut slot = shared.failure.lock();
    if slot.is_none() {
        *slot = Some(failure);
    }
    drop(slot);
    shared.changed.notify_waiters();
    shared.output_ready.notify_waiters();
}
