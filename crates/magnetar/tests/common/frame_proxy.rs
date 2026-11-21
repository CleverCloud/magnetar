// SPDX-License-Identifier: Apache-2.0

//! A TCP forwarder between a client and the broker that reads the Pulsar frames it forwards, so an
//! end-to-end test can choose the moment a session breaks: stop reading what the client writes,
//! drop one client frame, corrupt one so the broker's checksum check refuses it, hold the broker's
//! frames from one kind on, or cut every connection.
//!
//! The broker itself keeps running, so nothing depends on a standalone container coming back
//! from a restart.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use magnetar_proto::pb::base_command::Type;
use magnetar_proto::{FrameError, decode_one};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;

#[derive(Default)]
struct Controls {
    /// Connections accepted so far.
    accepted: AtomicUsize,
    /// Stop reading what clients write, on every connection.
    client_paused: AtomicBool,
    /// Drop the next client frame of this type, once.
    drop_client: Mutex<Option<Type>>,
    /// Client frames dropped so far.
    dropped: AtomicUsize,
    /// Flip the last byte of the next client frame of this type, once.
    corrupt_client: Mutex<Option<Type>>,
    /// Client frames corrupted so far.
    corrupted: AtomicUsize,
    /// On connections accepted from now on, hold the broker's frames from the first of this type.
    hold_broker: Mutex<Option<Type>>,
    /// Broker frames a connection started holding at.
    holding: AtomicUsize,
    released: AtomicBool,
    release: Notify,
    progress: Notify,
    live: Mutex<Vec<tokio::task::AbortHandle>>,
}

/// See the module documentation. Dropping it stops accepting and cuts every connection.
pub struct FrameProxy {
    controls: Arc<Controls>,
    accept: tokio::task::JoinHandle<()>,
    url: String,
}

impl FrameProxy {
    /// Forward `127.0.0.1:<ephemeral>` to `broker` (`host:port`).
    pub async fn start(broker: String) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let url = format!("pulsar://{}", listener.local_addr()?);
        let controls = Arc::new(Controls::default());
        let accept = {
            let controls = controls.clone();
            tokio::spawn(async move {
                while let Ok((client, _)) = listener.accept().await {
                    controls.accepted.fetch_add(1, Ordering::SeqCst);
                    let hold = *controls.hold_broker.lock().expect("proxy controls");
                    let forward =
                        tokio::spawn(forward(client, broker.clone(), controls.clone(), hold));
                    controls
                        .live
                        .lock()
                        .expect("proxy controls")
                        .push(forward.abort_handle());
                    controls.progress.notify_waiters();
                }
            })
        };
        Ok(Self {
            controls,
            accept,
            url,
        })
    }

    /// The `pulsar://` URL a client dials to go through the proxy.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Connections accepted so far.
    pub fn accepted(&self) -> usize {
        self.controls.accepted.load(Ordering::SeqCst)
    }

    /// Stop reading what clients write, on every connection, current and future.
    pub fn pause_client_writes(&self) {
        self.controls.client_paused.store(true, Ordering::SeqCst);
    }

    /// Drop the next client frame of type `kind`, once.
    pub fn drop_next_client_frame(&self, kind: Type) {
        *self.controls.drop_client.lock().expect("proxy controls") = Some(kind);
    }

    /// Client frames dropped so far.
    pub fn dropped(&self) -> usize {
        self.controls.dropped.load(Ordering::SeqCst)
    }

    /// Flip the last byte of the next client frame of type `kind`, once. For a frame that carries
    /// a payload that byte is the payload's last, which the frame's CRC32C covers, so the broker
    /// refuses the frame instead of reading a different message.
    pub fn corrupt_next_client_frame(&self, kind: Type) {
        *self.controls.corrupt_client.lock().expect("proxy controls") = Some(kind);
    }

    /// Client frames corrupted so far.
    pub fn corrupted(&self) -> usize {
        self.controls.corrupted.load(Ordering::SeqCst)
    }

    /// On connections accepted from now on, hold the broker's frames from the first of type `kind`
    /// on, until [`Self::release`].
    pub fn hold_broker_frames_from(&self, kind: Type) {
        *self.controls.hold_broker.lock().expect("proxy controls") = Some(kind);
    }

    /// Connections that have started holding the broker's frames.
    pub fn holding(&self) -> usize {
        self.controls.holding.load(Ordering::SeqCst)
    }

    /// Forward every held frame, and stop holding. The release is final: a connection accepted
    /// after it does not hold, even after another [`Self::hold_broker_frames_from`].
    pub fn release(&self) {
        *self.controls.hold_broker.lock().expect("proxy controls") = None;
        self.controls.released.store(true, Ordering::SeqCst);
        self.controls.release.notify_waiters();
    }

    /// Drop every connection still being forwarded; returns how many there were. A connection
    /// that already ended on its own is not counted, so each one counted redials.
    pub fn cut(&self) -> usize {
        let connections = std::mem::take(&mut *self.controls.live.lock().expect("proxy controls"));
        let live = connections.iter().filter(|c| !c.is_finished()).count();
        for connection in &connections {
            connection.abort();
        }
        live
    }

    /// Wait until `ready` holds, re-checking after every accept, drop or hold.
    pub async fn wait_until(&self, mut ready: impl FnMut(&Self) -> bool) {
        loop {
            let progress = self.controls.progress.notified();
            if ready(self) {
                return;
            }
            let _ = tokio::time::timeout(std::time::Duration::from_millis(100), progress).await;
        }
    }
}

impl Drop for FrameProxy {
    fn drop(&mut self) {
        self.accept.abort();
        self.cut();
    }
}

async fn forward(client: TcpStream, broker: String, controls: Arc<Controls>, hold: Option<Type>) {
    let Ok(upstream) = TcpStream::connect(&broker).await else {
        return;
    };
    let (client_read, client_write) = client.into_split();
    let (broker_read, broker_write) = upstream.into_split();
    let upstream_controls = controls.clone();
    tokio::select! {
        () = client_to_broker(client_read, broker_write, upstream_controls) => {}
        () = broker_to_client(broker_read, client_write, controls, hold) => {}
    }
}

/// Split every complete frame off `pending`, with its type. Bytes that do not decode as a frame
/// are forwarded as they are.
fn frames(pending: &mut BytesMut) -> Vec<(Option<Type>, Bytes)> {
    let mut out = Vec::new();
    loop {
        let before = pending.len();
        let mut probe = pending.clone().freeze();
        match decode_one(&mut probe) {
            Ok(frame) => {
                let consumed = before - probe.len();
                let kind = Type::try_from(frame.command.r#type).ok();
                out.push((kind, pending.split_to(consumed).freeze()));
            }
            Err(FrameError::Incomplete { .. }) => return out,
            Err(_) => {
                out.push((None, pending.split().freeze()));
                return out;
            }
        }
    }
}

async fn client_to_broker(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    controls: Arc<Controls>,
) {
    let mut pending = BytesMut::new();
    let mut buf = vec![0_u8; 64 * 1024];
    loop {
        if controls.client_paused.load(Ordering::SeqCst) {
            // The client's writes now pile up in the kernel buffers until they are full.
            std::future::pending::<()>().await;
        }
        let n =
            match tokio::time::timeout(std::time::Duration::from_millis(50), from.read(&mut buf))
                .await
            {
                Ok(Ok(0) | Err(_)) => return,
                Ok(Ok(n)) => n,
                Err(_elapsed) => continue,
            };
        pending.extend_from_slice(&buf[..n]);
        for (kind, bytes) in frames(&mut pending) {
            let drop_it = {
                let mut drop_client = controls.drop_client.lock().expect("proxy controls");
                if kind.is_some() && *drop_client == kind {
                    *drop_client = None;
                    true
                } else {
                    false
                }
            };
            if drop_it {
                controls.dropped.fetch_add(1, Ordering::SeqCst);
                controls.progress.notify_waiters();
                continue;
            }
            let corrupt_it = {
                let mut corrupt_client = controls.corrupt_client.lock().expect("proxy controls");
                if kind.is_some() && *corrupt_client == kind {
                    *corrupt_client = None;
                    true
                } else {
                    false
                }
            };
            let bytes = if corrupt_it && !bytes.is_empty() {
                let mut corrupted = BytesMut::from(&bytes[..]);
                let last = corrupted.len() - 1;
                corrupted[last] ^= 0xff;
                controls.corrupted.fetch_add(1, Ordering::SeqCst);
                controls.progress.notify_waiters();
                corrupted.freeze()
            } else {
                bytes
            };
            if to.write_all(&bytes).await.is_err() {
                return;
            }
        }
    }
}

async fn broker_to_client(
    mut from: OwnedReadHalf,
    mut to: OwnedWriteHalf,
    controls: Arc<Controls>,
    hold: Option<Type>,
) {
    let mut pending = BytesMut::new();
    let mut buf = vec![0_u8; 64 * 1024];
    let mut holding_now = false;
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        pending.extend_from_slice(&buf[..n]);
        for (kind, bytes) in frames(&mut pending) {
            if !holding_now && hold.is_some() && kind == hold {
                holding_now = true;
                controls.holding.fetch_add(1, Ordering::SeqCst);
                controls.progress.notify_waiters();
                loop {
                    let release = controls.release.notified();
                    if controls.released.load(Ordering::SeqCst) {
                        break;
                    }
                    release.await;
                }
            }
            if to.write_all(&bytes).await.is_err() {
                return;
            }
        }
    }
}

/// The `host:port` of a `pulsar://` service URL.
pub fn authority(service_url: &str) -> String {
    service_url
        .strip_prefix("pulsar://")
        .unwrap_or(service_url)
        .to_owned()
}
