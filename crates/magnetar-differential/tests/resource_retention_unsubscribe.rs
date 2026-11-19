// SPDX-License-Identifier: Apache-2.0

//! A cancelled unsubscribe must not strand the last consumer registration.

use std::future::Future;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use bytes::BytesMut;
use magnetar_differential::broker::ScriptedBroker;
use magnetar_proto::{
    Connection, ConnectionConfig, PendingOpKey, RequestId, SubscribeRequest, decode_one,
    encode_command, pb,
};
use magnetar_runtime_moonpool::{Client as MoonpoolClient, MoonpoolEngine};
use magnetar_runtime_tokio::Client as TokioClient;
use moonpool_core::TokioProviders;

fn response(conn: &mut Connection, request_id: u64, reject: bool) {
    let command = if reject {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Error as i32,
            error: Some(pb::CommandError {
                request_id,
                error: pb::ServerError::ServiceNotReady as i32,
                message: "unsubscribe rejected".to_owned(),
            }),
            ..Default::default()
        }
    } else {
        pb::BaseCommand {
            r#type: pb::base_command::Type::Success as i32,
            success: Some(pb::CommandSuccess {
                request_id,
                schema: None,
            }),
            ..Default::default()
        }
    };
    let mut frame = BytesMut::new();
    encode_command(&mut frame, &command).expect("encode response");
    conn.handle_bytes(Instant::now(), &frame).expect("response");
}

fn commands(conn: &mut Connection) -> Vec<pb::BaseCommand> {
    let mut bytes = conn.poll_transmit();
    let mut commands = Vec::new();
    while !bytes.is_empty() {
        commands.push(decode_one(&mut bytes).expect("complete command").command);
    }
    commands
}

macro_rules! scenario {
    ($client:expr, $name:expr) => {{
        let client = $client;
        let request = |suffix| SubscribeRequest {
            topic: format!("persistent://public/default/unsubscribe-{}-{suffix}", $name),
            subscription: format!("unsubscribe-{}-{suffix}", $name),
            ..Default::default()
        };
        let consumer = client
            .subscribe(request("cancelled"))
            .await
            .expect("subscribe");
        let handle = consumer.handle();
        let shared = client.shared();
        let _ = commands(&mut shared.inner.lock());
        let mut cx = Context::from_waker(Waker::noop());
        {
            let mut future = Box::pin(consumer.unsubscribe(false));
            assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        }
        drop(consumer);
        {
            let mut conn = shared.inner.lock();
            let unsubscribe = commands(&mut conn);
            assert_eq!(unsubscribe.len(), 1);
            let unsubscribe = unsubscribe[0]
                .unsubscribe
                .as_ref()
                .expect("unsubscribe frame");
            assert_eq!(unsubscribe.consumer_id, handle.0);
            response(&mut conn, unsubscribe.request_id, true);
            let close = commands(&mut conn);
            assert_eq!(close.len(), 1, "one close follows the rejected unsubscribe");
            let close = close[0].close_consumer.as_ref().expect("close frame");
            assert_eq!(close.consumer_id, handle.0);
            assert_ne!(close.request_id, unsubscribe.request_id);
            response(&mut conn, close.request_id, false);
            assert!(conn.consumer(handle).is_none());
            assert!(
                conn.take_outcome(PendingOpKey::Request(RequestId(unsubscribe.request_id)))
                    .is_none()
            );
            assert!(
                conn.take_outcome(PendingOpKey::Request(RequestId(close.request_id)))
                    .is_none(),
                "forgotten close does not retain a result"
            );
            assert!(commands(&mut conn).is_empty(), "no duplicate close");
        }

        let consumer = client
            .subscribe(request("live"))
            .await
            .expect("subscribe control");
        let handle = consumer.handle();
        let _ = commands(&mut shared.inner.lock());
        let mut future = Box::pin(consumer.unsubscribe(false));
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));
        {
            let mut conn = shared.inner.lock();
            let unsubscribe = commands(&mut conn);
            assert_eq!(unsubscribe.len(), 1);
            let unsubscribe = unsubscribe[0]
                .unsubscribe
                .as_ref()
                .expect("unsubscribe frame");
            response(&mut conn, unsubscribe.request_id, true);
            assert!(
                conn.consumer(handle).is_some(),
                "live owner keeps its registration"
            );
            assert!(
                commands(&mut conn).is_empty(),
                "live owner does not initiate close"
            );
        }
        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Ready(Err(_))));
        drop(future);
        consumer.close().await.expect("close live control");
        client.close().await;
    }};
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_cancelled_unsubscribe_closes_once_in_both_engines() {
    let broker = ScriptedBroker::bind().await.expect("broker");
    let tokio = TokioClient::connect(&broker.pulsar_url(), ConnectionConfig::default())
        .await
        .expect("tokio");
    scenario!(tokio, "tokio");
    let engine = MoonpoolEngine::new(TokioProviders::new());
    let moonpool =
        MoonpoolClient::connect_plain(&engine, &broker.host_port(), ConnectionConfig::default())
            .await
            .expect("moonpool");
    scenario!(moonpool, "moonpool");
    broker.shutdown().await;
}
