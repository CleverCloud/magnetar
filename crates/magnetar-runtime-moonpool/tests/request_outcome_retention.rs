// SPDX-License-Identifier: Apache-2.0

//! A session reset must not record an outcome for a producer open or a consumer subscribe it
//! retires, exercised through the moonpool engine's [`ConnectionShared`] state. Mirrors the
//! matching `magnetar-runtime-tokio` test, its `magnetar-proto` unit test
//! (`crates/magnetar-proto/tests/request_outcome_retention.rs`) and the `magnetar-differential`
//! equivalence scenario.
//!
//! `Connection::reset()` publishes `SessionLost` for every pending request it fails. A lookup is
//! awaited through that outcome, but the runtimes await a producer open and a consumer subscribe
//! through `ProducerReady` / `SubscribeAcked` (or their `*Failed`) events and the driver discards
//! the request ids `rebuild_producers` / `rebuild_consumers` return, so those two outcomes can
//! never be taken and live as long as the connection.
//!
//! Layer (c) of the ADR-0024 four-layer test policy.

mod common;

use std::time::Instant;

use magnetar_proto::{
    ConnectionConfig, CreateProducerRequest, OpOutcome, PendingOpKey, RequestId, SubscribeRequest,
};
use magnetar_runtime_moonpool::ConnectionShared;

use crate::common::handshake_response_bytes;

#[test]
fn reset_records_no_outcome_for_a_pending_producer_open_and_consumer_subscribe() {
    let now = Instant::now();
    let shared = ConnectionShared::new(ConnectionConfig::default());
    let mut conn = shared.inner.lock();

    conn.begin_handshake().expect("handshake");
    conn.handle_bytes(now, &handshake_response_bytes())
        .expect("Connected");
    let _ = conn.poll_event();
    let _ = conn.poll_transmit();

    let open = RequestId(conn.peek_next_request_id_for_test());
    let _producer = conn.create_producer(CreateProducerRequest {
        topic: "persistent://public/default/reset-pending-open".to_owned(),
        ..Default::default()
    });
    let subscribe = RequestId(conn.peek_next_request_id_for_test());
    let _consumer = conn.subscribe(SubscribeRequest {
        topic: "persistent://public/default/reset-pending-subscribe".to_owned(),
        subscription: "sub".to_owned(),
        ..Default::default()
    });
    let lookup = conn.lookup("persistent://public/default/reset-pending-lookup", false);
    assert!(
        [open, subscribe, lookup]
            .into_iter()
            .all(|id| conn.has_pending_request_for_test(id)),
        "precondition: the open, the subscribe and the lookup must be pending at the reset"
    );

    conn.reset();

    // Control: a lookup is awaited through its outcome (the engine re-issues it on
    // `SessionLost`), so the assertions below are about the open and the subscribe and not about
    // reset() publishing nothing.
    assert!(
        matches!(
            conn.take_outcome(PendingOpKey::Request(lookup)),
            Some(OpOutcome::SessionLost { .. })
        ),
        "reset() must still fail a pending lookup with SessionLost"
    );
    assert!(
        conn.take_outcome(PendingOpKey::Request(open)).is_none(),
        "reset() recorded an outcome for the pending producer open, which nothing can take"
    );
    assert!(
        conn.take_outcome(PendingOpKey::Request(subscribe))
            .is_none(),
        "reset() recorded an outcome for the pending subscribe, which nothing can take"
    );
}
