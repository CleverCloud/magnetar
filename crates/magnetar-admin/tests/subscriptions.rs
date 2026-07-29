// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the subscription admin REST endpoints.
//!
//! These pin the exact path, verb, query parameter, and JSON wire shape
//! against the upstream Apache Pulsar admin REST surface — the operator
//! subscription verbs implemented in
//! `pulsar-broker/.../v2/PersistentTopics.java`
//! (`getSubscriptions`, `resetCursor`, `resetCursorOnPosition`,
//! `skipMessages`, `skipAllMessages`, `expireMessagesForSubscription`,
//! `deleteSubscription`).

use magnetar_admin::{AdminClient, AdminError};
use magnetar_proto::MessageId;
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn subscriptions_list_returns_string_array() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscriptions",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["s-a", "s-b"])))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let subs = admin
        .subscriptions_list("public/default/orders")
        .await
        .expect("list returns 200 + JSON array");
    assert_eq!(subs, vec!["s-a".to_owned(), "s-b".to_owned()]);
}

#[tokio::test]
async fn reset_cursor_to_position_posts_reset_cursor_data_body() {
    let mock = MockServer::start().await;
    // Java `ResetCursorData` body shape — `partitionIndex` / `batchIndex`
    // are camelCase, and `isExcluded` retains its `is` prefix. The PIP-415
    // / pre-PIP-415 broker accepts both `partitionIndex: -1` (non-partitioned)
    // and `batchIndex: -1` (non-batched) sentinels.
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscription/s-a/resetcursor",
        ))
        .and(body_json(serde_json::json!({
            "ledgerId": 17,
            "entryId": 42,
            "partitionIndex": -1,
            "batchIndex": -1,
            "isExcluded": false,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let id = MessageId {
        ledger_id: 17,
        entry_id: 42,
        partition: -1,
        batch_index: -1,
        batch_size: -1,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };
    admin
        .subscription_reset_cursor_to_position("public/default/orders", "s-a", id, false)
        .await
        .expect("reset by position returns 204");
}

#[tokio::test]
async fn reset_cursor_to_position_maps_sentinel_u64_max_to_negative_one() {
    // Regression guard: `MessageId::EARLIEST` and `MessageId::LATEST`
    // carry `ledger_id = entry_id = u64::MAX` on the Rust side. Pulsar
    // Jackson-binds the wire fields to Java `long` (i64) and uses `-1`
    // as the sentinel. Without the u64::MAX → -1 mapping, serde emits
    // 18446744073709551615 which overflows the broker's `long` parser
    // and the call fails 400 instead of resetting the cursor.
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscription/s-a/resetcursor",
        ))
        .and(body_json(serde_json::json!({
            "ledgerId": -1,
            "entryId": -1,
            "partitionIndex": -1,
            "batchIndex": -1,
            "isExcluded": false,
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let earliest = MessageId {
        ledger_id: u64::MAX,
        entry_id: u64::MAX,
        partition: -1,
        batch_index: -1,
        batch_size: -1,
        #[cfg(feature = "scalable-topics")]
        segment_id: None,
    };
    admin
        .subscription_reset_cursor_to_position("public/default/orders", "s-a", earliest, false)
        .await
        .expect("reset to EARLIEST returns 204");
}

#[tokio::test]
async fn reset_cursor_to_timestamp_uses_path_param() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscription/s-a/resetcursor/1717000000000",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .subscription_reset_cursor_to_timestamp("public/default/orders", "s-a", 1_717_000_000_000)
        .await
        .expect("reset by timestamp returns 204");
}

#[tokio::test]
async fn skip_messages_posts_count_in_path() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscription/s-a/skip/100",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .subscription_skip_messages("public/default/orders", "s-a", 100)
        .await
        .expect("skip N returns 204");
}

#[tokio::test]
async fn skip_all_messages_posts_to_skip_all_path() {
    let mock = MockServer::start().await;
    // Note: the broker endpoint is `skip_all` (snake_case) — not
    // `skip-all` or `skipAll`. This matches `PersistentTopics.java`.
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscription/s-a/skip_all",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .subscription_skip_all_messages("public/default/orders", "s-a")
        .await
        .expect("skip-all returns 204");
}

#[tokio::test]
async fn expire_messages_posts_seconds_in_path() {
    let mock = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscription/s-a/expireMessages/3600",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .subscription_expire_messages("public/default/orders", "s-a", 3600)
        .await
        .expect("expire returns 204");
}

#[tokio::test]
async fn delete_subscription_without_force_omits_query() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscription/s-a",
        ))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .subscription_delete("public/default/orders", "s-a", false)
        .await
        .expect("delete returns 204");
}

/// Pulsar subscription names routinely embed `|` (Clever Cloud's
/// `<app-name>|<app-id>` convention, and any Java consumer that defaults its
/// subscription to a pipe-joined identifier). `|` is not an RFC 3986 `pchar`,
/// so a raw pipe on the wire makes the broker's Jetty front end reject the
/// request at URI-parse time with `400 Illegal Path Character` — before
/// routing, which is why the body carries no broker `reason`.
///
/// Java encodes the segment with `Codec.encode` (`URLEncoder.encode`) in
/// `TopicsImpl#deleteSubscriptionAsync`, putting `%7C` on the wire; the broker
/// percent-decodes it back to `|`. Assert the same byte sequence here.
#[tokio::test]
async fn delete_subscription_percent_encodes_pipe_in_name() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .subscription_delete(
            "persistent://orga_8b852f0a/pulsar_871e00e8/events",
            "ovd.loadbalancer.api|app_6153c255",
            false,
        )
        .await
        .expect("delete returns 204");

    let requests = mock.received_requests().await.expect("recording enabled");
    assert_eq!(
        requests[0].url.path(),
        "/admin/v2/persistent/orga_8b852f0a/pulsar_871e00e8/events/subscription/\
         ovd.loadbalancer.api%7Capp_6153c255",
    );
}

/// The same encoding gap applies to every segment the client interpolates, not
/// just subscription names: `[`, `]`, `^` and `|` are the four printable ASCII
/// bytes the `url` crate's WHATWG path-segment set leaves raw while RFC 3986
/// forbids them in a path. A topic name carrying one must not reach the broker
/// unencoded either.
#[tokio::test]
async fn subscriptions_list_percent_encodes_non_pchar_topic_bytes() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .subscriptions_list("public/default/or[der]s^v|1")
        .await
        .expect("list returns 200");

    let requests = mock.received_requests().await.expect("recording enabled");
    assert_eq!(
        requests[0].url.path(),
        "/admin/v2/persistent/public/default/or%5Bder%5Ds%5Ev%7C1/subscriptions",
    );
}

#[tokio::test]
async fn delete_subscription_with_force_sets_query() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path(
            "/admin/v2/persistent/public/default/orders/subscription/s-a",
        ))
        .and(query_param("force", "true"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .subscription_delete("public/default/orders", "s-a", true)
        .await
        .expect("force delete returns 204");
}

#[tokio::test]
async fn subscriptions_list_propagates_404_on_unknown_topic() {
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(
            "/admin/v2/persistent/public/default/missing/subscriptions",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_string("Topic not found"))
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let err = admin
        .subscriptions_list("public/default/missing")
        .await
        .unwrap_err();
    assert!(matches!(err, AdminError::Status { code: 404, .. }));
}
