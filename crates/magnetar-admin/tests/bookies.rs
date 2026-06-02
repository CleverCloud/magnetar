// SPDX-License-Identifier: Apache-2.0

//! Wire-level tests for the bookies REST endpoints — list-all,
//! rack-info GET / POST / DELETE.
//!
//! These pin the exact path, verb, and JSON body shape against
//! `BookiesBase` in `pulsar-broker/.../v2/Bookies.java`
//! (`getAllAvailableBookies`, `getBookieRackInfo`,
//! `updateBookieRackInfo`, `deleteBookieRackInfo`).

use magnetar_admin::{AdminClient, BookieInfo};
use wiremock::matchers::{body_json, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(mock: &MockServer) -> AdminClient {
    AdminClient::builder()
        .service_url(mock.uri().parse().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn bookies_list_all_returns_cluster_info() {
    let mock = MockServer::start().await;
    // `getAllAvailableBookies` ships the `BookiesClusterInfo`
    // envelope — a single `bookies` array of `{address}` objects.
    Mock::given(method("GET"))
        .and(path("/admin/v2/bookies/all"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "bookies": [
                { "address": "bookie-1:3181" },
                { "address": "bookie-2:3181" },
            ]
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let info = admin
        .bookies_list_all()
        .await
        .expect("bookies list returns 200");
    assert_eq!(info["bookies"][0]["address"], "bookie-1:3181");
    assert_eq!(info["bookies"][1]["address"], "bookie-2:3181");
}

#[tokio::test]
async fn bookies_racks_info_returns_nested_map() {
    let mock = MockServer::start().await;
    // The wire shape is `Map<group, Map<bookieAddress, BookieInfo>>` —
    // raw JSON because the nested-map layout shifts between releases.
    Mock::given(method("GET"))
        .and(path("/admin/v2/bookies/racks-info"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "default": {
                "bookie-1:3181": {
                    "group": "default",
                    "rack": "rack-a",
                    "hostname": "bookie-1.example",
                },
                "bookie-2:3181": {
                    "group": "default",
                    "rack": "rack-b",
                    "hostname": "bookie-2.example",
                },
            }
        })))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    let racks = admin
        .bookies_racks_info()
        .await
        .expect("racks-info returns 200");
    assert_eq!(racks["default"]["bookie-1:3181"]["rack"], "rack-a");
    assert_eq!(
        racks["default"]["bookie-2:3181"]["hostname"],
        "bookie-2.example"
    );
}

#[tokio::test]
async fn bookies_set_rack_posts_camelcase_body_and_group_query() {
    let mock = MockServer::start().await;
    // Pulsar's BookiesBase#updateBookieRackInfo takes `group` as a
    // @QueryParam; the JSON body Jackson-binds only to `{rack,
    // hostname}`. Pin both the query parameter AND the body shape so a
    // regression that puts `group` back in the body (silently ignored
    // by the broker) is caught.
    Mock::given(method("POST"))
        .and(path("/admin/v2/bookies/racks-info/bookie-1:3181"))
        .and(query_param("group", "default"))
        .and(body_json(serde_json::json!({
            "rack": "rack-a",
            "hostname": "bookie-1.example",
        })))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .bookies_set_rack(
            "bookie-1:3181",
            "default",
            BookieInfo {
                rack: "rack-a".into(),
                hostname: "bookie-1.example".into(),
            },
        )
        .await
        .expect("set-rack returns 204");
}

#[tokio::test]
async fn bookies_delete_rack_drops_assignment() {
    let mock = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/v2/bookies/racks-info/bookie-1:3181"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&mock)
        .await;

    let admin = client(&mock);
    admin
        .bookies_delete_rack("bookie-1:3181")
        .await
        .expect("delete-rack returns 204");
}
