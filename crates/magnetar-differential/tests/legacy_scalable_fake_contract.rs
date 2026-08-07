// SPDX-License-Identifier: Apache-2.0

//! Transcript compatibility for the legacy single-endpoint scalable fake.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]

use bytes::{Bytes, BytesMut};
use magnetar_fakes::ScriptedScalableBroker;
use magnetar_proto::{decode_one, encode_command, pb};

fn encoded(command: &pb::BaseCommand) -> Bytes {
    let mut bytes = BytesMut::new();
    encode_command(&mut bytes, command).expect("encode fake input");
    bytes.freeze()
}

fn decode_update(mut bytes: Bytes) -> pb::CommandScalableTopicUpdate {
    decode_one(&mut bytes)
        .expect("decode fake update")
        .command
        .scalable_topic_update
        .expect("scalable update body")
}

#[test]
fn legacy_scalable_fake_emits_initial_split_and_merge_layouts() {
    let mut broker = ScriptedScalableBroker::two_segment();
    assert!(format!("{broker:?}").contains("ScriptedScalableBroker"));
    assert_eq!(broker.controller_broker_url(), "pulsar://controller:6650");
    assert_eq!(
        broker
            .initial_dag()
            .iter()
            .map(|segment| (segment.segment_id, segment.hash_start, segment.hash_end))
            .collect::<Vec<_>>(),
        vec![(1, 0, 32_767), (2, 32_768, 65_535)]
    );
    assert_eq!(broker.session_id(), None);
    assert!(broker.split_update().is_none());
    assert!(broker.merge_update().is_none());

    let mut malformed = Bytes::from_static(b"not-a-frame");
    assert!(broker.on_client_bytes(&mut malformed).is_empty());
    let mut ping = encoded(&pb::BaseCommand {
        r#type: pb::base_command::Type::Ping as i32,
        ping: Some(pb::CommandPing {}),
        ..Default::default()
    });
    assert!(broker.on_client_bytes(&mut ping).is_empty());

    let mut lookup = encoded(&pb::BaseCommand {
        r#type: pb::base_command::Type::ScalableTopicLookup as i32,
        scalable_topic_lookup: Some(pb::CommandScalableTopicLookup {
            session_id: 42,
            topic: "topic://public/default/scaled".to_owned(),
        }),
        ..Default::default()
    });
    let initial = decode_update(broker.on_client_bytes(&mut lookup).freeze());
    assert_eq!(broker.session_id(), Some(42));
    assert_eq!(initial.session_id, 42);
    let initial_dag = initial.dag.expect("initial DAG");
    assert_eq!(initial_dag.epoch, 1);
    assert_eq!(initial_dag.segments.len(), 2);
    assert_eq!(initial_dag.segment_brokers.len(), 2);

    let split = decode_update(broker.split_update().expect("split update").freeze());
    let split_dag = split.dag.expect("split DAG");
    assert_eq!(split_dag.epoch, 2);
    assert_eq!(
        split_dag
            .segments
            .iter()
            .map(|segment| (segment.segment_id, segment.parent_ids.clone()))
            .collect::<Vec<_>>(),
        vec![(1, vec![]), (2, vec![]), (3, vec![1]), (4, vec![1])]
    );

    let merge = decode_update(broker.merge_update().expect("merge update").freeze());
    let merge_dag = merge.dag.expect("merge DAG");
    assert_eq!(merge_dag.epoch, 3);
    assert_eq!(
        merge_dag
            .segments
            .last()
            .map(|segment| (segment.segment_id, segment.parent_ids.clone())),
        Some((5, vec![3, 4]))
    );
}
