// SPDX-License-Identifier: Apache-2.0

//! Public `MSTR` position-codec coverage through the simulation runner.

#![cfg(feature = "scalable-topics")]
#![allow(clippy::expect_used)]
#![allow(clippy::too_many_lines)]

use magnetar_proto::{
    KeyRange, MAX_ORDINARY_MESSAGE_ID_SIZE, MAX_POSITION_COMPONENTS, MAX_POSITION_TOPIC_SIZE,
    MAX_STREAM_POSITION_SIZE, MessageId, PositionVector, SegmentId, SegmentSource, StreamMessageId,
    StreamPositionError, canonical_segment_topic, pb,
};
use prost::Message as _;

const HEADER_LEN: usize = 12;
const STREAM_MESSAGE_ID_KIND: u8 = 1;
const POSITION_VECTOR_KIND: u8 = 2;

fn source(id: u64, start: u32, end: u32) -> SegmentSource {
    let range = KeyRange::new(start, end).expect("valid test range");
    let topic = canonical_segment_topic("topic://t/n/x", range, SegmentId(id))
        .expect("valid segment topic");
    SegmentSource::new(SegmentId(id), topic).expect("canonical source")
}

const fn message_id(entry_id: u64) -> MessageId {
    MessageId {
        ledger_id: 1,
        entry_id,
        partition: 0,
        batch_index: 0,
        batch_size: 1,
    }
}

fn envelope(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(HEADER_LEN + payload.len());
    encoded.extend_from_slice(b"MSTR");
    encoded.push(1);
    encoded.push(kind);
    encoded.extend_from_slice(&0u16.to_be_bytes());
    encoded.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("test payload fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(payload);
    encoded
}

fn component(source: &SegmentSource, ordinary: &[u8]) -> Vec<u8> {
    let mut encoded = source.segment_id().0.to_be_bytes().to_vec();
    encoded.extend_from_slice(
        &u32::try_from(source.topic().len())
            .expect("test topic fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(source.topic().as_bytes());
    encoded.extend_from_slice(
        &u32::try_from(ordinary.len())
            .expect("test ordinary id fits u32")
            .to_be_bytes(),
    );
    encoded.extend_from_slice(ordinary);
    encoded
}

fn component_end(payload: &[u8], start: usize) -> usize {
    let topic_len = u32::from_be_bytes(
        payload[start + 8..start + 12]
            .try_into()
            .expect("topic length bytes"),
    ) as usize;
    let ordinary_len_offset = start + 12 + topic_len;
    let ordinary_len = u32::from_be_bytes(
        payload[ordinary_len_offset..ordinary_len_offset + 4]
            .try_into()
            .expect("ordinary length bytes"),
    ) as usize;
    ordinary_len_offset + 4 + ordinary_len
}

#[test]
fn stream_positions_round_trip_complete_ordinary_ids() {
    let ordinary = pb::MessageIdData {
        ledger_id: 10,
        entry_id: 20,
        partition: Some(0),
        batch_index: Some(1),
        ack_set: vec![3, 5],
        batch_size: Some(2),
        first_chunk_message_id: Some(Box::new(pb::MessageIdData {
            ledger_id: 10,
            entry_id: 18,
            partition: Some(0),
            batch_index: Some(-1),
            ack_set: Vec::new(),
            batch_size: None,
            first_chunk_message_id: None,
        })),
    };
    let first_source = source(1, 0, 32_767);
    let rich = StreamMessageId::from_message_id_data(first_source.clone(), &ordinary)
        .expect("complete ordinary id");
    assert_eq!(
        rich.ordinary_message_id_data().expect("decode ordinary id"),
        ordinary
    );
    assert_eq!(rich.ordinary_message_id(), MessageId::from_pb(&ordinary));
    assert_eq!(rich.source(), &first_source);
    assert_eq!(
        rich.encoded_len().expect("bounded stream id"),
        rich.to_bytes().expect("encode").len()
    );
    assert_eq!(
        StreamMessageId::from_bytes(&rich.to_bytes().expect("encode rich id")),
        Ok(rich.clone())
    );
    assert_eq!(
        StreamMessageId::from_ordinary_bytes(
            first_source.clone(),
            rich.ordinary_message_id_bytes(),
        ),
        Ok(rich.clone())
    );

    let second_source = source(2, 32_768, 65_535);
    let vector = PositionVector::new(
        9,
        [
            (second_source.clone(), message_id(2)),
            (first_source.clone(), message_id(1)),
        ],
    )
    .expect("position vector");
    assert_eq!(vector.layout_epoch(), 9);
    assert_eq!(vector.len(), 2);
    assert!(!vector.is_empty());
    assert_eq!(vector.get(&first_source), Some(message_id(1)));
    assert!(vector.ordinary_message_id_bytes(&second_source).is_some());
    assert_eq!(
        vector
            .iter()
            .map(|(source, _)| source.segment_id().0)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    let bytes = vector.to_bytes().expect("encode vector");
    assert_eq!(vector.encoded_len().expect("bounded vector"), bytes.len());
    assert_eq!(PositionVector::from_bytes(&bytes), Ok(vector));
}

#[test]
fn stream_position_rejects_noncanonical_ordinary_ids() {
    let source = source(1, 0, 65_535);
    for ordinary in [
        vec![0x10, 0x02, 0x08, 0x01],
        vec![0x08, 0x01, 0x10, 0x02, 0x18, 0x00, 0x18, 0x00],
        vec![0x08, 0x01, 0x10, 0x02, 0x2a, 0x02, 0x03, 0x05],
        vec![0x08, 0x01, 0x10, 0x02, 0x40, 0x00],
    ] {
        assert_eq!(
            StreamMessageId::from_ordinary_bytes(source.clone(), &ordinary),
            Err(StreamPositionError::NonCanonicalOrdinaryId)
        );
    }
    assert_eq!(
        StreamMessageId::from_ordinary_bytes(source.clone(), &[0xff]),
        Err(StreamPositionError::InvalidOrdinaryId)
    );
    assert_eq!(
        StreamMessageId::from_ordinary_bytes(
            source.clone(),
            &vec![0; MAX_ORDINARY_MESSAGE_ID_SIZE + 1],
        ),
        Err(StreamPositionError::OrdinaryIdTooLong {
            actual: MAX_ORDINARY_MESSAGE_ID_SIZE + 1,
            max: MAX_ORDINARY_MESSAGE_ID_SIZE,
        })
    );

    for (field, mut ordinary) in [
        (
            "partition",
            pb::MessageIdData {
                partition: Some(-2),
                ..Default::default()
            },
        ),
        (
            "batch_index",
            pb::MessageIdData {
                batch_index: Some(-2),
                ..Default::default()
            },
        ),
        (
            "batch_size",
            pb::MessageIdData {
                batch_size: Some(-2),
                ..Default::default()
            },
        ),
    ] {
        ordinary.ledger_id = 1;
        ordinary.entry_id = 1;
        assert_eq!(
            StreamMessageId::from_message_id_data(source.clone(), &ordinary),
            Err(StreamPositionError::ImpossibleOrdinaryId { field, value: -2 })
        );
    }

    let nested = pb::MessageIdData {
        first_chunk_message_id: Some(Box::new(pb::MessageIdData {
            first_chunk_message_id: Some(Box::default()),
            ..Default::default()
        })),
        ..Default::default()
    };
    assert_eq!(
        StreamMessageId::from_message_id_data(source.clone(), &nested),
        Err(StreamPositionError::NestedChunkId)
    );

    let oversized = pb::MessageIdData {
        ack_set: vec![-1; MAX_ORDINARY_MESSAGE_ID_SIZE / 8],
        ..Default::default()
    };
    assert!(matches!(
        StreamMessageId::from_message_id_data(source, &oversized),
        Err(StreamPositionError::OrdinaryIdTooLong { .. })
    ));
}

#[test]
fn stream_position_rejects_malformed_envelopes() {
    let valid = StreamMessageId::new(source(1, 0, 65_535), message_id(1))
        .expect("stream id")
        .to_bytes()
        .expect("encode stream id");
    assert_eq!(
        StreamMessageId::from_bytes(&valid[..4]),
        Err(StreamPositionError::TruncatedHeader)
    );

    let mut changed = valid.clone();
    changed[0] = b'X';
    assert_eq!(
        StreamMessageId::from_bytes(&changed),
        Err(StreamPositionError::InvalidMagic)
    );
    changed = valid.clone();
    changed[4] = 2;
    assert_eq!(
        StreamMessageId::from_bytes(&changed),
        Err(StreamPositionError::UnsupportedVersion(2))
    );
    changed = valid.clone();
    changed[5] = POSITION_VECTOR_KIND;
    assert_eq!(
        StreamMessageId::from_bytes(&changed),
        Err(StreamPositionError::UnexpectedKind {
            got: POSITION_VECTOR_KIND,
            expected: STREAM_MESSAGE_ID_KIND,
        })
    );
    changed = valid.clone();
    changed[7] = 1;
    assert_eq!(
        StreamMessageId::from_bytes(&changed),
        Err(StreamPositionError::UnsupportedFlags(1))
    );
    changed = valid.clone();
    changed.push(0);
    assert!(matches!(
        StreamMessageId::from_bytes(&changed),
        Err(StreamPositionError::LengthMismatch { .. })
    ));
    assert_eq!(
        StreamMessageId::from_bytes(&vec![0; MAX_STREAM_POSITION_SIZE + 1]),
        Err(StreamPositionError::EnvelopeTooLarge {
            actual: MAX_STREAM_POSITION_SIZE + 1,
            max: MAX_STREAM_POSITION_SIZE,
        })
    );

    assert_eq!(
        StreamMessageId::from_bytes(&envelope(STREAM_MESSAGE_ID_KIND, &[0])),
        Err(StreamPositionError::TruncatedPayload)
    );

    let mut invalid_utf8 = SegmentId(1).0.to_be_bytes().to_vec();
    invalid_utf8.extend_from_slice(&1u32.to_be_bytes());
    invalid_utf8.push(0xff);
    invalid_utf8.extend_from_slice(&0u32.to_be_bytes());
    assert_eq!(
        StreamMessageId::from_bytes(&envelope(STREAM_MESSAGE_ID_KIND, &invalid_utf8)),
        Err(StreamPositionError::InvalidUtf8)
    );

    let mut oversized_topic = SegmentId(1).0.to_be_bytes().to_vec();
    oversized_topic.extend_from_slice(
        &u32::try_from(MAX_POSITION_TOPIC_SIZE + 1)
            .expect("topic bound fits u32")
            .to_be_bytes(),
    );
    assert_eq!(
        StreamMessageId::from_bytes(&envelope(STREAM_MESSAGE_ID_KIND, &oversized_topic)),
        Err(StreamPositionError::TopicTooLong {
            actual: MAX_POSITION_TOPIC_SIZE + 1,
            max: MAX_POSITION_TOPIC_SIZE,
        })
    );

    let source = source(1, 0, 65_535);
    let mut oversized_ordinary = SegmentId(1).0.to_be_bytes().to_vec();
    oversized_ordinary.extend_from_slice(
        &u32::try_from(source.topic().len())
            .expect("topic length fits u32")
            .to_be_bytes(),
    );
    oversized_ordinary.extend_from_slice(source.topic().as_bytes());
    oversized_ordinary.extend_from_slice(
        &u32::try_from(MAX_ORDINARY_MESSAGE_ID_SIZE + 1)
            .expect("ordinary bound fits u32")
            .to_be_bytes(),
    );
    assert_eq!(
        StreamMessageId::from_bytes(&envelope(STREAM_MESSAGE_ID_KIND, &oversized_ordinary)),
        Err(StreamPositionError::OrdinaryIdTooLong {
            actual: MAX_ORDINARY_MESSAGE_ID_SIZE + 1,
            max: MAX_ORDINARY_MESSAGE_ID_SIZE,
        })
    );

    let ordinary = pb::MessageIdData {
        ledger_id: 1,
        entry_id: 1,
        ..Default::default()
    }
    .encode_to_vec();
    let mut trailing = component(&source, &ordinary);
    trailing.push(0);
    assert!(matches!(
        StreamMessageId::from_bytes(&envelope(STREAM_MESSAGE_ID_KIND, &trailing)),
        Err(StreamPositionError::LengthMismatch { .. })
    ));
}

#[test]
fn position_vector_rejects_duplicate_unsorted_and_oversized_components() {
    let first = source(1, 0, 32_767);
    let second = source(2, 32_768, 65_535);
    assert!(matches!(
        PositionVector::new(
            1,
            [
                (first.clone(), message_id(1)),
                (first.clone(), message_id(2)),
            ],
        ),
        Err(StreamPositionError::DuplicateComponent { .. })
    ));

    let too_many = (0..=MAX_POSITION_COMPONENTS).map(|id| {
        let id = u64::try_from(id).expect("component id fits u64");
        (source(id, 0, 65_535), message_id(id))
    });
    assert_eq!(
        PositionVector::new(1, too_many),
        Err(StreamPositionError::TooManyComponents {
            actual: MAX_POSITION_COMPONENTS + 1,
            max: MAX_POSITION_COMPONENTS,
        })
    );

    let vector = PositionVector::new(1, [(first.clone(), message_id(1)), (second, message_id(2))])
        .expect("two-component vector");
    let encoded = vector.to_bytes().expect("encode vector");
    let payload = &encoded[HEADER_LEN..];
    let first_start = 12;
    let first_end = component_end(payload, first_start);
    let second_end = component_end(payload, first_end);
    let mut reversed_payload = payload[..first_start].to_vec();
    reversed_payload.extend_from_slice(&payload[first_end..second_end]);
    reversed_payload.extend_from_slice(&payload[first_start..first_end]);
    assert_eq!(
        PositionVector::from_bytes(&envelope(POSITION_VECTOR_KIND, &reversed_payload)),
        Err(StreamPositionError::NonCanonicalComponentOrder)
    );

    let mut too_many_payload = 1u64.to_be_bytes().to_vec();
    too_many_payload.extend_from_slice(
        &u32::try_from(MAX_POSITION_COMPONENTS + 1)
            .expect("component bound fits u32")
            .to_be_bytes(),
    );
    assert_eq!(
        PositionVector::from_bytes(&envelope(POSITION_VECTOR_KIND, &too_many_payload)),
        Err(StreamPositionError::TooManyComponents {
            actual: MAX_POSITION_COMPONENTS + 1,
            max: MAX_POSITION_COMPONENTS,
        })
    );
}

#[test]
fn position_size_limits_fail_before_payload_allocation() {
    let parent = format!("topic://t/n/{}", "x".repeat(MAX_POSITION_TOPIC_SIZE));
    let topic = canonical_segment_topic(&parent, KeyRange::FULL, SegmentId(1))
        .expect("canonical long source");
    let oversized_source = SegmentSource::new(SegmentId(1), topic).expect("long source");
    assert!(matches!(
        StreamMessageId::new(oversized_source, message_id(1)),
        Err(StreamPositionError::TopicTooLong { .. })
    ));

    let local_name = "x".repeat(4_000);
    let parent = format!("topic://t/n/{local_name}");
    let components = (0..300u64).map(|id| {
        let topic = canonical_segment_topic(&parent, KeyRange::FULL, SegmentId(id))
            .expect("canonical source");
        let source = SegmentSource::new(SegmentId(id), topic).expect("source");
        (source, message_id(id))
    });
    let vector = PositionVector::new(1, components).expect("bounded component count");
    assert!(matches!(
        vector.encoded_len(),
        Err(StreamPositionError::EnvelopeTooLarge { .. })
    ));
    assert!(matches!(
        vector.to_bytes(),
        Err(StreamPositionError::EnvelopeTooLarge { .. })
    ));
}
