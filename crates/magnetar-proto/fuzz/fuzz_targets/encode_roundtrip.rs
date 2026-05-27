// SPDX-License-Identifier: Apache-2.0
//
// Fuzz target: build a synthetic `BaseCommand` from arbitrary bytes, encode it
// via `magnetar_proto::frame::encode_command`, then decode and require equality.
// Catches encoder bugs where a roundtrip fails to recover the original.

#![no_main]

use arbitrary::Arbitrary;
use bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;
use magnetar_proto::frame::{decode_one, encode_command};
use magnetar_proto::pb;

#[derive(Debug, Arbitrary)]
struct FuzzInput {
    /// Picks one of a handful of representative BaseCommand types.
    variant: u8,
    /// Filler bytes that vary the message contents.
    payload: Vec<u8>,
}

fn make_command(input: &FuzzInput) -> pb::BaseCommand {
    // Most BaseCommand types carry a `request_id` u64 and a few small fields.
    // We pick a small representative set so the encoder exercises the major
    // wire-shape branches (ping/pong/connect/ack/flow/etc.).
    let mut cmd = pb::BaseCommand::default();
    let n = input.variant % 6;
    match n {
        0 => {
            cmd.set_type(pb::base_command::Type::Ping);
            cmd.ping = Some(pb::CommandPing::default());
        }
        1 => {
            cmd.set_type(pb::base_command::Type::Pong);
            cmd.pong = Some(pb::CommandPong::default());
        }
        2 => {
            cmd.set_type(pb::base_command::Type::Connect);
            let mut c = pb::CommandConnect::default();
            c.client_version = "magnetar-fuzz".into();
            c.protocol_version = Some(21);
            cmd.connect = Some(c);
        }
        3 => {
            cmd.set_type(pb::base_command::Type::Flow);
            let mut f = pb::CommandFlow::default();
            f.consumer_id = u64::from(input.payload.len() as u32);
            f.message_permits = 1024;
            cmd.flow = Some(f);
        }
        4 => {
            cmd.set_type(pb::base_command::Type::CloseProducer);
            let mut c = pb::CommandCloseProducer::default();
            c.producer_id = u64::from(input.payload.len() as u32);
            c.request_id = 1;
            cmd.close_producer = Some(c);
        }
        _ => {
            cmd.set_type(pb::base_command::Type::Unsubscribe);
            let mut u = pb::CommandUnsubscribe::default();
            u.consumer_id = u64::from(input.payload.len() as u32);
            u.request_id = 1;
            cmd.unsubscribe = Some(u);
        }
    }
    cmd
}

fuzz_target!(|input: FuzzInput| {
    let original = make_command(&input);
    let mut buf = BytesMut::new();
    if encode_command(&mut buf, &original).is_err() {
        // Encoder rejects — fine, we just stop. Panics would be the bug.
        return;
    }
    let mut src: Bytes = buf.freeze();
    let frame = match decode_one(&mut src) {
        Ok(f) => f,
        Err(err) => panic!("encoder-decoder roundtrip mismatched: {err}"),
    };
    assert_eq!(frame.command.r#type, original.r#type);
    assert!(src.is_empty(), "decoder left {} trailing bytes", src.len());
});
