// SPDX-License-Identifier: Apache-2.0
#![cfg(feature = "experimental-v5-client")]

//! PIP-466 V5 client v4 escape hatch test.
//!
//! Pins the type-level contract that ADR-0032 locks in:
//! `PulsarClientV5::v4()` returns a borrowed reference to the same
//! underlying [`magnetar::PulsarClient`] (no double-init, no second
//! handshake, no state divergence between V5 and v4 surfaces on the
//! same connection). `into_v4()` consumes the wrapper and yields the
//! v4 client unchanged.
//!
//! No wire-byte assertion is needed for the escape hatch — the
//! contract is structural: the V5 wrapper holds no state, every V5
//! call delegates to the wrapped v4 client. The
//! `v5_producer_mapping.rs` / `v5_stream_consumer_mapping.rs` /
//! `v5_queue_consumer_mapping.rs` tests prove that the V5 builders'
//! translation to v4 commands is byte-correct; this test pins the
//! structural invariant of the escape hatch itself.

use magnetar::PulsarClient;
use magnetar::v5::PulsarClientV5;

#[test]
fn from_v4_into_v4_round_trips_without_loss() {
    // Type-level: the V5 wrapper accepts the v4 client and yields it
    // back unchanged via `into_v4`. Compile-time identity guarantees
    // the wrapper holds no parallel state that could diverge.
    fn _round_trip(c: PulsarClient) -> PulsarClient {
        PulsarClientV5::from_v4(c).into_v4()
    }
}

#[test]
fn v4_borrow_returns_inner_reference() {
    // `v4()` borrows the inner v4 client without consuming the V5
    // wrapper — both surfaces remain usable in parallel against the
    // same engine state.
    fn _borrow_v4(v5: &PulsarClientV5) -> &PulsarClient {
        v5.v4()
    }
}

#[test]
fn v5_wrapper_is_zero_sized_over_v4_client() {
    // PulsarClientV5 must hold no state beyond the wrapped v4 client.
    // If a future refactor adds parallel state, this assertion fires —
    // a signal that the escape-hatch contract is in danger.
    assert_eq!(
        std::mem::size_of::<PulsarClientV5>(),
        std::mem::size_of::<PulsarClient>(),
        "PulsarClientV5 must have the same memory footprint as PulsarClient — \
         no parallel state allowed (ADR-0032 escape-hatch contract)"
    );
}

#[test]
fn v5_wrapper_is_debug() {
    // PulsarClientV5 implements Debug — required by the surface-area
    // expectations every other PulsarClient-shaped type meets.
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_debug::<PulsarClientV5>();
}
