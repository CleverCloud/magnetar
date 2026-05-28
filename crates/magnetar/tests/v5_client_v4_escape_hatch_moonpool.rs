// SPDX-License-Identifier: Apache-2.0
#![cfg(all(feature = "experimental-v5-client", feature = "moonpool"))]

//! PIP-466 V5 client v4-escape-hatch test — moonpool engine mirror.
//!
//! Pins the same structural contract as `v5_client_v4_escape_hatch.rs`
//! (tokio mirror) against the moonpool engine: the V5 wrapper is
//! zero-sized over the v4 client, the `v4()` borrow is unchanged, and
//! `from_v4` / `into_v4` round-trip without loss. The WAVE 3 generic
//! lift means all of these properties hold for any `E: Engine`.

use magnetar::v5::PulsarClientV5;
use magnetar::{MoonpoolEngine, PulsarClient};
use moonpool_core::TokioProviders;

type Mp = MoonpoolEngine<TokioProviders>;

#[test]
fn from_v4_into_v4_round_trips_without_loss_under_moonpool() {
    fn _round_trip(c: PulsarClient<Mp>) -> PulsarClient<Mp> {
        PulsarClientV5::<Mp>::from_v4(c).into_v4()
    }
}

#[test]
fn v4_borrow_returns_inner_reference_under_moonpool() {
    fn _borrow_v4(v5: &PulsarClientV5<Mp>) -> &PulsarClient<Mp> {
        v5.v4()
    }
}

#[test]
fn v5_wrapper_is_zero_sized_over_v4_client_under_moonpool() {
    assert_eq!(
        std::mem::size_of::<PulsarClientV5<Mp>>(),
        std::mem::size_of::<PulsarClient<Mp>>(),
        "PulsarClientV5<MoonpoolEngine<P>> must have the same memory footprint as \
         PulsarClient<MoonpoolEngine<P>> — no parallel state allowed \
         (ADR-0032 escape-hatch contract, WAVE 3 lift)"
    );
}

#[test]
fn v5_wrapper_is_debug_under_moonpool() {
    fn assert_debug<T: std::fmt::Debug>() {}
    assert_debug::<PulsarClientV5<Mp>>();
}
