// SPDX-License-Identifier: Apache-2.0

//! In-process Pulsar broker fake — frame-in / frame-out, with per-command
//! hooks for fault injection.
//!
//! Mirrors the Java `MockBrokerService` design (`apache/pulsar`
//! `pulsar-broker/src/test/java/.../MockBrokerService.java`): a sans-io broker
//! that takes client frames in and emits responses out. Use it from
//! `magnetar-proto/tests/` and from runtime integration tests to validate
//! client behavior against scripted broker scenarios.
//!
//! Real implementation lands in M2 alongside the sans-io state machine.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

/// Placeholder broker fake.
#[derive(Debug, Default)]
pub struct BrokerFake {
    _private: (),
}

impl BrokerFake {
    /// Construct an idle broker fake.
    pub fn new() -> Self {
        Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::BrokerFake;

    #[test]
    fn fake_can_be_constructed() {
        let _ = BrokerFake::new();
    }
}
