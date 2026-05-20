// SPDX-License-Identifier: Apache-2.0

//! Athenz auth provider for magnetar.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

/// Placeholder Athenz provider. Implementation lands in M6.
#[derive(Debug, Default)]
pub struct AthenzProvider {
    _private: (),
}

#[cfg(test)]
mod tests {
    use super::AthenzProvider;

    #[test]
    fn provider_compiles() {
        let _ = AthenzProvider::default();
    }
}
