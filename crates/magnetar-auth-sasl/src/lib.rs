// SPDX-License-Identifier: Apache-2.0

//! SASL auth provider for magnetar.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

/// Placeholder SASL provider. Implementation lands in M6.
#[derive(Debug, Default)]
pub struct SaslProvider {
    _private: (),
}

#[cfg(test)]
mod tests {
    use super::SaslProvider;

    #[test]
    fn provider_compiles() {
        let _ = SaslProvider::default();
    }
}
