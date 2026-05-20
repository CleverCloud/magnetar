// SPDX-License-Identifier: Apache-2.0

//! `OAuth2` `ClientCredentialsFlow` auth provider for magnetar.

#![warn(unreachable_pub)]
#![forbid(unsafe_code)]

/// Placeholder `OAuth2` auth provider. Implementation lands in M6.
#[derive(Debug, Default)]
pub struct OAuth2Provider {
    _private: (),
}

#[cfg(test)]
mod tests {
    use super::OAuth2Provider;

    #[test]
    fn provider_compiles() {
        let _ = OAuth2Provider::default();
    }
}
