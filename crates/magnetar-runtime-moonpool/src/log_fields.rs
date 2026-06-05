// SPDX-License-Identifier: Apache-2.0

//! Log-field sanitisation helpers (ADR-0054).
//!
//! Broker-supplied strings (server error messages, redirect URLs, close
//! reasons, broker-requested auth method names) are hostile-peer-controlled
//! input. Before such a string lands in a structured `tracing` field it is
//! truncated to a fixed budget — a log-injection and cardinality defence
//! mirroring sozu's render-time sanitisation. Kept per-crate (duplicated
//! 1:1 in `magnetar-runtime-tokio`) rather than exported from
//! `magnetar-proto` so the engine slice does not touch the proto crate's
//! public API (its own copy lives at the proto detection sites).

/// Maximum length, in bytes, of a broker-supplied string embedded in a log
/// field (ADR-0054 §broker-controlled string sanitisation).
const MAX_BROKER_STR: usize = 256;

/// Truncate a broker-supplied string to [`MAX_BROKER_STR`] bytes for use as
/// a structured log field, backing off to the previous `char` boundary so
/// the slice stays valid UTF-8. Returns the input unchanged when it already
/// fits.
pub(crate) fn truncate_broker_str(s: &str) -> &str {
    if s.len() <= MAX_BROKER_STR {
        return s;
    }
    let mut end = MAX_BROKER_STR;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

#[cfg(test)]
mod tests {
    use super::truncate_broker_str;

    #[test]
    fn short_strings_pass_through() {
        assert_eq!(
            truncate_broker_str("pulsar://broker:6650"),
            "pulsar://broker:6650"
        );
    }

    #[test]
    fn long_strings_are_truncated_to_budget() {
        let long = "x".repeat(1000);
        assert_eq!(truncate_broker_str(&long).len(), 256);
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        // 'é' is 2 bytes; 200 of them = 400 bytes, with byte 256 falling
        // mid-char. The helper must back off to a valid boundary.
        let long = "é".repeat(200);
        let cut = truncate_broker_str(&long);
        assert!(cut.len() <= 256);
        assert!(long.is_char_boundary(cut.len()));
    }
}
