// SPDX-License-Identifier: Apache-2.0

//! Retry policy for broker-facing setup operations.
//!
//! This policy is deliberately separate from
//! [`SupervisorConfig`](crate::SupervisorConfig): the supervisor redials a
//! failed transport, while operation retry re-issues a command on a healthy
//! or freshly-reconnected transport after a retryable broker reply.

use core::time::Duration;

use crate::backoff::{Backoff, DEFAULT_MANDATORY_STOP};
use crate::pb;

/// Broker-facing setup operation whose error is being classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationKind {
    /// `CommandLookupTopic`.
    Lookup,
    /// `CommandPartitionedTopicMetadata`.
    PartitionedMetadata,
    /// `CommandProducer`.
    ProducerOpen,
    /// `CommandSubscribe`.
    Subscribe,
}

/// Classify a broker error under ADR-0080's operation-specific compatibility policy.
#[must_use]
pub fn is_retryable_broker_error(operation: OperationKind, code: i32) -> bool {
    let Ok(error) = pb::ServerError::try_from(code) else {
        return false;
    };
    let common_retryable = matches!(
        error,
        pb::ServerError::MetadataError
            | pb::ServerError::PersistenceError
            | pb::ServerError::ServiceNotReady
            | pb::ServerError::TooManyRequests
    );
    let producer_quota_retryable = operation == OperationKind::ProducerOpen
        && matches!(
            error,
            pb::ServerError::ProducerBlockedQuotaExceededError
                | pb::ServerError::ProducerBlockedQuotaExceededException
        );
    let busy_retryable = matches!(
        (operation, error),
        (OperationKind::ProducerOpen, pb::ServerError::ProducerBusy)
            | (OperationKind::Subscribe, pb::ServerError::ConsumerBusy)
    );
    common_retryable || producer_quota_retryable || busy_retryable
}

/// Configuration for lookup, partition-metadata, producer-open, and subscribe retries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationRetryConfig {
    /// Initial delay before the first re-issue.
    pub initial_backoff: Duration,
    /// Maximum delay between re-issues.
    pub max_backoff: Duration,
    /// Maximum re-issues after the initial attempt.
    ///
    /// `Some(0)` disables retries. `None` removes the count cap, but runtime
    /// engines still stop at the enclosing operation deadline.
    pub max_retries: Option<u32>,
}

impl Default for OperationRetryConfig {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(8),
            max_retries: Some(crate::conn::MAX_TRANSIENT_OPEN_RETRIES),
        }
    }
}

impl OperationRetryConfig {
    /// Whether a broker failure numbered `failures` should schedule a re-issue.
    ///
    /// `failures` is one-based: the first failed attempt passes `1`.
    #[must_use]
    pub fn should_retry_after_failure(&self, failures: u32) -> bool {
        self.max_retries.is_none_or(|max| failures <= max)
    }

    /// Delay for the one-based failed-attempt number.
    #[must_use]
    pub fn delay_after_failure(&self, failures: u32) -> Duration {
        let steps = failures.max(1);
        let mut backoff = Backoff::new(
            self.initial_backoff.min(self.max_backoff),
            self.max_backoff,
            DEFAULT_MANDATORY_STOP,
            0,
        );
        let mut delay = backoff.next();
        for _ in 1..steps {
            delay = backoff.next();
        }
        delay
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_preserve_the_existing_transient_open_policy() {
        let config = OperationRetryConfig::default();
        assert_eq!(config.initial_backoff, Duration::from_secs(2));
        assert_eq!(config.max_backoff, Duration::from_secs(8));
        assert_eq!(config.max_retries, Some(8));
    }

    #[test]
    fn max_retries_counts_reissues_after_the_initial_attempt() {
        let disabled = OperationRetryConfig {
            max_retries: Some(0),
            ..OperationRetryConfig::default()
        };
        assert!(!disabled.should_retry_after_failure(1));

        let finite = OperationRetryConfig {
            max_retries: Some(2),
            ..OperationRetryConfig::default()
        };
        assert!(finite.should_retry_after_failure(1));
        assert!(finite.should_retry_after_failure(2));
        assert!(!finite.should_retry_after_failure(3));

        let count_unbounded = OperationRetryConfig {
            max_retries: None,
            ..OperationRetryConfig::default()
        };
        assert!(count_unbounded.should_retry_after_failure(u32::MAX));
    }

    #[test]
    fn delay_grows_and_caps_at_the_configured_maximum() {
        let config = OperationRetryConfig::default();
        let first = config.delay_after_failure(1);
        let second = config.delay_after_failure(2);
        let tenth = config.delay_after_failure(10);

        assert!(first >= Duration::from_millis(1_600));
        assert!(first <= Duration::from_secs(2));
        assert!(second >= Duration::from_millis(3_200));
        assert!(second <= Duration::from_secs(4));
        assert!(tenth >= Duration::from_millis(6_400));
        assert!(tenth <= Duration::from_secs(8));
    }

    #[test]
    fn initial_delay_is_clamped_to_the_configured_maximum() {
        let config = OperationRetryConfig {
            initial_backoff: Duration::from_secs(10),
            max_backoff: Duration::from_secs(3),
            max_retries: Some(1),
        };

        assert!(config.delay_after_failure(1) <= Duration::from_secs(3));
    }

    #[test]
    fn compatibility_error_classification_is_operation_specific() {
        for operation in [
            OperationKind::Lookup,
            OperationKind::PartitionedMetadata,
            OperationKind::ProducerOpen,
            OperationKind::Subscribe,
        ] {
            for code in [
                pb::ServerError::MetadataError,
                pb::ServerError::PersistenceError,
                pb::ServerError::ServiceNotReady,
                pb::ServerError::TooManyRequests,
            ] {
                assert!(is_retryable_broker_error(operation, code as i32));
            }
        }

        for quota in [
            pb::ServerError::ProducerBlockedQuotaExceededError,
            pb::ServerError::ProducerBlockedQuotaExceededException,
        ] {
            assert!(is_retryable_broker_error(
                OperationKind::ProducerOpen,
                quota as i32
            ));
            assert!(!is_retryable_broker_error(
                OperationKind::Subscribe,
                quota as i32
            ));
        }

        assert!(is_retryable_broker_error(
            OperationKind::ProducerOpen,
            pb::ServerError::ProducerBusy as i32
        ));
        assert!(!is_retryable_broker_error(
            OperationKind::Subscribe,
            pb::ServerError::ProducerBusy as i32
        ));
        assert!(is_retryable_broker_error(
            OperationKind::Subscribe,
            pb::ServerError::ConsumerBusy as i32
        ));
        assert!(!is_retryable_broker_error(
            OperationKind::ProducerOpen,
            pb::ServerError::ConsumerBusy as i32
        ));

        for terminal in [
            pb::ServerError::AuthorizationError,
            pb::ServerError::TopicNotFound,
            pb::ServerError::InvalidTopicName,
            pb::ServerError::IncompatibleSchema,
            pb::ServerError::NotAllowedError,
            pb::ServerError::ProducerFenced,
        ] {
            assert!(!is_retryable_broker_error(
                OperationKind::ProducerOpen,
                terminal as i32
            ));
        }
    }
}
