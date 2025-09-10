//! Retry logic and backoff strategies

use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};
use crate::config::RetryConfig;

/// Retry policy trait
pub trait RetryPolicy: Send + Sync {
    fn should_retry(&self, attempt: u32, error: &dyn std::error::Error) -> bool;
    fn delay(&self, attempt: u32) -> Duration;
    fn max_attempts(&self) -> u32;
}

/// Exponential backoff retry policy
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    config: RetryConfig,
    max_attempts: u32,
}

impl ExponentialBackoff {
    pub fn new(config: RetryConfig, max_attempts: u32) -> Self {
        Self {
            config,
            max_attempts,
        }
    }

    pub fn from_config(config: RetryConfig) -> Self {
        Self::new(config, 5) // Default max attempts
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn should_retry(&self, attempt: u32, _error: &dyn std::error::Error) -> bool {
        attempt < self.max_attempts
    }

    fn delay(&self, attempt: u32) -> Duration {
        let base_delay = Duration::from_millis(self.config.initial_delay_ms);
        let exponential_delay = base_delay.mul_f64(self.config.multiplier.powi(attempt as i32));
        
        let delay_ms = exponential_delay
            .as_millis()
            .min(self.config.max_delay_ms as u128) as u64;
        
        // Add jitter
        let jitter = fastrand::u64(0..=self.config.jitter_ms);
        Duration::from_millis(delay_ms + jitter)
    }

    fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}

/// Fixed interval retry policy
#[derive(Debug, Clone)]
pub struct FixedInterval {
    interval: Duration,
    max_attempts: u32,
}

impl FixedInterval {
    pub fn new(interval: Duration, max_attempts: u32) -> Self {
        Self {
            interval,
            max_attempts,
        }
    }
}

impl RetryPolicy for FixedInterval {
    fn should_retry(&self, attempt: u32, _error: &dyn std::error::Error) -> bool {
        attempt < self.max_attempts
    }

    fn delay(&self, _attempt: u32) -> Duration {
        self.interval
    }

    fn max_attempts(&self) -> u32 {
        self.max_attempts
    }
}

/// No retry policy
#[derive(Debug, Clone)]
pub struct NoRetry;

impl RetryPolicy for NoRetry {
    fn should_retry(&self, _attempt: u32, _error: &dyn std::error::Error) -> bool {
        false
    }

    fn delay(&self, _attempt: u32) -> Duration {
        Duration::ZERO
    }

    fn max_attempts(&self) -> u32 {
        1
    }
}

/// Retry executor
pub struct RetryExecutor<P: RetryPolicy> {
    policy: P,
}

impl<P: RetryPolicy> RetryExecutor<P> {
    pub fn new(policy: P) -> Self {
        Self { policy }
    }

    /// Execute a function with retry logic
    pub async fn execute<F, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
        E: std::error::Error + Send + 'static,
    {
        let mut attempt = 0;

        loop {
            match operation().await {
                Ok(result) => {
                    if attempt > 0 {
                        debug!("Operation succeeded after {} attempts", attempt + 1);
                    }
                    return Ok(result);
                }
                Err(error) => {
                    if !self.policy.should_retry(attempt, &error) {
                        warn!(
                            "Operation failed after {} attempts: {}",
                            attempt + 1,
                            error
                        );
                        return Err(error);
                    }

                    let delay = self.policy.delay(attempt);
                    warn!(
                        "Operation failed (attempt {}), retrying in {:?}: {}",
                        attempt + 1,
                        delay,
                        error
                    );
                    
                    sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }

    /// Execute a function with retry and timeout
    pub async fn execute_with_timeout<F, T, E>(
        &self,
        operation: F,
        timeout: Duration,
    ) -> Result<T, RetryError<E>>
    where
        F: FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
        E: std::error::Error + Send + 'static,
    {
        match tokio::time::timeout(timeout, self.execute(operation)).await {
            Ok(result) => result.map_err(RetryError::Operation),
            Err(_) => Err(RetryError::Timeout),
        }
    }
}

/// Retry-specific errors
#[derive(Debug, thiserror::Error)]
pub enum RetryError<E> {
    #[error("Operation failed: {0}")]
    Operation(E),
    
    #[error("Operation timed out")]
    Timeout,
}

/// Convenience function to create an exponential backoff executor
pub fn exponential_backoff(config: RetryConfig, max_attempts: u32) -> RetryExecutor<ExponentialBackoff> {
    RetryExecutor::new(ExponentialBackoff::new(config, max_attempts))
}

/// Convenience function to create a fixed interval executor
pub fn fixed_interval(interval: Duration, max_attempts: u32) -> RetryExecutor<FixedInterval> {
    RetryExecutor::new(FixedInterval::new(interval, max_attempts))
}

/// Convenience function to create a no-retry executor
pub fn no_retry() -> RetryExecutor<NoRetry> {
    RetryExecutor::new(NoRetry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[derive(Debug, thiserror::Error)]
    #[error("Test error")]
    struct TestError;

    #[tokio::test]
    async fn test_exponential_backoff_success_after_retries() {
        let config = RetryConfig {
            initial_delay_ms: 10,
            max_delay_ms: 1000,
            multiplier: 2.0,
            jitter_ms: 5,
        };
        
        let executor = RetryExecutor::new(ExponentialBackoff::new(config, 3));
        let attempt_count = Arc::new(AtomicU32::new(0));
        
        let attempt_count_clone = attempt_count.clone();
        let result = executor
            .execute(move || {
                let attempt_count = attempt_count_clone.clone();
                Box::pin(async move {
                    let current_attempt = attempt_count.fetch_add(1, Ordering::Relaxed);
                    if current_attempt < 2 {
                        Err(TestError)
                    } else {
                        Ok("success")
                    }
                })
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(attempt_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn test_fixed_interval() {
        let executor = RetryExecutor::new(FixedInterval::new(Duration::from_millis(10), 2));
        let attempt_count = Arc::new(AtomicU32::new(0));
        
        let attempt_count_clone = attempt_count.clone();
        let result = executor
            .execute(move || {
                let attempt_count = attempt_count_clone.clone();
                Box::pin(async move {
                    attempt_count.fetch_add(1, Ordering::Relaxed);
                    Err(TestError)
                })
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempt_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn test_no_retry() {
        let executor = RetryExecutor::new(NoRetry);
        let attempt_count = Arc::new(AtomicU32::new(0));
        
        let attempt_count_clone = attempt_count.clone();
        let result = executor
            .execute(move || {
                let attempt_count = attempt_count_clone.clone();
                Box::pin(async move {
                    attempt_count.fetch_add(1, Ordering::Relaxed);
                    Err(TestError)
                })
            })
            .await;

        assert!(result.is_err());
        assert_eq!(attempt_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_timeout() {
        let executor = RetryExecutor::new(NoRetry);
        let result = executor
            .execute_with_timeout(
                move || {
                    Box::pin(async move {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        Ok("success")
                    })
                },
                Duration::from_millis(50),
            )
            .await;

        assert!(matches!(result, Err(RetryError::Timeout)));
    }
}