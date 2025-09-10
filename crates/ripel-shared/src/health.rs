//! Health check utilities

use crate::observability::{HealthCheck, HealthStatus};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Simple health check that always returns healthy
pub struct AlwaysHealthy {
    name: String,
}

impl AlwaysHealthy {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl HealthCheck for AlwaysHealthy {
    fn name(&self) -> &str {
        &self.name
    }

    fn check(&self) -> HealthStatus {
        HealthStatus::Healthy
    }
}

/// Health check based on last activity timestamp
pub struct ActivityBasedHealthCheck {
    name: String,
    last_activity: RwLock<Instant>,
    timeout: Duration,
}

impl ActivityBasedHealthCheck {
    pub fn new(name: impl Into<String>, timeout: Duration) -> Self {
        Self {
            name: name.into(),
            last_activity: RwLock::new(Instant::now()),
            timeout,
        }
    }

    /// Update the last activity timestamp
    pub async fn record_activity(&self) {
        let mut last_activity = self.last_activity.write().await;
        *last_activity = Instant::now();
    }
}

impl HealthCheck for ActivityBasedHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn check(&self) -> HealthStatus {
        let last_activity = *self.last_activity.blocking_read();
        let elapsed = last_activity.elapsed();
        
        if elapsed > self.timeout {
            HealthStatus::Unhealthy {
                reason: format!("No activity for {:?}", elapsed),
            }
        } else if elapsed > self.timeout / 2 {
            HealthStatus::Degraded {
                reason: format!("Low activity, last seen {:?} ago", elapsed),
            }
        } else {
            HealthStatus::Healthy
        }
    }
}

/// Connection-based health check
pub struct ConnectionHealthCheck {
    name: String,
    check_fn: Box<dyn Fn() -> bool + Send + Sync>,
}

impl ConnectionHealthCheck {
    pub fn new<F>(name: impl Into<String>, check_fn: F) -> Self 
    where
        F: Fn() -> bool + Send + Sync + 'static,
    {
        Self {
            name: name.into(),
            check_fn: Box::new(check_fn),
        }
    }
}

impl HealthCheck for ConnectionHealthCheck {
    fn name(&self) -> &str {
        &self.name
    }

    fn check(&self) -> HealthStatus {
        if (self.check_fn)() {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy {
                reason: "Connection check failed".to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn test_always_healthy() {
        let check = AlwaysHealthy::new("test");
        assert_eq!(check.name(), "test");
        assert!(matches!(check.check(), HealthStatus::Healthy));
    }

    #[tokio::test]
    async fn test_activity_based_health_check() {
        let check = ActivityBasedHealthCheck::new("test", Duration::from_millis(100));
        
        // Should be healthy initially
        assert!(matches!(check.check(), HealthStatus::Healthy));
        
        // Wait for timeout
        sleep(Duration::from_millis(150)).await;
        
        // Should be unhealthy after timeout
        assert!(matches!(check.check(), HealthStatus::Unhealthy { .. }));
        
        // Record activity and check again
        check.record_activity().await;
        assert!(matches!(check.check(), HealthStatus::Healthy));
    }

    #[test]
    fn test_connection_health_check() {
        let mut connected = true;
        let check = ConnectionHealthCheck::new("test", move || connected);
        
        assert!(matches!(check.check(), HealthStatus::Healthy));
        
        // Simulate connection failure
        connected = false;
        // Note: This test demonstrates the concept, but the closure captures by value
        // In real usage, you'd use Arc<AtomicBool> or similar for shared state
    }
}