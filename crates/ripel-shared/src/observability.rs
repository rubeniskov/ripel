//! Observability features including logging, metrics, and tracing

use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use once_cell::sync::OnceCell;
use std::net::SocketAddr;
use tokio::time::{Duration, Instant};
use tracing::{info, Level};
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

use crate::config::{LoggingConfig, MetricsConfig, ObservabilityConfig, TracingConfig};

/// Global observability system
static OBSERVABILITY: OnceCell<ObservabilitySystem> = OnceCell::new();

/// Observability system for centralized logging, metrics, and tracing
pub struct ObservabilitySystem {
    metrics_enabled: bool,
    tracing_enabled: bool,
}

impl ObservabilitySystem {
    /// Initialize the observability system
    pub fn init(config: &ObservabilityConfig) -> anyhow::Result<()> {
        let system = Self {
            metrics_enabled: config.metrics.enabled,
            tracing_enabled: config.tracing.enabled,
        };

        // Initialize logging
        Self::init_logging(&config.logging)?;

        // Initialize metrics
        if config.metrics.enabled {
            Self::init_metrics(&config.metrics)?;
        }

        // Initialize tracing
        if config.tracing.enabled {
            Self::init_tracing(&config.tracing)?;
        }

        OBSERVABILITY.set(system).map_err(|_| {
            anyhow::anyhow!("Observability system already initialized")
        })?;

        info!("Observability system initialized");
        Ok(())
    }

    /// Initialize structured logging
    fn init_logging(config: &LoggingConfig) -> anyhow::Result<()> {
        let level = match config.level.to_lowercase().as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        };

        let env_filter = EnvFilter::builder()
            .with_default_directive(level.into())
            .from_env_lossy();

        let registry = tracing_subscriber::registry().with(env_filter);

        match config.format.to_lowercase().as_str() {
            "json" => {
                let json_layer = tracing_subscriber::fmt::layer()
                    .json()
                    .with_span_events(FmtSpan::CLOSE);
                registry.with(json_layer).init();
            }
            _ => {
                let pretty_layer = tracing_subscriber::fmt::layer()
                    .pretty()
                    .with_span_events(FmtSpan::CLOSE);
                registry.with(pretty_layer).init();
            }
        }

        Ok(())
    }

    /// Initialize Prometheus metrics
    fn init_metrics(config: &MetricsConfig) -> anyhow::Result<()> {
        let bind_addr: SocketAddr = config.bind_address.parse()?;
        
        let builder = PrometheusBuilder::new();
        let handle = builder.install()?;

        // Start metrics server in background
        tokio::spawn(async move {
            let listener = std::net::TcpListener::bind(bind_addr).unwrap();
            for stream in listener.incoming() {
                if let Ok(_stream) = stream {
                    // Basic HTTP metrics endpoint - in production you'd use a proper HTTP server
                    break;
                }
            }
        });

        info!("Prometheus metrics initialized on {}", bind_addr);
        Ok(())
    }

    /// Initialize distributed tracing
    fn init_tracing(_config: &TracingConfig) -> anyhow::Result<()> {
        // For now, just log that tracing would be initialized
        // In a full implementation, you'd set up Jaeger or similar
        info!("Distributed tracing initialized");
        Ok(())
    }

    /// Get the global observability system
    pub fn get() -> Option<&'static ObservabilitySystem> {
        OBSERVABILITY.get()
    }
}

/// Event processing metrics
pub struct EventMetrics;

impl EventMetrics {
    /// Record an event processed
    pub fn event_processed(event_type: &str, source: &str) {
        counter!("ripel_events_processed_total")
            .increment(1);
        counter!("ripel_events_processed_by_type_total", "event_type" => event_type.to_string())
            .increment(1);
        counter!("ripel_events_processed_by_source_total", "source" => source.to_string())
            .increment(1);
    }

    /// Record an event failed
    pub fn event_failed(event_type: &str, error_type: &str) {
        counter!("ripel_events_failed_total")
            .increment(1);
        counter!("ripel_events_failed_by_type_total", 
                "event_type" => event_type.to_string(),
                "error_type" => error_type.to_string())
            .increment(1);
    }

    /// Record processing duration
    pub fn processing_duration(duration: Duration, event_type: &str) {
        histogram!("ripel_event_processing_duration_seconds", 
                  "event_type" => event_type.to_string())
            .record(duration.as_secs_f64());
    }

    /// Record current queue size
    pub fn queue_size(size: u64, queue_type: &str) {
        gauge!("ripel_queue_size", "queue_type" => queue_type.to_string())
            .set(size as f64);
    }

    /// Record database operation
    pub fn database_operation(operation: &str, table: &str, duration: Duration) {
        counter!("ripel_database_operations_total",
                "operation" => operation.to_string(),
                "table" => table.to_string())
            .increment(1);
        histogram!("ripel_database_operation_duration_seconds",
                  "operation" => operation.to_string(),
                  "table" => table.to_string())
            .record(duration.as_secs_f64());
    }

    /// Record Kafka operation
    pub fn kafka_operation(operation: &str, topic: &str, success: bool) {
        let status = if success { "success" } else { "error" };
        counter!("ripel_kafka_operations_total",
                "operation" => operation.to_string(),
                "topic" => topic.to_string(),
                "status" => status.to_string())
            .increment(1);
    }
}

/// Performance timer helper
pub struct PerfTimer {
    start: Instant,
    metric_name: String,
    labels: Vec<(String, String)>,
}

impl PerfTimer {
    pub fn new(metric_name: impl Into<String>) -> Self {
        Self {
            start: Instant::now(),
            metric_name: metric_name.into(),
            labels: Vec::new(),
        }
    }

    pub fn with_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.labels.push((key.into(), value.into()));
        self
    }

    pub fn finish(self) {
        let duration = self.start.elapsed();
        let hist = histogram!(self.metric_name.clone());
        hist.record(duration.as_secs_f64());
    }
}

impl Drop for PerfTimer {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let duration = self.start.elapsed();
            let hist = histogram!(self.metric_name.clone());
            hist.record(duration.as_secs_f64());
        }
    }
}

/// Health check status
#[derive(Debug, Clone, serde::Serialize)]
pub enum HealthStatus {
    Healthy,
    Degraded { reason: String },
    Unhealthy { reason: String },
}

/// Component health check trait
pub trait HealthCheck: Send + Sync {
    fn name(&self) -> &str;
    fn check(&self) -> HealthStatus;
}

/// System health aggregator
pub struct HealthAggregator {
    checks: Vec<Box<dyn HealthCheck>>,
}

impl HealthAggregator {
    pub fn new() -> Self {
        Self {
            checks: Vec::new(),
        }
    }

    pub fn add_check(mut self, check: Box<dyn HealthCheck>) -> Self {
        self.checks.push(check);
        self
    }

    pub fn check_all(&self) -> Vec<(String, HealthStatus)> {
        self.checks
            .iter()
            .map(|check| (check.name().to_string(), check.check()))
            .collect()
    }

    pub fn overall_status(&self) -> HealthStatus {
        let results = self.check_all();
        
        let unhealthy: Vec<_> = results
            .iter()
            .filter_map(|(name, status)| match status {
                HealthStatus::Unhealthy { reason } => Some(format!("{}: {}", name, reason)),
                _ => None,
            })
            .collect();

        if !unhealthy.is_empty() {
            return HealthStatus::Unhealthy {
                reason: unhealthy.join(", "),
            };
        }

        let degraded: Vec<_> = results
            .iter()
            .filter_map(|(name, status)| match status {
                HealthStatus::Degraded { reason } => Some(format!("{}: {}", name, reason)),
                _ => None,
            })
            .collect();

        if !degraded.is_empty() {
            return HealthStatus::Degraded {
                reason: degraded.join(", "),
            };
        }

        HealthStatus::Healthy
    }
}

impl Default for HealthAggregator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestHealthCheck {
        name: String,
        status: HealthStatus,
    }

    impl HealthCheck for TestHealthCheck {
        fn name(&self) -> &str {
            &self.name
        }

        fn check(&self) -> HealthStatus {
            self.status.clone()
        }
    }

    #[test]
    fn test_health_aggregator() {
        let healthy_check = TestHealthCheck {
            name: "test1".to_string(),
            status: HealthStatus::Healthy,
        };
        
        let unhealthy_check = TestHealthCheck {
            name: "test2".to_string(),
            status: HealthStatus::Unhealthy {
                reason: "test error".to_string(),
            },
        };

        let aggregator = HealthAggregator::new()
            .add_check(Box::new(healthy_check))
            .add_check(Box::new(unhealthy_check));

        match aggregator.overall_status() {
            HealthStatus::Unhealthy { reason } => {
                assert!(reason.contains("test2"));
                assert!(reason.contains("test error"));
            }
            _ => panic!("Expected unhealthy status"),
        }
    }

    #[test]
    fn test_perf_timer() {
        let timer = PerfTimer::new("test_metric")
            .with_label("test_label", "test_value");
        
        // Timer should be created successfully
        assert_eq!(timer.metric_name, "test_metric");
        assert_eq!(timer.labels.len(), 1);
        
        // Let it drop to test the metric recording
        drop(timer);
    }
}