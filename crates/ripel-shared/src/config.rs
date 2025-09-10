//! Configuration management for RIPeL components

use config::{Config, ConfigError, Environment, File};
use serde::{Deserialize, Serialize};
use std::path::Path;

/// Main configuration structure for RIPeL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RipelConfig {
    /// Database configuration
    pub database: DatabaseConfig,
    
    /// Kafka configuration
    pub kafka: KafkaConfig,
    
    /// gRPC server configuration
    pub grpc: GrpcConfig,
    
    /// Observability configuration
    pub observability: ObservabilityConfig,
    
    /// Processing configuration
    pub processing: ProcessingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// MySQL connection URL
    pub url: String,
    
    /// Maximum number of connections in the pool
    pub max_connections: u32,
    
    /// Connection timeout in seconds
    pub connection_timeout: u64,
    
    /// Idle timeout in seconds
    pub idle_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Kafka broker addresses
    pub brokers: Vec<String>,
    
    /// Client ID for Kafka connections
    pub client_id: String,
    
    /// Default topic for events
    pub default_topic: String,
    
    /// Dead letter queue topic
    pub dlq_topic: String,
    
    /// Producer configuration
    pub producer: KafkaProducerConfig,
    
    /// Consumer configuration
    pub consumer: KafkaConsumerConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaProducerConfig {
    /// Acknowledgment level (0, 1, or all)
    pub acks: String,
    
    /// Maximum time to wait for acknowledgments (ms)
    pub request_timeout_ms: u32,
    
    /// Batch size for batching messages
    pub batch_size: u32,
    
    /// Enable compression
    pub compression_type: String,
    
    /// Maximum retry attempts
    pub retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConsumerConfig {
    /// Consumer group ID
    pub group_id: String,
    
    /// Auto offset reset policy
    pub auto_offset_reset: String,
    
    /// Enable auto commit
    pub enable_auto_commit: bool,
    
    /// Session timeout (ms)
    pub session_timeout_ms: u32,
    
    /// Maximum poll records
    pub max_poll_records: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcConfig {
    /// Server bind address
    pub bind_address: String,
    
    /// Maximum message size
    pub max_message_size: usize,
    
    /// Request timeout in seconds
    pub timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Logging configuration
    pub logging: LoggingConfig,
    
    /// Metrics configuration
    pub metrics: MetricsConfig,
    
    /// Tracing configuration
    pub tracing: TracingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error)
    pub level: String,
    
    /// Log format (json or pretty)
    pub format: String,
    
    /// Enable logging to file
    pub file_enabled: bool,
    
    /// Log file path
    pub file_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics collection
    pub enabled: bool,
    
    /// Prometheus metrics bind address
    pub bind_address: String,
    
    /// Metrics collection interval in seconds
    pub collection_interval: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracingConfig {
    /// Enable distributed tracing
    pub enabled: bool,
    
    /// Jaeger endpoint
    pub jaeger_endpoint: Option<String>,
    
    /// Sampling rate (0.0 to 1.0)
    pub sampling_rate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingConfig {
    /// Number of worker threads
    pub worker_count: usize,
    
    /// Event buffer size
    pub buffer_size: usize,
    
    /// Batch size for batch processing
    pub batch_size: usize,
    
    /// Processing timeout in seconds
    pub timeout: u64,
    
    /// Maximum retry attempts
    pub max_retries: u32,
    
    /// Retry backoff configuration
    pub retry_backoff: RetryConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Initial delay in milliseconds
    pub initial_delay_ms: u64,
    
    /// Maximum delay in milliseconds
    pub max_delay_ms: u64,
    
    /// Backoff multiplier
    pub multiplier: f64,
    
    /// Maximum jitter in milliseconds
    pub jitter_ms: u64,
}

impl Default for RipelConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig {
                url: "mysql://root:password@localhost:3306/ripel".to_string(),
                max_connections: 10,
                connection_timeout: 30,
                idle_timeout: 600,
            },
            kafka: KafkaConfig {
                brokers: vec!["localhost:9092".to_string()],
                client_id: "ripel-client".to_string(),
                default_topic: "ripel-events".to_string(),
                dlq_topic: "ripel-dlq".to_string(),
                producer: KafkaProducerConfig {
                    acks: "all".to_string(),
                    request_timeout_ms: 30000,
                    batch_size: 16384,
                    compression_type: "snappy".to_string(),
                    retries: 3,
                },
                consumer: KafkaConsumerConfig {
                    group_id: "ripel-consumer".to_string(),
                    auto_offset_reset: "latest".to_string(),
                    enable_auto_commit: false,
                    session_timeout_ms: 30000,
                    max_poll_records: 500,
                },
            },
            grpc: GrpcConfig {
                bind_address: "0.0.0.0:50051".to_string(),
                max_message_size: 4 * 1024 * 1024, // 4MB
                timeout: 30,
            },
            observability: ObservabilityConfig {
                logging: LoggingConfig {
                    level: "info".to_string(),
                    format: "json".to_string(),
                    file_enabled: false,
                    file_path: None,
                },
                metrics: MetricsConfig {
                    enabled: true,
                    bind_address: "0.0.0.0:9090".to_string(),
                    collection_interval: 10,
                },
                tracing: TracingConfig {
                    enabled: false,
                    jaeger_endpoint: None,
                    sampling_rate: 0.1,
                },
            },
            processing: ProcessingConfig {
                worker_count: 4,
                buffer_size: 1000,
                batch_size: 100,
                timeout: 30,
                max_retries: 3,
                retry_backoff: RetryConfig {
                    initial_delay_ms: 1000,
                    max_delay_ms: 60000,
                    multiplier: 2.0,
                    jitter_ms: 500,
                },
            },
        }
    }
}

impl RipelConfig {
    /// Load configuration from file and environment variables
    pub fn load() -> Result<Self, ConfigError> {
        Self::load_from_file("config.toml")
    }
    
    /// Load configuration from a specific file
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let mut builder = Config::builder()
            .add_source(Config::try_from(&RipelConfig::default())?)
            .add_source(Environment::with_prefix("RIPEL").separator("__"));
            
        if path.as_ref().exists() {
            builder = builder.add_source(File::from(path.as_ref()));
        }
        
        builder.build()?.try_deserialize()
    }
    
    /// Load configuration from environment variables only
    pub fn load_from_env() -> Result<Self, ConfigError> {
        Config::builder()
            .add_source(Config::try_from(&RipelConfig::default())?)
            .add_source(Environment::with_prefix("RIPEL").separator("__"))
            .build()?
            .try_deserialize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RipelConfig::default();
        assert_eq!(config.database.max_connections, 10);
        assert_eq!(config.kafka.brokers.len(), 1);
        assert_eq!(config.processing.worker_count, 4);
    }

    #[test]
    fn test_config_serialization() {
        let config = RipelConfig::default();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: RipelConfig = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(config.database.max_connections, deserialized.database.max_connections);
    }
}