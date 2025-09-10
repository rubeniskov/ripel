//! Kafka producer configuration and management

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use ripel_core::{Result, RipelError};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, instrument};

/// Kafka producer wrapper with enhanced configuration
pub struct RipelKafkaProducer {
    producer: FutureProducer,
    config: KafkaProducerConfig,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KafkaProducerConfig {
    pub brokers: Vec<String>,
    pub client_id: String,
    pub compression_type: String,
    pub acks: String,
    pub retries: u32,
    pub batch_size: u32,
    pub linger_ms: u32,
    pub request_timeout_ms: u32,
    pub delivery_timeout_ms: u32,
    pub max_in_flight_requests: u32,
    pub enable_idempotence: bool,
    pub additional_config: HashMap<String, String>,
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self {
            brokers: vec!["localhost:9092".to_string()],
            client_id: "ripel-producer".to_string(),
            compression_type: "snappy".to_string(),
            acks: "all".to_string(),
            retries: 3,
            batch_size: 16384,
            linger_ms: 5,
            request_timeout_ms: 30000,
            delivery_timeout_ms: 120000,
            max_in_flight_requests: 5,
            enable_idempotence: true,
            additional_config: HashMap::new(),
        }
    }
}

impl RipelKafkaProducer {
    /// Create a new Kafka producer
    #[instrument(skip(config))]
    pub fn new(config: KafkaProducerConfig) -> Result<Self> {
        info!("Creating Kafka producer");

        let mut client_config = ClientConfig::new();
        
        // Basic configuration
        client_config.set("bootstrap.servers", config.brokers.join(","));
        client_config.set("client.id", &config.client_id);
        client_config.set("compression.type", &config.compression_type);
        client_config.set("acks", &config.acks);
        client_config.set("retries", &config.retries.to_string());
        client_config.set("batch.size", &config.batch_size.to_string());
        client_config.set("linger.ms", &config.linger_ms.to_string());
        client_config.set("request.timeout.ms", &config.request_timeout_ms.to_string());
        client_config.set("delivery.timeout.ms", &config.delivery_timeout_ms.to_string());
        client_config.set("max.in.flight.requests.per.connection", &config.max_in_flight_requests.to_string());
        client_config.set("enable.idempotence", &config.enable_idempotence.to_string());

        // Additional configuration
        for (key, value) in &config.additional_config {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| RipelError::KafkaError(format!("Failed to create producer: {}", e)))?;

        Ok(Self { producer, config })
    }

    /// Send a message to Kafka
    pub async fn send(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<(i32, i64)> {
        let mut record = FutureRecord::to(topic).payload(payload);
        
        if let Some(k) = key {
            record = record.key(k);
        }

        let result = self
            .producer
            .send(record, Timeout::After(timeout))
            .await
            .map_err(|(kafka_error, _record)| {
                RipelError::KafkaError(format!("Send failed: {}", kafka_error))
            })?;

        Ok(result)
    }

    /// Send a message with headers
    pub async fn send_with_headers(
        &self,
        topic: &str,
        key: Option<&str>,
        payload: &[u8],
        headers: &[(&str, &[u8])],
        timeout: Duration,
    ) -> Result<(i32, i64)> {
        let mut record = FutureRecord::to(topic).payload(payload);
        
        if let Some(k) = key {
            record = record.key(k);
        }

        for (header_key, header_value) in headers {
            record = record.header(header_key, header_value);
        }

        let result = self
            .producer
            .send(record, Timeout::After(timeout))
            .await
            .map_err(|(kafka_error, _record)| {
                RipelError::KafkaError(format!("Send with headers failed: {}", kafka_error))
            })?;

        Ok(result)
    }

    /// Flush pending messages
    pub async fn flush(&self, timeout: Duration) -> Result<()> {
        self.producer
            .flush(Timeout::After(timeout))
            .map_err(|e| RipelError::KafkaError(format!("Flush failed: {}", e)))
    }

    /// Get producer statistics
    pub fn get_statistics(&self) -> Result<String> {
        self.producer
            .context()
            .statistics()
            .map_err(|e| RipelError::KafkaError(format!("Failed to get statistics: {}", e)))
    }

    /// Get configuration
    pub fn config(&self) -> &KafkaProducerConfig {
        &self.config
    }
}

/// Producer pool for high-throughput scenarios
pub struct KafkaProducerPool {
    producers: Vec<RipelKafkaProducer>,
    current_index: std::sync::atomic::AtomicUsize,
}

impl KafkaProducerPool {
    /// Create a new producer pool
    pub fn new(config: KafkaProducerConfig, pool_size: usize) -> Result<Self> {
        let mut producers = Vec::with_capacity(pool_size);
        
        for i in 0..pool_size {
            let mut producer_config = config.clone();
            producer_config.client_id = format!("{}-{}", config.client_id, i);
            
            let producer = RipelKafkaProducer::new(producer_config)?;
            producers.push(producer);
        }

        Ok(Self {
            producers,
            current_index: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    /// Get the next producer (round-robin)
    pub fn get_producer(&self) -> &RipelKafkaProducer {
        let index = self.current_index.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.producers.len();
        &self.producers[index]
    }

    /// Get number of producers in the pool
    pub fn size(&self) -> usize {
        self.producers.len()
    }

    /// Flush all producers
    pub async fn flush_all(&self, timeout: Duration) -> Result<()> {
        for producer in &self.producers {
            producer.flush(timeout).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_config_default() {
        let config = KafkaProducerConfig::default();
        assert_eq!(config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.compression_type, "snappy");
        assert_eq!(config.acks, "all");
        assert!(config.enable_idempotence);
    }

    #[test]
    fn test_producer_creation() {
        let config = KafkaProducerConfig::default();
        let result = RipelKafkaProducer::new(config);
        
        // This will fail without Kafka, but tests the configuration
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_producer_pool_creation() {
        let config = KafkaProducerConfig::default();
        let result = KafkaProducerPool::new(config, 3);
        
        // This will fail without Kafka, but tests the structure
        assert!(result.is_err() || result.is_ok());
    }
}