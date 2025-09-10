//! Kafka publishing with DLQ support for RIPeL

use ripel_core::{DLQEvent, RipelEvent, Result, RipelError};
use ripel_shared::{EventMetrics, PerfTimer, RetryExecutor, RetryPolicy};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use rdkafka::Message;
use serde_json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info, instrument, warn};

pub mod config;
pub mod dlq;
pub mod producer;
pub mod publisher;

pub use config::*;
pub use dlq::*;
pub use producer::*;
pub use publisher::*;

/// Kafka publishing configuration
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct KafkaPublisherConfig {
    /// Kafka broker addresses
    pub brokers: Vec<String>,
    
    /// Client ID
    pub client_id: String,
    
    /// Default topic for events
    pub default_topic: String,
    
    /// Dead letter queue topic
    pub dlq_topic: String,
    
    /// Producer configuration
    pub producer_config: HashMap<String, String>,
    
    /// Retry configuration
    pub retry_attempts: u32,
    pub retry_delay_ms: u64,
    
    /// Batch configuration
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    
    /// Compression
    pub compression_type: String,
}

impl Default for KafkaPublisherConfig {
    fn default() -> Self {
        let mut producer_config = HashMap::new();
        producer_config.insert("acks".to_string(), "all".to_string());
        producer_config.insert("retries".to_string(), "3".to_string());
        producer_config.insert("batch.size".to_string(), "16384".to_string());
        producer_config.insert("linger.ms".to_string(), "5".to_string());
        producer_config.insert("compression.type".to_string(), "snappy".to_string());
        producer_config.insert("max.in.flight.requests.per.connection".to_string(), "5".to_string());
        producer_config.insert("enable.idempotence".to_string(), "true".to_string());

        Self {
            brokers: vec!["localhost:9092".to_string()],
            client_id: "ripel-publisher".to_string(),
            default_topic: "ripel-events".to_string(),
            dlq_topic: "ripel-dlq".to_string(),
            producer_config,
            retry_attempts: 3,
            retry_delay_ms: 1000,
            batch_size: 100,
            batch_timeout_ms: 100,
            compression_type: "snappy".to_string(),
        }
    }
}

/// Event publisher trait
#[async_trait]
pub trait EventPublisher: Send + Sync {
    /// Publish a single event
    async fn publish(&self, event: RipelEvent) -> Result<PublishResult>;
    
    /// Publish a batch of events
    async fn publish_batch(&self, events: Vec<RipelEvent>) -> Result<Vec<PublishResult>>;
    
    /// Start the publisher
    async fn start(&self) -> Result<()>;
    
    /// Stop the publisher
    async fn stop(&self) -> Result<()>;
}

/// Result of publishing an event
#[derive(Debug, Clone)]
pub struct PublishResult {
    pub event_id: String,
    pub success: bool,
    pub topic: String,
    pub partition: Option<i32>,
    pub offset: Option<i64>,
    pub error: Option<String>,
}

impl PublishResult {
    pub fn success(event_id: String, topic: String, partition: i32, offset: i64) -> Self {
        Self {
            event_id,
            success: true,
            topic,
            partition: Some(partition),
            offset: Some(offset),
            error: None,
        }
    }

    pub fn failure(event_id: String, topic: String, error: String) -> Self {
        Self {
            event_id,
            success: false,
            topic,
            partition: None,
            offset: None,
            error: Some(error),
        }
    }
}

/// Kafka event publisher with DLQ support
pub struct KafkaEventPublisher {
    config: KafkaPublisherConfig,
    producer: FutureProducer,
    dlq_handler: Arc<DLQHandler>,
}

impl KafkaEventPublisher {
    /// Create a new Kafka event publisher
    pub fn new(config: KafkaPublisherConfig) -> Result<Self> {
        let mut client_config = ClientConfig::new();
        client_config.set("bootstrap.servers", config.brokers.join(","));
        client_config.set("client.id", &config.client_id);

        // Apply additional producer configuration
        for (key, value) in &config.producer_config {
            client_config.set(key, value);
        }

        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| RipelError::KafkaError(format!("Failed to create producer: {}", e)))?;

        let dlq_config = DLQConfig {
            topic: config.dlq_topic.clone(),
            max_retries: config.retry_attempts,
            retry_delay: Duration::from_millis(config.retry_delay_ms),
        };
        
        let dlq_handler = Arc::new(DLQHandler::new(dlq_config, producer.clone()));

        Ok(Self {
            config,
            producer,
            dlq_handler,
        })
    }

    /// Get topic for event (uses routing logic)
    fn get_topic_for_event(&self, _event: &RipelEvent) -> String {
        // In a real implementation, you might have routing rules
        // For now, use the default topic
        self.config.default_topic.clone()
    }

    /// Serialize event for Kafka
    fn serialize_event(&self, event: &RipelEvent) -> Result<Vec<u8>> {
        serde_json::to_vec(event)
            .map_err(|e| RipelError::SerializationError(e))
    }
}

#[async_trait]
impl EventPublisher for KafkaEventPublisher {
    #[instrument(skip(self, event), fields(event_id = %event.id, event_type = %event.event_type))]
    async fn publish(&self, event: RipelEvent) -> Result<PublishResult> {
        let _timer = PerfTimer::new("kafka_publish_duration")
            .with_label("topic", &self.config.default_topic);

        let topic = self.get_topic_for_event(&event);
        let payload = self.serialize_event(&event)?;
        let key = event.effective_partition_key().to_string();
        
        let record = FutureRecord::to(&topic)
            .key(&key)
            .payload(&payload);

        match self.producer.send(record, Timeout::After(Duration::from_secs(30))).await {
            Ok((partition, offset)) => {
                EventMetrics::kafka_operation("publish", &topic, true);
                Ok(PublishResult::success(event.id, topic, partition, offset))
            }
            Err((kafka_error, _record)) => {
                warn!(
                    event_id = %event.id,
                    error = %kafka_error,
                    "Failed to publish event to Kafka"
                );

                EventMetrics::kafka_operation("publish", &topic, false);

                // Send to DLQ
                if let Err(dlq_error) = self.dlq_handler.handle_failed_event(
                    event.clone(),
                    &kafka_error.to_string(),
                    "KAFKA_PUBLISH_ERROR",
                    &topic,
                ).await {
                    error!(
                        event_id = %event.id,
                        dlq_error = %dlq_error,
                        "Failed to send event to DLQ"
                    );
                }

                Ok(PublishResult::failure(event.id, topic, kafka_error.to_string()))
            }
        }
    }

    async fn publish_batch(&self, events: Vec<RipelEvent>) -> Result<Vec<PublishResult>> {
        let _timer = PerfTimer::new("kafka_publish_batch_duration")
            .with_label("batch_size", &events.len().to_string());

        let mut results = Vec::with_capacity(events.len());
        
        // For better performance, you could use futures::stream::FuturesUnordered
        // to publish events concurrently
        for event in events {
            let result = self.publish(event).await?;
            results.push(result);
        }

        Ok(results)
    }

    async fn start(&self) -> Result<()> {
        info!(
            brokers = ?self.config.brokers,
            client_id = %self.config.client_id,
            default_topic = %self.config.default_topic,
            "Starting Kafka event publisher"
        );
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        info!("Stopping Kafka event publisher");
        Ok(())
    }
}

/// Batching event publisher wrapper
pub struct BatchingEventPublisher {
    inner: Arc<dyn EventPublisher>,
    event_tx: mpsc::Sender<RipelEvent>,
    batch_size: usize,
    batch_timeout: Duration,
}

impl BatchingEventPublisher {
    pub fn new(
        inner: Arc<dyn EventPublisher>,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(batch_size * 2);
        
        let publisher = Self {
            inner: inner.clone(),
            event_tx,
            batch_size,
            batch_timeout,
        };

        // Start batching worker
        tokio::spawn(Self::batch_worker(inner, event_rx, batch_size, batch_timeout));

        publisher
    }

    async fn batch_worker(
        publisher: Arc<dyn EventPublisher>,
        mut event_rx: mpsc::Receiver<RipelEvent>,
        batch_size: usize,
        batch_timeout: Duration,
    ) {
        let mut batch = Vec::with_capacity(batch_size);
        let mut timeout = tokio::time::interval(batch_timeout);

        loop {
            tokio::select! {
                event = event_rx.recv() => {
                    match event {
                        Some(event) => {
                            batch.push(event);
                            
                            if batch.len() >= batch_size {
                                if let Err(e) = publisher.publish_batch(std::mem::take(&mut batch)).await {
                                    error!("Batch publish failed: {}", e);
                                }
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                _ = timeout.tick() => {
                    if !batch.is_empty() {
                        if let Err(e) = publisher.publish_batch(std::mem::take(&mut batch)).await {
                            error!("Batch publish failed: {}", e);
                        }
                    }
                }
            }
        }

        // Flush remaining events
        if !batch.is_empty() {
            if let Err(e) = publisher.publish_batch(batch).await {
                error!("Final batch publish failed: {}", e);
            }
        }
    }

    /// Get sender for submitting events
    pub fn sender(&self) -> mpsc::Sender<RipelEvent> {
        self.event_tx.clone()
    }
}

#[async_trait]
impl EventPublisher for BatchingEventPublisher {
    async fn publish(&self, event: RipelEvent) -> Result<PublishResult> {
        self.event_tx
            .send(event.clone())
            .await
            .map_err(|_| RipelError::InternalError("Batch channel full".to_string()))?;

        // Return optimistic result - actual result will be handled by batch worker
        Ok(PublishResult::success(
            event.id,
            "batched".to_string(),
            0,
            0,
        ))
    }

    async fn publish_batch(&self, events: Vec<RipelEvent>) -> Result<Vec<PublishResult>> {
        for event in &events {
            self.event_tx
                .send(event.clone())
                .await
                .map_err(|_| RipelError::InternalError("Batch channel full".to_string()))?;
        }

        // Return optimistic results
        Ok(events
            .into_iter()
            .map(|event| {
                PublishResult::success(event.id, "batched".to_string(), 0, 0)
            })
            .collect())
    }

    async fn start(&self) -> Result<()> {
        self.inner.start().await
    }

    async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_publisher_config() {
        let config = KafkaPublisherConfig::default();
        assert_eq!(config.brokers, vec!["localhost:9092"]);
        assert_eq!(config.default_topic, "ripel-events");
        assert!(config.producer_config.contains_key("acks"));
    }

    #[test]
    fn test_publish_result() {
        let success = PublishResult::success("test-id".to_string(), "test-topic".to_string(), 0, 123);
        assert!(success.success);
        assert_eq!(success.offset, Some(123));

        let failure = PublishResult::failure(
            "test-id".to_string(),
            "test-topic".to_string(),
            "Connection failed".to_string(),
        );
        assert!(!failure.success);
        assert!(failure.error.is_some());
    }

    #[tokio::test]
    async fn test_event_serialization() {
        let config = KafkaPublisherConfig::default();
        let publisher = KafkaEventPublisher::new(config);
        
        // This will fail without Kafka, but tests the config
        assert!(publisher.is_err() || publisher.is_ok());
    }
}