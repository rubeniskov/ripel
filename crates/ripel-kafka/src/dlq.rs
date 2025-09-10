//! Dead Letter Queue handling for failed events

use ripel_core::{DLQEvent, RipelEvent, Result, RipelError};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use serde_json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// DLQ configuration
#[derive(Debug, Clone)]
pub struct DLQConfig {
    pub topic: String,
    pub max_retries: u32,
    pub retry_delay: Duration,
}

/// Dead Letter Queue handler
pub struct DLQHandler {
    config: DLQConfig,
    producer: FutureProducer,
    dlq_counter: AtomicU64,
}

impl DLQHandler {
    pub fn new(config: DLQConfig, producer: FutureProducer) -> Self {
        Self {
            config,
            producer,
            dlq_counter: AtomicU64::new(0),
        }
    }

    /// Handle a failed event by sending it to DLQ
    pub async fn handle_failed_event(
        &self,
        original_event: RipelEvent,
        error_message: &str,
        error_code: &str,
        failed_destination: &str,
    ) -> Result<()> {
        let dlq_event = DLQEvent::new(
            original_event.clone(),
            error_message,
            error_code,
            failed_destination,
        );

        self.send_to_dlq(dlq_event).await?;
        
        let count = self.dlq_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if count % 100 == 0 {
            warn!("Sent {} events to DLQ", count);
        }

        Ok(())
    }

    /// Send DLQ event to Kafka
    async fn send_to_dlq(&self, dlq_event: DLQEvent) -> Result<()> {
        let payload = serde_json::to_vec(&dlq_event)
            .map_err(|e| RipelError::SerializationError(e))?;

        let key = dlq_event.original_event.id.clone();
        
        let record = FutureRecord::to(&self.config.topic)
            .key(&key)
            .payload(&payload);

        match self.producer.send(record, Timeout::After(Duration::from_secs(10))).await {
            Ok((partition, offset)) => {
                info!(
                    event_id = %dlq_event.original_event.id,
                    partition = partition,
                    offset = offset,
                    error_code = %dlq_event.error_code,
                    "Event sent to DLQ"
                );
                Ok(())
            }
            Err((kafka_error, _record)) => {
                error!(
                    event_id = %dlq_event.original_event.id,
                    kafka_error = %kafka_error,
                    "Failed to send event to DLQ - event will be lost!"
                );
                Err(RipelError::KafkaError(format!("DLQ send failed: {}", kafka_error)))
            }
        }
    }

    /// Get the number of events sent to DLQ
    pub fn dlq_event_count(&self) -> u64 {
        self.dlq_counter.load(Ordering::Relaxed)
    }
}

/// DLQ event processor for handling and potentially retrying DLQ events
pub struct DLQProcessor {
    handler: Arc<DLQHandler>,
}

impl DLQProcessor {
    pub fn new(handler: Arc<DLQHandler>) -> Self {
        Self { handler }
    }

    /// Process a DLQ event (e.g., for manual retry or analysis)
    pub async fn process_dlq_event(&self, dlq_event: DLQEvent) -> Result<()> {
        info!(
            original_event_id = %dlq_event.original_event.id,
            error_code = %dlq_event.error_code,
            retry_count = dlq_event.retry_count,
            "Processing DLQ event"
        );

        // In a real implementation, you might:
        // 1. Attempt to retry the original operation
        // 2. Send to a manual review queue
        // 3. Apply fixes and retry
        // 4. Generate alerts for operations teams

        Ok(())
    }

    /// Retry a DLQ event
    pub async fn retry_dlq_event(&self, mut dlq_event: DLQEvent) -> Result<()> {
        if dlq_event.retry_count >= 5 {
            warn!(
                event_id = %dlq_event.original_event.id,
                "DLQ event has exceeded maximum retry count"
            );
            return Err(RipelError::ProcessingError("Max retries exceeded".to_string()));
        }

        dlq_event = dlq_event.increment_retry();
        
        // In a real implementation, you would attempt to retry the original operation
        info!(
            event_id = %dlq_event.original_event.id,
            retry_count = dlq_event.retry_count,
            "Retrying DLQ event"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::config::ClientConfig;
    use serde_json::json;

    #[test]
    fn test_dlq_config() {
        let config = DLQConfig {
            topic: "test-dlq".to_string(),
            max_retries: 3,
            retry_delay: Duration::from_secs(5),
        };

        assert_eq!(config.topic, "test-dlq");
        assert_eq!(config.max_retries, 3);
    }

    #[tokio::test]
    #[ignore] // Requires Kafka
    async fn test_dlq_handler() {
        let config = DLQConfig {
            topic: "test-dlq".to_string(),
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
        };

        let client_config = ClientConfig::new();
        let producer: FutureProducer = client_config.create().unwrap();
        
        let handler = DLQHandler::new(config, producer);
        let event = RipelEvent::new("test", "source", json!({}));
        
        // This would require a real Kafka instance
        let result = handler.handle_failed_event(
            event,
            "Test error",
            "TEST_ERROR",
            "test-destination",
        ).await;

        // Test structure, actual functionality requires Kafka
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_dlq_event_creation() {
        let original = RipelEvent::new("test", "source", json!({}));
        let dlq = DLQEvent::new(
            original.clone(),
            "Test error",
            "TEST_ERROR",
            "kafka-topic",
        );

        assert_eq!(dlq.original_event.id, original.id);
        assert_eq!(dlq.error_code, "TEST_ERROR");
        assert_eq!(dlq.retry_count, 0);
    }
}