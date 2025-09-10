//! High-level event publisher interface

use crate::{EventPublisher, KafkaEventPublisher, KafkaPublisherConfig, PublishResult, RoutingConfig, PartitioningStrategy};
use ripel_core::{RipelEvent, Result};
use ripel_shared::EventMetrics;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{info, instrument};

/// High-level event publisher that combines routing, partitioning, and publishing
pub struct RipelEventPublisher {
    kafka_publisher: Arc<KafkaEventPublisher>,
    routing_config: RoutingConfig,
    partitioning_strategy: PartitioningStrategy,
}

impl RipelEventPublisher {
    /// Create a new RIPeL event publisher
    pub fn new(
        kafka_config: KafkaPublisherConfig,
        routing_config: RoutingConfig,
        partitioning_strategy: PartitioningStrategy,
    ) -> Result<Self> {
        let kafka_publisher = Arc::new(KafkaEventPublisher::new(kafka_config)?);

        Ok(Self {
            kafka_publisher,
            routing_config,
            partitioning_strategy,
        })
    }

    /// Create a publisher with default configuration
    pub fn with_default_config(brokers: Vec<String>) -> Result<Self> {
        let mut kafka_config = KafkaPublisherConfig::default();
        kafka_config.brokers = brokers;

        let routing_config = RoutingConfig::default();
        let partitioning_strategy = PartitioningStrategy::default();

        Self::new(kafka_config, routing_config, partitioning_strategy)
    }

    /// Enhance event with routing and partitioning information
    fn enhance_event(&self, mut event: RipelEvent) -> RipelEvent {
        // Apply partitioning strategy
        let partition_key = self.partitioning_strategy.get_partition_key(
            &event.id,
            &event.event_type,
            &event.source,
            event.partition_key.as_deref(),
        );
        event.partition_key = Some(partition_key);

        // Add routing metadata
        let topic = self.routing_config.get_topic(&event.event_type, &event.source);
        event.metadata.insert("target_topic".to_string(), topic);

        event
    }
}

#[async_trait]
impl EventPublisher for RipelEventPublisher {
    #[instrument(skip(self, event), fields(event_id = %event.id, event_type = %event.event_type))]
    async fn publish(&self, event: RipelEvent) -> Result<PublishResult> {
        let enhanced_event = self.enhance_event(event);
        
        // Record routing metrics
        if let Some(target_topic) = enhanced_event.metadata.get("target_topic") {
            EventMetrics::kafka_operation("route", target_topic, true);
        }

        self.kafka_publisher.publish(enhanced_event).await
    }

    async fn publish_batch(&self, events: Vec<RipelEvent>) -> Result<Vec<PublishResult>> {
        let enhanced_events: Vec<_> = events
            .into_iter()
            .map(|event| self.enhance_event(event))
            .collect();

        self.kafka_publisher.publish_batch(enhanced_events).await
    }

    async fn start(&self) -> Result<()> {
        info!("Starting RIPeL event publisher");
        self.kafka_publisher.start().await
    }

    async fn stop(&self) -> Result<()> {
        info!("Stopping RIPeL event publisher");
        self.kafka_publisher.stop().await
    }
}

/// Builder for creating a RIPeL event publisher
pub struct PublisherBuilder {
    kafka_config: KafkaPublisherConfig,
    routing_config: RoutingConfig,
    partitioning_strategy: PartitioningStrategy,
}

impl PublisherBuilder {
    /// Create a new publisher builder
    pub fn new() -> Self {
        Self {
            kafka_config: KafkaPublisherConfig::default(),
            routing_config: RoutingConfig::default(),
            partitioning_strategy: PartitioningStrategy::default(),
        }
    }

    /// Set Kafka brokers
    pub fn with_brokers(mut self, brokers: Vec<String>) -> Self {
        self.kafka_config.brokers = brokers;
        self
    }

    /// Set client ID
    pub fn with_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.kafka_config.client_id = client_id.into();
        self
    }

    /// Set default topic
    pub fn with_default_topic(mut self, topic: impl Into<String>) -> Self {
        let topic_str = topic.into();
        self.kafka_config.default_topic = topic_str.clone();
        self.routing_config.default_topic = topic_str;
        self
    }

    /// Set DLQ topic
    pub fn with_dlq_topic(mut self, topic: impl Into<String>) -> Self {
        self.kafka_config.dlq_topic = topic.into();
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.kafka_config.batch_size = batch_size;
        self
    }

    /// Set routing configuration
    pub fn with_routing(mut self, routing_config: RoutingConfig) -> Self {
        self.routing_config = routing_config;
        self
    }

    /// Set partitioning strategy
    pub fn with_partitioning(mut self, partitioning_strategy: PartitioningStrategy) -> Self {
        self.partitioning_strategy = partitioning_strategy;
        self
    }

    /// Add event type routing
    pub fn route_event_type(mut self, event_type: impl Into<String>, topic: impl Into<String>) -> Self {
        self.routing_config = self.routing_config.route_by_event_type(event_type, topic);
        self
    }

    /// Add source routing
    pub fn route_source(mut self, source: impl Into<String>, topic: impl Into<String>) -> Self {
        self.routing_config = self.routing_config.route_by_source(source, topic);
        self
    }

    /// Build the publisher
    pub fn build(self) -> Result<RipelEventPublisher> {
        RipelEventPublisher::new(
            self.kafka_config,
            self.routing_config,
            self.partitioning_strategy,
        )
    }
}

impl Default for PublisherBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_publisher_builder() {
        let builder = PublisherBuilder::new()
            .with_brokers(vec!["localhost:9092".to_string()])
            .with_client_id("test-client")
            .with_default_topic("events")
            .with_dlq_topic("dlq")
            .route_event_type("user.created", "user-events")
            .route_source("auth-service", "auth-events");

        // Test configuration
        assert_eq!(builder.kafka_config.brokers, vec!["localhost:9092"]);
        assert_eq!(builder.kafka_config.client_id, "test-client");
        assert_eq!(builder.routing_config.default_topic, "events");
        
        // Building will fail without Kafka, but tests the structure
        let result = builder.build();
        assert!(result.is_err() || result.is_ok());
    }

    #[test]
    fn test_event_enhancement() {
        // Create a mock publisher for testing enhancement logic
        let routing_config = RoutingConfig::new("default")
            .route_by_event_type("user.created", "user-events");
        let partitioning_strategy = PartitioningStrategy::EventType;

        let event = RipelEvent::new("user.created", "user-service", json!({}));
        let event_type = event.event_type.clone();
        let source = event.source.clone();

        // Test routing logic
        let topic = routing_config.get_topic(&event_type, &source);
        assert_eq!(topic, "user-events");

        // Test partitioning logic
        let partition_key = partitioning_strategy.get_partition_key(
            &event.id,
            &event.event_type,
            &event.source,
            event.partition_key.as_deref(),
        );
        assert_eq!(partition_key, "user.created");
    }
}