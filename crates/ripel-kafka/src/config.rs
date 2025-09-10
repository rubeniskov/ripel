//! Kafka-specific configuration

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Topic configuration for event routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicConfig {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u16,
    pub config: HashMap<String, String>,
}

impl TopicConfig {
    pub fn new(name: impl Into<String>) -> Self {
        let mut config = HashMap::new();
        config.insert("cleanup.policy".to_string(), "delete".to_string());
        config.insert("retention.ms".to_string(), "604800000".to_string()); // 7 days
        config.insert("compression.type".to_string(), "producer".to_string());
        
        Self {
            name: name.into(),
            partitions: 3,
            replication_factor: 1,
            config,
        }
    }

    pub fn with_partitions(mut self, partitions: u32) -> Self {
        self.partitions = partitions;
        self
    }

    pub fn with_replication_factor(mut self, replication_factor: u16) -> Self {
        self.replication_factor = replication_factor;
        self
    }

    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.config.insert(key.into(), value.into());
        self
    }

    pub fn with_retention_ms(mut self, retention_ms: u64) -> Self {
        self.config.insert("retention.ms".to_string(), retention_ms.to_string());
        self
    }

    pub fn with_cleanup_policy(mut self, policy: impl Into<String>) -> Self {
        self.config.insert("cleanup.policy".to_string(), policy.into());
        self
    }
}

/// Event routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Default topic for events
    pub default_topic: String,
    
    /// Topic routing rules based on event type
    pub event_type_routing: HashMap<String, String>,
    
    /// Topic routing rules based on source
    pub source_routing: HashMap<String, String>,
    
    /// Custom routing function (not serializable)
    #[serde(skip)]
    pub custom_router: Option<Box<dyn Fn(&str, &str) -> String + Send + Sync>>,
}

impl Default for RoutingConfig {
    fn default() -> Self {
        Self {
            default_topic: "ripel-events".to_string(),
            event_type_routing: HashMap::new(),
            source_routing: HashMap::new(),
            custom_router: None,
        }
    }
}

impl RoutingConfig {
    pub fn new(default_topic: impl Into<String>) -> Self {
        Self {
            default_topic: default_topic.into(),
            ..Default::default()
        }
    }

    pub fn route_by_event_type(mut self, event_type: impl Into<String>, topic: impl Into<String>) -> Self {
        self.event_type_routing.insert(event_type.into(), topic.into());
        self
    }

    pub fn route_by_source(mut self, source: impl Into<String>, topic: impl Into<String>) -> Self {
        self.source_routing.insert(source.into(), topic.into());
        self
    }

    pub fn with_custom_router<F>(mut self, router: F) -> Self
    where
        F: Fn(&str, &str) -> String + Send + Sync + 'static,
    {
        self.custom_router = Some(Box::new(router));
        self
    }

    /// Get topic for an event based on routing rules
    pub fn get_topic(&self, event_type: &str, source: &str) -> String {
        // Check custom router first
        if let Some(ref router) = self.custom_router {
            return router(event_type, source);
        }

        // Check event type routing
        if let Some(topic) = self.event_type_routing.get(event_type) {
            return topic.clone();
        }

        // Check source routing
        if let Some(topic) = self.source_routing.get(source) {
            return topic.clone();
        }

        // Use default topic
        self.default_topic.clone()
    }
}

/// Partitioning strategy for events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitioningStrategy {
    /// Use event ID for partitioning
    EventId,
    
    /// Use partition key if available, otherwise event ID
    PartitionKey,
    
    /// Use source system for partitioning
    Source,
    
    /// Use event type for partitioning
    EventType,
    
    /// Round-robin partitioning
    RoundRobin,
    
    /// Custom partitioning function (not serializable)
    #[serde(skip)]
    Custom(Box<dyn Fn(&str, &str, &str) -> String + Send + Sync>),
}

impl Default for PartitioningStrategy {
    fn default() -> Self {
        PartitioningStrategy::PartitionKey
    }
}

impl PartitioningStrategy {
    /// Get partition key for an event
    pub fn get_partition_key(&self, event_id: &str, event_type: &str, source: &str, partition_key: Option<&str>) -> String {
        match self {
            PartitioningStrategy::EventId => event_id.to_string(),
            PartitioningStrategy::PartitionKey => {
                partition_key.unwrap_or(event_id).to_string()
            }
            PartitioningStrategy::Source => source.to_string(),
            PartitioningStrategy::EventType => event_type.to_string(),
            PartitioningStrategy::RoundRobin => {
                // For round-robin, we'd typically use a counter
                // For simplicity, use event_id hash
                format!("{:x}", md5::compute(event_id))
            }
            PartitioningStrategy::Custom(func) => func(event_id, event_type, source),
        }
    }
}

/// Schema registry configuration (for future use)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistryConfig {
    pub enabled: bool,
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub schema_subject_strategy: String,
}

impl Default for SchemaRegistryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: "http://localhost:8081".to_string(),
            username: None,
            password: None,
            schema_subject_strategy: "TopicNameStrategy".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_config_builder() {
        let config = TopicConfig::new("test-topic")
            .with_partitions(5)
            .with_replication_factor(3)
            .with_retention_ms(86400000) // 1 day
            .with_cleanup_policy("compact");

        assert_eq!(config.name, "test-topic");
        assert_eq!(config.partitions, 5);
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.config.get("retention.ms"), Some(&"86400000".to_string()));
        assert_eq!(config.config.get("cleanup.policy"), Some(&"compact".to_string()));
    }

    #[test]
    fn test_routing_config() {
        let config = RoutingConfig::new("default-topic")
            .route_by_event_type("user.created", "user-events")
            .route_by_source("auth-service", "auth-events");

        assert_eq!(config.get_topic("user.created", "user-service"), "user-events");
        assert_eq!(config.get_topic("login.attempted", "auth-service"), "auth-events");
        assert_eq!(config.get_topic("order.placed", "order-service"), "default-topic");
    }

    #[test]
    fn test_partitioning_strategy() {
        let strategy = PartitioningStrategy::default();
        let key = strategy.get_partition_key("event-123", "user.created", "user-service", Some("user-456"));
        assert_eq!(key, "user-456");

        let strategy = PartitioningStrategy::EventType;
        let key = strategy.get_partition_key("event-123", "user.created", "user-service", None);
        assert_eq!(key, "user.created");

        let strategy = PartitioningStrategy::Source;
        let key = strategy.get_partition_key("event-123", "user.created", "user-service", None);
        assert_eq!(key, "user-service");
    }

    #[test]
    fn test_custom_routing() {
        let config = RoutingConfig::new("default")
            .with_custom_router(|event_type, source| {
                if event_type.starts_with("user.") {
                    "user-topic".to_string()
                } else if source == "payment-service" {
                    "payment-topic".to_string()
                } else {
                    "misc-topic".to_string()
                }
            });

        assert_eq!(config.get_topic("user.created", "any-source"), "user-topic");
        assert_eq!(config.get_topic("payment.processed", "payment-service"), "payment-topic");
        assert_eq!(config.get_topic("other.event", "other-source"), "misc-topic");
    }
}