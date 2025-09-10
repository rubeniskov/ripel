//! Core event types and utilities

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Core event structure for the event-driven architecture
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RipelEvent {
    /// Unique event identifier
    pub id: String,
    
    /// Event type/schema identifier
    pub event_type: String,
    
    /// Source system identifier
    pub source: String,
    
    /// Event timestamp
    pub timestamp: DateTime<Utc>,
    
    /// Event payload as JSON
    pub data: serde_json::Value,
    
    /// Metadata for routing and processing
    pub metadata: HashMap<String, String>,
    
    /// Correlation ID for distributed tracing
    pub correlation_id: String,
    
    /// Partition key for consistent routing
    pub partition_key: Option<String>,
}

impl RipelEvent {
    /// Create a new event with automatic ID and timestamp generation
    pub fn new(
        event_type: impl Into<String>,
        source: impl Into<String>,
        data: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            event_type: event_type.into(),
            source: source.into(),
            timestamp: Utc::now(),
            data,
            metadata: HashMap::new(),
            correlation_id: Uuid::new_v4().to_string(),
            partition_key: None,
        }
    }

    /// Add metadata to the event
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set partition key for consistent routing
    pub fn with_partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    /// Set correlation ID for tracing
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = id.into();
        self
    }

    /// Get the partition key, using event ID as fallback
    pub fn effective_partition_key(&self) -> &str {
        self.partition_key.as_deref().unwrap_or(&self.id)
    }
}

/// Database change event with CDC-specific information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseChangeEvent {
    /// Base event information
    pub base_event: RipelEvent,
    
    /// Type of database operation
    pub operation: OperationType,
    
    /// Database name
    pub database: String,
    
    /// Table name
    pub table: String,
    
    /// Before state (for updates and deletes)
    pub before: Option<serde_json::Value>,
    
    /// After state (for inserts and updates)
    pub after: Option<serde_json::Value>,
    
    /// Transaction identifier
    pub transaction_id: Option<String>,
    
    /// Log sequence number or similar ordering identifier
    pub lsn: Option<i64>,
}

impl DatabaseChangeEvent {
    pub fn new(
        operation: OperationType,
        database: impl Into<String>,
        table: impl Into<String>,
        before: Option<serde_json::Value>,
        after: Option<serde_json::Value>,
    ) -> Self {
        let database = database.into();
        let table = table.into();
        
        let event_type = format!("database.{}.{}.{}", database, table, operation.as_str());
        let source = format!("mysql://{}/{}", database, table);
        
        let data = serde_json::json!({
            "operation": operation.as_str(),
            "database": database,
            "table": table,
            "before": before,
            "after": after,
        });

        Self {
            base_event: RipelEvent::new(event_type, source, data)
                .with_partition_key(format!("{}:{}", database, table)),
            operation,
            database,
            table,
            before,
            after,
            transaction_id: None,
            lsn: None,
        }
    }

    pub fn with_transaction_id(mut self, tx_id: impl Into<String>) -> Self {
        self.transaction_id = Some(tx_id.into());
        self
    }

    pub fn with_lsn(mut self, lsn: i64) -> Self {
        self.lsn = Some(lsn);
        self
    }
}

/// Database operation types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum OperationType {
    Insert,
    Update,
    Delete,
    Ddl,  // Data Definition Language changes
}

impl OperationType {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::Insert => "insert",
            OperationType::Update => "update", 
            OperationType::Delete => "delete",
            OperationType::Ddl => "ddl",
        }
    }
}

/// Dead Letter Queue event for failed processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DLQEvent {
    /// Original event that failed processing
    pub original_event: RipelEvent,
    
    /// Error message
    pub error_message: String,
    
    /// Error code for categorization
    pub error_code: String,
    
    /// Number of processing attempts
    pub retry_count: u32,
    
    /// When the failure occurred
    pub failed_at: DateTime<Utc>,
    
    /// Destination that failed to process the event
    pub failed_destination: String,
}

impl DLQEvent {
    pub fn new(
        original_event: RipelEvent,
        error_message: impl Into<String>,
        error_code: impl Into<String>,
        failed_destination: impl Into<String>,
    ) -> Self {
        Self {
            original_event,
            error_message: error_message.into(),
            error_code: error_code.into(),
            retry_count: 0,
            failed_at: Utc::now(),
            failed_destination: failed_destination.into(),
        }
    }

    pub fn increment_retry(mut self) -> Self {
        self.retry_count += 1;
        self.failed_at = Utc::now();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_creation() {
        let data = serde_json::json!({"key": "value"});
        let event = RipelEvent::new("test.event", "test-system", data.clone());
        
        assert_eq!(event.event_type, "test.event");
        assert_eq!(event.source, "test-system");
        assert_eq!(event.data, data);
        assert!(!event.id.is_empty());
        assert!(!event.correlation_id.is_empty());
    }

    #[test]
    fn test_database_change_event() {
        let before = serde_json::json!({"id": 1, "name": "old"});
        let after = serde_json::json!({"id": 1, "name": "new"});
        
        let change = DatabaseChangeEvent::new(
            OperationType::Update,
            "test_db",
            "users",
            Some(before.clone()),
            Some(after.clone()),
        );

        assert_eq!(change.operation, OperationType::Update);
        assert_eq!(change.database, "test_db");
        assert_eq!(change.table, "users");
        assert_eq!(change.before, Some(before));
        assert_eq!(change.after, Some(after));
    }

    #[test]
    fn test_dlq_event() {
        let original = RipelEvent::new("test", "source", serde_json::json!({}));
        let dlq = DLQEvent::new(
            original.clone(),
            "Processing failed",
            "PROC_ERROR",
            "kafka-topic",
        );

        assert_eq!(dlq.original_event.id, original.id);
        assert_eq!(dlq.error_message, "Processing failed");
        assert_eq!(dlq.retry_count, 0);
    }
}