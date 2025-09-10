# RIPeL: Replication-Integrated Propagation for Event Logs

A high-performance Rust monorepo for event-driven architecture with MySQL CDC, Kafka publishing, and comprehensive observability.

## 🏗️ Architecture

RIPeL is designed as a modular monorepo with the following crates:

### Core Crates

- **`ripel-core`** - Core event types, processing pipeline, and streaming abstractions
- **`ripel-shared`** - Shared utilities, configuration, observability, and retry logic  
- **`ripel-mysql-cdc`** - MySQL Change Data Capture with binlog processing
- **`ripel-kafka`** - Kafka publishing with Dead Letter Queue support

## 🚀 Features

### Event Processing
- **Event-driven architecture** with async processing pipelines
- **Concurrent worker pools** for high-throughput event processing
- **Event streaming** with filtering and multiplexing capabilities
- **Type-safe event serialization** with Protocol Buffers and Serde

### MySQL CDC
- **Binlog-based change capture** for real-time data replication
- **Table-level filtering** and column selection
- **Transaction boundary tracking** with LSN/GTID support
- **Configurable event routing** based on database and table patterns

### Kafka Integration
- **High-performance publishing** with batching and compression
- **Dead Letter Queue** handling for failed events
- **Event routing** based on content and metadata
- **Producer pooling** for maximum throughput
- **Exactly-once semantics** with idempotent producers

### Observability
- **Structured logging** with JSON output and filtering
- **Prometheus metrics** for monitoring and alerting
- **Distributed tracing** with correlation IDs
- **Health checks** with circuit breakers
- **Performance timers** for latency tracking

### Resilience
- **Exponential backoff** retry strategies
- **Circuit breaker** patterns for fault tolerance
- **Graceful degradation** with DLQ fallbacks
- **Configuration hot-reloading** from files and environment

## 📦 Installation

Add RIPeL to your `Cargo.toml`:

```toml
[dependencies]
ripel-core = "0.1.0"
ripel-shared = "0.1.0" 
ripel-mysql-cdc = "0.1.0"
ripel-kafka = "0.1.0"
```

## 🏃 Quick Start

### Basic Event Processing

```rust
use ripel_core::{RipelEvent, EventPipeline, LoggingProcessor};
use serde_json::json;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create event processor
    let processor = Arc::new(LoggingProcessor);
    let pipeline = EventPipeline::new(processor, 1000, 4);
    let sender = pipeline.sender();
    
    // Start pipeline
    let handle = tokio::spawn(pipeline.start());
    
    // Create and send events
    let event = RipelEvent::new(
        "user.created",
        "user-service", 
        json!({"user_id": 123, "email": "user@example.com"})
    );
    
    sender.send(event).await?;
    
    // Cleanup
    drop(sender);
    handle.await?;
    Ok(())
}
```

### MySQL CDC

```rust
use ripel_mysql_cdc::{MySqlCdcProcessor, MySqlCdcConfig};

#[tokio::main] 
async fn main() -> anyhow::Result<()> {
    let config = MySqlCdcConfig {
        connection_url: "mysql://user:pass@localhost:3306".to_string(),
        database: "myapp".to_string(),
        tables: vec!["users".to_string(), "orders".to_string()],
        server_id: 1001,
        ..Default::default()
    };
    
    let processor = MySqlCdcProcessor::new(config).await?;
    
    processor.start_processing(|change_event| Box::pin(async move {
        println!("Change: {} on {}.{}", 
                 change_event.operation.as_str(),
                 change_event.database,
                 change_event.table);
        Ok(())
    })).await?;
    
    Ok(())
}
```

### Kafka Publishing

```rust
use ripel_kafka::{PublisherBuilder, RoutingConfig, PartitioningStrategy};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let publisher = PublisherBuilder::new()
        .with_brokers(vec!["localhost:9092".to_string()])
        .with_default_topic("events")
        .route_event_type("user.created", "user-events")
        .route_source("payment-service", "payment-events")
        .build()?;
    
    publisher.start().await?;
    
    let event = RipelEvent::new("user.created", "api", json!({}));
    let result = publisher.publish(event).await?;
    
    println!("Published to partition {} offset {}", 
             result.partition.unwrap(), result.offset.unwrap());
    
    Ok(())
}
```

## 🔧 Configuration

RIPeL uses a hierarchical configuration system:

```toml
# config.toml
[database]
url = "mysql://localhost:3306/ripel"
max_connections = 10

[kafka]
brokers = ["localhost:9092"]
default_topic = "ripel-events"
dlq_topic = "ripel-dlq"

[observability.logging]
level = "info"
format = "json"

[observability.metrics]
enabled = true
bind_address = "0.0.0.0:9090"

[processing]
worker_count = 4
batch_size = 100
```

Environment variables override file settings:

```bash
export RIPEL__KAFKA__BROKERS="broker1:9092,broker2:9092"
export RIPEL__DATABASE__MAX_CONNECTIONS=20
```

## 📊 Monitoring

### Prometheus Metrics

- `ripel_events_processed_total` - Total events processed
- `ripel_events_failed_total` - Total failed events  
- `ripel_event_processing_duration_seconds` - Processing latency
- `ripel_kafka_operations_total` - Kafka operation counts
- `ripel_database_operations_total` - Database operation counts
- `ripel_queue_size` - Current queue depths

### Health Endpoints

- `/health` - Overall system health
- `/metrics` - Prometheus metrics endpoint
- `/ready` - Readiness probe for Kubernetes

## 🏗️ Development

### Prerequisites

- Rust 1.70+
- protobuf-compiler
- MySQL 8.0+ (for CDC features)
- Apache Kafka (for publishing features)

### Building

```bash
# Install dependencies
apt-get install protobuf-compiler

# Build workspace
cargo build --workspace

# Run tests  
cargo test --workspace

# Run example
cargo run --example basic_example
```

### Project Structure

```
ripel/
├── Cargo.toml                 # Workspace configuration
├── crates/
│   ├── ripel-core/            # Core event processing
│   ├── ripel-shared/          # Shared utilities
│   ├── ripel-mysql-cdc/       # MySQL CDC implementation
│   └── ripel-kafka/           # Kafka publishing
├── proto/                     # Protocol Buffer schemas
├── examples/                  # Usage examples
└── docs/                      # Documentation
```

## 📋 Roadmap

- [ ] **Performance Optimizations**
  - Zero-copy serialization with Cap'n Proto
  - SIMD-accelerated JSON parsing
  - Lock-free data structures

- [ ] **Additional Sources**  
  - PostgreSQL logical replication
  - MongoDB change streams
  - Redis pub/sub integration

- [ ] **Enhanced Observability**
  - OpenTelemetry integration
  - Custom dashboards
  - Alerting rules

- [ ] **Operational Features**
  - Schema evolution support
  - Blue/green deployments
  - Multi-region replication

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## 📄 License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

## 🔗 Related Projects

- [Debezium](https://debezium.io/) - CDC platform for Java
- [Maxwell](https://maxwells-daemon.io/) - MySQL binlog parser
- [Kafka Connect](https://kafka.apache.org/documentation/#connect) - Kafka integration framework
