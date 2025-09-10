//! Simple example demonstrating RIPeL event processing

use ripel_core::{RipelEvent, EventPipeline, ProcessorChain, LoggingProcessor, InMemoryEventStream, EventStream};
use ripel_shared::{ObservabilitySystem, RipelConfig};
use serde_json::json;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize observability
    let config = RipelConfig::default();
    ObservabilitySystem::init(&config.observability)?;

    info!("Starting RIPeL example");

    // Create event stream
    let event_stream = InMemoryEventStream::new(100);
    
    // Create event processor chain
    let processor = Arc::new(LoggingProcessor);
    let chain = ProcessorChain::new().add_processor(processor);
    
    // Create processing pipeline
    let pipeline = EventPipeline::new(Arc::new(chain), 100, 2);
    let sender = pipeline.sender();
    
    // Start pipeline in background
    let pipeline_handle = tokio::spawn(pipeline.start());
    
    // Start event stream
    event_stream.start().await?;
    
    // Create and publish sample events
    for i in 0..5 {
        let event = RipelEvent::new(
            "user.created",
            "user-service",
            json!({
                "user_id": i,
                "email": format!("user{}@example.com", i),
                "created_at": chrono::Utc::now()
            })
        ).with_metadata("version", "1.0")
         .with_partition_key(format!("user_{}", i % 2));
        
        info!("Publishing event: {}", event.id);
        
        // Send to pipeline
        sender.send(event.clone()).await?;
        
        // Also publish to stream
        event_stream.publish(event)?;
        
        sleep(Duration::from_millis(500)).await;
    }
    
    // Let processing finish
    sleep(Duration::from_secs(2)).await;
    
    // Cleanup
    drop(sender);
    event_stream.stop().await?;
    
    // Wait a bit for pipeline to finish
    sleep(Duration::from_millis(500)).await;
    pipeline_handle.abort();
    
    info!("RIPeL example completed");
    Ok(())
}