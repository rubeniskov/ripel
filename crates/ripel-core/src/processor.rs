//! Event processor traits and implementations

use crate::{RipelEvent, Result};
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, instrument};

/// Trait for processing events in the event-driven architecture
#[async_trait]
pub trait EventProcessor: Send + Sync {
    /// Process a single event
    async fn process(&self, event: RipelEvent) -> Result<()>;
    
    /// Process a batch of events for better performance
    async fn process_batch(&self, events: Vec<RipelEvent>) -> Result<Vec<Result<()>>> {
        let mut results = Vec::with_capacity(events.len());
        for event in events {
            results.push(self.process(event).await);
        }
        Ok(results)
    }
    
    /// Called when processor starts up
    async fn start(&self) -> Result<()> {
        info!("Event processor starting up");
        Ok(())
    }
    
    /// Called when processor shuts down
    async fn shutdown(&self) -> Result<()> {
        info!("Event processor shutting down");
        Ok(())
    }
}

/// Chain multiple processors together
pub struct ProcessorChain {
    processors: Vec<Arc<dyn EventProcessor>>,
}

impl ProcessorChain {
    pub fn new() -> Self {
        Self {
            processors: Vec::new(),
        }
    }

    pub fn add_processor(mut self, processor: Arc<dyn EventProcessor>) -> Self {
        self.processors.push(processor);
        self
    }

    pub fn len(&self) -> usize {
        self.processors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.processors.is_empty()
    }
}

impl Default for ProcessorChain {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventProcessor for ProcessorChain {
    #[instrument(skip(self, event), fields(event_id = %event.id, event_type = %event.event_type))]
    async fn process(&self, event: RipelEvent) -> Result<()> {
        for (i, processor) in self.processors.iter().enumerate() {
            if let Err(e) = processor.process(event.clone()).await {
                error!("Processor {} failed: {}", i, e);
                return Err(e);
            }
        }
        Ok(())
    }

    async fn start(&self) -> Result<()> {
        for processor in &self.processors {
            processor.start().await?;
        }
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        for processor in &self.processors {
            processor.shutdown().await?;
        }
        Ok(())
    }
}

/// Event processing pipeline with concurrent processing
pub struct EventPipeline {
    processor: Arc<dyn EventProcessor>,
    event_tx: mpsc::Sender<RipelEvent>,
    event_rx: Option<mpsc::Receiver<RipelEvent>>,
    buffer_size: usize,
    worker_count: usize,
}

impl EventPipeline {
    pub fn new(
        processor: Arc<dyn EventProcessor>,
        buffer_size: usize,
        worker_count: usize,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(buffer_size);
        
        Self {
            processor,
            event_tx,
            event_rx: Some(event_rx),
            buffer_size,
            worker_count,
        }
    }

    /// Get a sender for submitting events to the pipeline
    pub fn sender(&self) -> mpsc::Sender<RipelEvent> {
        self.event_tx.clone()
    }

    /// Start the processing pipeline
    #[instrument(skip(self))]
    pub async fn start(mut self) -> Result<()> {
        let event_rx = self.event_rx.take().expect("Pipeline already started");
        
        info!(
            worker_count = self.worker_count,
            buffer_size = self.buffer_size,
            "Starting event processing pipeline"
        );

        // Start the processor
        self.processor.start().await?;

        // Create worker tasks
        let mut handles = Vec::new();
        let event_rx = Arc::new(tokio::sync::Mutex::new(event_rx));
        
        for worker_id in 0..self.worker_count {
            let processor = self.processor.clone();
            let event_rx = event_rx.clone();
            
            let handle = tokio::spawn(async move {
                loop {
                    let event = {
                        let mut rx = event_rx.lock().await;
                        rx.recv().await
                    };
                    
                    match event {
                        Some(event) => {
                            if let Err(e) = processor.process(event.clone()).await {
                                error!(
                                    worker_id = worker_id,
                                    event_id = %event.id,
                                    error = %e,
                                    "Event processing failed"
                                );
                            }
                        }
                        None => {
                            info!(worker_id = worker_id, "Event channel closed, worker stopping");
                            break;
                        }
                    }
                }
            });
            
            handles.push(handle);
        }

        // Wait for all workers to complete
        for handle in handles {
            if let Err(e) = handle.await {
                error!("Worker task failed: {}", e);
            }
        }

        // Shutdown the processor
        self.processor.shutdown().await?;
        
        info!("Event processing pipeline stopped");
        Ok(())
    }
}

/// Simple logging processor for debugging and development
pub struct LoggingProcessor;

#[async_trait]
impl EventProcessor for LoggingProcessor {
    #[instrument(skip(self, event), fields(event_id = %event.id))]
    async fn process(&self, event: RipelEvent) -> Result<()> {
        info!(
            event_id = %event.id,
            event_type = %event.event_type,
            source = %event.source,
            correlation_id = %event.correlation_id,
            "Processing event"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::time::{sleep, Duration};

    struct TestProcessor {
        processed_events: Arc<tokio::sync::Mutex<Vec<RipelEvent>>>,
    }

    impl TestProcessor {
        fn new() -> Self {
            Self {
                processed_events: Arc::new(tokio::sync::Mutex::new(Vec::new())),
            }
        }

        async fn get_processed_events(&self) -> Vec<RipelEvent> {
            self.processed_events.lock().await.clone()
        }
    }

    #[async_trait]
    impl EventProcessor for TestProcessor {
        async fn process(&self, event: RipelEvent) -> Result<()> {
            self.processed_events.lock().await.push(event);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_processor_chain() {
        let processor1 = Arc::new(TestProcessor::new());
        let processor2 = Arc::new(TestProcessor::new());
        
        let chain = ProcessorChain::new()
            .add_processor(processor1.clone())
            .add_processor(processor2.clone());

        let event = RipelEvent::new("test", "source", json!({}));
        chain.process(event.clone()).await.unwrap();

        assert_eq!(processor1.get_processed_events().await.len(), 1);
        assert_eq!(processor2.get_processed_events().await.len(), 1);
    }

    #[tokio::test]
    async fn test_event_pipeline() {
        let processor = Arc::new(TestProcessor::new());
        let pipeline = EventPipeline::new(processor.clone(), 10, 2);
        
        let sender = pipeline.sender();
        
        // Start pipeline in background
        let pipeline_handle = tokio::spawn(pipeline.start());
        
        // Send some events
        for i in 0..5 {
            let event = RipelEvent::new("test", "source", json!({"index": i}));
            sender.send(event).await.unwrap();
        }
        
        // Close sender and wait for pipeline to finish
        drop(sender);
        sleep(Duration::from_millis(100)).await;
        
        pipeline_handle.abort(); // Force stop for test
        
        // Check that events were processed
        assert!(!processor.get_processed_events().await.is_empty());
    }
}