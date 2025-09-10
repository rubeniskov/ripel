//! Error types for RIPeL core

use thiserror::Error;

#[derive(Error, Debug)]
pub enum RipelError {
    #[error("Event processing error: {0}")]
    ProcessingError(String),

    #[error("Stream error: {0}")]
    StreamError(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Database error: {0}")]
    DatabaseError(String),

    #[error("Kafka error: {0}")]
    KafkaError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Network error: {0}")]
    NetworkError(#[from] tonic::transport::Error),

    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),

    #[error("Internal error: {0}")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, RipelError>;

impl From<anyhow::Error> for RipelError {
    fn from(err: anyhow::Error) -> Self {
        RipelError::InternalError(err.to_string())
    }
}