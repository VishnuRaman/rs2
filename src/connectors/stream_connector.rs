//! Core traits for stream connectors

use async_trait::async_trait;
use serde::{Serialize, Deserialize};
use std::time::Duration;
use crate::RS2Stream;

/// Common configuration for all connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonConfig {
    /// Batch size for processing
    pub batch_size: usize,
    /// Timeout for operations in milliseconds
    pub timeout_ms: u64,
    /// Number of retry attempts
    pub retry_attempts: usize,
    /// Enable compression
    pub compression: bool,
}

impl Default for CommonConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            timeout_ms: 30000,
            retry_attempts: 3,
            compression: false,
        }
    }
}

/// Main trait for stream connectors
#[async_trait]
pub trait StreamConnector<T>: Send + Sync
where
    T: Send + 'static,
{
    /// Configuration type for this connector
    type Config: Send + Sync;

    /// Error type for this connector
    type Error: std::error::Error + Send + Sync + 'static;

    /// Metadata type returned by operations
    type Metadata: Send + Sync;

    /// Create a source stream from the connector
    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<T>, Self::Error>;

    /// Send a stream to the connector as a sink
    async fn to_sink(&self, stream: RS2Stream<T>, config: Self::Config) -> Result<Self::Metadata, Self::Error>;

    /// Check if the connector is healthy
    async fn health_check(&self) -> Result<bool, Self::Error>;

    /// Get connector metadata
    async fn metadata(&self) -> Result<Self::Metadata, Self::Error>;

    /// Get connector name
    fn name(&self) -> &'static str;

    /// Get connector version
    fn version(&self) -> &'static str;
}

/// Trait for bidirectional connectors (can both produce and consume)
#[async_trait]
pub trait BidirectionalConnector<T>: StreamConnector<T>
where
    T: Send + 'static,
{
    /// Create a bidirectional stream (source and sink combined)
    async fn bidirectional(
        &self,
        input_config: Self::Config,
        output_config: Self::Config,
    ) -> Result<(RS2Stream<T>, Box<dyn Fn(RS2Stream<T>) -> Result<(), Self::Error> + Send + Sync>), Self::Error>;
}

/// Health status for connectors
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub healthy: bool,
    pub message: String,
    pub last_check: Duration,
}

/// Connection statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub errors: u64,
    pub uptime: Duration,
}