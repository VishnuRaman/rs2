//! Core traits for stream connectors

use crate::stream::Stream;
use crate::error::RetryPolicy;
use async_trait::async_trait;
use std::pin::Pin;
use std::future::Future;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Configuration for stream connectors
pub trait ConnectorConfig: Send + Sync + Clone + 'static {}

/// Metadata returned by connector operations
pub trait ConnectorMetadata: Send + Sync + Clone + 'static {}

/// Error types for connector operations
pub trait ConnectorError: std::error::Error + Send + Sync + Clone + 'static {}

/// Core trait for stream connectors
#[async_trait]
pub trait StreamConnector<T, C: ConnectorConfig, M: ConnectorMetadata, E: std::error::Error + Send + Sync>
where
    T: Send + 'static,
    C: ConnectorConfig,
    M: ConnectorMetadata,
    E: ConnectorError,
{
    type Config: ConnectorConfig;
    type Metadata: ConnectorMetadata;
    type Error: ConnectorError;
    type SourceStream: Stream<Item = T> + Send + 'static;
    type SinkStream: Stream<Item = T> + Send + 'static;

    /// Create a source stream from the connector
    async fn from_source(&self, config: Self::Config) -> Result<Self::SourceStream, Self::Error>;

    /// Send a stream to the connector as a sink
    async fn to_sink(
        &self,
        stream: Self::SinkStream,
        config: Self::Config,
    ) -> Result<Self::Metadata, Self::Error>;

    /// Create a bidirectional stream (source + sink)
    async fn bidirectional(
        &self,
        config: Self::Config,
    ) -> Result<
        (
            Self::SourceStream,
            Box<dyn Fn(Self::SinkStream) -> BoxFuture<'static, Result<(), Self::Error>> + Send + Sync>,
        ),
        Self::Error,
    >;

    /// Get connector capabilities
    fn capabilities(&self) -> ConnectorCapabilities;

    /// Validate configuration
    fn validate_config(&self, config: &Self::Config) -> Result<(), Self::Error>;

    /// Check if the connector is healthy
    async fn health_check(&self) -> Result<(), Self::Error>;

    /// Get connector statistics
    async fn get_stats(&self) -> Result<ConnectorStats, Self::Error>;
}

/// Capabilities of a stream connector
#[derive(Debug, Clone)]
pub struct ConnectorCapabilities {
    pub supports_source: bool,
    pub supports_sink: bool,
    pub supports_bidirectional: bool,
    pub max_buffer_size: Option<usize>,
    pub supports_retry: bool,
    pub supports_backpressure: bool,
}

impl Default for ConnectorCapabilities {
    fn default() -> Self {
        Self {
            supports_source: true,
            supports_sink: true,
            supports_bidirectional: false,
            max_buffer_size: None,
            supports_retry: true,
            supports_backpressure: true,
        }
    }
}

/// Retry configuration for connectors
#[derive(Debug, Clone)]
pub struct ConnectorRetryConfig {
    pub policy: RetryPolicy,
    pub max_attempts: usize,
    pub backoff_multiplier: f64,
}

impl Default for ConnectorRetryConfig {
    fn default() -> Self {
        Self {
            policy: RetryPolicy::Immediate { max_retries: 3 },
            max_attempts: 3,
            backoff_multiplier: 2.0,
        }
    }
}

/// Statistics for a connector
#[derive(Debug, Clone)]
pub struct ConnectorStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub errors: u64,
    pub last_error: Option<String>,
    pub uptime: std::time::Duration,
}

impl Default for ConnectorStats {
    fn default() -> Self {
        Self {
            messages_sent: 0,
            messages_received: 0,
            errors: 0,
            last_error: None,
            uptime: std::time::Duration::ZERO,
        }
    }
}

/// Common configuration for all connectors
#[derive(Debug, Clone)]
pub struct CommonConfig {
    pub buffer_size: usize,
    pub timeout: std::time::Duration,
    pub retry_config: ConnectorRetryConfig,
    pub enable_metrics: bool,
}

impl Default for CommonConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1000,
            timeout: std::time::Duration::from_secs(30),
            retry_config: ConnectorRetryConfig::default(),
            enable_metrics: false,
        }
    }
}
