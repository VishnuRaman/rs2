use async_trait::async_trait;
use rs2_stream::connectors::stream_connector::{ConnectorConfig, ConnectorMetadata, ConnectorError as StreamConnectorError, ConnectorCapabilities, ConnectorStats, StreamConnector};
use rs2_stream::connectors::CommonConfig;
use tokio::runtime::Runtime;
use rs2_stream::stream::{from_iter, Stream, StreamExt};
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// Mock connector for testing
struct MockConnector {
    name: &'static str,
    version: &'static str,
    healthy: bool,
}

// Mock configuration for testing
#[derive(Clone)]
struct MockConfig {
    topic: String,
    common: CommonConfig,
}
impl ConnectorConfig for MockConfig {}

// Mock metadata for testing
#[derive(Debug, Clone)]
struct MockMetadata {
    topic: String,
    messages_processed: usize,
}
impl ConnectorMetadata for MockMetadata {}

// Mock error type for testing
#[derive(Debug, Clone, PartialEq, Eq)]
struct MockError {
    message: String,
}
impl std::fmt::Display for MockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MockError: {}", self.message)
    }
}
impl std::error::Error for MockError {}
impl StreamConnectorError for MockError {}

// Wrapper type for mock streams
pub struct MockStream<T> {
    inner: Pin<Box<dyn Stream<Item = T> + Send + Sync>>,
}

impl<T> MockStream<T> {
    fn new<S>(stream: S) -> Self 
    where 
        S: Stream<Item = T> + Send + Sync + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<T> Stream for MockStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        unsafe {
            let this = self.get_unchecked_mut();
            this.inner.as_mut().poll_next(cx)
        }
    }
}

#[async_trait]
impl StreamConnector<String, MockConfig, MockMetadata, MockError> for MockConnector {
    type Config = MockConfig;
    type Metadata = MockMetadata;
    type Error = MockError;
    type SourceStream = MockStream<String>;
    type SinkStream = MockStream<String>;

    async fn from_source(&self, config: Self::Config) -> Result<Self::SourceStream, Self::Error> {
        if !self.healthy {
            return Err(MockError {
                message: "Mock connector is unhealthy".to_string(),
            });
        }
        let messages = vec![
            format!("Message 1 from {}", config.topic),
            format!("Message 2 from {}", config.topic),
            format!("Message 3 from {}", config.topic),
        ];
        Ok(MockStream::new(from_iter(messages)))
    }

    async fn to_sink(
        &self,
        mut stream: Self::SinkStream,
        config: Self::Config,
    ) -> Result<Self::Metadata, Self::Error> {
        if !self.healthy {
            return Err(MockError {
                message: "Mock connector is unhealthy".to_string(),
            });
        }
        // Collect all items from the stream using our custom stream methods
        let mut messages = Vec::new();
        while let Some(item) = stream.next().await {
            messages.push(item);
        }
        Ok(MockMetadata {
            topic: config.topic,
            messages_processed: messages.len(),
        })
    }

    async fn bidirectional(
        &self,
        config: Self::Config,
    ) -> Result<
        (
            Self::SourceStream,
            Box<dyn Fn(Self::SinkStream) -> BoxFuture<'static, Result<(), Self::Error>> + Send + Sync>,
        ),
        Self::Error,
    > {
        if !self.healthy {
            return Err(MockError {
                message: "Mock connector is unhealthy".to_string(),
            });
        }
        let source_stream = self.from_source(config.clone()).await?;
        let sink_fn = Box::new(move |_stream: Self::SinkStream| {
            Box::pin(async move { Ok(()) }) as BoxFuture<'static, Result<(), Self::Error>>
        });
        Ok((source_stream, sink_fn))
    }

    fn capabilities(&self) -> ConnectorCapabilities {
        ConnectorCapabilities {
            supports_source: true,
            supports_sink: true,
            supports_bidirectional: true,
            max_buffer_size: Some(1000),
            supports_retry: true,
            supports_backpressure: true,
        }
    }

    fn validate_config(&self, _config: &Self::Config) -> Result<(), Self::Error> {
        if !self.healthy {
            return Err(MockError {
                message: "Mock connector is unhealthy".to_string(),
            });
        }
        Ok(())
    }

    async fn health_check(&self) -> Result<(), Self::Error> {
        if !self.healthy {
            return Err(MockError {
                message: "Mock connector is unhealthy".to_string(),
            });
        }
        Ok(())
    }

    async fn get_stats(&self) -> Result<ConnectorStats, Self::Error> {
        if !self.healthy {
            return Err(MockError {
                message: "Mock connector is unhealthy".to_string(),
            });
        }
        Ok(ConnectorStats {
            messages_sent: 0,
            messages_received: 0,
            errors: 0,
            last_error: None,
            uptime: std::time::Duration::ZERO,
        })
    }
}

#[test]
fn test_mock_connector_healthy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let connector = MockConnector { name: "mock-connector", version: "1.0.0", healthy: true };
        let health = connector.health_check().await;
        assert!(health.is_ok());
        let capabilities = connector.capabilities();
        assert!(capabilities.supports_source);
        assert!(capabilities.supports_sink);
        assert!(capabilities.supports_bidirectional);
        let stats = connector.get_stats().await.unwrap();
        assert_eq!(stats.messages_sent, 0);
        assert_eq!(stats.messages_received, 0);
        assert_eq!(stats.errors, 0);
    });
}

#[test]
fn test_mock_connector_unhealthy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let connector = MockConnector { name: "mock-connector", version: "1.0.0", healthy: false };
        let health = connector.health_check().await;
        assert!(health.is_err());
        let stats_result = connector.get_stats().await;
        assert!(stats_result.is_err());
        if let Err(MockError { message }) = stats_result {
            assert_eq!(message, "Mock connector is unhealthy");
        } else {
            panic!("Expected MockError");
        }
    });
}

#[test]
fn test_mock_connector_source() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let connector = MockConnector { name: "mock-connector", version: "1.0.0", healthy: true };
        let config = MockConfig { topic: "test-topic".to_string(), common: CommonConfig::default() };
        let mut stream = connector.from_source(config).await.unwrap();
        let mut messages = Vec::new();
        while let Some(item) = stream.next().await {
            messages.push(item);
        }
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0], "Message 1 from test-topic");
        assert_eq!(messages[1], "Message 2 from test-topic");
        assert_eq!(messages[2], "Message 3 from test-topic");
    });
}

#[test]
fn test_mock_connector_sink() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let connector = MockConnector { name: "mock-connector", version: "1.0.0", healthy: true };
        let config = MockConfig { topic: "test-topic".to_string(), common: CommonConfig::default() };
        let messages = vec![
            "Test message 1".to_string(),
            "Test message 2".to_string(),
            "Test message 3".to_string(),
            "Test message 4".to_string(),
        ];
        let stream = MockStream::new(from_iter(messages));
        let metadata = connector.to_sink(stream, config).await.unwrap();
        assert_eq!(metadata.topic, "test-topic");
        assert_eq!(metadata.messages_processed, 4);
    });
}

#[test]
fn test_mock_connector_source_unhealthy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let connector = MockConnector { name: "mock-connector", version: "1.0.0", healthy: false };
        let config = MockConfig { topic: "test-topic".to_string(), common: CommonConfig::default() };
        let result = connector.from_source(config).await;
        assert!(result.is_err());
        if let Err(MockError { message }) = result {
            assert_eq!(message, "Mock connector is unhealthy");
        } else {
            panic!("Expected MockError");
        }
    });
}

#[test]
fn test_mock_connector_sink_unhealthy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let connector = MockConnector { name: "mock-connector", version: "1.0.0", healthy: false };
        let config = MockConfig { topic: "test-topic".to_string(), common: CommonConfig::default() };
        let messages = vec!["Test message".to_string()];
        let stream = MockStream::new(from_iter(messages));
        let result = connector.to_sink(stream, config).await;
        assert!(result.is_err());
        if let Err(MockError { message }) = result {
            assert_eq!(message, "Mock connector is unhealthy");
        } else {
            panic!("Expected MockError");
        }
    });
}

#[test]
fn test_connector_with_transformations() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let connector = MockConnector { name: "mock-connector", version: "1.0.0", healthy: true };
        let config = MockConfig { topic: "test-topic".to_string(), common: CommonConfig::default() };
        
        // Test source with transformations
        let mut stream = connector.from_source(config.clone()).await.unwrap();
        let mut messages = Vec::new();
        while let Some(item) = stream.next().await {
            messages.push(item);
        }
        
        // Verify we got the expected messages
        assert_eq!(messages.len(), 3);
        assert!(messages.iter().all(|msg| msg.contains("test-topic")));
        
        // Test sink with transformations
        let transformed_messages: Vec<String> = messages.into_iter()
            .map(|msg| format!("TRANSFORMED: {}", msg))
            .collect();
        
        let sink_stream = MockStream::new(from_iter(transformed_messages));
        let metadata = connector.to_sink(sink_stream, config).await.unwrap();
        assert_eq!(metadata.messages_processed, 3);
    });
}
