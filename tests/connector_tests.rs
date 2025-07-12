use async_trait::async_trait;
use rs2_stream::connectors::stream_connector::{ConnectorConfig, ConnectorMetadata, ConnectorError as StreamConnectorError, ConnectorCapabilities, ConnectorStats, StreamConnector};
use rs2_stream::connectors::CommonConfig;
use tokio::runtime::Runtime;
use rs2_stream::stream::{from_iter, Stream};
use std::sync::Arc;

use std::task::{Context, Poll, Waker, RawWaker, RawWakerVTable};
use std::pin::Pin;

// Helper to create a no-op waker
fn dummy_waker() -> Waker {
    fn no_op(_: *const ()) {}
    fn clone(_: *const ()) -> RawWaker { dummy_raw_waker() }
    fn dummy_raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &RawWakerVTable::new(clone, no_op, no_op, no_op))
    }
    unsafe { Waker::from_raw(dummy_raw_waker()) }
}

// Helper function to collect from Box<dyn Stream>
async fn collect_from_box_stream<T>(mut stream: Box<dyn Stream<Item = T> + Send + Sync>) -> Vec<T>
where
    T: Send + 'static,
{
    let mut messages = Vec::new();
    let waker = dummy_waker();
    let mut cx = Context::from_waker(&waker);

    // SAFETY: We own the Box, so this is safe.
    let mut pinned: Pin<&mut (dyn Stream<Item = T> + Send + Sync)> = unsafe { Pin::new_unchecked(&mut *stream) };

    loop {
        match pinned.as_mut().poll_next(&mut cx) {
            Poll::Ready(Some(item)) => messages.push(item),
            Poll::Ready(None) => break,
            Poll::Pending => {
                // Use a busy-wait approach instead of yielding
                // This avoids .await and keeps the function Send
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }
    messages
}

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

#[async_trait]
impl StreamConnector<String, MockConfig, MockMetadata, MockError> for MockConnector {
    type Config = MockConfig;
    type Metadata = MockMetadata;
    type Error = MockError;

    async fn from_source(&self, config: Self::Config) -> Result<Box<dyn Stream<Item = String> + Send + Sync>, Self::Error> {
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
        Ok(Box::new(from_iter(messages)))
    }

    async fn to_sink(
        &self,
        stream: Box<dyn Stream<Item = String> + Send + Sync>,
        config: Self::Config,
    ) -> Result<Self::Metadata, Self::Error> {
        if !self.healthy {
            return Err(MockError {
                message: "Mock connector is unhealthy".to_string(),
            });
        }
        // Collect all items from the stream
        let messages: Vec<String> = collect_from_box_stream(stream).await;
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
            Box<dyn Stream<Item = String> + Send + Sync>,
            Box<dyn Fn(Box<dyn Stream<Item = String> + Send + Sync>) -> Result<(), Self::Error> + Send + Sync>,
        ),
        Self::Error,
    > {
        if !self.healthy {
            return Err(MockError {
                message: "Mock connector is unhealthy".to_string(),
            });
        }
        let source_stream = self.from_source(config.clone()).await?;
        let sink_fn = Box::new(move |_stream: Box<dyn Stream<Item = String> + Send + Sync>| Ok(()));
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
        let stream = connector.from_source(config).await.unwrap();
        let messages: Vec<String> = collect_from_box_stream(stream).await;
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
        let stream = Box::new(from_iter(messages));
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
        let messages = vec!["Test message 1".to_string(), "Test message 2".to_string()];
        let stream = Box::new(from_iter(messages));
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
        let stream = connector.from_source(config.clone()).await.unwrap();
        let messages: Vec<String> = collect_from_box_stream(stream).await;
        assert_eq!(messages.len(), 3);
        assert_eq!(messages[0], "Message 1 from test-topic");
        assert_eq!(messages[1], "Message 2 from test-topic");
        assert_eq!(messages[2], "Message 3 from test-topic");
        let new_stream = Box::new(from_iter(vec![
            "Input 1".to_string(),
            "Input 2".to_string(),
            "Input 3".to_string(),
        ]));
        let metadata = connector.to_sink(new_stream, config).await.unwrap();
        assert_eq!(metadata.topic, "test-topic");
        assert_eq!(metadata.messages_processed, 3);
    });
}
