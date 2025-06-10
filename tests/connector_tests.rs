use rs2::connectors::{ConnectorError, StreamConnector, CommonConfig};
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use async_trait::async_trait;

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

// Mock metadata for testing
#[derive(Debug, Clone)]
struct MockMetadata {
    topic: String,
    messages_processed: usize,
}

#[async_trait]
impl StreamConnector<String> for MockConnector {
    type Config = MockConfig;
    type Error = ConnectorError;
    type Metadata = MockMetadata;

    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<String>, Self::Error> {
        if !self.healthy {
            return Err(ConnectorError::ConnectionFailed("Mock connector is unhealthy".to_string()));
        }

        // Create a stream of mock messages
        let messages = vec![
            format!("Message 1 from {}", config.topic),
            format!("Message 2 from {}", config.topic),
            format!("Message 3 from {}", config.topic),
        ];

        Ok(from_iter(messages))
    }

    async fn to_sink(&self, stream: RS2Stream<String>, config: Self::Config) -> Result<Self::Metadata, Self::Error> {
        if !self.healthy {
            return Err(ConnectorError::ConnectionFailed("Mock connector is unhealthy".to_string()));
        }

        // Count the messages in the stream
        let messages: Vec<String> = stream.collect().await;
        let count = messages.len();

        Ok(MockMetadata {
            topic: config.topic,
            messages_processed: count,
        })
    }

    async fn health_check(&self) -> Result<bool, Self::Error> {
        Ok(self.healthy)
    }

    async fn metadata(&self) -> Result<Self::Metadata, Self::Error> {
        if !self.healthy {
            return Err(ConnectorError::ConnectionFailed("Mock connector is unhealthy".to_string()));
        }

        Ok(MockMetadata {
            topic: "mock-topic".to_string(),
            messages_processed: 0,
        })
    }

    fn name(&self) -> &'static str {
        self.name
    }

    fn version(&self) -> &'static str {
        self.version
    }
}

#[test]
fn test_mock_connector_healthy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a healthy mock connector
        let connector = MockConnector {
            name: "mock-connector",
            version: "1.0.0",
            healthy: true,
        };

        // Test health check
        let health = connector.health_check().await.unwrap();
        assert!(health);

        // Test metadata
        let metadata = connector.metadata().await.unwrap();
        assert_eq!(metadata.topic, "mock-topic");
        assert_eq!(metadata.messages_processed, 0);

        // Test name and version
        assert_eq!(connector.name(), "mock-connector");
        assert_eq!(connector.version(), "1.0.0");
    });
}

#[test]
fn test_mock_connector_unhealthy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an unhealthy mock connector
        let connector = MockConnector {
            name: "mock-connector",
            version: "1.0.0",
            healthy: false,
        };

        // Test health check
        let health = connector.health_check().await.unwrap();
        assert!(!health);

        // Test metadata should fail
        let metadata_result = connector.metadata().await;
        assert!(metadata_result.is_err());
        if let Err(ConnectorError::ConnectionFailed(msg)) = metadata_result {
            assert_eq!(msg, "Mock connector is unhealthy");
        } else {
            panic!("Expected ConnectionFailed error");
        }
    });
}

#[test]
fn test_mock_connector_source() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a healthy mock connector
        let connector = MockConnector {
            name: "mock-connector",
            version: "1.0.0",
            healthy: true,
        };

        // Create a mock config
        let config = MockConfig {
            topic: "test-topic".to_string(),
            common: CommonConfig::default(),
        };

        // Test from_source
        let stream = connector.from_source(config).await.unwrap();
        let messages: Vec<String> = stream.collect().await;

        // Verify the messages
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
        // Create a healthy mock connector
        let connector = MockConnector {
            name: "mock-connector",
            version: "1.0.0",
            healthy: true,
        };

        // Create a mock config
        let config = MockConfig {
            topic: "test-topic".to_string(),
            common: CommonConfig::default(),
        };

        // Create a stream of messages
        let messages = vec![
            "Test message 1".to_string(),
            "Test message 2".to_string(),
            "Test message 3".to_string(),
            "Test message 4".to_string(),
        ];
        let stream = from_iter(messages);

        // Test to_sink
        let metadata = connector.to_sink(stream, config).await.unwrap();

        // Verify the metadata
        assert_eq!(metadata.topic, "test-topic");
        assert_eq!(metadata.messages_processed, 4);
    });
}

#[test]
fn test_mock_connector_source_unhealthy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an unhealthy mock connector
        let connector = MockConnector {
            name: "mock-connector",
            version: "1.0.0",
            healthy: false,
        };

        // Create a mock config
        let config = MockConfig {
            topic: "test-topic".to_string(),
            common: CommonConfig::default(),
        };

        // Test from_source should fail
        let result = connector.from_source(config).await;
        assert!(result.is_err());
        if let Err(ConnectorError::ConnectionFailed(msg)) = result {
            assert_eq!(msg, "Mock connector is unhealthy");
        } else {
            panic!("Expected ConnectionFailed error");
        }
    });
}

#[test]
fn test_mock_connector_sink_unhealthy() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create an unhealthy mock connector
        let connector = MockConnector {
            name: "mock-connector",
            version: "1.0.0",
            healthy: false,
        };

        // Create a mock config
        let config = MockConfig {
            topic: "test-topic".to_string(),
            common: CommonConfig::default(),
        };

        // Create a stream of messages
        let messages = vec![
            "Test message 1".to_string(),
            "Test message 2".to_string(),
        ];
        let stream = from_iter(messages);

        // Test to_sink should fail
        let result = connector.to_sink(stream, config).await;
        assert!(result.is_err());
        if let Err(ConnectorError::ConnectionFailed(msg)) = result {
            assert_eq!(msg, "Mock connector is unhealthy");
        } else {
            panic!("Expected ConnectionFailed error");
        }
    });
}

// Test stream transformations with connectors
#[test]
fn test_connector_with_transformations() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a healthy mock connector
        let connector = MockConnector {
            name: "mock-connector",
            version: "1.0.0",
            healthy: true,
        };

        // Create a mock config
        let config = MockConfig {
            topic: "test-topic".to_string(),
            common: CommonConfig::default(),
        };

        // Get a stream from the connector
        let stream = connector.from_source(config.clone()).await.unwrap();

        // Apply transformations
        let transformed_stream = stream
            .map_rs2(|msg| format!("Transformed: {}", msg))
            .filter_rs2(|msg| msg.contains("Message 2"))
            .collect::<Vec<_>>()
            .await;

        // Verify the transformed stream
        assert_eq!(transformed_stream.len(), 1);
        assert_eq!(transformed_stream[0], "Transformed: Message 2 from test-topic");

        // Create a new stream with transformations and send to sink
        let new_stream = from_iter(vec![
            "Input 1".to_string(),
            "Input 2".to_string(),
            "Input 3".to_string(),
        ])
        .map_rs2(|msg| format!("Processed: {}", msg));

        // Send to sink
        let metadata = connector.to_sink(new_stream, config).await.unwrap();

        // Verify the metadata
        assert_eq!(metadata.topic, "test-topic");
        assert_eq!(metadata.messages_processed, 3);
    });
}
