# Connectors Documentation for RS2

RS2 provides a powerful connector system for integrating with external systems like Kafka, Redis, and more. Connectors allow you to create streams from external sources and send streams to external sinks.

## Core Concepts

### StreamConnector Trait

The `StreamConnector` trait is the core abstraction for all connectors in RS2. It defines the methods that all connectors must implement:

```rust
#[async_trait]
pub trait StreamConnector<T>: Send + Sync
where
    T: Send + 'static,
{
    type Config: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;
    type Metadata: Send + Sync;

    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<T>, Self::Error>;
    async fn to_sink(&self, stream: RS2Stream<T>, config: Self::Config) -> Result<Self::Metadata, Self::Error>;
    async fn health_check(&self) -> Result<bool, Self::Error>;
    async fn metadata(&self) -> Result<Self::Metadata, Self::Error>;
    fn name(&self) -> &'static str;
    fn version(&self) -> &'static str;
}
```

### BidirectionalConnector Trait

For connectors that can both produce and consume data, the `BidirectionalConnector` trait extends `StreamConnector`:

```rust
#[async_trait]
pub trait BidirectionalConnector<T>: StreamConnector<T>
where
    T: Send + 'static,
{
    async fn bidirectional(
        &self,
        input_config: Self::Config,
        output_config: Self::Config,
    ) -> Result<(RS2Stream<T>, Box<dyn Fn(RS2Stream<T>) -> Result<(), Self::Error> + Send + Sync>), Self::Error>;
}
```

### CommonConfig

RS2 provides a common configuration struct that can be used by all connectors:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommonConfig {
    pub batch_size: usize,
    pub timeout_ms: u64,
    pub retry_attempts: usize,
    pub compression: bool,
}
```

## Kafka Connector

RS2 includes a Kafka connector that allows you to create streams from Kafka topics and send streams to Kafka topics.

### KafkaConnector

The `KafkaConnector` struct implements the `StreamConnector` trait for Kafka:

```rust
pub struct KafkaConnector {
    bootstrap_servers: String,
    consumer_group: Option<String>,
}
```

### KafkaConfig

The `KafkaConfig` struct provides configuration options for the Kafka connector:

```rust
#[derive(Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    pub topic: String,
    pub partition: Option<i32>,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
    pub auto_commit: bool,
    pub auto_offset_reset: String,
    pub common: CommonConfig,
}
```

## Real-World Examples

### Basic Kafka Consumer

```rust
use rs2::connectors::{KafkaConnector, CommonConfig};
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a Kafka connector
        let connector = KafkaConnector::new("localhost:9092")
            .with_consumer_group("my-consumer-group");

        // Create a Kafka configuration
        let config = KafkaConfig {
            topic: "my-topic".to_string(),
            partition: None, // All partitions
            key: None,
            headers: HashMap::new(),
            auto_commit: true,
            auto_offset_reset: "earliest".to_string(),
            common: CommonConfig::default(),
        };

        // Check if the connector is healthy
        let healthy = connector.health_check().await.unwrap();
        if !healthy {
            println!("Kafka connector is not healthy!");
            return;
        }

        // Create a stream from Kafka
        let stream = connector.from_source(config).await.unwrap();

        // Process the stream
        stream
            .map_rs2(|msg| {
                println!("Received message: {}", msg);
                msg
            })
            .for_each_rs2(|_| async {})
            .await;
    });
}
```

### Kafka Producer with Transformations

```rust
use rs2::connectors::{KafkaConnector, CommonConfig};
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use std::time::Duration;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a Kafka connector
        let connector = KafkaConnector::new("localhost:9092");

        // Create a Kafka configuration
        let config = KafkaConfig {
            topic: "output-topic".to_string(),
            partition: Some(0), // Specific partition
            key: Some("my-key".to_string()),
            headers: HashMap::new(),
            auto_commit: true,
            auto_offset_reset: "latest".to_string(),
            common: CommonConfig {
                batch_size: 100,
                timeout_ms: 30000,
                retry_attempts: 3,
                compression: true,
            },
        };

        // Create a stream of data
        let data = vec![
            "Message 1".to_string(),
            "Message 2".to_string(),
            "Message 3".to_string(),
        ];
        let stream = from_iter(data);

        // Apply transformations
        let transformed_stream = stream
            .map_rs2(|msg| format!("Transformed: {}", msg))
            .throttle_rs2(Duration::from_millis(100)) // Rate limit
            .auto_backpressure_rs2(); // Apply backpressure

        // Send the stream to Kafka
        let metadata = connector.to_sink(transformed_stream, config).await.unwrap();
        
        println!("Sent messages to Kafka: {:?}", metadata);
    });
}
```

### Error Handling with Connectors

```rust
use rs2::connectors::{KafkaConnector, ConnectorError, CommonConfig};
use rs2::rs2::*;
use rs2::error::RetryPolicy;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use std::time::Duration;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a Kafka connector
        let connector = KafkaConnector::new("localhost:9092")
            .with_consumer_group("my-consumer-group");

        // Create a Kafka configuration
        let config = KafkaConfig {
            topic: "my-topic".to_string(),
            partition: None,
            key: None,
            headers: HashMap::new(),
            auto_commit: true,
            auto_offset_reset: "earliest".to_string(),
            common: CommonConfig::default(),
        };

        // Try to create a stream with error handling
        match connector.from_source(config.clone()).await {
            Ok(stream) => {
                // Process the stream with error handling
                let result_stream = stream
                    .map_rs2(|msg| -> Result<String, ConnectorError> {
                        // Simulate some processing that might fail
                        if msg.contains("error") {
                            Err(ConnectorError::ConnectorSpecific("Processing error".to_string()))
                        } else {
                            Ok(format!("Processed: {}", msg))
                        }
                    })
                    .retry_with_policy(
                        RetryPolicy::Exponential {
                            max_retries: 3,
                            initial_delay: Duration::from_millis(100),
                            multiplier: 2.0,
                        },
                        || {
                            // Return a new stream on retry
                            connector.from_source(config.clone())
                                .map_ok(|s| s.map_rs2(|msg| -> Result<String, ConnectorError> {
                                    if msg.contains("error") {
                                        Err(ConnectorError::ConnectorSpecific("Processing error".to_string()))
                                    } else {
                                        Ok(format!("Processed: {}", msg))
                                    }
                                }))
                                .unwrap_or_else(|e| from_iter(vec![Err(e)]))
                        }
                    );

                // Collect successful results
                let successful = result_stream
                    .collect_ok()
                    .collect::<Vec<_>>()
                    .await;

                println!("Successfully processed messages: {:?}", successful);
            },
            Err(e) => {
                println!("Failed to create stream: {}", e);
            }
        }
    });
}
```

### Bidirectional Connector Example

```rust
use rs2::connectors::{BidirectionalConnector, KafkaConnector, CommonConfig};
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a connector that implements BidirectionalConnector
        let connector = MyBidirectionalConnector::new("localhost:9092");

        // Create input and output configurations
        let input_config = MyConfig {
            source: "input-topic".to_string(),
            common: CommonConfig::default(),
        };
        
        let output_config = MyConfig {
            source: "output-topic".to_string(),
            common: CommonConfig::default(),
        };

        // Create a bidirectional stream
        let (input_stream, output_fn) = connector.bidirectional(
            input_config,
            output_config
        ).await.unwrap();

        // Process the input stream
        let processed_stream = input_stream
            .map_rs2(|msg| format!("Processed: {}", msg));

        // Send the processed stream to the output
        output_fn(processed_stream).unwrap();
    });
}

// Example implementation of a bidirectional connector
struct MyBidirectionalConnector {
    connection_string: String,
}

impl MyBidirectionalConnector {
    fn new(connection_string: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
        }
    }
}

#[derive(Clone)]
struct MyConfig {
    source: String,
    common: CommonConfig,
}

#[async_trait]
impl StreamConnector<String> for MyBidirectionalConnector {
    type Config = MyConfig;
    type Error = ConnectorError;
    type Metadata = ();

    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<String>, Self::Error> {
        // Implementation details...
        Ok(from_iter(vec!["Message 1".to_string(), "Message 2".to_string()]))
    }

    async fn to_sink(&self, stream: RS2Stream<String>, config: Self::Config) -> Result<Self::Metadata, Self::Error> {
        // Implementation details...
        Ok(())
    }

    async fn health_check(&self) -> Result<bool, Self::Error> {
        Ok(true)
    }

    async fn metadata(&self) -> Result<Self::Metadata, Self::Error> {
        Ok(())
    }

    fn name(&self) -> &'static str {
        "my-bidirectional-connector"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }
}

#[async_trait]
impl BidirectionalConnector<String> for MyBidirectionalConnector {
    async fn bidirectional(
        &self,
        input_config: Self::Config,
        output_config: Self::Config,
    ) -> Result<(RS2Stream<String>, Box<dyn Fn(RS2Stream<String>) -> Result<(), Self::Error> + Send + Sync>), Self::Error> {
        // Create the input stream
        let input_stream = self.from_source(input_config).await?;
        
        // Create a closure for the output function
        let output_config_clone = output_config.clone();
        let self_clone = self.clone();
        let output_fn = Box::new(move |stream: RS2Stream<String>| {
            // This would typically be implemented to send to the output asynchronously
            // For simplicity, we're just returning Ok(())
            Ok(())
        });
        
        Ok((input_stream, output_fn))
    }
}

impl Clone for MyBidirectionalConnector {
    fn clone(&self) -> Self {
        Self {
            connection_string: self.connection_string.clone(),
        }
    }
}
```

## Creating Custom Connectors

You can create your own connectors by implementing the `StreamConnector` trait:

```rust
use rs2::connectors::{ConnectorError, ConnectorResult, StreamConnector, CommonConfig};
use rs2::rs2::*;
use async_trait::async_trait;
use std::collections::HashMap;

// Custom connector for a hypothetical message queue
struct MyQueueConnector {
    connection_string: String,
}

// Custom configuration for the connector
#[derive(Clone)]
struct MyQueueConfig {
    queue_name: String,
    common: CommonConfig,
}

// Custom metadata for the connector
#[derive(Debug, Clone)]
struct MyQueueMetadata {
    queue_name: String,
    messages_processed: usize,
}

impl MyQueueConnector {
    fn new(connection_string: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
        }
    }
}

#[async_trait]
impl StreamConnector<String> for MyQueueConnector {
    type Config = MyQueueConfig;
    type Error = ConnectorError;
    type Metadata = MyQueueMetadata;

    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<String>, Self::Error> {
        // In a real implementation, you would connect to your message queue
        // and create a stream of messages
        println!("Connecting to {} with queue {}", self.connection_string, config.queue_name);
        
        // For this example, we'll just return a stream of mock messages
        let messages = vec![
            "Message 1".to_string(),
            "Message 2".to_string(),
            "Message 3".to_string(),
        ];
        
        Ok(from_iter(messages))
    }

    async fn to_sink(&self, stream: RS2Stream<String>, config: Self::Config) -> Result<Self::Metadata, Self::Error> {
        // In a real implementation, you would send each message in the stream
        // to your message queue
        println!("Sending to {} with queue {}", self.connection_string, config.queue_name);
        
        // For this example, we'll just count the messages
        let messages: Vec<String> = stream.collect().await;
        let count = messages.len();
        
        Ok(MyQueueMetadata {
            queue_name: config.queue_name,
            messages_processed: count,
        })
    }

    async fn health_check(&self) -> Result<bool, Self::Error> {
        // In a real implementation, you would check the health of your connection
        println!("Checking health of connection to {}", self.connection_string);
        
        // For this example, we'll just return true
        Ok(true)
    }

    async fn metadata(&self) -> Result<Self::Metadata, Self::Error> {
        // In a real implementation, you would return metadata about your connection
        Ok(MyQueueMetadata {
            queue_name: "default".to_string(),
            messages_processed: 0,
        })
    }

    fn name(&self) -> &'static str {
        "my-queue-connector"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }
}

fn main() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        // Create a custom connector
        let connector = MyQueueConnector::new("my-queue-server:1234");
        
        // Create a configuration
        let config = MyQueueConfig {
            queue_name: "my-queue".to_string(),
            common: CommonConfig::default(),
        };
        
        // Check if the connector is healthy
        let healthy = connector.health_check().await.unwrap();
        println!("Connector is healthy: {}", healthy);
        
        // Create a stream from the connector
        let stream = connector.from_source(config.clone()).await.unwrap();
        
        // Process the stream
        let processed_stream = stream
            .map_rs2(|msg| format!("Processed: {}", msg));
        
        // Send the processed stream back to the connector
        let metadata = connector.to_sink(processed_stream, config).await.unwrap();
        
        println!("Processed {} messages for queue {}", 
                 metadata.messages_processed, metadata.queue_name);
    });
}
```

This documentation provides an overview of the connector system in RS2 and examples of how to use it. For more details, refer to the API documentation.