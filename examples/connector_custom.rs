use async_trait::async_trait;
use rs2_stream::stream::constructors::from_iter;
use rs2_stream::stream::{Stream, StreamExt};
use rs2_stream::connectors::stream_connector::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::future::Future;

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

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

// Implement the required trait
impl ConnectorConfig for MyQueueConfig {}

// Custom metadata for the connector
#[derive(Debug, Clone)]
struct MyQueueMetadata {
    queue_name: String,
    messages_processed: usize,
}

// Implement the required trait
impl ConnectorMetadata for MyQueueMetadata {}

// Custom error type
#[derive(Debug, Clone)]
struct MyQueueError {
    message: String,
}

impl std::fmt::Display for MyQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MyQueueError: {}", self.message)
    }
}

impl std::error::Error for MyQueueError {}

impl ConnectorError for MyQueueError {}

// Custom stream wrapper
struct MyQueueStream<T> {
    inner: Pin<Box<dyn Stream<Item = T> + Send + Sync>>,
}

impl<T> MyQueueStream<T> {
    fn new<S>(stream: S) -> Self 
    where 
        S: Stream<Item = T> + Send + Sync + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }
}

impl<T> Stream for MyQueueStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

impl MyQueueConnector {
    fn new(connection_string: &str) -> Self {
        Self {
            connection_string: connection_string.to_string(),
        }
    }
}

#[async_trait]
impl StreamConnector<String, MyQueueConfig, MyQueueMetadata, MyQueueError> for MyQueueConnector {
    type Config = MyQueueConfig;
    type Error = MyQueueError;
    type Metadata = MyQueueMetadata;
    type SourceStream = MyQueueStream<String>;
    type SinkStream = MyQueueStream<String>;

    async fn from_source(&self, config: Self::Config) -> Result<Self::SourceStream, Self::Error> {
        // In a real implementation, you would connect to your message queue
        // and create a stream of messages
        println!(
            "Connecting to {} with queue {}",
            self.connection_string, config.queue_name
        );

        // For this example, we'll just return a stream of mock messages
        let messages = vec![
            "Message 1".to_string(),
            "Message 2".to_string(),
            "Message 3".to_string(),
        ];

        Ok(MyQueueStream::new(from_iter(messages)))
    }

    async fn to_sink(
        &self,
        mut stream: Self::SinkStream,
        config: Self::Config,
    ) -> Result<Self::Metadata, Self::Error> {
        // In a real implementation, you would send each message in the stream
        // to your message queue
        println!(
            "Sending to {} with queue {}",
            self.connection_string, config.queue_name
        );

        // For this example, we'll just count the messages
        let mut messages = Vec::new();
        while let Some(item) = stream.next().await {
            messages.push(item);
        }
        let count = messages.len();

        Ok(MyQueueMetadata {
            queue_name: config.queue_name,
            messages_processed: count,
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
        let source_stream = self.from_source(config.clone()).await?;
        let sink_fn = Box::new(move |stream: Self::SinkStream| {
            Box::pin(async move {
                // Just consume the stream for demonstration
                let mut s = stream;
                while let Some(_item) = s.next().await {}
                Ok(())
            }) as BoxFuture<'static, Result<(), Self::Error>>
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
        Ok(())
    }

    async fn health_check(&self) -> Result<(), Self::Error> {
        // In a real implementation, you would check the health of your connection
        Ok(())
    }

    async fn get_stats(&self) -> Result<ConnectorStats, Self::Error> {
        // In a real implementation, you would return metadata about your connection
        Ok(ConnectorStats {
            messages_sent: 0,
            messages_received: 0,
            errors: 0,
            last_error: None,
            uptime: std::time::Duration::ZERO,
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Custom Connector Example ===\n");

    // Create a custom connector
    let connector = MyQueueConnector::new("my-queue-server:1234");

    // Create a configuration
    let config = MyQueueConfig {
        queue_name: "my-queue".to_string(),
        common: CommonConfig::default(),
    };

    // Test connector capabilities
    let capabilities = connector.capabilities();
    println!("Connector capabilities:");
    println!("  - Supports source: {}", capabilities.supports_source);
    println!("  - Supports sink: {}", capabilities.supports_sink);
    println!("  - Supports bidirectional: {}", capabilities.supports_bidirectional);
    println!("  - Max buffer size: {:?}", capabilities.max_buffer_size);

    // Test health check
    let health = connector.health_check().await;
    println!("Health check: {:?}", health);

    // Create a stream from the connector
    let stream = connector.from_source(config.clone()).await?;

    // Process the stream using our custom API
    let processed_stream = MyQueueStream::new(stream.map_rs2(|msg| format!("Processed: {}", msg)));

    // Send the processed stream back to the connector
    let metadata = connector.to_sink(processed_stream, config.clone()).await?;

    println!(
        "\nProcessed {} messages for queue {}",
        metadata.messages_processed, metadata.queue_name
    );

    // Test bidirectional functionality
    println!("\nTesting bidirectional functionality...");
    let (source_stream, sink_fn) = connector.bidirectional(config.clone()).await?;
    
    // Process the source stream
    let processed_bidirectional = MyQueueStream::new(source_stream.map_rs2(|msg| format!("Bidirectional: {}", msg)));
    
    // Send to sink
    let _ = sink_fn(processed_bidirectional).await?;
    println!("Bidirectional test completed successfully!");

    println!("\n=== Custom Connector Example Completed! ===");
    Ok(())
}
