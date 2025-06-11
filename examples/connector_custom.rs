use rs2::connectors::{ConnectorError, StreamConnector, CommonConfig};
use rs2::rs2::*;
use async_trait::async_trait;
use futures_util::stream::StreamExt;

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
