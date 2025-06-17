use rs2_stream::connectors::{KafkaConnector, StreamConnector};
use rs2_stream::connectors::kafka_connector::KafkaConfig;
use rs2_stream::rs2::*;
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
            group_id: None, // Use the connector's consumer group
            partition: None, // All partitions
            from_beginning: true,
            kafka_config: None,
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        };

        // Check if the connector is healthy
        let healthy = <KafkaConnector as StreamConnector<String>>::health_check(&connector).await.unwrap();
        if !healthy {
            println!("Kafka connector is not healthy!");
            return;
        }

        // Create a stream from Kafka
        let stream = <KafkaConnector as StreamConnector<String>>::from_source(&connector, config).await.unwrap();

        // Process the stream with RS2 transformations
        let processed_stream = stream
            .map_rs2(|msg: String| {
                println!("Received message: {}", msg);
                format!("Processed: {}", msg)
            })
            .filter_rs2(|msg: &String| !msg.contains("ignore"))
            .throttle_rs2(Duration::from_millis(100));

        // Send the processed stream back to a different Kafka topic
        let sink_config = KafkaConfig {
            topic: "output-topic".to_string(),
            group_id: None,
            partition: Some(0),
            from_beginning: false,
            kafka_config: Some({
                let mut config = HashMap::new();
                config.insert("compression.type".to_string(), "gzip".to_string());
                config
            }),
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        };

        // Send to sink
        let metadata = <KafkaConnector as StreamConnector<String>>::to_sink(&connector, processed_stream, sink_config).await.unwrap();
        println!("Processed messages sent to Kafka: {:?}", metadata);
    });
}
