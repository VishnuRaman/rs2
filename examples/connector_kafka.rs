use rs2_stream::connectors::kafka_connector::{KafkaConfig, KafkaMetadata};
use rs2_stream::connectors::{KafkaConnector, StreamConnector};
use rs2_stream::connectors::connection_errors::ConnectorError;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::stream::constructors::from_iter;
use std::collections::HashMap;
use std::time::Duration;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🚀 Starting Kafka Connector Example");
        
        // Create a Kafka connector
        let connector =
            KafkaConnector::new("localhost:9092").with_consumer_group("my-consumer-group");

        // Create a Kafka configuration for consuming
        let consumer_config = KafkaConfig {
            topic: "my-topic".to_string(),
            group_id: None,  // Use the connector's consumer group
            partition: None, // All partitions
            from_beginning: true,
            kafka_config: None,
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        };

        // Check if the connector is healthy
        println!("📋 Checking Kafka connector health...");
        let health = <KafkaConnector as StreamConnector<String, KafkaConfig, KafkaMetadata, ConnectorError>>::health_check(&connector)
            .await;
        if health.is_err() {
            println!("❌ Kafka connector is not healthy: {:?}", health);
            return;
        }
        println!("✅ Kafka connector is healthy!");

        // For demonstration, we'll create a sample stream since real Kafka might not be running
        // In production, you would use the actual Kafka stream:
        // let stream = connector.from_source(consumer_config).await.unwrap();
        
        println!("📊 Creating sample data stream (simulating Kafka messages)...");
        let sample_messages = vec![
            "message-1".to_string(),
            "ignore-this-message".to_string(),
            "message-2".to_string(),
            "important-data".to_string(),
            "ignore-again".to_string(),
            "final-message".to_string(),
        ];
        
        let source_stream = from_iter(sample_messages);

        // Process the stream with RS2 transformations
        println!("🔄 Processing stream with RS2 transformations...");
        let processed_stream = source_stream
            .map_rs2(|msg: String| {
                println!("  📨 Received message: {}", msg);
                format!("Processed: {}", msg)
            })
            .filter_rs2(|msg: &String| !msg.contains("ignore"))
            .throttle_rs2(Duration::from_millis(100));

        // Collect processed messages for demonstration
        let processed_messages = processed_stream.collect_rs2().await;
        println!("✨ Processed {} messages", processed_messages.len());
        for msg in &processed_messages {
            println!("  ✅ {}", msg);
        }

        // Send the processed messages to a different Kafka topic
        println!("📤 Sending processed messages to output topic...");
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

        // Create a new stream from processed messages and box it for the connector
        let output_stream = from_iter(processed_messages);
        let boxed_stream = Box::new(output_stream);

        // Send to sink
        let metadata = <KafkaConnector as StreamConnector<String, KafkaConfig, KafkaMetadata, ConnectorError>>::to_sink(
            &connector,
            boxed_stream,
            sink_config,
        )
        .await;
        
        match metadata {
            Ok(meta) => println!("🎉 Messages sent to Kafka successfully: {:?}", meta),
            Err(e) => println!("❌ Failed to send messages to Kafka: {:?}", e),
        }
        
        println!("✅ Kafka Connector Example completed!");
    });
}
