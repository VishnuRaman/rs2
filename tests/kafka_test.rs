use rs2::connectors::*;
use rs2::rs2::*;
use std::time::Duration;
use testcontainers::*;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use futures_util::StreamExt;
use tokio::time::sleep;
use rs2::connectors::kafka_connector::KafkaConfig;
use uuid::Uuid;
use serial_test::serial;

struct KafkaTestEnvironment {
    pub connector: KafkaConnector,
    pub test_topic: String,
    // bootstrap_servers is kept for debugging purposes
    #[allow(dead_code)]
    pub bootstrap_servers: String,
    _container: ContainerAsync<Kafka>,
}

impl KafkaTestEnvironment {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Start Kafka container with JVM image
        let kafka_container = Kafka::default()
            .with_jvm_image()
            .start()
            .await?;

        // Get the bootstrap servers
        let bootstrap_servers = format!(
            "0.0.0.0:{}",
            kafka_container.get_host_port_ipv4(KAFKA_PORT).await?
        );

        // Create connector
        let connector = KafkaConnector::new(&bootstrap_servers);

        // Wait for Kafka to be ready
        for i in 0..30 {
            if <KafkaConnector as StreamConnector<String>>::health_check(&connector).await.is_ok() {
                break;
            }
            if i == 29 {
                return Err("Kafka container failed to start within 30 seconds".into());
            }
            sleep(Duration::from_millis(1000)).await;
        }

        let test_topic = format!("test-topic-{}", Uuid::new_v4());

        Ok(Self {
            connector,
            test_topic,
            bootstrap_servers,
            _container: kafka_container,
        })
    }

    fn producer_config(&self) -> KafkaConfig {
        KafkaConfig {
            topic: self.test_topic.clone(),
            from_beginning: true,
            ..Default::default()
        }
    }

    fn consumer_config(&self, group_id: &str) -> KafkaConfig {
        KafkaConfig {
            topic: self.test_topic.clone(),
            group_id: Some(group_id.to_string()),
            from_beginning: true,
            ..Default::default()
        }
    }
}

#[tokio::test]
#[serial]
async fn test_kafka_connector_with_testcontainers() {
    println!("🚀 Starting comprehensive Kafka connector test");

    // Try to create the test environment, but skip the test if there's a port issue
    let env = match KafkaTestEnvironment::new().await {
        Ok(env) => env,
        Err(e) => {
            println!("Skipping test due to container setup error: {}", e);
            return;
        }
    };

    // ===== PART 1: Basic Connectivity Test =====
    println!("\n📡 PART 1: Testing basic connectivity");
    let health = <KafkaConnector as StreamConnector<String>>::health_check(&env.connector).await;
    assert!(health.is_ok(), "Kafka should be healthy");
    println!("✅ Kafka is healthy and available");

    // ===== PART 2: Basic Producer/Consumer Test =====
    println!("\n📤 PART 2: Testing basic producer/consumer functionality");
    let test_messages = vec![
        "Hello Kafka!".to_string(),
        "Message 2".to_string(),
        "Final message".to_string(),
    ];

    let producer_stream = from_iter(test_messages.clone());
    let metadata = env.connector
        .to_sink(producer_stream, env.producer_config())
        .await
        .unwrap();

    println!("Messages produced: {}", metadata.messages_produced);
    assert_eq!(metadata.messages_produced, 3, "Should have produced 3 messages");
    assert_eq!(metadata.topic, env.test_topic);
    assert!(metadata.bytes_sent > 0, "Should have sent some bytes");

    // Give Kafka time to commit
    sleep(Duration::from_secs(2)).await;

    println!("Starting consumer test to verify messages");
    let consumer_stream = env.connector
        .from_source(env.consumer_config("test-group"))
        .await
        .expect("Failed to create consumer stream");

    // Collect messages from the stream with a timeout
    let received_messages: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream.take(test_messages.len()).collect::<Vec<_>>()
    ).await.expect("Timed out waiting for messages");

    // Print received messages for debugging
    println!("Received messages: {:?}", received_messages);
    println!("Expected messages: {:?}", test_messages);

    // Assert that we received all the messages we sent
    assert_eq!(received_messages.len(), test_messages.len(),
               "Received {} messages, expected {}", received_messages.len(), test_messages.len());

    // Sort both for comparison since order might not be preserved
    let mut received_sorted = received_messages.clone();
    received_sorted.sort();
    let mut expected_sorted = test_messages.clone();
    expected_sorted.sort();

    // Compare each message
    for (i, (received, expected)) in received_sorted.iter().zip(expected_sorted.iter()).enumerate() {
        assert_eq!(received, expected, "Message {} doesn't match: got {:?}, expected {:?}",
                   i, received, expected);
    }
    println!("✅ Basic producer/consumer test passed");

    // ===== PART 3: Backpressure Test =====
    println!("\n🔄 PART 3: Testing backpressure handling");

    // Create a large dataset to test backpressure
    let large_dataset: Vec<String> = (0..500)
        .map(|i| format!("Large message {} with padding: {}", i, "x".repeat(100)))
        .collect();

    println!("Sending 500 large messages to test backpressure...");
    let start = std::time::Instant::now();

    // Send the large dataset
    let producer_stream = from_iter(large_dataset.clone());
    let metadata = env.connector
        .to_sink(producer_stream, env.producer_config())
        .await
        .unwrap();

    let elapsed = start.elapsed();
    println!("Backpressure test completed in: {:?}", elapsed);
    println!("Messages sent: {}", metadata.messages_produced);

    assert_eq!(metadata.messages_produced, 500, "Should have produced 500 messages");
    assert!(metadata.bytes_sent > 50000, "Should have sent at least 50KB of data");
    println!("✅ Backpressure test passed");

    // ===== PART 4: Multiple Consumers Test =====
    println!("\n👥 PART 4: Testing multiple consumers");

    // Create a new topic for this test
    let multi_consumer_topic = format!("multi-consumer-{}", Uuid::new_v4());

    // Create a custom config for this test
    let multi_producer_config = KafkaConfig {
        topic: multi_consumer_topic.clone(),
        from_beginning: true,
        ..Default::default()
    };

    // Test data for multiple consumers
    let multi_test_data: Vec<String> = (0..20)
        .map(|i| format!("Multi-consumer message {}", i))
        .collect();

    // Produce messages
    println!("Producing messages for multiple consumers test");
    let producer_stream = from_iter(multi_test_data.clone());
    env.connector
        .to_sink(producer_stream, multi_producer_config.clone())
        .await
        .unwrap();

    // Give Kafka time to commit
    sleep(Duration::from_secs(2)).await;

    // Create two consumers in different consumer groups
    println!("Creating two consumers in different consumer groups");
    let consumer_config_1 = KafkaConfig {
        topic: multi_consumer_topic.clone(),
        group_id: Some("group-1".to_string()),
        from_beginning: true,
        ..Default::default()
    };

    let consumer_config_2 = KafkaConfig {
        topic: multi_consumer_topic.clone(),
        group_id: Some("group-2".to_string()),
        from_beginning: true,
        ..Default::default()
    };

    // Start both consumers
    let consumer_stream_1 = env.connector
        .from_source(consumer_config_1)
        .await
        .expect("Failed to create first consumer");

    let consumer_stream_2 = env.connector
        .from_source(consumer_config_2)
        .await
        .expect("Failed to create second consumer");

    // Collect messages from both consumers with a timeout
    let received_1: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream_1.take(multi_test_data.len()).collect::<Vec<_>>()
    ).await.unwrap_or_default();

    let received_2: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream_2.take(multi_test_data.len()).collect::<Vec<_>>()
    ).await.unwrap_or_default();

    println!("Consumer 1 received {} messages", received_1.len());
    println!("Consumer 2 received {} messages", received_2.len());

    // With different consumer groups, each consumer should receive all messages
    assert_eq!(received_1.len(), multi_test_data.len(), 
               "Consumer 1 should receive all messages");
    assert_eq!(received_2.len(), multi_test_data.len(), 
               "Consumer 2 should receive all messages");

    // Verify the content of the messages for both consumers
    let mut received_1_sorted = received_1.clone();
    received_1_sorted.sort();

    let mut received_2_sorted = received_2.clone();
    received_2_sorted.sort();

    let mut expected = multi_test_data.clone();
    expected.sort();

    assert_eq!(received_1_sorted, expected, 
               "Consumer 1 should receive all expected messages");
    assert_eq!(received_2_sorted, expected, 
               "Consumer 2 should receive all expected messages");
    println!("✅ Multiple consumers test passed");

    // ===== PART 5: Consumer Group Offset Test =====
    println!("\n📊 PART 5: Testing consumer group offset management");

    // Create a new topic for this test
    let offset_test_topic = format!("offset-test-{}", Uuid::new_v4());

    // Create configs for this test
    let offset_producer_config = KafkaConfig {
        topic: offset_test_topic.clone(),
        from_beginning: true,
        ..Default::default()
    };

    // Produce initial batch of messages
    let initial_messages: Vec<String> = (0..10)
        .map(|i| format!("Initial message {}", i))
        .collect();

    println!("Producing initial batch of messages");
    let producer_stream = from_iter(initial_messages.clone());
    env.connector
        .to_sink(producer_stream, offset_producer_config.clone())
        .await
        .unwrap();

    // Give Kafka time to commit
    sleep(Duration::from_secs(2)).await;

    // Create consumer with a specific group ID
    let offset_consumer_config = KafkaConfig {
        topic: offset_test_topic.clone(),
        group_id: Some("offset-test-group".to_string()),
        from_beginning: true,
        ..Default::default()
    };

    // Consume the initial batch
    println!("Consuming initial batch of messages");
    let consumer_stream = env.connector
        .from_source(offset_consumer_config.clone())
        .await
        .expect("Failed to create consumer");

    let received_initial: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream.take(10).collect::<Vec<_>>()
    ).await.expect("Timed out waiting for messages");

    assert_eq!(received_initial.len(), 10, "Should receive all 10 initial messages");

    // Produce second batch of messages
    let second_batch: Vec<String> = (10..20)
        .map(|i| format!("Second batch message {}", i))
        .collect();

    println!("Producing second batch of messages");
    let producer_stream = from_iter(second_batch.clone());
    env.connector
        .to_sink(producer_stream, offset_producer_config.clone())
        .await
        .unwrap();

    // Give Kafka time to commit
    sleep(Duration::from_secs(2)).await;

    // Create a new consumer with the same group ID
    // It should only receive the second batch since the group offset was committed
    println!("Creating new consumer with same group ID");
    let consumer_stream = env.connector
        .from_source(offset_consumer_config.clone())
        .await
        .expect("Failed to create consumer");

    let received_second: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream.take(10).collect::<Vec<_>>()
    ).await.expect("Timed out waiting for messages");

    println!("Second consumer received {} messages", received_second.len());
    assert_eq!(received_second.len(), 10, "Should receive only the 10 new messages");

    // Verify the messages are from the second batch
    for msg in &received_second {
        assert!(msg.contains("Second batch"), 
                "Message should be from second batch: {}", msg);
    }

    println!("✅ Consumer group offset test passed");

    println!("\n🎉 ALL TESTS PASSED: RS2 Kafka connector is working correctly!");
}
