use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::stream::constructors::from_iter;
use rs2_stream::stream::StreamExt;
use rs2_stream::connectors::kafka_connector::{KafkaConfig, KafkaMetadata, KafkaConnector};
use rs2_stream::connectors::connection_errors::ConnectorError;
use rs2_stream::connectors::stream_connector::StreamConnector;
use serial_test::serial;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers::*;
use testcontainers_modules::kafka::apache::{Kafka, KAFKA_PORT};
use tokio::time::sleep;
use uuid::Uuid;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::ClientConfig;
use rdkafka::config::RDKafkaLogLevel;

struct KafkaTestEnvironment {
    connector: KafkaConnector,
    test_topic: String,
    bootstrap_servers: String,
    kafka_port: u16,
    _container: ContainerAsync<Kafka>,
}

impl KafkaTestEnvironment {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        // Start Kafka container with JVM image
        let kafka_container = Kafka::default().with_jvm_image().start().await?;

        // Get the bootstrap servers
        let kafka_port = kafka_container.get_host_port_ipv4(KAFKA_PORT).await?;
        let bootstrap_servers = format!("0.0.0.0:{}", kafka_port);

        // Create connector
        let connector = KafkaConnector::new(&bootstrap_servers);

        // Wait for Kafka to be ready
        for i in 0..30 {
            if <KafkaConnector as StreamConnector<String, KafkaConfig, KafkaMetadata, ConnectorError>>::health_check(&connector)
                .await
                .is_ok()
            {
                break;
            }
            if i == 29 {
                return Err("Kafka failed to become ready within 30 seconds".into());
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        let test_topic = format!("test-topic-{}", Uuid::new_v4());
        Ok(Self {
            connector,
            test_topic,
            bootstrap_servers,
            kafka_port,
            _container: kafka_container,
        })
    }

    async fn create_topic_with_partitions(&self, topic_name: &str, num_partitions: i32) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔧 Creating topic '{}' with {} partitions", topic_name, num_partitions);
        let mut admin_config = ClientConfig::new();
        admin_config
            .set("bootstrap.servers", format!("localhost:{}", self.kafka_port))
            .set("request.timeout.ms", "5000")
            .set("session.timeout.ms", "3000")
            .set_log_level(RDKafkaLogLevel::Debug);
        let admin_client: AdminClient<_> = admin_config.create()?;
        let new_topic = NewTopic::new(topic_name, num_partitions, TopicReplication::Fixed(1));
        let admin_options = AdminOptions::new()
            .operation_timeout(Some(rdkafka::util::Timeout::After(Duration::from_secs(10))));
        match admin_client.create_topics(&[new_topic], &admin_options).await {
            Ok(results) => {
                for result in results {
                    match result {
                        Ok(_) => println!("✅ Successfully created topic '{}'", topic_name),
                        Err((topic, e)) => {
                            if e == rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists {
                                println!("ℹ️ Topic '{}' already exists", topic);
                            } else {
                                println!("❌ Failed to create topic '{}': {:?}", topic, e);
                                return Err(format!("Failed to create topic: {:?}", e).into());
                            }
                        }
                    }
                }
                Ok(())
            }
            Err(e) => {
                println!("❌ Admin client error: {:?}", e);
                Err(format!("Admin client error: {:?}", e).into())
            }
        }
    }

    fn producer_config(&self) -> KafkaConfig {
        KafkaConfig {
            topic: self.test_topic.clone(),
            group_id: Some("producer-group".to_string()),
            partition: None,
            from_beginning: false,
            kafka_config: None,
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        }
    }

    fn consumer_config(&self, group_id: &str) -> KafkaConfig {
        KafkaConfig {
            topic: self.test_topic.clone(),
            group_id: Some(group_id.to_string()),
            partition: None,
            from_beginning: true,
            kafka_config: None,
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        }
    }
}

#[tokio::test]
#[serial]
async fn test_kafka_connector_with_testcontainers() {
    // Set up logging to see detailed messages
    env_logger::init();
    
    println!("🚀 Starting comprehensive Kafka connector test");

    // Create the test environment - this will fail the test if container setup fails
    let env = KafkaTestEnvironment::new().await.unwrap();

    // ===== PART 1: Basic Connectivity Test =====
    println!("\n📡 PART 1: Testing basic connectivity");
    let health = <KafkaConnector as StreamConnector<String, KafkaConfig, KafkaMetadata, ConnectorError>>::health_check(&env.connector).await;
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
    let metadata = env
        .connector
        .to_sink(Box::new(producer_stream), env.producer_config())
        .await
        .unwrap();

    // In stub implementation, we can only check basic metadata
    assert_eq!(metadata.topic, env.test_topic);
    println!("✅ Producer test passed (stub implementation)");

    // Give Kafka time to commit
    sleep(Duration::from_secs(2)).await;

    println!("Starting consumer test to verify messages");
    
    // Create a separate topic for this test to avoid interference
    let consumer_test_topic = format!("consumer-test-{}", Uuid::new_v4());
    
    // First, produce some test messages to the new topic
    let test_messages: Vec<String> = (0..5).map(|i| format!("Test message {}", i)).collect();
    let producer_stream = from_iter(test_messages.clone());
    
    let consumer_test_producer_config = KafkaConfig {
        topic: consumer_test_topic.clone(),
        group_id: Some("producer-group".to_string()),
        partition: None,
        from_beginning: false,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };
    
    env.connector
        .to_sink(Box::new(producer_stream), consumer_test_producer_config)
        .await
        .expect("Failed to produce test messages");

    // Give Kafka time to commit
    sleep(Duration::from_secs(2)).await;

    // Create consumer stream for the new topic
    let consumer_test_config = KafkaConfig {
        topic: consumer_test_topic,
        group_id: Some("test-group".to_string()),
        partition: None,
        from_beginning: true,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };
    
    let consumer_stream = env
        .connector
        .from_source(consumer_test_config)
        .await
        .expect("Failed to create consumer stream");

    // Test that the consumer stream can receive messages
    let received_messages: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream
            .take_rs2(test_messages.len())
            .collect_rs2(),
    )
    .await
    .expect("Timeout waiting for consumer messages");

    // Verify we received the expected number of messages
    assert_eq!(
        received_messages.len(),
        test_messages.len(),
        "Consumer should receive all test messages"
    );

    // Verify the content of the messages
    let mut received_sorted = received_messages.clone();
    received_sorted.sort();
    let mut expected_sorted = test_messages.clone();
    expected_sorted.sort();

    assert_eq!(
        received_sorted, expected_sorted,
        "Consumer should receive the correct test messages"
    );

    println!("✅ Consumer stream created successfully and received {} messages", received_messages.len());
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
    let metadata = env
        .connector
        .to_sink(Box::new(producer_stream), env.producer_config())
        .await
        .unwrap();

    let elapsed = start.elapsed();
    println!("Backpressure test completed in: {:?}", elapsed);
    println!("Messages sent: {}", metadata.messages_produced);

    assert_eq!(
        metadata.messages_produced, 500,
        "Should have produced 500 messages"
    );
    assert!(
        metadata.bytes_sent > 50000,
        "Should have sent at least 50KB of data"
    );
    println!("✅ Backpressure test passed");

    // ===== PART 4: Multiple Consumers Test =====
    println!("\n👥 PART 4: Testing multiple consumers");

    // Create a new topic for this test
    let multi_consumer_topic = format!("multi-consumer-{}", Uuid::new_v4());

    // Create a custom config for this test
    let multi_producer_config = KafkaConfig {
        topic: multi_consumer_topic.clone(),
        group_id: Some("producer-group".to_string()),
        partition: None,
        from_beginning: false,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };

    // Test data for multiple consumers
    let multi_test_data: Vec<String> = (0..20)
        .map(|i| format!("Multi-consumer message {}", i))
        .collect();

    // Produce messages
    println!("Producing messages for multiple consumers test");
    let producer_stream = from_iter(multi_test_data.clone());
    env.connector
        .to_sink(Box::new(producer_stream), multi_producer_config.clone())
        .await
        .unwrap();

    // Give Kafka time to commit and ensure topic is created
    sleep(Duration::from_secs(5)).await;
    println!("Topic created and messages produced. Creating consumers...");

    // Create two consumers in different consumer groups
    println!("Creating two consumers in different consumer groups");
    let consumer_config_1 = KafkaConfig {
        topic: multi_consumer_topic.clone(),
        group_id: Some("group-1".to_string()),
        partition: None,
        from_beginning: true,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };

    let consumer_config_2 = KafkaConfig {
        topic: multi_consumer_topic.clone(),
        group_id: Some("group-2".to_string()),
        partition: None,
        from_beginning: true,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };

    // Start both consumers
    let consumer_stream_1 = env
        .connector
        .from_source(consumer_config_1)
        .await
        .expect("Failed to create first consumer");

    let consumer_stream_2 = env
        .connector
        .from_source(consumer_config_2)
        .await
        .expect("Failed to create second consumer");

    // Collect messages from both consumers with a timeout
    println!("Starting to collect messages from consumer 1...");
    let received_1: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream_1
            .take_rs2(multi_test_data.len())
            .collect_rs2(),
    )
    .await
    .unwrap_or_default();
    println!("Consumer 1 collection completed, got {} messages", received_1.len());

    println!("Starting to collect messages from consumer 2...");
    let received_2: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream_2
            .take_rs2(multi_test_data.len())
            .collect_rs2(),
    )
    .await
    .unwrap_or_default();
    println!("Consumer 2 collection completed, got {} messages", received_2.len());

    println!("Consumer 1 received {} messages: {:?}", received_1.len(), received_1);
    println!("Consumer 2 received {} messages: {:?}", received_2.len(), received_2);

    // With different consumer groups, each consumer should receive all messages
    assert_eq!(
        received_1.len(),
        multi_test_data.len(),
        "Consumer 1 should receive all messages"
    );
    assert_eq!(
        received_2.len(),
        multi_test_data.len(),
        "Consumer 2 should receive all messages"
    );

    // Verify the content of the messages for both consumers
    let mut received_1_sorted = received_1.clone();
    received_1_sorted.sort();

    let mut received_2_sorted = received_2.clone();
    received_2_sorted.sort();

    let mut expected = multi_test_data.clone();
    expected.sort();

    assert_eq!(
        received_1_sorted, expected,
        "Consumer 1 should receive all expected messages"
    );
    assert_eq!(
        received_2_sorted, expected,
        "Consumer 2 should receive all expected messages"
    );
    println!("✅ Multiple consumers test passed");

    // ===== PART 5: Consumer Group Offset Test =====
    println!("\n📊 PART 5: Testing consumer group offset management");

    // Create a new topic for this test
    let offset_test_topic = format!("offset-test-{}", Uuid::new_v4());

    // Create configs for this test
    let offset_producer_config = KafkaConfig {
        topic: offset_test_topic.clone(),
        group_id: Some("producer-group".to_string()),
        partition: None,
        from_beginning: false,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };

    // Produce initial batch of messages
    let initial_messages: Vec<String> = (0..10).map(|i| format!("Initial message {}", i)).collect();

    println!("Producing initial batch of messages");
    let producer_stream = from_iter(initial_messages.clone());
    env.connector
        .to_sink(Box::new(producer_stream), offset_producer_config.clone())
        .await
        .unwrap();

    // Give Kafka time to commit
    sleep(Duration::from_secs(2)).await;

    // Create consumer with a specific group ID
    let offset_consumer_config = KafkaConfig {
        topic: offset_test_topic.clone(),
        group_id: Some("offset-test-group".to_string()),
        partition: None,
        from_beginning: true,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };

    // Consume the initial batch
    println!("Consuming initial batch of messages");
    let consumer_stream = env
        .connector
        .from_source(offset_consumer_config.clone())
        .await
        .expect("Failed to create consumer");

    let received_initial: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream.take_rs2(10).collect_rs2(),
    )
    .await
    .expect("Timed out waiting for messages");

    assert_eq!(
        received_initial.len(),
        10,
        "Should receive all 10 initial messages"
    );

    // Produce second batch of messages
    let second_batch: Vec<String> = (10..20)
        .map(|i| format!("Second batch message {}", i))
        .collect();

    println!("Producing second batch of messages");
    let producer_stream = from_iter(second_batch.clone());
    env.connector
        .to_sink(Box::new(producer_stream), offset_producer_config.clone())
        .await
        .unwrap();

    // Give Kafka time to commit
    sleep(Duration::from_secs(2)).await;

    // Create a new consumer with the same group ID
    // It should only receive the second batch since the group offset was committed
    println!("Creating new consumer with same group ID");
    let consumer_stream = env
        .connector
        .from_source(offset_consumer_config.clone())
        .await
        .expect("Failed to create consumer");

    let received_second: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        consumer_stream.take_rs2(10).collect_rs2(),
    )
    .await
    .expect("Timed out waiting for messages");

    println!(
        "Second consumer received {} messages",
        received_second.len()
    );
    assert_eq!(
        received_second.len(),
        10,
        "Should receive only the 10 new messages"
    );

    // Verify the messages are from the second batch
    for msg in &received_second {
        assert!(
            msg.contains("Second batch"),
            "Message should be from second batch: {}",
            msg
        );
    }

    println!("✅ Consumer group offset test passed");

    // ===== PART 7: Order Guarantee Test =====
    println!("\n📋 PART 7: Testing order guarantees within partitions");
    let order_topic = format!("order-test-{}", Uuid::new_v4());
    let ordered_msgs: Vec<String> = (0..10).map(|i| format!("ORDER-{}", i)).collect();

    println!("🔧 Creating topic: {}", order_topic);
    
    // Create topic with 2 partitions for order testing
    env.create_topic_with_partitions(&order_topic, 2).await
        .expect("Failed to create topic for order testing");
    
    println!("📤 Producing {} ordered messages: {:?}", ordered_msgs.len(), ordered_msgs);

    let order_producer_stream = from_iter(ordered_msgs.clone());
    let order_producer_config = KafkaConfig {
        topic: order_topic.clone(),
        group_id: Some("producer-group".to_string()),
        partition: Some(0),
        from_beginning: false,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };
    env.connector.to_sink(Box::new(order_producer_stream), order_producer_config).await.unwrap();
    sleep(Duration::from_secs(2)).await;
    let order_consumer_config = KafkaConfig {
        topic: order_topic,
        group_id: Some("order-group".to_string()),
        partition: Some(0),
        from_beginning: true,
        kafka_config: None,
        enable_auto_commit: true,
        auto_commit_interval_ms: Some(5000),
        session_timeout_ms: Some(30000),
        message_timeout_ms: Some(30000),
    };
    let order_consumer_stream = env.connector.from_source(order_consumer_config).await.unwrap();
    let received_ordered: Vec<String> = tokio::time::timeout(
        Duration::from_secs(10),
        order_consumer_stream.take_rs2(ordered_msgs.len()).collect_rs2(),
    ).await.expect("Timeout waiting for ordered messages");
    assert_eq!(received_ordered, ordered_msgs, "Order guarantee failed: received {:?}, expected {:?}", received_ordered, ordered_msgs);
    println!("✅ Order guarantee test passed: {:?}", received_ordered);

    println!("\n🎉 ALL TESTS PASSED: RS2 Kafka connector is working correctly!");
}
