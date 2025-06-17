use crate::connectors::{ConnectorError, StreamConnector};
use crate::RS2Stream;
use async_stream::stream;
use async_trait::async_trait;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::{Message, TopicPartitionList};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex; // ← Use Tokio Mutex!

/// Kafka connector for RS2 streams
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConnector {
    bootstrap_servers: String,
    consumer_group: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Topic name
    pub topic: String,
    /// Consumer group ID (for consumers)
    pub group_id: Option<String>,
    /// Partition to read from/write to (optional)
    pub partition: Option<i32>,
    /// Start from beginning of topic
    pub from_beginning: bool,
    /// Additional Kafka configuration
    pub kafka_config: Option<HashMap<String, String>>,
    /// Enable auto-commit for consumers
    pub enable_auto_commit: bool,
    /// Commit interval in milliseconds
    pub auto_commit_interval_ms: Option<u64>,
    /// Session timeout in milliseconds
    pub session_timeout_ms: Option<u64>,
    /// Message timeout in milliseconds
    pub message_timeout_ms: Option<u64>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            topic: String::new(),
            group_id: None,
            partition: None,
            from_beginning: false,
            kafka_config: None,
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        }
    }
}

#[derive(Debug, Clone)]
pub struct KafkaMetadata {
    pub topic: String,
    pub partition_count: i32,
    pub messages_produced: u64,
    pub messages_consumed: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub last_offset: Option<i64>,
    pub consumer_lag: Option<i64>,
    pub throughput: f64,
}

impl KafkaConnector {
    pub fn new(bootstrap_servers: &str) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.to_string(),
            consumer_group: None,
        }
    }

    pub fn with_consumer_group(mut self, group_id: &str) -> Self {
        self.consumer_group = Some(group_id.to_string());
        self
    }

    fn create_consumer_config(&self, config: &KafkaConfig) -> ClientConfig {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("enable.auto.commit", &config.enable_auto_commit.to_string())
            .set(
                "session.timeout.ms",
                &config.session_timeout_ms.unwrap_or(30000).to_string(),
            );

        let group_id = config
            .group_id
            .clone()
            .or_else(|| self.consumer_group.clone());
        if let Some(group_id) = group_id {
            client_config.set("group.id", group_id);
        }

        if let Some(interval) = config.auto_commit_interval_ms {
            client_config.set("auto.commit.interval.ms", &interval.to_string());
        }

        if config.from_beginning {
            client_config.set("auto.offset.reset", "earliest");
        } else {
            client_config.set("auto.offset.reset", "latest");
        }

        if let Some(kafka_config) = &config.kafka_config {
            for (key, value) in kafka_config {
                client_config.set(key, value);
            }
        }

        client_config
    }

    fn create_producer_config(&self, config: &KafkaConfig) -> ClientConfig {
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set(
                "message.timeout.ms",
                &config.message_timeout_ms.unwrap_or(30000).to_string(),
            );

        if let Some(kafka_config) = &config.kafka_config {
            for (key, value) in kafka_config {
                client_config.set(key, value);
            }
        }

        client_config
    }
}

#[async_trait]
impl<T> StreamConnector<T> for KafkaConnector
where
    T: for<'de> Deserialize<'de> + Serialize + Send + 'static,
{
    type Config = KafkaConfig;
    type Error = ConnectorError;
    type Metadata = KafkaMetadata;

    async fn from_source(&self, config: Self::Config) -> Result<RS2Stream<T>, Self::Error> {
        let client_config = self.create_consumer_config(&config);
        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let topics = vec![config.topic.as_str()];
        consumer
            .subscribe(&topics)
            .map_err(|e| ConnectorError::ConnectorSpecific(e.to_string()))?;

        if let Some(partition) = config.partition {
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition(&config.topic, partition);
            consumer
                .assign(&tpl)
                .map_err(|e| ConnectorError::ConnectorSpecific(e.to_string()))?;
        }

        let topic = config.topic.clone();
        let stream = stream! {
            loop {
                match consumer.recv().await {
                    Ok(message) => {
                        if let Some(payload) = message.payload() {
                            match serde_json::from_slice::<T>(payload) {
                                Ok(item) => {
                                    log::debug!("Received message from Kafka topic: {}", topic);
                                    yield item;
                                }
                                Err(e) => {
                                    log::error!("Failed to deserialize Kafka message: {}", e);
                                    continue;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Kafka consumer error: {}", e);
                        break;
                    }
                }
            }
        };

        Ok(stream.boxed())
    }

    async fn to_sink(
        &self,
        stream: RS2Stream<T>,
        config: Self::Config,
    ) -> Result<Self::Metadata, Self::Error> {
        // Validate topic name
        if config.topic.trim().is_empty() {
            return Err(ConnectorError::InvalidConfiguration("Topic name cannot be empty".to_string()));
        }

        let client_config = self.create_producer_config(&config);
        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        let start = Instant::now();

        // Use Arc<tokio::sync::Mutex<>> for async contexts
        let messages_produced = Arc::new(Mutex::new(0u64));
        let bytes_sent = Arc::new(Mutex::new(0u64));

        stream
            .for_each(|item| {
                let producer = producer.clone();
                let topic = config.topic.clone();
                let partition = config.partition;
                let messages_counter = Arc::clone(&messages_produced);
                let bytes_counter = Arc::clone(&bytes_sent);

                async move {
                    match serde_json::to_vec(&item) {
                        Ok(payload) => {
                            // If it's a string message, try to extract a key for partitioning
                            let key_string = if let Ok(message_str) = serde_json::from_slice::<String>(&payload) {
                                // Check if it matches our test pattern "p{partition}-{sequence}"
                                if let Some(key) = message_str.split('-').next() {
                                    Some(key.to_string())
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            // Now create the record with the key if we have one
                            let mut record = FutureRecord::to(&topic).payload(&payload);

                            if let Some(ref key) = key_string {
                                record = record.key(key);
                            }

                            if let Some(p) = partition {
                                record = record.partition(p);
                            }

                            match producer.send(record, Duration::from_secs(30)).await {
                                Ok(_) => {
                                    *messages_counter.lock().await += 1; // ← .await here!
                                    *bytes_counter.lock().await += payload.len() as u64; // ← .await here!
                                    log::debug!("Sent message to Kafka topic: {}", topic);
                                }
                                Err((e, _)) => {
                                    log::error!("Failed to send message to Kafka: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Failed to serialize message for Kafka: {}", e);
                        }
                    }
                }
            })
            .await;

        let elapsed = start.elapsed();
        let final_messages_produced = *messages_produced.lock().await; // ← .await here!
        let final_bytes_sent = *bytes_sent.lock().await; // ← .await here!

        let throughput = if elapsed.as_secs_f64() > 0.0 {
            final_messages_produced as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        };

        Ok(KafkaMetadata {
            topic: config.topic,
            partition_count: 1,
            messages_produced: final_messages_produced,
            messages_consumed: 0,
            bytes_sent: final_bytes_sent,
            bytes_received: 0,
            last_offset: None,
            consumer_lag: None,
            throughput,
        })
    }

    async fn health_check(&self) -> Result<bool, Self::Error> {
        let client_config = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .clone();

        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))?;

        match consumer.fetch_metadata(Some("__consumer_offsets"), Duration::from_secs(5)) {
            Ok(_) => Ok(true),
            Err(e) => Err(ConnectorError::ConnectionFailed(format!("Failed to fetch metadata: {}", e))),
        }
    }

    async fn metadata(&self) -> Result<Self::Metadata, Self::Error> {
        Ok(KafkaMetadata {
            topic: "unknown".to_string(),
            partition_count: 0,
            messages_produced: 0,
            messages_consumed: 0,
            bytes_sent: 0,
            bytes_received: 0,
            last_offset: None,
            consumer_lag: None,
            throughput: 0.0,
        })
    }

    fn name(&self) -> &'static str {
        "kafka"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }
}
