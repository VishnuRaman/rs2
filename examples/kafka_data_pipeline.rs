//! # Kafka Data Streaming Pipeline Example (with Pipeline Builder)
//!
//! This example demonstrates a **production-style, multi-branch streaming pipeline** using Kafka and the `rs2` library in Rust, now using the ergonomic `Pipeline` builder API.
//!
//! ## What This Pipeline Does
//!
//! - **Auto-generates random user activity events** for demo/testing (no external producer required).
//! - **Reads** user activity events from a Kafka topic (`user-activity`).
//! - **Parses and validates** each event.
//! - **Branches** the pipeline to three sinks:
//!   1. **Analytics**: Performs time-windowed aggregation and writes results to `activity-analytics`.
//!   2. **Alerts**: Detects suspicious/invalid activity and writes alerts to `activity-alerts`.
//!   3. **Validated**: Forwards all validated activities to `validated-activity`.
//! - **Handles errors and retries** for Kafka writes, and collects per-sink metrics.
//! - **Demonstrates** modular, real-world streaming pipeline patterns: branching, windowing, metrics, and error handling, all with the new Pipeline builder.
//!
//! ## Requirements to Run
//!
//! - **Rust** (with async/await, tokio, and dependencies in `Cargo.toml`)
//! - **Kafka** broker running and accessible (default: `localhost:9092`)
//! - The following Kafka topics must exist:
//!   - `user-activity`
//!   - `validated-activity`
//!   - `activity-analytics`
//!   - `activity-alerts`
//! - (Optional) Populate `user-activity` with sample events to see the pipeline in action.
//!
//! ### Quick Kafka Setup with Docker Compose
//!
//! You can quickly spin up Kafka and Zookeeper using Docker Compose:
//!
//! ```yaml
//! version: '2'
//! services:
//!   zookeeper:
//!     image: wurstmeister/zookeeper:3.4.6
//!     ports:
//!       - "2181:2181"
//!   kafka:
//!     image: wurstmeister/kafka:2.12-2.2.1
//!     ports:
//!       - "9092:9092"
//!     environment:
//!       KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
//!       KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
//!     depends_on:
//!       - zookeeper
//! ```
//!
//! After starting Kafka, create the required topics (using `kafka-topics.sh` or a UI tool).
//!
//! ## Running the Example
//!
//! 1. Ensure Kafka is running and topics are created.
//! 2. Build and run this example:
//!    ```sh
//!    cargo run --example kafka_data_pipeline
//!    ```
//! 3. Observe analytics, alerts, and validated activities being processed and written to their respective topics.
//!
//! ---
//! This example is intended as a real-world, extensible template for streaming pipelines using `rs2` and the new Pipeline builder.

use rs2::connectors::{KafkaConnector, StreamConnector};
use rs2::connectors::kafka_connector::KafkaConfig;
use rs2::rs2::*;
use rs2::stream_configuration::{BufferConfig, GrowthStrategy};
use rs2::error::{RetryPolicy, StreamError, StreamResult};
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use rand::{thread_rng, Rng};
use chrono::{DateTime, Utc};
use async_stream::stream;
use rdkafka::consumer::Consumer;
use rdkafka::producer::Producer;
use rs2::pipeline::builder::{Pipeline, PipelineNode};
use tokio::sync::broadcast;
use futures_util::stream;
use std::sync::atomic::{AtomicUsize, Ordering};
use futures_util::FutureExt;

// ================================
// Data Models
// ================================

/// Represents a user activity event
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserActivity {
    /// Unique identifier for the activity
    id: String,
    /// User ID who performed the activity
    user_id: u64,
    /// Type of activity (e.g., "login", "purchase", "view")
    activity_type: String,
    /// Timestamp when the activity occurred
    timestamp: DateTime<Utc>,
    /// Additional metadata about the activity
    metadata: HashMap<String, String>,
}

/// Represents a validated user activity
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidatedActivity {
    /// The original activity
    activity: UserActivity,
    /// Whether the activity is valid
    is_valid: bool,
    /// Reason for validation failure, if any
    validation_message: Option<String>,
    /// Processing timestamp
    processed_at: DateTime<Utc>,
}

/// Represents analytics derived from user activities
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityAnalytics {
    /// Time window for the analytics
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    /// User ID
    user_id: u64,
    /// Count of activities by type
    activity_counts: HashMap<String, u64>,
    /// Total activities in the window
    total_activities: u64,
}

/// Represents an alert triggered by suspicious activity
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActivityAlert {
    /// User ID
    user_id: u64,
    /// Alert type
    alert_type: String,
    /// Alert message
    message: String,
    /// Timestamp when the alert was generated
    timestamp: DateTime<Utc>,
    /// Severity level (1-5)
    severity: u8,
}

// ================================
// Helper Functions
// ================================

/// Generate a random user activity
fn generate_random_activity() -> UserActivity {
    let mut rng = thread_rng();

    // Generate random activity type
    let activity_types = ["login", "logout", "purchase", "view", "search", "share"];
    let activity_type = activity_types[rng.gen_range(0..activity_types.len())].to_string();

    // Generate random metadata
    let mut metadata = HashMap::new();
    if activity_type == "purchase" {
        metadata.insert("amount".to_string(), format!("{:.2}", rng.gen_range(1.0..1000.0)));
        metadata.insert("product_id".to_string(), format!("PROD-{}", rng.gen_range(1000..9999)));
    } else if activity_type == "view" {
        metadata.insert("page".to_string(), format!("/product/{}", rng.gen_range(1000..9999)));
        metadata.insert("duration".to_string(), format!("{}", rng.gen_range(5..300)));
    } else if activity_type == "search" {
        metadata.insert("query".to_string(), "example search query".to_string());
        metadata.insert("results".to_string(), format!("{}", rng.gen_range(0..100)));
    }

    UserActivity {
        id: format!("ACT-{}", rng.gen_range(10000..99999)),
        user_id: rng.gen_range(1..1000),
        activity_type,
        timestamp: Utc::now(),
        metadata,
    }
}

/// Validate a user activity
fn validate_activity(activity: UserActivity) -> ValidatedActivity {
    let mut is_valid = true;
    let mut validation_message = None;

    // Validate based on activity type
    match activity.activity_type.as_str() {
        "purchase" => {
            // Check if amount is present for purchases
            if !activity.metadata.contains_key("amount") {
                is_valid = false;
                validation_message = Some("Purchase activity missing amount".to_string());
            } else {
                // Parse amount and validate
                let amount = activity.metadata.get("amount")
                    .and_then(|a| a.parse::<f64>().ok())
                    .unwrap_or(0.0);

                if amount <= 0.0 {
                    is_valid = false;
                    validation_message = Some("Invalid purchase amount".to_string());
                }
            }
        },
        "view" => {
            // Check if page is present for views
            if !activity.metadata.contains_key("page") {
                is_valid = false;
                validation_message = Some("View activity missing page".to_string());
            }
        },
        _ => {
            // Other activity types are always valid
        }
    }

    ValidatedActivity {
        activity,
        is_valid,
        validation_message,
        processed_at: Utc::now(),
    }
}

/// Check if an activity is suspicious and should trigger an alert
fn check_for_alerts(activity: &ValidatedActivity) -> Option<ActivityAlert> {
    if !activity.is_valid {
        // Invalid activities might be suspicious
        return Some(ActivityAlert {
            user_id: activity.activity.user_id,
            alert_type: "INVALID_ACTIVITY".to_string(),
            message: format!("Invalid activity detected: {}", 
                            activity.validation_message.clone().unwrap_or_default()),
            timestamp: Utc::now(),
            severity: 2,
        });
    }

    // Check for suspicious purchase amounts
    if activity.activity.activity_type == "purchase" {
        if let Some(amount_str) = activity.activity.metadata.get("amount") {
            if let Ok(amount) = amount_str.parse::<f64>() {
                if amount > 500.0 {
                    return Some(ActivityAlert {
                        user_id: activity.activity.user_id,
                        alert_type: "LARGE_PURCHASE".to_string(),
                        message: format!("Large purchase of ${:.2} detected", amount),
                        timestamp: Utc::now(),
                        severity: 3,
                    });
                }
            }
        }
    }

    // Check for rapid succession of activities (would require state in a real implementation)
    // This is just a placeholder for demonstration purposes
    if thread_rng().gen_ratio(1, 20) {  // 5% chance of triggering this alert
        return Some(ActivityAlert {
            user_id: activity.activity.user_id,
            alert_type: "RAPID_ACTIVITY".to_string(),
            message: "Multiple activities in rapid succession".to_string(),
            timestamp: Utc::now(),
            severity: 1,
        });
    }

    None
}

// --- Analytics Transform: Time-based Windowed Aggregation ---
fn analytics_transform<S>(mut stream: S) -> RS2Stream<ActivityAnalytics>
where
    S: futures_util::Stream<Item = ValidatedActivity> + Unpin + Send + 'static,
{
    use tokio::time::interval;
    use std::collections::HashMap;
    use chrono::Utc;
    stream! {
        let mut buffer: Vec<ValidatedActivity> = Vec::new();
        let mut window_start = Utc::now();
        let mut ticker = interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if !buffer.is_empty() {
                        let user_id = buffer[0].activity.user_id;
                        let mut activity_counts = HashMap::new();
                        for va in &buffer {
                            *activity_counts.entry(va.activity.activity_type.clone()).or_insert(0) += 1;
                        }
                        let analytics = ActivityAnalytics {
                            window_start,
                            window_end: Utc::now(),
                            user_id,
                            activity_counts,
                            total_activities: buffer.len() as u64,
                        };
                        yield analytics;
                        buffer.clear();
                        window_start = Utc::now();
                    }
                }
                item = stream.next() => {
                    if let Some(va) = item {
                        buffer.push(va);
                    } else {
                        break;
                    }
                }
            }
        }
        // Emit any remaining
        if !buffer.is_empty() {
            let user_id = buffer[0].activity.user_id;
            let mut activity_counts = HashMap::new();
            for va in &buffer {
                *activity_counts.entry(va.activity.activity_type.clone()).or_insert(0) += 1;
            }
            let analytics = ActivityAnalytics {
                window_start,
                window_end: Utc::now(),
                user_id,
                activity_counts,
                total_activities: buffer.len() as u64,
            };
            yield analytics;
        }
    }.boxed()
}

// --- Metrics Struct ---
struct SinkMetrics {
    processed: AtomicUsize,
    errors: AtomicUsize,
}

// --- Real Kafka Sink with Metrics and Retries ---
async fn kafka_sink<T: Serialize + std::fmt::Debug>(
    connector: &KafkaConnector,
    config: &KafkaConfig,
    mut stream: RS2Stream<T>,
    metrics: Arc<SinkMetrics>,
) {
    use rs2::connectors::StreamConnector;
    use futures_util::stream;
    use tokio::time::sleep;
    while let Some(item) = stream.next().await {
        let msg = serde_json::to_string(&item).unwrap();
        let mut attempts = 0;
        let max_retries = 5;
        loop {
            let msg_clone = msg.clone();
            let single_item_stream = stream::once(async move { msg_clone }).boxed();
            let result = <KafkaConnector as StreamConnector<String>>::to_sink(
                connector, single_item_stream, config.clone()
            ).await;
            if result.is_ok() {
                metrics.processed.fetch_add(1, Ordering::Relaxed);
                println!("[Kafka] Topic: {} | {} | Processed: {}", config.topic, msg, metrics.processed.load(Ordering::Relaxed));
                break;
            } else {
                metrics.errors.fetch_add(1, Ordering::Relaxed);
                attempts += 1;
                if attempts > max_retries {
                    eprintln!("Failed to send to Kafka after {} attempts: {}", attempts, msg);
                    break;
                }
                let backoff = Duration::from_millis(100 * 2u64.pow(attempts as u32));
                sleep(backoff).await;
            }
        }
    }
}

// ================================
// Main Function
// ================================

#[tokio::main]
async fn main() {
    // 1. Setup connectors and configs
    let kafka_brokers = "localhost:9092";
    let connector = KafkaConnector::new(kafka_brokers).with_consumer_group("prod-group");
    let producer_config = KafkaConfig { topic: "validated-activity".to_string(), ..Default::default() };
    let alert_config = KafkaConfig { topic: "activity-alerts".to_string(), ..Default::default() };
    let analytics_config = KafkaConfig { topic: "activity-analytics".to_string(), ..Default::default() };
    let source_config = KafkaConfig { topic: "user-activity".to_string(), group_id: Some("prod-group".to_string()), ..Default::default() };

    // --- Inject random activities into Kafka for demo/testing ---
    let connector_clone = connector.clone();
    tokio::spawn(async move {
        let producer_config = KafkaConfig { topic: "user-activity".to_string(), ..Default::default() };
        loop {
            let activity = generate_random_activity();
            let msg = serde_json::to_string(&activity).unwrap();
            let single_item_stream = futures_util::stream::once(async move { msg }).boxed();
            let _ = <KafkaConnector as StreamConnector<String>>::to_sink(
                &connector_clone, single_item_stream, producer_config.clone()
            ).await;
            tokio::time::sleep(Duration::from_millis(500)).await; // Adjust rate as needed
        }
    });

    // --- Setup connectors/configs/metrics as Arc for easy sharing ---
    let connector = Arc::new(connector);
    let producer_config = Arc::new(producer_config);
    let alert_config = Arc::new(alert_config);
    let analytics_config = Arc::new(analytics_config);
    let validated_metrics = Arc::new(SinkMetrics { processed: AtomicUsize::new(0), errors: AtomicUsize::new(0) });
    let analytics_metrics = Arc::new(SinkMetrics { processed: AtomicUsize::new(0), errors: AtomicUsize::new(0) });
    let alert_metrics = Arc::new(SinkMetrics { processed: AtomicUsize::new(0), errors: AtomicUsize::new(0) });

    // --- Kafka source and validation ---
    let raw_activity_stream = <KafkaConnector as StreamConnector<String>>::from_source(
        &*connector,
        source_config.clone(),
    ).await.expect("Failed to connect to Kafka source");
    let validated_stream = raw_activity_stream
        .filter_map(|json| async move { serde_json::from_str::<UserActivity>(&json).ok() })
        .boxed()
        .map_rs2(validate_activity)
        .boxed();

    // --- Broadcast validated stream ---
    let (tx, _) = broadcast::channel(100);
    let mut validated_stream_for_broadcast = validated_stream;
    let tx_clone = tx.clone();
    tokio::spawn(async move {
        while let Some(item) = validated_stream_for_broadcast.next().await {
            let _ = tx_clone.send(item);
        }
    });

    // --- Main pipeline ---
    let tx_main = tx.clone();
    let connector_main = connector.clone();
    let producer_config_main = producer_config.clone();
    let validated_metrics_main = validated_metrics.clone();
    let analytics_metrics_main = analytics_metrics.clone();
    let alert_metrics_main = alert_metrics.clone();
    let analytics_config_main = analytics_config.clone();
    let alert_config_main = alert_config.clone();

    Pipeline::<ValidatedActivity>::new()
        .source(move || {
            let mut rx = tx_main.clone().subscribe();
            async_stream::stream! {
                while let Ok(item) = rx.recv().await {
                    yield item;
                }
            }.boxed()
        })
        .branch(
            "main-branch",
            move |stream| {
                let connector = connector_main.clone();
                let producer_config = producer_config_main.clone();
                let metrics = validated_metrics_main.clone();
                Box::pin(async move {
                    let connector = connector.clone();
                    let producer_config = producer_config.clone();
                    let metrics = metrics.clone();
                    kafka_sink(&*connector, &*producer_config, stream, metrics).await;
                })
            },
            move |_| {
                let tx_analytics = tx.clone();
                let tx_alerts = tx.clone();
                let connector_analytics = connector.clone();
                let analytics_config = analytics_config_main.clone();
                let analytics_metrics = analytics_metrics_main.clone();
                let connector_alerts = connector.clone();
                let alert_config = alert_config_main.clone();
                let alert_metrics = alert_metrics_main.clone();
                let analytics_pipeline = Pipeline::<ActivityAnalytics>::new()
                    .source(move || {
                        let mut rx = tx_analytics.clone().subscribe();
                        async_stream::stream! {
                            while let Ok(item) = rx.recv().await {
                                for agg in analytics_transform(futures_util::stream::once(async move { item }).boxed()).collect::<Vec<_>>().await {
                                    yield agg;
                                }
                            }
                        }.boxed()
                    })
                    .sink(move |stream| {
                        let connector = connector_analytics.clone();
                        let analytics_config = analytics_config.clone();
                        let metrics = analytics_metrics.clone();
                        Box::pin(async move {
                            let connector = connector.clone();
                            let analytics_config = analytics_config.clone();
                            let metrics = metrics.clone();
                            kafka_sink(&*connector, &*analytics_config, stream, metrics).await;
                        })
                    });
                let alerts_pipeline = Pipeline::<ActivityAlert>::new()
                    .source(move || {
                        let mut rx = tx_alerts.clone().subscribe();
                        async_stream::stream! {
                            while let Ok(item) = rx.recv().await {
                                if let Some(alert) = check_for_alerts(&item) {
                                    yield alert;
                                }
                            }
                        }.boxed()
                    })
                    .sink(move |stream| {
                        let connector = connector_alerts.clone();
                        let alert_config = alert_config.clone();
                        let metrics = alert_metrics.clone();
                        Box::pin(async move {
                            let connector = connector.clone();
                            let alert_config = alert_config.clone();
                            let metrics = metrics.clone();
                            kafka_sink(&*connector, &*alert_config, stream, metrics).await;
                        })
                    });
                Box::pin(async move {
                    let _ = tokio::join!(analytics_pipeline.run(), alerts_pipeline.run());
                })
            }
        )
        .sink(|_| Box::pin(async {}))
        .run()
        .await
        .expect("Pipeline run failed");

    // --- Print final metrics ---
    println!("Validated Sink: processed={}, errors={}", validated_metrics.processed.load(Ordering::Relaxed), validated_metrics.errors.load(Ordering::Relaxed));
    println!("Analytics Sink: processed={}, errors={}", analytics_metrics.processed.load(Ordering::Relaxed), analytics_metrics.errors.load(Ordering::Relaxed));
    println!("Alert Sink: processed={}, errors={}", alert_metrics.processed.load(Ordering::Relaxed), alert_metrics.errors.load(Ordering::Relaxed));
}
