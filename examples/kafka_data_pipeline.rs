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

use chrono::{DateTime, Utc};
use rand::{thread_rng, Rng};
use rs2_stream::connectors::kafka_connector::{KafkaConfig, KafkaMetadata};
use rs2_stream::connectors::{KafkaConnector, StreamConnector, ConnectorError};
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::schema_validation::JsonSchemaValidator;
use rs2_stream::stream::constructors::{from_iter, unfold};
use rs2_stream::stream::{Stream, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

// Create a custom stream wrapper that implements the necessary traits
struct StreamWrapper<T> {
    inner: Box<dyn Stream<Item = T> + Send + Unpin>,
}

impl<T> StreamWrapper<T> {
    fn new(stream: impl Stream<Item = T> + Send + Unpin + 'static) -> Self {
        Self {
            inner: Box::new(stream),
        }
    }
}

impl<T> Stream for StreamWrapper<T> {
    type Item = T;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use rs2_stream::stream::Stream as StreamTrait;
        // For Box<dyn Stream>, we need to pin it and call poll_next
        let pinned = std::pin::Pin::new(self.inner.as_mut());
        pinned.poll_next(cx)
    }
}

impl<T> Unpin for StreamWrapper<T> {}

// RS2StreamExt is automatically implemented for all Stream types

// Create type aliases for the complex function types
type StreamProcessor<T> = Box<dyn Fn(StreamWrapper<T>) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send>;

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
        metadata.insert(
            "amount".to_string(),
            format!("{:.2}", rng.gen_range(1.0..1000.0)),
        );
        metadata.insert(
            "product_id".to_string(),
            format!("PROD-{}", rng.gen_range(1000..9999)),
        );
    } else if activity_type == "view" {
        metadata.insert(
            "page".to_string(),
            format!("/product/{}", rng.gen_range(1000..9999)),
        );
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
                let amount = activity
                    .metadata
                    .get("amount")
                    .and_then(|a| a.parse::<f64>().ok())
                    .unwrap_or(0.0);

                if amount <= 0.0 {
                    is_valid = false;
                    validation_message = Some("Invalid purchase amount".to_string());
                }
            }
        }
        "view" => {
            // Check if page is present for views
            if !activity.metadata.contains_key("page") {
                is_valid = false;
                validation_message = Some("View activity missing page".to_string());
            }
        }
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
            message: format!(
                "Invalid activity detected: {}",
                activity.validation_message.clone().unwrap_or_default()
            ),
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
    if thread_rng().gen_ratio(1, 20) {
        // 5% chance of triggering this alert
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
fn analytics_transform(
    stream: StreamWrapper<ValidatedActivity>
) -> impl Stream<Item = ActivityAnalytics> + Send + 'static {
    unfold(
        (stream, Vec::<ValidatedActivity>::new(), Utc::now()),
        |(mut stream, mut buffer, mut window_start)| async move {
            // Simple windowing: collect items for 1 second, then emit analytics
            let window_duration = Duration::from_secs(1);
            let start_time = std::time::Instant::now();
            
            // Collect items until window expires or we get enough items
            while start_time.elapsed() < window_duration && buffer.len() < 10 {
                tokio::select! {
                    item = stream.next() => {
                        if let Some(va) = item {
                            buffer.push(va);
                        } else {
                            // Stream ended
                            break;
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // Check timeout periodically
                        break;
                    }
                }
            }
            
            if !buffer.is_empty() {
                // Create analytics from buffer
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
                buffer.clear();
                window_start = Utc::now();
                Some((analytics, (stream, buffer, window_start)))
            } else {
                // No more items and buffer is empty
                None
            }
        }
    )
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
    mut stream: StreamWrapper<T>,
    metrics: Arc<SinkMetrics>,
) {
    while let Some(item) = stream.next().await {
        let msg = serde_json::to_string(&item).unwrap();
        let mut attempts = 0;
        let max_retries = 5;
        loop {
            // Create a single item stream for the connector
            let single_item_stream = from_iter(vec![msg.clone()]);
            let result = <KafkaConnector as StreamConnector<String, KafkaConfig, KafkaMetadata, ConnectorError>>::to_sink(
                connector,
                Box::new(single_item_stream), // Box the stream
                config.clone(),
            )
            .await;
            if result.is_ok() {
                metrics.processed.fetch_add(1, Ordering::Relaxed);
                println!(
                    "[Kafka] Topic: {} | {} | Processed: {}",
                    config.topic,
                    msg,
                    metrics.processed.load(Ordering::Relaxed)
                );
                break;
            } else {
                metrics.errors.fetch_add(1, Ordering::Relaxed);
                attempts += 1;
                if attempts > max_retries {
                    eprintln!(
                        "Failed to send to Kafka after {} attempts: {}",
                        attempts, msg
                    );
                    break;
                }
                let backoff = Duration::from_millis(100 * 2u64.pow(attempts as u32));
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

// Create a simplified pipeline runner
async fn run_pipeline<T: Clone + Send + 'static>(
    source_stream: impl Stream<Item = T> + Send + 'static,
    sinks: Vec<StreamProcessor<T>>,
) {
    // Create a broadcast channel for distributing items to multiple sinks
    let (tx, _) = broadcast::channel::<T>(1000);
    
    // Spawn the source task
    let tx_clone = tx.clone();
    let source_task = tokio::spawn(async move {
        use rs2_stream::stream::StreamExt;
        let mut source_stream = source_stream;
        while let Some(item) = source_stream.next().await {
            if tx_clone.send(item).is_err() {
                break; // All receivers dropped
            }
        }
    });
    
    // Start sink tasks
    let mut sink_tasks = Vec::new();
    for sink_fn in sinks {
        let rx = tx.subscribe();
        let sink_stream = unfold(rx, |mut rx| async move {
            match rx.recv().await {
                Ok(item) => Some((item, rx)),
                Err(_) => None,
            }
        });
        let wrapped_stream = StreamWrapper::new(sink_stream);
        let task = tokio::spawn(sink_fn(wrapped_stream));
        sink_tasks.push(task);
    }
    
    // Wait for source to complete
    let _ = source_task.await;
    
    // Wait for all sinks to complete (with timeout)
    let timeout_duration = Duration::from_secs(5);
    for task in sink_tasks {
        let _ = tokio::time::timeout(timeout_duration, task).await;
    }
}

// ================================
// Main Function
// ================================

#[tokio::main]
async fn main() {
    println!("🚀 Starting Kafka Data Pipeline Example");
    
    // 1. Setup connectors and configs
    let kafka_brokers = "localhost:9092";
    let connector = KafkaConnector::new(kafka_brokers).with_consumer_group("prod-group");
    let producer_config = KafkaConfig {
        topic: "validated-activity".to_string(),
        ..Default::default()
    };
    let alert_config = KafkaConfig {
        topic: "activity-alerts".to_string(),
        ..Default::default()
    };
    let analytics_config = KafkaConfig {
        topic: "activity-analytics".to_string(),
        ..Default::default()
    };
    let source_config = KafkaConfig {
        topic: "user-activity".to_string(),
        group_id: Some("prod-group".to_string()),
        ..Default::default()
    };

    // --- Inject random activities into Kafka for demo/testing ---
    let connector_clone = connector.clone();
    tokio::spawn(async move {
        let producer_config = KafkaConfig {
            topic: "user-activity".to_string(),
            ..Default::default()
        };
        loop {
            let activity = generate_random_activity();
            let msg = serde_json::to_string(&activity).unwrap();
            let single_item_stream = from_iter(vec![msg]);
            let _ = <KafkaConnector as StreamConnector<String, KafkaConfig, KafkaMetadata, ConnectorError>>::to_sink(
                &connector_clone,
                Box::new(single_item_stream), // Box the stream
                producer_config.clone(),
            )
            .await;
            tokio::time::sleep(Duration::from_millis(500)).await; // Adjust rate as needed
        }
    });

    // --- Setup connectors/configs/metrics as Arc for easy sharing ---
    let connector = Arc::new(connector);
    let producer_config = Arc::new(producer_config);
    let alert_config = Arc::new(alert_config);
    let analytics_config = Arc::new(analytics_config);
    let validated_metrics = Arc::new(SinkMetrics {
        processed: AtomicUsize::new(0),
        errors: AtomicUsize::new(0),
    });
    let analytics_metrics = Arc::new(SinkMetrics {
        processed: AtomicUsize::new(0),
        errors: AtomicUsize::new(0),
    });
    let alert_metrics = Arc::new(SinkMetrics {
        processed: AtomicUsize::new(0),
        errors: AtomicUsize::new(0),
    });

    // --- Example JSON schema for UserActivity ---
    let user_activity_schema = serde_json::json!({
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "user_id": {"type": "integer"},
            "activity_type": {"type": "string"},
            "timestamp": {"type": "string"},
            "metadata": {"type": "object"}
        },
        "required": ["id", "user_id", "activity_type", "timestamp", "metadata"]
    });
    let schema_validator = JsonSchemaValidator::new("user-activity-v1", user_activity_schema);

    println!("📡 Connecting to Kafka source...");

    // --- Kafka source and schema validation ---
    let raw_activity_stream = <KafkaConnector as StreamConnector<String, KafkaConfig, KafkaMetadata, ConnectorError>>::from_source(
        &*connector,
        source_config.clone(),
    )
    .await
    .expect("Failed to connect to Kafka source");
    
    let schema_validated_stream = raw_activity_stream
        .with_schema_validation_rs2(schema_validator)
        .filter_map_rs2(|json: String| { 
            serde_json::from_str::<UserActivity>(&json).ok() 
        });
    
    let validated_stream = schema_validated_stream.map_rs2(validate_activity);

    println!("🔄 Setting up processing pipeline...");

    // --- Create sink functions ---
    let validated_sink: StreamProcessor<ValidatedActivity> = {
        let connector = Arc::clone(&connector);
        let config = Arc::clone(&producer_config);
        let metrics = Arc::clone(&validated_metrics);
        Box::new(move |stream| {
            let connector = Arc::clone(&connector);
            let config = Arc::clone(&config);
            let metrics = Arc::clone(&metrics);
            Box::pin(async move {
                kafka_sink(&*connector, &*config, stream, metrics).await;
            })
        })
    };

    let analytics_sink: StreamProcessor<ValidatedActivity> = {
        let connector = Arc::clone(&connector);
        let config = Arc::clone(&analytics_config);
        let metrics = Arc::clone(&analytics_metrics);
        Box::new(move |stream| {
            let connector = Arc::clone(&connector);
            let config = Arc::clone(&config);
            let metrics = Arc::clone(&metrics);
            Box::pin(async move {
                let analytics_stream = analytics_transform(stream);
                let wrapped_analytics_stream = StreamWrapper::new(analytics_stream);
                kafka_sink(&*connector, &*config, wrapped_analytics_stream, metrics).await;
            })
        })
    };

    let alerts_sink: StreamProcessor<ValidatedActivity> = {
        let connector = Arc::clone(&connector);
        let config = Arc::clone(&alert_config);
        let metrics = Arc::clone(&alert_metrics);
        Box::new(move |stream| {
            let connector = Arc::clone(&connector);
            let config = Arc::clone(&config);
            let metrics = Arc::clone(&metrics);
            Box::pin(async move {
                // Create alerts stream manually
                let alerts_stream = unfold(stream, |mut stream| async move {
                    while let Some(activity) = stream.next().await {
                        if let Some(alert) = check_for_alerts(&activity) {
                            return Some((alert, stream));
                        }
                    }
                    None
                });
                let wrapped_alerts_stream = StreamWrapper::new(alerts_stream);
                kafka_sink(&*connector, &*config, wrapped_alerts_stream, metrics).await;
            })
        })
    };

    println!("🎯 Running pipeline for 30 seconds...");

    // --- Run the pipeline ---
    let pipeline_task = run_pipeline(
        validated_stream,
        vec![validated_sink, analytics_sink, alerts_sink]
    );

    // Run pipeline for 30 seconds
    tokio::select! {
        _ = pipeline_task => {
            println!("Pipeline completed");
        }
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            println!("⏰ Time limit reached, stopping pipeline");
        }
    }

    // --- Print final metrics ---
    println!("\n📊 Final Pipeline Metrics:");
    println!(
        "✅ Validated Sink: processed={}, errors={}",
        validated_metrics.processed.load(Ordering::Relaxed),
        validated_metrics.errors.load(Ordering::Relaxed)
    );
    println!(
        "📈 Analytics Sink: processed={}, errors={}",
        analytics_metrics.processed.load(Ordering::Relaxed),
        analytics_metrics.errors.load(Ordering::Relaxed)
    );
    println!(
        "🚨 Alert Sink: processed={}, errors={}",
        alert_metrics.processed.load(Ordering::Relaxed),
        alert_metrics.errors.load(Ordering::Relaxed)
    );
    
    println!("🏁 Kafka Data Pipeline Example completed!");
}
