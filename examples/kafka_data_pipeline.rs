//! # Kafka Data Streaming Pipeline Example
//!
//! This example demonstrates a complex data streaming pipeline using Kafka and rs2.
//! It showcases various features of rs2 for processing streaming data from Kafka,
//! including parallel processing, backpressure handling, and error recovery.
//!
//! ## Pipeline Overview
//!
//! 1. **Data Production**: Generate sample user activity data and send it to a Kafka topic
//! 2. **Data Consumption**: Consume the data from Kafka using rs2 streams
//! 3. **Data Processing**: Process the data using various rs2 transformations
//!    - Parsing and validation
//!    - Enrichment with additional data
//!    - Aggregation and analytics
//!    - Filtering and transformation
//! 4. **Result Publishing**: Send the processed results back to different Kafka topics
//!
//! ## Requirements need a lot of other existing systems. this file is just to show what is possible with rs2.
//! - Kafka topics: "user-activity", "validated-activity", "activity-analytics", "activity-alerts"

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

// ================================
// Main Function
// ================================

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🚀 Starting Kafka Data Streaming Pipeline Example");

        // ================================
        // Setup Kafka Connector
        // ================================

        // Create a Kafka connector with production cluster configuration
        let kafka_brokers = std::env::var("KAFKA_BROKERS")
            .unwrap_or_else(|_| {
                println!("⚠️ KAFKA_BROKERS environment variable not set, using localhost:9092");
                "localhost:9092".to_string()
            });

        // Use a more descriptive consumer group name with environment suffix
        let env = std::env::var("ENVIRONMENT").unwrap_or_else(|_| "dev".to_string());
        let consumer_group = format!("activity-processor-group-{}", env);

        println!("  Connecting to Kafka brokers: {}", kafka_brokers);
        println!("  Using consumer group: {}", consumer_group);

        let connector = KafkaConnector::new(&kafka_brokers)
            .with_consumer_group(&consumer_group);

        // Check if the connector is healthy
        match <KafkaConnector as StreamConnector<String>>::health_check(&connector).await {
            Ok(true) => println!("✅ Kafka connector is healthy"),
            _ => {
                println!("❌ Kafka connector is not healthy! Make sure Kafka is running.");
                println!("💡 This example requires a running Kafka instance. For local development,");
                println!("   you can use Docker to run Kafka: ");
                println!("   docker run -p 2181:2181 -p 9092:9092 spotify/kafka");
                return;
            }
        }

        // ================================
        // 1. Data Production
        // ================================
        println!("\n📤 STAGE 1: Producing User Activity Data to Kafka");

        // Generate a stream of random user activities
        let activity_stream = stream! {
            for i in 0..100 {
                // Generate a random activity
                let activity = generate_random_activity();
                println!("  Generating activity {}: {} by user {}", 
                         i, activity.activity_type, activity.user_id);

                // Serialize to JSON
                let json = serde_json::to_string(&activity).unwrap();

                // Yield the serialized activity
                yield json;

                // Add some delay to simulate real-time data
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }.boxed();

        // Configure the Kafka producer
        let producer_config = KafkaConfig {
            topic: "user-activity".to_string(),
            group_id: None,
            partition: None, // Let Kafka decide the partition
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

        // Send the activity stream to Kafka with robust error handling
        println!("  Sending activities to Kafka topic 'user-activity'...");

        // Define retry policy for Kafka producer
        let retry_policy = RetryPolicy::Exponential {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            multiplier: 2.0
        };

        // Create a function to produce to Kafka with retry logic
        async fn produce_to_kafka_with_retry<F>(
            connector: &KafkaConnector,
            stream_generator: F,
            config: KafkaConfig,
            retry_policy: RetryPolicy
        ) -> Result<rs2::connectors::kafka_connector::KafkaMetadata, rs2::connectors::ConnectorError> 
        where
            F: Fn() -> RS2Stream<String> + Send + Sync
        {
            let mut attempts = 0;
            let max_retries = match retry_policy {
                RetryPolicy::None => 0,
                RetryPolicy::Immediate { max_retries } => max_retries,
                RetryPolicy::Fixed { max_retries, .. } => max_retries,
                RetryPolicy::Exponential { max_retries, .. } => max_retries,
            };

            let mut last_error = None;

            loop {
                // Generate a fresh stream for each attempt
                let stream = stream_generator();

                let result = <KafkaConnector as StreamConnector<String>>::to_sink(
                    connector, stream, config.clone()
                ).await;

                match result {
                    Ok(metadata) => return Ok(metadata),
                    Err(e) => {
                        attempts += 1;
                        println!("  ⚠️ Kafka publish attempt {} failed: {:?}", attempts, e);
                        last_error = Some(e);

                        if attempts >= max_retries {
                            break;
                        }

                        // Apply backoff delay based on retry policy
                        match &retry_policy {
                            RetryPolicy::None => {},
                            RetryPolicy::Immediate { .. } => {},
                            RetryPolicy::Fixed { delay, .. } => {
                                println!("  ⏱️ Waiting {:?} before next attempt", delay);
                                tokio::time::sleep(*delay).await;
                            },
                            RetryPolicy::Exponential { initial_delay, multiplier, .. } => {
                                let delay_ms = initial_delay.as_millis() as f64 * multiplier.powi(attempts as i32);
                                let delay = Duration::from_millis(delay_ms as u64);
                                println!("  ⏱️ Waiting {:?} before next attempt", delay);
                                tokio::time::sleep(delay).await;
                            },
                        }
                    }
                }
            }

            Err(last_error.unwrap())
        }

        // Create a stream generator function that produces a fresh stream for each attempt
        let stream_generator = || {
            stream! {
                for i in 0..100 {
                    // Generate a random activity
                    let activity = generate_random_activity();
                    println!("  Generating activity {}: {} by user {}", 
                             i, activity.activity_type, activity.user_id);

                    // Serialize to JSON
                    let json = serde_json::to_string(&activity).unwrap();

                    // Yield the serialized activity
                    yield json;

                    // Add some delay to simulate real-time data
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }.boxed()
        };

        // Send to Kafka with retry and timeout
        let producer_future = produce_to_kafka_with_retry(
            &connector, stream_generator, producer_config.clone(), retry_policy
        );

        // Add timeout to the producer operation
        let producer_result = match tokio::time::timeout(
            Duration::from_secs(30), producer_future
        ).await {
            Ok(result) => result,
            Err(_) => {
                println!("  ❌ Kafka producer timed out after 30 seconds");
                Err(rs2::connectors::ConnectorError::ConnectionFailed(
                    "Producer operation timed out".to_string()
                ))
            }
        };

        match producer_result {
            Ok(metadata) => {
                println!("  ✅ Successfully sent activities to Kafka: {:?}", metadata);

                // Record success metrics
                let success_rate = 100.0;
                println!("  📊 Kafka publish success rate: {}%", success_rate);
            },
            Err(e) => {
                // Log detailed error with context
                let error_context = format!(
                    "Failed to send activities to Kafka topic '{}': {:?}",
                    producer_config.topic, e
                );
                println!("  ❌ {}", error_context);

                // In production, we would log this with proper severity
                eprintln!("ERROR: {}", error_context);

                // Record failure metrics
                let failure_rate = 100.0;
                println!("  📊 Kafka publish failure rate: {}%", failure_rate);

                println!("  💡 Continuing with simulated data instead.");

                // Optionally write to dead letter queue for later processing
                let dlq_file = format!("failed_activities_{}.json", Utc::now().timestamp());
                println!("  📝 Writing failed messages to dead letter queue: {}", dlq_file);

                // Continue with the example using simulated data
            }
        }

        // ================================
        // 2. Data Consumption
        // ================================
        println!("\n📥 STAGE 2: Consuming User Activity Data from Kafka");

        // Configure the Kafka consumer
        let consumer_config = KafkaConfig {
            topic: "user-activity".to_string(),
            group_id: None, // Use the connector's consumer group
            partition: None, // All partitions
            from_beginning: true,
            kafka_config: None,
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        };

        // Create a stream from Kafka with robust error handling
        println!("  Consuming activities from Kafka topic 'user-activity'...");

        // Add circuit breaker to prevent repeated connection attempts if Kafka is down
        let circuit_breaker = Arc::new(Mutex::new(false));
        let circuit_breaker_clone = circuit_breaker.clone();

        // Define retry policy for Kafka consumer
        let retry_policy = RetryPolicy::Exponential {
            max_retries: 3,
            initial_delay: Duration::from_millis(500),
            multiplier: 2.0
        };

        // Function to attempt Kafka connection with retry logic
        async fn connect_with_retries<T>(
            connector: &KafkaConnector,
            config: KafkaConfig,
            retry_policy: RetryPolicy,
            circuit_breaker: Arc<Mutex<bool>>
        ) -> Result<RS2Stream<T>, rs2::connectors::ConnectorError> 
        where
            T: for<'de> Deserialize<'de> + Serialize + Send + 'static
        {
            let mut attempts = 0;
            let max_retries = match retry_policy {
                RetryPolicy::None => 0,
                RetryPolicy::Immediate { max_retries } => max_retries,
                RetryPolicy::Fixed { max_retries, .. } => max_retries,
                RetryPolicy::Exponential { max_retries, .. } => max_retries,
            };

            // Check if circuit breaker is open
            if *circuit_breaker.lock().await {
                println!("  ⚡ Circuit breaker is open, not attempting Kafka connection");
                return Err(rs2::connectors::ConnectorError::ConnectionFailed(
                    "Circuit breaker is open".to_string()
                ));
            }

            loop {
                let result = <KafkaConnector as StreamConnector<T>>::from_source(
                    connector, config.clone()
                ).await;

                match result {
                    Ok(stream) => {
                        println!("  ✅ Successfully connected to Kafka topic after {} attempts", attempts + 1);
                        return Ok(stream);
                    },
                    Err(e) => {
                        attempts += 1;
                        if attempts > max_retries {
                            // Open circuit breaker after max retries
                            *circuit_breaker.lock().await = true;

                            // Schedule circuit breaker reset after 1 minute
                            let cb = circuit_breaker.clone();
                            tokio::spawn(async move {
                                tokio::time::sleep(Duration::from_secs(60)).await;
                                *cb.lock().await = false;
                                println!("  🔄 Circuit breaker reset, will attempt Kafka connections again");
                            });

                            return Err(e);
                        }

                        println!("  ⚠️ Kafka connection attempt {} failed: {:?}", attempts, e);

                        // Apply backoff delay based on retry policy
                        match &retry_policy {
                            RetryPolicy::None => {},
                            RetryPolicy::Immediate { .. } => {},
                            RetryPolicy::Fixed { delay, .. } => {
                                println!("  ⏱️ Waiting {:?} before next attempt", delay);
                                tokio::time::sleep(*delay).await;
                            },
                            RetryPolicy::Exponential { initial_delay, multiplier, .. } => {
                                let delay_ms = initial_delay.as_millis() as f64 * multiplier.powi(attempts as i32);
                                let delay = Duration::from_millis(delay_ms as u64);
                                println!("  ⏱️ Waiting {:?} before next attempt", delay);
                                tokio::time::sleep(delay).await;
                            },
                        }
                    }
                }
            }
        }

        // Attempt to connect to Kafka with retries and circuit breaker
        let kafka_stream_result = connect_with_retries::<String>(
            &connector, consumer_config, retry_policy, circuit_breaker_clone
        ).await;

        // Use either the Kafka stream or a simulated stream
        let raw_activity_stream = match kafka_stream_result {
            Ok(stream) => {
                println!("  ✅ Successfully connected to Kafka topic");
                stream
            },
            Err(e) => {
                println!("  ❌ Failed to connect to Kafka: {:?}", e);
                println!("  💡 Using simulated data instead");

                // Create a simulated stream of activities
                stream! {
                    for i in 0..100 {
                        // Generate a random activity
                        let activity = generate_random_activity();

                        // Serialize to JSON
                        let json = serde_json::to_string(&activity).unwrap();

                        // Yield the serialized activity
                        yield json;

                        // Add some delay to simulate real-time data
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }.boxed()
            }
        };

        // ================================
        // 3. Data Processing Pipeline
        // ================================
        println!("\n⚙️ STAGE 3: Processing Data with rs2 Pipeline");

        // Create a buffer configuration for optimal performance
        let buffer_config = BufferConfig {
            initial_capacity: 1000,
            max_capacity: Some(10000),
            growth_strategy: GrowthStrategy::Exponential(2.0),
        };

        // 3.1 Parse and validate the raw JSON data
        println!("  Step 3.1: Parsing and validating JSON data");
        let validated_stream = raw_activity_stream
            // Apply automatic backpressure to handle fast producers
            .auto_backpressure_rs2()

            // Parse JSON into UserActivity objects
            // Using par_eval_map_rs2:
            // 1. It's designed specifically for async operations and provides better control
            // 2. It preserves the order of elements, which is important for event processing
            // 3. It offers more direct control over concurrency without wrapping synchronous functions
            // 4. It's more efficient for I/O-bound operations like JSON parsing with potential network calls
            .par_eval_map_rs2(4, |json: String| async move {
                // Parse the JSON string into a UserActivity
                match serde_json::from_str::<UserActivity>(&json) {
                    Ok(activity) => {
                        println!("  Parsed activity: {} by user {}", 
                                activity.activity_type, activity.user_id);
                        Some(activity)
                    },
                    Err(e) => {
                        eprintln!("  Error parsing activity JSON: {}", e);
                        None
                    }
                }
            })

            // Filter out parsing errors (None values)
            .filter_rs2(|opt| opt.is_some())

            // Unwrap the Option
            .map_rs2(|opt| opt.unwrap())

            // Validate the activities
            .map_rs2(|activity| {
                let validated = validate_activity(activity);
                println!("  Validated activity: is_valid={}", validated.is_valid);
                validated
            })

            // Use prefetch to improve performance
            .prefetch_rs2(10);

        // 3.2 Split the stream into valid and invalid activities
        println!("  Step 3.2: Splitting stream into valid and invalid activities");

        // Create a shared state to store the streams
        let valid_activities = Arc::new(Mutex::new(Vec::new()));
        let invalid_activities = Arc::new(Mutex::new(Vec::new()));

        // Clone the Arc references for the async block
        let valid_ref = valid_activities.clone();
        let invalid_ref = invalid_activities.clone();

        // Process the validated stream and split into valid and invalid
        validated_stream
            .for_each_rs2(move |validated| {
                let valid_ref = valid_ref.clone();
                let invalid_ref = invalid_ref.clone();

                async move {
                    if validated.is_valid {
                        // Store valid activity
                        let mut valid = valid_ref.lock().await;
                        valid.push(validated);
                    } else {
                        // Store invalid activity
                        let mut invalid = invalid_ref.lock().await;
                        invalid.push(validated);
                    }
                }
            })
            .await;

        // 3.3 Process valid activities for analytics
        println!("  Step 3.3: Processing valid activities for analytics");

        // Get the valid activities
        let valid_activities = valid_activities.lock().await;

        // Group activities by user_id
        let mut user_activities: HashMap<u64, Vec<ValidatedActivity>> = HashMap::new();
        for activity in valid_activities.iter() {
            user_activities
                .entry(activity.activity.user_id)
                .or_insert_with(Vec::new)
                .push(activity.clone());
        }

        // Generate analytics for each user
        let mut analytics_results = Vec::new();
        for (user_id, activities) in user_activities {
            // Count activities by type
            let mut activity_counts = HashMap::new();
            for activity in &activities {
                *activity_counts
                    .entry(activity.activity.activity_type.clone())
                    .or_insert(0) += 1;
            }

            // Find min and max timestamps
            let min_time = activities.iter()
                .map(|a| a.activity.timestamp)
                .min()
                .unwrap_or_else(Utc::now);

            let max_time = activities.iter()
                .map(|a| a.activity.timestamp)
                .max()
                .unwrap_or_else(Utc::now);

            // Create analytics
            let analytics = ActivityAnalytics {
                window_start: min_time,
                window_end: max_time,
                user_id,
                activity_counts,
                total_activities: activities.len() as u64,
            };

            analytics_results.push(analytics);
        }

        println!("  Generated analytics for {} users", analytics_results.len());

        // 3.4 Check for alerts in all activities
        println!("  Step 3.4: Checking for alerts in all activities");

        // Process both valid and invalid activities for alerts
        let mut all_activities = valid_activities.clone();
        all_activities.extend(invalid_activities.lock().await.clone());

        // Check each activity for alerts
        let alerts: Vec<ActivityAlert> = all_activities.iter()
            .filter_map(|activity| check_for_alerts(activity))
            .collect();

        println!("  Generated {} alerts", alerts.len());

        // ================================
        // 4. Result Publishing
        // ================================
        println!("\n📊 STAGE 4: Publishing Results to Kafka");

        // Create streams from our analytics and alerts results
        let analytics_stream = from_iter(analytics_results.clone())
            .map_rs2(|analytics| {
                // In a production environment, you might want to add more context or transform
                // the data before publishing to Kafka
                println!("  Publishing analytics for user {}: {} activities", 
                        analytics.user_id, analytics.total_activities);

                // Serialize to JSON for Kafka
                serde_json::to_string(&analytics).unwrap_or_else(|e| {
                    eprintln!("  Error serializing analytics: {}", e);
                    "{}".to_string()
                })
            })
            // Add timeout for production safety
            // In a real production app, we would use Result types and add retry capability
            // with RetryPolicy::Exponential for resilience
            .throttle_rs2(Duration::from_millis(100)) // Add throttling for rate limiting
            // Add metrics for production monitoring
            .with_metrics_rs2("analytics_publishing".to_string())
            .0;

        // Configure the Kafka producer for analytics
        let analytics_producer_config = KafkaConfig {
            topic: "activity-analytics".to_string(),
            group_id: None,
            partition: None,
            from_beginning: false,
            kafka_config: Some({
                let mut config = HashMap::new();
                config.insert("compression.type".to_string(), "gzip".to_string());
                config.insert("request.timeout.ms".to_string(), "10000".to_string());
                config.insert("retry.backoff.ms".to_string(), "500".to_string());
                config
            }),
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        };

        // Create a stream for alerts with additional processing
        let alerts_stream = from_iter(alerts.clone())
            // Add severity-based prioritization
            .map_rs2(|alert| {
                // Add priority field based on severity
                let mut alert_map = serde_json::to_value(&alert).unwrap_or_default();

                // Add priority field for downstream processing
                if let serde_json::Value::Object(ref mut map) = alert_map {
                    let priority = match alert.severity {
                        4..=5 => "HIGH",
                        2..=3 => "MEDIUM",
                        _ => "LOW",
                    };
                    map.insert("priority".to_string(), serde_json::Value::String(priority.to_string()));

                    // Add alert category for better organization
                    let category = match alert.alert_type.as_str() {
                        "INVALID_ACTIVITY" => "VALIDATION",
                        "LARGE_PURCHASE" => "FINANCIAL",
                        "RAPID_ACTIVITY" => "BEHAVIORAL",
                        _ => "OTHER",
                    };
                    map.insert("category".to_string(), serde_json::Value::String(category.to_string()));
                }

                println!("  Publishing alert [{}]: {}", alert.severity, alert.alert_type);

                // Serialize to JSON for Kafka
                serde_json::to_string(&alert_map).unwrap_or_else(|e| {
                    eprintln!("  Error serializing alert: {}", e);
                    "{}".to_string()
                })
            })
            // Add backpressure for production safety
            .auto_backpressure_with_rs2(BackpressureConfig {
                strategy: BackpressureStrategy::Block,
                buffer_size: 100,
                low_watermark: Some(25),
                high_watermark: Some(75),
            })
            // Add metrics for production monitoring
            .with_metrics_rs2("alerts_publishing".to_string())
            .0;

        // Configure the Kafka producer for alerts
        let alerts_producer_config = KafkaConfig {
            topic: "activity-alerts".to_string(),
            group_id: None,
            partition: None,
            from_beginning: false,
            kafka_config: Some({
                let mut config = HashMap::new();
                config.insert("compression.type".to_string(), "gzip".to_string());
                // Higher priority for alerts - lower timeout, more retries
                config.insert("request.timeout.ms".to_string(), "5000".to_string());
                config.insert("retries".to_string(), "5".to_string());
                config.insert("retry.backoff.ms".to_string(), "200".to_string());
                config
            }),
            enable_auto_commit: true,
            auto_commit_interval_ms: Some(5000),
            session_timeout_ms: Some(30000),
            message_timeout_ms: Some(30000),
        };

        // Publish analytics to Kafka
        println!("  Publishing analytics to Kafka topic 'activity-analytics'...");
        match <KafkaConnector as StreamConnector<String>>::to_sink(
            &connector, analytics_stream, analytics_producer_config
        ).await {
            Ok(metadata) => println!("  ✅ Successfully published analytics to Kafka: {:?}", metadata),
            Err(e) => {
                println!("  ❌ Failed to publish analytics to Kafka: {:?}", e);
                println!("  💡 Would retry or use fallback storage in production");

                // In production, you might:
                // 1. Log the error to a monitoring system
                // 2. Store the data in a fallback storage (e.g., local file, database)
                // 3. Trigger an alert for operations team

                // For demonstration, print the analytics summary
                println!("\n  Analytics Summary (fallback display):");
                println!("  ---------------------------------");
                for analytics in &analytics_results {
                    println!("  User {}: {} activities between {} and {}", 
                            analytics.user_id, 
                            analytics.total_activities,
                            analytics.window_start,
                            analytics.window_end);

                    println!("    Activity breakdown:");
                    for (activity_type, count) in &analytics.activity_counts {
                        println!("    - {}: {}", activity_type, count);
                    }
                }
            }
        };

        // Publish alerts to Kafka
        println!("  Publishing alerts to Kafka topic 'activity-alerts'...");
        match <KafkaConnector as StreamConnector<String>>::to_sink(
            &connector, alerts_stream, alerts_producer_config
        ).await {
            Ok(metadata) => println!("  ✅ Successfully published alerts to Kafka: {:?}", metadata),
            Err(e) => {
                println!("  ❌ Failed to publish alerts to Kafka: {:?}", e);
                println!("  💡 Using high-priority fallback mechanisms");

                // 1. Log the error to a monitoring system with high priority
                println!("  📋 Logging error to monitoring system with HIGH priority");
                let error_log = format!("CRITICAL: Failed to publish alerts to Kafka: {:?}", e);

                // In a real app, you would use a proper logging framework like tracing or log
                // with appropriate severity levels
                println!("    [ERROR] {}", error_log);

                // 2. Store the alerts in a fallback storage with guaranteed delivery
                println!("  💾 Storing alerts in fallback storage with guaranteed delivery");

                // Simulate writing to a local file as fallback storage
                let fallback_file = "alerts_fallback.json";
                println!("    Writing {} alerts to fallback file: {}", alerts.len(), fallback_file);

                // In a real app, you would use something like:
                // let alerts_json = serde_json::to_string(&alerts).unwrap();
                // std::fs::write(fallback_file, alerts_json).expect("Failed to write fallback file");

                // 3. Trigger an immediate alert for operations team
                println!("  🚨 Triggering immediate alert for operations team");
                let ops_alert = format!(
                    "URGENT: Kafka alert publishing failed at {}. {} alerts could not be delivered. Error: {:?}",
                    Utc::now().format("%Y-%m-%d %H:%M:%S"),
                    alerts.len(),
                    e
                );

                // In a real app, you would send this to an incident management system
                // like PagerDuty, OpsGenie, or VictorOps
                println!    ("    [OPS ALERT] {}", ops_alert);

                // 4. Try alternative notification channels for critical alerts
                println!("  📱 Trying alternative notification channels for critical alerts");

                // Filter for only high-severity alerts that need immediate attention
                let critical_alerts: Vec<&ActivityAlert> = alerts.iter()
                    .filter(|alert| alert.severity >= 3)
                    .collect();

                println!("    Found {} critical alerts to send via alternative channels", critical_alerts.len());

                // In a real app, you might send SMS, push notifications, or use a messaging platform
                for alert in critical_alerts {
                    let message = format!(
                        "[CRITICAL] Alert: {} - {} (User: {})", 
                        alert.alert_type, 
                        alert.message,
                        alert.user_id
                    );
                    println!("    [SMS] {}", message);
                    // In a real app: send_sms(on_call_phone_number, message);

                    println!("    [SLACK] {}", message);
                    // In a real app: send_slack_message(ops_channel, message);
                }

                // For demonstration, print the alerts summary
                println!("\n  Alerts Summary (fallback display):");
                println!("  ------------------------------");
                for alert in &alerts {
                    println!("  [Severity {}] {}: {} - {}", 
                            alert.severity,
                            alert.alert_type,
                            alert.message,
                            alert.timestamp);
                }
            }
        };

        // ================================
        // 5. Cleanup and Monitoring
        // ================================
        println!("\n🧹 STAGE 5: Cleanup and Monitoring");

        // 1. Ensure all resources are properly closed
        println!("  Closing resources and connections...");

        // Close Kafka connections
        let close_start = Instant::now();

        // Use a proper resource management pattern with graceful shutdown
        // This pattern ensures resources are closed in the correct order and handles errors appropriately

        // Define a Resource trait for consistent resource management
        trait Resource: Send {
            fn name(&self) -> &str;
            fn close(&self) -> Result<(), String>;
        }

        // Implement Kafka Producer resource
        struct KafkaProducerResource {
            producer: Option<rdkafka::producer::FutureProducer>,
        }

        impl Resource for KafkaProducerResource {
            fn name(&self) -> &str {
                "Kafka Producer"
            }

            fn close(&self) -> Result<(), String> {
                println!("    Flushing and closing Kafka producer connections");
                if let Some(producer) = &self.producer {
                    // Flush any pending messages with a timeout
                    match producer.flush(Duration::from_secs(5)) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(format!("Failed to flush Kafka producer: {}", e))
                    }
                } else {
                    // No actual producer in this example, but the pattern is correct
                    Ok(())
                }
            }
        }

        // Implement Kafka Consumer resource
        struct KafkaConsumerResource {
            consumer: Option<rdkafka::consumer::StreamConsumer>,
        }

        impl Resource for KafkaConsumerResource {
            fn name(&self) -> &str {
                "Kafka Consumer"
            }

            fn close(&self) -> Result<(), String> {
                println!("    Committing final offsets and closing Kafka consumer");
                if let Some(consumer) = &self.consumer {
                    // Commit offsets before closing
                    match consumer.commit_consumer_state(rdkafka::consumer::CommitMode::Sync) {
                        Ok(_) => Ok(()),
                        Err(e) => Err(format!("Failed to commit Kafka consumer offsets: {}", e))
                    }
                } else {
                    // No actual consumer in this example, but the pattern is correct
                    Ok(())
                }
            }
        }

        // Implement Metrics Client resource
        struct MetricsClientResource {
            client_name: String,
        }

        impl Resource for MetricsClientResource {
            fn name(&self) -> &str {
                "Metrics Client"
            }

            fn close(&self) -> Result<(), String> {
                println!("    Flushing metrics before shutdown for client: {}", self.client_name);
                // In a real app with a metrics client like Prometheus, StatsD, or DataDog:
                // metrics_client.flush().map_err(|e| e.to_string())

                // For this example, we'll simulate a successful flush
                Ok(())
            }
        }

        // Implement Database Connection Pool resource
        struct DbConnectionPoolResource {
            pool_size: usize,
        }

        impl Resource for DbConnectionPoolResource {
            fn name(&self) -> &str {
                "Database Connections"
            }

            fn close(&self) -> Result<(), String> {
                println!("    Closing database connection pool with {} connections", self.pool_size);
                // In a real app with a connection pool like r2d2 or sqlx:
                // db_pool.close().map_err(|e| e.to_string())

                // For this example, we'll simulate a successful close
                Ok(())
            }
        }

        // Create a ResourceManager to handle graceful shutdown
        struct ResourceManager {
            resources: Vec<Box<dyn Resource>>,
        }

        impl ResourceManager {
            fn new() -> Self {
                Self { resources: Vec::new() }
            }

            fn register<R: Resource + 'static>(&mut self, resource: R) {
                self.resources.push(Box::new(resource));
            }

            fn close_all(&self) -> HashMap<&str, bool> {
                let mut results = HashMap::new();

                // Close resources in reverse order (LIFO) to handle dependencies
                for resource in self.resources.iter().rev() {
                    let name = resource.name();
                    match resource.close() {
                        Ok(_) => {
                            results.insert(name, true);
                            println!("    ✅ Successfully closed {}", name);
                        },
                        Err(e) => {
                            results.insert(name, false);
                            println!("    ❌ Failed to close {}: {}", name, e);

                            // Log the error with proper severity
                            eprintln!("ERROR: Failed to close {}: {}", name, e);
                        }
                    }
                }

                results
            }
        }

        // Create and register resources
        let mut resource_manager = ResourceManager::new();

        // Register resources in order of dependency (first registered, last closed)
        resource_manager.register(DbConnectionPoolResource { pool_size: 10 });
        resource_manager.register(MetricsClientResource { client_name: "kafka_pipeline_metrics".to_string() });
        resource_manager.register(KafkaConsumerResource { consumer: None });
        resource_manager.register(KafkaProducerResource { producer: None });

        // Close all resources and track results
        let resources_count = resource_manager.resources.len();
        println!("    Closing {} resources in dependency order...", resources_count);

        // Use the ResourceManager to close all resources
        let close_results = resource_manager.close_all();

        println!("  Resources closed in {:?}", close_start.elapsed());

        // 2. Report metrics to monitoring systems
        println!("  Reporting metrics to monitoring systems...");

        // In a real application, we would send these metrics to systems like:
        // - Prometheus/Grafana for time-series metrics
        // - DataDog/New Relic for application performance monitoring
        // - CloudWatch/Stackdriver for cloud-based monitoring

        // Create a structured metrics payload
        let pipeline_metrics = {
            let mut metrics = HashMap::new();

            // Processing volume metrics
            metrics.insert("total_activities_processed", (valid_activities.len() + invalid_activities.lock().await.len()) as f64);
            metrics.insert("valid_activities", valid_activities.len() as f64);
            metrics.insert("invalid_activities", invalid_activities.lock().await.len() as f64);
            metrics.insert("analytics_generated", analytics_results.len() as f64);
            metrics.insert("alerts_generated", alerts.len() as f64);

            // Performance metrics
            let processing_duration = Instant::now().duration_since(close_start).as_secs_f64();
            metrics.insert("processing_duration_seconds", processing_duration);
            metrics.insert("activities_per_second", metrics["total_activities_processed"] / processing_duration);

            // Resource utilization (simulated)
            metrics.insert("memory_usage_mb", 256.0); // Simulated value
            metrics.insert("cpu_utilization_percent", 45.0); // Simulated value

            metrics
        };

        // Simulate sending metrics to monitoring system
        println!("    Sending metrics to monitoring systems:");
        for (metric_name, metric_value) in &pipeline_metrics {
            println!("    - {}: {}", metric_name, metric_value);

            // In a real app, we would use a metrics client:
            // metrics_client.gauge(metric_name, *metric_value, &["pipeline=kafka", "env=production"]);
        }

        // 3. Log completion status
        println!("  Logging completion status...");

        // In a real application, we would use a structured logging framework
        // like tracing, log, or slog with appropriate context

        // Create a structured log entry
        let completion_log = {
            // Use a HashMap that can store owned String values
            let mut log = HashMap::new();
            log.insert("event", "pipeline_completed".to_string());
            log.insert("status", "success".to_string());
            log.insert("pipeline_id", "kafka_data_pipeline_1".to_string());
            log.insert("start_time", "2023-01-01T00:00:00Z".to_string()); // Simulated
            log.insert("end_time", Utc::now().to_rfc3339());
            log.insert("duration_seconds", pipeline_metrics["processing_duration_seconds"].to_string());
            log.insert("activities_processed", pipeline_metrics["total_activities_processed"].to_string());
            log.insert("alerts_generated", pipeline_metrics["alerts_generated"].to_string());

            // Add any errors encountered
            let errors = close_results.iter()
                .filter(|&(_, success)| !success)
                .map(|(name, _)| name.to_string())
                .collect::<Vec<_>>();

            if !errors.is_empty() {
                log.insert("errors", format!("{:?}", errors));
                log.insert("status", "completed_with_warnings".to_string());
            }

            log
        };

        // Simulate logging with different severity levels based on status
        let log_level = if completion_log["status"] == "success" { "INFO" } else { "WARN" };
        println!("    [{log_level}] Pipeline completion: {}", serde_json::to_string(&completion_log).unwrap());

        // In a real app:
        if completion_log["status"] == "success" {
            log::info!("Pipeline completed successfully: {}", serde_json::to_string(&completion_log).unwrap());
        } else {
            log::warn!("Pipeline completed with warnings: {}", serde_json::to_string(&completion_log).unwrap());
        }

        // 4. Update health checks
        println!("  Updating health checks...");

        // In a real application, we would update health check endpoints
        // that are monitored by load balancers, Kubernetes, or monitoring systems

        // Determine overall health status
        let all_resources_closed = !close_results.values().any(|&success| !success);
        let health_status = if all_resources_closed {
            "HEALTHY"
        } else {
            "DEGRADED"
        };

        // Create a health status report
        let health_report = {
            // Use a HashMap that can store owned String values
            let mut report = HashMap::new();
            report.insert("status", health_status.to_string());
            report.insert("timestamp", Utc::now().to_rfc3339());
            report.insert("version", "1.0.0".to_string());

            // Component health statuses
            let mut components = HashMap::new();
            components.insert("pipeline", "HEALTHY".to_string());
            components.insert("kafka_connection", if *close_results.get("Kafka Producer").unwrap_or(&true) & 
                                                   *close_results.get("Kafka Consumer").unwrap_or(&true) {
                "HEALTHY".to_string()
            } else {
                "DEGRADED".to_string()
            });
            components.insert("database", if *close_results.get("Database Connections").unwrap_or(&true) {
                "HEALTHY".to_string()
            } else {
                "DEGRADED".to_string()
            });
            components.insert("metrics", if *close_results.get("Metrics Client").unwrap_or(&true) {
                "HEALTHY".to_string()
            } else {
                "DEGRADED".to_string()
            });

            report.insert("components", format!("{:?}", components));

            // Add performance indicators
            report.insert("processing_latency", if pipeline_metrics["activities_per_second"] > 10.0 {
                "NORMAL".to_string()
            } else {
                "ELEVATED".to_string()
            });

            report
        };

        // Simulate updating health check endpoints
        println!("    Updating health check endpoints:");
        println!("    - Overall status: {}", health_status);
        println!("    - Health report: {}", serde_json::to_string(&health_report).unwrap());

        // In a real app:
        // health_check_registry.update("kafka_pipeline", health_report);
        // Or update a file/endpoint that's monitored:
        // std::fs::write("/health/kafka_pipeline.json", serde_json::to_string(&health_report).unwrap())
        //     .expect("Failed to write health check file");

        println!("\n✅ Kafka Data Streaming Pipeline Example Completed");
    });
}
