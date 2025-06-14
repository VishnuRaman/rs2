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
//! ## Requirements
//!
//! - A running Kafka cluster (or local instance)
//! - Kafka topics: "user-activity", "validated-activity", "activity-analytics", "activity-alerts"

use rs2::connectors::{KafkaConnector, StreamConnector};
use rs2::connectors::kafka_connector::KafkaConfig;
use rs2::rs2::*;
use rs2::stream_configuration::{BufferConfig, GrowthStrategy};
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

        // Create a Kafka connector
        // In a real application, you would use your actual Kafka cluster address
        let connector = KafkaConnector::new("localhost:9092")
            .with_consumer_group("activity-processor-group");

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

        // Send the activity stream to Kafka
        // In a real application, you would handle errors more gracefully
        println!("  Sending activities to Kafka topic 'user-activity'...");
        let producer_result = <KafkaConnector as StreamConnector<String>>::to_sink(
            &connector, activity_stream, producer_config.clone()
        ).await;

        match producer_result {
            Ok(metadata) => println!("  ✅ Successfully sent activities to Kafka: {:?}", metadata),
            Err(e) => {
                println!("  ❌ Failed to send activities to Kafka: {:?}", e);
                println!("  💡 This example will continue with simulated data instead.");
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

        // Create a stream from Kafka
        // In a real application, you would handle errors more gracefully
        println!("  Consuming activities from Kafka topic 'user-activity'...");
        let kafka_stream_result = <KafkaConnector as StreamConnector<String>>::from_source(
            &connector, consumer_config
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
            .map_parallel_with_concurrency_rs2(4, |json: String| {
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

        // In a real application, you would publish the results to Kafka
        // Here we'll just print some summary statistics

        println!("  Analytics Summary:");
        println!("  -----------------");
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

        println!("\n  Alerts Summary:");
        println!("  --------------");
        for alert in &alerts {
            println!("  [Severity {}] {}: {} - {}", 
                    alert.severity,
                    alert.alert_type,
                    alert.message,
                    alert.timestamp);
        }

        println!("\n✅ Kafka Data Streaming Pipeline Example Completed");
    });
}
