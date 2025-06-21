//! Advanced Analytics Example
//!
//! Demonstrates RS2's production-ready advanced analytics capabilities:
//! - Time-based windowed aggregations with custom time semantics
//! - Stream joins with time windows
//!
//! This example shows real-world scenarios like user behavior analysis
//! and system monitoring.

use futures_util::stream::StreamExt;
use rs2_stream::advanced_analytics::*;
use rs2_stream::rs2::*;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;

// ================================
// Data Models
// ================================

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: u64,
    event_type: String,
    timestamp: SystemTime,
    metadata: std::collections::HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserProfile {
    user_id: u64,
    risk_score: f64,
    location: String,
    last_login: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SystemMetric {
    service: String,
    metric_type: String,
    value: f64,
    timestamp: SystemTime,
}

#[derive(Debug, Clone)]
struct WindowStats {
    window_start: SystemTime,
    window_end: SystemTime,
    event_count: usize,
    unique_users: usize,
    event_types: std::collections::HashMap<String, usize>,
}

// ================================
// Example 1: Time-based Windowed Aggregations
// ================================

async fn time_windowed_aggregations() {
    println!("\n==================================================");
    println!("🟦 [1/3] Time-based Windowed Aggregations");
    println!("==================================================");
    println!("Preparing user event stream and windowing configuration...");

    // Create a stream of user events
    let now = SystemTime::now();
    let events = vec![
        UserEvent {
            user_id: 1,
            event_type: "login".to_string(),
            timestamp: now,
            metadata: [("ip".to_string(), "192.168.1.1".to_string())]
                .into_iter()
                .collect(),
        },
        UserEvent {
            user_id: 2,
            event_type: "purchase".to_string(),
            timestamp: now + Duration::from_secs(10),
            metadata: [("amount".to_string(), "100.0".to_string())]
                .into_iter()
                .collect(),
        },
        UserEvent {
            user_id: 1,
            event_type: "logout".to_string(),
            timestamp: now + Duration::from_secs(20),
            metadata: std::collections::HashMap::new(),
        },
    ];

    let config = TimeWindowConfig {
        window_size: Duration::from_secs(60),
        slide_interval: Duration::from_secs(30),
        watermark_delay: Duration::from_secs(5),
        allowed_lateness: Duration::from_secs(2),
    };

    let windowed_stream = from_iter(events)
        .window_by_time_rs2(config, |event| event.timestamp)
        .map_rs2(|window| {
            // Calculate statistics for each window
            let mut stats = WindowStats {
                window_start: window.start_time,
                window_end: window.end_time,
                event_count: window.events.len(),
                unique_users: 0,
                event_types: std::collections::HashMap::new(),
            };

            let mut user_ids = std::collections::HashSet::new();
            for event in &window.events {
                user_ids.insert(event.user_id);
                *stats
                    .event_types
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;
            }
            stats.unique_users = user_ids.len();

            stats
        });

    let results = windowed_stream.collect::<Vec<_>>().await;
    println!("\n[Results] Windowed User Event Statistics:");
    for (i, stats) in results.iter().enumerate() {
        println!("  Window {}:", i + 1);
        println!(
            "    Time: {:?} - {:?}",
            stats.window_start, stats.window_end
        );
        println!("    Events: {}", stats.event_count);
        println!("    Unique Users: {}", stats.unique_users);
        println!("    Event Types: {:?}", stats.event_types);
    }
    println!("--------------------------------------------------");
    println!("✅ Completed time-based windowed aggregations\n");
}

// ================================
// Example 2: Stream Joins with Time Windows
// ================================

async fn time_windowed_joins() {
    println!("\n==================================================");
    println!("🟩 [2/3] Stream Joins with Time Windows");
    println!("==================================================");
    println!("Preparing user events and profiles for join...");

    let now = SystemTime::now();
    // Create streams of user events and profiles
    let user_events = vec![
        UserEvent {
            user_id: 1,
            event_type: "login".to_string(),
            timestamp: now,
            metadata: std::collections::HashMap::new(),
        },
        UserEvent {
            user_id: 2,
            event_type: "purchase".to_string(),
            timestamp: now + Duration::from_secs(2),
            metadata: [("amount".to_string(), "50.0".to_string())]
                .into_iter()
                .collect(),
        },
    ];

    let user_profiles = vec![
        UserProfile {
            user_id: 1,
            risk_score: 0.1,
            location: "US".to_string(),
            last_login: now,
        },
        UserProfile {
            user_id: 2,
            risk_score: 0.8,
            location: "UK".to_string(),
            last_login: now + Duration::from_secs(2),
        },
    ];

    let config = TimeJoinConfig {
        window_size: Duration::from_secs(60),
        watermark_delay: Duration::from_secs(5),
    };

    // Join events with profiles using time windows
    let enriched_events = from_iter(user_events)
        .join_with_time_window_rs2::<UserProfile, _, _, _, u64, fn(&UserEvent) -> u64, fn(&UserProfile) -> u64>(
            from_iter(user_profiles),
            config,
            |event| event.timestamp,
            |profile| profile.last_login,
            |event, profile| (event, profile),
            None::<(fn(&UserEvent) -> u64, fn(&UserProfile) -> u64)>, // No key selector - cross join
        );

    let results = enriched_events.collect::<Vec<_>>().await;
    println!("\n[Results] Enriched Events:");
    for (i, (event, profile)) in results.iter().enumerate() {
        println!("  Match {}:", i + 1);
        println!(
            "    User {} ({}): {}",
            event.user_id, profile.location, event.event_type
        );
        println!("    Risk Score: {}", profile.risk_score);
    }
    println!("--------------------------------------------------");
    println!("✅ Completed time-windowed stream joins\n");
}

// ================================
// Example 3: System Monitoring with Windowed Analytics
// ================================

async fn system_monitoring() {
    println!("\n==================================================");
    println!("🟨 [3/3] System Monitoring with Windowed Analytics");
    println!("==================================================");
    println!("Preparing system metrics and windowing configuration...");

    let now = SystemTime::now();
    // Create system metrics stream
    let metrics = vec![
        SystemMetric {
            service: "database".to_string(),
            metric_type: "cpu_usage".to_string(),
            value: 85.0,
            timestamp: now,
        },
        SystemMetric {
            service: "api".to_string(),
            metric_type: "response_time".to_string(),
            value: 2000.0, // 2 seconds
            timestamp: now + Duration::from_secs(1),
        },
        SystemMetric {
            service: "frontend".to_string(),
            metric_type: "error_rate".to_string(),
            value: 15.0, // 15% error rate
            timestamp: now + Duration::from_secs(2),
        },
    ];

    // Use windowed aggregations to monitor system health
    let config = TimeWindowConfig {
        window_size: Duration::from_secs(60),
        slide_interval: Duration::from_secs(30),
        watermark_delay: Duration::from_secs(5),
        allowed_lateness: Duration::from_secs(2),
    };

    let monitoring_stream = from_iter(metrics)
        .window_by_time_rs2(config, |metric| metric.timestamp)
        .map_rs2(|window| {
            let mut total_cpu = 0.0;
            let mut total_response_time = 0.0;
            let mut total_error_rate = 0.0;
            let mut cpu_count = 0;
            let mut response_count = 0;
            let mut error_count = 0;

            for metric in &window.events {
                match metric.metric_type.as_str() {
                    "cpu_usage" => {
                        total_cpu += metric.value;
                        cpu_count += 1;
                    }
                    "response_time" => {
                        total_response_time += metric.value;
                        response_count += 1;
                    }
                    "error_rate" => {
                        total_error_rate += metric.value;
                        error_count += 1;
                    }
                    _ => {}
                }
            }

            println!(
                "System Health Window: {:?} - {:?}",
                window.start_time, window.end_time
            );
            if cpu_count > 0 {
                println!("  Average CPU Usage: {:.1}%", total_cpu / cpu_count as f64);
            }
            if response_count > 0 {
                println!(
                    "  Average Response Time: {:.0}ms",
                    total_response_time / response_count as f64
                );
            }
            if error_count > 0 {
                println!(
                    "  Average Error Rate: {:.1}%",
                    total_error_rate / error_count as f64
                );
            }
        });

    println!("\n[Results] System Health Windows:");
    let _results = monitoring_stream.collect::<Vec<_>>().await;
    println!("--------------------------------------------------");
    println!("✅ Completed system monitoring analytics\n");
}

// ================================
// Main Function
// ================================

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("\n================ RS2 Advanced Analytics Example ================");
        println!("This demo showcases production-ready analytics features:\n");
        time_windowed_aggregations().await;
        time_windowed_joins().await;
        system_monitoring().await;
        println!("==================== Summary ====================");
        println!("✅ Time-based windowed aggregations with custom time semantics");
        println!("✅ Stream joins with time windows");
        println!("✅ Real-world scenarios: user behavior analysis, system monitoring");
        println!("📋 Note: Complex Event Processing (CEP) is planned for future releases");
        println!("==================================================\n");
    });
}
