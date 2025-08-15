use rs2_stream::rs2;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::session::{SessionBuilder, SessionPreset, clear_global_session, set_global_session};
use rs2_stream::stream_performance_metrics::HealthThresholds;
use rs2_stream::stream::StreamExt;

use serde::{Serialize, Deserialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

/// Real-world data structure representing a user interaction event
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    event_type: String,
    timestamp: u64,
    data: serde_json::Value,
    session_id: String,
    device_info: DeviceInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DeviceInfo {
    device_type: String,
    os_version: String,
    app_version: String,
    network_type: String,
}

/// Analytics result from processing user events
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalyticsResult {
    user_id: String,
    session_id: String,
    metrics: UserMetrics,
    recommendations: Vec<String>,
    risk_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserMetrics {
    total_events: u32,
    unique_event_types: u32,
    session_duration: Duration,
    interaction_frequency: f64,
    device_switches: u32,
}

/// Media content recommendation
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContentRecommendation {
    user_id: String,
    content_id: String,
    content_type: String,
    priority: String, // Simplified to avoid MediaPriority serialization issues
    quality_level: String, // Simplified to avoid QualityLevel serialization issues
    personalized_score: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Comprehensive Session Pipeline Example");
    println!("{}", "=".repeat(60));

    // Clear any existing global session
    clear_global_session();

    // Build a comprehensive session configuration for production use
    let session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .parallel(|config| {
            config.concurrency = 8;
            config.max_buffer_size = 2048;
            config.timeout = Duration::from_secs(60);
            config.sequence_timeout = Duration::from_secs(30);
            config.task_timeout = Duration::from_secs(15);
        })
        .stream_buffer(|config| {
            config.initial_capacity = 500;
            config.max_capacity = Some(5000);
            config.growth_strategy = rs2_stream::stream_configuration::GrowthStrategy::Exponential(1.5);
        })
        .backpressure(|config| {
            config.strategy = rs2_stream::rs2::BackpressureStrategy::DropOldest;
            config.buffer_size = 2000;
        })
        .state(|config| {
            config.max_size = Some(10000);
            config.ttl = Duration::from_secs(3600); // 1 hour
            config.cleanup_interval = Duration::from_secs(300); // 5 minutes
        })
        .metrics(|config| {
            config.enabled = true;
            config.sample_rate = 0.1; // Sample 10% of items
            config.labels = vec![
                ("environment".to_string(), "production".to_string()),
                ("service".to_string(), "user_analytics".to_string()),
                ("version".to_string(), "1.0.0".to_string()),
            ];
        })
        .time_window(|config| {
            config.window_size = Duration::from_secs(300); // 5 minutes
            config.slide_interval = Duration::from_secs(60); // 1 minute
        })
        .build();

    // Set the global session
    set_global_session(session);
    println!("✅ Session configured with production settings");

    // Simulate real-time user events stream
    let user_events = generate_user_events(100);
    println!("📊 Generated {} user events for processing", user_events.len());

    // Create the main processing pipeline
    let (processing_pipeline, metrics) = rs2::from_iter_rs2(user_events)
        // 1. Add metrics tracking with session config
        .with_metrics_with_session_rs2("user_events_pipeline".to_string(), HealthThresholds {
            max_consecutive_errors: 10,
            max_error_rate: 0.05,
        });
    
    let processing_pipeline = processing_pipeline
        // 2. Apply backpressure using session config
        .auto_backpressure_with_session_rs2()
        // 3. Process events in parallel with session config
        .par_eval_map_with_session_config_rs2(|event| {
            let event = event.clone();
            async move {
                // Simulate async processing (API calls, database queries, etc.)
                sleep(Duration::from_millis(10)).await;
                
                // Enrich event with additional data
                let mut enriched_event = event.clone();
                enriched_event.data = serde_json::json!({
                    "processed_at": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    "processing_node": "worker-1",
                    "enrichment_score": rand::random::<f64>(),
                });
                
                enriched_event
            }
        })
        // 4. Simple grouping by user (simplified for now)
        .group_by_rs2(|event| event.user_id.clone())
        .map(|(user_id, events)| {
            let events: Vec<_> = events.into_iter().collect();
            let analytics = AnalyticsResult {
                user_id: user_id.clone(),
                session_id: events.first().unwrap().session_id.clone(),
                metrics: UserMetrics {
                    total_events: events.len() as u32,
                    unique_event_types: events.iter().map(|e| &e.event_type).collect::<std::collections::HashSet<_>>().len() as u32,
                    session_duration: Duration::from_secs(300), // Simplified
                    interaction_frequency: events.len() as f64 / 300.0, // events per second
                    device_switches: events.iter().map(|e| &e.device_info.device_type).collect::<std::collections::HashSet<_>>().len() as u32,
                },
                recommendations: generate_recommendations(&events),
                risk_score: calculate_risk_score(&events),
            };
            analytics
        })
        // 5. Simple aggregation (removed complex stateful window for now)
        .map(|result| result)
        // 6. Generate media recommendations with session config
        .par_eval_map_with_session_config_rs2(|analytics| {
            let analytics = analytics.clone();
            async move {
                let recommendations = analytics.recommendations.iter()
                    .filter(|rec| rec.contains("content"))
                    .map(|rec| {
                        ContentRecommendation {
                            user_id: analytics.user_id.clone(),
                            content_id: format!("content_{}", rand::random::<u32>()),
                            content_type: "video".to_string(),
                            priority: if analytics.risk_score > 0.7 {
                                "High".to_string()
                            } else {
                                "Normal".to_string()
                            },
                            quality_level: if analytics.metrics.interaction_frequency > 2.0 {
                                "High".to_string()
                            } else {
                                "Medium".to_string()
                            },
                            personalized_score: 1.0 - analytics.risk_score,
                        }
                    })
                    .collect::<Vec<_>>();

                (analytics, recommendations)
            }
        })
        // 7. Final collection with session config
        .collect_with_session_rs2();

    // Execute the pipeline
    println!("🔄 Executing comprehensive processing pipeline...");
    let start_time = SystemTime::now();
    
    let results: Vec<(AnalyticsResult, Vec<ContentRecommendation>)> = processing_pipeline.await;
    
    let processing_time = start_time.elapsed().unwrap();
    println!("✅ Pipeline completed in {:?}", processing_time);

    // Display metrics
    let metrics_guard = metrics.lock().await;
    println!("📊 Pipeline Metrics:");
    println!("  Items processed: {}", metrics_guard.items_processed);
    println!("  Bytes processed: {}", metrics_guard.bytes_processed);
    println!("  Errors: {}", metrics_guard.errors);
    println!("  Processing time: {:?}", metrics_guard.processing_time);
    drop(metrics_guard);

    // Display results summary
    println!("\n📈 Processing Results Summary:");
    println!("{}", "=".repeat(40));
    println!("Total results processed: {}", results.len());
    
    if let Some((first_analytics, first_recommendations)) = results.first() {
        println!("Sample user: {}", first_analytics.user_id);
        println!("Sample metrics: {} events, {} unique types", 
            first_analytics.metrics.total_events,
            first_analytics.metrics.unique_event_types);
        println!("Sample recommendations: {}", first_recommendations.len());
        println!("Risk score: {:.2}", first_analytics.risk_score);
    }

    // Show session configuration summary
    println!("\n⚙️  Session Configuration Summary:");
    println!("{}", "=".repeat(40));
    println!("Parallel concurrency: 8");
    println!("Buffer capacity: 5000 items");
    println!("Backpressure strategy: DropOldest");
    println!("State TTL: 1 hour");
    println!("Media chunk size: 8KB");
    println!("Metrics sampling: 10%");
    println!("Time window: 5 minutes");

    // Clean up
    clear_global_session();
    println!("\n🧹 Session cleaned up");

    Ok(())
}

/// Generate realistic user events for demonstration
fn generate_user_events(count: usize) -> Vec<UserEvent> {
    let event_types = vec![
        "page_view", "button_click", "form_submit", "video_play", 
        "video_pause", "scroll", "search", "purchase", "login", "logout"
    ];
    
    let device_types = vec!["mobile", "desktop", "tablet"];
    let os_versions = vec!["iOS 17.0", "Android 14", "Windows 11", "macOS 14"];
    let network_types = vec!["wifi", "4g", "5g", "ethernet"];

    (0..count).map(|i| {
        let user_id = format!("user_{:03}", (i % 50) + 1);
        let session_id = format!("session_{:03}", (i % 20) + 1);
        
        UserEvent {
            user_id,
            event_type: event_types[i % event_types.len()].to_string(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() + i as u64,
            data: serde_json::json!({
                "page_url": format!("https://example.com/page/{}", i % 10),
                "referrer": "https://google.com",
                "user_agent": "Mozilla/5.0 (compatible; ExampleBot/1.0)",
                "viewport_width": 1920,
                "viewport_height": 1080,
            }),
            session_id,
            device_info: DeviceInfo {
                device_type: device_types[i % device_types.len()].to_string(),
                os_version: os_versions[i % os_versions.len()].to_string(),
                app_version: "2.1.0".to_string(),
                network_type: network_types[i % network_types.len()].to_string(),
            },
        }
    }).collect()
}

/// Generate personalized recommendations based on user behavior
fn generate_recommendations(events: &[UserEvent]) -> Vec<String> {
    let mut recommendations = Vec::new();
    
    if events.iter().any(|e| e.event_type == "video_play") {
        recommendations.push("content_recommendation: trending_videos".to_string());
    }
    
    if events.iter().any(|e| e.event_type == "purchase") {
        recommendations.push("content_recommendation: related_products".to_string());
    }
    
    if events.iter().any(|e| e.event_type == "search") {
        recommendations.push("content_recommendation: search_suggestions".to_string());
    }
    
    if recommendations.is_empty() {
        recommendations.push("content_recommendation: general_content".to_string());
    }
    
    recommendations
}

/// Calculate risk score based on user behavior patterns
fn calculate_risk_score(events: &[UserEvent]) -> f64 {
    let mut risk_score = 0.0_f64;
    
    // High frequency of events might indicate bot behavior
    if events.len() > 50 {
        risk_score += 0.3;
    }
    
    // Multiple device types in short time
    let unique_devices = events.iter()
        .map(|e| &e.device_info.device_type)
        .collect::<std::collections::HashSet<_>>()
        .len();
    
    if unique_devices > 2 {
        risk_score += 0.2;
    }
    
    // Suspicious event patterns
    if events.iter().any(|e| e.event_type == "purchase") && events.len() < 3 {
        risk_score += 0.4;
    }
    
    // Normalize to 0.0 - 1.0 range
    risk_score.min(1.0_f64)
} 