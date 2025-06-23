use rs2_stream::rs2::*;
use rs2_stream::state::{KeyExtractor, StateConfig, StateError};
use rs2_stream::state::stream_ext::StatefulStreamExt;
use serde::{Deserialize, Serialize};
use futures_util::{StreamExt, stream::iter};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::pin::Pin;
use std::task::{Context, Poll};
use futures_core::Stream;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    event_type: String,
    timestamp: u64,
    data: HashMap<String, String>,
    session_id: String,
    page_url: String,
    user_agent: String,
    ip_address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserSession {
    session_id: String,
    user_id: String,
    start_time: u64,
    last_activity: u64,
    page_views: u32,
    total_duration: u64,
    events: Vec<String>,
    is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserMetrics {
    user_id: String,
    total_sessions: u32,
    total_page_views: u32,
    total_duration: u64,
    last_seen: u64,
    favorite_pages: Vec<String>,
    event_counts: HashMap<String, u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RealTimeMetrics {
    timestamp: u64,
    active_users: u32,
    total_events: u32,
    events_per_minute: f64,
    top_pages: Vec<(String, u32)>,
    error_rate: f64,
    avg_session_duration: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Alert {
    alert_type: String,
    message: String,
    timestamp: u64,
    severity: String,
    user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PageView {
    page_url: String,
    view_count: u32,
    unique_users: u32,
    avg_time_on_page: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EventPattern {
    pattern: Vec<String>,
    count: u32,
    first_seen: u64,
    last_seen: u64,
}

// Generate sample user events
fn generate_user_events() -> Vec<UserEvent> {
    let mut events = Vec::new();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    let pages = vec![
        "/home",
        "/products",
        "/cart",
        "/checkout",
        "/profile",
        "/search",
        "/help",
    ];
    
    let event_types = vec![
        "page_view",
        "click",
        "scroll",
        "form_submit",
        "error",
        "api_call",
    ];
    
    for user_id in 1..=100 {
        let session_id = format!("session_{}", user_id);
        let session_start = now - (user_id % 3600); // Different session start times
        
        for event_idx in 0..(5 + (user_id % 10)) { // 5-14 events per user
            let event_time = session_start + (event_idx * 30); // 30 seconds between events
            let event_type = event_types[event_idx as usize % event_types.len()].to_string();
            let page_url = pages[event_idx as usize % pages.len()].to_string();
            
            let mut data = HashMap::new();
            data.insert("referrer".to_string(), "google.com".to_string());
            data.insert("device".to_string(), "desktop".to_string());
            
            if event_type == "error" {
                data.insert("error_code".to_string(), "500".to_string());
                data.insert("error_message".to_string(), "Internal server error".to_string());
            }
            
            events.push(UserEvent {
                user_id: format!("user_{}", user_id),
                event_type,
                timestamp: event_time,
                data,
                session_id: session_id.clone(),
                page_url,
                user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64)".to_string(),
                ip_address: format!("192.168.1.{}", user_id % 255),
            });
        }
    }
    
    events
}

// Key extractors for different stateful operations
struct UserIdExtractor;
impl KeyExtractor<UserEvent> for UserIdExtractor {
    fn extract_key(&self, event: &UserEvent) -> String {
        event.user_id.clone()
    }
}

struct SessionIdExtractor;
impl KeyExtractor<UserEvent> for SessionIdExtractor {
    fn extract_key(&self, event: &UserEvent) -> String {
        event.session_id.clone()
    }
}

struct PageUrlExtractor;
impl KeyExtractor<UserEvent> for PageUrlExtractor {
    fn extract_key(&self, event: &UserEvent) -> String {
        event.page_url.clone()
    }
}

struct EventTypeExtractor;
impl KeyExtractor<UserEvent> for EventTypeExtractor {
    fn extract_key(&self, event: &UserEvent) -> String {
        event.event_type.clone()
    }
}

// Helper function to create a stream with correct trait bounds
fn stream_from_vec<T: Send + Sync + Clone + 'static>(v: Vec<T>) -> impl Stream<Item = T> + Send + Sync + Unpin + 'static {
    struct VecStream<T> {
        data: VecDeque<T>,
    }
    
    impl<T: Send + Sync + Clone + 'static> Stream for VecStream<T> {
        type Item = T;
        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<T>> {
            Poll::Ready(self.data.pop_front())
        }
    }
    
    impl<T: Send + Sync + Clone + 'static> Unpin for VecStream<T> {}
    
    VecStream { data: v.into() }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📊 RS2 Real-Time Analytics Pipeline Example");
    println!("===========================================\n");

    // Initialize state configuration
    let state_config = StateConfig::new()
        .max_size(10000)
        .ttl(Duration::from_secs(3600)); // 1 hour TTL

    // Generate sample data
    let events = generate_user_events();
    println!("📈 Generated {} user events for processing", events.len());

    // 1. Session Management with Stateful Operations
    println!("\n1️⃣ Session Management");
    println!("-------------------");
    
    let session_results: Vec<Result<UserEvent, StateError>> = stream_from_vec(events.clone())
        .stateful_session_rs2(
            state_config.clone(),
            SessionIdExtractor,
            Duration::from_secs(1800),
            |event, is_new_session| {
                if is_new_session {
                    println!("   🆕 New session started: {} for user {}", event.session_id, event.user_id);
                }
                event
            },
        )
        .collect()
        .await;
    let session_stream: Vec<UserEvent> = session_results.into_iter().filter_map(Result::ok).collect();

    println!("   ✅ Processed {} events with session tracking", session_stream.len());

    // 2. User Metrics Aggregation
    println!("\n2️⃣ User Metrics Aggregation");
    println!("---------------------------");
    
    let user_metrics_results: Vec<Result<UserMetrics, StateError>> = stream_from_vec(events.clone())
        .stateful_reduce_rs2(
            state_config.clone(),
            UserIdExtractor,
            UserMetrics {
                user_id: String::new(),
                total_sessions: 0,
                total_page_views: 0,
                total_duration: 0,
                last_seen: 0,
                favorite_pages: Vec::new(),
                event_counts: HashMap::new(),
            },
            |mut metrics, event, _state_access| {
                Box::pin(async move {
                    metrics.user_id = event.user_id.clone();
                    metrics.last_seen = event.timestamp;
                    *metrics.event_counts.entry(event.event_type.clone()).or_insert(0) += 1;
                    if event.event_type == "page_view" {
                        metrics.total_page_views += 1;
                        if !metrics.favorite_pages.contains(&event.page_url) {
                            metrics.favorite_pages.push(event.page_url.clone());
                        }
                    }
                    Ok(metrics)
                })
            },
        )
        .collect()
        .await;
    let user_metrics: Vec<UserMetrics> = user_metrics_results.into_iter().filter_map(Result::ok).collect();

    println!("   ✅ Generated metrics for {} users", user_metrics.len());
    
    // Show some sample metrics
    for metrics in user_metrics.iter().take(3) {
        println!("   👤 User {}: {} page views, {} event types", 
                metrics.user_id, metrics.total_page_views, metrics.event_counts.len());
    }

    // 3. Real-Time Page Analytics
    println!("\n3️⃣ Real-Time Page Analytics");
    println!("---------------------------");
    
    let page_analytics_results: Vec<Result<PageView, StateError>> = stream_from_vec(events.clone())
        .stateful_group_by_rs2(
            state_config.clone(),
            PageUrlExtractor,
            |page_url, events, _state_access| {
                Box::pin(async move {
                    let view_count = events.len() as u32;
                    let unique_users = events.iter()
                        .map(|e| e.user_id.clone())
                        .collect::<std::collections::HashSet<_>>()
                        .len() as u32;
                    
                    Ok(PageView {
                        page_url,
                        view_count,
                        unique_users,
                        avg_time_on_page: 120.0, // Simulated average time
                    })
                })
            },
        )
        .collect()
        .await;
    let page_analytics: Vec<PageView> = page_analytics_results.into_iter().filter_map(Result::ok).collect();

    println!("   ✅ Analyzed {} pages", page_analytics.len());
    for page in page_analytics.iter().take(3) {
        println!("   📄 {}: {} views, {} unique users", 
                page.page_url, page.view_count, page.unique_users);
    }

    // 4. Event Pattern Detection
    println!("\n4️⃣ Event Pattern Detection");
    println!("--------------------------");
    
    let patterns_results: Vec<Result<Option<String>, StateError>> = stream_from_vec(events.clone())
        .stateful_pattern_rs2(
            state_config.clone(),
            UserIdExtractor,
            3, // Detect patterns of 3 events
            |pattern_events, _state_access| {
                Box::pin(async move {
                    let pattern: Vec<String> = pattern_events.iter()
                        .map(|e| e.event_type.clone())
                        .collect();
                    
                    let pattern_str = pattern.join(" -> ");
                    
                    // Detect interesting patterns
                    if pattern_str.contains("page_view -> click -> form_submit") {
                        Ok(Some(format!("Conversion funnel detected: {}", pattern_str)))
                    } else if pattern_str.contains("error") {
                        Ok(Some(format!("Error pattern detected: {}", pattern_str)))
                    } else {
                        Ok(None)
                    }
                })
            },
        )
        .collect()
        .await;
    let patterns: Vec<Option<String>> = patterns_results.into_iter().filter_map(Result::ok).collect();

    let detected_patterns: Vec<String> = patterns.into_iter()
        .filter_map(|p| p)
        .collect();
    
    println!("   ✅ Detected {} interesting patterns", detected_patterns.len());
    for pattern in detected_patterns.iter().take(3) {
        println!("   🔍 Pattern: {}", pattern);
    }

    // 5. Error Rate Monitoring with Throttling
    println!("\n5️⃣ Error Rate Monitoring");
    println!("-------------------------");
    
    let error_alerts_results: Vec<Result<UserEvent, StateError>> = stream_from_vec(events.clone())
        .stateful_filter_rs2(
            state_config.clone(),
            EventTypeExtractor,
            |event, _state_access| {
                let event = event.clone();
                Box::pin(async move {
                    Ok(event.event_type == "error")
                })
            },
        )
        .collect()
        .await;
    let filtered_errors: Vec<UserEvent> = error_alerts_results.into_iter().filter_map(Result::ok).collect();
    let error_alerts: Vec<Result<UserEvent, StateError>> = stream_from_vec(filtered_errors)
        .stateful_throttle_rs2(
            state_config.clone(),
            UserIdExtractor,
            5,
            Duration::from_secs(300),
            |event| event,
        )
        .collect()
        .await;

    println!("   ✅ Monitored {} error events with throttling", error_alerts.len());

    // 6. Real-Time Metrics Window
    println!("\n6️⃣ Real-Time Metrics Window");
    println!("----------------------------");
    
    let real_time_metrics_results: Vec<Result<RealTimeMetrics, StateError>> = stream_from_vec(events.clone())
        .stateful_window_rs2(
            state_config.clone(),
            UserIdExtractor,
            50, // Process in windows of 50 events
            |window_events, _state_access| {
                Box::pin(async move {
                    let timestamp = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    
                    let active_users = window_events.iter()
                        .map(|e| e.user_id.clone())
                        .collect::<std::collections::HashSet<_>>()
                        .len() as u32;
                    
                    let total_events = window_events.len() as u32;
                    let events_per_minute = total_events as f64 * 60.0 / 50.0; // Assuming 50 events per window
                    
                    // Calculate top pages
                    let mut page_counts: HashMap<String, u32> = HashMap::new();
                    for event in &window_events {
                        *page_counts.entry(event.page_url.clone()).or_insert(0) += 1;
                    }
                    let mut top_pages: Vec<(String, u32)> = page_counts.into_iter().collect();
                    top_pages.sort_by(|a, b| b.1.cmp(&a.1));
                    top_pages.truncate(5);
                    
                    let error_count = window_events.iter()
                        .filter(|e| e.event_type == "error")
                        .count() as f64;
                    let error_rate = error_count / total_events as f64;
                    
                    Ok(RealTimeMetrics {
                        timestamp,
                        active_users,
                        total_events,
                        events_per_minute,
                        top_pages,
                        error_rate,
                        avg_session_duration: 1800.0, // 30 minutes average
                    })
                })
            },
        )
        .collect()
        .await;
    let real_time_metrics: Vec<RealTimeMetrics> = real_time_metrics_results.into_iter().filter_map(Result::ok).collect();

    println!("   ✅ Generated {} real-time metric windows", real_time_metrics.len());
    
    // Show latest metrics
    if let Some(latest) = real_time_metrics.last() {
        println!("   📊 Latest Metrics:");
        println!("      Active Users: {}", latest.active_users);
        println!("      Events/min: {:.2}", latest.events_per_minute);
        println!("      Error Rate: {:.2}%", latest.error_rate * 100.0);
        println!("      Top Page: {}", latest.top_pages.first().map(|p| &p.0).unwrap_or(&"N/A".to_string()));
    }

    // 7. Deduplication of Events
    println!("\n7️⃣ Event Deduplication");
    println!("----------------------");
    
    let deduplicated_events_results: Vec<Result<UserEvent, StateError>> = stream_from_vec(events.clone())
        .stateful_deduplicate_rs2(
            state_config.clone(),
            UserIdExtractor,
            Duration::from_secs(60), // 1 minute TTL for deduplication
            |event| event,
        )
        .collect()
        .await;
    let deduplicated_events: Vec<UserEvent> = deduplicated_events_results.into_iter().filter_map(Result::ok).collect();

    println!("   ✅ Deduplicated {} events to {} events", 
            events.len(), deduplicated_events.len());

    // 8. Complex Analytics Pipeline
    println!("\n8️⃣ Complex Analytics Pipeline");
    println!("-----------------------------");
    
    let analytics_pipeline_filter_results: Vec<Result<UserEvent, StateError>> = stream_from_vec(events.clone())
        .stateful_filter_rs2(
            state_config.clone(),
            EventTypeExtractor,
            |event, _state_access| {
                let event = event.clone();
                Box::pin(async move {
                    Ok(event.event_type == "error" || event.event_type == "form_submit")
                })
            },
        )
        .collect()
        .await;
    let filtered_analytics: Vec<UserEvent> = analytics_pipeline_filter_results.into_iter().filter_map(Result::ok).collect();
    let analytics_pipeline_group_results: Vec<Result<Vec<Alert>, StateError>> = stream_from_vec(filtered_analytics)
        .stateful_group_by_advanced_rs2(
            state_config.clone(),
            UserIdExtractor,
            Some(10),
            Some(Duration::from_secs(300)),
            true,
            |user_id, user_events, _state_access| {
                Box::pin(async move {
                    let error_count = user_events.iter()
                        .filter(|e| e.event_type == "error")
                        .count();
                    let form_submit_count = user_events.iter()
                        .filter(|e| e.event_type == "form_submit")
                        .count();
                    let mut alerts = Vec::new();
                    if error_count >= 3 {
                        alerts.push(Alert {
                            alert_type: "High Error Rate".to_string(),
                            message: format!("User {} has {} errors", user_id, error_count),
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                            severity: "High".to_string(),
                            user_id: Some(user_id.clone()),
                        });
                    }
                    if form_submit_count >= 5 {
                        alerts.push(Alert {
                            alert_type: "High Engagement".to_string(),
                            message: format!("User {} submitted {} forms", user_id, form_submit_count),
                            timestamp: SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs(),
                            severity: "Info".to_string(),
                            user_id: Some(user_id),
                        });
                    }
                    Ok(alerts)
                })
            },
        )
        .collect()
        .await;
    let analytics_pipeline: Vec<Alert> = analytics_pipeline_group_results
        .into_iter()
        .filter_map(Result::ok)
        .flatten()
        .collect();

    println!("   ✅ Generated {} alerts from analytics pipeline", analytics_pipeline.len());
    
    for alert in analytics_pipeline.iter().take(3) {
        println!("   🚨 {}: {} ({})", alert.alert_type, alert.message, alert.severity);
    }

    // 9. Performance Summary
    println!("\n📊 Analytics Pipeline Summary");
    println!("============================");
    println!("Total Events Processed:     {}", events.len());
    println!("Unique Users:               {}", user_metrics.len());
    println!("Pages Analyzed:             {}", page_analytics.len());
    println!("Patterns Detected:          {}", detected_patterns.len());
    println!("Error Events Monitored:     {}", error_alerts.len());
    println!("Real-time Metric Windows:   {}", real_time_metrics.len());
    println!("Deduplicated Events:        {}", deduplicated_events.len());
    println!("Alerts Generated:           {}", analytics_pipeline.len());

    println!("\n🎯 Key Features Demonstrated:");
    println!("• Session management with automatic timeout detection");
    println!("• User metrics aggregation with persistent state");
    println!("• Real-time page analytics with grouping");
    println!("• Event pattern detection for insights");
    println!("• Error rate monitoring with throttling");
    println!("• Real-time metrics with sliding windows");
    println!("• Event deduplication to prevent duplicates");
    println!("• Complex analytics pipeline with multiple stateful operations");

    println!("\n✅ Real-time analytics pipeline example completed!");
    Ok(())
} 