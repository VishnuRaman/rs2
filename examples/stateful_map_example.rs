use futures::StreamExt;
use rs2_stream::state::config::StateConfig;
use rs2_stream::state::{CustomKeyExtractor, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserActivity {
    user_id: String,
    action: String,
    timestamp: u64,
    duration: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserProfile {
    total_actions: u64,
    last_action: String,
    average_duration: f64,
    action_counts: HashMap<String, u64>,
    last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserState {
    visit_count: u64,
    last_visit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnrichedUser {
    id: String,
    name: String,
    visit_count: u64,
    last_visit: u64,
    is_returning: bool,
}

#[tokio::main]
async fn main() {
    println!("\n=== Stateful Map Example ===\n");

    // Create sample user activities
    let activities = vec![
        UserActivity {
            user_id: "alice".to_string(),
            action: "login".to_string(),
            timestamp: 1000,
            duration: 5,
        },
        UserActivity {
            user_id: "alice".to_string(),
            action: "browse".to_string(),
            timestamp: 1020,
            duration: 120,
        },
        UserActivity {
            user_id: "alice".to_string(),
            action: "purchase".to_string(),
            timestamp: 1150,
            duration: 30,
        },
        UserActivity {
            user_id: "bob".to_string(),
            action: "login".to_string(),
            timestamp: 1100,
            duration: 3,
        },
        UserActivity {
            user_id: "bob".to_string(),
            action: "search".to_string(),
            timestamp: 1110,
            duration: 45,
        },
        UserActivity {
            user_id: "charlie".to_string(),
            action: "login".to_string(),
            timestamp: 1200,
            duration: 8,
        },
    ];

    // Example 1: Basic stateful mapping - enrich activities with user profile
    println!("1. Basic Stateful Mapping - Enriching Activities with User Profile:");
    let stream = futures::stream::iter(activities.clone());
    let enriched_stream = stream.stateful_map_rs2(
        StateConfig::new(),
        CustomKeyExtractor::new(|activity: &UserActivity| activity.user_id.clone()),
        |activity, state_access| {
            Box::pin(async move {
                let fut = async move {
                    let mut state = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(UserState {
                            visit_count: 0,
                            last_visit: 0,
                        })
                    } else {
                        UserState {
                            visit_count: 0,
                            last_visit: 0,
                        }
                    };

                    state.visit_count += 1;
                    state.last_visit = activity.timestamp;

                    let state_bytes = serde_json::to_vec(&state).unwrap();
                    state_access.set(&state_bytes).await.unwrap();

                    EnrichedUser {
                        id: activity.user_id.clone(),
                        name: String::new(),
                        visit_count: state.visit_count,
                        last_visit: state.last_visit,
                        is_returning: state.visit_count > 1,
                    }
                };

                Ok(fut.await)
            })
        },
    );

    let enriched_results: Vec<EnrichedUser> = enriched_stream
        .collect::<Vec<Result<EnrichedUser, _>>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in enriched_results {
        println!("  {:?}", result);
    }

    // Example 2: Stateful mapping with session tracking
    println!("\n2. Stateful Mapping with Session Tracking:");
    let stream = futures::stream::iter(activities.clone());
    let session_stream = stream.stateful_map_rs2(
        StateConfig::new(),
        CustomKeyExtractor::new(|activity: &UserActivity| activity.user_id.clone()),
        |activity, state_access| {
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());

                let mut session_data: HashMap<String, u64> = if state_bytes.is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_slice(&state_bytes).unwrap_or(HashMap::new())
                };

                // Track session start time
                if !session_data.contains_key("session_start") {
                    session_data.insert("session_start".to_string(), activity.timestamp);
                }
                session_data.insert("last_activity".to_string(), activity.timestamp);

                // Calculate session duration
                let session_start = session_data
                    .get("session_start")
                    .unwrap_or(&activity.timestamp);
                let session_duration = activity.timestamp - session_start;

                // Save session data
                let state_bytes = serde_json::to_vec(&session_data).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!(
                    "User: {} | Action: {} | Session Duration: {}s | Timestamp: {}",
                    activity.user_id, activity.action, session_duration, activity.timestamp
                ))
            })
        },
    );

    let session_results: Vec<String> = session_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in session_results {
        println!("  {}", result);
    }

    // Example 3: Stateful mapping with rate limiting
    println!("\n3. Stateful Mapping with Rate Limiting:");
    let stream = futures::stream::iter(activities.clone());
    let rate_limited_stream = stream.stateful_map_rs2(
        StateConfig::new(),
        CustomKeyExtractor::new(|activity: &UserActivity| activity.user_id.clone()),
        |activity, state_access| {
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());

                let mut rate_data: HashMap<String, u64> = if state_bytes.is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_slice(&state_bytes).unwrap_or(HashMap::new())
                };

                // Get current rate info
                let current_count = rate_data.get("count").unwrap_or(&0);
                let window_start = rate_data.get("window_start").unwrap_or(&activity.timestamp);

                // Reset window if more than 60 seconds have passed
                let window_duration = 60;
                let new_count = if activity.timestamp - window_start > window_duration {
                    1
                } else {
                    current_count + 1
                };

                // Update rate data
                rate_data.insert("count".to_string(), new_count);
                if new_count == 1 {
                    rate_data.insert("window_start".to_string(), activity.timestamp);
                }

                // Save rate data
                let state_bytes = serde_json::to_vec(&rate_data).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                // Apply rate limiting (max 3 actions per minute)
                if new_count > 3 {
                    Ok(format!(
                        "User: {} | Action: {} | STATUS: RATE LIMITED ({} actions in window)",
                        activity.user_id, activity.action, new_count
                    ))
                } else {
                    Ok(format!(
                        "User: {} | Action: {} | Rate: {}/{} actions per minute",
                        activity.user_id, activity.action, new_count, window_duration
                    ))
                }
            })
        },
    );

    let rate_results: Vec<String> = rate_limited_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in rate_results {
        println!("  {}", result);
    }

    // Example 4: Stateful mapping with anomaly detection
    println!("\n4. Stateful Mapping with Anomaly Detection:");
    let stream = futures::stream::iter(activities.clone());
    let anomaly_stream = stream.stateful_map_rs2(
        StateConfig::new(),
        CustomKeyExtractor::new(|activity: &UserActivity| activity.user_id.clone()),
        |activity, state_access| {
            Box::pin(async move {
                let state_bytes = state_access.get().await.unwrap_or(Vec::new());

                let mut anomaly_data: HashMap<String, f64> = if state_bytes.is_empty() {
                    HashMap::new()
                } else {
                    serde_json::from_slice(&state_bytes).unwrap_or(HashMap::new())
                };

                // Calculate average duration for this user
                let current_avg = anomaly_data.get("avg_duration").unwrap_or(&0.0);
                let count = anomaly_data.get("count").unwrap_or(&0.0);

                let new_count = count + 1.0;
                let new_avg = (current_avg * count + activity.duration as f64) / new_count;

                // Update anomaly data
                anomaly_data.insert("avg_duration".to_string(), new_avg);
                anomaly_data.insert("count".to_string(), new_count);

                // Save anomaly data
                let state_bytes = serde_json::to_vec(&anomaly_data).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                // Detect anomalies (duration > 2x average)
                let threshold = new_avg * 2.0;
                let is_anomaly = activity.duration as f64 > threshold && new_count > 1.0;

                if is_anomaly {
                    Ok(format!(
                        "ANOMALY DETECTED: User: {} | Action: {} | Score: {:.2} | Avg: {:.2}",
                        activity.user_id, activity.action, activity.duration as f64, new_avg
                    ))
                } else {
                    Ok(format!(
                        "Normal: User: {} | Action: {} | Duration: {}s | Avg: {:.2}s",
                        activity.user_id, activity.action, activity.duration, new_avg
                    ))
                }
            })
        },
    );

    let anomaly_results: Vec<String> = anomaly_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in anomaly_results {
        println!("  {}", result);
    }

    println!("\n=== Stateful Map Example Complete ===");
}
