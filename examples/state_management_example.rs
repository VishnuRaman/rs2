use futures::StreamExt;
use rs2_stream::state::{CustomKeyExtractor, StateConfig, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_stream;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    event_type: String,
    timestamp: u64,
    data: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserState {
    total_events: u64,
    last_event_type: String,
    event_counts: HashMap<String, u64>,
    last_seen: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AnalyticsState {
    total_revenue: f64,
    total_orders: u64,
    average_order_value: f64,
    last_order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FraudState {
    suspicious_transactions: u64,
    total_amount: f64,
    risk_score: f64,
    flagged_users: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WindowState {
    window_count: u64,
    total_items: u64,
    last_window_size: usize,
}

#[tokio::main]
async fn main() {
    println!("=== RS2 State Management Example ===\n");

    // Create sample event stream
    let events = tokio_stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "100.0".to_string());
                data.insert("product".to_string(), "book".to_string());
                data
            },
        },
        UserEvent {
            user_id: "user2".to_string(),
            event_type: "login".to_string(),
            timestamp: 3000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 4000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "50.0".to_string());
                data.insert("product".to_string(), "magazine".to_string());
                data
            },
        },
    ]);

    // Example 1: Session Management
    println!("1. Session Management - Tracking user sessions");
    let config = StateConfig::default();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let session_stream =
        events.stateful_map_rs2(config.clone(), key_extractor, |event, state_access| {
            let event = event.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let mut state = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap_or(UserState {
                        total_events: 0,
                        last_event_type: String::new(),
                        event_counts: HashMap::new(),
                        last_seen: 0,
                    })
                } else {
                    UserState {
                        total_events: 0,
                        last_event_type: String::new(),
                        event_counts: HashMap::new(),
                        last_seen: 0,
                    }
                };

                state.total_events += 1;
                state.last_event_type = event.event_type.clone();
                state.last_seen = event.timestamp;
                *state
                    .event_counts
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!(
                    "User {}: {} (total events: {}, last seen: {})",
                    event.user_id, event.event_type, state.total_events, state.last_seen
                ))
            })
        });

    let session_results: Vec<_> = session_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in session_results {
        println!("  {}", result);
    }

    // Example 2: Analytics Aggregation
    println!("\n2. Analytics Aggregation - Running totals and averages");
    let events2 = tokio_stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "100.0".to_string());
                data.insert("product".to_string(), "book".to_string());
                data
            },
        },
        UserEvent {
            user_id: "user2".to_string(),
            event_type: "login".to_string(),
            timestamp: 3000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 4000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "50.0".to_string());
                data.insert("product".to_string(), "magazine".to_string());
                data
            },
        },
    ]);

    let analytics_stream = events2.stateful_fold_rs2(
        config.clone(),
        CustomKeyExtractor::new(|event: &UserEvent| "global_analytics".to_string()),
        AnalyticsState {
            total_revenue: 0.0,
            total_orders: 0,
            average_order_value: 0.0,
            last_order_id: String::new(),
        },
        |acc, event, state_access| {
            let event = event.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let mut state = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap_or(AnalyticsState {
                        total_revenue: 0.0,
                        total_orders: 0,
                        average_order_value: 0.0,
                        last_order_id: String::new(),
                    })
                } else {
                    AnalyticsState {
                        total_revenue: 0.0,
                        total_orders: 0,
                        average_order_value: 0.0,
                        last_order_id: String::new(),
                    }
                };

                if event.event_type == "purchase" {
                    if let Some(amount_str) = event.data.get("amount") {
                        if let Ok(amount) = amount_str.parse::<f64>() {
                            state.total_revenue += amount;
                            state.total_orders += 1;
                            state.average_order_value =
                                state.total_revenue / state.total_orders as f64;
                            state.last_order_id = format!("order_{}", state.total_orders);
                        }
                    }
                }

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(state)
            })
        },
    );

    let analytics_results: Vec<_> = analytics_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in analytics_results {
        println!(
            "  Revenue: ${:.2}, Orders: {}, Avg: ${:.2}",
            result.total_revenue, result.total_orders, result.average_order_value
        );
    }

    // Example 3: Fraud Detection
    println!("\n3. Fraud Detection - Suspicious activity monitoring");
    let events3 = tokio_stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "100.0".to_string());
                data.insert("product".to_string(), "book".to_string());
                data
            },
        },
        UserEvent {
            user_id: "user2".to_string(),
            event_type: "login".to_string(),
            timestamp: 3000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 4000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "50.0".to_string());
                data.insert("product".to_string(), "magazine".to_string());
                data
            },
        },
    ]);

    let fraud_stream = events3.stateful_filter_rs2(
        config.clone(),
        CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
        |event, state_access| {
            let event = event.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let mut state = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap_or(FraudState {
                        suspicious_transactions: 0,
                        total_amount: 0.0,
                        risk_score: 0.0,
                        flagged_users: Vec::new(),
                    })
                } else {
                    FraudState {
                        suspicious_transactions: 0,
                        total_amount: 0.0,
                        risk_score: 0.0,
                        flagged_users: Vec::new(),
                    }
                };

                if event.event_type == "purchase" {
                    if let Some(amount_str) = event.data.get("amount") {
                        if let Ok(amount) = amount_str.parse::<f64>() {
                            state.total_amount += amount;

                            if amount > 800.0 || state.total_amount > 1000.0 {
                                state.suspicious_transactions += 1;
                                if !state.flagged_users.contains(&event.user_id) {
                                    state.flagged_users.push(event.user_id.clone());
                                }
                            }

                            state.risk_score = (state.suspicious_transactions as f64)
                                / (state.total_amount / 100.0);
                        }
                    }
                }

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(state.suspicious_transactions > 0)
            })
        },
    );

    let fraud_results: Vec<_> = fraud_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for event in fraud_results {
        println!(
            "  FRAUD ALERT: User {} made suspicious purchase",
            event.user_id
        );
    }

    // Example 4: Window Processing
    println!("\n4. Window Processing - Sliding window analytics");
    let events4 = tokio_stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "100.0".to_string());
                data.insert("product".to_string(), "book".to_string());
                data
            },
        },
        UserEvent {
            user_id: "user2".to_string(),
            event_type: "login".to_string(),
            timestamp: 3000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 4000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "50.0".to_string());
                data.insert("product".to_string(), "magazine".to_string());
                data
            },
        },
    ]);

    let window_stream = events4.stateful_window_rs2(
        config.clone(),
        CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
        2, // window size
        |window, state_access| {
            let window = window.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let mut state = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap_or(WindowState {
                        window_count: 0,
                        total_items: 0,
                        last_window_size: 0,
                    })
                } else {
                    WindowState {
                        window_count: 0,
                        total_items: 0,
                        last_window_size: 0,
                    }
                };

                state.window_count += 1;
                state.total_items += window.len() as u64;
                state.last_window_size = window.len();

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!(
                    "Window {}: {} events (total processed: {})",
                    state.window_count,
                    window.len(),
                    state.total_items
                ))
            })
        },
    );

    let window_results: Vec<_> = window_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in window_results {
        println!("  {}", result);
    }

    // Example 5: Distributed State Management
    println!("\n5. Distributed State Management - Multi-key processing");
    let events5 = tokio_stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "100.0".to_string());
                data.insert("product".to_string(), "book".to_string());
                data
            },
        },
        UserEvent {
            user_id: "user2".to_string(),
            event_type: "login".to_string(),
            timestamp: 3000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 4000,
            data: {
                let mut data = HashMap::new();
                data.insert("amount".to_string(), "50.0".to_string());
                data.insert("product".to_string(), "magazine".to_string());
                data
            },
        },
    ]);

    let distributed_stream = events5.stateful_map_rs2(
        config.clone(),
        CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
        |event, state_access| {
            let event = event.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let mut state = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap_or(UserState {
                        total_events: 0,
                        last_event_type: String::new(),
                        event_counts: HashMap::new(),
                        last_seen: 0,
                    })
                } else {
                    UserState {
                        total_events: 0,
                        last_event_type: String::new(),
                        event_counts: HashMap::new(),
                        last_seen: 0,
                    }
                };

                state.total_events += 1;
                state.last_event_type = event.event_type.clone();
                state.last_seen = event.timestamp;
                *state
                    .event_counts
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;

                let state_bytes = serde_json::to_vec(&state).unwrap();
                state_access.set(&state_bytes).await.unwrap();

                Ok(format!(
                    "{}: {} (key: {}, events: {})",
                    event.user_id, event.event_type, event.user_id, state.total_events
                ))
            })
        },
    );

    let distributed_results: Vec<_> = distributed_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in distributed_results {
        println!("  {}", result);
    }

    println!("\n=== State Management Example Complete ===");
}
