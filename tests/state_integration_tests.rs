use futures::StreamExt;
use rs2_stream::state::config::StateConfigs;
use rs2_stream::state::stream_ext::StateAccess;
use rs2_stream::state::{CustomKeyExtractor, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UserEvent {
    user_id: String,
    event_type: String,
    timestamp: u64,
    data: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct UserState {
    total_events: u64,
    last_event_type: String,
    event_counts: HashMap<String, u64>,
    last_seen: u64,
}

impl Default for UserState {
    fn default() -> Self {
        UserState {
            total_events: 0,
            last_event_type: String::new(),
            event_counts: HashMap::new(),
            last_seen: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct OrderEvent {
    order_id: String,
    customer_id: String,
    amount: f64,
    status: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct CustomerState {
    total_orders: u64,
    total_spent: f64,
    last_order_id: String,
    order_history: Vec<String>,
    last_seen: u64,
}

impl Default for CustomerState {
    fn default() -> Self {
        CustomerState {
            total_orders: 0,
            total_spent: 0.0,
            last_order_id: String::new(),
            order_history: Vec::new(),
            last_seen: 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FraudEvent {
    transaction_id: String,
    user_id: String,
    amount: f64,
    location: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct FraudState {
    suspicious_transactions: u64,
    total_amount: f64,
    locations: Vec<String>,
    risk_score: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SessionEvent {
    session_id: String,
    user_id: String,
    action: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct SessionState {
    total_actions: u64,
    last_action: String,
    session_duration: u64,
    is_active: bool,
}

#[tokio::test]
async fn test_user_activity_tracking() {
    let config = StateConfigs::high_performance();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let events = vec![
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
                let mut map = HashMap::new();
                map.insert("amount".to_string(), "100.0".to_string());
                map
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
            event_type: "logout".to_string(),
            timestamp: 4000,
            data: HashMap::new(),
        },
    ];

    let stream = futures::stream::iter(events);
    let result_stream =
        stream.stateful_map_rs2(config, key_extractor, |event, state_access: StateAccess| {
            Box::pin(async move {
                let state: UserState = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap()
                } else {
                    UserState::default()
                };

                let mut new_state = state;
                // Update state
                new_state.total_events += 1;
                new_state.last_event_type = event.event_type.clone();
                new_state.last_seen = event.timestamp;

                *new_state
                    .event_counts
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;

                // Persist state
                let bytes = serde_json::to_vec(&new_state).unwrap();
                state_access.set(&bytes).await.unwrap();

                // Return enriched event
                Ok(format!(
                    "User {}: {} (total events: {}, event count: {})",
                    event.user_id,
                    event.event_type,
                    new_state.total_events,
                    new_state.event_counts[&event.event_type]
                ))
            })
        });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 4);
    assert!(results[0].contains("User user1: login (total events: 1, event count: 1)"));
    assert!(results[1].contains("User user1: purchase (total events: 2, event count: 1)"));
    assert!(results[2].contains("User user2: login (total events: 1, event count: 1)"));
    assert!(results[3].contains("User user1: logout (total events: 3, event count: 1)"));
}

#[tokio::test]
async fn test_customer_order_analytics() {
    let config = StateConfigs::high_performance();
    let key_extractor = CustomKeyExtractor::new(|order: &OrderEvent| order.customer_id.clone());

    let orders = vec![
        OrderEvent {
            order_id: "order1".to_string(),
            customer_id: "customer1".to_string(),
            amount: 100.0,
            status: "completed".to_string(),
            timestamp: 1000,
        },
        OrderEvent {
            order_id: "order2".to_string(),
            customer_id: "customer1".to_string(),
            amount: 250.0,
            status: "completed".to_string(),
            timestamp: 2000,
        },
        OrderEvent {
            order_id: "order3".to_string(),
            customer_id: "customer2".to_string(),
            amount: 75.0,
            status: "pending".to_string(),
            timestamp: 3000,
        },
    ];

    let stream = futures::stream::iter(orders);
    let result_stream = stream.stateful_fold_rs2(
        config,
        key_extractor,
        0.0f64,
        |acc, order, state_access: StateAccess| Box::pin(async move { Ok(acc + order.amount) }),
    );

    let results: Vec<f64> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 3);
    assert_eq!(results[0], 100.0); // First order for customer1
    assert_eq!(results[1], 350.0); // Second order for customer1 (100 + 250)
    assert_eq!(results[2], 75.0); // First order for customer2
}

#[tokio::test]
async fn test_real_time_fraud_detection() {
    let config = StateConfigs::high_performance();
    let key_extractor = CustomKeyExtractor::new(|order: &OrderEvent| order.customer_id.clone());

    let orders = vec![
        OrderEvent {
            order_id: "order1".to_string(),
            customer_id: "customer1".to_string(),
            amount: 100.0,
            status: "completed".to_string(),
            timestamp: 1000,
        },
        OrderEvent {
            order_id: "order2".to_string(),
            customer_id: "customer1".to_string(),
            amount: 500.0,
            status: "completed".to_string(),
            timestamp: 2000,
        },
        OrderEvent {
            order_id: "order3".to_string(),
            customer_id: "customer1".to_string(),
            amount: 1000.0,
            status: "completed".to_string(),
            timestamp: 3000,
        },
        OrderEvent {
            order_id: "order4".to_string(),
            customer_id: "customer2".to_string(),
            amount: 50.0,
            status: "completed".to_string(),
            timestamp: 4000,
        },
    ];

    let stream = futures::stream::iter(orders);
    let result_stream =
        stream.stateful_filter_rs2(config, key_extractor, |order, state_access: StateAccess| {
            let order = order.clone();
            let state_access = state_access.clone();
            Box::pin(async move {
                let state: CustomerState = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap()
                } else {
                    CustomerState::default()
                };

                let mut new_state = state;
                new_state.total_orders += 1;
                new_state.total_spent += order.amount;
                new_state.last_order_id = order.order_id.clone();
                new_state.order_history.push(order.order_id.clone());
                new_state.last_seen = order.timestamp;

                // Persist state
                let bytes = serde_json::to_vec(&new_state).unwrap();
                state_access.set(&bytes).await.unwrap();

                // Flag suspicious activity: orders > 800 or total spent > 1000
                Ok(order.amount > 800.0 || new_state.total_spent > 1000.0)
            })
        });

    let results: Vec<OrderEvent> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].order_id, "order3"); // Amount > 800
}

#[tokio::test]
async fn test_session_management() {
    let config = StateConfigs::session();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let events = vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "page_view".to_string(),
            timestamp: 2000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "logout".to_string(),
            timestamp: 3000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user2".to_string(),
            event_type: "login".to_string(),
            timestamp: 4000,
            data: HashMap::new(),
        },
    ];

    let stream = futures::stream::iter(events);
    let result_stream =
        stream.stateful_map_rs2(config, key_extractor, |event, state_access: StateAccess| {
            Box::pin(async move {
                let state: UserState = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap()
                } else {
                    UserState::default()
                };

                let mut new_state = state;
                new_state.total_events += 1;
                new_state.last_event_type = event.event_type.clone();
                new_state.last_seen = event.timestamp;

                *new_state
                    .event_counts
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;

                // Persist state
                let bytes = serde_json::to_vec(&new_state).unwrap();
                state_access.set(&bytes).await.unwrap();

                // Return session info
                Ok(format!(
                    "Session for {}: {} events, last activity: {}",
                    event.user_id, new_state.total_events, new_state.last_seen
                ))
            })
        });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 4);
    assert!(results[0].contains("Session for user1: 1 events"));
    assert!(results[1].contains("Session for user1: 2 events"));
    assert!(results[2].contains("Session for user1: 3 events"));
    assert!(results[3].contains("Session for user2: 1 events"));
}

#[tokio::test]
async fn test_multi_stream_join() {
    let config = StateConfigs::high_performance();
    let user_key_extractor = CustomKeyExtractor::new(|event: &UserEvent| {
        let key = event.user_id.clone();
        println!("USER KEY: {}", key);
        key
    });
    let order_key_extractor = CustomKeyExtractor::new(|order: &OrderEvent| {
        let key = order.customer_id.clone();
        println!("ORDER KEY: {}", key);
        key
    });

    // Interleave user and order events
    let user_events = vec![
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
            data: HashMap::new(),
        },
    ];
    let order_events = vec![
        OrderEvent {
            order_id: "order1".to_string(),
            customer_id: "user1".to_string(),
            amount: 100.0,
            status: "completed".to_string(),
            timestamp: 1500,
        },
        OrderEvent {
            order_id: "order2".to_string(),
            customer_id: "user2".to_string(),
            amount: 200.0,
            status: "completed".to_string(),
            timestamp: 2500,
        },
    ];

    // Interleave the events
    let mut interleaved = Vec::new();
    let max_len = user_events.len().max(order_events.len());
    for i in 0..max_len {
        if i < user_events.len() {
            interleaved.push((Some(user_events[i].clone()), None));
        }
        if i < order_events.len() {
            interleaved.push((None, Some(order_events[i].clone())));
        }
    }

    // Split into two streams: one for users, one for orders, but yield alternately
    let (user_tx, user_rx) = tokio::sync::mpsc::unbounded_channel();
    let (order_tx, order_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        for (user, order) in interleaved {
            if let Some(u) = user {
                user_tx.send(u).unwrap();
            }
            if let Some(o) = order {
                order_tx.send(o).unwrap();
            }
            // Small yield to allow polling
            tokio::task::yield_now().await;
        }
    });
    let user_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(user_rx);
    let order_stream = tokio_stream::wrappers::UnboundedReceiverStream::new(order_rx);

    let result_stream = user_stream.stateful_join_rs2(
        Box::pin(order_stream),
        config,
        user_key_extractor,
        order_key_extractor,
        std::time::Duration::from_secs(10),
        |user_event: UserEvent, order_event: OrderEvent, state_access: StateAccess| {
            println!(
                "JOINING: user_event={:?}, order_event={:?}",
                user_event, order_event
            );
            Box::pin(async move {
                let state: UserState = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap()
                } else {
                    UserState::default()
                };
                let mut new_state = state;
                new_state.total_events += 1;
                new_state.last_event_type = user_event.event_type.clone();
                new_state.last_seen = user_event.timestamp;
                *new_state
                    .event_counts
                    .entry(user_event.event_type.clone())
                    .or_insert(0) += 1;
                let bytes = serde_json::to_vec(&new_state).unwrap();
                state_access.set(&bytes).await.unwrap();
                Ok(format!(
                    "User {} {} and placed order {} for ${:.2}",
                    user_event.user_id,
                    user_event.event_type,
                    order_event.order_id,
                    order_event.amount
                ))
            })
        },
    );

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("MULTI STREAM JOIN RESULTS: {:?}", results);

    // The join should produce at least one result for the matching key (user1)
    // Since we have 2 user events and 1 matching order event, we should get at least 1 result
    assert!(
        results.len() >= 1,
        "Expected at least 1 join result, got {}",
        results.len()
    );

    // Check that we have the expected join results
    let mut found_login_order = false;
    let mut found_purchase_order = false;

    for result in &results {
        if result.contains("User user1 login") && result.contains("order1") {
            found_login_order = true;
        }
        if result.contains("User user1 purchase") && result.contains("order1") {
            found_purchase_order = true;
        }
    }

    // We should have at least one of the expected joins
    assert!(
        found_login_order || found_purchase_order,
        "Expected at least one of the join results, got: {:?}",
        results
    );

    // If we got both results, that's great
    if results.len() == 2 {
        assert!(
            found_login_order,
            "Expected 'User user1 login' join in results: {:?}",
            results
        );
        assert!(
            found_purchase_order,
            "Expected 'User user1 purchase' join in results: {:?}",
            results
        );
    }
}

#[tokio::test]
async fn test_error_recovery_and_continuity() {
    let config = StateConfigs::high_performance();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let events = vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "login".to_string(),
            timestamp: 1000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "error".to_string(),
            timestamp: 2000,
            data: HashMap::new(),
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "recovery".to_string(),
            timestamp: 3000,
            data: HashMap::new(),
        },
    ];

    let stream = futures::stream::iter(events);
    let result_stream =
        stream.stateful_map_rs2(config, key_extractor, |event, state_access: StateAccess| {
            Box::pin(async move {
                let state: UserState = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap()
                } else {
                    UserState::default()
                };

                let mut new_state = state;
                new_state.total_events += 1;
                new_state.last_event_type = event.event_type.clone();
                new_state.last_seen = event.timestamp;

                *new_state
                    .event_counts
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;

                // Persist state
                let bytes = serde_json::to_vec(&new_state).unwrap();
                state_access.set(&bytes).await.unwrap();

                // Return event with state info
                Ok(format!(
                    "Event {} for user {} (total: {})",
                    event.event_type, event.user_id, new_state.total_events
                ))
            })
        });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 3);
    assert!(results[0].contains("Event login for user user1 (total: 1)"));
    assert!(results[1].contains("Event error for user user1 (total: 2)"));
    assert!(results[2].contains("Event recovery for user user1 (total: 3)"));
}

#[tokio::test]
async fn test_performance_under_load() {
    let config = StateConfigs::high_performance();
    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    // Create many events for different users
    let mut events = Vec::new();
    for i in 0..100 {
        events.push(UserEvent {
            user_id: format!("user_{}", i % 10), // 10 different users
            event_type: "test".to_string(),
            timestamp: i as u64,
            data: HashMap::new(),
        });
    }

    let stream = futures::stream::iter(events);
    let result_stream =
        stream.stateful_map_rs2(config, key_extractor, |event, state_access: StateAccess| {
            Box::pin(async move {
                let state: UserState = if let Some(bytes) = state_access.get().await {
                    serde_json::from_slice(&bytes).unwrap()
                } else {
                    UserState::default()
                };

                let mut new_state = state;
                new_state.total_events += 1;
                new_state.last_event_type = event.event_type.clone();
                new_state.last_seen = event.timestamp;

                *new_state
                    .event_counts
                    .entry(event.event_type.clone())
                    .or_insert(0) += 1;

                // Persist state
                let bytes = serde_json::to_vec(&new_state).unwrap();
                state_access.set(&bytes).await.unwrap();

                // Return processed event
                Ok(format!(
                    "Processed {} for user {} (count: {})",
                    event.event_type, event.user_id, new_state.total_events
                ))
            })
        });

    let results: Vec<String> = result_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    assert_eq!(results.len(), 100);

    // Verify that each user has the correct number of events
    let mut user_counts = HashMap::new();
    for result in results {
        if let Some(user_id) = result.split_whitespace().nth(4) {
            *user_counts.entry(user_id.to_string()).or_insert(0) += 1;
        }
    }

    // Each user should have 10 events (100 total events / 10 users)
    for (user_id, count) in user_counts {
        assert_eq!(count, 10, "User {} should have 10 events", user_id);
    }
}
