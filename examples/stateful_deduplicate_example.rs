use futures::StreamExt;
use rs2_stream::state::{CustomKeyExtractor, StateConfig, StatefulStreamExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct Event {
    id: String,
    user_id: String,
    action: String,
    timestamp: u64,
    data: String,
}

#[tokio::main]
async fn main() {
    println!("=== RS2 Stateful Deduplicate Example ===\n");

    // Create state configuration
    let config = StateConfig::new();

    // Create sample events with duplicates
    let events = vec![
        Event {
            id: "event1".to_string(),
            user_id: "user1".to_string(),
            action: "login".to_string(),
            timestamp: 1000,
            data: "login_data".to_string(),
        },
        Event {
            id: "event1".to_string(), // Duplicate ID
            user_id: "user1".to_string(),
            action: "login".to_string(),
            timestamp: 1100,
            data: "login_data".to_string(),
        },
        Event {
            id: "event2".to_string(),
            user_id: "user1".to_string(),
            action: "purchase".to_string(),
            timestamp: 1200,
            data: "purchase_data".to_string(),
        },
        Event {
            id: "event3".to_string(),
            user_id: "user2".to_string(),
            action: "login".to_string(),
            timestamp: 1300,
            data: "login_data".to_string(),
        },
        Event {
            id: "event2".to_string(), // Duplicate ID
            user_id: "user1".to_string(),
            action: "purchase".to_string(),
            timestamp: 1400,
            data: "purchase_data".to_string(),
        },
        Event {
            id: "event4".to_string(),
            user_id: "user2".to_string(),
            action: "logout".to_string(),
            timestamp: 1500,
            data: "logout_data".to_string(),
        },
    ];

    // Example 1: Deduplicate by event ID
    println!("1. Deduplicate by Event ID:");
    let dedup_by_id = futures::stream::iter(events.clone())
        .stateful_deduplicate_rs2(
            config.clone(),
            CustomKeyExtractor::new(|event: &Event| event.id.to_string()),
            std::time::Duration::from_secs(60), // 60 second TTL
            |event| event.clone(),
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    println!("  Unique events by ID:");
    for event in &dedup_by_id {
        println!(
            "    ID: {}, User: {}, Action: {}",
            event.id, event.user_id, event.action
        );
    }

    // Example 2: Deduplicate by user + action combination
    println!("\n2. Deduplicate by User + Action:");
    let dedup_by_user_action = futures::stream::iter(events.clone())
        .stateful_deduplicate_rs2(
            config.clone(),
            CustomKeyExtractor::new(|event: &Event| format!("{}_{}", event.user_id, event.action)),
            std::time::Duration::from_secs(30), // 30 second TTL
            |event| event.clone(),
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    println!("  Unique user-action combinations:");
    for event in &dedup_by_user_action {
        println!("    User: {}, Action: {}", event.user_id, event.action);
    }

    // Example 3: Deduplicate by data content
    println!("\n3. Deduplicate by Data Content:");
    let dedup_by_data = futures::stream::iter(events.clone())
        .stateful_deduplicate_rs2(
            config.clone(),
            CustomKeyExtractor::new(|event: &Event| {
                serde_json::to_string(&event.data).unwrap_or_default()
            }),
            std::time::Duration::from_secs(120), // 2 minute TTL
            |event| event.clone(),
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    println!("  Unique data content:");
    for event in &dedup_by_data {
        println!("    Data: {:?}", event.data);
    }

    // Example 4: Deduplicate by timestamp window
    println!("\n4. Deduplicate by Timestamp Window:");
    let dedup_by_timestamp = futures::stream::iter(events.clone())
        .stateful_deduplicate_rs2(
            config.clone(),
            CustomKeyExtractor::new(|event: &Event| (event.timestamp / 100).to_string()), // 100ms windows
            std::time::Duration::from_secs(10), // 10 second TTL
            |event| event.clone(),
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    println!("  Unique timestamp windows:");
    for event in &dedup_by_timestamp {
        println!(
            "    Timestamp: {}, Window: {}",
            event.timestamp,
            event.timestamp / 100
        );
    }

    println!("\n=== Stateful Deduplicate Example Complete ===");
}
