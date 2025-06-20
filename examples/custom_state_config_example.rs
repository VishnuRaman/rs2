use futures::StreamExt;
use rs2_stream::{
    state::config::{StateConfigBuilder, StateConfigs},
    state::{CustomKeyExtractor, StateConfig, StatefulStreamExt},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    event_type: String,
    timestamp: u64,
    amount: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserState {
    total_events: u64,
    total_amount: f64,
    last_event_type: String,
    last_seen: u64,
}

#[tokio::main]
async fn main() {
    println!("=== RS2 Custom State Configuration Example ===\n");

    // Create sample event stream
    let events = vec![
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 1000,
            amount: 100.0,
        },
        UserEvent {
            user_id: "user1".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 2000,
            amount: 50.0,
        },
        UserEvent {
            user_id: "user2".to_string(),
            event_type: "purchase".to_string(),
            timestamp: 3000,
            amount: 75.0,
        },
    ];

    // Example 1: Using Builder Pattern (Recommended)
    println!("1. Custom Configuration using Builder Pattern");
    let custom_config = StateConfigBuilder::new()
        .ttl(Duration::from_secs(2 * 60 * 60)) // 2 hours
        .cleanup_interval(Duration::from_secs(10 * 60)) // 10 minutes
        .max_size(5000)
        .build()
        .unwrap();

    println!("   TTL: 2 hours");
    println!("   Cleanup Interval: 10 minutes");
    println!("   Max Size: 5000 entries");

    let builder_results: Vec<String> = futures::stream::iter(events.clone())
        .stateful_map_rs2(
            custom_config,
            CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
            |event, state_access| {
                Box::pin(async move {
                    let mut state = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(UserState {
                            total_events: 0,
                            total_amount: 0.0,
                            last_event_type: String::new(),
                            last_seen: 0,
                        })
                    } else {
                        UserState {
                            total_events: 0,
                            total_amount: 0.0,
                            last_event_type: String::new(),
                            last_seen: 0,
                        }
                    };

                    state.total_events += 1;
                    state.total_amount += event.amount;
                    state.last_event_type = event.event_type.clone();
                    state.last_seen = event.timestamp;

                    let bytes = serde_json::to_vec(&state).unwrap();
                    state_access.set(&bytes).await.unwrap();

                    Ok(format!(
                        "User {}: {} (total: ${:.2}, events: {}, last seen: {})",
                        event.user_id,
                        event.event_type,
                        state.total_amount,
                        state.total_events,
                        state.last_seen
                    ))
                })
            },
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    for result in builder_results {
        println!("   {}", result);
    }

    // Example 2: Using Method Chaining
    println!("\n2. Custom Configuration using Method Chaining");
    let chained_config = StateConfig::new()
        .ttl(Duration::from_secs(45 * 60)) // 45 minutes
        .cleanup_interval(Duration::from_secs(2 * 60)) // 2 minutes
        .max_size(2000);

    println!("   TTL: 45 minutes");
    println!("   Cleanup Interval: 2 minutes");
    println!("   Max Size: 2000 entries");

    let chained_results: Vec<String> = futures::stream::iter(events.clone())
        .stateful_map_rs2(
            chained_config,
            CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
            |event, state_access| {
                Box::pin(async move {
                    let mut state = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(UserState {
                            total_events: 0,
                            total_amount: 0.0,
                            last_event_type: String::new(),
                            last_seen: 0,
                        })
                    } else {
                        UserState {
                            total_events: 0,
                            total_amount: 0.0,
                            last_event_type: String::new(),
                            last_seen: 0,
                        }
                    };

                    state.total_events += 1;
                    state.total_amount += event.amount;
                    state.last_event_type = event.event_type.clone();
                    state.last_seen = event.timestamp;

                    let bytes = serde_json::to_vec(&state).unwrap();
                    state_access.set(&bytes).await.unwrap();

                    Ok(format!(
                        "User {}: {} (total: ${:.2}, events: {}, last seen: {})",
                        event.user_id,
                        event.event_type,
                        state.total_amount,
                        state.total_events,
                        state.last_seen
                    ))
                })
            },
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    for result in chained_results {
        println!("   {}", result);
    }

    // Example 3: Starting from Predefined and Modifying
    println!("\n3. Starting from Predefined Configuration and Modifying");
    let modified_config = StateConfigs::session()
        .ttl(Duration::from_secs(45 * 60)) // Override 30min to 45min
        .max_size(2000); // Override 1000 to 2000

    println!("   Base: Session config (30min TTL, 5min cleanup, 1000 max)");
    println!("   Modified: 45min TTL, 2000 max size");

    let modified_results: Vec<String> = futures::stream::iter(events.clone())
        .stateful_map_rs2(
            modified_config,
            CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
            |event, state_access| {
                Box::pin(async move {
                    let mut state = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(UserState {
                            total_events: 0,
                            total_amount: 0.0,
                            last_event_type: String::new(),
                            last_seen: 0,
                        })
                    } else {
                        UserState {
                            total_events: 0,
                            total_amount: 0.0,
                            last_event_type: String::new(),
                            last_seen: 0,
                        }
                    };

                    state.total_events += 1;
                    state.total_amount += event.amount;
                    state.last_event_type = event.event_type.clone();
                    state.last_seen = event.timestamp;

                    let bytes = serde_json::to_vec(&state).unwrap();
                    state_access.set(&bytes).await.unwrap();

                    Ok(format!(
                        "User {}: {} (total: ${:.2}, events: {}, last seen: {})",
                        event.user_id,
                        event.event_type,
                        state.total_amount,
                        state.total_events,
                        state.last_seen
                    ))
                })
            },
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    for result in modified_results {
        println!("   {}", result);
    }

    // Example 4: Direct Construction
    println!("\n4. Direct Construction of StateConfig");
    let direct_config = StateConfig {
        storage_type: rs2_stream::state::traits::StateStorageType::InMemory,
        ttl: Duration::from_secs(3 * 60 * 60), // 3 hours
        cleanup_interval: Duration::from_secs(15 * 60), // 15 minutes
        max_size: Some(3000),
        custom_storage: None,
    };

    println!("   TTL: 3 hours");
    println!("   Cleanup Interval: 15 minutes");
    println!("   Max Size: 3000 entries");

    let direct_results: Vec<String> = futures::stream::iter(events)
        .stateful_map_rs2(
            direct_config,
            CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
            |event, state_access| {
                Box::pin(async move {
                    let mut state = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(UserState {
                            total_events: 0,
                            total_amount: 0.0,
                            last_event_type: String::new(),
                            last_seen: 0,
                        })
                    } else {
                        UserState {
                            total_events: 0,
                            total_amount: 0.0,
                            last_event_type: String::new(),
                            last_seen: 0,
                        }
                    };

                    state.total_events += 1;
                    state.total_amount += event.amount;
                    state.last_event_type = event.event_type.clone();
                    state.last_seen = event.timestamp;

                    let bytes = serde_json::to_vec(&state).unwrap();
                    state_access.set(&bytes).await.unwrap();

                    Ok(format!(
                        "User {}: {} (total: ${:.2}, events: {}, last seen: {})",
                        event.user_id,
                        event.event_type,
                        state.total_amount,
                        state.total_events,
                        state.last_seen
                    ))
                })
            },
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    for result in direct_results {
        println!("   {}", result);
    }

    // Example 5: Configuration Validation
    println!("\n5. Configuration Validation Examples");

    // Valid configuration
    let valid_config = StateConfigBuilder::new()
        .ttl(Duration::from_secs(60 * 60)) // 1 hour
        .cleanup_interval(Duration::from_secs(10 * 60)) // 10 minutes
        .max_size(1000)
        .build();

    match valid_config {
        Ok(config) => println!(
            "   ✓ Valid config: TTL={:?}, Cleanup={:?}, MaxSize={:?}",
            config.ttl, config.cleanup_interval, config.max_size
        ),
        Err(e) => println!("   ✗ Invalid config: {}", e),
    }

    // Invalid configuration (cleanup > TTL)
    let invalid_config = StateConfigBuilder::new()
        .ttl(Duration::from_secs(60 * 60)) // 1 hour
        .cleanup_interval(Duration::from_secs(2 * 60 * 60)) // 2 hours (invalid!)
        .max_size(1000)
        .build();

    match invalid_config {
        Ok(_) => println!("   ✓ Valid config (unexpected)"),
        Err(e) => println!("   ✗ Invalid config (expected): {}", e),
    }

    println!("\n=== Configuration Summary ===");
    println!("Available predefined configurations:");
    println!("  - StateConfigs::high_performance() - 1 hour TTL, 1 min cleanup, 10k max");
    println!("  - StateConfigs::session() - 30 min TTL, 5 min cleanup, 1k max");
    println!("  - StateConfigs::short_lived() - 5 min TTL, 30 sec cleanup, 100 max");
    println!("  - StateConfigs::long_lived() - 7 days TTL, 1 hour cleanup, 100k max");
    println!("\nCustom configuration methods:");
    println!("  - StateConfigBuilder::new().ttl().cleanup_interval().max_size().build()");
    println!("  - StateConfig::new().ttl().cleanup_interval().max_size()");
    println!("  - StateConfig {{ ttl, cleanup_interval, max_size, .. }}");
    println!("  - Start from predefined and modify: StateConfigs::session().ttl().max_size()");
}
