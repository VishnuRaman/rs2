use futures::StreamExt;
use rs2_stream::state::{CustomKeyExtractor, StatefulStreamExt};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    service: String,
    level: String,
    user_id: String,
    message: String,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ServiceStats {
    total_logs: u64,
    error_count: u64,
    last_message: String,
}

#[tokio::main]
async fn main() {
    println!("=== RS2 Stateful Group By Example ===\n");

    // Create sample log entries
    let logs = vec![
        LogEntry {
            service: "api".to_string(),
            level: "info".to_string(),
            user_id: "user1".to_string(),
            message: "Request received".to_string(),
            timestamp: 1000,
        },
        LogEntry {
            service: "api".to_string(),
            level: "error".to_string(),
            user_id: "user1".to_string(),
            message: "Database connection failed".to_string(),
            timestamp: 1100,
        },
        LogEntry {
            service: "api".to_string(),
            level: "info".to_string(),
            user_id: "user2".to_string(),
            message: "Request processed".to_string(),
            timestamp: 1200,
        },
        LogEntry {
            service: "auth".to_string(),
            level: "warn".to_string(),
            user_id: "user1".to_string(),
            message: "Failed login attempt".to_string(),
            timestamp: 1300,
        },
        LogEntry {
            service: "auth".to_string(),
            level: "info".to_string(),
            user_id: "user2".to_string(),
            message: "User authenticated".to_string(),
            timestamp: 1400,
        },
        LogEntry {
            service: "api".to_string(),
            level: "error".to_string(),
            user_id: "user3".to_string(),
            message: "Rate limit exceeded".to_string(),
            timestamp: 1500,
        },
        LogEntry {
            service: "api".to_string(),
            level: "info".to_string(),
            user_id: "user1".to_string(),
            message: "Cache hit".to_string(),
            timestamp: 1600,
        },
        LogEntry {
            service: "auth".to_string(),
            level: "info".to_string(),
            user_id: "user3".to_string(),
            message: "Password reset".to_string(),
            timestamp: 1700,
        },
    ];

    // Example 1: Group by service with size-based emission (emit when group reaches 3 items)
    println!("1. Group by Service with Size-Based Emission (max 3 items per group):");
    let service_groups = futures::stream::iter(logs.clone())
        .stateful_group_by_advanced_rs2(
            rs2_stream::state::StateConfig::new(),
            CustomKeyExtractor::new(|log: &LogEntry| log.service.clone()),
            Some(3), // max_group_size: emit when group reaches 3 items
            None,    // group_timeout: no timeout
            false,   // emit_on_key_change: don't emit on key change
            |service, group_logs, state_access| {
                let fut = async move {
                    let mut stats = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(ServiceStats {
                            total_logs: 0,
                            error_count: 0,
                            last_message: String::new(),
                        })
                    } else {
                        ServiceStats {
                            total_logs: 0,
                            error_count: 0,
                            last_message: String::new(),
                        }
                    };

                    stats.total_logs += group_logs.len() as u64;
                    stats.error_count +=
                        group_logs.iter().filter(|log| log.level == "error").count() as u64;
                    stats.last_message = group_logs.last().unwrap().message.clone();

                    let stats_bytes = serde_json::to_vec(&stats).unwrap();
                    state_access.set(&stats_bytes).await.unwrap();

                    Ok(format!(
                        "Service: {}, Batch: {} items, Total: {}, Errors: {}, Last: {}",
                        service,
                        group_logs.len(),
                        stats.total_logs,
                        stats.error_count,
                        stats.last_message
                    ))
                };
                Box::pin(fut)
            },
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    println!("  Service groups (size-based):");
    for group in &service_groups {
        println!("    {}", group);
    }

    // Example 2: Group by service with key-change emission (emit when service changes)
    println!("\n2. Group by Service with Key-Change Emission:");
    let service_groups_key_change = futures::stream::iter(logs.clone())
        .stateful_group_by_advanced_rs2(
            rs2_stream::state::StateConfig::new(),
            CustomKeyExtractor::new(|log: &LogEntry| log.service.clone()),
            None, // max_group_size: no size limit
            None, // group_timeout: no timeout
            true, // emit_on_key_change: emit when key changes
            |service, group_logs, state_access| {
                let fut = async move {
                    let mut stats = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(ServiceStats {
                            total_logs: 0,
                            error_count: 0,
                            last_message: String::new(),
                        })
                    } else {
                        ServiceStats {
                            total_logs: 0,
                            error_count: 0,
                            last_message: String::new(),
                        }
                    };

                    stats.total_logs += group_logs.len() as u64;
                    stats.error_count +=
                        group_logs.iter().filter(|log| log.level == "error").count() as u64;
                    stats.last_message = group_logs.last().unwrap().message.clone();

                    let stats_bytes = serde_json::to_vec(&stats).unwrap();
                    state_access.set(&stats_bytes).await.unwrap();

                    Ok(format!(
                        "Service: {}, Session: {} items, Total: {}, Errors: {}, Last: {}",
                        service,
                        group_logs.len(),
                        stats.total_logs,
                        stats.error_count,
                        stats.last_message
                    ))
                };
                Box::pin(fut)
            },
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    println!("  Service groups (key-change):");
    for group in &service_groups_key_change {
        println!("    {}", group);
    }

    // Example 3: Group by user ID with time-based emission (simulate time-based grouping)
    println!("\n3. Group by User ID (default behavior - emit at stream end):");
    let user_groups = futures::stream::iter(logs.clone())
        .stateful_group_by_rs2(
            rs2_stream::state::StateConfig::new(),
            CustomKeyExtractor::new(|log: &LogEntry| log.user_id.clone()),
            |user_id, group_logs, state_access| {
                let fut = async move {
                    let mut stats = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(ServiceStats {
                            total_logs: 0,
                            error_count: 0,
                            last_message: String::new(),
                        })
                    } else {
                        ServiceStats {
                            total_logs: 0,
                            error_count: 0,
                            last_message: String::new(),
                        }
                    };

                    stats.total_logs += group_logs.len() as u64;
                    stats.error_count +=
                        group_logs.iter().filter(|log| log.level == "error").count() as u64;
                    stats.last_message = group_logs.last().unwrap().message.clone();

                    let stats_bytes = serde_json::to_vec(&stats).unwrap();
                    state_access.set(&stats_bytes).await.unwrap();

                    Ok(format!(
                        "User: {}, Total: {}, Errors: {}, Last: {}",
                        user_id, stats.total_logs, stats.error_count, stats.last_message
                    ))
                };
                Box::pin(fut)
            },
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    println!("  User groups:");
    for group in &user_groups {
        println!("    {}", group);
    }

    // Example 4: Group by log level with size-based emission
    println!("\n4. Group by Log Level with Size-Based Emission (max 2 items per group):");
    let level_groups = futures::stream::iter(logs.clone())
        .stateful_group_by_advanced_rs2(
            rs2_stream::state::StateConfig::new(),
            CustomKeyExtractor::new(|log: &LogEntry| log.level.clone()),
            Some(2), // max_group_size: emit when group reaches 2 items
            None,    // group_timeout: no timeout
            false,   // emit_on_key_change: don't emit on key change
            |level, group_logs, state_access| {
                let fut = async move {
                    let mut stats = if let Some(bytes) = state_access.get().await {
                        serde_json::from_slice(&bytes).unwrap_or(ServiceStats {
                            total_logs: 0,
                            error_count: 0,
                            last_message: String::new(),
                        })
                    } else {
                        ServiceStats {
                            total_logs: 0,
                            error_count: 0,
                            last_message: String::new(),
                        }
                    };

                    stats.total_logs += group_logs.len() as u64;
                    stats.last_message = group_logs.last().unwrap().message.clone();

                    let stats_bytes = serde_json::to_vec(&stats).unwrap();
                    state_access.set(&stats_bytes).await.unwrap();

                    Ok(format!(
                        "Level: {}, Batch: {} items, Total: {}, Last: {}",
                        level,
                        group_logs.len(),
                        stats.total_logs,
                        stats.last_message
                    ))
                };
                Box::pin(fut)
            },
        )
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect::<Vec<_>>();

    println!("  Level groups:");
    for group in &level_groups {
        println!("    {}", group);
    }

    println!("\n=== Stateful Group By Example Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("1. Size-based grouping: Emit groups when they reach a certain size");
    println!("2. Key-change grouping: Emit groups when the key changes");
    println!("3. End-of-stream grouping: Emit all remaining groups at stream end");
    println!("4. Stateful accumulation: Maintain statistics across group emissions");
    println!("5. True streaming: Process items as they arrive without buffering entire stream");
}
