use futures::StreamExt;
use rs2_stream::{
    state::config::StateConfigs,
    state::{CustomKeyExtractor, StatefulStreamExt},
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Request {
    id: String,
    user_id: String,
    endpoint: String,
    timestamp: u64,
    priority: u8,
}

#[tokio::main]
async fn main() {
    println!("=== RS2 Stateful Throttle Example ===\n");

    // Create sample requests
    let requests = vec![
        Request {
            id: "req1".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/data".to_string(),
            timestamp: 1000,
            priority: 1,
        },
        Request {
            id: "req2".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/data".to_string(),
            timestamp: 1100,
            priority: 2,
        },
        Request {
            id: "req3".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/data".to_string(),
            timestamp: 1200,
            priority: 1,
        },
        Request {
            id: "req4".to_string(),
            user_id: "user2".to_string(),
            endpoint: "/api/users".to_string(),
            timestamp: 1300,
            priority: 3,
        },
        Request {
            id: "req5".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/users".to_string(),
            timestamp: 1400,
            priority: 1,
        },
        Request {
            id: "req6".to_string(),
            user_id: "user2".to_string(),
            endpoint: "/api/data".to_string(),
            timestamp: 1500,
            priority: 2,
        },
    ];

    // Example 1: Throttle by user with high performance config
    println!("1. Throttle by User (2 requests per second):");
    let high_perf_config = StateConfigs::high_performance();

    let user_throttled = futures::stream::iter(requests.clone()).stateful_throttle_rs2(
        high_perf_config,
        CustomKeyExtractor::new(|req: &Request| req.user_id.clone()),
        2,                      // Rate limit: 2 requests per window
        Duration::from_secs(1), // 1 second window
        |req| req,              // Identity function
    );

    let user_results: Vec<Request> = user_throttled
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("  Original requests: {}", requests.len());
    println!("  After throttling: {}", user_results.len());
    println!("  Allowed requests:");
    for req in user_results {
        println!(
            "    {}: {} by {} (priority: {})",
            req.id, req.endpoint, req.user_id, req.priority
        );
    }

    // Example 2: Throttle by endpoint with session config
    println!("\n2. Throttle by Endpoint (3 requests per 2 seconds):");
    let session_config = StateConfigs::session();

    let endpoint_throttled = futures::stream::iter(requests.clone()).stateful_throttle_rs2(
        session_config,
        CustomKeyExtractor::new(|req: &Request| req.endpoint.clone()),
        3,                      // Rate limit: 3 requests per window
        Duration::from_secs(2), // 2 second window
        |req| req,              // Identity function
    );

    let endpoint_results: Vec<Request> = endpoint_throttled
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("  Original requests: {}", requests.len());
    println!("  After throttling: {}", endpoint_results.len());
    println!("  Allowed requests by endpoint:");
    for req in endpoint_results {
        println!(
            "    {}: {} by {} (priority: {})",
            req.id, req.endpoint, req.user_id, req.priority
        );
    }

    // Example 3: Throttle by user-endpoint combination with short-lived config
    println!("\n3. Throttle by User-Endpoint Combination (1 request per 500ms):");
    let short_lived_config = StateConfigs::short_lived();

    let user_endpoint_throttled = futures::stream::iter(requests.clone()).stateful_throttle_rs2(
        short_lived_config,
        CustomKeyExtractor::new(|req: &Request| format!("{}_{}", req.user_id, req.endpoint)),
        1,                          // Rate limit: 1 request per window
        Duration::from_millis(500), // 500ms window
        |req| req,                  // Identity function
    );

    let user_endpoint_results: Vec<Request> = user_endpoint_throttled
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("  Original requests: {}", requests.len());
    println!("  After throttling: {}", user_endpoint_results.len());
    println!("  Allowed requests by user-endpoint:");
    for req in user_endpoint_results {
        println!(
            "    {}: {} by {} (priority: {})",
            req.id, req.endpoint, req.user_id, req.priority
        );
    }

    // Example 4: Throttle by priority with long-lived config
    println!("\n4. Throttle by Priority (5 requests per 3 seconds):");
    let long_lived_config = StateConfigs::long_lived();

    let priority_throttled = futures::stream::iter(requests.clone()).stateful_throttle_rs2(
        long_lived_config,
        CustomKeyExtractor::new(|req: &Request| req.priority.to_string()),
        5,                      // Rate limit: 5 requests per window
        Duration::from_secs(3), // 3 second window
        |req| req,              // Identity function
    );

    let priority_results: Vec<Request> = priority_throttled
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("  Original requests: {}", requests.len());
    println!("  After throttling: {}", priority_results.len());
    println!("  Allowed requests by priority:");
    for req in priority_results {
        println!(
            "    {}: {} by {} (priority: {})",
            req.id, req.endpoint, req.user_id, req.priority
        );
    }

    // Example 5: Simulate real-time throttling with different time windows
    println!("\n5. Real-time Throttling Simulation:");
    let realtime_config = StateConfigs::high_performance();

    // Simulate requests coming in rapid succession
    let rapid_requests = vec![
        Request {
            id: "rapid1".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/stream".to_string(),
            timestamp: 1000,
            priority: 1,
        },
        Request {
            id: "rapid2".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/stream".to_string(),
            timestamp: 1001,
            priority: 1,
        },
        Request {
            id: "rapid3".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/stream".to_string(),
            timestamp: 1002,
            priority: 1,
        },
        Request {
            id: "rapid4".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/stream".to_string(),
            timestamp: 1003,
            priority: 1,
        },
        Request {
            id: "rapid5".to_string(),
            user_id: "user1".to_string(),
            endpoint: "/api/stream".to_string(),
            timestamp: 1004,
            priority: 1,
        },
    ];

    let rapid_throttled = futures::stream::iter(rapid_requests).stateful_throttle_rs2(
        realtime_config,
        CustomKeyExtractor::new(|req: &Request| req.user_id.clone()),
        2,                          // Rate limit: 2 requests per window
        Duration::from_millis(100), // 100ms window
        |req| req,                  // Identity function
    );

    let rapid_results: Vec<Request> = rapid_throttled
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

    println!("  Rapid requests: 5");
    println!("  After throttling: {}", rapid_results.len());
    println!("  Throttled requests:");
    for req in rapid_results {
        println!("    {} at timestamp {}", req.id, req.timestamp);
    }

    println!("\n=== Stateful Throttle Example Complete ===");
}
