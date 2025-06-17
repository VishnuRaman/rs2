//! WebSocket Client Example - Request-Response Pattern
//!
//! This example demonstrates how to use the WebSocket connector as a client
//! that sends requests and receives responses, similar to traditional WebSocket clients.

use rs2::connectors::WebSocketConnector;
use rs2::connectors::web_socket_connector::{WebSocketConfig, WebSocketMessage};
use rs2::connectors::StreamConnector;
use serde_json::json;
use std::time::Duration;
use std::sync::Arc;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure the WebSocket client
    let config = WebSocketConfig {
        url: "wss://echo.websocket.org".to_string(), // Using echo server for demo
        connection_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(30),
        auto_reconnect: true,
        max_reconnect_attempts: 3,
        ping_interval: Duration::from_secs(30),
        heartbeat_timeout: Duration::from_secs(60),
        backpressure_limit: 1000,
        enable_metrics: true,
        auto_request_id: true,
        ..Default::default()
    };

    // Create the WebSocket client
    let client = WebSocketConnector::new(config);

    // Connect to the server
    println!("Connecting to WebSocket server...");
    client.connect().await?;
    println!("Connected successfully!");

    // Example 1: Simple request-response
    println!("\n=== Example 1: Simple Request-Response ===");
    let response = client.request(
        "echo".to_string(),
        json!({
            "message": "Hello, WebSocket!",
            "timestamp": chrono::Utc::now().timestamp()
        })
    ).await?;
    
    println!("Response received: {}", response);

    // Example 2: Multiple sequential requests
    println!("\n=== Example 2: Multiple Sequential Requests ===");
    for i in 1..=5 {
        let response = client.request(
            "getData".to_string(),
            json!({
                "id": i,
                "type": "user_data"
            })
        ).await?;
        
        println!("Request {} response: {}", i, response);
        
        // Small delay between requests
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Example 3: Send messages without waiting for response
    println!("\n=== Example 3: Fire-and-Forget Messages ===");
    for i in 1..=3 {
        let message = WebSocketMessage::Json(json!({
            "type": "notification",
            "id": i,
            "content": format!("Notification message {}", i),
            "timestamp": chrono::Utc::now().timestamp()
        }));
        
        client.send_message(message).await?;
        println!("Sent notification {}", i);
    }

    // Example 4: Listen for incoming messages
    println!("\n=== Example 4: Listening for Messages ===");
    let mut message_stream = client.listen();
    
    // Listen for a few messages
    for i in 0..3 {
        match tokio::time::timeout(Duration::from_secs(5), message_stream.next()).await {
            Ok(Some(message)) => {
                println!("Received message {}: {:?}", i + 1, message);
            }
            Ok(None) => {
                println!("No more messages");
                break;
            }
            Err(_) => {
                println!("Timeout waiting for message {}", i + 1);
                break;
            }
        }
    }

    // Example 5: Monitor connection events
    println!("\n=== Example 5: Connection Events ===");
    let mut event_stream = client.events();
    
    // Listen for events for a short time
    match tokio::time::timeout(Duration::from_secs(2), event_stream.next()).await {
        Ok(Some(event)) => {
            println!("Connection event: {:?}", event);
        }
        _ => {
            println!("No events received in timeout period");
        }
    }

    // Example 6: Check client status
    println!("\n=== Example 6: Client Status ===");
    let status = client.status().await;
    println!("Client Status:");
    println!("  Connection ID: {}", status.connection_id);
    println!("  State: {:?}", status.state);
    println!("  Messages Sent: {}", status.messages_sent);
    println!("  Messages Received: {}", status.messages_received);
    println!("  Requests Sent: {}", status.requests_sent);
    println!("  Responses Received: {}", status.responses_received);
    println!("  Pending Requests: {}", status.pending_requests);
    println!("  Average Response Time: {:.2}ms", status.average_response_time_ms);
    println!("  Connected At: {:?}", status.connected_at);

    // Example 7: Health check
    println!("\n=== Example 7: Health Check ===");
    let is_healthy = rs2::connectors::StreamConnector::<serde_json::Value>::health_check(&client).await?;
    println!("Client is healthy: {}", is_healthy);

    // Disconnect
    println!("\n=== Disconnecting ===");
    client.disconnect().await?;
    println!("Disconnected successfully!");

    println!("\n=== Optionally running complex_example (advanced usage, requires real WebSocket server) ===");
    if let Err(e) = complex_example().await {
        println!("complex_example failed (expected if no real server): {}", e);
    }
    Ok(())
}

// Example of how to use the client in a more complex scenario
async fn complex_example() -> Result<(), Box<dyn std::error::Error>> {
    let config = WebSocketConfig {
        url: "wss://your-websocket-server.com".to_string(),
        request_timeout: Duration::from_secs(30),
        auto_reconnect: true,
        ..Default::default()
    };

    let client = WebSocketConnector::new(config);
    client.connect().await?;

    // Start listening for messages in the background
    let mut message_stream = client.listen();
    let client_clone = Arc::new(client);
    
    let listener_task = tokio::spawn(async move {
        while let Some(message) = message_stream.next().await {
            match message {
                WebSocketMessage::Response { id, result, error } => {
                    if let Some(err) = error {
                        println!("Error response for {}: {}", id, err);
                    } else if let Some(res) = result {
                        println!("Success response for {}: {}", id, res);
                    }
                }
                WebSocketMessage::Json(json) => {
                    println!("Received JSON message: {}", json);
                }
                _ => {
                    println!("Received other message: {:?}", message);
                }
            }
        }
    });

    // Send requests in the main task
    let requests = vec![
        ("getUser", json!({"user_id": 123})),
        ("getOrders", json!({"user_id": 123, "limit": 10})),
        ("getProfile", json!({"user_id": 123})),
    ];

    for (method, params) in requests {
        match client_clone.request(method.to_string(), params).await {
            Ok(response) => {
                println!("Request '{}' successful: {}", method, response);
            }
            Err(e) => {
                println!("Request '{}' failed: {}", method, e);
            }
        }
        
        // Small delay between requests
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Wait for listener to finish
    let _ = listener_task.await;
    
    client_clone.disconnect().await?;
    Ok(())
} 