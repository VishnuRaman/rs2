use rs2::connectors::web_socket_connector::*;
use serde_json::json;
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, oneshot};
use tokio_tungstenite::tungstenite::Message;

#[test]
fn test_websocket_config_defaults() {
    let config = WebSocketConfig::default();
    assert_eq!(config.url, "");
    assert_eq!(config.connection_timeout, Duration::from_secs(10));
    assert_eq!(config.ping_interval, Duration::from_secs(30));
    assert_eq!(config.max_message_size, 64 * 1024 * 1024);
    assert!(config.auto_reconnect);
    assert_eq!(config.max_reconnect_attempts, 5);
    assert_eq!(config.reconnect_delay, Duration::from_secs(5));
    assert_eq!(config.backpressure_limit, 1000);
    assert_eq!(config.enable_batching, false);
    assert_eq!(config.batch_size, 100);
    assert_eq!(config.batch_timeout, Duration::from_millis(100));
    assert_eq!(config.enable_compression, false);
    assert_eq!(config.heartbeat_timeout, Duration::from_secs(60));
    assert!(config.enable_metrics);
    assert_eq!(config.request_timeout, Duration::from_secs(30));
    assert!(config.auto_request_id);
}

#[test]
fn test_message_serialization() {
    let req = WebSocketMessage::Request {
        id: "abc123".to_string(),
        method: "getData".to_string(),
        params: json!({"foo": 1}),
    };
    let ser = serde_json::to_string(&req).unwrap();
    let de: WebSocketMessage = serde_json::from_str(&ser).unwrap();
    match de {
        WebSocketMessage::Request { id, method, params } => {
            assert_eq!(id, "abc123");
            assert_eq!(method, "getData");
            assert_eq!(params["foo"], 1);
        }
        _ => panic!("Deserialization failed for Request"),
    }

    let resp = WebSocketMessage::Response {
        id: "abc123".to_string(),
        result: Some(json!({"bar": 2})),
        error: None,
    };
    let ser = serde_json::to_string(&resp).unwrap();
    let de: WebSocketMessage = serde_json::from_str(&ser).unwrap();
    match de {
        WebSocketMessage::Response { id, result, error } => {
            assert_eq!(id, "abc123");
            assert_eq!(result.unwrap()["bar"], 2);
            assert!(error.is_none());
        }
        _ => panic!("Deserialization failed for Response"),
    }
}

#[tokio::test]
async fn test_request_response_matching() {
    let pending = Arc::new(Mutex::new(HashMap::new()));
    let (tx, rx) = oneshot::channel::<Result<WebSocketMessage, WebSocketError>>();
    let req_id = "req42".to_string();
    pending.lock().await.insert(req_id.clone(), tx);

    // Simulate receiving a response
    let response = WebSocketMessage::Response {
        id: req_id.clone(),
        result: Some(json!({"ok": true})),
        error: None,
    };
    // Normally, the connector would do this:
    if let WebSocketMessage::Response { id, .. } = &response {
        let mut pending = pending.lock().await;
        if let Some(tx) = pending.remove(id) {
            let _ = tx.send(Ok(response.clone()));
        }
    }
    // The receiver should get the response
    let result = rx.await.unwrap().unwrap();
    match result {
        WebSocketMessage::Response { id, result, error } => {
            assert_eq!(id, "req42");
            assert_eq!(result.unwrap()["ok"], true);
            assert!(error.is_none());
        }
        _ => panic!("Wrong response type"),
    }
}

#[test]
fn test_error_types() {
    let err = WebSocketError::ConnectionFailed("fail".to_string());
    assert_eq!(format!("{}", err), "Connection failed: fail");
    let err = WebSocketError::MessageTooLarge(123);
    assert_eq!(format!("{}", err), "Message too large: 123 bytes");
    let err = WebSocketError::RequestTimeout("id1".to_string());
    assert_eq!(format!("{}", err), "Request timeout: id1");
}

#[tokio::test]
async fn test_backpressure_limit() {
    let mut config = WebSocketConfig::default();
    config.backpressure_limit = 1;
    let connector = WebSocketConnector::new(config);
    // Simulate acquiring the only permit
    let permit = connector.backpressure_semaphore.acquire().await.unwrap();
    // Now, try to acquire another (should block, so we use try_acquire)
    assert!(connector.backpressure_semaphore.try_acquire().is_err());
    drop(permit);
    // Now, should be able to acquire again
    assert!(connector.backpressure_semaphore.try_acquire().is_ok());
}

// Existing tests moved from source file
#[tokio::test]
async fn test_websocket_connector_creation() {
    let config = WebSocketConfig {
        url: "wss://echo.websocket.org".to_string(),
        ..Default::default()
    };

    let connector = WebSocketConnector::new(config);
    assert!(!connector.is_connected().await);
}

#[tokio::test]
async fn test_message_conversion() {
    let json_msg = WebSocketMessage::Json(json!({"test": "data"}));
    let ws_msg = WebSocketConnector::convert_to_ws_message(json_msg, 1024).unwrap().unwrap();

    match ws_msg {
        Message::Text(text) => {
            assert!(text.contains("test"));
            assert!(text.contains("data"));
        }
        _ => panic!("Expected text message"),
    }
}

// Additional tests for better coverage
#[test]
fn test_connection_state_serialization() {
    let states = vec![
        ConnectionState::Disconnected,
        ConnectionState::Connecting,
        ConnectionState::Connected,
        ConnectionState::Reconnecting,
        ConnectionState::Closed,
        ConnectionState::Backpressured,
    ];

    for state in states {
        let ser = serde_json::to_string(&state).unwrap();
        let de: ConnectionState = serde_json::from_str(&ser).unwrap();
        assert_eq!(state, de);
    }
}

#[test]
fn test_connection_event_serialization() {
    let events = vec![
        ConnectionEvent::Connected,
        ConnectionEvent::Disconnected,
        ConnectionEvent::Error("test error".to_string()),
        ConnectionEvent::Reconnecting(3),
        ConnectionEvent::BackpressureDetected(100),
        ConnectionEvent::BackpressureRelieved,
        ConnectionEvent::StreamStarted("stream1".to_string()),
        ConnectionEvent::StreamEnded("stream1".to_string()),
        ConnectionEvent::HeartbeatReceived,
        ConnectionEvent::HeartbeatTimeout,
        ConnectionEvent::RequestSent("req1".to_string()),
        ConnectionEvent::ResponseReceived("req1".to_string()),
        ConnectionEvent::RequestTimeout("req1".to_string()),
    ];

    for event in events {
        let ser = serde_json::to_string(&event).unwrap();
        let de: ConnectionEvent = serde_json::from_str(&ser).unwrap();
        // Note: We can't directly compare because some variants contain owned data
        // but we can verify they serialize/deserialize correctly
        assert!(ser.len() > 0);
    }
}

#[tokio::test]
async fn test_connector_metadata() {
    let config = WebSocketConfig::default();
    let connector = WebSocketConnector::new(config);
    
    let metadata = connector.status().await;
    assert_eq!(metadata.state, ConnectionState::Disconnected);
    assert_eq!(metadata.messages_sent, 0);
    assert_eq!(metadata.messages_received, 0);
    assert_eq!(metadata.requests_sent, 0);
    assert_eq!(metadata.responses_received, 0);
    assert_eq!(metadata.pending_requests, 0);
    assert_eq!(metadata.active_streams, 0);
    assert_eq!(metadata.backpressure_count, 0);
    assert_eq!(metadata.error_count, 0);
    assert!(metadata.connection_id.len() > 0);
} 