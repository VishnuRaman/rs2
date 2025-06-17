//! WebSocket Client Connector - Enhanced for Request-Response API
//!
//! A robust WebSocket client that handles bidirectional communication
//! with support for request-response patterns, streaming, and connection management.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex, oneshot, broadcast, Semaphore};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use uuid::Uuid;
use crate::rs2::{unfold, RS2Stream};
use crate::connectors::{StreamConnector, ConnectorError};

/// WebSocket client configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    pub url: String,
    pub auth_headers: Option<HashMap<String, String>>,
    pub subprotocols: Vec<String>,
    pub connection_timeout: Duration,
    pub ping_interval: Duration,
    pub max_message_size: usize,
    pub auto_reconnect: bool,
    pub max_reconnect_attempts: usize,
    pub reconnect_delay: Duration,
    /// Backpressure configuration
    pub backpressure_limit: usize,
    /// Enable message batching
    pub enable_batching: bool,
    /// Batch size for messages
    pub batch_size: usize,
    /// Batch timeout
    pub batch_timeout: Duration,
    /// Enable compression
    pub enable_compression: bool,
    /// Heartbeat timeout
    pub heartbeat_timeout: Duration,
    /// Enable metrics collection
    pub enable_metrics: bool,
    /// Request timeout for request-response operations
    pub request_timeout: Duration,
    /// Enable automatic request ID generation
    pub auto_request_id: bool,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            auth_headers: None,
            subprotocols: vec![],
            connection_timeout: Duration::from_secs(10),
            ping_interval: Duration::from_secs(30),
            max_message_size: 64 * 1024 * 1024, // 64MB
            auto_reconnect: true,
            max_reconnect_attempts: 5,
            reconnect_delay: Duration::from_secs(5),
            backpressure_limit: 1000,
            enable_batching: false,
            batch_size: 100,
            batch_timeout: Duration::from_millis(100),
            enable_compression: false,
            heartbeat_timeout: Duration::from_secs(60),
            enable_metrics: true,
            request_timeout: Duration::from_secs(30),
            auto_request_id: true,
        }
    }
}

/// WebSocket message types with request-response support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
    Json(serde_json::Value),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    /// Request message with ID for response matching
    Request {
        id: String,
        method: String,
        params: serde_json::Value,
    },
    /// Response message with ID for request matching
    Response {
        id: String,
        result: Option<serde_json::Value>,
        error: Option<serde_json::Value>,
    },
    /// Streaming message with sequence number
    Stream {
        sequence: u64,
        data: Vec<u8>,
        is_final: bool,
    },
    /// Heartbeat message
    Heartbeat {
        timestamp: u64,
        client_id: String,
    },
    /// Flow control message
    FlowControl {
        window_size: usize,
        client_id: String,
    },
}

/// Connection events with client context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConnectionEvent {
    Connected,
    Disconnected,
    Error(String),
    Reconnecting(usize), // attempt number
    BackpressureDetected(usize), // queue size
    BackpressureRelieved,
    StreamStarted(String), // stream_id
    StreamEnded(String), // stream_id
    HeartbeatReceived,
    HeartbeatTimeout,
    RequestSent(String), // request_id
    ResponseReceived(String), // request_id
    RequestTimeout(String), // request_id
}

/// WebSocket client metadata with request-response metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketMetadata {
    pub connection_id: String,
    pub state: ConnectionState,
    pub connected_at: Option<chrono::DateTime<chrono::Utc>>,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub reconnect_attempts: usize,
    pub last_ping: Option<chrono::DateTime<chrono::Utc>>,
    pub last_pong: Option<chrono::DateTime<chrono::Utc>>,
    // Request-response metrics
    pub requests_sent: u64,
    pub responses_received: u64,
    pub pending_requests: usize,
    pub average_response_time_ms: f64,
    pub active_streams: usize,
    pub backpressure_count: u64,
    pub error_count: u64,
    pub last_heartbeat: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Closed,
    Backpressured,
}

/// WebSocket error types with client context
#[derive(Debug, thiserror::Error)]
pub enum WebSocketError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("WebSocket error: {0}")]
    WebSocketError(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("Connection timeout")]
    ConnectionTimeout,

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Message too large: {0} bytes")]
    MessageTooLarge(usize),

    #[error("Invalid message format")]
    InvalidMessageFormat,

    #[error("Send channel closed")]
    SendChannelClosed,

    #[error("Backpressure limit exceeded: {0}")]
    BackpressureLimitExceeded(usize),

    #[error("Stream not found: {0}")]
    StreamNotFound(String),

    #[error("Heartbeat timeout")]
    HeartbeatTimeout,

    #[error("Flow control error: {0}")]
    FlowControlError(String),

    #[error("Request timeout: {0}")]
    RequestTimeout(String),

    #[error("Request failed: {0}")]
    RequestFailed(String),

    #[error("Response error: {0}")]
    ResponseError(String),
}

/// Internal commands for the WebSocket connection
#[derive(Debug)]
enum Command {
    Send(WebSocketMessage),
    SendBatch(Vec<WebSocketMessage>),
    SendAndWaitForId {
        message: WebSocketMessage,
        id: String,
        response_tx: oneshot::Sender<Result<WebSocketMessage, WebSocketError>>
    },
    Close,
    Ping(Vec<u8>),
    Heartbeat,
    FlowControl { window_size: usize },
}

/// WebSocket Client - Enhanced for request-response applications
pub struct WebSocketConnector {
    config: WebSocketConfig,
    metadata: Arc<Mutex<WebSocketMetadata>>,
    command_tx: Arc<Mutex<Option<mpsc::UnboundedSender<Command>>>>,
    incoming_messages: Arc<Mutex<Option<broadcast::Sender<WebSocketMessage>>>>,
    connection_events: Arc<Mutex<Option<broadcast::Sender<ConnectionEvent>>>>,
    pending_responses: Arc<Mutex<HashMap<String, oneshot::Sender<Result<WebSocketMessage, WebSocketError>>>>>,
    // Client-specific fields
    pub backpressure_semaphore: Arc<Semaphore>,
    active_streams: Arc<Mutex<HashMap<String, StreamInfo>>>,
    heartbeat_timer: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    metrics: Arc<Mutex<StreamMetrics>>,
    request_counter: Arc<Mutex<u64>>,
}

/// Stream information for tracking active streams
#[derive(Debug, Clone)]
struct StreamInfo {
    pub sequence: u64,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub messages_sent: u64,
    pub bytes_sent: u64,
}

/// Streaming metrics
#[derive(Debug, Clone)]
struct StreamMetrics {
    pub total_latency_ms: u64,
    pub message_count: u64,
    pub last_update: chrono::DateTime<chrono::Utc>,
}

impl WebSocketConnector {
    pub fn new(config: WebSocketConfig) -> Self {
        Self {
            config: config.clone(),
            metadata: Arc::new(Mutex::new(WebSocketMetadata {
                connection_id: Uuid::new_v4().to_string(),
                state: ConnectionState::Disconnected,
                connected_at: None,
                messages_sent: 0,
                messages_received: 0,
                bytes_sent: 0,
                bytes_received: 0,
                reconnect_attempts: 0,
                last_ping: None,
                last_pong: None,
                requests_sent: 0,
                responses_received: 0,
                pending_requests: 0,
                average_response_time_ms: 0.0,
                active_streams: 0,
                backpressure_count: 0,
                error_count: 0,
                last_heartbeat: None,
            })),
            command_tx: Arc::new(Mutex::new(None)),
            incoming_messages: Arc::new(Mutex::new(None)),
            connection_events: Arc::new(Mutex::new(None)),
            pending_responses: Arc::new(Mutex::new(HashMap::new())),
            backpressure_semaphore: Arc::new(Semaphore::new(config.backpressure_limit)),
            active_streams: Arc::new(Mutex::new(HashMap::new())),
            heartbeat_timer: Arc::new(Mutex::new(None)),
            metrics: Arc::new(Mutex::new(StreamMetrics {
                total_latency_ms: 0,
                message_count: 0,
                last_update: chrono::Utc::now(),
            })),
            request_counter: Arc::new(Mutex::new(0)),
        }
    }

    /// Connect to the WebSocket server
    pub async fn connect(&self) -> Result<(), WebSocketError> {
        // Create channels once
        let (command_tx, mut command_rx) = mpsc::unbounded_channel();
        let (message_tx, _) = broadcast::channel(self.config.backpressure_limit);
        let (event_tx, _) = broadcast::channel(100);

        // Store channels
        {
            *self.command_tx.lock().await = Some(command_tx);
            *self.incoming_messages.lock().await = Some(message_tx.clone());
            *self.connection_events.lock().await = Some(event_tx.clone());
        }

        // Update state
        {
            let mut metadata = self.metadata.lock().await;
            metadata.state = ConnectionState::Connecting;
        }

        // Attempt connection with retries
        let mut attempts = 0;
        loop {
            match self.try_connect(command_rx, message_tx.clone(), event_tx.clone()).await {
                Ok(_) => {
                    let mut metadata = self.metadata.lock().await;
                    metadata.state = ConnectionState::Connected;
                    metadata.connected_at = Some(chrono::Utc::now());
                    metadata.reconnect_attempts = 0;

                    let _ = event_tx.send(ConnectionEvent::Connected);
                    
                    // Start heartbeat if enabled
                    if self.config.heartbeat_timeout > Duration::ZERO {
                        self.start_heartbeat().await;
                    }
                    
                    return Ok(());
                }
                Err(e) => {
                    attempts += 1;

                    if !self.config.auto_reconnect || attempts >= self.config.max_reconnect_attempts {
                        let mut metadata = self.metadata.lock().await;
                        metadata.state = ConnectionState::Disconnected;
                        return Err(e);
                    }

                    let _ = event_tx.send(ConnectionEvent::Reconnecting(attempts));

                    {
                        let mut metadata = self.metadata.lock().await;
                        metadata.state = ConnectionState::Reconnecting;
                        metadata.reconnect_attempts = attempts;
                    }

                    tokio::time::sleep(self.config.reconnect_delay).await;

                    // Create new channels for retry attempt
                    let (new_command_tx, new_command_rx) = mpsc::unbounded_channel();
                    *self.command_tx.lock().await = Some(new_command_tx);
                    command_rx = new_command_rx;
                }
            }
        }
    }

    /// Send a request and wait for response (main client API)
    pub async fn request(&self, method: String, params: serde_json::Value) -> Result<serde_json::Value, WebSocketError> {
        let request_id = if self.config.auto_request_id {
            let mut counter = self.request_counter.lock().await;
            *counter += 1;
            format!("req_{}", *counter)
        } else {
            Uuid::new_v4().to_string()
        };

        let message = WebSocketMessage::Request {
            id: request_id.clone(),
            method,
            params,
        };

        let response = self.send_and_wait_for_response(message, self.config.request_timeout).await?;
        
        match response {
            WebSocketMessage::Response { id, result, error } => {
                if id == request_id {
                    if let Some(err) = error {
                        Err(WebSocketError::ResponseError(err.to_string()))
                    } else if let Some(res) = result {
                        Ok(res)
                    } else {
                        Err(WebSocketError::ResponseError("Empty response".to_string()))
                    }
                } else {
                    Err(WebSocketError::ResponseError("Response ID mismatch".to_string()))
                }
            }
            _ => Err(WebSocketError::ResponseError("Unexpected response type".to_string())),
        }
    }

    /// Send a simple message without waiting for response
    pub async fn send_message(&self, message: WebSocketMessage) -> Result<(), WebSocketError> {
        // Check backpressure
        let _permit = self.backpressure_semaphore
            .acquire()
            .await
            .map_err(|_| WebSocketError::BackpressureLimitExceeded(self.config.backpressure_limit))?;

        let command_tx = {
            let guard = self.command_tx.lock().await;
            guard.as_ref().ok_or(WebSocketError::ConnectionClosed)?.clone()
        };

        command_tx
            .send(Command::Send(message))
            .map_err(|_| WebSocketError::SendChannelClosed)
    }

    /// Send a batch of messages
    pub async fn send_batch(&self, messages: Vec<WebSocketMessage>) -> Result<(), WebSocketError> {
        if messages.len() > self.config.batch_size {
            return Err(WebSocketError::MessageTooLarge(messages.len()));
        }

        let command_tx = {
            let guard = self.command_tx.lock().await;
            guard.as_ref().ok_or(WebSocketError::ConnectionClosed)?.clone()
        };

        command_tx
            .send(Command::SendBatch(messages))
            .map_err(|_| WebSocketError::SendChannelClosed)
    }

    /// Get a stream of incoming messages (for continuous listening)
    pub fn listen(&self) -> RS2Stream<WebSocketMessage> {
        let incoming_guard = self.incoming_messages.clone();
        let backpressure_sem = Arc::clone(&self.backpressure_semaphore);

        unfold((), move |_| {
            let incoming = Arc::clone(&incoming_guard);
            let sem = Arc::clone(&backpressure_sem);
            
            async move {
                // Acquire permit to control backpressure
                let _permit = sem.acquire().await.ok()?;
                
                let mut receiver = {
                    let guard = incoming.lock().await;
                    guard.as_ref()?.subscribe()
                };

                match receiver.recv().await {
                    Ok(message) => Some((message, ())),
                    Err(_) => None,
                }
            }
        })
    }

    /// Get a stream of connection events
    pub fn events(&self) -> RS2Stream<ConnectionEvent> {
        let events_guard = self.connection_events.clone();

        unfold((), move |_| {
            let events = Arc::clone(&events_guard);
            async move {
                let mut receiver = {
                    let guard = events.lock().await;
                    guard.as_ref()?.subscribe()
                };

                match receiver.recv().await {
                    Ok(event) => Some((event, ())),
                    Err(_) => None,
                }
            }
        })
    }

    /// Get current client status
    pub async fn status(&self) -> WebSocketMetadata {
        let mut metadata = self.metadata.lock().await.clone();
        
        // Update request-response metrics
        let pending = self.pending_responses.lock().await;
        metadata.pending_requests = pending.len();
        
        let streams = self.active_streams.lock().await;
        metadata.active_streams = streams.len();
        
        let metrics = self.metrics.lock().await;
        if metrics.message_count > 0 {
            metadata.average_response_time_ms = metrics.total_latency_ms as f64 / metrics.message_count as f64;
        }
        
        metadata
    }

    /// Close the connection
    pub async fn disconnect(&self) -> Result<(), WebSocketError> {
        // Stop heartbeat timer
        if let Some(timer) = self.heartbeat_timer.lock().await.take() {
            timer.abort();
        }

        let command_tx = {
            let guard = self.command_tx.lock().await;
            guard.as_ref().ok_or(WebSocketError::ConnectionClosed)?.clone()
        };

        command_tx
            .send(Command::Close)
            .map_err(|_| WebSocketError::SendChannelClosed)?;

        // Update state
        {
            let mut metadata = self.metadata.lock().await;
            metadata.state = ConnectionState::Closed;
        }

        Ok(())
    }

    /// Check if connected
    pub async fn is_connected(&self) -> bool {
        let metadata = self.metadata.lock().await;
        metadata.state == ConnectionState::Connected
    }

    /// Start heartbeat monitoring
    async fn start_heartbeat(&self) {
        let heartbeat_interval = self.config.heartbeat_timeout / 2;
        let command_tx = {
            let guard = self.command_tx.lock().await;
            guard.as_ref().unwrap().clone()
        };
        let event_tx = {
            let guard = self.connection_events.lock().await;
            guard.as_ref().unwrap().clone()
        };

        let timer = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat_interval);
            loop {
                interval.tick().await;
                if command_tx.send(Command::Heartbeat).is_err() {
                    break;
                }
                
                // Check for heartbeat timeout
                tokio::time::sleep(heartbeat_interval).await;
                let _ = event_tx.send(ConnectionEvent::HeartbeatTimeout);
            }
        });

        *self.heartbeat_timer.lock().await = Some(timer);
    }

    async fn try_connect(
        &self,
        mut command_rx: mpsc::UnboundedReceiver<Command>,
        message_tx: broadcast::Sender<WebSocketMessage>,
        event_tx: broadcast::Sender<ConnectionEvent>,
    ) -> Result<(), WebSocketError> {
        // Connect to WebSocket
        let (ws_stream, _) = tokio::time::timeout(
            self.config.connection_timeout,
            connect_async(&self.config.url)
        )
            .await
            .map_err(|_| WebSocketError::ConnectionTimeout)?
            .map_err(|e| WebSocketError::ConnectionFailed(e.to_string()))?;

        let (mut write_half, mut read_half) = ws_stream.split();

        let metadata = Arc::clone(&self.metadata);
        let pending_responses = Arc::clone(&self.pending_responses);
        let max_message_size = self.config.max_message_size;

        // Handle outgoing messages
        let write_task = {
            let metadata = Arc::clone(&metadata);
            let pending_responses = Arc::clone(&pending_responses);

            tokio::spawn(async move {
                while let Some(command) = command_rx.recv().await {
                    match command {
                        Command::Send(msg) => {
                            if let Some(ws_msg_result) = Self::convert_to_ws_message(msg, max_message_size) {
                                match ws_msg_result {
                                    Ok(ws_msg) => {
                                        if let Err(e) = write_half.send(ws_msg).await {
                                            log::error!("Failed to send message: {}", e);
                                            break;
                                        } else {
                                            let mut meta = metadata.lock().await;
                                            meta.messages_sent += 1;
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Message conversion error: {}", e);
                                    }
                                }
                            }
                        }
                        Command::SendBatch(messages) => {
                            for msg in messages {
                                if let Some(ws_msg_result) = Self::convert_to_ws_message(msg, max_message_size) {
                                    match ws_msg_result {
                                        Ok(ws_msg) => {
                                            if let Err(e) = write_half.send(ws_msg).await {
                                                log::error!("Failed to send batch message: {}", e);
                                                break;
                                            } else {
                                                let mut meta = metadata.lock().await;
                                                meta.messages_sent += 1;
                                            }
                                        }
                                        Err(e) => {
                                            log::error!("Batch message conversion error: {}", e);
                                        }
                                    }
                                }
                            }
                        }
                        Command::SendAndWaitForId { message, id, response_tx } => {
                            // Store pending response
                            {
                                let mut pending = pending_responses.lock().await;
                                pending.insert(id.clone(), response_tx);
                            }

                            if let Some(ws_msg_result) = Self::convert_to_ws_message(message, max_message_size) {
                                match ws_msg_result {
                                    Ok(ws_msg) => {
                                        if let Err(e) = write_half.send(ws_msg).await {
                                            // Remove pending response on error
                                            let mut pending = pending_responses.lock().await;
                                            if let Some(tx) = pending.remove(&id) {
                                                let _ = tx.send(Err(WebSocketError::WebSocketError(e)));
                                            }
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        let mut pending = pending_responses.lock().await;
                                        if let Some(tx) = pending.remove(&id) {
                                            let _ = tx.send(Err(e));
                                        }
                                    }
                                }
                            }
                        }
                        Command::Ping(data) => {
                            if let Err(e) = write_half.send(Message::Ping(data.into())).await {
                                log::error!("Failed to send ping: {}", e);
                                break;
                            } else {
                                let mut meta = metadata.lock().await;
                                meta.last_ping = Some(chrono::Utc::now());
                            }
                        }
                        Command::Heartbeat => {
                            let heartbeat_msg = WebSocketMessage::Heartbeat {
                                timestamp: chrono::Utc::now().timestamp_millis() as u64,
                                client_id: metadata.lock().await.connection_id.clone(),
                            };
                            
                            if let Some(ws_msg_result) = Self::convert_to_ws_message(heartbeat_msg, max_message_size) {
                                match ws_msg_result {
                                    Ok(ws_msg) => {
                                        if let Err(e) = write_half.send(ws_msg).await {
                                            log::error!("Failed to send heartbeat: {}", e);
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Heartbeat conversion error: {}", e);
                                    }
                                }
                            }
                        }
                        Command::FlowControl { window_size } => {
                            let flow_control_msg = WebSocketMessage::FlowControl {
                                window_size,
                                client_id: metadata.lock().await.connection_id.clone(),
                            };
                            
                            if let Some(ws_msg_result) = Self::convert_to_ws_message(flow_control_msg, max_message_size) {
                                match ws_msg_result {
                                    Ok(ws_msg) => {
                                        if let Err(e) = write_half.send(ws_msg).await {
                                            log::error!("Failed to send flow control: {}", e);
                                            break;
                                        }
                                    }
                                    Err(e) => {
                                        log::error!("Flow control conversion error: {}", e);
                                    }
                                }
                            }
                        }
                        Command::Close => {
                            let _ = write_half.close().await;
                            break;
                        }
                    }
                }
            })
        };

        // Handle incoming messages
        let read_task = {
            let metadata = Arc::clone(&metadata);
            let pending_responses = Arc::clone(&pending_responses);

            tokio::spawn(async move {
                while let Some(msg_result) = read_half.next().await {
                    match msg_result {
                        Ok(ws_msg) => {
                            match &ws_msg {
                                Message::Text(_) | Message::Binary(_) | Message::Ping(_) => {
                                    if let Some(app_msg) = Self::convert_from_ws_message(ws_msg) {
                                        // Update metadata
                                        {
                                            let mut meta = metadata.lock().await;
                                            meta.messages_received += 1;
                                        }

                                        // Check if this is a response to a pending request
                                        if let WebSocketMessage::Json(ref json) = app_msg {
                                            if let Some(id) = json.get("id").and_then(|v| v.as_str()) {
                                                let mut pending = pending_responses.lock().await;
                                                if let Some(response_tx) = pending.remove(id) {
                                                    let _ = response_tx.send(Ok(app_msg.clone()));
                                                    continue; // Don't broadcast this message
                                                }
                                            }
                                        }

                                        // Handle heartbeat
                                        if let WebSocketMessage::Heartbeat { .. } = app_msg {
                                            let mut meta = metadata.lock().await;
                                            meta.last_heartbeat = Some(chrono::Utc::now());
                                            let _ = event_tx.send(ConnectionEvent::HeartbeatReceived);
                                            continue;
                                        }

                                        // Broadcast to all listeners
                                        let _ = message_tx.send(app_msg);
                                    }
                                }
                                Message::Pong(_) => {
                                    let mut meta = metadata.lock().await;
                                    meta.last_pong = Some(chrono::Utc::now());
                                }
                                Message::Close(_) => {
                                    log::info!("WebSocket connection closed by server");
                                    break;
                                }
                                _ => {}
                            }
                        }
                        Err(e) => {
                            log::error!("WebSocket read error: {}", e);
                            let _ = event_tx.send(ConnectionEvent::Error(e.to_string()));
                            break;
                        }
                    }
                }

                // Connection closed
                let _ = event_tx.send(ConnectionEvent::Disconnected);
            })
        };

        // Start ping task
        let ping_task = if self.config.ping_interval > Duration::ZERO {
            let command_tx = {
                let guard = self.command_tx.lock().await;
                guard.as_ref().unwrap().clone()
            };
            let ping_interval = self.config.ping_interval;

            Some(tokio::spawn(async move {
                let mut interval = tokio::time::interval(ping_interval);
                loop {
                    interval.tick().await;
                    if command_tx.send(Command::Ping(vec![])).is_err() {
                        break;
                    }
                }
            }))
        } else {
            None
        };

        // Wait for any task to complete (indicating connection closed)
        tokio::select! {
            _ = write_task => {},
            _ = read_task => {},
            _ = async {
                if let Some(task) = ping_task {
                    let _ = task.await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {},
        }

        Ok(())
    }

    pub fn convert_to_ws_message(msg: WebSocketMessage, max_size: usize) -> Option<Result<Message, WebSocketError>> {
        match msg {
            WebSocketMessage::Text(text) => {
                if text.len() > max_size {
                    Some(Err(WebSocketError::MessageTooLarge(text.len())))
                } else {
                    Some(Ok(Message::Text(text.into())))
                }
            }
            WebSocketMessage::Binary(data) => {
                if data.len() > max_size {
                    Some(Err(WebSocketError::MessageTooLarge(data.len())))
                } else {
                    Some(Ok(Message::Binary(data.into())))
                }
            }
            WebSocketMessage::Json(json) => {
                match serde_json::to_string(&json) {
                    Ok(text) => {
                        if text.len() > max_size {
                            Some(Err(WebSocketError::MessageTooLarge(text.len())))
                        } else {
                            Some(Ok(Message::Text(text.into())))
                        }
                    }
                    Err(e) => Some(Err(WebSocketError::SerializationError(e))),
                }
            }
            WebSocketMessage::Ping(data) => Some(Ok(Message::Ping(data.into()))),
            WebSocketMessage::Pong(data) => Some(Ok(Message::Pong(data.into()))),
            WebSocketMessage::Stream { sequence, data, is_final } => {
                // Convert stream message to JSON
                let stream_json = serde_json::json!({
                    "type": "stream",
                    "sequence": sequence,
                    "data": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &data),
                    "is_final": is_final
                });
                
                match serde_json::to_string(&stream_json) {
                    Ok(text) => {
                        if text.len() > max_size {
                            Some(Err(WebSocketError::MessageTooLarge(text.len())))
                        } else {
                            Some(Ok(Message::Text(text.into())))
                        }
                    }
                    Err(e) => Some(Err(WebSocketError::SerializationError(e))),
                }
            }
            WebSocketMessage::Request { id, method, params } => {
                let request_json = serde_json::json!({
                    "type": "request",
                    "id": id,
                    "method": method,
                    "params": params
                });
                
                match serde_json::to_string(&request_json) {
                    Ok(text) => {
                        if text.len() > max_size {
                            Some(Err(WebSocketError::MessageTooLarge(text.len())))
                        } else {
                            Some(Ok(Message::Text(text.into())))
                        }
                    }
                    Err(e) => Some(Err(WebSocketError::SerializationError(e))),
                }
            }
            WebSocketMessage::Response { id, result, error } => {
                let response_json = serde_json::json!({
                    "type": "response",
                    "id": id,
                    "result": result,
                    "error": error
                });
                
                match serde_json::to_string(&response_json) {
                    Ok(text) => {
                        if text.len() > max_size {
                            Some(Err(WebSocketError::MessageTooLarge(text.len())))
                        } else {
                            Some(Ok(Message::Text(text.into())))
                        }
                    }
                    Err(e) => Some(Err(WebSocketError::SerializationError(e))),
                }
            }
            WebSocketMessage::Heartbeat { timestamp, client_id } => {
                let heartbeat_json = serde_json::json!({
                    "type": "heartbeat",
                    "timestamp": timestamp,
                    "client_id": client_id
                });
                
                match serde_json::to_string(&heartbeat_json) {
                    Ok(text) => Some(Ok(Message::Text(text.into()))),
                    Err(e) => Some(Err(WebSocketError::SerializationError(e))),
                }
            }
            WebSocketMessage::FlowControl { window_size, client_id } => {
                let flow_control_json = serde_json::json!({
                    "type": "flow_control",
                    "window_size": window_size,
                    "client_id": client_id
                });
                
                match serde_json::to_string(&flow_control_json) {
                    Ok(text) => Some(Ok(Message::Text(text.into()))),
                    Err(e) => Some(Err(WebSocketError::SerializationError(e))),
                }
            }
        }
    }

    fn convert_from_ws_message(msg: Message) -> Option<WebSocketMessage> {
        match msg {
            Message::Text(text) => {
                // Try to parse as JSON first
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
                    // Check for special message types
                    if let Some(msg_type) = json.get("type").and_then(|v| v.as_str()) {
                        match msg_type {
                            "stream" => {
                                if let (Some(sequence), Some(data_b64), Some(is_final)) = (
                                    json.get("sequence").and_then(|v| v.as_u64()),
                                    json.get("data").and_then(|v| v.as_str()),
                                    json.get("is_final").and_then(|v| v.as_bool())
                                ) {
                                    if let Ok(data) = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data_b64) {
                                        return Some(WebSocketMessage::Stream {
                                            sequence,
                                            data,
                                            is_final,
                                        });
                                    }
                                }
                            }
                            "heartbeat" => {
                                if let (Some(timestamp), Some(client_id)) = (
                                    json.get("timestamp").and_then(|v| v.as_u64()),
                                    json.get("client_id").and_then(|v| v.as_str())
                                ) {
                                    return Some(WebSocketMessage::Heartbeat {
                                        timestamp,
                                        client_id: client_id.to_string(),
                                    });
                                }
                            }
                            "flow_control" => {
                                if let (Some(window_size), Some(client_id)) = (
                                    json.get("window_size").and_then(|v| v.as_u64()),
                                    json.get("client_id").and_then(|v| v.as_str())
                                ) {
                                    return Some(WebSocketMessage::FlowControl {
                                        window_size: window_size as usize,
                                        client_id: client_id.to_string(),
                                    });
                                }
                            }
                            _ => {}
                        }
                    }
                    Some(WebSocketMessage::Json(json))
                } else {
                    Some(WebSocketMessage::Text(text.to_string()))
                }
            }
            Message::Binary(data) => Some(WebSocketMessage::Binary(Vec::from(data))),
            Message::Ping(data) => Some(WebSocketMessage::Ping(Vec::from(data))),
            Message::Pong(data) => Some(WebSocketMessage::Pong(Vec::from(data))),
            _ => None,
        }
    }

    /// Send a message and wait for a response with matching ID
    pub async fn send_and_wait_for_response(
        &self,
        message: WebSocketMessage,
        timeout: Duration
    ) -> Result<WebSocketMessage, WebSocketError> {
        let id = Uuid::new_v4().to_string();
        let (response_tx, response_rx) = oneshot::channel();

        let command_tx = {
            let guard = self.command_tx.lock().await;
            guard.as_ref().ok_or(WebSocketError::ConnectionClosed)?.clone()
        };

        command_tx
            .send(Command::SendAndWaitForId { message, id, response_tx })
            .map_err(|_| WebSocketError::SendChannelClosed)?;

        tokio::time::timeout(timeout, response_rx)
            .await
            .map_err(|_| WebSocketError::ConnectionTimeout)?
            .map_err(|_| WebSocketError::ConnectionClosed)?
    }
}

// Implement StreamConnector trait for WebSocket
#[async_trait]
impl<T> StreamConnector<T> for WebSocketConnector
where
    T: for<'de> Deserialize<'de> + Serialize + Send + 'static,
{
    type Config = WebSocketConfig;
    type Error = ConnectorError;
    type Metadata = WebSocketMetadata;

    async fn from_source(&self, _config: Self::Config) -> Result<RS2Stream<T>, Self::Error> {
        // Convert incoming WebSocket messages to type T
        let stream = self.listen()
            .filter_map(|msg| async move {
                match msg {
                    WebSocketMessage::Json(json) => {
                        serde_json::from_value::<T>(json).ok()
                    }
                    WebSocketMessage::Text(text) => {
                        serde_json::from_str::<T>(&text).ok()
                    }
                    WebSocketMessage::Binary(data) => {
                        serde_json::from_slice::<T>(&data).ok()
                    }
                    _ => None,
                }
            });

        Ok(stream.boxed())
    }

    async fn to_sink(&self, stream: RS2Stream<T>, _config: Self::Config) -> Result<Self::Metadata, Self::Error> {
        // Convert stream of T to WebSocket messages and send them
        let command_tx = {
            let guard = self.command_tx.lock().await;
            guard.as_ref().ok_or(ConnectorError::ConnectionFailed("Not connected".to_string()))?.clone()
        };
        
        let sink_task = tokio::spawn(async move {
            let mut stream = stream;
            while let Some(item) = stream.next().await {
                let message = WebSocketMessage::Json(serde_json::to_value(item)
                    .map_err(|e| ConnectorError::SerializationError(e.to_string()))?);
                
                command_tx
                    .send(Command::Send(message))
                    .map_err(|_| ConnectorError::ConnectionFailed("Send channel closed".to_string()))?;
            }
            Ok::<(), ConnectorError>(())
        });

        sink_task.await
            .map_err(|e| ConnectorError::ConnectionFailed(e.to_string()))??;

        Ok(self.status().await)
    }

    async fn health_check(&self) -> Result<bool, Self::Error> {
        let is_connected = self.is_connected().await;
        if !is_connected {
            return Ok(false);
        }

        // Send a ping to check connectivity
        match self.send_message(WebSocketMessage::Ping(vec![])).await {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }

    async fn metadata(&self) -> Result<Self::Metadata, Self::Error> {
        Ok(self.status().await)
    }

    fn name(&self) -> &'static str {
        "WebSocket"
    }

    fn version(&self) -> &'static str {
        "1.0.0"
    }
}