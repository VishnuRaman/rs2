//! Stream connectors for integrating with external systems

pub mod stream_connector;
pub mod connection_errors;

pub mod kafka_connector;
pub mod web_socket_connector;

// Re-export main types
pub use stream_connector::{StreamConnector, BidirectionalConnector, CommonConfig};
pub use connection_errors::{ConnectorError, ConnectorResult};

// Re-export connector implementations
pub use kafka_connector::KafkaConnector;
pub use web_socket_connector::WebSocketConnector;


