//! Stream connectors for integrating with external systems

pub mod connection_errors;
pub mod stream_connector;

pub mod kafka_connector;

// Re-export main types
pub use connection_errors::{ConnectorError, ConnectorResult};
pub use stream_connector::{BidirectionalConnector, CommonConfig, StreamConnector};

// Re-export connector implementations
pub use kafka_connector::KafkaConnector;
