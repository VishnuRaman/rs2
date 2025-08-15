//! Connectors for external data sources

pub mod connection_errors;
pub mod kafka_connector;
pub mod stream_connector;

pub use connection_errors::ConnectorError;
pub use kafka_connector::KafkaConnector;
pub use stream_connector::{CommonConfig, StreamConnector};
