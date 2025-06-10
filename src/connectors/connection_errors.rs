//! Error types for connectors

use std::fmt;

/// Main error type for connector operations
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectorError {
    /// Connection failed
    ConnectionFailed(String),
    /// Authentication failed
    AuthenticationFailed(String),
    /// Invalid configuration
    InvalidConfiguration(String),
    /// Serialization/deserialization error
    SerializationError(String),
    /// I/O error
    IO(String),
    /// Timeout occurred
    Timeout,
    /// Resource not found
    NotFound(String),
    /// Permission denied
    PermissionDenied(String),
    /// Resource exhausted
    ResourceExhausted,
    /// Connector-specific error
    ConnectorSpecific(String),
    /// Multiple errors occurred
    Multiple(Vec<ConnectorError>),
}

impl fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectorError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            ConnectorError::AuthenticationFailed(msg) => write!(f, "Authentication failed: {}", msg),
            ConnectorError::InvalidConfiguration(msg) => write!(f, "Invalid configuration: {}", msg),
            ConnectorError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            ConnectorError::IO(msg) => write!(f, "I/O error: {}", msg),
            ConnectorError::Timeout => write!(f, "Operation timed out"),
            ConnectorError::NotFound(msg) => write!(f, "Resource not found: {}", msg),
            ConnectorError::PermissionDenied(msg) => write!(f, "Permission denied: {}", msg),
            ConnectorError::ResourceExhausted => write!(f, "Resource exhausted"),
            ConnectorError::ConnectorSpecific(msg) => write!(f, "Connector error: {}", msg),
            ConnectorError::Multiple(errors) => {
                write!(f, "Multiple errors: ")?;
                for (i, error) in errors.iter().enumerate() {
                    if i > 0 { write!(f, ", ")?; }
                    write!(f, "{}", error)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for ConnectorError {}

impl From<std::io::Error> for ConnectorError {
    fn from(err: std::io::Error) -> Self {
        ConnectorError::IO(err.to_string())
    }
}

impl From<serde_json::Error> for ConnectorError {
    fn from(err: serde_json::Error) -> Self {
        ConnectorError::SerializationError(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for ConnectorError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        ConnectorError::Timeout
    }
}

/// Result type for connector operations
pub type ConnectorResult<T> = Result<T, ConnectorError>;