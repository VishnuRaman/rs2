//! Error types and handling for RStream
//!
//! This module provides comprehensive error handling capabilities
//! for streaming operations.

use std::fmt;
use std::time::Duration;

/// Main error type for RStream operations
#[derive(Debug, Clone, PartialEq)]
pub enum StreamError {
    /// I/O related errors
    IO(String),
    /// Operation timed out
    Timeout,
    /// Resource exhausted (memory, file descriptors, etc.)
    ResourceExhausted,
    /// Operation was cancelled
    Cancelled,
    /// Backpressure buffer overflow
    BackpressureOverflow,
    /// Custom error with message
    Custom(String),
}

impl fmt::Display for StreamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamError::IO(msg) => write!(f, "IO error: {}", msg),
            StreamError::Timeout => write!(f, "Operation timed out"),
            StreamError::ResourceExhausted => write!(f, "Resource exhausted"),
            StreamError::Cancelled => write!(f, "Operation cancelled"),
            StreamError::BackpressureOverflow => write!(f, "Backpressure buffer overflow"),
            StreamError::Custom(msg) => write!(f, "Stream error: {}", msg),
        }
    }
}

impl std::error::Error for StreamError {}

impl From<std::io::Error> for StreamError {
    fn from(err: std::io::Error) -> Self {
        StreamError::IO(err.to_string())
    }
}

impl From<tokio::time::error::Elapsed> for StreamError {
    fn from(_: tokio::time::error::Elapsed) -> Self {
        StreamError::Timeout
    }
}

/// Result type for rs2_stream operations
pub type StreamResult<T> = Result<T, StreamError>;

/// Retry policy for error handling
#[derive(Debug, Clone)]
pub enum RetryPolicy {
    /// No retries
    None,
    /// Immediate retry up to max_retries
    Immediate { max_retries: usize },
    /// Fixed delay between retries
    Fixed { max_retries: usize, delay: Duration },
    /// Exponential backoff
    Exponential {
        max_retries: usize,
        initial_delay: Duration,
        multiplier: f64,
    },
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy::Fixed {
            max_retries: 3,
            delay: Duration::from_millis(100),
        }
    }
}
