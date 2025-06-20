//! Media streaming module
//!
//! Provides video and audio streaming capabilities with priority handling,
//! chunked processing, and integration with existing queue system.

pub mod chunk_processor;
pub mod codec;
pub mod events;
pub mod priority_queue;
pub mod streaming;
pub mod types;

// Re-export main types for convenience
pub use events::*;
pub use priority_queue::*;
pub use streaming::*;
pub use types::*;
