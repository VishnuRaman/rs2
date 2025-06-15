//! Media streaming module
//!
//! Provides video and audio streaming capabilities with priority handling,
//! chunked processing, and integration with existing queue system.

pub mod types;
pub mod streaming;
pub mod priority_queue;
pub mod chunk_processor;
pub mod events;
pub mod codec;

// Re-export main types for convenience
pub use types::*;
pub use streaming::*;
pub use priority_queue::*;
pub use events::*;