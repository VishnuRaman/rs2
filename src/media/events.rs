//! Media streaming events for Kafka integration

use super::types::*;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MediaStreamEvent {
    StreamStarted {
        stream_id: String,
        user_id: u64,
        quality: QualityLevel,
        timestamp: DateTime<Utc>,
    },
    StreamStopped {
        stream_id: String,
        user_id: u64,
        duration_seconds: u64,
        bytes_transferred: u64,
        timestamp: DateTime<Utc>,
    },
    QualityChanged {
        stream_id: String,
        user_id: u64,
        old_quality: QualityLevel,
        new_quality: QualityLevel,
        timestamp: DateTime<Utc>,
    },
    BufferUnderrun {
        stream_id: String,
        user_id: u64,
        buffer_level: f64,
        timestamp: DateTime<Utc>,
    },
    ChunkDropped {
        stream_id: String,
        user_id: u64,
        sequence_number: u64,
        reason: String,
        timestamp: DateTime<Utc>,
    },
}

impl MediaStreamEvent {
    pub fn stream_id(&self) -> &str {
        match self {
            MediaStreamEvent::StreamStarted { stream_id, .. } => stream_id,
            MediaStreamEvent::StreamStopped { stream_id, .. } => stream_id,
            MediaStreamEvent::QualityChanged { stream_id, .. } => stream_id,
            MediaStreamEvent::BufferUnderrun { stream_id, .. } => stream_id,
            MediaStreamEvent::ChunkDropped { stream_id, .. } => stream_id,
        }
    }

    pub fn user_id(&self) -> u64 {
        match self {
            MediaStreamEvent::StreamStarted { user_id, .. } => *user_id,
            MediaStreamEvent::StreamStopped { user_id, .. } => *user_id,
            MediaStreamEvent::QualityChanged { user_id, .. } => *user_id,
            MediaStreamEvent::BufferUnderrun { user_id, .. } => *user_id,
            MediaStreamEvent::ChunkDropped { user_id, .. } => *user_id,
        }
    }
}

// Integration with your existing activity system
impl From<MediaStreamEvent> for super::types::UserActivity {
    fn from(event: MediaStreamEvent) -> Self {
        use std::collections::HashMap;

        let mut metadata = HashMap::new();
        let activity_type = match &event {
            MediaStreamEvent::StreamStarted { quality, .. } => {
                metadata.insert("quality".to_string(), format!("{:?}", quality));
                "media_stream_started".to_string()
            }
            MediaStreamEvent::StreamStopped {
                duration_seconds,
                bytes_transferred,
                ..
            } => {
                metadata.insert("duration_seconds".to_string(), duration_seconds.to_string());
                metadata.insert(
                    "bytes_transferred".to_string(),
                    bytes_transferred.to_string(),
                );
                "media_stream_stopped".to_string()
            }
            MediaStreamEvent::QualityChanged {
                old_quality,
                new_quality,
                ..
            } => {
                metadata.insert("old_quality".to_string(), format!("{:?}", old_quality));
                metadata.insert("new_quality".to_string(), format!("{:?}", new_quality));
                "media_quality_changed".to_string()
            }
            MediaStreamEvent::BufferUnderrun { buffer_level, .. } => {
                metadata.insert("buffer_level".to_string(), buffer_level.to_string());
                "media_buffer_underrun".to_string()
            }
            MediaStreamEvent::ChunkDropped {
                sequence_number,
                reason,
                ..
            } => {
                metadata.insert("sequence_number".to_string(), sequence_number.to_string());
                metadata.insert("reason".to_string(), reason.clone());
                "media_chunk_dropped".to_string()
            }
        };

        metadata.insert("stream_id".to_string(), event.stream_id().to_string());

        super::types::UserActivity {
            id: uuid::Uuid::new_v4().to_string(),
            user_id: event.user_id(),
            activity_type,
            timestamp: chrono::Utc::now(),
            metadata,
        }
    }
}
