//! Media types and structures

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserActivity {
    // Add 'pub' here
    /// Unique identifier for the activity
    pub id: String, // Make fields public too
    /// User ID who performed the activity
    pub user_id: u64,
    /// Type of activity (e.g., "login", "purchase", "view")
    pub activity_type: String,
    /// Timestamp when the activity occurred
    pub timestamp: DateTime<Utc>,
    /// Additional metadata about the activity
    pub metadata: HashMap<String, String>,
}

/// Media priority levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum MediaPriority {
    Low,
    Normal,
    High,
    Critical,
}

impl Default for MediaPriority {
    fn default() -> Self {
        MediaPriority::Normal
    }
}

/// Media stream configuration
#[derive(Debug, Clone)]
pub struct MediaStream {
    pub id: String,
    pub user_id: u64,
    pub content_type: MediaType,
    pub quality: QualityLevel,
    pub chunk_size: usize,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}

impl Default for MediaStream {
    fn default() -> Self {
        Self {
            id: "default-stream".to_string(),
            user_id: 0,
            content_type: MediaType::Video,
            quality: QualityLevel::Low,
            chunk_size: 1024,
            created_at: Utc::now(),
            metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MediaType {
    Video,
    Audio,
    Mixed, // Audio + Video
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum QualityLevel {
    Low,       // 240p, 64kbps audio
    Medium,    // 480p, 128kbps audio
    High,      // 720p, 192kbps audio
    UltraHigh, // 1080p+, 320kbps audio
}

/// Media chunk types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkType {
    VideoIFrame,
    VideoPFrame,
    VideoBFrame,
    Audio,
    Metadata,
    Thumbnail,
}

/// Media chunk structure
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaChunk {
    pub stream_id: String,
    pub sequence_number: u64,
    pub data: Vec<u8>,
    pub chunk_type: ChunkType,
    pub priority: MediaPriority,
    pub timestamp: Duration,
    pub is_final: bool,
    pub checksum: Option<u32>,
}

impl ChunkType {
    pub fn default_priority(&self) -> MediaPriority {
        match self {
            ChunkType::VideoIFrame | ChunkType::Audio | ChunkType::Metadata | ChunkType::Thumbnail => MediaPriority::High,
            ChunkType::VideoPFrame => MediaPriority::Normal,
            ChunkType::VideoBFrame => MediaPriority::Low,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamMetrics {
    pub stream_id: String,
    pub bytes_processed: u64,
    pub chunks_processed: u64,
    pub dropped_chunks: u64,
    pub average_chunk_size: f64,
    pub buffer_utilization: f64,
    pub last_updated: DateTime<Utc>,
}

/// Media stream event types
#[derive(Debug, Clone)]
pub enum MediaEvent {
    ChunkReceived(MediaChunk),
    StreamStarted(String),
    StreamEnded(String),
    Error(String),
}

/// Media quality metrics
#[derive(Debug, Clone)]
pub struct MediaQuality {
    pub bitrate: u64,
    pub framerate: f64,
    pub resolution: (u32, u32),
    pub quality_score: f64,
}

impl Default for MediaQuality {
    fn default() -> Self {
        Self {
            bitrate: 1000000,
            framerate: 30.0,
            resolution: (1920, 1080),
            quality_score: 0.95,
        }
    }
}
