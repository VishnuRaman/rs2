//! Core types for media streaming

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum MediaPriority {
    High = 3,   // I-frames, audio, critical metadata
    Normal = 2, // P-frames, standard video data
    Low = 1,    // B-frames, thumbnails, preview data
}

impl Default for MediaPriority {
    fn default() -> Self {
        MediaPriority::Normal
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MediaStream {
    pub id: String,
    pub user_id: u64,
    pub content_type: MediaType,
    pub quality: QualityLevel,
    pub chunk_size: usize,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
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

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct MediaChunk {
    pub stream_id: String,
    pub sequence_number: u64,
    pub data: Vec<u8>,
    pub chunk_type: ChunkType,
    pub priority: MediaPriority,
    pub timestamp: Duration,
    pub is_final: bool,
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChunkType {
    VideoIFrame, // Key frame - high priority
    VideoPFrame, // Predicted frame - normal priority
    VideoBFrame, // Bidirectional frame - low priority
    Audio,       // Audio data - high priority
    Metadata,    // Stream metadata - high priority
    Thumbnail,   // Preview images - low priority
}

impl ChunkType {
    pub fn default_priority(&self) -> MediaPriority {
        match self {
            ChunkType::VideoIFrame | ChunkType::Audio | ChunkType::Metadata => MediaPriority::High,
            ChunkType::VideoPFrame => MediaPriority::Normal,
            ChunkType::VideoBFrame | ChunkType::Thumbnail => MediaPriority::Low,
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
