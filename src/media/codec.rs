//! Codec implementations for media processing
//!
//! Provides encoding/decoding capabilities with support for different formats
//! and quality levels. Uses async processing to integrate with RS2Stream.

use super::types::*;
use crate::*;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::Instant;

/// Errors that can occur during codec operations
#[derive(Debug, Clone)]
pub enum CodecError {
    UnsupportedFormat(String),
    EncodingFailed(String),
    DecodingFailed(String),
    InvalidData(String),
    ResourceExhausted,
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::UnsupportedFormat(format) => write!(f, "Unsupported format: {}", format),
            CodecError::EncodingFailed(reason) => write!(f, "Encoding failed: {}", reason),
            CodecError::DecodingFailed(reason) => write!(f, "Decoding failed: {}", reason),
            CodecError::InvalidData(reason) => write!(f, "Invalid data: {}", reason),
            CodecError::ResourceExhausted => write!(f, "Resource exhausted"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Configuration for encoding operations
#[derive(Debug, Clone)]
pub struct EncodingConfig {
    pub quality: QualityLevel,
    pub target_bitrate: u32,
    pub keyframe_interval: u32,
    pub enable_compression: bool,
    pub preserve_metadata: bool,
}

impl Default for EncodingConfig {
    fn default() -> Self {
        Self {
            quality: QualityLevel::Medium,
            target_bitrate: 1_000_000, // 1 Mbps
            keyframe_interval: 30,     // Every 30 frames
            enable_compression: true,
            preserve_metadata: true,
        }
    }
}

/// Raw media data before processing
#[derive(Debug, Clone)]
pub struct RawMediaData {
    pub data: Vec<u8>,
    pub media_type: MediaType,
    pub timestamp: Duration,
    pub metadata: HashMap<String, String>,
}

/// Codec statistics for monitoring
#[derive(Debug, Clone, Default)]
pub struct CodecStats {
    pub frames_encoded: u64,
    pub frames_decoded: u64,
    pub bytes_processed: u64,
    pub encoding_time_ms: u64,
    pub average_compression_ratio: f64,
    pub error_count: u64,
}

/// Main codec implementation
pub struct MediaCodec {
    config: EncodingConfig,
    stats: tokio::sync::Mutex<CodecStats>,
}

impl Clone for MediaCodec {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            stats: tokio::sync::Mutex::new(CodecStats::default()),
        }
    }
}

impl MediaCodec {
    pub fn new(config: EncodingConfig) -> Self {
        Self {
            config,
            stats: tokio::sync::Mutex::new(CodecStats::default()),
        }
    }

    /// Encode raw media data into chunks
    pub fn encode_stream(
        &self,
        raw_data_stream: RS2Stream<RawMediaData>,
        stream_id: String,
    ) -> RS2Stream<Result<MediaChunk, CodecError>> {
        let self_clone = self.clone();
        auto_backpressure_block(par_eval_map(raw_data_stream, 4, move |raw_data| {
            let stream_id = stream_id.clone();
            let codec = self_clone.clone();
            async move { codec.encode_single_frame(raw_data, stream_id).await }
        }), 256) // Prevent encoder from overwhelming system
    }

    /// Encode a single frame/audio sample
    async fn encode_single_frame(
        &self,
        raw_data: RawMediaData,
        stream_id: String,
    ) -> Result<MediaChunk, CodecError> {
        let start_time = Instant::now();

        // Simulate encoding process
        let encoded_data = self.perform_encoding(&raw_data).await?;

        // Determine chunk type based on content
        let chunk_type = self.determine_chunk_type(&raw_data, &encoded_data);
        let priority = chunk_type.default_priority();

        // Generate checksum for integrity
        let checksum = if self.config.preserve_metadata {
            Some(self.calculate_checksum(&encoded_data))
        } else {
            None
        };

        // Update statistics
        {
            let mut stats = self.stats.lock().await;
            stats.frames_encoded += 1;
            stats.bytes_processed += encoded_data.len() as u64;
            stats.encoding_time_ms += start_time.elapsed().as_millis() as u64;

            let compression_ratio = raw_data.data.len() as f64 / encoded_data.len() as f64;
            stats.average_compression_ratio = (stats.average_compression_ratio
                * (stats.frames_encoded - 1) as f64
                + compression_ratio)
                / stats.frames_encoded as f64;
        }

        Ok(MediaChunk {
            stream_id,
            sequence_number: 0, // Will be set by chunk processor
            data: encoded_data,
            chunk_type,
            priority,
            timestamp: raw_data.timestamp,
            is_final: false,
            checksum,
        })
    }

    /// Perform the actual encoding (simplified mock implementation)
    async fn perform_encoding(&self, raw_data: &RawMediaData) -> Result<Vec<u8>, CodecError> {
        // Simulate encoding work
        tokio::time::sleep(Duration::from_micros(100)).await;

        match raw_data.media_type {
            MediaType::Video => self.encode_video(&raw_data.data).await,
            MediaType::Audio => self.encode_audio(&raw_data.data).await,
            MediaType::Mixed => {
                // Split and encode both components
                let (video_data, audio_data) = self.split_mixed_data(&raw_data.data)?;
                let encoded_video = self.encode_video(&video_data).await?;
                let encoded_audio = self.encode_audio(&audio_data).await?;
                Ok(self.combine_encoded_data(encoded_video, encoded_audio))
            }
        }
    }

    async fn encode_video(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if data.is_empty() {
            return Err(CodecError::InvalidData("Empty video data".to_string()));
        }

        // Mock video encoding with compression based on quality
        let compression_factor = match self.config.quality {
            QualityLevel::Low => 0.3,
            QualityLevel::Medium => 0.5,
            QualityLevel::High => 0.7,
            QualityLevel::UltraHigh => 0.9,
        };

        let target_size = (data.len() as f64 * compression_factor) as usize;
        let mut encoded = Vec::with_capacity(target_size);

        // Simulate compression (in reality, this would use actual video codecs)
        for chunk in data.chunks(8) {
            let compressed_chunk: Vec<u8> = chunk
                .iter()
                .step_by(if self.config.enable_compression { 2 } else { 1 })
                .copied()
                .collect();
            encoded.extend(compressed_chunk);
        }

        Ok(encoded)
    }

    async fn encode_audio(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        if data.is_empty() {
            return Err(CodecError::InvalidData("Empty audio data".to_string()));
        }

        // Mock audio encoding - audio typically compresses better than video
        let compression_factor = match self.config.quality {
            QualityLevel::Low => 0.2,
            QualityLevel::Medium => 0.4,
            QualityLevel::High => 0.6,
            QualityLevel::UltraHigh => 0.8,
        };

        let target_size = (data.len() as f64 * compression_factor) as usize;
        let mut encoded = Vec::with_capacity(target_size);

        // Simulate audio compression
        for chunk in data.chunks(4) {
            if let Some(&sample) = chunk.first() {
                encoded.push(sample);
            }
        }

        Ok(encoded)
    }

    fn split_mixed_data(&self, data: &[u8]) -> Result<(Vec<u8>, Vec<u8>), CodecError> {
        if data.len() < 2 {
            return Err(CodecError::InvalidData("Mixed data too short".to_string()));
        }

        // Simple split: assume first 60% is video, rest is audio
        let split_point = (data.len() as f64 * 0.6) as usize;
        let video_data = data[..split_point].to_vec();
        let audio_data = data[split_point..].to_vec();

        Ok((video_data, audio_data))
    }

    fn combine_encoded_data(&self, video: Vec<u8>, audio: Vec<u8>) -> Vec<u8> {
        let mut combined = Vec::with_capacity(video.len() + audio.len() + 8);

        // Add a simple header indicating sizes
        combined.extend_from_slice(&(video.len() as u32).to_le_bytes());
        combined.extend_from_slice(&(audio.len() as u32).to_le_bytes());
        combined.extend(video);
        combined.extend(audio);

        combined
    }

    fn determine_chunk_type(&self, raw_data: &RawMediaData, _encoded_data: &[u8]) -> ChunkType {
        match raw_data.media_type {
            MediaType::Audio => ChunkType::Audio,
            MediaType::Video => {
                // Simulate keyframe detection (every N frames)
                if raw_data.timestamp.as_millis() % (self.config.keyframe_interval as u128 * 33)
                    == 0
                {
                    ChunkType::VideoIFrame
                } else {
                    ChunkType::VideoPFrame
                }
            }
            MediaType::Mixed => ChunkType::VideoIFrame, // Treat mixed as high priority
        }
    }

    fn calculate_checksum(&self, data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    /// Decode chunks back to raw data (for testing/validation)
    pub async fn decode_chunk(&self, chunk: MediaChunk) -> Result<RawMediaData, CodecError> {
        let decoded_data = match chunk.chunk_type {
            ChunkType::Audio => self.decode_audio(&chunk.data).await?,
            ChunkType::VideoIFrame | ChunkType::VideoPFrame | ChunkType::VideoBFrame => {
                self.decode_video(&chunk.data).await?
            }
            ChunkType::Metadata => chunk.data.clone(), // Metadata passed through
            ChunkType::Thumbnail => self.decode_video(&chunk.data).await?, // Treat as video
        };

        // Verify checksum if present
        if let Some(expected_checksum) = &chunk.checksum {
            let actual_checksum = self.calculate_checksum(&chunk.data);
            if actual_checksum != *expected_checksum {
                return Err(CodecError::InvalidData("Checksum mismatch".to_string()));
            }
        }

        let mut stats = self.stats.lock().await;
        stats.frames_decoded += 1;

        Ok(RawMediaData {
            data: decoded_data,
            media_type: match chunk.chunk_type {
                ChunkType::Audio => MediaType::Audio,
                _ => MediaType::Video,
            },
            timestamp: chunk.timestamp,
            metadata: HashMap::new(),
        })
    }

    async fn decode_video(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        // Mock video decoding - expand compressed data
        let mut decoded = Vec::with_capacity(data.len() * 2);

        for &byte in data {
            decoded.push(byte);
            if self.config.enable_compression {
                decoded.push(byte); // Duplicate to simulate decompression
            }
        }

        Ok(decoded)
    }

    async fn decode_audio(&self, data: &[u8]) -> Result<Vec<u8>, CodecError> {
        // Mock audio decoding
        let mut decoded = Vec::with_capacity(data.len() * 4);

        for &sample in data {
            // Expand each sample
            decoded.extend_from_slice(&[sample, sample, sample, sample]);
        }

        Ok(decoded)
    }

    /// Get current codec statistics
    pub async fn get_stats(&self) -> CodecStats {
        let stats = self.stats.lock().await;
        stats.clone()
    }

    /// Reset statistics
    pub async fn reset_stats(&self) {
        let mut stats = self.stats.lock().await;
        *stats = CodecStats::default();
    }
}

/// Factory for creating codecs with different configurations
pub struct CodecFactory;

impl CodecFactory {
    pub fn create_h264_codec(quality: QualityLevel) -> MediaCodec {
        let config = EncodingConfig {
            quality,
            target_bitrate: match quality {
                QualityLevel::Low => 500_000,          // 500 Kbps
                QualityLevel::Medium => 1_500_000,     // 1.5 Mbps
                QualityLevel::High => 5_000_000,       // 5 Mbps
                QualityLevel::UltraHigh => 15_000_000, // 15 Mbps
            },
            keyframe_interval: 30,
            enable_compression: true,
            preserve_metadata: true,
        };

        MediaCodec::new(config)
    }

    pub fn create_audio_codec(quality: QualityLevel) -> MediaCodec {
        let config = EncodingConfig {
            quality,
            target_bitrate: match quality {
                QualityLevel::Low => 64_000,        // 64 Kbps
                QualityLevel::Medium => 128_000,    // 128 Kbps
                QualityLevel::High => 320_000,      // 320 Kbps
                QualityLevel::UltraHigh => 500_000, // 500 Kbps
            },
            keyframe_interval: 0, // N/A for audio
            enable_compression: true,
            preserve_metadata: false, // Audio usually doesn't need checksums
        };

        MediaCodec::new(config)
    }

    pub fn create_adaptive_codec() -> MediaCodec {
        // Codec that can adjust quality based on network conditions
        let config = EncodingConfig {
            quality: QualityLevel::Medium, // Start with medium
            target_bitrate: 1_000_000,
            keyframe_interval: 60, // Longer interval for adaptive streaming
            enable_compression: true,
            preserve_metadata: true,
        };

        MediaCodec::new(config)
    }
}
