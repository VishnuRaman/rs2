# Media Streaming in RS2

This document provides in-depth documentation on the media streaming components in the RS2 library, including codec, chunk processing, and the streaming API.

## Table of Contents

- [Overview](#overview)
- [Core Components](#core-components)
  - [Media Types](#media-types)
  - [Codec](#codec)
  - [Chunk Processor](#chunk-processor)
  - [Priority Queue](#priority-queue)
  - [Streaming Service](#streaming-service)
  - [Events System](#events-system)
- [Examples](#examples)
  - [Basic File Streaming](#basic-file-streaming)
  - [Live Streaming](#live-streaming)
  - [Custom Codec Configuration](#custom-codec-configuration)
  - [Handling Stream Events](#handling-stream-events)
- [API Reference](#api-reference)

## Overview

The RS2 media streaming system provides a robust framework for processing and streaming media content. It supports both file-based and live streaming with features like chunk processing, priority-based delivery, and adaptive quality control.

The system is designed with a modular architecture, allowing components to be used independently or together as a complete streaming solution.

## Core Components

### Media Types

The media streaming system uses several core types defined in `types.rs`:

#### MediaChunk

The fundamental unit of media data:

```rust
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
```

#### ChunkType

Defines the type of media chunk:

```rust
pub enum ChunkType {
    VideoIFrame,    // Key frame - high priority
    VideoPFrame,    // Predicted frame - normal priority  
    VideoBFrame,    // Bidirectional frame - low priority
    Audio,          // Audio data - high priority
    Metadata,       // Stream metadata - high priority
    Thumbnail,      // Preview images - low priority
}
```

#### MediaPriority

Determines the delivery priority of chunks:

```rust
pub enum MediaPriority {
    High = 3,    // I-frames, audio, critical metadata
    Normal = 2,  // P-frames, standard video data
    Low = 1,     // B-frames, thumbnails, preview data
}
```

#### MediaStream

Configuration for a media stream:

```rust
pub struct MediaStream {
    pub id: String,
    pub user_id: u64,
    pub content_type: MediaType,
    pub quality: QualityLevel,
    pub chunk_size: usize,
    pub created_at: DateTime<Utc>,
    pub metadata: HashMap<String, String>,
}
```

#### MediaType and QualityLevel

Enums for content type and quality settings:

```rust
pub enum MediaType {
    Video,
    Audio,
    Mixed, // Audio + Video
}

pub enum QualityLevel {
    Low,      // 240p, 64kbps audio
    Medium,   // 480p, 128kbps audio  
    High,     // 720p, 192kbps audio
    UltraHigh, // 1080p+, 320kbps audio
}
```

#### StreamMetrics

Metrics for monitoring stream performance:

```rust
pub struct StreamMetrics {
    pub stream_id: String,
    pub bytes_processed: u64,
    pub chunks_processed: u64,
    pub dropped_chunks: u64,
    pub average_chunk_size: f64,
    pub buffer_utilization: f64,
    pub last_updated: DateTime<Utc>,
}
```

### Codec

The `MediaCodec` in `codec.rs` handles encoding and decoding of media data:

#### EncodingConfig

Configuration for the codec:

```rust
pub struct EncodingConfig {
    pub video_bitrate: u32,
    pub audio_bitrate: u32,
    pub video_codec: String,
    pub audio_codec: String,
    pub frame_rate: u32,
    pub keyframe_interval: u32,
}
```

Default configuration:

```rust
impl Default for EncodingConfig {
    fn default() -> Self {
        Self {
            video_bitrate: 2_000_000,  // 2 Mbps
            audio_bitrate: 128_000,    // 128 kbps
            video_codec: "h264".to_string(),
            audio_codec: "aac".to_string(),
            frame_rate: 30,
            keyframe_interval: 60,     // Every 2 seconds at 30fps
        }
    }
}
```

#### MediaCodec

The main codec implementation:

```rust
pub struct MediaCodec {
    config: EncodingConfig,
    // Internal state...
}
```

Key methods:
- `encode_stream`: Encodes a stream of raw media data into chunks
- `encode_single_frame`: Encodes a single frame of raw data
- `decode_chunk`: Decodes a media chunk back to raw data
- `get_stats`: Retrieves codec performance statistics

#### CodecFactory

Factory for creating pre-configured codecs:

```rust
impl CodecFactory {
    pub fn create_h264_codec(quality: QualityLevel) -> MediaCodec { ... }
    pub fn create_audio_codec(quality: QualityLevel) -> MediaCodec { ... }
    pub fn create_adaptive_codec() -> MediaCodec { ... }
}
```

### Chunk Processor

The `ChunkProcessor` in `chunk_processor.rs` handles the processing pipeline for media chunks:

#### ChunkProcessorConfig

Configuration for chunk processing:

```rust
pub struct ChunkProcessorConfig {
    pub max_buffer_size: usize,
    pub sequence_timeout: Duration,
    pub enable_reordering: bool,
    pub max_reorder_window: usize,
    pub enable_validation: bool,
    pub parallel_processing: usize,
}
```

Default configuration:

```rust
impl Default for ChunkProcessorConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 1024,
            sequence_timeout: Duration::from_secs(5),
            enable_reordering: true,
            max_reorder_window: 32,
            enable_validation: true,
            parallel_processing: 4,
        }
    }
}
```

#### ChunkProcessor

The main chunk processor implementation:

```rust
pub struct ChunkProcessor {
    config: ChunkProcessorConfig,
    codec: Arc<MediaCodec>,
    // Internal state...
}
```

Key methods:
- `process_chunk_stream`: Processes a stream of incoming chunks
- `process_single_chunk`: Processes a single chunk
- `validate_chunk`: Validates chunk integrity and format
- `handle_reordering`: Handles chunk reordering
- `get_stats`: Retrieves processing statistics

#### ChunkProcessorStats

Statistics for monitoring chunk processing:

```rust
pub struct ChunkProcessorStats {
    pub chunks_processed: u64,
    pub chunks_reordered: u64,
    pub chunks_dropped: u64,
    pub sequence_gaps: u64,
    pub validation_failures: u64,
    pub average_processing_time_ms: f64,
    pub buffer_utilization: f64,
}
```

### Priority Queue

The `MediaPriorityQueue` in `priority_queue.rs` provides priority-based delivery of media chunks:

```rust
pub struct MediaPriorityQueue {
    internal_queue: Queue<PriorityItem>,
    priority_buffer: Arc<Mutex<BinaryHeap<PriorityItem>>>,
    buffer_size: usize,
}
```

Key methods:
- `enqueue`: Adds a chunk to the queue with priority handling
- `dequeue`: Returns a stream of chunks in priority order
- `try_enqueue`: Non-blocking version of enqueue
- `len`: Returns the current queue length

### Streaming Service

The `MediaStreamingService` in `streaming.rs` provides high-level APIs for media streaming:

```rust
pub struct MediaStreamingService {
    // Internal state...
}
```

Key methods:
- `start_file_stream`: Starts streaming from a file
- `start_live_stream`: Starts a live stream
- `get_chunk_stream`: Gets the stream of media chunks
- `get_metrics`: Gets current streaming metrics
- `get_metrics_stream`: Gets a stream of metrics updates
- `shutdown`: Shuts down the streaming service

#### StreamingServiceFactory

Factory for creating pre-configured streaming services:

```rust
impl StreamingServiceFactory {
    pub fn create_live_streaming_service() -> MediaStreamingService { ... }
    pub fn create_file_streaming_service() -> MediaStreamingService { ... }
    pub fn create_low_latency_service() -> MediaStreamingService { ... }
}
```

### Events System

The `MediaStreamEvent` enum in `events.rs` defines events for the media streaming system:

```rust
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
```

These events can be used for monitoring, logging, and integration with other systems.

## Examples

See the [examples directory](../examples/) for complete examples of using the media streaming components.

### Basic File Streaming

See [basic_file_streaming.rs](../examples/basic_file_streaming.rs) for an example of streaming media from a file.

### Live Streaming

See [live_streaming.rs](../examples/live_streaming.rs) for an example of setting up a live stream.

### Custom Codec Configuration

See [custom_codec.rs](../examples/custom_codec.rs) for an example of configuring a custom codec.

### Handling Stream Events

See [stream_events.rs](../examples/stream_events.rs) for an example of handling media stream events.

## API Reference

For detailed API documentation, see the rustdoc comments in the source code:

- [streaming.rs](../src/media/streaming.rs)
- [codec.rs](../src/media/codec.rs)
- [chunk_processor.rs](../src/media/chunk_processor.rs)
- [priority_queue.rs](../src/media/priority_queue.rs)
- [types.rs](../src/media/types.rs)
- [events.rs](../src/media/events.rs)
