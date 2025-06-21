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
- [Advanced Features](#advanced-features)
  - [Sequence Gap Detection](#sequence-gap-detection)
  - [Buffer Overflow Protection](#buffer-overflow-protection)
  - [Memory Management](#memory-management)
  - [Shared Statistics](#shared-statistics)
  - [Comprehensive Testing](#comprehensive-testing)
- [Examples](#examples)
  - [Basic File Streaming](#basic-file-streaming)
  - [Live Streaming](#live-streaming)
  - [Custom Codec Configuration](#custom-codec-configuration)
  - [Handling Stream Events](#handling-stream-events)
- [API Reference](#api-reference)

## Overview

The RS2 media streaming system provides a robust framework for processing and streaming media content. It supports both file-based and live streaming with features like chunk processing, priority-based delivery, and adaptive quality control.

The system is designed with a modular architecture, allowing components to be used independently or together as a complete streaming solution. Recent improvements include enhanced error handling, memory management, and comprehensive testing coverage.

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

The `MediaCodec` in `codec.rs` handles encoding and decoding of media data with **shared statistics** across all cloned instances:

#### EncodingConfig

Configuration for the codec:

```rust
pub struct EncodingConfig {
    pub quality: QualityLevel,
    pub target_bitrate: u32,
    pub keyframe_interval: u32,
    pub enable_compression: bool,
    pub preserve_metadata: bool,
}
```

Default configuration:

```rust
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
```

#### MediaCodec

The main codec implementation with **shared statistics**:

```rust
pub struct MediaCodec {
    config: EncodingConfig,
    stats: Arc<tokio::sync::Mutex<CodecStats>>, // Shared across all clones
}
```

**Key Features:**
- **Shared Statistics**: All cloned instances share the same stats via `Arc<Mutex<CodecStats>>`
- **Thread-Safe**: Concurrent access to statistics is safe
- **Clone-Friendly**: Cloning preserves shared state for parallel processing

Key methods:
- `encode_stream`: Encodes a stream of raw media data into chunks
- `encode_single_frame`: Encodes a single frame of raw data
- `decode_chunk`: Decodes a media chunk back to raw data
- `get_stats`: Retrieves codec performance statistics
- `reset_stats`: Resets all statistics to zero

#### CodecStats

Comprehensive statistics for monitoring codec performance:

```rust
pub struct CodecStats {
    pub frames_encoded: u64,
    pub frames_decoded: u64,
    pub bytes_processed: u64,
    pub encoding_time_ms: u64,
    pub average_compression_ratio: f64,
    pub error_count: u64,
}
```

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

The `ChunkProcessor` in `chunk_processor.rs` handles the processing pipeline for media chunks with **advanced error handling**:

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

The main chunk processor implementation with **advanced error handling**:

```rust
pub struct ChunkProcessor {
    config: ChunkProcessorConfig,
    codec: Arc<MediaCodec>,
    reorder_buffers: Arc<RwLock<HashMap<String, ReorderBuffer>>>,
    stats: Arc<Mutex<ChunkProcessorStats>>,
    output_queue: Arc<Queue<MediaChunk>>,
}
```

**Advanced Features:**
- **Sequence Gap Detection**: Automatically detects and reports sequence gaps
- **Buffer Overflow Protection**: Prevents memory exhaustion with configurable limits
- **Duplicate Detection**: O(1) duplicate chunk detection using HashSet
- **Periodic Cleanup**: Automatic cleanup of expired buffers to prevent memory leaks
- **Parallel Processing**: Configurable parallel processing with backpressure

Key methods:
- `process_chunk_stream`: Processes a stream of incoming chunks
- `process_single_chunk`: Processes a single chunk
- `validate_chunk`: Validates chunk integrity and format
- `handle_reordering`: Handles chunk reordering with gap detection
- `get_stats`: Retrieves processing statistics
- `create_monitoring_stream`: Creates a real-time monitoring stream

#### ChunkProcessingError

Comprehensive error handling for all processing scenarios:

```rust
pub enum ChunkProcessingError {
    SequenceGap { expected: u64, received: u64 },
    DuplicateChunk(u64),
    BufferOverflow,
    ValidationFailed(String),
    CodecError(String),
    Timeout,
}
```

#### ChunkProcessorStats

Detailed statistics for monitoring chunk processing:

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

**Features:**
- **Priority-Based Ordering**: High-priority chunks (I-frames, audio) delivered first
- **Non-Blocking Operations**: `try_enqueue` for live streaming scenarios
- **Overflow Protection**: Configurable capacity limits with error handling
- **Efficient Sorting**: Binary heap for O(log n) priority operations

Key methods:
- `enqueue`: Adds a chunk to the queue with priority handling
- `dequeue`: Returns a stream of chunks in priority order
- `try_enqueue`: Non-blocking version of enqueue
- `len`: Returns the current queue length

### Streaming Service

The `MediaStreamingService` in `streaming.rs` provides high-level APIs for media streaming:

```rust
pub struct MediaStreamingService {
    chunk_queue: Arc<MediaPriorityQueue>,
    metrics: Arc<tokio::sync::Mutex<StreamMetrics>>,
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

## Advanced Features

### Sequence Gap Detection

The chunk processor automatically detects sequence gaps in incoming chunks:

```rust
// Example: Gap detection in reorder buffer
if seq_num > self.next_expected_sequence + self.max_reorder_window as u64 {
    return Err(ChunkProcessingError::SequenceGap {
        expected: self.next_expected_sequence,
        received: seq_num,
    });
}
```

**Benefits:**
- **Early Error Detection**: Identifies missing chunks before processing
- **Configurable Tolerance**: Adjustable gap size via `max_reorder_window`
- **Stream Isolation**: Each stream has independent sequence tracking
- **Statistics Tracking**: Gap events are counted in processing stats

### Buffer Overflow Protection

Comprehensive buffer management prevents memory exhaustion:

```rust
// Example: Buffer overflow check
if self.buffer.len() >= self.max_size {
    return Err(ChunkProcessingError::BufferOverflow);
}
```

**Features:**
- **Configurable Limits**: Set via `max_buffer_size` and `max_reorder_window`
- **Per-Stream Buffers**: Each stream has independent buffer limits
- **Graceful Degradation**: Overflow errors allow for error handling
- **Memory Efficiency**: Bounded memory usage prevents OOM conditions

### Memory Management

Advanced memory management features prevent memory leaks:

```rust
// Example: Periodic cleanup with statistical sampling
if rand::random::<f32>() < 0.01 { // 1% chance per chunk
    self.cleanup_expired_buffers().await;
}
```

**Features:**
- **Automatic Cleanup**: Expired buffers are automatically flushed
- **Statistical Sampling**: Reduces cleanup overhead while ensuring regular execution
- **Timeout-Based Expiration**: Buffers expire after `sequence_timeout`
- **Memory Leak Prevention**: Comprehensive testing ensures no memory leaks

### Shared Statistics

All codec instances share statistics for accurate monitoring:

```rust
// Example: Shared stats across all clones
pub struct MediaCodec {
    config: EncodingConfig,
    stats: Arc<tokio::sync::Mutex<CodecStats>>, // Shared across clones
}
```

**Benefits:**
- **Accurate Monitoring**: All parallel operations contribute to the same stats
- **Thread-Safe**: Concurrent access is handled safely
- **Clone-Friendly**: Cloning preserves shared state
- **Real-Time Updates**: Statistics update immediately across all instances

### Comprehensive Testing

The media module includes extensive test coverage:

**Test Categories:**
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end workflow testing
- **Performance Tests**: Throughput and latency testing
- **Memory Tests**: Long-running operations with leak detection
- **Error Handling Tests**: All error conditions covered
- **Edge Case Tests**: Boundary conditions and unusual scenarios

**Test Coverage:**
- ✅ **30+ Comprehensive Tests** covering all components
- ✅ **Sequence Gap Detection** with various gap sizes
- ✅ **Buffer Overflow Handling** with configurable limits
- ✅ **Memory Leak Prevention** with 10,000+ chunk processing
- ✅ **Priority Queue Operations** including overflow scenarios
- ✅ **Codec Statistics** with shared state verification
- ✅ **Performance Benchmarks** for throughput optimization
- ✅ **Error Recovery** testing for all error types

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

## Performance Characteristics

### Throughput
- **High-Throughput Processing**: Up to 100+ chunks/second with parallel processing
- **Memory Efficient**: Bounded memory usage with configurable limits
- **Low Latency**: Sub-millisecond processing times for individual chunks

### Scalability
- **Parallel Processing**: Configurable parallelism up to available CPU cores
- **Stream Isolation**: Independent processing per stream
- **Backpressure Handling**: Automatic flow control to prevent overwhelming

### Reliability
- **Error Recovery**: Comprehensive error handling with detailed error types
- **Memory Safety**: Automatic cleanup prevents memory leaks
- **Data Integrity**: Checksum validation and duplicate detection
