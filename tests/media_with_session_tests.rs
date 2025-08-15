use rs2_stream::rs2::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::media::chunk_processor::{ChunkProcessor, ChunkProcessorConfig};
use rs2_stream::media::codec::{EncodingConfig, MediaCodec};
use rs2_stream::media::types::{MediaChunk, ChunkType, MediaPriority};
use rs2_stream::media::streaming::MediaStreamingService;
use rs2_stream::queue::Queue;
use rs2_stream::session::{SessionBuilder, SessionPreset, set_global_session, clear_global_session, get_global_parallel_config, get_global_buffer_config, get_global_backpressure_config};
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tokio;



#[tokio::test]
#[serial]
async fn test_chunk_processor_with_session() {
    // Set up a session with custom chunk processor configuration
    let session_config = SessionBuilder::new()
        .chunk_processor(|c| {
            c.max_buffer_size = 2048;
            c.parallel_processing = 4;
            c.max_retries = 3;
        })
        .parallel(|p| {
            p.concurrency = 8;
            p.max_buffer_size = 4096;
        })
        .stream_buffer(|b| {
            b.initial_capacity = 1024;
            b.max_capacity = Some(2 * 1024 * 1024); // 2MB
        })
        .build();
    
    set_global_session(session_config);

    let codec = Arc::new(MediaCodec::new(EncodingConfig::default()));
    let output_queue = Arc::new(Queue::<MediaChunk>::bounded(100));
    let processor = ChunkProcessor::new(ChunkProcessorConfig::default(), codec, output_queue);
    let chunks = vec![
        create_test_chunk("test_stream", 1, ChunkType::VideoIFrame),
        create_test_chunk("test_stream", 2, ChunkType::VideoPFrame),
        create_test_chunk("test_stream", 3, ChunkType::VideoBFrame),
    ];

    let chunk_stream = from_iter_rs2(chunks);
    let processed_stream = processor.process_chunks_with_session(chunk_stream);

    let results: Vec<MediaChunk> = processed_stream.collect_rs2().await;
    assert_eq!(results.len(), 3);
    
    // Verify that chunks were processed
    assert_eq!(results[0].sequence_number, 1);
    assert_eq!(results[1].sequence_number, 2);
    assert_eq!(results[2].sequence_number, 3);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_media_streaming_with_session() {
    // Set up a session with custom backpressure and buffer configuration
    let session_config = SessionBuilder::new()
        .backpressure(|b| {
            b.strategy = rs2_stream::BackpressureStrategy::Block;
            b.buffer_size = 1000;
            b.low_watermark = Some(200);
            b.high_watermark = Some(800);
        })
        .stream_buffer(|b| {
            b.initial_capacity = 512;
            b.max_capacity = Some(1024 * 1024); // 1MB
        })
        .build();
    
    set_global_session(session_config);

    let service = MediaStreamingService::new(1000);
    let chunks = vec![
        create_test_chunk("test_stream", 1, ChunkType::VideoIFrame),
        create_test_chunk("test_stream", 2, ChunkType::VideoPFrame),
    ];

    let chunk_stream = from_iter_rs2(chunks);
    let enhanced_stream = service.enhance_quality_with_session(chunk_stream);

    let results: Vec<rs2_stream::media::streaming::ProcessedChunk> = enhanced_stream.collect_rs2().await;
    assert_eq!(results.len(), 2);
    
    // Verify that chunks were processed
    assert_eq!(results[0].chunk.sequence_number, 1);
    assert_eq!(results[1].chunk.sequence_number, 2);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_session_aware_chunk_processor() {
    // Set up a session with custom configuration
    let session_config = SessionBuilder::new()
        .chunk_processor(|c| {
            c.max_buffer_size = 1024;
            c.parallel_processing = 2;
            c.max_retries = 1;
        })
        .parallel(|p| {
            p.concurrency = 4;
            p.max_buffer_size = 2048;
        })
        .build();
    
    set_global_session(session_config);

    let codec = Arc::new(MediaCodec::new(EncodingConfig::default()));
    let output_queue = Arc::new(Queue::<MediaChunk>::bounded(100));
    let processor = ChunkProcessor::new(ChunkProcessorConfig::default(), codec, output_queue);
    let session_aware_processor = processor.with_session_config();
    
    let chunks = vec![
        create_test_chunk("test_stream", 1, ChunkType::VideoIFrame),
        create_test_chunk("test_stream", 2, ChunkType::VideoPFrame),
    ];

    let chunk_stream = from_iter_rs2(chunks);
    let processed_stream = session_aware_processor.process_chunks(chunk_stream);

    let results: Vec<MediaChunk> = processed_stream.collect_rs2().await;
    assert_eq!(results.len(), 2);
    
    // Verify that chunks were processed
    assert_eq!(results[0].sequence_number, 1);
    assert_eq!(results[1].sequence_number, 2);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_media_streaming_adaptive_with_session() {
    // Set up a session with custom configuration
    let session_config = SessionBuilder::new()
        .backpressure(|b| {
            b.strategy = rs2_stream::BackpressureStrategy::DropNewest;
            b.buffer_size = 500;
            b.low_watermark = Some(100);
            b.high_watermark = Some(400);
        })
        .stream_buffer(|b| {
            b.initial_capacity = 256;
            b.max_capacity = Some(512 * 1024); // 512KB
        })
        .build();
    
    set_global_session(session_config);

    let service = MediaStreamingService::new(1000);
    
    // Create test chunks to process
    let chunks = vec![
        create_test_chunk("test_stream", 1, ChunkType::VideoIFrame),
        create_test_chunk("test_stream", 2, ChunkType::VideoPFrame),
        create_test_chunk("test_stream", 3, ChunkType::VideoBFrame),
    ];

    let chunk_stream = from_iter_rs2(chunks);
    
    // Process the chunks through the session-aware adaptive stream
    let adaptive_stream = service.enhance_quality_with_session(chunk_stream);

    let results: Vec<rs2_stream::media::streaming::ProcessedChunk> = adaptive_stream.collect_rs2().await;
    assert!(!results.is_empty());
    
    // Verify that chunks were processed
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].chunk.sequence_number, 1);
    assert_eq!(results[1].chunk.sequence_number, 2);
    assert_eq!(results[2].chunk.sequence_number, 3);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_session_config_fallback() {
    // Don't set global session - should use defaults
    let codec = Arc::new(MediaCodec::new(EncodingConfig::default()));
    let output_queue = Arc::new(Queue::<MediaChunk>::bounded(100));
    let processor = ChunkProcessor::new(ChunkProcessorConfig::default(), codec, output_queue);
    let chunks = vec![
        create_test_chunk("test_stream", 1, ChunkType::VideoIFrame),
    ];

    let chunk_stream = from_iter_rs2(chunks);
    let processed_stream = processor.process_chunks_with_session(chunk_stream);

    let results: Vec<MediaChunk> = processed_stream.collect_rs2().await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].sequence_number, 1);
}

#[tokio::test]
#[serial]
async fn test_session_presets() {
    // Test with development preset
    let dev_session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    
    set_global_session(dev_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    // Development preset should have lower concurrency
    assert_eq!(parallel_config.concurrency, 2);

    let buffer_config = get_global_buffer_config();
    assert!(buffer_config.is_some());
    let buffer_config = buffer_config.unwrap();
    
    // Development preset should have reasonable buffer settings
    assert!(buffer_config.initial_capacity > 0);

    // Clear the session before testing production preset
    clear_global_session();

    // Test with production preset
    let prod_session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .build();
    
    set_global_session(prod_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    // Production preset should have higher concurrency
    assert_eq!(parallel_config.concurrency, 16);

    let backpressure_config = get_global_backpressure_config();
    assert!(backpressure_config.is_some());
    let backpressure_config = backpressure_config.unwrap();
    
    // Production preset should use blocking strategy
    assert_eq!(backpressure_config.strategy, rs2_stream::BackpressureStrategy::Block);
    
    // Clear the session at the end of the test
    clear_global_session();
}

#[tokio::test]
#[serial]
async fn test_custom_session_configuration() {
    // Test with custom configuration
    let custom_session = SessionBuilder::new()
        .chunk_processor(|c| {
            c.max_buffer_size = 8192;
            c.parallel_processing = 16;
            c.max_retries = 5;
        })
        .parallel(|p| {
            p.concurrency = 32;
            p.max_buffer_size = 16384;
            p.task_timeout = Duration::from_secs(120);
        })
        .backpressure(|b| {
            b.strategy = rs2_stream::BackpressureStrategy::DropOldest;
            b.buffer_size = 2000;
            b.low_watermark = Some(500);
            b.high_watermark = Some(1500);
        })
        .build();
    
    set_global_session(custom_session);

    let parallel_config = get_global_parallel_config();
    assert!(parallel_config.is_some());
    let parallel_config = parallel_config.unwrap();
    
    assert_eq!(parallel_config.concurrency, 32);
    assert_eq!(parallel_config.max_buffer_size, 16384);
    assert_eq!(parallel_config.task_timeout, Duration::from_secs(120));

    let backpressure_config = get_global_backpressure_config();
    assert!(backpressure_config.is_some());
    let backpressure_config = backpressure_config.unwrap();
    
    assert_eq!(backpressure_config.strategy, rs2_stream::BackpressureStrategy::DropOldest);
    assert_eq!(backpressure_config.buffer_size, 2000);
    assert_eq!(backpressure_config.low_watermark, Some(500));
    assert_eq!(backpressure_config.high_watermark, Some(1500));
    
    // Clear the session at the end of the test
    clear_global_session();
}

// Helper function to create test chunks
fn create_test_chunk(stream_id: &str, sequence: u64, chunk_type: ChunkType) -> MediaChunk {
    MediaChunk {
        stream_id: stream_id.to_string(),
        sequence_number: sequence,
        data: vec![0u8; 1024], // 1KB of test data
        chunk_type,
        priority: MediaPriority::Normal,
        timestamp: Duration::from_millis(sequence * 33),
        is_final: false,
        checksum: None,
    }
} 