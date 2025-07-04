//! Main streaming implementation

use super::priority_queue::MediaPriorityQueue;
use super::types::*;
use crate::stream_performance_metrics::StreamMetrics;
use crate::{auto_backpressure_block, tick, unfold};
use crate::{auto_backpressure_drop_newest, from_iter, throttle, RS2Stream, RS2StreamExt};
use futures_core::Stream;
use std::path::PathBuf;
use std::sync::Arc;
use std::io::Write;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub struct MediaStreamingService {
    chunk_queue: Arc<MediaPriorityQueue>,
    metrics: Arc<tokio::sync::Mutex<StreamMetrics>>,
}

impl MediaStreamingService {
    pub fn new(buffer_capacity: usize) -> Self {
        Self {
            chunk_queue: Arc::new(MediaPriorityQueue::new(buffer_capacity, 64)),
            metrics: Arc::new(tokio::sync::Mutex::new(
                StreamMetrics::new().with_name("media-stream".to_string()),
            )),
        }
    }

    /// Start streaming from a file with custom configuration
    pub async fn start_file_stream_with_config(
        &self,
        file_path: PathBuf,
        stream_config: MediaStream,
        file_config: crate::stream_configuration::FileConfig,
    ) -> RS2Stream<MediaChunk> {
        let file = self.acquire_file_resource(file_path, &file_config).await;
        let chunk_queue = Arc::clone(&self.chunk_queue);
        let metrics = Arc::clone(&self.metrics);

        self.create_chunk_stream_with_config(file, stream_config, chunk_queue, metrics, file_config)
    }

    /// Start streaming from a file with default configuration
    pub async fn start_file_stream(
        &self,
        file_path: PathBuf,
        stream_config: MediaStream,
    ) -> RS2Stream<MediaChunk> {
        self.start_file_stream_with_config(file_path, stream_config, crate::stream_configuration::FileConfig::default()).await
    }

    /// Start streaming from live input (camera, microphone, etc.)
    pub async fn start_live_stream(&self, stream_config: MediaStream) -> RS2Stream<MediaChunk> {
        let chunk_queue = Arc::clone(&self.chunk_queue);
        let metrics = Arc::clone(&self.metrics);

        // Create live stream using from_iter with throttling
        auto_backpressure_drop_newest(
            throttle(
                from_iter(0u64..)
                    .take_rs2(
                        stream_config
                            .metadata
                            .get("max_chunks")
                            .and_then(|s| s.parse().ok())
                            .unwrap_or(u64::MAX as usize),
                    )
                    .par_eval_map_rs2(4, move |sequence| {
                        let queue = Arc::clone(&chunk_queue);
                        let metrics = Arc::clone(&metrics);
                        let config = stream_config.clone();

                        async move {
                            // Simulate live capture - create chunk directly here
                            tokio::time::sleep(std::time::Duration::from_micros(100)).await;

                            let chunk = MediaChunk {
                                stream_id: config.id.clone(),
                                sequence_number: sequence,
                                data: vec![0u8; config.chunk_size],
                                chunk_type: if sequence % 30 == 0 {
                                    ChunkType::VideoIFrame
                                } else if sequence % 3 == 0 {
                                    ChunkType::VideoBFrame
                                } else {
                                    ChunkType::VideoPFrame
                                },
                                priority: if sequence % 30 == 0 {
                                    MediaPriority::High
                                } else if sequence % 3 == 0 {
                                    MediaPriority::Low
                                } else {
                                    MediaPriority::Normal
                                },
                                timestamp: std::time::Duration::from_millis(sequence * 33),
                                is_final: false,
                                checksum: None,
                            };

                            // Update metrics
                            {
                                let mut m = metrics.lock().await;
                                m.items_processed += 1;
                                m.bytes_processed += chunk.data.len() as u64;
                                m.average_item_size =
                                    m.bytes_processed as f64 / m.items_processed as f64;
                                m.last_activity = Some(std::time::Instant::now());
                            }

                            // Try to enqueue (don't block for live streaming)
                            if let Err(_) = queue.try_enqueue(chunk.clone()).await {
                                let mut m = metrics.lock().await;
                                m.errors += 1;
                            }

                            chunk
                        }
                    }),
                std::time::Duration::from_millis(33), // ~30fps
            ),
            512,
        )
    }

    async fn acquire_file_resource(&self, path: PathBuf, file_config: &crate::stream_configuration::FileConfig) -> File {
        // Use file_config for optimized file opening
        let file = File::open(&path)
            .await
            .unwrap_or_else(|e| panic!("Failed to open media file {:?}: {}", path, e));
        
        // In a real implementation, we would:
        // - Set buffer size based on file_config.buffer_size
        // - Enable read-ahead if file_config.read_ahead is true
        // - Configure compression based on file_config.compression
        // For now, we acknowledge the config fields
        let _ = file_config.buffer_size;
        let _ = file_config.read_ahead;
        let _ = file_config.sync_on_write;
        let _ = &file_config.compression;
        
        file
    }

    fn create_chunk_stream_with_config(
        &self,
        file: File,
        config: MediaStream,
        queue: Arc<MediaPriorityQueue>,
        metrics: Arc<tokio::sync::Mutex<StreamMetrics>>,
        file_config: crate::stream_configuration::FileConfig,
    ) -> RS2Stream<MediaChunk> {
        // Use file_config.buffer_size for reading chunks
        let buffer_size = file_config.buffer_size.max(config.chunk_size);
        
        // Use unfold to read file sequentially with custom buffer size
        auto_backpressure_block(
            unfold((file, 0u64), move |state| {
                let queue = Arc::clone(&queue);
                let metrics = Arc::clone(&metrics);
                let config = config.clone();
                let file_config = file_config.clone();

                async move {
                    let (mut file, sequence) = state;

                    // Use buffer_size from file_config
                    let chunk_size = if file_config.read_ahead {
                        // Read ahead with larger buffer
                        (config.chunk_size * 2).min(buffer_size)
                    } else {
                        config.chunk_size.min(buffer_size)
                    };

                    // Read chunk from file
                    let mut buffer = vec![0u8; chunk_size];
                    match file.read(&mut buffer).await {
                        Ok(0) => {
                            // EOF reached
                            None
                        }
                        Ok(bytes_read) => {
                            // Truncate buffer to actual bytes read
                            buffer.truncate(bytes_read);

                            // Apply compression if configured
                            let final_data = match &file_config.compression {
                                Some(crate::stream_configuration::CompressionType::Gzip) => {
                                    // Compress with gzip
                                    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                                    match encoder.write_all(&buffer).and_then(|_| encoder.finish()) {
                                        Ok(compressed) => compressed,
                                        Err(_) => {
                                            log::warn!("Failed to compress with gzip, using uncompressed data");
                                            buffer
                                        }
                                    }
                                }
                                Some(crate::stream_configuration::CompressionType::Deflate) => {
                                    // Compress with deflate
                                    let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
                                    match encoder.write_all(&buffer).and_then(|_| encoder.finish()) {
                                        Ok(compressed) => compressed,
                                        Err(_) => {
                                            log::warn!("Failed to compress with deflate, using uncompressed data");
                                            buffer
                                        }
                                    }
                                }
                                Some(crate::stream_configuration::CompressionType::Lz4) => {
                                    // Compress with LZ4
                                    match lz4::block::compress(&buffer, None, false) {
                                        Ok(compressed) => compressed,
                                        Err(_) => {
                                            log::warn!("Failed to compress with LZ4, using uncompressed data");
                                            buffer
                                        }
                                    }
                                }
                                None => buffer,
                            };

                            // Determine chunk type inline
                            let chunk_type = if sequence % 30 == 0 {
                                ChunkType::VideoIFrame
                            } else if sequence % 3 == 0 {
                                ChunkType::VideoBFrame
                            } else {
                                ChunkType::VideoPFrame
                            };

                            // Determine priority inline
                            let priority = if sequence % 30 == 0 {
                                MediaPriority::High
                            } else if sequence % 3 == 0 {
                                MediaPriority::Low
                            } else {
                                MediaPriority::Normal
                            };

                            let chunk = MediaChunk {
                                stream_id: config.id.clone(),
                                sequence_number: sequence,
                                data: final_data,
                                chunk_type,
                                priority,
                                timestamp: std::time::Duration::from_millis(sequence * 33),
                                is_final: bytes_read < chunk_size,
                                checksum: None,
                            };

                            // Update metrics inline
                            {
                                let mut m = metrics.lock().await;
                                m.items_processed += 1;
                                m.bytes_processed += chunk.data.len() as u64;
                                m.average_item_size =
                                    m.bytes_processed as f64 / m.items_processed as f64;
                                m.last_activity = Some(std::time::Instant::now());
                            }

                            // Enqueue with priority
                            if let Err(_) = queue.enqueue(chunk.clone()).await {
                                let mut m = metrics.lock().await;
                                m.errors += 1;
                            }

                            Some((chunk, (file, sequence + 1)))
                        }
                        Err(e) => {
                            log::error!("Error reading file: {}", e);
                            None
                        }
                    }
                }
            }),
            256,
        )
    }

    fn create_chunk_stream(
        &self,
        file: File,
        config: MediaStream,
        queue: Arc<MediaPriorityQueue>,
        metrics: Arc<tokio::sync::Mutex<StreamMetrics>>,
    ) -> RS2Stream<MediaChunk> {
        // Use default file config for backward compatibility
        self.create_chunk_stream_with_config(file, config, queue, metrics, crate::stream_configuration::FileConfig::default())
    }

    // Backward compatibility method for acquire_file_resource
    async fn acquire_file_resource_simple(&self, path: PathBuf) -> File {
        self.acquire_file_resource(path, &crate::stream_configuration::FileConfig::default()).await
    }

    /// Create a chunk for live streaming
    async fn create_live_chunk(&self, config: &MediaStream, sequence: u64) -> MediaChunk {
        // Simulate capturing from live source
        tokio::time::sleep(std::time::Duration::from_micros(100)).await;

        MediaChunk {
            stream_id: config.id.clone(),
            sequence_number: sequence,
            data: vec![0u8; config.chunk_size], // Mock data - replace with actual capture
            chunk_type: self.determine_chunk_type(sequence),
            priority: self.determine_priority(sequence),
            timestamp: std::time::Duration::from_millis(sequence * 33),
            is_final: false, // Live streams don't end
            checksum: None,
        }
    }

    /// Determine chunk type based on sequence
    pub fn determine_chunk_type(&self, sequence: u64) -> ChunkType {
        if sequence % 30 == 0 {
            ChunkType::VideoIFrame // Keyframe every 30 frames
        } else if sequence % 3 == 0 {
            ChunkType::VideoBFrame // B-frame every 3rd frame
        } else {
            ChunkType::VideoPFrame // P-frame otherwise
        }
    }

    /// Determine priority based on sequence and chunk type
    pub fn determine_priority(&self, sequence: u64) -> MediaPriority {
        if sequence % 30 == 0 {
            MediaPriority::High // I-frames are high priority
        } else if sequence % 3 == 0 {
            MediaPriority::Low // B-frames are low priority
        } else {
            MediaPriority::Normal // P-frames are normal priority
        }
    }

    /// Update metrics efficiently
    async fn update_metrics(
        &self,
        metrics: &Arc<tokio::sync::Mutex<StreamMetrics>>,
        chunk: &MediaChunk,
    ) {
        let mut m = metrics.lock().await;
        m.items_processed += 1;
        m.bytes_processed += chunk.data.len() as u64;
        m.average_item_size = m.bytes_processed as f64 / m.items_processed as f64;
        m.last_activity = Some(std::time::Instant::now());
    }

    /// Get stream from priority queue
    pub fn get_chunk_stream(&self) -> impl Stream<Item = MediaChunk> + Send + 'static {
        self.chunk_queue.dequeue()
    }

    /// Get current metrics with updated buffer utilization
    pub async fn get_metrics(&self) -> StreamMetrics {
        let metrics = self.metrics.lock().await;
        metrics.clone()
    }

    /// Create a metrics monitoring stream
    pub fn get_metrics_stream(&self) -> RS2Stream<StreamMetrics> {
        let metrics = Arc::clone(&self.metrics);

        tick(std::time::Duration::from_secs(1), ()).par_eval_map_rs2(1, move |_| {
            let metrics = Arc::clone(&metrics);

            async move {
                let m = metrics.lock().await;
                m.clone()
            }
        })
    }

    /// Gracefully shutdown the streaming service
    pub async fn shutdown(&self) {
        log::info!("Shutting down media streaming service");
        self.chunk_queue.close().await;
    }
}

/// Factory for creating different types of streaming services
pub struct StreamingServiceFactory;

impl StreamingServiceFactory {
    /// Create service optimized for live streaming
    pub fn create_live_streaming_service() -> MediaStreamingService {
        MediaStreamingService::new(2048) // Larger buffer for live
    }

    /// Create service optimized for file streaming
    pub fn create_file_streaming_service() -> MediaStreamingService {
        MediaStreamingService::new(512) // Smaller buffer for files
    }

    /// Create service optimized for low-latency streaming
    pub fn create_low_latency_service() -> MediaStreamingService {
        MediaStreamingService::new(128) // Very small buffer for low latency
    }
}
