//! Main streaming implementation

use super::priority_queue::MediaPriorityQueue;
use super::types::*;
use crate::queue::Queue;
use crate::rs2::*;
use futures_core::Stream;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, BufReader};

pub struct MediaStreamingService {
    chunk_queue: Arc<MediaPriorityQueue>,
    metrics: Arc<tokio::sync::Mutex<StreamMetrics>>,
}

impl MediaStreamingService {
    pub fn new(buffer_capacity: usize) -> Self {
        Self {
            chunk_queue: Arc::new(MediaPriorityQueue::new(buffer_capacity, 64)),
            metrics: Arc::new(tokio::sync::Mutex::new(StreamMetrics {
                stream_id: String::new(),
                bytes_processed: 0,
                chunks_processed: 0,
                dropped_chunks: 0,
                average_chunk_size: 0.0,
                buffer_utilization: 0.0,
                last_updated: chrono::Utc::now(),
            })),
        }
    }

    /// Start streaming from a file
    pub async fn start_file_stream(
        &self,
        file_path: PathBuf,
        stream_config: MediaStream,
    ) -> RS2Stream<MediaChunk> {
        let file = self.acquire_file_resource(file_path).await;
        let chunk_queue = Arc::clone(&self.chunk_queue);
        let metrics = Arc::clone(&self.metrics);

        self.create_chunk_stream(file, stream_config, chunk_queue, metrics)
    }

    /// Start streaming from live input (camera, microphone, etc.)
    pub async fn start_live_stream(
        &self,
        stream_config: MediaStream,
    ) -> RS2Stream<MediaChunk> {
        let chunk_queue = Arc::clone(&self.chunk_queue);
        let metrics = Arc::clone(&self.metrics);

        // Create live stream using from_iter with throttling
        auto_backpressure_drop_newest(
            throttle(
                from_iter(0u64..)
                    .take_rs2(stream_config.metadata.get("max_chunks")
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(u64::MAX as usize))
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
                                m.chunks_processed += 1;
                                m.bytes_processed += chunk.data.len() as u64;
                                m.average_chunk_size = m.bytes_processed as f64 / m.chunks_processed as f64;
                                m.last_updated = chrono::Utc::now();
                            }

                            // Try to enqueue (don't block for live streaming)
                            if let Err(_) = queue.try_enqueue(chunk.clone()).await {
                                let mut m = metrics.lock().await;
                                m.dropped_chunks += 1;
                            }

                            chunk
                        }
                    }),
                std::time::Duration::from_millis(33) // ~30fps
            ),
            512
        )
    }

    async fn acquire_file_resource(&self, path: PathBuf) -> File {
        File::open(&path).await
            .unwrap_or_else(|e| panic!("Failed to open media file {:?}: {}", path, e))
    }

    fn create_chunk_stream(
        &self,
        mut file: File,
        config: MediaStream,
        queue: Arc<MediaPriorityQueue>,
        metrics: Arc<tokio::sync::Mutex<StreamMetrics>>,
    ) -> RS2Stream<MediaChunk> {
        // Use unfold to read file sequentially
        auto_backpressure_block(
            unfold((file, 0u64), move |state| {
                let queue = Arc::clone(&queue);
                let metrics = Arc::clone(&metrics);
                let config = config.clone();

                async move {
                    let (mut file, sequence) = state;

                    // Read chunk from file
                    let mut buffer = vec![0u8; config.chunk_size];
                    match file.read(&mut buffer).await {
                        Ok(0) => {
                            // EOF reached
                            None
                        }
                        Ok(bytes_read) => {
                            // Truncate buffer to actual bytes read
                            buffer.truncate(bytes_read);

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
                                data: buffer,
                                chunk_type,
                                priority,
                                timestamp: std::time::Duration::from_millis(sequence * 33),
                                is_final: bytes_read < config.chunk_size,
                                checksum: None,
                            };

                            // Update metrics inline
                            {
                                let mut m = metrics.lock().await;
                                m.chunks_processed += 1;
                                m.bytes_processed += chunk.data.len() as u64;
                                m.average_chunk_size = m.bytes_processed as f64 / m.chunks_processed as f64;
                                m.last_updated = chrono::Utc::now();
                            }

                            // Enqueue with priority
                            if let Err(_) = queue.enqueue(chunk.clone()).await {
                                let mut m = metrics.lock().await;
                                m.dropped_chunks += 1;
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
            256
        )
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
            MediaPriority::Low  // B-frames are low priority
        } else {
            MediaPriority::Normal // P-frames are normal priority
        }
    }

    /// Update metrics efficiently
    async fn update_metrics(&self, metrics: &Arc<tokio::sync::Mutex<StreamMetrics>>, chunk: &MediaChunk) {
        let mut m = metrics.lock().await;
        m.chunks_processed += 1;
        m.bytes_processed += chunk.data.len() as u64;
        m.average_chunk_size = m.bytes_processed as f64 / m.chunks_processed as f64;
        m.last_updated = chrono::Utc::now();
    }

    /// Get stream from priority queue
    pub fn get_chunk_stream(&self) -> impl Stream<Item = MediaChunk> + Send + 'static {
        self.chunk_queue.dequeue()
    }

    /// Get current metrics with updated buffer utilization
    pub async fn get_metrics(&self) -> StreamMetrics {
        let mut metrics = self.metrics.lock().await;

        // Update buffer utilization based on queue length
        let queue_len = self.chunk_queue.len().await;
        let queue_capacity = 1024; // You might want to store this in the service
        metrics.buffer_utilization = queue_len as f64 / queue_capacity as f64;

        metrics.clone()
    }

    /// Create a metrics monitoring stream
    pub fn get_metrics_stream(&self) -> RS2Stream<StreamMetrics> {
        let metrics = Arc::clone(&self.metrics);
        let chunk_queue = Arc::clone(&self.chunk_queue);

        tick(std::time::Duration::from_secs(1), ())
            .par_eval_map_rs2(1, move |_| {
                let metrics = Arc::clone(&metrics);
                let chunk_queue = Arc::clone(&chunk_queue);

                async move {
                    let mut m = metrics.lock().await;

                    // Update real-time buffer utilization
                    let queue_len = chunk_queue.len().await;
                    m.buffer_utilization = queue_len as f64 / 1024.0;

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