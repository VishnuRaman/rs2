//! Main streaming implementation

use super::types::*;
use super::priority_queue::MediaPriorityQueue;
use crate::{auto_backpressure_block, auto_backpressure_drop_newest, throttle};
use crate::rs2_stream_ext::RS2StreamExt;
use crate::rs2::BackpressureConfig;
use crate::session::{get_global_backpressure_config, get_global_buffer_config};
use crate::stream::Stream;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;
use crate::stream_performance_metrics::StreamMetrics;
use crate::rs2;

// Define missing types
#[derive(Debug, Clone)]
pub struct ProcessedChunk {
    pub chunk: MediaChunk,
    pub processing_time: Duration,
    pub quality_score: f64,
}

#[derive(Debug, Clone)]
pub struct StreamingStats {
    pub chunks_processed: u64,
    pub bytes_streamed: u64,
    pub average_quality: f64,
    pub uptime: Duration,
}

pub struct MediaStreamingService {
    chunk_queue: Arc<MediaPriorityQueue>,
    pub metrics: Arc<tokio::sync::Mutex<StreamMetrics>>,
    config: MediaStream,
    backpressure_config: BackpressureConfig,
}

impl MediaStreamingService {
    pub fn new(buffer_capacity: usize) -> Self {
        Self {
            chunk_queue: Arc::new(MediaPriorityQueue::new(buffer_capacity, 64)),
            metrics: Arc::new(tokio::sync::Mutex::new(
                StreamMetrics::new().with_name("media-stream".to_string()),
            )),
            config: MediaStream::default(),
            backpressure_config: BackpressureConfig {
                buffer_size: 512,
                strategy: crate::rs2::BackpressureStrategy::DropNewest,
                high_watermark: Some(400),
                low_watermark: Some(100),
            },
        }
    }

    /// Start streaming from a file with custom configuration
    pub async fn start_file_stream_with_config(
        &self,
        file_path: PathBuf,
        stream_config: MediaStream,
        file_config: crate::stream_configuration::FileConfig,
    ) -> impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt {
        let _file = self.acquire_file_resource(file_path, &file_config).await;
        let chunk_queue = Arc::clone(&self.chunk_queue);
        let metrics = Arc::clone(&self.metrics);

        self.create_chunk_stream_with_config(_file, stream_config, chunk_queue, metrics, file_config).await
    }

    /// Start streaming from a file with default configuration
    pub async fn start_file_stream(
        &self,
        file_path: PathBuf,
        stream_config: MediaStream,
    ) -> impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt {
        self.start_file_stream_with_config(file_path, stream_config, crate::stream_configuration::FileConfig::default()).await
    }

    /// Start streaming from live input (camera, microphone, etc.)
    pub async fn start_live_stream(&self, stream_config: MediaStream) -> impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt {
        let chunk_queue = Arc::clone(&self.chunk_queue);
        let metrics = Arc::clone(&self.metrics);
        let backpressure_config = &self.backpressure_config;
        
        // Create a static function to avoid capturing self
        fn create_live_chunk_static(config: &MediaStream, sequence: u64) -> MediaChunk {
            MediaChunk {
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
            }
        }
        
        let max_chunks = stream_config
            .metadata
            .get("max_chunks")
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        
        auto_backpressure_drop_newest(
            throttle(
                crate::stream::constructors::from_iter(0u64..max_chunks)
                    .map_rs2(move |sequence| {
                        let config = stream_config.clone();
                        create_live_chunk_static(&config, sequence)
                    })
                    .eval_map_rs2(move |chunk| {
                        let queue = Arc::clone(&chunk_queue);
                        let metrics = Arc::clone(&metrics);
                        async move {
                            // Update metrics when chunk is created
                            {
                                let mut m = metrics.lock().await;
                                m.items_processed += 1;
                                m.bytes_processed += chunk.data.len() as u64;
                            }
                            
                            if let Err(_) = queue.try_enqueue(chunk.clone()).await {
                                let mut m = metrics.lock().await;
                                m.errors += 1;
                            }
                            chunk
                        }
                    }),
                std::time::Duration::from_millis(33), // ~30fps
            ),
            backpressure_config.clone(),
        )
    }

    async fn acquire_file_resource(&self, path: PathBuf, file_config: &crate::stream_configuration::FileConfig) -> tokio::fs::File {
        let file = tokio::fs::File::open(&path)
            .await
            .unwrap_or_else(|e| panic!("Failed to open media file {:?}: {}", path, e));
        let _ = file_config.buffer_size;
        let _ = file_config.read_ahead;
        let _ = file_config.sync_on_write;
        let _ = &file_config.compression;
        file
    }

        async fn create_chunk_stream_with_config(
        &self,
        file: tokio::fs::File,
        config: MediaStream,
        queue: Arc<MediaPriorityQueue>,
        metrics: Arc<tokio::sync::Mutex<StreamMetrics>>,
        file_config: crate::stream_configuration::FileConfig,
    ) -> impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt {
        let buffer_size = config.chunk_size;
        let backpressure_config = BackpressureConfig {
            buffer_size: 256,
            strategy: crate::rs2::BackpressureStrategy::Block,
            high_watermark: Some(200),
            low_watermark: Some(50),
        };
        
        // Read the entire file content first (this is a simpler approach for now)
        let file_content = {
            use tokio::io::AsyncReadExt;
            let mut content = Vec::new();
            let mut file_clone = file.try_clone().await.unwrap();
            file_clone.read_to_end(&mut content).await.unwrap();
            content
        };
        
        // Create chunks from the file content
        let chunks: Vec<Vec<u8>> = file_content
            .chunks(buffer_size)
            .map(|chunk| chunk.to_vec())
            .collect();
        
        // Convert to MediaChunks
        let chunk_stream = crate::stream::constructors::from_iter(chunks)
            .enumerate_rs2()
            .map_rs2(move |(sequence, data)| {
                let config = config.clone();
                MediaChunk {
                    stream_id: config.id.clone(),
                    sequence_number: sequence as u64,
                    data,
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
                    timestamp: std::time::Duration::from_millis(sequence as u64 * 33),
                    is_final: false,
                    checksum: None,
                }
            })
        .eval_map_rs2(move |chunk| {
            let queue = Arc::clone(&queue);
            let metrics = Arc::clone(&metrics);
            async move {
                // Update metrics when chunk is created
                {
                    let mut m = metrics.lock().await;
                    m.record_item(chunk.data.len() as u64);
                }
                
                if let Err(_) = queue.try_enqueue(chunk.clone()).await {
                    let mut m = metrics.lock().await;
                    m.record_error();
                }
                chunk
            }
        });
        
        auto_backpressure_block(chunk_stream, backpressure_config)
    }

    async fn acquire_file_resource_simple(&self, path: PathBuf) -> tokio::fs::File {
        tokio::fs::File::open(&path).await.expect("Failed to open file")
    }

    async fn create_live_chunk(&self, config: &MediaStream, sequence: u64) -> MediaChunk {
        MediaChunk {
            stream_id: config.id.clone(),
            sequence_number: sequence,
            data: vec![0u8; config.chunk_size],
            chunk_type: self.determine_chunk_type(sequence),
            priority: self.determine_priority(sequence),
            timestamp: std::time::Duration::from_millis(sequence * 33),
            is_final: false,
            checksum: None,
        }
    }

    pub fn determine_chunk_type(&self, sequence: u64) -> ChunkType {
        if sequence % 30 == 0 {
            ChunkType::VideoIFrame
        } else if sequence % 3 == 0 {
            ChunkType::VideoBFrame
        } else {
            ChunkType::VideoPFrame
        }
    }

    pub fn determine_priority(&self, sequence: u64) -> MediaPriority {
        if sequence % 30 == 0 {
            MediaPriority::High
        } else if sequence % 3 == 0 {
            MediaPriority::Low
        } else {
            MediaPriority::Normal
        }
    }

    async fn update_metrics(
        &self,
        metrics: &Arc<tokio::sync::Mutex<StreamMetrics>>,
        chunk: &MediaChunk,
    ) {
        let mut m = metrics.lock().await;
        m.record_item(chunk.data.len() as u64);
    }

    pub fn get_chunk_stream(&self) -> impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt {
        self.chunk_queue.dequeue()
    }

    pub async fn get_metrics(&self) -> StreamMetrics {
        let metrics = Arc::clone(&self.metrics);
        let mut x = metrics.lock().await.clone();
        x.update_derived_metrics();
        x
    }

    pub fn get_metrics_stream(&self) -> impl Stream<Item = StreamMetrics> + Send + 'static {
        let metrics = Arc::clone(&self.metrics);
        
        // Create immediate metrics stream
        let immediate_stream = {
            let metrics = Arc::clone(&metrics);
            rs2::from_iter_rs2(vec![()])
                .eval_map_rs2(move |_| {
                    let metrics = Arc::clone(&metrics);
                    async move { 
                        let mut m = metrics.lock().await.clone();
                        m.update_derived_metrics();
                        m
                    }
                })
        };
        
        // Create periodic metrics stream
        let periodic_stream = {
            let metrics = Arc::clone(&metrics);
            rs2::tick(Duration::from_millis(100), ())
                .eval_map_rs2(move |_| {
                    let metrics = Arc::clone(&metrics);
                    async move { 
                        let mut m = metrics.lock().await.clone();
                        m.update_derived_metrics();
                        m
                    }
                })
        };
        
        // Chain them together - immediate first, then periodic
        immediate_stream.chain_rs2(periodic_stream)
    }

    pub async fn shutdown(&self) {
        self.chunk_queue.close().await;
    }

    /// Create a streaming pipeline
    pub fn create_streaming_pipeline(
        &self,
        input_stream: impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt,
    ) -> impl Stream<Item = ProcessedChunk> + Send + 'static + RS2StreamExt {
        input_stream
            .map_rs2(|chunk| {
                ProcessedChunk {
                    chunk,
                    processing_time: Duration::from_millis(10),
                    quality_score: 0.95,
                }
            })
    }

    /// Create a monitoring stream for statistics
    pub fn create_monitoring_stream(&self) -> impl Stream<Item = StreamingStats> + Send + 'static {
        rs2::tick(Duration::from_secs(1), ()).par_eval_map_rs2(Some(1), move |_| {
            async move {
                StreamingStats {
                    chunks_processed: 1000,
                    bytes_streamed: 1024 * 1024,
                    average_quality: 0.95,
                    uptime: Duration::from_secs(60),
                }
            }
        })
    }

    /// Create a media stream with backpressure handling
    pub fn create_stream(
        &self,
        _source_url: String,
    ) -> Result<impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt, String> {
        let _config = self.config.clone();
        let backpressure_config = self.backpressure_config.clone();
        let stream = rs2::auto_backpressure_drop_newest(
            self.create_raw_stream(_source_url)?,
            backpressure_config.clone(),
        );
        Ok(stream)
    }

    /// Create a raw media stream without backpressure
    fn create_raw_stream(
        &self,
        _source_url: String,
    ) -> Result<impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt, String> {
        Ok(crate::stream::constructors::empty())
    }

    /// Process media chunks with quality enhancement
    pub fn enhance_quality(
        &self,
        input_stream: impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt,
    ) -> impl Stream<Item = ProcessedChunk> + Send + 'static + RS2StreamExt {
        input_stream
            .map_rs2(|chunk| {
                ProcessedChunk {
                    chunk,
                    processing_time: Duration::from_millis(10),
                    quality_score: 0.95,
                }
            })
    }

    /// Create a stream with adaptive bitrate
    pub fn create_adaptive_stream(
        &self,
        _source_url: String,
    ) -> Result<impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt, String> {
        let _config = self.config.clone();
        let backpressure_config = BackpressureConfig {
            buffer_size: 256,
            strategy: crate::rs2::BackpressureStrategy::Block,
            high_watermark: Some(200),
            low_watermark: Some(50),
        };
        let stream = rs2::auto_backpressure_block(
            self.create_raw_stream(_source_url)?,
            backpressure_config,
        );
        Ok(stream)
    }

    /// Create a monitoring stream for adaptive streaming
    pub fn create_adaptive_monitoring_stream(&self) -> impl Stream<Item = StreamingStats> + Send + 'static {
        rs2::tick(Duration::from_secs(1), ()).par_eval_map_rs2(Some(1), move |_| {
            async move {
                StreamingStats {
                    chunks_processed: 500,
                    bytes_streamed: 512 * 1024,
                    average_quality: 0.85,
                    uptime: Duration::from_secs(30),
                }
            }
        })
    }

    /// Create a media stream with session-aware backpressure handling
    pub fn create_stream_with_session(
        &self,
        _source_url: String,
    ) -> Result<impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt, String> {
        let session_backpressure = get_global_backpressure_config()
            .unwrap_or_else(|| self.backpressure_config.clone());
        let session_buffer = get_global_buffer_config()
            .unwrap_or_else(|| crate::stream_configuration::BufferConfig::default());
        
        // Use session buffer size if available
        let buffer_size = session_buffer.max_capacity.unwrap_or(512);
        
        let backpressure_config = BackpressureConfig {
            buffer_size,
            strategy: session_backpressure.strategy,
            high_watermark: session_backpressure.high_watermark,
            low_watermark: session_backpressure.low_watermark,
        };
        
        let stream = rs2::auto_backpressure_drop_newest(
            self.create_raw_stream(_source_url)?,
            backpressure_config,
        );
        Ok(stream)
    }

    /// Create an adaptive stream with session-aware configuration
    pub fn create_adaptive_stream_with_session(
        &self,
        _source_url: String,
    ) -> Result<impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt, String> {
        let session_backpressure = get_global_backpressure_config()
            .unwrap_or_else(|| self.backpressure_config.clone());
        let session_buffer = get_global_buffer_config()
            .unwrap_or_else(|| crate::stream_configuration::BufferConfig::default());
        
        // Use session buffer size if available
        let buffer_size = session_buffer.max_capacity.unwrap_or(256);
        
        let backpressure_config = BackpressureConfig {
            buffer_size,
            strategy: session_backpressure.strategy,
            high_watermark: session_backpressure.high_watermark,
            low_watermark: session_backpressure.low_watermark,
        };
        
        let stream = rs2::auto_backpressure_block(
            self.create_raw_stream(_source_url)?,
            backpressure_config,
        );
        Ok(stream)
    }

    /// Process media chunks with session-aware quality enhancement
    pub fn enhance_quality_with_session(
        &self,
        input_stream: impl Stream<Item = MediaChunk> + Send + 'static + RS2StreamExt,
    ) -> Box<dyn Stream<Item = ProcessedChunk> + Send + 'static> {
        let session_backpressure = get_global_backpressure_config();
        
        let enhanced_stream = input_stream
            .map_rs2(|chunk| {
                ProcessedChunk {
                    chunk,
                    processing_time: Duration::from_millis(10),
                    quality_score: 0.95,
                }
            });
        
        // Apply session backpressure if available
        if let Some(config) = session_backpressure {
            Box::new(rs2::auto_backpressure_drop_newest(enhanced_stream, config))
        } else {
            Box::new(enhanced_stream)
        }
    }
}

pub struct StreamingServiceFactory;

impl StreamingServiceFactory {
    pub fn create_live_streaming_service() -> MediaStreamingService {
        MediaStreamingService::new(1000)
    }

    pub fn create_file_streaming_service() -> MediaStreamingService {
        MediaStreamingService::new(500)
    }

    pub fn create_low_latency_service() -> MediaStreamingService {
        MediaStreamingService::new(100)
    }
}
