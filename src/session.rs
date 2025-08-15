use std::time::Duration;
use crate::stream::parallel::ParallelConfig;
use crate::BackpressureConfig;
use crate::BackpressureStrategy;
use crate::state::config::StateConfig;
use crate::stream_configuration::CompressionType;
use crate::media::chunk_processor::ChunkProcessorConfig;
use crate::media::codec::EncodingConfig;
use crate::resource_manager::ResourceConfig;
use crate::pipeline::builder::PipelineConfig;
use crate::stream_configuration::{BufferConfig as StreamBufferConfig, FileConfig as StreamFileConfig, MetricsConfig};
use crate::advanced_analytics::TimeWindowConfig;

/// Media processing configuration
#[derive(Debug, Clone)]
pub struct MediaConfig {
    /// Compression type for media files
    pub compression: Option<CompressionType>,
    /// Quality setting for compression
    pub quality: u8,
    /// Whether to use hardware acceleration
    pub hardware_acceleration: bool,
}

impl Default for MediaConfig {
    fn default() -> Self {
        Self {
            compression: None,
            quality: 80,
            hardware_acceleration: false,
        }
    }
}

/// Global session configuration for RS2 stream processing
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Parallel processing configuration
    pub parallel: ParallelConfig,
    /// Backpressure configuration
    pub backpressure: BackpressureConfig,
    /// State management configuration
    pub state: StateConfig,
    /// Media processing configuration
    pub media: MediaConfig,
    /// Chunk processor configuration
    pub chunk_processor: ChunkProcessorConfig,
    /// Encoding configuration
    pub encoding: EncodingConfig,
    /// Resource management configuration
    pub resource: ResourceConfig,
    /// Pipeline configuration
    pub pipeline: PipelineConfig,
    /// Stream buffer configuration
    pub stream_buffer: StreamBufferConfig,
    /// Stream file configuration
    pub stream_file: StreamFileConfig,
    /// Metrics configuration
    pub metrics: MetricsConfig,
    /// Time window configuration for advanced analytics
    pub time_window: TimeWindowConfig,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            parallel: ParallelConfig::default(),
            backpressure: BackpressureConfig::default(),
            state: StateConfig::default(),
            media: MediaConfig::default(),
            chunk_processor: ChunkProcessorConfig::default(),
            encoding: EncodingConfig::default(),
            resource: ResourceConfig::default(),
            pipeline: PipelineConfig::default(),
            stream_buffer: StreamBufferConfig::default(),
            stream_file: StreamFileConfig::default(),
            metrics: MetricsConfig::default(),
            time_window: TimeWindowConfig::default(),
        }
    }
}

/// Builder for creating session configurations
pub struct SessionBuilder {
    config: SessionConfig,
}

impl SessionBuilder {
    /// Create a new session builder with default configuration
    pub fn new() -> Self {
        Self {
            config: SessionConfig::default(),
        }
    }

    /// Configure parallel processing settings
    pub fn parallel<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut ParallelConfig),
    {
        f(&mut self.config.parallel);
        self
    }

    /// Configure backpressure settings
    pub fn backpressure<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut BackpressureConfig),
    {
        f(&mut self.config.backpressure);
        self
    }

    /// Configure state management settings
    pub fn state<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut StateConfig),
    {
        f(&mut self.config.state);
        self
    }

    /// Configure media processing settings
    pub fn media<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut MediaConfig),
    {
        f(&mut self.config.media);
        self
    }

    /// Configure metrics settings
    pub fn metrics<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut MetricsConfig),
    {
        f(&mut self.config.metrics);
        self
    }

    /// Configure time window settings
    pub fn time_window<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut TimeWindowConfig),
    {
        f(&mut self.config.time_window);
        self
    }

    /// Configure stream buffer settings
    pub fn stream_buffer<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut StreamBufferConfig),
    {
        f(&mut self.config.stream_buffer);
        self
    }

    /// Configure stream file settings
    pub fn stream_file<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut StreamFileConfig),
    {
        f(&mut self.config.stream_file);
        self
    }

    /// Configure chunk processor settings
    pub fn chunk_processor<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut ChunkProcessorConfig),
    {
        f(&mut self.config.chunk_processor);
        self
    }

    /// Configure encoding settings
    pub fn encoding<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut EncodingConfig),
    {
        f(&mut self.config.encoding);
        self
    }

    /// Configure resource management settings
    pub fn resource<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut ResourceConfig),
    {
        f(&mut self.config.resource);
        self
    }

    /// Configure pipeline settings
    pub fn pipeline<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut PipelineConfig),
    {
        f(&mut self.config.pipeline);
        self
    }

    /// Build the final session configuration
    pub fn build(self) -> SessionConfig {
        self.config
    }

    /// Build with common presets
    pub fn preset(mut self, preset: SessionPreset) -> Self {
        match preset {
            SessionPreset::Development => {
                self = self.parallel(|p| {
                    p.concurrency = 2;
                    p.max_buffer_size = 100;
                    p.task_timeout = Duration::from_secs(5);
                });
                self = self.backpressure(|b| {
                    b.strategy = BackpressureStrategy::DropOldest;
                    b.buffer_size = 100;
                });
                self = self.stream_buffer(|b| {
                    b.initial_capacity = 256;
                    b.max_capacity = Some(1024 * 1024);
                });
                self = self.state(|s| {
                    s.max_size = Some(1000);
                    s.ttl = Duration::from_secs(3600);
                });
            }
            SessionPreset::Production => {
                self = self.parallel(|p| {
                    p.concurrency = 16;
                    p.max_buffer_size = 10000;
                    p.task_timeout = Duration::from_secs(300);
                });
                self = self.backpressure(|b| {
                    b.strategy = BackpressureStrategy::Block;
                    b.buffer_size = 10000;
                });
                self = self.stream_buffer(|b| {
                    b.initial_capacity = 4096;
                    b.max_capacity = Some(100 * 1024 * 1024); // 100MB
                });
                self = self.state(|s| {
                    s.max_size = Some(10000);
                    s.ttl = Duration::from_secs(7200);
                });
            }
            SessionPreset::HighPerformance => {
                self = self.parallel(|p| {
                    p.concurrency = 32;
                    p.max_buffer_size = 50000;
                    p.task_timeout = Duration::from_secs(600);
                });
                self = self.backpressure(|b| {
                    b.strategy = BackpressureStrategy::Block;
                    b.buffer_size = 50000;
                });
                self = self.stream_buffer(|b| {
                    b.initial_capacity = 8192;
                    b.max_capacity = Some(500 * 1024 * 1024); // 500MB
                });
                self = self.state(|s| {
                    s.max_size = Some(50000);
                    s.ttl = Duration::from_secs(14400);
                });
            }
            SessionPreset::LowMemory => {
                self = self.parallel(|p| {
                    p.concurrency = 4;
                    p.max_buffer_size = 1000;
                    p.task_timeout = Duration::from_secs(60);
                });
                self = self.backpressure(|b| {
                    b.strategy = BackpressureStrategy::DropOldest;
                    b.buffer_size = 1000;
                });
                self = self.stream_buffer(|b| {
                    b.initial_capacity = 128;
                    b.max_capacity = Some(10 * 1024 * 1024); // 10MB
                });
                self = self.state(|s| {
                    s.max_size = Some(500);
                    s.ttl = Duration::from_secs(1800);
                });
            }
        }
        self
    }
}

/// Common session presets for different use cases
#[derive(Debug, Clone)]
pub enum SessionPreset {
    /// Development environment - low resource usage, fast feedback
    Development,
    /// Production environment - balanced performance and resource usage
    Production,
    /// High performance - maximum throughput, higher resource usage
    HighPerformance,
    /// Low memory - minimal memory footprint, lower performance
    LowMemory,
}

impl Default for SessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Global session registry for automatic configuration access
pub struct SessionRegistry {
    current: std::sync::RwLock<Option<SessionConfig>>,
}

impl SessionRegistry {
    /// Create a new session registry
    pub fn new() -> Self {
        Self {
            current: std::sync::RwLock::new(None),
        }
    }

    /// Set the current session configuration
    pub fn set_session(&self, config: SessionConfig) {
        if let Ok(mut current) = self.current.write() {
            *current = Some(config);
        }
    }

    /// Get the current session configuration
    pub fn get_session(&self) -> Option<SessionConfig> {
        if let Ok(current) = self.current.read() {
            current.clone()
        } else {
            None
        }
    }

    /// Get a specific configuration section
    pub fn get_parallel_config(&self) -> Option<ParallelConfig> {
        self.get_session().map(|s| s.parallel)
    }

    /// Get backpressure configuration
    pub fn get_backpressure_config(&self) -> Option<BackpressureConfig> {
        self.get_session().map(|s| s.backpressure)
    }

    /// Get state configuration
    pub fn get_state_config(&self) -> Option<StateConfig> {
        self.get_session().map(|s| s.state)
    }

    /// Get media configuration
    pub fn get_media_config(&self) -> Option<MediaConfig> {
        self.get_session().map(|s| s.media)
    }

    /// Get buffer configuration
    pub fn get_buffer_config(&self) -> Option<StreamBufferConfig> {
        self.get_session().map(|s| s.stream_buffer)
    }

    /// Get file configuration
    pub fn get_file_config(&self) -> Option<StreamFileConfig> {
        self.get_session().map(|s| s.stream_file)
    }

    /// Get chunk processor configuration
    pub fn get_chunk_processor_config(&self) -> Option<ChunkProcessorConfig> {
        self.get_session().map(|s| s.chunk_processor)
    }

    /// Get encoding configuration
    pub fn get_encoding_config(&self) -> Option<EncodingConfig> {
        self.get_session().map(|s| s.encoding)
    }

    /// Get resource configuration
    pub fn get_resource_config(&self) -> Option<ResourceConfig> {
        self.get_session().map(|s| s.resource)
    }

    /// Get pipeline configuration
    pub fn get_pipeline_config(&self) -> Option<PipelineConfig> {
        self.get_session().map(|s| s.pipeline)
    }

    /// Get stream buffer configuration
    pub fn get_stream_buffer_config(&self) -> Option<StreamBufferConfig> {
        self.get_session().map(|s| s.stream_buffer)
    }

    /// Get stream file configuration
    pub fn get_stream_file_config(&self) -> Option<StreamFileConfig> {
        self.get_session().map(|s| s.stream_file)
    }

    /// Get metrics configuration
    pub fn get_metrics_config(&self) -> Option<MetricsConfig> {
        self.get_session().map(|s| s.metrics)
    }

    /// Get time window configuration
    pub fn get_time_window_config(&self) -> Option<TimeWindowConfig> {
        self.get_session().map(|s| s.time_window)
    }

    /// Clear the current session
    pub fn clear_session(&self) {
        if let Ok(mut current) = self.current.write() {
            *current = None;
        }
    }
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global session registry instance
pub static GLOBAL_SESSION: once_cell::sync::Lazy<SessionRegistry> = 
    once_cell::sync::Lazy::new(SessionRegistry::new);

/// Convenience functions for global session management
pub fn set_global_session(config: SessionConfig) {
    GLOBAL_SESSION.set_session(config);
}

/// Clear the global session configuration
pub fn clear_global_session() {
    GLOBAL_SESSION.clear_session();
}

pub fn get_global_session() -> Option<SessionConfig> {
    GLOBAL_SESSION.get_session()
}

pub fn get_global_parallel_config() -> Option<ParallelConfig> {
    GLOBAL_SESSION.get_parallel_config()
}

pub fn get_global_backpressure_config() -> Option<BackpressureConfig> {
    GLOBAL_SESSION.get_backpressure_config()
}

pub fn get_global_state_config() -> Option<StateConfig> {
    GLOBAL_SESSION.get_state_config()
}

pub fn get_global_media_config() -> Option<MediaConfig> {
    GLOBAL_SESSION.get_media_config()
}

pub fn get_global_buffer_config() -> Option<StreamBufferConfig> {
    GLOBAL_SESSION.get_buffer_config()
}

pub fn get_global_file_config() -> Option<StreamFileConfig> {
    GLOBAL_SESSION.get_file_config()
}

pub fn get_global_chunk_processor_config() -> Option<ChunkProcessorConfig> {
    GLOBAL_SESSION.get_chunk_processor_config()
}

pub fn get_global_encoding_config() -> Option<EncodingConfig> {
    GLOBAL_SESSION.get_encoding_config()
}

pub fn get_global_resource_config() -> Option<ResourceConfig> {
    GLOBAL_SESSION.get_resource_config()
}

pub fn get_global_pipeline_config() -> Option<PipelineConfig> {
    GLOBAL_SESSION.get_pipeline_config()
}

pub fn get_global_stream_buffer_config() -> Option<StreamBufferConfig> {
    GLOBAL_SESSION.get_stream_buffer_config()
}

pub fn get_global_stream_file_config() -> Option<StreamFileConfig> {
    GLOBAL_SESSION.get_stream_file_config()
}

pub fn get_global_metrics_config() -> Option<MetricsConfig> {
    GLOBAL_SESSION.get_metrics_config()
}

/// Get the global time window configuration
pub fn get_global_time_window_config() -> Option<TimeWindowConfig> {
    GLOBAL_SESSION.get_time_window_config()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_builder_default() {
        let config = SessionBuilder::new().build();
        // Default concurrency depends on system CPU count, so just check it's reasonable
        assert!(config.parallel.concurrency >= 1);
        assert!(config.parallel.concurrency <= 128); // Reasonable upper bound
        assert_eq!(config.stream_buffer.initial_capacity, 1024);
    }

    #[test]
    fn test_session_builder_custom() {
        let config = SessionBuilder::new()
            .parallel(|p| {
                p.concurrency = 16;
                p.max_buffer_size = 2000;
            })
            .backpressure(|b| {
                b.strategy = BackpressureStrategy::Block;
                b.buffer_size = 5000;
            })
            .build();

        assert_eq!(config.parallel.concurrency, 16);
        assert_eq!(config.parallel.max_buffer_size, 2000);
        assert_eq!(config.backpressure.strategy, BackpressureStrategy::Block);
        assert_eq!(config.backpressure.buffer_size, 5000);
    }

    #[test]
    fn test_session_presets() {
        let dev_config = SessionBuilder::new()
            .preset(SessionPreset::Development)
            .build();
        
        assert_eq!(dev_config.parallel.concurrency, 2);
        assert_eq!(dev_config.backpressure.strategy, BackpressureStrategy::DropOldest);

        let prod_config = SessionBuilder::new()
            .preset(SessionPreset::Production)
            .build();
        
        assert_eq!(prod_config.parallel.concurrency, 16);
        assert_eq!(prod_config.backpressure.strategy, BackpressureStrategy::Block);
    }

    #[test]
    fn test_global_session() {
        let config = SessionBuilder::new()
            .parallel(|p| p.concurrency = 32)
            .build();
        
        set_global_session(config);
        
        let retrieved = get_global_parallel_config();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().concurrency, 32);
    }
} 