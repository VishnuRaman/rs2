//! Configuration types for RStream operations

/// Buffer configuration for rs2_stream operations
#[derive(Debug, Clone)]
pub struct BufferConfig {
    pub initial_capacity: usize,
    pub max_capacity: Option<usize>,
    pub growth_strategy: GrowthStrategy,
}

#[derive(Debug, Clone)]
pub enum GrowthStrategy {
    Linear(usize),
    Exponential(f64),
    Fixed,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            initial_capacity: 1024,
            max_capacity: Some(1024 * 1024), // 1MB
            growth_strategy: GrowthStrategy::Exponential(2.0),
        }
    }
}

/// File configuration for I/O operations
#[derive(Debug, Clone)]
pub struct FileConfig {
    pub buffer_size: usize,
    pub read_ahead: bool,
    pub sync_on_write: bool,
    pub compression: Option<CompressionType>,
}

#[derive(Debug, Clone)]
pub enum CompressionType {
    Gzip,
    Deflate,
    Lz4,
}

impl Default for FileConfig {
    fn default() -> Self {
        Self {
            buffer_size: 8192,
            read_ahead: true,
            sync_on_write: false,
            compression: None,
        }
    }
}

/// Metrics configuration
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub sample_rate: f64,
    pub labels: Vec<(String, String)>,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sample_rate: 1.0,
            labels: Vec::new(),
        }
    }
}
