//! Performance optimization utilities for RStream
//!
//! This module provides buffering strategies, metrics collection,
//! and performance monitoring tools.

use std::time::{Duration, Instant};

/// Buffer configuration for optimal performance
#[derive(Debug, Clone)]
pub struct BufferConfig {
    pub initial_capacity: usize,
    pub max_capacity: Option<usize>,
    pub growth_strategy: GrowthStrategy,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            initial_capacity: 8192,
            max_capacity: Some(1048576),
            growth_strategy: GrowthStrategy::Exponential(1.5),
        }
    }
}

/// Strategy for growing buffers
#[derive(Debug, Clone)]
pub enum GrowthStrategy {
    /// Grow linearly by fixed amount
    Linear(usize),
    /// Grow exponentially by multiplier
    Exponential(f64),
    /// Fixed size, don't grow
    Fixed,
}

/// Metrics collected for rs2_stream operations
#[derive(Debug, Clone, Default)]
pub struct StreamMetrics {
    pub items_processed: u64,
    pub bytes_processed: u64,
    pub processing_time: Duration,
    pub errors: u64,
    pub start_time: Option<Instant>,
}

impl StreamMetrics {
    pub fn new() -> Self {
        Self {
            start_time: Some(Instant::now()),
            ..Default::default()
        }
    }

    pub fn record_item(&mut self, size_bytes: u64) {
        self.items_processed += 1;
        self.bytes_processed += size_bytes;
    }

    pub fn record_error(&mut self) {
        self.errors += 1;
    }

    pub fn finalize(&mut self) {
        if let Some(start) = self.start_time.take() {
            self.processing_time = start.elapsed();
        }
    }

    pub fn throughput_items_per_sec(&self) -> f64 {
        if self.processing_time.as_secs_f64() > 0.0 {
            self.items_processed as f64 / self.processing_time.as_secs_f64()
        } else {
            0.0
        }
    }

    pub fn throughput_bytes_per_sec(&self) -> f64 {
        if self.processing_time.as_secs_f64() > 0.0 {
            self.bytes_processed as f64 / self.processing_time.as_secs_f64()
        } else {
            0.0
        }
    }
}