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

#[derive(Debug, Clone)]
pub struct HealthThresholds {
    pub max_error_rate: f64,
    pub max_consecutive_errors: u64,
}

impl Default for HealthThresholds {
    fn default() -> Self {
        Self {
            max_error_rate: 0.1,       // 10% error rate
            max_consecutive_errors: 5, // 5 consecutive errors
        }
    }
}

impl HealthThresholds {
    /// Conservative thresholds for critical systems
    pub fn strict() -> Self {
        Self {
            max_error_rate: 0.01,      // 1% error rate
            max_consecutive_errors: 2, // 2 consecutive errors
        }
    }

    /// Permissive thresholds for high-throughput systems
    pub fn relaxed() -> Self {
        Self {
            max_error_rate: 0.20,       // 20% error rate
            max_consecutive_errors: 20, // 20 consecutive errors
        }
    }

    /// Custom thresholds
    pub fn custom(max_error_rate: f64, max_consecutive_errors: u64) -> Self {
        Self {
            max_error_rate,
            max_consecutive_errors,
        }
    }
}

/// Metrics collected for rs2_stream operations
#[derive(Debug, Clone, Default)]
pub struct StreamMetrics {
    pub name: Option<String>,
    pub items_processed: u64,
    pub bytes_processed: u64,
    pub processing_time: Duration,
    pub errors: u64,
    pub start_time: Option<Instant>,

    pub retries: u64,
    pub items_per_second: f64,
    pub bytes_per_second: f64,
    pub average_item_size: f64,
    pub peak_processing_time: Duration,
    pub last_activity: Option<Instant>,
    pub consecutive_errors: u64,
    pub error_rate: f64,
    pub backpressure_events: u64,
    pub queue_depth: u64,
    pub health_thresholds: HealthThresholds,
}

impl StreamMetrics {
    pub fn new() -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        Self {
            name: Some(format!("rs2-stream-{}", timestamp)),
            start_time: Some(Instant::now()),
            ..Default::default()
        }
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = Some(name);
        self
    }
    pub fn set_name(&mut self, name: String) {
        self.name = Some(name);
    }

    pub fn with_health_thresholds(mut self, thresholds: HealthThresholds) -> Self {
        self.health_thresholds = thresholds;
        self
    }

    pub fn set_health_thresholds(&mut self, thresholds: HealthThresholds) {
        self.health_thresholds = thresholds;
    }

    pub fn record_item(&mut self, size_bytes: u64) {
        self.items_processed += 1;
        self.bytes_processed += size_bytes;
        self.last_activity = Some(Instant::now());
        self.consecutive_errors = 0;
        self.update_derived_metrics();
    }

    pub fn record_error(&mut self) {
        self.errors += 1;
        self.consecutive_errors += 1;
        self.last_activity = Some(Instant::now());
        self.update_derived_metrics();
    }

    pub fn record_retry(&mut self) {
        self.retries += 1;
    }

    pub fn record_processing_time(&mut self, duration: Duration) {
        self.processing_time += duration;
        if duration > self.peak_processing_time {
            self.peak_processing_time = duration;
        }
    }

    pub fn record_backpressure(&mut self) {
        self.backpressure_events += 1;
    }

    pub fn update_queue_depth(&mut self, depth: u64) {
        self.queue_depth = depth;
    }

    pub fn finalize(&mut self) {
        if let Some(start) = self.start_time.take() {
            self.processing_time = start.elapsed();
        }
        self.update_derived_metrics();
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

    pub fn update_derived_metrics(&mut self) {
        if let Some(start) = self.start_time {
            let elapsed_secs = start.elapsed().as_secs_f64();
            if elapsed_secs > 0.0 {
                self.items_per_second = self.items_processed as f64 / elapsed_secs;
                self.bytes_per_second = self.bytes_processed as f64 / elapsed_secs;
            }
        }

        if self.items_processed > 0 {
            self.average_item_size = self.bytes_processed as f64 / self.items_processed as f64;
        }

        let total_attempts = self.items_processed + self.errors;
        if total_attempts > 0 {
            self.error_rate = self.errors as f64 / total_attempts as f64;
        }
    }

    pub fn is_healthy(&self) -> bool {
        self.error_rate < self.health_thresholds.max_error_rate
            && self.consecutive_errors < self.health_thresholds.max_consecutive_errors
    }

    pub fn throughput_summary(&self) -> String {
        format!(
            "{:.1} items/sec, {:.1} KB/sec",
            self.items_per_second,
            self.bytes_per_second / 1000.0
        )
    }

    pub fn throughput_summary_processing_time(&self) -> String {
        format!(
            "{:.1} items/sec, {:.1} KB/sec",
            self.throughput_items_per_sec(),
            self.throughput_bytes_per_sec() / 1000.0
        )
    }
}
