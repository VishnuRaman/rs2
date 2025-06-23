use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Resource management configuration
#[derive(Debug, Clone)]
pub struct ResourceConfig {
    /// Maximum memory usage in bytes
    pub max_memory_bytes: u64,
    /// Maximum number of active keys/streams
    pub max_keys: usize,
    /// Memory threshold for circuit breaker (percentage of max_memory)
    pub memory_threshold_percent: u8,
    /// Buffer overflow threshold
    pub buffer_overflow_threshold: usize,
    /// Cleanup interval
    pub cleanup_interval: Duration,
    /// Emergency cleanup trigger threshold (percentage of max_memory)
    pub emergency_cleanup_threshold: u8,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 1024 * 1024 * 1024, // 1GB
            max_keys: 100_000,
            memory_threshold_percent: 80,
            buffer_overflow_threshold: 10_000,
            cleanup_interval: Duration::from_secs(30),
            emergency_cleanup_threshold: 95,
        }
    }
}

/// Circuit breaker states
#[derive(Debug, Clone, PartialEq)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

/// Resource usage metrics
#[derive(Debug, Clone)]
pub struct ResourceMetrics {
    pub current_memory_bytes: u64,
    pub peak_memory_bytes: u64,
    pub active_keys: usize,
    pub buffer_overflow_count: u64,
    pub circuit_breaker_trips: u64,
    pub emergency_cleanups: u64,
}

/// Production-grade resource manager for RS2 streaming library
pub struct ResourceManager {
    config: ResourceConfig,
    memory_usage: Arc<AtomicU64>,
    active_keys: Arc<AtomicUsize>,
    buffer_overflow_count: Arc<AtomicU64>,
    circuit_breaker_trips: Arc<AtomicU64>,
    emergency_cleanups: Arc<AtomicU64>,
    peak_memory: Arc<AtomicU64>,
    circuit_breaker_state: Arc<RwLock<CircuitBreakerState>>,
    last_cleanup: Arc<RwLock<Instant>>,
    resource_trackers: Arc<RwLock<HashMap<String, u64>>>,
}

impl ResourceManager {
    /// Create a new resource manager with default configuration
    pub fn new() -> Self {
        Self::with_config(ResourceConfig::default())
    }

    /// Create a new resource manager with custom configuration
    pub fn with_config(config: ResourceConfig) -> Self {
        Self {
            config,
            memory_usage: Arc::new(AtomicU64::new(0)),
            active_keys: Arc::new(AtomicUsize::new(0)),
            buffer_overflow_count: Arc::new(AtomicU64::new(0)),
            circuit_breaker_trips: Arc::new(AtomicU64::new(0)),
            emergency_cleanups: Arc::new(AtomicU64::new(0)),
            peak_memory: Arc::new(AtomicU64::new(0)),
            circuit_breaker_state: Arc::new(RwLock::new(CircuitBreakerState::Closed)),
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
            resource_trackers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Track memory allocation
    pub async fn track_memory_allocation(&self, bytes: u64) -> Result<(), ResourceError> {
        let current = self.memory_usage.fetch_add(bytes, Ordering::Relaxed) + bytes;
        
        // Update peak memory
        let mut peak = self.peak_memory.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_memory.compare_exchange_weak(
                peak, current, Ordering::Relaxed, Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(new_peak) => peak = new_peak,
            }
        }

        // Check memory threshold
        if current > self.config.max_memory_bytes * self.config.memory_threshold_percent as u64 / 100 {
            self.trip_circuit_breaker().await;
        }

        // Emergency cleanup if needed
        if current > self.config.max_memory_bytes * self.config.emergency_cleanup_threshold as u64 / 100 {
            self.emergency_cleanup().await;
        }

        Ok(())
    }

    /// Track memory deallocation
    pub async fn track_memory_deallocation(&self, bytes: u64) {
        self.memory_usage.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Track key creation
    pub async fn track_key_creation(&self) -> Result<(), ResourceError> {
        let current_keys = self.active_keys.fetch_add(1, Ordering::Relaxed) + 1;
        
        if current_keys > self.config.max_keys {
            self.active_keys.fetch_sub(1, Ordering::Relaxed);
            return Err(ResourceError::MaxKeysExceeded);
        }

        Ok(())
    }

    /// Track key removal
    pub async fn track_key_removal(&self) {
        self.active_keys.fetch_sub(1, Ordering::Relaxed);
    }

    /// Track buffer overflow
    pub async fn track_buffer_overflow(&self) -> Result<(), ResourceError> {
        self.buffer_overflow_count.fetch_add(1, Ordering::Relaxed);
        
        let overflow_count = self.buffer_overflow_count.load(Ordering::Relaxed);
        if overflow_count > self.config.buffer_overflow_threshold as u64 {
            self.trip_circuit_breaker().await;
            return Err(ResourceError::BufferOverflowThresholdExceeded);
        }

        Ok(())
    }

    /// Get current resource metrics
    pub async fn get_metrics(&self) -> ResourceMetrics {
        ResourceMetrics {
            current_memory_bytes: self.memory_usage.load(Ordering::Relaxed),
            peak_memory_bytes: self.peak_memory.load(Ordering::Relaxed),
            active_keys: self.active_keys.load(Ordering::Relaxed),
            buffer_overflow_count: self.buffer_overflow_count.load(Ordering::Relaxed),
            circuit_breaker_trips: self.circuit_breaker_trips.load(Ordering::Relaxed),
            emergency_cleanups: self.emergency_cleanups.load(Ordering::Relaxed),
        }
    }

    /// Check if circuit breaker is open
    pub async fn is_circuit_open(&self) -> bool {
        let state = self.circuit_breaker_state.read().await;
        *state == CircuitBreakerState::Open
    }

    /// Trip the circuit breaker
    async fn trip_circuit_breaker(&self) {
        let mut state = self.circuit_breaker_state.write().await;
        if *state == CircuitBreakerState::Closed {
            *state = CircuitBreakerState::Open;
            self.circuit_breaker_trips.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Reset circuit breaker to half-open state
    pub async fn reset_circuit_breaker(&self) {
        let mut state = self.circuit_breaker_state.write().await;
        *state = CircuitBreakerState::HalfOpen;
    }

    /// Close circuit breaker
    pub async fn close_circuit_breaker(&self) {
        let mut state = self.circuit_breaker_state.write().await;
        *state = CircuitBreakerState::Closed;
    }

    /// Perform emergency cleanup
    async fn emergency_cleanup(&self) {
        self.emergency_cleanups.fetch_add(1, Ordering::Relaxed);
        log::warn!("Emergency cleanup triggered - memory pressure detected");
        self.perform_cleanup().await;
        self.perform_emergency_measures().await;
        if self.is_under_memory_pressure().await {
            log::error!("Memory pressure persists after emergency cleanup - tripping circuit breaker");
            self.trip_circuit_breaker().await;
        }
    }

    /// Perform emergency measures for severe memory pressure
    async fn perform_emergency_measures(&self) {
        let mut trackers = self.resource_trackers.write().await;
        trackers.clear();
        drop(trackers);
        log::info!("Emergency measures completed - cleared non-essential resources");
    }

    /// Perform periodic cleanup
    pub async fn periodic_cleanup(&self) {
        let mut last_cleanup = self.last_cleanup.write().await;
        if last_cleanup.elapsed() >= self.config.cleanup_interval {
            self.perform_cleanup().await;
            *last_cleanup = Instant::now();
        }
    }

    /// Perform actual cleanup operations
    async fn perform_cleanup(&self) {
        log::debug!("Performing periodic cleanup");
        
        let mut trackers = self.resource_trackers.write().await;
        let before_count = trackers.len();
        
       
        if trackers.len() > 1000 {
            let to_remove: Vec<String> = trackers.keys().take(100).cloned().collect();
            for key in to_remove {
                trackers.remove(&key);
            }
        }
        
        let after_count = trackers.len();
        drop(trackers);
        
        if before_count != after_count {
            log::info!("Cleanup completed: removed {} resources", before_count - after_count);
        }
        
        let overflow_count = self.buffer_overflow_count.load(Ordering::Relaxed);
        if overflow_count > 1000 {
            self.buffer_overflow_count.store(0, Ordering::Relaxed);
            log::info!("Reset buffer overflow counter");
        }
    }

    /// Track a specific resource
    pub async fn track_resource(&self, resource_id: String, size_bytes: u64) {
        let mut trackers = self.resource_trackers.write().await;
        trackers.insert(resource_id, size_bytes);
    }

    /// Untrack a specific resource
    pub async fn untrack_resource(&self, resource_id: &str) -> Option<u64> {
        let mut trackers = self.resource_trackers.write().await;
        trackers.remove(resource_id)
    }

    /// Get memory pressure level (0-100)
    pub async fn get_memory_pressure(&self) -> u8 {
        let current = self.memory_usage.load(Ordering::Relaxed);
        let max = self.config.max_memory_bytes;
        ((current * 100) / max) as u8
    }

    /// Check if system is under memory pressure
    pub async fn is_under_memory_pressure(&self) -> bool {
        let pressure = self.get_memory_pressure().await;
        pressure > self.config.memory_threshold_percent
    }
}

/// Resource management errors
#[derive(Debug, thiserror::Error)]
pub enum ResourceError {
    #[error("Maximum memory usage exceeded")]
    MaxMemoryExceeded,
    #[error("Maximum number of keys exceeded")]
    MaxKeysExceeded,
    #[error("Buffer overflow threshold exceeded")]
    BufferOverflowThresholdExceeded,
    #[error("Circuit breaker is open")]
    CircuitBreakerOpen,
    #[error("Resource allocation failed: {0}")]
    AllocationFailed(String),
}

/// Global resource manager instance
lazy_static::lazy_static! {
    pub static ref GLOBAL_RESOURCE_MANAGER: Arc<ResourceManager> = Arc::new(ResourceManager::new());
}

/// Get the global resource manager
pub fn get_global_resource_manager() -> Arc<ResourceManager> {
    GLOBAL_RESOURCE_MANAGER.clone()
} 