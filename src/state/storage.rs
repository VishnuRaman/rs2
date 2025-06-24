use super::StateStorage;
use async_trait::async_trait;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use std::sync::atomic::{AtomicUsize, AtomicU64, Ordering};

/// High-performance in-memory state storage with advanced data structures
pub struct InMemoryState {
    // Core storage with enhanced structure
    data: Arc<RwLock<HashMap<String, StateEntry>>>,

    // LRU tracking for efficient eviction
    access_order: Arc<RwLock<VecDeque<String>>>,
    access_map: Arc<RwLock<HashMap<String, usize>>>, // Key -> position in access_order

    // Configuration
    ttl: Duration,
    max_size: Option<usize>,

    // Performance counters (atomic for lock-free access)
    total_size: AtomicUsize,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
    eviction_count: AtomicU64,

    // Cleanup optimization
    last_cleanup: Arc<RwLock<Instant>>,
    cleanup_interval: Duration,
}

/// Enhanced storage entry with metadata
#[derive(Clone)]
struct StateEntry {
    data: Vec<u8>,
    timestamp: Instant,
    access_count: u32,
    size: usize,
}

impl StateEntry {
    fn new(data: Vec<u8>) -> Self {
        let size = data.len();
        Self {
            data,
            timestamp: Instant::now(),
            access_count: 1,
            size,
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        Instant::now().duration_since(self.timestamp) >= ttl
    }

    fn touch(&mut self) {
        self.access_count = self.access_count.saturating_add(1);
    }
}

impl InMemoryState {
    pub fn new(ttl: Duration) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            access_order: Arc::new(RwLock::new(VecDeque::new())),
            access_map: Arc::new(RwLock::new(HashMap::new())),
            ttl,
            max_size: None,
            total_size: AtomicUsize::new(0),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            last_cleanup: Arc::new(RwLock::new(Instant::now())),
            cleanup_interval: Duration::from_secs(60),
        }
    }

    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }

    pub fn with_cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Fast, lock-free size check
    pub fn current_size(&self) -> usize {
        self.total_size.load(Ordering::Relaxed)
    }

    /// Performance statistics
    pub fn stats(&self) -> StateStorageStats {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total_requests = hits + misses;
        let hit_rate = if total_requests > 0 {
            hits as f64 / total_requests as f64
        } else {
            0.0
        };

        StateStorageStats {
            total_size: self.current_size(),
            hit_count: hits,
            miss_count: misses,
            hit_rate,
            eviction_count: self.eviction_count.load(Ordering::Relaxed),
        }
    }

    /// Intelligent cleanup based on access patterns and time
    async fn smart_cleanup(&self) {
        let now = Instant::now();

        // Check if cleanup is needed
        {
            let last_cleanup = self.last_cleanup.read().await;
            if now.duration_since(*last_cleanup) < self.cleanup_interval {
                return;
            }
        }

        let mut data = self.data.write().await;
        let mut access_order = self.access_order.write().await;
        let mut access_map = self.access_map.write().await;

        let mut expired_keys = Vec::new();
        let mut size_freed = 0;

        // Find expired entries
        for (key, entry) in data.iter() {
            if entry.is_expired(self.ttl) {
                expired_keys.push(key.clone());
                size_freed += entry.size;
            }
        }

        // Remove expired entries
        for key in &expired_keys {
            data.remove(key);
            access_map.remove(key);

            // Remove from access order (O(n) but only during cleanup)
            if let Some(pos) = access_order.iter().position(|k| k == key) {
                access_order.remove(pos);
            }
        }

        // Update size counter
        self.total_size.fetch_sub(size_freed, Ordering::Relaxed);

        // Update last cleanup time
        *self.last_cleanup.write().await = now;

        // Update eviction counter
        if !expired_keys.is_empty() {
            self.eviction_count.fetch_add(expired_keys.len() as u64, Ordering::Relaxed);
        }
    }

    /// LRU-based eviction when max size is exceeded
    async fn enforce_max_size_lru(&self) {
        if let Some(max_size) = self.max_size {
            let mut data = self.data.write().await;
            let mut access_order = self.access_order.write().await;
            let mut access_map = self.access_map.write().await;

            let mut size_freed = 0;
            let mut evicted_count = 0;

            // Remove least recently used items until under max_size
            while data.len() > max_size && !access_order.is_empty() {
                if let Some(lru_key) = access_order.pop_front() {
                    if let Some(entry) = data.remove(&lru_key) {
                        size_freed += entry.size;
                        evicted_count += 1;
                    }
                    access_map.remove(&lru_key);
                }
            }

            // Update counters
            if size_freed > 0 {
                self.total_size.fetch_sub(size_freed, Ordering::Relaxed);
                self.eviction_count.fetch_add(evicted_count, Ordering::Relaxed);
            }
        }
    }

    /// Update LRU tracking for a key
    async fn update_lru(&self, key: &str) {
        let mut access_order = self.access_order.write().await;
        let mut access_map = self.access_map.write().await;

        // Remove from current position if exists
        if let Some(&pos) = access_map.get(key) {
            if pos < access_order.len() {
                access_order.remove(pos);

                // Update positions for all keys after the removed position
                for (k, p) in access_map.iter_mut() {
                    if *p > pos {
                        *p -= 1;
                    }
                }
            }
        }

        // Add to end (most recently used)
        access_order.push_back(key.to_string());
        access_map.insert(key.to_string(), access_order.len() - 1);
    }

    // Keep the same cleanup_expired method for backward compatibility
    async fn cleanup_expired(&self) {
        self.smart_cleanup().await;
    }

    // Keep the same enforce_max_size method for backward compatibility  
    async fn enforce_max_size(&self) {
        self.enforce_max_size_lru().await;
    }
}

/// Performance statistics for the state storage
#[derive(Debug, Clone)]
pub struct StateStorageStats {
    pub total_size: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub hit_rate: f64,
    pub eviction_count: u64,
}

#[async_trait]
impl StateStorage for InMemoryState {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        // Try smart cleanup occasionally during reads
        tokio::spawn({
            let storage = Arc::new(self.clone());
            async move {
                if rand::random::<f32>() < 0.001 { // 0.1% chance
                    storage.smart_cleanup().await;
                }
            }
        });

        let mut data = self.data.write().await;

        if let Some(entry) = data.get_mut(key) {
            if !entry.is_expired(self.ttl) {
                entry.touch(); // Update access count
                self.hit_count.fetch_add(1, Ordering::Relaxed);

                // Update LRU asynchronously to avoid blocking
                let key_clone = key.to_string();
                let storage_clone = self.clone();
                tokio::spawn(async move {
                    storage_clone.update_lru(&key_clone).await;
                });

                return Some(entry.data.clone());
            } else {
                // Remove expired entry
                let size = entry.size;
                data.remove(key);
                self.total_size.fetch_sub(size, Ordering::Relaxed);
            }
        }

        self.miss_count.fetch_add(1, Ordering::Relaxed);
        None
    }

    async fn set(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let new_entry = StateEntry::new(value.to_vec());
        let new_size = new_entry.size;

        {
            let mut data = self.data.write().await;

            // Calculate size delta
            let old_size = data.get(key).map(|e| e.size).unwrap_or(0);
            data.insert(key.to_string(), new_entry);

            // Update total size atomically
            if old_size > 0 {
                self.total_size.fetch_sub(old_size, Ordering::Relaxed);
            }
            self.total_size.fetch_add(new_size, Ordering::Relaxed);
        }

        // Update LRU tracking
        self.update_lru(key).await;

        // Enforce size limits
        self.enforce_max_size_lru().await;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        let mut access_order = self.access_order.write().await;
        let mut access_map = self.access_map.write().await;

        if let Some(entry) = data.remove(key) {
            self.total_size.fetch_sub(entry.size, Ordering::Relaxed);

            // Remove from LRU tracking
            if let Some(&pos) = access_map.get(key) {
                if pos < access_order.len() {
                    access_order.remove(pos);

                    // Update positions
                    for (k, p) in access_map.iter_mut() {
                        if *p > pos {
                            *p -= 1;
                        }
                    }
                }
            }
            access_map.remove(key);
        }

        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        let data = self.data.read().await;
        if let Some(entry) = data.get(key) {
            !entry.is_expired(self.ttl)
        } else {
            false
        }
    }

    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        {
            let mut data = self.data.write().await;
            data.clear();
        }

        {
            let mut access_order = self.access_order.write().await;
            access_order.clear();
        }

        {
            let mut access_map = self.access_map.write().await;
            access_map.clear();
        }

        self.total_size.store(0, Ordering::Relaxed);

        Ok(())
    }
}

impl Clone for InMemoryState {
    fn clone(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            access_order: Arc::clone(&self.access_order),
            access_map: Arc::clone(&self.access_map),
            ttl: self.ttl,
            max_size: self.max_size,
            total_size: AtomicUsize::new(self.total_size.load(Ordering::Relaxed)),
            hit_count: AtomicU64::new(self.hit_count.load(Ordering::Relaxed)),
            miss_count: AtomicU64::new(self.miss_count.load(Ordering::Relaxed)),
            eviction_count: AtomicU64::new(self.eviction_count.load(Ordering::Relaxed)),
            last_cleanup: Arc::clone(&self.last_cleanup),
            cleanup_interval: self.cleanup_interval,
        }
    }
}