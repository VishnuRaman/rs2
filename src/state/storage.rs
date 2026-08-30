use super::StateStorage;
use async_trait::async_trait;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// How many `set` calls between sweeps for expired entries.
///
/// Expiry has to be swept eventually rather than only checked on read: a key
/// that is written once and never read again would otherwise keep its bytes
/// alive for the lifetime of the process.
const EXPIRY_SWEEP_INTERVAL: u64 = 256;

/// Entries plus a write-ordered index over them.
///
/// The index is what keeps eviction cheap. Without it, enforcing `max_size`
/// meant cloning and sorting every key on each `set` — measured at ~2.2ms per
/// write once at capacity, versus microseconds now.
struct Inner {
    data: HashMap<String, (Vec<u8>, Instant)>,
    /// `(written_at, key)`, ordered oldest first. The key breaks ties between
    /// entries written within the same clock tick.
    order: BTreeSet<(Instant, String)>,
}

impl Inner {
    fn insert(&mut self, key: &str, value: &[u8], now: Instant) {
        if let Some((_, previous)) = self.data.get(key) {
            self.order.remove(&(*previous, key.to_string()));
        }
        self.data.insert(key.to_string(), (value.to_vec(), now));
        self.order.insert((now, key.to_string()));
    }

    fn remove(&mut self, key: &str) {
        if let Some((_, written_at)) = self.data.remove(key) {
            self.order.remove(&(written_at, key.to_string()));
        }
    }

    /// Drop entries past their TTL. Expired entries are always at the front of
    /// the index, so this stops at the first live one.
    fn sweep_expired(&mut self, ttl: Duration) {
        let now = Instant::now();
        while let Some((written_at, key)) = self.order.iter().next().cloned() {
            if now.duration_since(written_at) < ttl {
                break;
            }
            self.order.remove(&(written_at, key.clone()));
            self.data.remove(&key);
        }
    }

    /// Evict oldest-written entries until at most `max_size` remain.
    fn enforce_max_size(&mut self, max_size: usize) {
        while self.data.len() > max_size {
            let oldest = match self.order.iter().next().cloned() {
                Some(entry) => entry,
                None => break,
            };
            self.order.remove(&oldest);
            self.data.remove(&oldest.1);
        }
    }
}

/// In-memory state storage (fastest, but not persistent)
pub struct InMemoryState {
    inner: Arc<RwLock<Inner>>,
    ttl: Duration,
    max_size: Option<usize>,
    sets_since_sweep: Arc<AtomicU64>,
}

impl InMemoryState {
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                data: HashMap::new(),
                order: BTreeSet::new(),
            })),
            ttl,
            max_size: None,
            sets_since_sweep: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }

    /// Number of live (non-expired) entries. Primarily for tests.
    pub async fn len(&self) -> usize {
        let now = Instant::now();
        let inner = self.inner.read().await;
        inner
            .data
            .values()
            .filter(|(_, written_at)| now.duration_since(*written_at) < self.ttl)
            .count()
    }

    /// Number of entries actually retained, expired or not. Primarily for tests.
    pub async fn allocated_len(&self) -> usize {
        self.inner.read().await.data.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

#[async_trait]
impl StateStorage for InMemoryState {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let inner = self.inner.read().await;
        if let Some((value_bytes, written_at)) = inner.data.get(key) {
            if Instant::now().duration_since(*written_at) < self.ttl {
                Some(value_bytes.clone())
            } else {
                None // Expired
            }
        } else {
            None
        }
    }

    async fn set(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Decide whether this call owns the sweep before taking the lock, so
        // the counter advances exactly once per set.
        let sweep =
            self.sets_since_sweep.fetch_add(1, Ordering::Relaxed) % EXPIRY_SWEEP_INTERVAL == 0;

        // One write lock for the whole operation. An earlier version dropped and
        // re-acquired it between insert and max-size enforcement, letting
        // concurrent writers both overshoot the limit.
        let mut inner = self.inner.write().await;
        inner.insert(key, value, Instant::now());

        if sweep {
            inner.sweep_expired(self.ttl);
        }
        if let Some(max_size) = self.max_size {
            inner.enforce_max_size(max_size);
        }

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut inner = self.inner.write().await;
        inner.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        let inner = self.inner.read().await;
        if let Some((_, written_at)) = inner.data.get(key) {
            Instant::now().duration_since(*written_at) < self.ttl
        } else {
            false
        }
    }

    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut inner = self.inner.write().await;
        inner.data.clear();
        inner.order.clear();
        Ok(())
    }
}
