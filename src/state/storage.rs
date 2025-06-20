use super::StateStorage;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use crate::resource_manager::get_global_resource_manager;

/// In-memory state storage (fastest, but not persistent)
pub struct InMemoryState {
    data: Arc<RwLock<HashMap<String, (Vec<u8>, Instant)>>>,
    ttl: Duration,
    max_size: Option<usize>,
}

impl InMemoryState {
    pub fn new(ttl: Duration) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            ttl,
            max_size: None,
        }
    }

    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }

    async fn cleanup_expired(&self) {
        let mut data = self.data.write().await;
        let now = Instant::now();
        data.retain(|_, (_, timestamp)| now.duration_since(*timestamp) < self.ttl);
    }

    async fn enforce_max_size(&self) {
        if let Some(max_size) = self.max_size {
            let mut data = self.data.write().await;
            if data.len() > max_size {
                // Remove oldest entries
                let mut entries: Vec<_> = data.iter().collect();
                entries.sort_by_key(|(_, (_, timestamp))| *timestamp);

                let to_remove = entries.len() - max_size;
                let keys_to_remove: Vec<String> = entries
                    .iter()
                    .take(to_remove)
                    .map(|(key, _)| (*key).clone())
                    .collect();

                for key in keys_to_remove {
                    data.remove(&key);
                }
            }
        }
    }
}

#[async_trait]
impl StateStorage for InMemoryState {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let data = self.data.read().await;
        if let Some((value_bytes, timestamp)) = data.get(key) {
            if Instant::now().duration_since(*timestamp) < self.ttl {
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
        let mut data = self.data.write().await;
        let is_new_key = !data.contains_key(key);
        let old_size = data.get(key).map(|(v, _)| v.len()).unwrap_or(0);
        data.insert(key.to_string(), (value.to_vec(), Instant::now()));
        let new_size = value.len();
        drop(data);
        let resource_manager = get_global_resource_manager();
        if is_new_key {
            resource_manager.track_key_creation().await.ok();
        }
        if new_size > old_size {
            resource_manager.track_memory_allocation((new_size - old_size) as u64).await.ok();
        } else if old_size > new_size {
            resource_manager.track_memory_deallocation((old_size - new_size) as u64).await;
        }
        // Enforce max size within the same lock
        let mut data = self.data.write().await;
        if let Some(max_size) = self.max_size {
            if data.len() > max_size {
                // Remove oldest entries
                let mut entries: Vec<_> = data.iter().collect();
                entries.sort_by_key(|(_, (_, timestamp))| *timestamp);
                let to_remove = entries.len() - max_size;
                let keys_to_remove: Vec<String> = entries
                    .iter()
                    .take(to_remove)
                    .map(|(key, _)| (*key).clone())
                    .collect();
                for key in keys_to_remove {
                    if let Some((v, _)) = data.remove(&key) {
                        resource_manager.track_memory_deallocation(v.len() as u64).await;
                        resource_manager.track_key_removal().await;
                    }
                }
            }
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        if let Some((v, _)) = data.remove(key) {
            let resource_manager = get_global_resource_manager();
            resource_manager.track_memory_deallocation(v.len() as u64).await;
            resource_manager.track_key_removal().await;
        }
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        let data = self.data.read().await;
        if let Some((_, timestamp)) = data.get(key) {
            Instant::now().duration_since(*timestamp) < self.ttl
        } else {
            false
        }
    }

    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        let resource_manager = get_global_resource_manager();
        for (v, _) in data.values() {
            resource_manager.track_memory_deallocation(v.len() as u64).await;
            resource_manager.track_key_removal().await;
        }
        data.clear();
        Ok(())
    }
}
