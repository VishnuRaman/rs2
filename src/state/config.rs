use super::storage::InMemoryState;
use super::traits::StateStorage;
use super::traits::StateStorageType;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for state management
#[derive(Clone)]
pub struct StateConfig {
    pub storage_type: StateStorageType,
    pub ttl: Duration,
    pub cleanup_interval: Duration,
    pub max_size: Option<usize>,
    pub custom_storage: Option<Arc<dyn StateStorage + Send + Sync>>,
}

impl std::fmt::Debug for StateConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateConfig")
            .field("storage_type", &self.storage_type)
            .field("ttl", &self.ttl)
            .field("cleanup_interval", &self.cleanup_interval)
            .field("max_size", &self.max_size)
            .field(
                "custom_storage",
                &if self.custom_storage.is_some() {
                    "Some(CustomStorage)"
                } else {
                    "None"
                },
            )
            .finish()
    }
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            storage_type: StateStorageType::InMemory,
            ttl: Duration::from_secs(24 * 60 * 60), // 24 hours
            cleanup_interval: Duration::from_secs(5 * 60), // 5 minutes
            max_size: None,
            custom_storage: None,
        }
    }
}

impl StateConfig {
    /// Create a new state configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the storage type
    pub fn storage_type(mut self, storage_type: StateStorageType) -> Self {
        self.storage_type = storage_type;
        self
    }

    /// Set the TTL (time to live) for state entries
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Set the cleanup interval for expired entries
    pub fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.cleanup_interval = interval;
        self
    }

    /// Set the maximum size for in-memory storage
    pub fn max_size(mut self, max_size: usize) -> Self {
        self.max_size = Some(max_size);
        self
    }

    /// Set a custom storage backend
    pub fn with_custom_storage(mut self, storage: Arc<dyn StateStorage + Send + Sync>) -> Self {
        self.custom_storage = Some(storage);
        self.storage_type = StateStorageType::Custom;
        self
    }

    /// Create a storage instance from this configuration
    pub fn create_storage(&self) -> Box<dyn StateStorage + Send + Sync> {
        match self.storage_type {
            StateStorageType::InMemory => {
                let mut storage = InMemoryState::new(self.ttl);
                if let Some(max_size) = self.max_size {
                    storage = storage.with_max_size(max_size);
                }
                Box::new(storage)
            }
            StateStorageType::Custom => {
                if let Some(ref custom_storage) = self.custom_storage {
                    // Clone the Arc to get a new reference to the same storage
                    let cloned_storage = custom_storage.clone();
                    // Convert Arc to Box by dereferencing and boxing
                    Box::new(ArcStorageWrapper(cloned_storage))
                } else {
                    // Fallback to in-memory if no custom storage is provided
                    let mut storage = InMemoryState::new(self.ttl);
                    if let Some(max_size) = self.max_size {
                        storage = storage.with_max_size(max_size);
                    }
                    Box::new(storage)
                }
            }
        }
    }

    /// Create a storage instance as Arc from this configuration
    pub fn create_storage_arc(&self) -> Arc<dyn StateStorage + Send + Sync> {
        match self.storage_type {
            StateStorageType::InMemory => {
                let mut storage = InMemoryState::new(self.ttl);
                if let Some(max_size) = self.max_size {
                    storage = storage.with_max_size(max_size);
                }
                Arc::new(storage)
            }
            StateStorageType::Custom => {
                if let Some(ref custom_storage) = self.custom_storage {
                    // Clone the Arc to get a new reference to the same storage
                    custom_storage.clone()
                } else {
                    // Fallback to in-memory if no custom storage is provided
                    let mut storage = InMemoryState::new(self.ttl);
                    if let Some(max_size) = self.max_size {
                        storage = storage.with_max_size(max_size);
                    }
                    Arc::new(storage)
                }
            }
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.ttl.is_zero() {
            return Err("TTL cannot be zero".to_string());
        }

        if self.cleanup_interval.is_zero() {
            return Err("Cleanup interval cannot be zero".to_string());
        }

        if self.cleanup_interval > self.ttl {
            return Err("Cleanup interval should be less than or equal to TTL".to_string());
        }

        if let Some(max_size) = self.max_size {
            if max_size == 0 {
                return Err("Max size cannot be zero".to_string());
            }
        }

        Ok(())
    }
}

/// Wrapper to convert Arc<dyn StateStorage> to Box<dyn StateStorage>
struct ArcStorageWrapper(Arc<dyn StateStorage + Send + Sync>);

#[async_trait::async_trait]
impl StateStorage for ArcStorageWrapper {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.0.get(key).await
    }

    async fn set(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.set(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.delete(key).await
    }

    async fn exists(&self, key: &str) -> bool {
        self.0.exists(key).await
    }

    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.0.clear().await
    }
}

/// Builder for state configurations
pub struct StateConfigBuilder {
    config: StateConfig,
}

impl StateConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: StateConfig::default(),
        }
    }

    pub fn storage_type(mut self, storage_type: StateStorageType) -> Self {
        self.config.storage_type = storage_type;
        self
    }

    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.config.ttl = ttl;
        self
    }

    pub fn cleanup_interval(mut self, interval: Duration) -> Self {
        self.config.cleanup_interval = interval;
        self
    }

    pub fn max_size(mut self, max_size: usize) -> Self {
        self.config.max_size = Some(max_size);
        self
    }

    pub fn custom_storage(mut self, storage: Arc<dyn StateStorage + Send + Sync>) -> Self {
        self.config.custom_storage = Some(storage);
        self.config.storage_type = StateStorageType::Custom;
        self
    }

    pub fn build(self) -> Result<StateConfig, String> {
        self.config.validate()?;
        Ok(self.config)
    }
}

impl Default for StateConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Predefined state configurations for common use cases
pub struct StateConfigs;

impl StateConfigs {
    /// Configuration for high-performance, in-memory state
    pub fn high_performance() -> StateConfig {
        StateConfigBuilder::new()
            .storage_type(StateStorageType::InMemory)
            .ttl(Duration::from_secs(60 * 60)) // 1 hour
            .cleanup_interval(Duration::from_secs(60)) // 1 minute
            .max_size(10000)
            .build()
            .unwrap()
    }

    /// Configuration for session state
    pub fn session() -> StateConfig {
        StateConfigBuilder::new()
            .storage_type(StateStorageType::InMemory)
            .ttl(Duration::from_secs(30 * 60)) // 30 minutes
            .cleanup_interval(Duration::from_secs(5 * 60)) // 5 minutes
            .max_size(1000)
            .build()
            .unwrap()
    }

    /// Configuration for short-lived state
    pub fn short_lived() -> StateConfig {
        StateConfigBuilder::new()
            .storage_type(StateStorageType::InMemory)
            .ttl(Duration::from_secs(5 * 60)) // 5 minutes
            .cleanup_interval(Duration::from_secs(30)) // 30 seconds
            .max_size(100)
            .build()
            .unwrap()
    }

    /// Configuration for long-lived state
    pub fn long_lived() -> StateConfig {
        StateConfigBuilder::new()
            .storage_type(StateStorageType::InMemory)
            .ttl(Duration::from_secs(7 * 24 * 60 * 60)) // 7 days
            .cleanup_interval(Duration::from_secs(60 * 60)) // 1 hour
            .max_size(100000)
            .build()
            .unwrap()
    }
}
