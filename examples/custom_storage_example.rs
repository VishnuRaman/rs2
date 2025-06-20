use async_trait::async_trait;
use futures::StreamExt;
use rs2_stream::state::StateConfig;
use rs2_stream::state::{CustomKeyExtractor, StateStorage, StatefulStreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    amount: f64,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserState {
    total_amount: f64,
    event_count: u64,
}

/// Example custom storage backend using in-memory HashMap with TTL
pub struct CustomHashMapStorage {
    data: Arc<RwLock<HashMap<String, (Vec<u8>, u64)>>>,
    ttl: u64,
}

impl CustomHashMapStorage {
    pub fn new(ttl_seconds: u64) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            ttl: ttl_seconds * 1000,
        }
    }

    fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[async_trait]
impl StateStorage for CustomHashMapStorage {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let data = self.data.read().await;
        if let Some((value, timestamp)) = data.get(key) {
            let now = Self::current_timestamp();
            if now - timestamp < self.ttl {
                Some(value.clone())
            } else {
                None
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
        let timestamp = Self::current_timestamp();
        data.insert(key.to_string(), (value.to_vec(), timestamp));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        data.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        let data = self.data.read().await;
        if let Some((_, timestamp)) = data.get(key) {
            let now = Self::current_timestamp();
            now - timestamp < self.ttl
        } else {
            false
        }
    }

    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }
}

// Example 1: Custom storage with built-in atomic update logic
struct AtomicHashMapStorage {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl AtomicHashMapStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // Custom atomic increment method
    pub async fn atomic_increment(
        &self,
        key: &str,
        increment: i64,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;

        let current_value = data
            .get(key)
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        let new_value = current_value + increment;
        data.insert(key.to_string(), new_value.to_string().into_bytes());

        Ok(new_value)
    }

    // Custom atomic update with closure
    pub async fn atomic_update<F, T>(
        &self,
        key: &str,
        f: F,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(Option<Vec<u8>>) -> (Vec<u8>, T),
    {
        let mut data = self.data.write().await;
        let current_value = data.get(key).cloned();
        let (new_value, result) = f(current_value);
        data.insert(key.to_string(), new_value);
        Ok(result)
    }
}

#[async_trait]
impl StateStorage for AtomicHashMapStorage {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let data = self.data.read().await;
        data.get(key).cloned()
    }

    async fn set(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        data.insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        data.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        let data = self.data.read().await;
        data.contains_key(key)
    }

    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }
}

// Example 2: Redis-like storage with transaction support
struct RedisLikeStorage {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
}

impl RedisLikeStorage {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    // Simulate Redis HSET operation
    pub async fn hset(
        &self,
        key: &str,
        field: &str,
        value: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;

        // In a real Redis implementation, this would be a hash
        // For simplicity, we'll store it as "key:field" -> value
        let hash_key = format!("{}:{}", key, field);
        data.insert(hash_key, value.as_bytes().to_vec());

        Ok(())
    }

    // Simulate Redis HGET operation
    pub async fn hget(&self, key: &str, field: &str) -> Option<String> {
        let data = self.data.read().await;
        let hash_key = format!("{}:{}", key, field);
        data.get(&hash_key)
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
    }

    // Simulate Redis INCR operation
    pub async fn incr(&self, key: &str) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;

        let current_value = data
            .get(key)
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(0);

        let new_value = current_value + 1;
        data.insert(key.to_string(), new_value.to_string().into_bytes());

        Ok(new_value)
    }

    // Simulate Redis transaction with MULTI/EXEC
    pub async fn transaction<F, T>(
        &self,
        f: F,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce(&mut Transaction) -> T,
    {
        let mut transaction = Transaction::new(self.data.clone());
        let result = f(&mut transaction);
        transaction.commit().await?;
        Ok(result)
    }
}

// Transaction helper for Redis-like storage
struct Transaction {
    data: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    pending_changes: HashMap<String, Option<Vec<u8>>>, // None means delete
}

impl Transaction {
    fn new(data: Arc<RwLock<HashMap<String, Vec<u8>>>>) -> Self {
        Self {
            data,
            pending_changes: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: &str, value: &[u8]) {
        self.pending_changes
            .insert(key.to_string(), Some(value.to_vec()));
    }

    pub fn delete(&mut self, key: &str) {
        self.pending_changes.insert(key.to_string(), None);
    }

    pub async fn commit(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        for (key, value) in self.pending_changes {
            match value {
                Some(value) => data.insert(key, value),
                None => data.remove(&key),
            };
        }
        Ok(())
    }
}

#[async_trait]
impl StateStorage for RedisLikeStorage {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let data = self.data.read().await;
        data.get(key).cloned()
    }

    async fn set(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        data.insert(key.to_string(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        data.remove(key);
        Ok(())
    }

    async fn exists(&self, key: &str) -> bool {
        let data = self.data.read().await;
        data.contains_key(key)
    }

    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut data = self.data.write().await;
        data.clear();
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    println!("=== Custom Storage Backend Example ===\n");

    // Example 1: Using AtomicHashMapStorage with custom update methods
    println!("1. Atomic HashMap Storage Example:");
    let atomic_storage = Arc::new(AtomicHashMapStorage::new());

    // Use custom atomic increment
    let counter = atomic_storage.atomic_increment("counter", 5).await.unwrap();
    println!("   Counter after increment: {}", counter);

    // Use custom atomic update with closure
    let result = atomic_storage
        .atomic_update("user:john", |current| {
            let current_count = current
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0);

            let new_count = current_count + 1;
            let new_value = new_count.to_string().into_bytes();
            (new_value, new_count)
        })
        .await
        .unwrap();

    println!("   User john visits: {}", result);

    // Example 2: Using Redis-like storage with transactions
    println!("\n2. Redis-like Storage Example:");
    let redis_storage = Arc::new(RedisLikeStorage::new());

    // Use Redis-like operations
    redis_storage
        .hset("user:alice", "name", "Alice Smith")
        .await
        .unwrap();
    redis_storage
        .hset("user:alice", "email", "alice@example.com")
        .await
        .unwrap();

    let name = redis_storage.hget("user:alice", "name").await.unwrap();
    let email = redis_storage.hget("user:alice", "email").await.unwrap();
    println!("   User Alice - Name: {}, Email: {}", name, email);

    // Use Redis-like transaction
    redis_storage
        .transaction(|tx| {
            tx.set("account:alice", b"1000");
            tx.set("account:bob", b"500");
            tx.delete("temp:data");
        })
        .await
        .unwrap();

    println!("   Transaction completed successfully");

    // Example 3: Using with stateful stream operations
    println!("\n3. Stateful Stream with Custom Storage:");

    let config = StateConfig::new().with_custom_storage(atomic_storage.clone());

    let events = futures::stream::iter(vec![
        UserEvent {
            user_id: "user1".to_string(),
            amount: 100.0,
            timestamp: 1000,
        },
        UserEvent {
            user_id: "user1".to_string(),
            amount: 200.0,
            timestamp: 2000,
        },
        UserEvent {
            user_id: "user2".to_string(),
            amount: 150.0,
            timestamp: 3000,
        },
        UserEvent {
            user_id: "user1".to_string(),
            amount: 50.0,
            timestamp: 4000,
        },
    ]);

    let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone());

    let processed_stream =
        events.stateful_map_rs2(config, key_extractor, move |event, _state_access| {
            let atomic_storage = atomic_storage.clone();
            Box::pin(async move {
                // Use custom atomic update for user totals
                let total = atomic_storage
                    .atomic_update(&format!("total:{}", event.user_id), |current| {
                        let current_total = current
                            .and_then(|bytes| String::from_utf8(bytes).ok())
                            .and_then(|s| s.parse::<f64>().ok())
                            .unwrap_or(0.0);
                        let new_total = current_total + event.amount;
                        (new_total.to_string().into_bytes(), new_total)
                    })
                    .await
                    .unwrap();

                // Use custom atomic increment for event count
                let count = atomic_storage
                    .atomic_increment(&format!("count:{}", event.user_id), 1)
                    .await
                    .unwrap();

                Ok(format!(
                    "User {}: ${:.2} total, {} events",
                    event.user_id, total, count
                ))
            })
        });

    let results: Vec<String> = processed_stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();
    for result in results {
        println!("   {}", result);
    }

    println!("\n=== Example completed successfully ===");
}
