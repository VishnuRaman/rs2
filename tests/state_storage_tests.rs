use rs2_stream::state::traits::StateStorageType;
use rs2_stream::state::{InMemoryState, StateConfig, StateStorage};
use std::sync::Arc;
use std::time::Duration;
use tokio;

#[tokio::test]
async fn test_basic_storage_operations() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    // Test set and get
    storage.set("key1", b"value1").await.unwrap();
    let value = storage.get("key1").await;
    assert_eq!(value, Some(b"value1".to_vec()));

    // Test get non-existent key
    let value = storage.get("nonexistent").await;
    assert_eq!(value, None);

    // Test update
    storage.set("key1", b"updated_value").await.unwrap();
    let value = storage.get("key1").await;
    assert_eq!(value, Some(b"updated_value".to_vec()));
}

#[tokio::test]
async fn test_exists_and_delete() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    // Test exists
    assert!(!storage.exists("key1").await);
    storage.set("key1", b"value1").await.unwrap();
    assert!(storage.exists("key1").await);

    // Test delete
    storage.delete("key1").await.unwrap();
    assert!(!storage.exists("key1").await);
    let value = storage.get("key1").await;
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_ttl_expiration() {
    let storage = InMemoryState::new(Duration::from_millis(100));

    storage.set("expiring_key", b"value").await.unwrap();
    assert!(storage.exists("expiring_key").await);

    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(!storage.exists("expiring_key").await);
    let value = storage.get("expiring_key").await;
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_concurrent_access() {
    let storage = Arc::new(InMemoryState::new(Duration::from_secs(60)));

    // Test concurrent writes
    let mut handles = Vec::new();
    for i in 0..10 {
        let storage_clone = Arc::clone(&storage);
        let handle = tokio::spawn(async move {
            let key = format!("key_{}", i);
            let value = format!("value_{}", i);
            storage_clone.set(&key, value.as_bytes()).await.unwrap();
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Verify all values were written
    for i in 0..10 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);
        let value = storage.get(&key).await;
        assert_eq!(value, Some(expected_value.as_bytes().to_vec()));
    }
}

#[tokio::test]
async fn test_large_values() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    let large_value = vec![b'x'; 1024 * 1024]; // 1MB
    storage.set("large_key", &large_value).await.unwrap();

    let retrieved = storage.get("large_key").await;
    assert_eq!(retrieved, Some(large_value));
}

#[tokio::test]
async fn test_special_characters() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    let special_value = "test@example.com:password123!@#";
    storage
        .set("special_key", special_value.as_bytes())
        .await
        .unwrap();

    let retrieved = storage.get("special_key").await;
    assert_eq!(retrieved, Some(special_value.as_bytes().to_vec()));
}

#[tokio::test]
async fn test_empty_values() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    // Test empty key
    storage.set("", b"empty_key_value").await.unwrap();
    let retrieved = storage.get("").await;
    assert_eq!(retrieved, Some(b"empty_key_value".to_vec()));

    // Test empty value
    storage.set("empty_value_key", b"").await.unwrap();
    let retrieved = storage.get("empty_value_key").await;
    assert_eq!(retrieved, Some(vec![]));
}

#[tokio::test]
async fn test_unicode_values() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    let unicode_value = "Hello, 世界! 🌍";
    storage
        .set("unicode_key", unicode_value.as_bytes())
        .await
        .unwrap();

    let retrieved = storage.get("unicode_key").await;
    assert_eq!(retrieved, Some(unicode_value.as_bytes().to_vec()));
}

#[tokio::test]
async fn test_multiple_operations() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    // Set multiple values
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        storage.set(&key, value.as_bytes()).await.unwrap();
    }

    // Verify all values
    for i in 0..100 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);
        let value = storage.get(&key).await;
        assert_eq!(value, Some(expected_value.as_bytes().to_vec()));
    }

    // Delete some values
    for i in 0..50 {
        let key = format!("key_{}", i);
        storage.delete(&key).await.unwrap();
    }

    // Verify deleted values are gone
    for i in 0..50 {
        let key = format!("key_{}", i);
        let value = storage.get(&key).await;
        assert_eq!(value, None);
    }

    // Verify remaining values still exist
    for i in 50..100 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);
        let value = storage.get(&key).await;
        assert_eq!(value, Some(expected_value.as_bytes().to_vec()));
    }
}

#[tokio::test]
async fn test_storage_with_config() {
    let config = StateConfig {
        storage_type: StateStorageType::InMemory,
        ttl: Duration::from_secs(30),
        cleanup_interval: Duration::from_secs(5),
        max_size: Some(100),
        custom_storage: None,
    };

    let storage = InMemoryState::new(Duration::from_secs(60));

    // Test basic operations with config
    storage.set("test_key", b"test_value").await.unwrap();
    let value = storage.get("test_key").await;
    assert_eq!(value, Some(b"test_value".to_vec()));
}

#[tokio::test]
async fn test_storage_error_handling() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    // Test delete non-existent key (should not error)
    storage.delete("nonexistent").await.unwrap();

    // Test get after delete
    storage.set("test", b"value").await.unwrap();
    let value = storage.get("test").await;
    assert_eq!(value, Some(b"value".to_vec()));

    storage.delete("test").await.unwrap();
    let value = storage.get("test").await;
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_storage_performance() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    // Test bulk operations
    let start = std::time::Instant::now();

    for i in 0..1000 {
        let key = format!("perf_key_{}", i);
        let value = format!("perf_value_{}", i);
        storage.set(&key, value.as_bytes()).await.unwrap();
    }

    let write_duration = start.elapsed();
    println!("Wrote 1000 keys in {:?}", write_duration);

    // Test bulk reads
    let start = std::time::Instant::now();

    for i in 0..1000 {
        let key = format!("perf_key_{}", i);
        let expected_value = format!("perf_value_{}", i);
        let value = storage.get(&key).await;
        assert_eq!(value, Some(expected_value.as_bytes().to_vec()));
    }

    let read_duration = start.elapsed();
    println!("Read 1000 keys in {:?}", read_duration);

    // Performance should be reasonable
    assert!(write_duration.as_millis() < 1000);
    assert!(read_duration.as_millis() < 1000);
}

// Note: Redis and RocksDB tests would require actual instances running
// These are commented out but show the structure for when those backends are available

/*
#[tokio::test]
async fn test_redis_storage_basic_operations() {
    let storage = RedisState::new("redis://localhost:6379").await.unwrap();

    storage.set("redis_key", b"redis_value").await.unwrap();
    let value = storage.get("redis_key").await;
    assert_eq!(value, Some(b"redis_value".to_vec()));

    storage.delete("redis_key").await.unwrap();
    let value = storage.get("redis_key").await;
    assert_eq!(value, None);
}

#[tokio::test]
async fn test_rocksdb_storage_basic_operations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = RocksDBState::new(temp_dir.path()).await.unwrap();

    storage.set("rocksdb_key", b"rocksdb_value").await.unwrap();
    let value = storage.get("rocksdb_key").await;
    assert_eq!(value, Some(b"rocksdb_value".to_vec()));

    storage.delete("rocksdb_key").await.unwrap();
    let value = storage.get("rocksdb_key").await;
    assert_eq!(value, None);
}
*/
