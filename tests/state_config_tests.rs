use rs2_stream::state::config::{StateConfigBuilder, StateConfigs};
use rs2_stream::state::traits::StateStorageType;
use rs2_stream::state::StateConfig;
use rs2_stream::state::StateStorage;
use std::time::Duration;

#[tokio::test]
async fn test_state_config_default() {
    let config = StateConfig::default();

    assert_eq!(config.ttl, Duration::from_secs(24 * 60 * 60)); // 24 hours
    assert_eq!(config.cleanup_interval, Duration::from_secs(5 * 60)); // 5 minutes
    assert_eq!(config.max_size, None);
}

#[tokio::test]
async fn test_state_config_new() {
    let config = StateConfig::new();

    assert_eq!(config.ttl, Duration::from_secs(24 * 60 * 60)); // 24 hours
    assert_eq!(config.cleanup_interval, Duration::from_secs(5 * 60)); // 5 minutes
    assert_eq!(config.max_size, None);
}

#[tokio::test]
async fn test_state_config_builder() {
    let config = StateConfigBuilder::new()
        .ttl(Duration::from_secs(3600))
        .cleanup_interval(Duration::from_secs(300))
        .max_size(1000)
        .build()
        .unwrap();

    assert_eq!(config.ttl, Duration::from_secs(3600));
    assert_eq!(config.cleanup_interval, Duration::from_secs(300));
    assert_eq!(config.max_size, Some(1000));
}

#[tokio::test]
async fn test_state_config_create_storage() {
    let config = StateConfig::new();
    let storage = config.create_storage();

    // Test that storage can be created and used
    storage.set("test_key", b"test_value").await.unwrap();
    let value = storage.get("test_key").await;
    assert_eq!(value, Some(b"test_value".to_vec()));
}

#[tokio::test]
async fn test_state_configs_presets() {
    // Test high performance config
    let high_perf = StateConfigs::high_performance();
    assert_eq!(high_perf.ttl, Duration::from_secs(60 * 60)); // 1 hour
    assert_eq!(high_perf.cleanup_interval, Duration::from_secs(60)); // 1 minute
    assert_eq!(high_perf.max_size, Some(10000));

    // Test session config
    let session = StateConfigs::session();
    assert_eq!(session.ttl, Duration::from_secs(30 * 60)); // 30 minutes
    assert_eq!(session.cleanup_interval, Duration::from_secs(5 * 60)); // 5 minutes
    assert_eq!(session.max_size, Some(1000));

    // Test short-lived config
    let short_lived = StateConfigs::short_lived();
    assert_eq!(short_lived.ttl, Duration::from_secs(5 * 60)); // 5 minutes
    assert_eq!(short_lived.cleanup_interval, Duration::from_secs(30)); // 30 seconds
    assert_eq!(short_lived.max_size, Some(100));

    // Test long-lived config
    let long_lived = StateConfigs::long_lived();
    assert_eq!(long_lived.ttl, Duration::from_secs(7 * 24 * 60 * 60)); // 7 days
    assert_eq!(long_lived.cleanup_interval, Duration::from_secs(60 * 60)); // 1 hour
    assert_eq!(long_lived.max_size, Some(100000));
}

#[tokio::test]
async fn test_state_config_validation() {
    let mut config = StateConfig::default();

    // Valid config should pass
    assert!(config.validate().is_ok());

    // Test zero TTL
    config.ttl = Duration::from_secs(0);
    assert!(config.validate().is_err());

    // Test zero cleanup interval
    config.ttl = Duration::from_secs(3600);
    config.cleanup_interval = Duration::from_secs(0);
    assert!(config.validate().is_err());

    // Test cleanup interval greater than TTL
    config.cleanup_interval = Duration::from_secs(7200); // 2 hours
    config.ttl = Duration::from_secs(3600); // 1 hour
    assert!(config.validate().is_err());

    // Test zero max size
    config.cleanup_interval = Duration::from_secs(1800); // 30 minutes
    config.max_size = Some(0);
    assert!(config.validate().is_err());

    // Test valid max size
    config.max_size = Some(1000);
    assert!(config.validate().is_ok());
}

#[tokio::test]
async fn test_state_config_validation_edge_cases() {
    let mut config = StateConfig::default();

    // Test with very small but valid TTL and cleanup interval
    config.ttl = Duration::from_nanos(100);
    config.cleanup_interval = Duration::from_nanos(50);
    assert!(config.validate().is_ok());

    // Test with cleanup interval equal to TTL - this should be valid
    config.ttl = Duration::from_secs(60);
    config.cleanup_interval = Duration::from_secs(60);
    assert!(config.validate().is_ok());

    // Test with cleanup interval slightly less than TTL
    config.cleanup_interval = Duration::from_secs(59);
    assert!(config.validate().is_ok());
}

#[tokio::test]
async fn test_state_config_max_size_validation() {
    let mut config = StateConfig::default();

    // Test with zero max_size
    config.max_size = Some(0);
    assert!(config.validate().is_err());

    // Test with valid max_size
    config.max_size = Some(1000);
    assert!(config.validate().is_ok());

    // Test with None max_size (unlimited)
    config.max_size = None;
    assert!(config.validate().is_ok());
}

#[tokio::test]
async fn test_state_config_clone() {
    let config = StateConfig {
        ttl: Duration::from_secs(3600),
        cleanup_interval: Duration::from_secs(300),
        max_size: Some(1000),
        storage_type: StateStorageType::InMemory,
        custom_storage: None,
    };

    let cloned_config = config.clone();

    assert_eq!(config.ttl, cloned_config.ttl);
    assert_eq!(config.cleanup_interval, cloned_config.cleanup_interval);
    assert_eq!(config.max_size, cloned_config.max_size);
}

#[tokio::test]
async fn test_state_config_debug() {
    let config = StateConfig {
        ttl: Duration::from_secs(3600),
        cleanup_interval: Duration::from_secs(300),
        max_size: Some(1000),
        storage_type: StateStorageType::InMemory,
        custom_storage: None,
    };

    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("3600s"));
    assert!(debug_str.contains("300s"));
    assert!(debug_str.contains("1000"));
}

#[tokio::test]
async fn test_state_config_builder_methods() {
    let config = StateConfigBuilder::new()
        .ttl(Duration::from_secs(1800))
        .cleanup_interval(Duration::from_secs(150))
        .max_size(500)
        .build()
        .unwrap();

    assert_eq!(config.ttl, Duration::from_secs(1800));
    assert_eq!(config.cleanup_interval, Duration::from_secs(150));
    assert_eq!(config.max_size, Some(500));
}

#[tokio::test]
async fn test_state_config_builder_validation() {
    // Test invalid builder configuration
    let result = StateConfigBuilder::new()
        .ttl(Duration::from_secs(0))
        .build();

    assert!(result.is_err());
}

#[tokio::test]
async fn test_state_config_with_max_size() {
    let config = StateConfig::new().max_size(1000);

    let storage = config.create_storage_arc();

    // Test that max size is enforced
    for i in 0..1000 {
        storage
            .set(&format!("key_{}", i), &[i as u8])
            .await
            .unwrap();
    }

    // The 1001st item should cause eviction
    storage.set("key_1000", &[100]).await.unwrap();

    // Verify that some older items were evicted
    let value = storage.get("key_0").await;
    assert!(value.is_none()); // Should have been evicted
}

#[tokio::test]
async fn test_state_config_ttl_expiration() {
    let config = StateConfig::new().ttl(Duration::from_millis(100));

    let storage = config.create_storage_arc();

    storage.set("test_key", b"test_value").await.unwrap();
    assert!(storage.exists("test_key").await);

    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(150)).await;

    // After expiration, exists() should return false
    assert!(!storage.exists("test_key").await);
    assert!(storage.get("test_key").await.is_none());
}

#[tokio::test]
async fn test_state_config_cleanup_interval() {
    let config = StateConfig::new()
        .ttl(Duration::from_secs(1))
        .cleanup_interval(Duration::from_secs(1));

    // The cleanup interval is used internally by the storage implementation
    // We can't directly test it, but we can verify the config is valid
    assert!(config.validate().is_ok());
}

#[tokio::test]
async fn test_state_config_serialization() {
    // Test that config can be used in serialization contexts
    let config = StateConfig::new();
    let storage = config.create_storage_arc();

    // Test serialization of data through the storage
    let test_data = serde_json::json!({
        "key": "value",
        "number": 42,
        "array": [1, 2, 3]
    });

    let bytes = serde_json::to_vec(&test_data).unwrap();
    storage.set("json_key", &bytes).await.unwrap();

    let retrieved_bytes = storage.get("json_key").await.unwrap();
    let retrieved_data: serde_json::Value = serde_json::from_slice(&retrieved_bytes).unwrap();

    assert_eq!(test_data, retrieved_data);
}

#[tokio::test]
async fn test_state_config_error_handling() {
    let config = StateConfig::new();
    let storage = config.create_storage_arc();

    // Test that storage operations handle errors gracefully
    storage.set("key1", b"value1").await.unwrap();
    storage.set("key2", b"value2").await.unwrap();

    // Test delete operation
    storage.delete("key1").await.unwrap();
    assert!(!storage.exists("key1").await);
    assert!(storage.exists("key2").await);

    // Test clear operation
    storage.clear().await.unwrap();
    assert!(!storage.exists("key2").await);
}

#[tokio::test]
async fn test_state_config_concurrent_access() {
    let config = StateConfig::new();
    let storage = config.create_storage_arc();

    // Test concurrent access to the same storage
    let mut handles = Vec::new();

    for i in 0..10 {
        let storage_clone = storage.clone();
        let handle = tokio::spawn(async move {
            storage_clone
                .set(&format!("key_{}", i), &[i as u8])
                .await
                .unwrap();
            let value = storage_clone.get(&format!("key_{}", i)).await;
            assert_eq!(value, Some(vec![i as u8]));
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_state_config_performance() {
    let config = StateConfig::new().max_size(10000);

    let storage = config.create_storage_arc();

    // Test performance with many operations
    let start = std::time::Instant::now();

    for i in 0..1000 {
        storage
            .set(&format!("key_{}", i), &[i as u8])
            .await
            .unwrap();
    }

    for i in 0..1000 {
        let value = storage.get(&format!("key_{}", i)).await;
        assert_eq!(value, Some(vec![i as u8]));
    }

    let duration = start.elapsed();
    println!("1000 set/get operations took: {:?}", duration);

    // Should complete in reasonable time (less than 1 second)
    assert!(duration < Duration::from_secs(1));
}

#[tokio::test]
async fn test_state_config_memory_usage() {
    let config = StateConfig::new().max_size(100);

    let storage = config.create_storage_arc();

    // Fill storage to capacity
    for i in 0..100 {
        storage
            .set(&format!("key_{}", i), &vec![i as u8; 1000])
            .await
            .unwrap();
    }

    // Add one more item to trigger eviction
    storage.set("overflow_key", &vec![255; 1000]).await.unwrap();

    // Verify that some items were evicted
    let value = storage.get("key_0").await;
    assert!(value.is_none()); // Should have been evicted due to max size
}

#[tokio::test]
async fn test_state_config_custom_ttl() {
    let custom_ttl = Duration::from_millis(50);
    let config = StateConfig::new().ttl(custom_ttl);

    let storage = config.create_storage_arc();

    storage.set("temp_key", b"temp_value").await.unwrap();
    assert!(storage.exists("temp_key").await);

    // Wait for expiration
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Should be expired
    assert!(!storage.exists("temp_key").await);
}

#[tokio::test]
async fn test_state_config_builder_chain() {
    let config = StateConfigBuilder::new()
        .ttl(Duration::from_secs(3600))
        .cleanup_interval(Duration::from_secs(300))
        .max_size(1000)
        .build()
        .unwrap();

    assert_eq!(config.ttl, Duration::from_secs(3600));
    assert_eq!(config.cleanup_interval, Duration::from_secs(300));
    assert_eq!(config.max_size, Some(1000));
}

#[tokio::test]
async fn test_state_config_default_builder() {
    let builder = StateConfigBuilder::default();
    let config = builder.build().unwrap();

    // Should have default values
    assert_eq!(config.ttl, Duration::from_secs(24 * 60 * 60)); // 24 hours
    assert_eq!(config.cleanup_interval, Duration::from_secs(5 * 60)); // 5 minutes
    assert_eq!(config.max_size, None);
}

#[tokio::test]
async fn test_state_config_storage_types() {
    // Test that we can create storage with different configurations
    let configs = vec![
        StateConfigs::high_performance(),
        StateConfigs::session(),
        StateConfigs::short_lived(),
        StateConfigs::long_lived(),
    ];

    for config in configs {
        let storage = config.create_storage_arc();
        storage.set("test_key", b"test_value").await.unwrap();
        let value = storage.get("test_key").await;
        assert_eq!(value, Some(b"test_value".to_vec()));
    }
}
