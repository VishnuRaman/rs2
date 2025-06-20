use rs2_stream::state::traits::StateStorageType;
use rs2_stream::state::{
    CustomKeyExtractor, InMemoryState, KeyExtractor, StateConfig, StateError, StateResult,
    StateStorage,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestData {
    id: u64,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestState {
    count: u64,
    total: f64,
}

#[test]
fn test_key_extractor_trait() {
    let extractor = CustomKeyExtractor::new(|data: &TestData| format!("key_{}", data.id));

    let data = TestData {
        id: 42,
        value: "test".to_string(),
    };

    let key = extractor.extract_key(&data);
    assert_eq!(key, "key_42");
}

#[test]
fn test_custom_key_extractor_with_string() {
    let extractor = CustomKeyExtractor::new(|data: &TestData| data.value.clone());

    let data = TestData {
        id: 1,
        value: "user_123".to_string(),
    };

    let key = extractor.extract_key(&data);
    assert_eq!(key, "user_123");
}

#[test]
fn test_custom_key_extractor_with_complex_key() {
    let extractor =
        CustomKeyExtractor::new(|data: &TestData| format!("{}:{}", data.id, data.value));

    let data = TestData {
        id: 100,
        value: "session_abc".to_string(),
    };

    let key = extractor.extract_key(&data);
    assert_eq!(key, "100:session_abc");
}

#[tokio::test]
async fn test_state_storage_trait_implementation() {
    let storage = InMemoryState::new(Duration::from_secs(60));

    let key = "test_key";
    let value = TestState {
        count: 42,
        total: 100.5,
    };

    // Test set and get
    let bytes = serde_json::to_vec(&value).unwrap();
    storage.set(key, &bytes).await.unwrap();

    let retrieved_bytes = storage.get(key).await.unwrap();
    let retrieved: TestState = serde_json::from_slice(&retrieved_bytes).unwrap();
    assert_eq!(retrieved, value);

    // Test exists
    assert!(storage.exists(key).await);

    // Test delete
    storage.delete(key).await.unwrap();
    assert!(!storage.exists(key).await);
    assert!(storage.get(key).await.is_none());
}

#[test]
fn test_state_error_variants() {
    // Test serialization error
    let serialization_error =
        StateError::Serialization(serde_json::from_str::<u32>("not a number").unwrap_err());
    match serialization_error {
        StateError::Serialization(_) => (),
        _ => panic!("Expected Serialization variant"),
    }

    // Test storage error
    let storage_error = StateError::Storage("Storage error".to_string());
    match storage_error {
        StateError::Storage(_) => assert!(true),
        _ => assert!(false, "Expected Storage error"),
    }

    // Test validation error
    let validation_error = StateError::Validation("Invalid config".to_string());
    match validation_error {
        StateError::Validation(_) => assert!(true),
        _ => assert!(false, "Expected Validation error"),
    }
}

#[test]
fn test_state_error_display() {
    let error = StateError::Storage("Connection failed".to_string());
    let error_string = format!("{}", error);
    assert!(error_string.contains("Connection failed"));
}

#[test]
fn test_state_config_default() {
    let config = StateConfig::default();
    assert_eq!(config.storage_type, StateStorageType::InMemory);
    assert_eq!(config.ttl, Duration::from_secs(24 * 60 * 60)); // 24 hours
    assert_eq!(config.cleanup_interval, Duration::from_secs(5 * 60)); // 5 minutes
    assert_eq!(config.max_size, None);
}

#[test]
fn test_state_config_validation() {
    let mut config = StateConfig::default();

    // Valid config
    assert!(config.validate().is_ok());

    // Invalid TTL
    config.ttl = Duration::from_secs(0);
    assert!(config.validate().is_err());

    // Reset TTL and test invalid cleanup interval
    config.ttl = Duration::from_secs(60 * 60);
    config.cleanup_interval = Duration::from_secs(2 * 60 * 60); // 2 hours
    assert!(config.validate().is_err());
}

#[test]
fn test_state_config_builder_pattern() {
    let config = StateConfig {
        storage_type: StateStorageType::InMemory,
        ttl: Duration::from_secs(2 * 60 * 60), // 2 hours
        cleanup_interval: Duration::from_secs(5 * 60), // 5 minutes
        max_size: Some(1000),
        custom_storage: None,
    };
    assert_eq!(config.storage_type, StateStorageType::InMemory);
    assert_eq!(config.ttl, Duration::from_secs(2 * 60 * 60));
    assert_eq!(config.cleanup_interval, Duration::from_secs(5 * 60));
    assert_eq!(config.max_size, Some(1000));
}

#[test]
fn test_state_result_type() {
    // Test Ok result
    let ok_result: StateResult<()> = Ok(());
    assert!(ok_result.is_ok());

    // Test Err result
    let err_result: StateResult<()> = Err(StateError::Storage("Test error".to_string()));
    assert!(err_result.is_err());

    match err_result {
        Err(StateError::Storage(msg)) => assert_eq!(msg, "Test error"),
        _ => assert!(false, "Expected Storage error"),
    }
}

#[test]
fn test_key_extractor_with_different_types() {
    // Test with integer key
    let int_extractor = CustomKeyExtractor::new(|data: &TestData| data.id.to_string());

    let data = TestData {
        id: 123,
        value: "test".to_string(),
    };

    let key = int_extractor.extract_key(&data);
    assert_eq!(key, "123");

    // Test with hash-based key
    let hash_extractor = CustomKeyExtractor::new(|data: &TestData| {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        data.id.hash(&mut hasher);
        data.value.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    });

    let key = hash_extractor.extract_key(&data);
    assert!(!key.is_empty());
    assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
}

#[test]
fn test_state_storage_type_enum() {
    // Test InMemory variant
    match StateStorageType::InMemory {
        StateStorageType::InMemory => assert!(true),
        StateStorageType::Custom => assert!(false, "Expected InMemory variant"),
    }

    // Test Custom variant
    match StateStorageType::Custom {
        StateStorageType::InMemory => assert!(false, "Expected Custom variant"),
        StateStorageType::Custom => assert!(true),
    }

    // Test enum comparison
    assert_eq!(StateStorageType::InMemory, StateStorageType::InMemory);
    assert_eq!(StateStorageType::Custom, StateStorageType::Custom);
    assert_ne!(StateStorageType::InMemory, StateStorageType::Custom);
}

#[test]
fn test_state_error_clone() {
    let error = StateError::Storage("Test error".to_string());
    let cloned_error = error.clone();

    match (error, cloned_error) {
        (StateError::Storage(msg1), StateError::Storage(msg2)) => {
            assert_eq!(msg1, msg2);
        }
        _ => assert!(false, "Expected Storage errors"),
    }
}

#[test]
fn test_state_error_debug() {
    let error = StateError::Validation("Invalid configuration".to_string());
    let debug_string = format!("{:?}", error);
    assert!(debug_string.contains("Validation"));
    assert!(debug_string.contains("Invalid configuration"));
}
