use rs2_stream::resource_manager::{ResourceConfig, ResourceManager};

#[tokio::test]
async fn test_resource_manager_creation() {
    let manager = ResourceManager::new();
    let metrics = manager.get_metrics().await;
    assert_eq!(metrics.current_memory_bytes, 0);
    assert_eq!(metrics.active_keys, 0);
}

#[tokio::test]
async fn test_memory_tracking() {
    let manager = ResourceManager::new();
    
    manager.track_memory_allocation(1000).await.unwrap();
    let metrics = manager.get_metrics().await;
    assert_eq!(metrics.current_memory_bytes, 1000);
    assert_eq!(metrics.peak_memory_bytes, 1000);
    
    manager.track_memory_deallocation(500).await;
    let metrics = manager.get_metrics().await;
    assert_eq!(metrics.current_memory_bytes, 500);
    assert_eq!(metrics.peak_memory_bytes, 1000);
}

#[tokio::test]
async fn test_key_tracking() {
    let manager = ResourceManager::new();
    
    manager.track_key_creation().await.unwrap();
    let metrics = manager.get_metrics().await;
    assert_eq!(metrics.active_keys, 1);
    
    manager.track_key_removal().await;
    let metrics = manager.get_metrics().await;
    assert_eq!(metrics.active_keys, 0);
}

#[tokio::test]
async fn test_circuit_breaker() {
    let manager = ResourceManager::new();
    
    assert!(!manager.is_circuit_open().await);
    
    // Note: trip_circuit_breaker is private, so we'll test the public interface
    // by checking the initial state and metrics
    let metrics = manager.get_metrics().await;
    assert_eq!(metrics.circuit_breaker_trips, 0);
    
    manager.reset_circuit_breaker().await;
    // We can't directly test the internal state, but we can test that the method doesn't panic
}

#[tokio::test]
async fn test_resource_config() {
    let config = ResourceConfig::default();
    assert_eq!(config.max_memory_bytes, 1024 * 1024 * 1024); // 1GB
    assert_eq!(config.max_keys, 100_000);
    assert_eq!(config.memory_threshold_percent, 80);
    assert_eq!(config.buffer_overflow_threshold, 10_000);
}

#[tokio::test]
async fn test_custom_resource_config() {
    let config = ResourceConfig {
        max_memory_bytes: 512 * 1024 * 1024, // 512MB
        max_keys: 50_000,
        memory_threshold_percent: 70,
        buffer_overflow_threshold: 5_000,
        cleanup_interval: std::time::Duration::from_secs(60),
        emergency_cleanup_threshold: 90,
    };
    
    let manager = ResourceManager::with_config(config);
    let metrics = manager.get_metrics().await;
    assert_eq!(metrics.current_memory_bytes, 0);
    assert_eq!(metrics.active_keys, 0);
}

#[tokio::test]
async fn test_buffer_overflow_tracking() {
    let manager = ResourceManager::new();
    
    // Track some buffer overflows
    manager.track_buffer_overflow().await.unwrap();
    manager.track_buffer_overflow().await.unwrap();
    
    let metrics = manager.get_metrics().await;
    assert_eq!(metrics.buffer_overflow_count, 2);
}

#[tokio::test]
async fn test_memory_pressure() {
    let manager = ResourceManager::new();
    
    // Initially no pressure
    assert!(!manager.is_under_memory_pressure().await);
    
    // Allocate memory to trigger pressure - use a large amount to ensure we exceed threshold
    let large_allocation = 1024 * 1024 * 1024; // 1GB
    manager.track_memory_allocation(large_allocation).await.unwrap();
    
    assert!(manager.is_under_memory_pressure().await);
    
    // Check memory pressure percentage
    let pressure = manager.get_memory_pressure().await;
    assert!(pressure > 0);
}

#[tokio::test]
async fn test_resource_tracking() {
    let manager = ResourceManager::new();
    
    // Track a resource
    manager.track_resource("test_resource".to_string(), 1024).await;
    
    // Untrack the resource
    let size = manager.untrack_resource("test_resource").await;
    assert_eq!(size, Some(1024));
    
    // Try to untrack non-existent resource
    let size = manager.untrack_resource("non_existent").await;
    assert_eq!(size, None);
}

#[tokio::test]
async fn test_global_resource_manager() {
    use rs2_stream::resource_manager::get_global_resource_manager;
    
    let manager = get_global_resource_manager();
    let metrics = manager.get_metrics().await;
    
    // Global manager should be initialized with default values
    assert_eq!(metrics.current_memory_bytes, 0);
    assert_eq!(metrics.active_keys, 0);
    assert_eq!(metrics.circuit_breaker_trips, 0);
    assert_eq!(metrics.emergency_cleanups, 0);
} 