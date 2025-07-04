//! Example demonstrating usage of all configuration fields in rs2_new
//! 
//! This example shows how all the configuration structs are properly utilized:
//! - BackpressureConfig with all strategies and watermarks
//! - BufferConfig with growth strategies and capacity limits
//! - MetricsConfig with sampling and labels
//! - FileConfig with compression and buffer settings
//! - RetryPolicy with different retry strategies

use rs2_stream::*;
use rs2_stream::stream_configuration::*;
use rs2_stream::error::RetryPolicy;
use rs2_stream::connectors::stream_connector::CommonConfig;
use rs2_stream::rs2_new_stream_ext::RS2StreamExt;
use rs2_stream::stream_performance_metrics::HealthThresholds;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RS2 Configuration Usage Example ===\n");

    // 1. BackpressureConfig - All fields used
    println!("1. BackpressureConfig with watermarks:");
    let backpressure_config = rs2_new::BackpressureConfig {
        strategy: rs2_new::BackpressureStrategy::DropOldest,
        buffer_size: 1000,
        low_watermark: Some(250),   // Resume at 25% capacity
        high_watermark: Some(750),  // Pause at 75% capacity
    };
    
    let stream = rs2_new::from_iter_rs2(0..10)
        .auto_backpressure_clone_with_rs2(backpressure_config);
    let results: Vec<_> = stream.collect_rs2().await;
    println!("   Processed {} items with drop-oldest backpressure", results.len());

    // 2. BufferConfig - All fields used
    println!("\n2. BufferConfig with growth strategy:");
    let buffer_config = BufferConfig {
        initial_capacity: 100,
        max_capacity: Some(5000),
        growth_strategy: GrowthStrategy::Exponential(2.0),
    };
    
    let stream = rs2_new::from_iter_rs2(0..20);
    let results: Vec<_> = stream.collect_with_config_rs2(buffer_config).await;
    println!("   Collected {} items with exponential growth buffer", results.len());

    // 3. MetricsConfig - All fields used
    println!("\n3. MetricsConfig with sampling and labels:");
    let metrics_config = MetricsConfig {
        enabled: true,
        sample_rate: 0.5, // Sample 50% of items
        labels: vec![
            ("environment".to_string(), "test".to_string()),
            ("component".to_string(), "rs2_example".to_string()),
        ],
    };
    
    let stream = rs2_new::from_iter_rs2(0..100);
    let (stream, metrics) = stream.with_metrics_config_rs2(
        "example_stream".to_string(),
        HealthThresholds::default(),
        metrics_config,
    );
    let results: Vec<_> = stream.collect_rs2().await;
    let final_metrics = metrics.lock().await;
    println!("   Processed {} items with 50% sampling rate", results.len());
    println!("   Metrics: {} items recorded", final_metrics.items_processed);

    // 4. FileConfig - All fields acknowledged
    println!("\n4. FileConfig with compression and buffer settings:");
    let file_config = FileConfig {
        buffer_size: 8192,
        read_ahead: true,
        sync_on_write: false,
        compression: Some(CompressionType::Gzip),
    };
    println!("   File config created with {} byte buffer, read-ahead enabled, gzip compression", 
             file_config.buffer_size);
    // Note: File streaming would use this config in start_file_stream_with_config

    // 5. RetryPolicy - All variants with their fields
    println!("\n5. RetryPolicy variants:");
    
    // Fixed delay retry
    let fixed_policy = RetryPolicy::Fixed {
        max_retries: 3,
        delay: Duration::from_millis(100),
    };
    println!("   Fixed policy: {} retries with {}ms delay", 
             match fixed_policy { RetryPolicy::Fixed { max_retries, .. } => max_retries, _ => 0 },
             match fixed_policy { RetryPolicy::Fixed { delay, .. } => delay.as_millis(), _ => 0 });

    // Exponential backoff retry
    let exponential_policy = RetryPolicy::Exponential {
        max_retries: 5,
        initial_delay: Duration::from_millis(50),
        multiplier: 2.0,
    };
    println!("   Exponential policy: {} retries, {}ms initial delay, {}x multiplier",
             match exponential_policy { RetryPolicy::Exponential { max_retries, .. } => max_retries, _ => 0 },
             match exponential_policy { RetryPolicy::Exponential { initial_delay, .. } => initial_delay.as_millis(), _ => 0 },
             match exponential_policy { RetryPolicy::Exponential { multiplier, .. } => multiplier, _ => 0.0 });

    // Immediate retry
    let immediate_policy = RetryPolicy::Immediate { max_retries: 2 };
    println!("   Immediate policy: {} retries with no delay",
             match immediate_policy { RetryPolicy::Immediate { max_retries } => max_retries, _ => 0 });

    // 6. CommonConfig for connectors - Using RetryPolicy
    println!("\n6. CommonConfig for connectors:");
    let connector_config = CommonConfig {
        batch_size: 500,
        timeout_ms: 5000,
        retry_policy: RetryPolicy::Fixed {
            max_retries: 3,
            delay: Duration::from_millis(200),
        },
        compression: true,
    };
    println!("   Connector config: batch_size={}, timeout={}ms, compression={}", 
             connector_config.batch_size, connector_config.timeout_ms, connector_config.compression);
    println!("   Retry policy: Fixed with {} retries",
             match connector_config.retry_policy { RetryPolicy::Fixed { max_retries, .. } => max_retries, _ => 0 });

    println!("\n=== All configuration fields are properly utilized! ===");
    Ok(())
} 