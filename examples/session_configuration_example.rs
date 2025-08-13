use rs2_stream::session::{SessionBuilder, SessionPreset, set_global_session, get_global_parallel_config};
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::from_iter_rs2;
use rs2_stream::media::types::QualityLevel;
use rs2_stream::stream::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("=== RS2 Session Configuration Example ===\n");

    // Example 1: Basic session configuration
    println!("1. Basic Session Configuration:");
    let basic_session = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 4;
            p.max_buffer_size = 1000;
            p.task_timeout = Duration::from_secs(30);
        })
        .backpressure(|b| {
            b.strategy = rs2_stream::BackpressureStrategy::Block;
            b.buffer_size = 500;
        })
        .build();

    println!("   - Parallel concurrency: {}", basic_session.parallel.concurrency);
    println!("   - Max buffer size: {}", basic_session.parallel.max_buffer_size);
    println!("   - Backpressure strategy: {:?}", basic_session.backpressure.strategy);
    println!("   - Backpressure buffer size: {}", basic_session.backpressure.buffer_size);

    // Example 2: Using presets
    println!("\n2. Session Presets:");
    
    let dev_session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    println!("   - Development preset - Concurrency: {}", dev_session.parallel.concurrency);

    let prod_session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .build();
    println!("   - Production preset - Concurrency: {}", prod_session.parallel.concurrency);

    let high_perf_session = SessionBuilder::new()
        .preset(SessionPreset::HighPerformance)
        .build();
    println!("   - High Performance preset - Concurrency: {}", high_perf_session.parallel.concurrency);

    let low_mem_session = SessionBuilder::new()
        .preset(SessionPreset::LowMemory)
        .build();
    println!("   - Low Memory preset - Concurrency: {}", low_mem_session.parallel.concurrency);

    // Example 3: Advanced configuration with all components
    println!("\n3. Advanced Configuration:");
    let advanced_session = SessionBuilder::new()
        .parallel(|p| {
            p.concurrency = 8;
            p.max_buffer_size = 2000;
            p.task_timeout = Duration::from_secs(60);
        })
        .backpressure(|b| {
            b.strategy = rs2_stream::BackpressureStrategy::Block;
            b.buffer_size = 1000;
            b.low_watermark = Some(200);
            b.high_watermark = Some(800);
        })
        .chunk_processor(|c| {
            c.max_buffer_size = 2048;
            c.parallel_processing = 4;
            c.max_retries = 3;
        })
        .encoding(|e| {
            e.quality = QualityLevel::High;
            e.target_bitrate = 2_000_000; // 2 Mbps
            e.keyframe_interval = 60;
        })
        .resource(|r| {
            r.max_memory_bytes = 2 * 1024 * 1024 * 1024; // 2GB
            r.max_keys = 50_000;
            r.memory_threshold_percent = 75;
        })
        .pipeline(|p| {
            p.name = "example-pipeline".to_string();
            p.buffer_size = 2000;
            p.enable_metrics = true;
        })
        .stream_buffer(|b| {
            b.initial_capacity = 2048;
            b.max_capacity = Some(50 * 1024 * 1024); // 50MB
        })
        .metrics(|m| {
            m.enabled = true;
            m.sample_rate = 0.1; // 10% sampling
        })
        .build();

    println!("   - Pipeline name: {}", advanced_session.pipeline.name);
    println!("   - Chunk processor max buffer: {}", advanced_session.chunk_processor.max_buffer_size);
    println!("   - Encoding quality: {:?}", advanced_session.encoding.quality);
    println!("   - Resource max memory: {} GB", advanced_session.resource.max_memory_bytes / (1024 * 1024 * 1024));
    println!("   - Metrics enabled: {}", advanced_session.metrics.enabled);

    // Example 4: Global session usage
    println!("\n4. Global Session Usage:");
    
    // Set the global session
    set_global_session(advanced_session.clone());
    
    // Retrieve configuration from global session
    if let Some(parallel_config) = get_global_parallel_config() {
        println!("   - Global parallel concurrency: {}", parallel_config.concurrency);
        println!("   - Global max buffer size: {}", parallel_config.max_buffer_size);
    }

    // Example 5: Using the configuration with streams
    println!("\n5. Stream Processing with Session Config:");
    
    // Create a stream and process it using the session configuration
    let numbers: Vec<usize> = (1..=100).collect();
    let stream = from_iter_rs2(numbers)
        .par_eval_map_rs2(Some(4), |x| async move {
            // Simulate some async work
            tokio::time::sleep(Duration::from_millis(10)).await;
            x * 2
        });

    let start = std::time::Instant::now();
    let results: Vec<usize> = stream.collect().await;
    let duration = start.elapsed();

    println!("   - Processed {} items in {:?}", results.len(), duration);
    println!("   - First few results: {:?}", &results[..5.min(results.len())]);

    // Example 6: Session comparison
    println!("\n6. Session Comparison:");
    println!("   - Basic session concurrency: {}", basic_session.parallel.concurrency);
    println!("   - Advanced session concurrency: {}", advanced_session.parallel.concurrency);
    println!("   - Development preset concurrency: {}", dev_session.parallel.concurrency);
    println!("   - Production preset concurrency: {}", prod_session.parallel.concurrency);

    println!("\n=== Session Configuration Example Complete ===");
} 