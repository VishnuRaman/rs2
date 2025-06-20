use rs2_stream::{
    rs2_stream_ext::*,
    resource_manager::{get_global_resource_manager},
    from_iter,
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== RS2 Resource Management Example ===\n");

    // Example: Resource Management with Memory Tracking
    // RS2 automatically tracks resources via the global resource manager
    let stream = from_iter(0..1000)
        .map_rs2(|item| {
            // Simulate memory allocation
            let data = vec![item; 1000];
            data.len()
        })
        .collect_rs2::<Vec<_>>()
        .await;

    // Get metrics from the global resource manager
    let global_manager = get_global_resource_manager();
    let metrics = global_manager.get_metrics().await;
    let is_open = global_manager.is_circuit_open().await;
    
    println!("Processed {} items", stream.len());
    println!("Peak memory usage: {} bytes", metrics.peak_memory_bytes);
    println!("Current memory usage: {} bytes", metrics.current_memory_bytes);
    println!("Circuit breaker open: {}", is_open);
    println!("Buffer overflow count: {}", metrics.buffer_overflow_count);
    println!("Circuit breaker trips: {}", metrics.circuit_breaker_trips);

    // Example: Using resource-intensive operations
    println!("\n=== Resource-Intensive Operations ===");
    
    // Group operations automatically track memory
    let grouped_stream = from_iter(0..500)
        .group_by_rs2(|&x| x % 10)
        .collect_rs2::<Vec<_>>()
        .await;
    
    let metrics2 = global_manager.get_metrics().await;
    println!("Grouped {} groups", grouped_stream.len());
    println!("Updated memory usage: {} bytes", metrics2.current_memory_bytes);
    println!("Updated peak memory: {} bytes", metrics2.peak_memory_bytes);

    // Sliding window operations also track memory
    let windowed_stream = from_iter(0..200)
        .sliding_window_rs2(10)
        .collect_rs2::<Vec<_>>()
        .await;
    
    let metrics3 = global_manager.get_metrics().await;
    println!("Created {} windows", windowed_stream.len());
    println!("Final memory usage: {} bytes", metrics3.current_memory_bytes);
    println!("Final peak memory: {} bytes", metrics3.peak_memory_bytes);

    Ok(())
} 