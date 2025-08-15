use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::stream::constructors::from_iter;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    println!("Async Filter Map Example");
    println!("========================");

    // Create a stream of numbers
    let numbers = from_iter(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Use async filter_map to filter even numbers and double them
    let result = numbers
        .filter_map_async_rs2(|num| async move {
            // Simulate some async work
            sleep(Duration::from_millis(10)).await;
            
            // Only keep even numbers and double them
            if num % 2 == 0 {
                Some(num * 2)
            } else {
                None
            }
        })
        .collect_rs2()
        .await;

    println!("Original numbers: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]");
    println!("After async filter_map (even numbers doubled): {:?}", result);
    println!("Expected: [4, 8, 12, 16, 20]");

    // Example with schema validation simulation
    println!("\nSchema Validation Simulation");
    println!("============================");

    let data = from_iter(vec!["valid", "invalid", "valid", "error", "valid"]);

    let validated = data
        .filter_map_async_rs2(|item| async move {
            // Simulate async validation
            sleep(Duration::from_millis(5)).await;
            
            match item {
                "valid" => Some(format!("✅ {}", item)),
                "invalid" => {
                    println!("❌ Invalid item: {}", item);
                    None
                }
                "error" => {
                    println!("💥 Error processing: {}", item);
                    None
                }
                _ => Some(format!("✅ {}", item)),
            }
        })
        .collect_rs2()
        .await;

    println!("Validation results: {:?}", validated);
} 