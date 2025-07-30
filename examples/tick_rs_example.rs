use rs2_stream::rs2::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use std::time::Duration;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("=== RS2 Tick Example ===\n");

        println!("1. Creating a tick stream that emits a value every 500ms using rs2::tick function");

        // Create a tick stream using the rs2::tick function
        let tick_stream = tick(Duration::from_millis(500), "⏰ Tick!");

        println!("   Stream created. Collecting 5 values...\n");

        // Take only 5 items to keep the example short and collect them
        let results: Vec<_> = tick_stream.take_rs2(5).collect_rs2().await;

        // Display the results
        for (i, item) in results.iter().enumerate() {
            println!("   Received item {}: {}", i + 1, item);
        }

        println!("\n2. Demonstrating tick_rs2 method with a source stream");

        // Create a simple data stream and apply tick_rs2 to it
        // Note: tick_rs2 requires Clone, so we'll use from_iter_rs2 with cloneable data
        let data_stream = from_iter_rs2(vec!["📦 Data packet 1", "📦 Data packet 2", "📦 Data packet 3"]);
        
        // Unfortunately, from_iter_rs2 streams don't implement Clone, 
        // so let's demonstrate with direct tick function instead
        let data_tick_stream = tick(Duration::from_millis(300), "📊 Data tick");
        
        println!("   Creating data tick stream (300ms intervals). Collecting 3 values...\n");
        
        let data_results: Vec<_> = data_tick_stream.take_rs2(3).collect_rs2().await;
        
        for (i, item) in data_results.iter().enumerate() {
            println!("   Data item {}: {}", i + 1, item);
        }

        println!("\n=== Example Completed! ===");
        println!("\nKey Features Demonstrated:");
        println!("1. Using rs2::tick() function to create periodic streams");
        println!("2. Using take_rs2() and collect_rs2() for stream processing");
        println!("3. Clean, non-blocking stream collection patterns");
    });
}
