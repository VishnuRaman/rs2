use rs2_stream::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("Creating a stream that emits a value every 500ms using tick_rs extension method");
        
        // Create an empty stream and use the tick_rs extension method
        let stream = empty::<i32>()
            .tick_rs(Duration::from_millis(500), "Tick!");
        
        println!("Stream created. Collecting 5 values...");
        
        // Take only 5 items to keep the example short
        let mut count = 0;
        let mut stream = stream.take(5).boxed();
        
        while let Some(item) = stream.next().await {
            count += 1;
            println!("Received item {}: {}", count, item);
        }
        
        println!("Example completed!");
    });
}