use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use async_stream::stream;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("Demonstrating rate_limit_backpressure extension method");

        // Create a stream of results that produces elements faster than they can be consumed
        let fast_stream = stream! {
            for i in 1..=20 {
                yield Ok::<i32, String>(i);
                // Simulate fast production
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }.boxed();

        println!("Stream created. Applying rate_limit_backpressure with capacity 5...");

        // Apply rate_limit_backpressure with a capacity of 5
        let controlled_stream = fast_stream.rate_limit_backpressure_rs2(5);

        // Process elements with a delay to simulate slow consumption
        println!("Processing elements with a delay to simulate slow consumption...");
        let counter = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let start = std::time::Instant::now();

        let result = controlled_stream
            .eval_map_rs2(move |x| {
                let counter = counter.clone();
                async move {
                    // Simulate slow consumption
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    let count = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    println!("Processed item {}: {:?}", count, x);
                    x
                }
            })
            .collect::<Vec<_>>()
            .await;

        let elapsed = start.elapsed();

        println!("Processed {} elements with backpressure in {:?}", result.len(), elapsed);
        println!("Without backpressure, all elements would have been produced in {:?}", Duration::from_millis(10 * 20));
        println!("With backpressure, the production rate is limited by the consumption rate");
    });
}
