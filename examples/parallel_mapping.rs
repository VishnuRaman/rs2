use rs2_stream::stream::constructors::from_iter;
use rs2_stream::stream::StreamExt;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use tokio::runtime::Runtime;

// Define a CPU-intensive operation
fn compute_fibonacci(n: u64) -> u64 {
    match n {
        0 => 0,
        1 => 1,
        _ => compute_fibonacci(n - 1) + compute_fibonacci(n - 2),
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🚀 Demonstrating parallel mapping operations");

        // Create a stream of numbers
        let numbers = (20..30).collect::<Vec<u64>>();
        println!(
            "Processing Fibonacci calculations for numbers: {:?}",
            numbers
        );

        // Example 1: Using map_parallel_rs2 for automatic concurrency
        println!("\n📊 Example 1: Using map_parallel_rs2 with automatic concurrency");
        let start = std::time::Instant::now();

        let results = from_iter(numbers.clone())
            .map_parallel_rs2(Some(4), |n| {
                println!(
                    "  Computing Fibonacci for {} on thread {:?}",
                    n,
                    std::thread::current().id()
                );
                compute_fibonacci(n)
            })
            .collect::<Vec<_>>()
            .await;

        let elapsed = start.elapsed();
        println!("✅ Completed in {:.2?}", elapsed);
        println!("Results: {:?}", results);

        // Example 2: Using map_parallel_with_concurrency_rs2 for custom concurrency
        println!("\n📊 Example 2: Using map_parallel_with_concurrency_rs2 with custom concurrency");
        let concurrency = 2; // Deliberately use fewer threads than available cores
        println!("  Using concurrency level: {}", concurrency);

        let start = std::time::Instant::now();

        let results = from_iter(numbers.clone())
            .map_parallel_with_concurrency_rs2(concurrency, |n| {
                println!(
                    "  Computing Fibonacci for {} on thread {:?}",
                    n,
                    std::thread::current().id()
                );
                compute_fibonacci(n)
            })
            .collect::<Vec<_>>()
            .await;

        let elapsed = start.elapsed();
        println!("✅ Completed in {:.2?}", elapsed);
        println!("Results: {:?}", results);

        // Example 3: Comparing with sequential processing
        println!("\n📊 Example 3: Sequential processing for comparison");
        let start = std::time::Instant::now();

        let results = from_iter(numbers.clone())
            .map_rs2(|n| {
                println!(
                    "  Computing Fibonacci for {} on thread {:?}",
                    n,
                    std::thread::current().id()
                );
                compute_fibonacci(n)
            })
            .collect::<Vec<_>>()
            .await;

        let elapsed = start.elapsed();
        println!("✅ Completed in {:.2?}", elapsed);
        println!("Results: {:?}", results);
    });
}
