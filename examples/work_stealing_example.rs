use rs2::rs2::*;
use rs2::work_stealing::{WorkStealingExt, WorkStealingConfig};
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;

// Simple task that simulates light work
async fn simple_task(x: usize) -> usize {
    // Simulate varying workloads with much shorter delays
    let delay = if x % 5 == 0 {
        Duration::from_millis(2) // Some items take slightly longer
    } else {
        Duration::from_millis(1) // Most items are fast
    };

    tokio::time::sleep(delay).await;

    // Light computation
    x * 2
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🚀 Work Stealing Example");
        println!("========================");

        // Use fewer items for faster execution
        let item_count = 10;
        let source_data: Vec<usize> = (0..item_count).collect();

        println!("Processing {} items...\n", item_count);

        // 1. Regular sequential processing
        println!("1️⃣ Sequential processing:");
        let start = std::time::Instant::now();

        let results = from_iter(source_data.clone())
            .eval_map_rs2(|x| simple_task(x))
            .collect::<Vec<_>>()
            .await;

        println!("   ⏱️  Sequential: {:?}", start.elapsed());
        println!("   📊 Results: {:?}\n", results);

        // 2. Regular parallel processing
        println!("2️⃣ Regular parallel processing:");
        let start = std::time::Instant::now();

        let results = from_iter(source_data.clone())
            .par_eval_map_rs2(2, |x| simple_task(x))
            .collect::<Vec<_>>()
            .await;

        println!("   ⏱️  Parallel: {:?}", start.elapsed());
        println!("   📊 Results: {:?}\n", results);

        // 3. Work stealing (default config)
        println!("3️⃣ Work stealing (default config):");
        let start = std::time::Instant::now();

        let results = from_iter(source_data.clone())
            .par_eval_map_work_stealing_rs2(|x| simple_task(x))
            .collect::<Vec<_>>()
            .await;

        println!("   ⏱️  Work stealing: {:?}", start.elapsed());
        println!("   📊 Results: {:?}\n", results);

        // 4. Work stealing with custom config (optimized for this small example)
        println!("4️⃣ Work stealing (custom config):");
        let config = WorkStealingConfig {
            num_workers: Some(2),     // Just 2 workers for small dataset
            local_queue_size: 2,      // Small queue for aggressive sharing
            steal_interval_ms: 1,     // Aggressive stealing
            use_blocking: false,      // No blocking for light tasks
        };

        let start = std::time::Instant::now();

        let results = from_iter(source_data.clone())
            .par_eval_map_work_stealing_with_config_rs2(config, |x| simple_task(x))
            .collect::<Vec<_>>()
            .await;

        println!("   ⏱️  Work stealing (custom): {:?}", start.elapsed());
        println!("   📊 Results: {:?}\n", results);

        println!("✨ Work stealing benefits:");
        println!("   • Automatic load balancing");
        println!("   • Better CPU utilization");
        println!("   • Handles uneven workloads efficiently");
        println!("   • Scales with available cores");
    });
}