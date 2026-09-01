//! The four backpressure strategies, and what each does when the buffer fills.
//!
//! Run with: `cargo run --example backpressure_strategies`

use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::time::{Duration, Instant};

/// A source that produces far faster than the consumer below drains it.
fn fast_source(n: u32) -> RS2Stream<u32> {
    from_iter(0..n)
}

#[tokio::main]
async fn main() {
    println!("=== Block: producer waits for the consumer (nothing is lost) ===");
    let out = auto_backpressure_block(fast_source(50), 8)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(out.len(), 50, "Block must not drop anything");
    println!("  produced 50, received {} — no loss\n", out.len());

    println!("=== DropOldest: keeps the newest, discards the backlog ===");
    // Drain slowly so the buffer genuinely overflows.
    let mut s = auto_backpressure_drop_oldest(fast_source(2000), 16);
    let mut got = Vec::new();
    while let Some(v) = s.next().await {
        got.push(v);
        if got.len() % 4 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    println!("  produced 2000, received {}", got.len());
    println!("  output stays in source order: {}", got.windows(2).all(|w| w[0] < w[1]));
    println!("  last value seen: {:?}\n", got.last());

    println!("=== DropNewest: keeps the backlog, discards new arrivals ===");
    let mut s = auto_backpressure_drop_newest(fast_source(2000), 16);
    let mut got = Vec::new();
    while let Some(v) = s.next().await {
        got.push(v);
        if got.len() % 4 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    println!("  produced 2000, received {}", got.len());
    println!("  first values: {:?}\n", &got[..got.len().min(5)]);

    println!("=== Error strategy (bounded channel) ===");
    let out = auto_backpressure_error(fast_source(20), 8)
        .collect::<Vec<_>>()
        .await;
    println!("  received {}\n", out.len());

    println!("=== via config: pick a strategy declaratively ===");
    for strategy in [
        BackpressureStrategy::Block,
        BackpressureStrategy::DropOldest,
        BackpressureStrategy::DropNewest,
        BackpressureStrategy::Error,
    ] {
        let config = BackpressureConfig {
            strategy,
            buffer_size: 16,
            ..Default::default()
        };
        let start = Instant::now();
        let n = auto_backpressure(fast_source(200), config)
            .collect::<Vec<_>>()
            .await
            .len();
        println!("  {:?}: {} items in {:?}", strategy, n, start.elapsed());
    }

    println!("\nChoosing: Block when every item matters; DropOldest for \
live dashboards where freshness beats completeness; DropNewest to protect an \
already-queued backlog.");
}
