//! Collecting a stream, bounding that collection, and measuring throughput.
//!
//! Run with: `cargo run --example collecting_and_metrics`

use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use rs2_stream::stream_configuration::{BufferConfig, GrowthStrategy};
use rs2_stream::stream_performance_metrics::HealthThresholds;

#[tokio::main]
async fn main() {
    println!("=== collect_rs2: collect everything ===");
    let all: Vec<u32> = from_iter(0..1000u32).collect_rs2().await;
    assert_eq!(all.len(), 1000, "collect_rs2 must not truncate");
    println!("  {} items\n", all.len());

    println!("=== try_collect_bounded_rs2: cap memory, and be TOLD on overflow ===");
    let ok = from_iter(0..5u32).try_collect_bounded_rs2::<Vec<_>>(10).await;
    println!("  under the limit -> {:?}", ok.map(|v| v.len()));

    let over = from_iter(0..100u32).try_collect_bounded_rs2::<Vec<_>>(10).await;
    match over {
        Ok(v) => println!("  unexpectedly ok with {} items", v.len()),
        Err(e) => println!("  over the limit -> Err: {}", e),
    }
    println!("     (an earlier version silently truncated instead)\n");

    println!("=== collect_vec_with_config_rs2: pre-size the buffer ===");
    let config = BufferConfig {
        initial_capacity: 4096,
        max_capacity: None,
        growth_strategy: GrowthStrategy::Exponential(2.0),
    };
    let v = from_iter(0..1000u32)
        .collect_vec_with_config_rs2(config)
        .await
        .expect("growing strategies never error");
    println!("  {} items, capacity {}\n", v.len(), v.capacity());

    println!("=== GrowthStrategy::Fixed: a hard cap that ERRORS, never truncates ===");
    let config = BufferConfig {
        initial_capacity: 16,
        max_capacity: None,
        growth_strategy: GrowthStrategy::Fixed,
    };
    match from_iter(0..1000u32).collect_vec_with_config_rs2(config).await {
        Ok(v) => println!("  unexpectedly ok with {} items", v.len()),
        Err(e) => println!("  -> Err: {}", e),
    }
    println!();

    println!("=== with_metrics_rs2: items and timing (bytes stay 0) ===");
    let (stream, metrics) = from_iter(0..500u32)
        .map_rs2(|x| x.to_string())
        .with_metrics_rs2("unsized".to_string(), HealthThresholds::default());
    let _ = stream.collect::<Vec<_>>().await;
    let m = metrics.lock().await;
    println!("  items={} bytes={} healthy={}", m.items_processed, m.bytes_processed, m.is_healthy());
    println!("     (bytes is 0 by design — a generic stream cannot know an item's real size)");
    std::mem::drop(m);
    println!();

    println!("=== with_metrics_sized_rs2: supply a sizing function for real bytes ===");
    let words = vec!["a".to_string(), "bb".to_string(), "cccc".to_string()];
    let expected: usize = words.iter().map(|s| s.len()).sum();
    let (stream, metrics) = from_iter(words).with_metrics_sized_rs2(
        "sized".to_string(),
        HealthThresholds::default(),
        |s: &String| s.len() as u64,
    );
    let _ = stream.collect::<Vec<_>>().await;
    let m = metrics.lock().await;
    println!("  items={} bytes={} (expected {})", m.items_processed, m.bytes_processed, expected);
    println!("  {}", m.throughput_summary());
}
