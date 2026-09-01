//! Stream metrics: thresholds, health, and driving a StreamMetrics by hand.
//!
//! Run with: `cargo run --example metrics_deep_dive`

use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use rs2_stream::stream_performance_metrics::{HealthThresholds, StreamMetrics};
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("=== health thresholds: three presets ===");
    let strict = HealthThresholds::strict();
    let default = HealthThresholds::default();
    let relaxed = HealthThresholds::relaxed();
    for (name, t) in [("strict", &strict), ("default", &default), ("relaxed", &relaxed)] {
        println!(
            "  {:8} max_error_rate={:.2} max_consecutive_errors={}",
            name, t.max_error_rate, t.max_consecutive_errors
        );
    }
    let custom = HealthThresholds::custom(0.05, 3);
    println!("  custom   max_error_rate={:.2} max_consecutive_errors={}\n",
        custom.max_error_rate, custom.max_consecutive_errors);

    println!("=== attaching metrics to a stream ===");
    let (stream, metrics) = from_iter(0..200u32)
        .with_metrics_rs2("pipeline-a".to_string(), HealthThresholds::strict());
    let _ = stream.collect::<Vec<_>>().await;
    let m = metrics.lock().await;
    println!("  name={:?} items={}", m.name, m.items_processed);
    println!("  {}", m.throughput_summary());
    println!("  finalized throughput: {}", m.throughput_summary_processing_time());
    println!("  healthy: {}\n", m.is_healthy());
    std::mem::drop(m);

    println!("=== byte accounting needs a sizing function ===");
    let words = vec!["alpha".to_string(), "beta".to_string(), "gamma!!".to_string()];
    let expected: usize = words.iter().map(|s| s.len()).sum();
    let (stream, metrics) = from_iter(words).with_metrics_sized_rs2(
        "sized".to_string(),
        HealthThresholds::default(),
        |s: &String| s.len() as u64,
    );
    let _ = stream.collect::<Vec<_>>().await;
    let m = metrics.lock().await;
    assert_eq!(m.bytes_processed, expected as u64, "sized metrics report real bytes");
    println!("  bytes={} (expected {}), avg item {:.1} bytes",
        m.bytes_processed, expected, m.average_item_size);
    std::mem::drop(m);
    println!();

    println!("=== driving a StreamMetrics directly ===");
    // Useful when instrumenting something that is not an RS2 stream.
    let mut m = StreamMetrics::new().with_name("manual".to_string());
    m.set_health_thresholds(HealthThresholds::custom(0.25, 5));

    for i in 0..10 {
        if i % 5 == 4 {
            m.record_error();
        } else {
            m.record_item(64);
        }
    }
    m.record_retry();
    m.record_backpressure();
    m.record_processing_time(Duration::from_millis(12));
    m.update_queue_depth(7);
    m.update_derived_metrics();

    println!("  items={} errors={} retries={} backpressure={} queue_depth={}",
        m.items_processed, m.errors, m.retries, m.backpressure_events, m.queue_depth);
    println!("  error_rate={:.2} consecutive_errors={}", m.error_rate, m.consecutive_errors);
    println!("  healthy under 0.25 threshold: {}", m.is_healthy());

    // Tighten the thresholds and the same numbers become unhealthy.
    m.set_health_thresholds(HealthThresholds::strict());
    println!("  healthy under strict thresholds: {}\n", m.is_healthy());

    println!("=== finalize(): stop the clock and settle derived values ===");
    let mut m = StreamMetrics::new();
    m.set_name("short-lived".to_string());
    for _ in 0..100 {
        m.record_item(16);
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    m.finalize();
    println!("  name={:?}", m.name);
    println!("  processing_time={:?}", m.processing_time);
    println!("  {}", m.throughput_summary_processing_time());
    println!("     (before finalize, processing_time is zero and only the");
    println!("      live items/sec figures are meaningful)");

    println!("\n=== builder form ===");
    let built = StreamMetrics::new()
        .with_name("built".to_string())
        .with_health_thresholds(HealthThresholds::relaxed());
    println!("  name={:?} max_error_rate={:.2}",
        built.name, built.health_thresholds.max_error_rate);
}
