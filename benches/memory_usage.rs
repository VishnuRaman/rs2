use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rs2::rs2::*;
use std::time::Duration;
use tokio::runtime::Runtime;

// Helper to get current memory usage (simplified)
fn get_memory_usage() -> usize {
    // In a real benchmark, you'd use a proper memory profiling tool
    // This is a placeholder
    0
}

fn bench_memory_efficiency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("memory_efficiency");

    for size in [10_000, 100_000, 1_000_000].iter() {
        group.bench_with_input(
            BenchmarkId::new("large_objects", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let large_strings: Vec<String> = (0..size)
                        .map(|i| format!("Large string data {}: {}", i, "x".repeat(100)))
                        .collect();

                    let result = from_iter(large_strings)
                        .map_rs2(|s| black_box(s.len()))
                        .fold_rs2(0usize, |acc, len| async move { acc + len })
                        .await;
                    black_box(result)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("chunked_processing", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..size)
                        .chunk_rs2(1000)
                        .map_rs2(|chunk| black_box(chunk.into_iter().sum::<i32>()))
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_memory_efficiency);
criterion_main!(benches);