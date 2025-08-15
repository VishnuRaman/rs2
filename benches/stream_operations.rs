// ============================================================================
// Stream Operations Benchmarks for RS2 Streaming Library
// ============================================================================
//
// This benchmark suite comprehensively tests basic stream operations
// provided by the RS2 streaming library. It measures performance across
// different scenarios including:
//
// 1. Basic transformations (map, filter, collect)
// 2. Aggregation operations (fold, reduce-like operations)
// 3. Chunking and batching operations
// 4. Asynchronous mapping operations
// 5. Chain and composition operations
// 6. Memory-efficient streaming patterns
// 7. Various data sizes and workloads
//
// All benchmarks use external APIs from the rs2 module and rs2_stream_ext
// to demonstrate proper usage patterns while measuring performance
// characteristics under different conditions.

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rs2_stream::rs2::from_iter_rs2;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use tokio::runtime::Runtime;

fn bench_basic_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("basic_operations");
    group.measurement_time(std::time::Duration::from_secs(15));
    group.sample_size(20);

    // Test different data sizes
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        group.bench_with_input(BenchmarkId::new("map_filter", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter_rs2(0..size)
                    .map_rs2(|x| black_box(x * 2))
                    .filter_rs2(|&x| black_box(x % 4 == 0))
                    .collect_rs2()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("fold", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter_rs2(0..size)
                    .fold_rs2(0i64, |acc, x| black_box(acc + x as i64))
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(
            BenchmarkId::new("chunk_and_process", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter_rs2(0..size)
                        .chunk_rs2(100)
                        .map_rs2(|chunk| black_box(chunk.len()))
                        .collect_rs2()
                        .await;
                    black_box(result)
                });
            },
        );

        // Add more comprehensive tests for smaller sizes to avoid timeout
        if *size <= 100_000 {
            group.bench_with_input(BenchmarkId::new("take_skip", size), size, |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter_rs2(0..size)
                        .take_rs2((size / 2) as usize)
                        .map_rs2(|x| black_box(x * 3))
                        .collect_rs2()
                        .await;
                    black_box(result)
                });
            });

            group.bench_with_input(BenchmarkId::new("flat_map", size), size, |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter_rs2(0..(size / 10))
                        .flat_map_rs2(|x| from_iter_rs2(0..10).map_rs2(move |y| x * 10 + y))
                        .collect_rs2()
                        .await;
                    black_box(result)
                });
            });
        }
    }

    group.finish();
}

fn bench_async_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("async_operations");
    group.measurement_time(std::time::Duration::from_secs(12));
    group.sample_size(15);

    for size in [1_000, 10_000, 50_000].iter() {
        group.bench_with_input(BenchmarkId::new("eval_map", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter_rs2(0..size)
                    .eval_map_rs2(|x| async move {
                        // Simulate async work
                        tokio::task::yield_now().await;
                        black_box(x * 2)
                    })
                    .collect_rs2()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("filter_map_async", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter_rs2(0..size)
                    .filter_map_async_rs2(|x| async move {
                        tokio::task::yield_now().await;
                        if x % 2 == 0 {
                            Some(black_box(x * 2))
                        } else {
                            None
                        }
                    })
                    .collect_rs2()
                    .await;
                black_box(result)
            });
        });
    }

    group.finish();
}

fn bench_aggregation_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("aggregation_operations");
    group.measurement_time(std::time::Duration::from_secs(10));
    group.sample_size(15);

    for size in [1_000, 10_000, 100_000].iter() {
        group.bench_with_input(BenchmarkId::new("count", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter_rs2(0..size)
                    .filter_rs2(|&x| x % 2 == 0)
                    .count_rs2()
                    .await;
                black_box(result)
            });
        });

        group.bench_with_input(BenchmarkId::new("reduce", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter_rs2(1..=size)
                    .reduce_rs2(|acc, x| black_box(acc + x))
                    .await;
                black_box(result)
            });
        });

        if *size <= 10_000 {
            group.bench_with_input(BenchmarkId::new("find", size), size, |b, &size| {
                b.to_async(&rt).iter(|| async move {
                    let target = size / 2;
                    let result = from_iter_rs2(0..size)
                        .find_rs2(move |&x| x == target)
                        .await;
                    black_box(result)
                });
            });

            group.bench_with_input(BenchmarkId::new("any_all", size), size, |b, &size| {
                b.to_async(&rt).iter(|| async move {
                    let threshold = size / 2;
                    let upper_limit = size + 1000;
                    let any_result = from_iter_rs2(0..size)
                        .any_rs2(move |&x| x > threshold)
                        .await;
                    let all_result = from_iter_rs2(0..size)
                        .all_rs2(move |&x| x < upper_limit)
                        .await;
                    black_box((any_result, all_result))
                });
            });
        }
    }

    group.finish();
}

fn bench_composition_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("composition_operations");
    group.measurement_time(std::time::Duration::from_secs(12));
    group.sample_size(15);

    for size in [1_000, 10_000, 50_000].iter() {
        group.bench_with_input(BenchmarkId::new("zip", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let stream1 = from_iter_rs2(0..size);
                let stream2 = from_iter_rs2((size..size * 2).rev());
                let result = stream1
                    .zip_rs2(stream2)
                    .map_rs2(|(a, b)| black_box(a + b))
                    .collect_rs2()
                    .await;
                black_box(result)
            });
        });

        if *size <= 10_000 {
            group.bench_with_input(BenchmarkId::new("merge", size), size, |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let stream1 = from_iter_rs2(0..size / 2);
                    let stream2 = from_iter_rs2(size / 2..size);
                    let result = stream1
                        .merge_rs2(stream2)
                        .collect_rs2()
                        .await;
                    black_box(result)
                });
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_basic_operations,
    bench_async_operations,
    bench_aggregation_operations,
    bench_composition_operations
);
criterion_main!(benches);
