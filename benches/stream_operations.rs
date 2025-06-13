use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rs2::rs2::*;
use std::time::Duration;
use tokio::runtime::Runtime;

fn bench_basic_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("basic_operations");

    // Test different data sizes
    for size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        group.bench_with_input(
            BenchmarkId::new("map_filter", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..size)
                        .map_rs2(|x| black_box(x * 2))
                        .filter_rs2(|&x| black_box(x % 4 == 0))
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("fold", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..size)
                        .fold_rs2(0i64, |acc, x| async move {
                            black_box(acc + x as i64)
                        })
                        .await;
                    black_box(result)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("chunk_and_process", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..size)
                        .chunk_rs2(100)
                        .map_rs2(|chunk| black_box(chunk.len()))
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_async_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("async_operations");

    for size in [1_000, 10_000, 50_000].iter() {
        group.bench_with_input(
            BenchmarkId::new("eval_map", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..size)
                        .eval_map_rs2(|x| async move {
                            // Simulate async work
                            tokio::task::yield_now().await;
                            black_box(x * 2)
                        })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_basic_operations, bench_async_operations);
criterion_main!(benches);