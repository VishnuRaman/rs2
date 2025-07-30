use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::rs2::*;
use tokio::runtime::Runtime;
use std::sync::atomic::{AtomicUsize, Ordering};

// Helper to simulate memory tracking (simplified)
static MEMORY_COUNTER: AtomicUsize = AtomicUsize::new(0);

fn track_allocation(size: usize) {
    MEMORY_COUNTER.fetch_add(size, Ordering::SeqCst);
}

fn get_memory_usage() -> usize {
    MEMORY_COUNTER.load(Ordering::SeqCst)
}

fn reset_memory_tracking() {
    MEMORY_COUNTER.store(0, Ordering::SeqCst);
}

fn bench_memory_efficiency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("memory_efficiency");
    group.sample_size(10); // Reduce sample size for large data sets

    for size in [10_000, 100_000, 1_000_000].iter() {
        // Benchmark 1: Large object processing with memory tracking
        group.bench_with_input(BenchmarkId::new("large_objects", size), size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                reset_memory_tracking();
                
                let large_strings: Vec<String> = (0..size)
                    .map(|i| {
                        let s = format!("Large string data {}: {}", i, "x".repeat(100));
                        track_allocation(s.len());
                        s
                    })
                    .collect();

                let result = from_iter_rs2(large_strings)
                    .map_rs2(|s| {
                        let len = s.len();
                        black_box(len)
                    })
                    .fold_rs2(0usize, |acc, len| acc + len)
                    .await;
                
                let memory_used = get_memory_usage();
                black_box((result, memory_used))
            });
        });

        // Benchmark 2: Chunked processing with reduced memory footprint
        group.bench_with_input(
            BenchmarkId::new("chunked_processing", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter_rs2(0..size)
                        .chunk_rs2(1000)
                        .map_rs2(|chunk| {
                            let sum = chunk.into_iter().sum::<i32>();
                            black_box(sum)
                        })
                        .collect_rs2()
                        .await;
                    black_box(result)
                });
            },
        );

        // Benchmark 3: Streaming vs collecting - memory comparison
        group.bench_with_input(
            BenchmarkId::new("streaming_vs_collecting", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    reset_memory_tracking();
                    
                    // Stream processing without collecting everything at once
                    let result = from_iter_rs2(0..size)
                        .map_rs2(|n| {
                            track_allocation(std::mem::size_of::<i32>());
                            n * 2
                        })
                        .filter_rs2(|&n| n % 4 == 0)
                        .take_rs2((size as usize).min(1000)) // Limit results to avoid excessive memory
                        .reduce_rs2(|acc, n| acc + n)
                        .await;
                    
                    let memory_used = get_memory_usage();
                    black_box((result, memory_used))
                });
            },
        );

        // Benchmark 4: Parallel processing memory efficiency
        group.bench_with_input(
            BenchmarkId::new("parallel_memory", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    reset_memory_tracking();
                    
                    let data: Vec<i32> = (0..size).map(|i| {
                        track_allocation(std::mem::size_of::<i32>());
                        i
                    }).collect();
                    
                    let result = from_iter_rs2(data)
                        .par_eval_map_rs2(4, |n| async move {
                            // Simulate some async processing
                            tokio::task::yield_now().await;
                            n * n
                        })
                        .take_rs2((size as usize).min(1000)) // Limit to avoid memory explosion
                        .fold_rs2(0i64, |acc, n| acc + n as i64)
                        .await;
                    
                    let memory_used = get_memory_usage();
                    black_box((result, memory_used))
                });
            },
        );

        // Benchmark 5: Memory-efficient string processing
        group.bench_with_input(
            BenchmarkId::new("efficient_string_processing", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    reset_memory_tracking();
                    
                    let result = from_iter_rs2(0..size)
                        .map_rs2(|i| {
                            let s = i.to_string();
                            track_allocation(s.len());
                            s
                        })
                        .filter_rs2(|s| s.len() <= 5) // Filter out very long strings
                        .eval_map_rs2(|s| async move {
                            // Simulate async string processing
                            tokio::task::yield_now().await;
                            s.parse::<i32>().unwrap_or(0)
                        })
                        .take_rs2((size as usize).min(10000)) // Reasonable limit
                        .count_rs2()
                        .await;
                    
                    let memory_used = get_memory_usage();
                    black_box((result, memory_used))
                });
            },
        );
    }

    group.finish();
}

fn bench_memory_patterns(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("memory_patterns");

    // Benchmark different memory access patterns
    let size = 100_000;

    // Sequential access pattern
    group.bench_function("sequential_access", |b| {
        b.to_async(&rt).iter(|| async {
            let data: Vec<i32> = (0..size).collect();
            let result = from_iter_rs2(data)
                .map_rs2(|n| black_box(n * 2))
                .fold_rs2(0i64, |acc, n| acc + n as i64)
                .await;
            black_box(result)
        });
    });

    // Random access pattern simulation
    group.bench_function("chunked_access", |b| {
        b.to_async(&rt).iter(|| async {
            let result = from_iter_rs2(0..size)
                .chunk_rs2(100)
                .map_rs2(|chunk| {
                    let sum: i32 = chunk.into_iter().map(|n| black_box(n * 2)).sum();
                    sum
                })
                .fold_rs2(0i64, |acc, sum| acc + sum as i64)
                .await;
            black_box(result)
        });
    });

    // Buffered processing
    group.bench_function("buffered_processing", |b| {
        b.to_async(&rt).iter(|| async {
            let result = from_iter_rs2(0..size)
                .chunk_rs2(1000)
                .eval_map_rs2(|chunk| async move {
                    // Simulate buffered async processing
                    tokio::task::yield_now().await;
                    chunk.into_iter().map(|n| n * 3).sum::<i32>()
                })
                .fold_rs2(0i64, |acc, sum| acc + sum as i64)
                .await;
            black_box(result)
        });
    });

    group.finish();
}

criterion_group!(benches, bench_memory_efficiency, bench_memory_patterns);
criterion_main!(benches);
