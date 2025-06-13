use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rs2::rs2::*;
use std::time::Duration;
use tokio::runtime::Runtime;
use futures::StreamExt as FuturesStreamExt;
use tokio_stream::StreamExt as TokioStreamExt;

// === OPTIMIZED TEST DATA AND OPERATIONS ===

async fn simulate_async_work(x: u32) -> u32 {
    // Slightly larger sleep for more stable measurements
    if x % 1000 == 0 {
        tokio::time::sleep(Duration::from_micros(1)).await; // Changed from nanos to micros
    }
    x * 2
}


async fn cpu_work(x: u32) -> u32 {
    // Consistent CPU work
    let mut result = x;
    for _ in 0..50 {
        result = result.wrapping_mul(1664525).wrapping_add(1013904223);
    }
    black_box(result)
}

fn sync_transform(x: u32) -> u32 {
    black_box(x * 2 + 1)
}

fn sync_filter(x: &u32) -> bool {
    black_box(x % 4 == 0)
}

// === DIRECT RS2 vs TOKIO-STREAM COMPARISON ===

fn bench_basic_pipeline_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("rs2_vs_tokio_stream_basic");

    // Optimize for stable measurements
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(50);

    for size in [1_000, 10_000].iter() {
        // RS2 basic pipeline
        group.bench_with_input(
            BenchmarkId::new("rs2_basic", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..size)
                        .map_rs2(|x| sync_transform(x))
                        .filter_rs2(|x| sync_filter(x))
                        .take_rs2((size / 4) as usize)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result)
                });
            },
        );

        // tokio-stream basic pipeline  
        group.bench_with_input(
            BenchmarkId::new("tokio_stream_basic", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result: Vec<_> = tokio_stream::StreamExt::collect(
                        tokio_stream::StreamExt::take(
                            tokio_stream::StreamExt::filter(
                                tokio_stream::StreamExt::map(
                                    tokio_stream::iter(0..size),
                                    |x| sync_transform(x)
                                ),
                                |x| sync_filter(x)
                            ),
                            (size / 4) as usize
                        )
                    ).await;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_async_operations_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("rs2_vs_tokio_stream_async");

    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(30);

    for size in [500, 2_000].iter() {
        // RS2 async operations
        group.bench_with_input(
            BenchmarkId::new("rs2_async", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..size)
                        .eval_map_rs2(|x| simulate_async_work(x))
                        .filter_rs2(|x| sync_filter(x))
                        .take_rs2((size / 2) as usize)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result)
                });
            },
        );

        // tokio-stream async operations
        group.bench_with_input(
            BenchmarkId::new("tokio_stream_async", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result: Vec<_> = tokio_stream::StreamExt::collect(
                        tokio_stream::StreamExt::take(
                            tokio_stream::StreamExt::filter(
                                tokio_stream::StreamExt::then(
                                    tokio_stream::iter(0..size),
                                    |x| simulate_async_work(x)
                                ),
                                |x| sync_filter(x)
                            ),
                            (size / 2) as usize
                        )
                    ).await;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_chunking_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("rs2_vs_tokio_stream_chunking");

    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(30);

    let data_size = 5_000;

    // RS2 chunking
    group.bench_function("rs2_chunked", |b| {
        b.to_async(&rt).iter(|| async {
            let result = from_iter(0..data_size)
                .chunk_rs2(100)
                .map_rs2(|chunk| {
                    let sum: u32 = chunk.iter().sum();
                    black_box(sum)
                })
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result)
        });
    });

    // tokio-stream chunking
    group.bench_function("tokio_stream_chunked", |b| {
        b.to_async(&rt).iter(|| async {
            let result: Vec<_> = tokio_stream::StreamExt::collect(
                tokio_stream::StreamExt::map(
                    tokio_stream::StreamExt::chunks_timeout(
                        tokio_stream::iter(0..data_size),
                        100,
                        Duration::from_secs(10)
                    ),
                    |chunk| {
                        let sum: u32 = chunk.iter().sum();
                        black_box(sum)
                    }
                )
            ).await;
            black_box(result)
        });
    });

    group.finish();
}

fn bench_sequential_vs_parallel(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("rs2_vs_tokio_stream_concurrency");

    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(12));
    group.sample_size(20);

    let data_size = 400;

    // tokio-stream sequential (baseline)
    group.bench_function("tokio_stream_sequential", |b| {
        b.to_async(&rt).iter(|| async {
            let result: Vec<_> = tokio_stream::StreamExt::collect(
                tokio_stream::StreamExt::then(
                    tokio_stream::iter(0..data_size),
                    |x| cpu_work(x)
                )
            ).await;
            black_box(result)
        });
    });

    // RS2 sequential (for fair comparison)
    group.bench_function("rs2_sequential", |b| {
        b.to_async(&rt).iter(|| async {
            let result = from_iter(0..data_size)
                .eval_map_rs2(|x| cpu_work(x))
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result)
        });
    });

    // RS2 parallel (advantage)
    for concurrency in [2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("rs2_parallel", concurrency),
            concurrency,
            |b, &conc| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..data_size)
                        .par_eval_map_rs2(conc, |x| cpu_work(x))
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_memory_efficiency_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("rs2_vs_tokio_stream_memory");

    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(8));
    group.sample_size(20);

    for size in [10_000, 50_000].iter() {
        // RS2 large data processing
        group.bench_with_input(
            BenchmarkId::new("rs2_large_data", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let result = from_iter(0..size)
                        .map_rs2(|x| format!("item_{}", x))
                        .filter_rs2(|s| s.len() > 5)
                        .take_rs2(size / 10)
                        .fold_rs2(0usize, |acc, s| async move { acc + s.len() })
                        .await;
                    black_box(result)
                });
            },
        );

        // tokio-stream large data processing
        group.bench_with_input(
            BenchmarkId::new("tokio_stream_large_data", size),
            size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    // Use a different approach without StreamExt
                    let mut total_len = 0usize;
                    let mut count = 0;

                    for i in 0..size {
                        let item = format!("item_{}", i);
                        if item.len() > 5 {
                            total_len += item.len();
                            count += 1;
                            if count >= size / 10 {
                                break;
                            }
                        }
                    }

                    let result = total_len;
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

fn bench_practical_scenarios(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("rs2_vs_tokio_stream_practical");

    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(25);

    let data_size = 2_000;

    // Scenario 1: Data transformation pipeline
    group.bench_function("rs2_data_pipeline", |b| {
        b.to_async(&rt).iter(|| async {
            let result = from_iter(0..data_size)
                .map_rs2(|x| x * 3)
                .filter_rs2(|&x| x % 6 == 0)
                .chunk_rs2(50)
                .map_rs2(|chunk| chunk.len())
                .fold_rs2(0usize, |acc, len| async move { acc + len })
                .await;
            black_box(result)
        });
    });

    group.bench_function("tokio_stream_data_pipeline", |b| {
        b.to_async(&rt).iter(|| async {
            // Use a different approach without StreamExt
            let mut result = 0usize;
            let mut filtered_values = Vec::new();

            // Filter values
            for i in 0..data_size {
                let val = i * 3;
                if val % 6 == 0 {
                    filtered_values.push(val);
                }
            }

            // Process in chunks
            for chunk in filtered_values.chunks(50) {
                result += chunk.len();
            }
            black_box(result)
        });
    });

    // Scenario 2: Async I/O simulation
    group.bench_function("rs2_async_io", |b| {
        b.to_async(&rt).iter(|| async {
            let result = from_iter(0..500)
                .eval_map_rs2(|x| async move {
                    tokio::time::sleep(Duration::from_nanos(x as u64 % 100)).await;
                    x * 2
                })
                .filter_rs2(|&x| x % 8 == 0)
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result)
        });
    });

    group.bench_function("tokio_stream_async_io", |b| {
        b.to_async(&rt).iter(|| async {
            let result: Vec<_> = tokio_stream::StreamExt::collect(
                tokio_stream::StreamExt::filter(
                    tokio_stream::StreamExt::then(
                        tokio_stream::iter(0..500),
                        |x| async move {
                            tokio::time::sleep(Duration::from_nanos(x as u64 % 100)).await;
                            x * 2
                        }
                    ),
                    |&x| x % 8 == 0
                )
            ).await;
            black_box(result)
        });
    });

    group.finish();
}

// Quick validation for development
fn bench_quick_validation(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("quick_validation");

    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(2));
    group.sample_size(10);

    group.bench_function("rs2_simple", |b| {
        b.to_async(&rt).iter(|| async {
            let result = from_iter(0..1000)
                .map_rs2(|x| x * 2)
                .filter_rs2(|&x| x % 4 == 0)
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result)
        });
    });

    group.bench_function("tokio_stream_simple", |b| {
        b.to_async(&rt).iter(|| async {
            let result: Vec<_> = tokio_stream::StreamExt::collect(
                tokio_stream::StreamExt::filter(
                    tokio_stream::StreamExt::map(
                        tokio_stream::iter(0..1000),
                        |x| x * 2
                    ),
                    |&x| x % 4 == 0
                )
            ).await;
            black_box(result)
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_quick_validation,
    bench_basic_pipeline_comparison,
    bench_async_operations_comparison,
    bench_chunking_comparison,
    bench_sequential_vs_parallel,
    bench_memory_efficiency_comparison,
    bench_practical_scenarios
);
criterion_main!(benches);
