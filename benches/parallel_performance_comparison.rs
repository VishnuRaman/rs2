use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rs2_stream::rs2;
use rs2_stream::rs2_new_stream_ext::RS2StreamExt;
use rs2_stream::rs2_stream_ext::RS2StreamExt as RS2OriginalStreamExt;
use rs2_stream::stream::constructors::from_iter;
use std::time::Duration;
use tokio::runtime::Runtime;

const SIZES: &[usize] = &[100, 1000, 10000];
const CONCURRENCY_LEVELS: &[usize] = &[1, 2, 4, 8];

/// Benchmark parallel map operations
fn bench_parallel_map_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel_map_comparison");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &size in SIZES {
        for &concurrency in CONCURRENCY_LEVELS {
            let bench_name = format!("{}x{}", size, concurrency);

            // RS2 Original - map_parallel_rs2
            group.bench_with_input(
                BenchmarkId::new("rs2_original_map_parallel", &bench_name), 
                &(size, concurrency), 
                |b, &(size, concurrency)| {
                    b.to_async(&rt).iter(|| async {
                        let data: Vec<i32> = (0..size as i32).collect();
                        let stream = rs2::from_iter(data);
                        let result: Vec<i32> = if concurrency == 1 {
                            stream.map_rs2(|x| x * 2).collect_rs2::<Vec<_>>().await
                        } else {
                            stream.map_parallel_rs2(|x| x * 2).collect_rs2::<Vec<_>>().await
                        };
                        black_box(result);
                    })
                }
            );

            // RS2 New - map_parallel_rs2  
            group.bench_with_input(
                BenchmarkId::new("rs2_new_map_parallel", &bench_name),
                &(size, concurrency), 
                |b, &(size, concurrency)| {
                    b.to_async(&rt).iter(|| async {
                        let data: Vec<i32> = (0..size as i32).collect();
                        let stream = from_iter(data);
                        let result: Vec<i32> = if concurrency == 1 {
                            stream.map_rs2(|x| x * 2).collect_rs2::<Vec<_>>().await
                        } else {
                            stream.map_parallel_rs2(|x| x * 2).collect_rs2::<Vec<_>>().await
                        };
                        black_box(result);
                    })
                }
            );
        }
    }
    group.finish();
}

/// Benchmark par_eval_map operations
fn bench_par_eval_map_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("par_eval_map_comparison");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &size in SIZES {
        for &concurrency in CONCURRENCY_LEVELS {
            let bench_name = format!("{}x{}", size, concurrency);

            // RS2 Original - par_eval_map_rs2
            group.bench_with_input(
                BenchmarkId::new("rs2_original_par_eval_map", &bench_name),
                &(size, concurrency), 
                |b, &(size, concurrency)| {
                    b.to_async(&rt).iter(|| async {
                        let data: Vec<i32> = (0..size as i32).collect();
                        let stream = rs2::from_iter(data);
                        let result: Vec<i32> = if concurrency == 1 {
                            stream.eval_map_rs2(|x| async move { x * 2 }).collect_rs2::<Vec<_>>().await
                        } else {
                            stream.par_eval_map_rs2(concurrency, |x| async move { x * 2 }).collect_rs2::<Vec<_>>().await
                        };
                        black_box(result);
                    })
                }
            );

            // RS2 New - par_eval_map_rs2
            group.bench_with_input(
                BenchmarkId::new("rs2_new_par_eval_map", &bench_name),
                &(size, concurrency), 
                |b, &(size, concurrency)| {
                    b.to_async(&rt).iter(|| async {
                        let data: Vec<i32> = (0..size as i32).collect();
                        let stream = from_iter(data);
                        let result: Vec<i32> = if concurrency == 1 {
                            stream.eval_map_rs2(|x| async move { x * 2 }).collect_rs2::<Vec<_>>().await
                        } else {
                            stream.par_eval_map_rs2(concurrency, |x| async move { x * 2 }).collect_rs2::<Vec<_>>().await
                        };
                        black_box(result);
                    })
                }
            );
        }
    }
    group.finish();
}

/// Benchmark par_eval_map_unordered operations
fn bench_par_eval_map_unordered_comparison(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("par_eval_map_unordered_comparison");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    for &size in SIZES {
        for &concurrency in CONCURRENCY_LEVELS {
            let bench_name = format!("{}x{}", size, concurrency);

            // RS2 Original - par_eval_map_unordered_rs2
            group.bench_with_input(
                BenchmarkId::new("rs2_original_par_eval_map_unordered", &bench_name),
                &(size, concurrency), 
                |b, &(size, concurrency)| {
                    b.to_async(&rt).iter(|| async {
                        let data: Vec<i32> = (0..size as i32).collect();
                        let stream = rs2::from_iter(data);
                        let result: Vec<i32> = if concurrency == 1 {
                            stream.eval_map_rs2(|x| async move { x * 2 }).collect_rs2::<Vec<_>>().await
                        } else {
                            stream.par_eval_map_unordered_rs2(concurrency, |x| async move { x * 2 }).collect_rs2::<Vec<_>>().await
                        };
                        black_box(result);
                    })
                }
            );

            // RS2 New - par_eval_map_unordered_rs2
            group.bench_with_input(
                BenchmarkId::new("rs2_new_par_eval_map_unordered", &bench_name),
                &(size, concurrency), 
                |b, &(size, concurrency)| {
                    b.to_async(&rt).iter(|| async {
                        let data: Vec<i32> = (0..size as i32).collect();
                        let stream = from_iter(data);
                        let result: Vec<i32> = if concurrency == 1 {
                            stream.eval_map_rs2(|x| async move { x * 2 }).collect_rs2::<Vec<_>>().await
                        } else {
                            stream.par_eval_map_unordered_rs2(concurrency, |x| async move { x * 2 }).collect_rs2::<Vec<_>>().await
                        };
                        black_box(result);
                    })
                }
            );
        }
    }
    group.finish();
}

/// Benchmark with realistic async workload
fn bench_realistic_async_workload(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("realistic_async_workload");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(15));

    async fn simulate_async_work(x: i32) -> i32 {
        // Simulate some async I/O with a tiny delay
        tokio::time::sleep(Duration::from_micros(10)).await;
        x * x + x
    }

    for &size in &[50, 100, 200] {
        for &concurrency in &[2, 4, 8] {
            let bench_name = format!("{}x{}", size, concurrency);

            // RS2 Original
            group.bench_with_input(
                BenchmarkId::new("rs2_original_async", &bench_name),
                &(size, concurrency), 
                |b, &(size, concurrency)| {
                    b.to_async(&rt).iter(|| async {
                        let data: Vec<i32> = (0..size as i32).collect();
                        let stream = rs2::from_iter(data);
                        let result: Vec<i32> = stream
                            .par_eval_map_rs2(concurrency, |x| simulate_async_work(x))
                            .collect_rs2::<Vec<_>>()
                            .await;
                        black_box(result);
                    })
                }
            );

            // RS2 New
            group.bench_with_input(
                BenchmarkId::new("rs2_new_async", &bench_name),
                &(size, concurrency), 
                |b, &(size, concurrency)| {
                    b.to_async(&rt).iter(|| async {
                        let data: Vec<i32> = (0..size as i32).collect();
                        let stream = from_iter(data);
                        let result: Vec<i32> = stream
                            .par_eval_map_rs2(concurrency, |x| simulate_async_work(x))
                            .collect_rs2::<Vec<_>>()
                            .await;
                        black_box(result);
                    })
                }
            );
        }
    }
    group.finish();
}

/// Benchmark concurrency scaling
fn bench_concurrency_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrency_scaling");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));

    let size = 1000;
    let concurrency_levels = &[1, 2, 4, 8, 16];

    for &concurrency in concurrency_levels {
        // RS2 Original
        group.bench_with_input(
            BenchmarkId::new("rs2_original_scaling", concurrency),
            &concurrency, 
            |b, &concurrency| {
                b.to_async(&rt).iter(|| async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .par_eval_map_rs2(concurrency, |x| async move { x * x })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            }
        );

        // RS2 New
        group.bench_with_input(
            BenchmarkId::new("rs2_new_scaling", concurrency),
            &concurrency, 
            |b, &concurrency| {
                b.to_async(&rt).iter(|| async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .par_eval_map_rs2(concurrency, |x| async move { x * x })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            }
        );
    }
    group.finish();
}

/// Benchmark order preservation overhead
fn bench_order_preservation_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("order_preservation_overhead");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(10));

    async fn variable_workload(x: i32) -> i32 {
        // Simulate variable processing time
        let delay = Duration::from_micros(((x % 10) * 5) as u64);
        tokio::time::sleep(delay).await;
        x * x
    }

    let size = 500;
    let concurrency = 8;

    // Ordered vs Unordered comparison
    group.bench_function("rs2_original_ordered", |b| {
        b.to_async(&rt).iter(|| async {
            let data: Vec<i32> = (0..size as i32).collect();
            let stream = rs2::from_iter(data);
            let result: Vec<i32> = stream
                .par_eval_map_rs2(concurrency, |x| variable_workload(x))
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result);
        })
    });

    group.bench_function("rs2_original_unordered", |b| {
        b.to_async(&rt).iter(|| async {
            let data: Vec<i32> = (0..size as i32).collect();
            let stream = rs2::from_iter(data);
            let result: Vec<i32> = stream
                .par_eval_map_unordered_rs2(concurrency, |x| variable_workload(x))
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result);
        })
    });

    group.bench_function("rs2_new_ordered", |b| {
        b.to_async(&rt).iter(|| async {
            let data: Vec<i32> = (0..size as i32).collect();
            let stream = from_iter(data);
            let result: Vec<i32> = stream
                .par_eval_map_rs2(concurrency, |x| variable_workload(x))
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result);
        })
    });

    group.bench_function("rs2_new_unordered", |b| {
        b.to_async(&rt).iter(|| async {
            let data: Vec<i32> = (0..size as i32).collect();
            let stream = from_iter(data);
            let result: Vec<i32> = stream
                .par_eval_map_unordered_rs2(concurrency, |x| variable_workload(x))
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result);
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_parallel_map_comparison,
    bench_par_eval_map_comparison,
    bench_par_eval_map_unordered_comparison,
    bench_realistic_async_workload,
    bench_concurrency_scaling,
    bench_order_preservation_overhead,
);
criterion_main!(benches); 