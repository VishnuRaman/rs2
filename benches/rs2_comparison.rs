use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rs2_stream::rs2;
use rs2_stream::rs2_new_stream_ext::RS2StreamExt;
use rs2_stream::rs2_stream_ext::RS2StreamExt as RS2OriginalStreamExt;
use rs2_stream::stream::constructors::from_iter;
use rs2_stream::advanced_analytics::{AdvancedAnalyticsExt, TimeWindowConfig, TimeJoinConfig};
use rs2_stream::new_advanced_analytics::{NewAdvancedAnalyticsExt, TimeWindowConfig as NewTimeWindowConfig, TimeJoinConfig as NewTimeJoinConfig};
use std::time::{Duration, SystemTime};
use tokio::runtime::Runtime;

const SIZES: &[usize] = &[1000, 10000, 100000];

fn bench_map_filter_collect(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("map_filter_collect");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .map_rs2(|x| x * 2)
                        .filter_rs2(|&x| x % 4 == 0)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .map_rs2(|x| x * 2)
                        .filter_rs2(|&x| x % 4 == 0)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_eval_map(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("eval_map");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .eval_map_rs2(|x| async move { x * 2 })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .eval_map_rs2(|x| async move { x * 2 })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_take(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("take");

    for &size in SIZES {
        let take_amount = size / 10; // Take 10% of items

        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .take_rs2(take_amount)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .take_rs2(take_amount)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_merge(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("merge");

    for &size in SIZES {
        let half_size = size / 2;

        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &_size| {
            b.iter(|| {
                rt.block_on(async {
                    let data1: Vec<i32> = (0..half_size as i32).collect();
                    let data2: Vec<i32> = (half_size as i32..size as i32).collect();
                    let stream1 = rs2::from_iter(data1);
                    let stream2 = rs2::from_iter(data2);
                    let result: Vec<i32> = stream1
                        .merge_rs2(stream2)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &_size| {
            b.iter(|| {
                rt.block_on(async {
                    let data1: Vec<i32> = (0..half_size as i32).collect();
                    let data2: Vec<i32> = (half_size as i32..size as i32).collect();
                    let stream1 = from_iter(data1);
                    let stream2 = Box::new(from_iter(data2));
                    let result: Vec<i32> = stream1
                        .merge_rs2(stream2)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_chunk(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("chunk");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<Vec<i32>> = stream
                        .chunk_rs2(100)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<Vec<i32>> = stream
                        .chunk_rs2(100)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_throttle(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("throttle");

    // For throttle, use smaller sizes to avoid extremely long test times
    let throttle_sizes = &[100, 1000, 10000];

    for &size in throttle_sizes {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .throttle_rs2(Duration::ZERO) // No throttling for benchmark
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .throttle_rs2(Duration::ZERO) // No throttling for benchmark
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_parallel_map(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel_map");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .map_parallel_rs2(|x| x * x)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .map_parallel_rs2(|x| x * x)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_par_eval_map(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("par_eval_map");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .par_eval_map_rs2(4, |x| async move { x * x })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .par_eval_map_rs2(4, |x| async move { x * x })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_zip(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("zip");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data1: Vec<i32> = (0..size as i32).collect();
                    let data2: Vec<i32> = (size as i32..2 * size as i32).collect();
                    let stream1 = rs2::from_iter(data1);
                    let stream2 = rs2::from_iter(data2);
                    let result: Vec<(i32, i32)> = stream1
                        .zip_rs2(stream2)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data1: Vec<i32> = (0..size as i32).collect();
                    let data2: Vec<i32> = (size as i32..2 * size as i32).collect();
                    let stream1 = from_iter(data1);
                    let stream2 = Box::new(from_iter(data2));
                    let result: Vec<(i32, i32)> = stream1
                        .zip_rs2(stream2)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_scan(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("scan");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .scan_rs2(0, |acc, x| acc + x)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .scan_rs2(0, |acc: &mut i32, x| { *acc += x; Some(*acc) })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_sliding_window(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sliding_window");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<Vec<i32>> = stream
                        .sliding_window_rs2(5)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<Vec<i32>> = stream
                        .sliding_window_rs2(5)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_batch_process(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("batch_process");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .batch_process_rs2(50, |batch| {
                            batch.into_iter().map(|x| x * 2).collect()
                        })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .batch_process_rs2(50, |batch| {
                            batch.into_iter().map(|x| x * 2).collect()
                        })
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_distinct_until_changed(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("distinct_until_changed");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).map(|x| x / 3).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .distinct_until_changed_rs2()
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).map(|x| x / 3).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .distinct_until_changed_rs2()
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_group_adjacent_by(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("group_adjacent_by");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).map(|x| x / 10).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<(i32, Vec<i32>)> = stream
                        .group_adjacent_by_rs2(|&x| x)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).map(|x| x / 10).collect();
                    let stream = from_iter(data);
                    let result: Vec<(i32, Vec<i32>)> = stream
                        .group_adjacent_by_rs2(|&x| x)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_debounce(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("debounce");

    // For debounce, use smaller sizes to avoid extremely long test times
    let debounce_sizes = &[10, 50, 100];

    for &size in debounce_sizes {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .debounce_rs2(Duration::ZERO) // No debouncing for benchmark
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .debounce_rs2(Duration::ZERO) // No debouncing for benchmark
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_fold(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("fold");

    for &size in SIZES {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: i64 = stream
                        .fold_rs2(0i64, |acc, x| async move { acc + x as i64 })
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: i64 = stream
                        .fold_rs2(0i64, |acc, x| async move { acc + x as i64 })
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

// ================================
// Advanced Analytics Benchmarks (RS2 New only)
// ================================

fn bench_moving_average(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("moving_average");

    // Use smaller sizes for analytics to avoid long test times
    let analytics_sizes = &[1000, 10000, 50000];

    for &size in analytics_sizes {
        // RS2 New only (moving_average is new feature)
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<f64> = (0..size).map(|x| x as f64).collect();
                    let stream = from_iter(data);
                    let result: Vec<f64> = stream
                        .moving_average_rs2_new(10)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_sliding_window_aggregate(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sliding_window_aggregate");

    let analytics_sizes = &[1000, 10000, 50000];

    for &size in analytics_sizes {
        // RS2 New only (sliding_window_aggregate is new feature)
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .sliding_window_aggregate_rs2_new(10, |window| window.iter().sum::<i32>())
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_sliding_count(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sliding_count");

    let analytics_sizes = &[1000, 10000, 50000];

    for &size in analytics_sizes {
        // RS2 New only (sliding_count is new feature)
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<usize> = stream
                        .sliding_count_rs2_new(5)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_sliding_sum(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sliding_sum");

    let analytics_sizes = &[1000, 10000, 50000];

    for &size in analytics_sizes {
        // RS2 New only (sliding_sum is new feature)
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .sliding_sum_rs2_new(8)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_sliding_min_max(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("sliding_min_max");

    let analytics_sizes = &[1000, 10000, 50000];

    for &size in analytics_sizes {
        // RS2 New only (sliding_min/max are new features)
        group.bench_with_input(BenchmarkId::new("rs2_new_min", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<Option<i32>> = stream
                        .sliding_min_rs2_new(5)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        group.bench_with_input(BenchmarkId::new("rs2_new_max", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<Option<i32>> = stream
                        .sliding_max_rs2_new(5)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_time_windowing(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("time_windowing");

    let analytics_sizes = &[1000, 5000, 10000];

    for &size in analytics_sizes {
        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<(i32, SystemTime)> = (0..size as i32)
                        .map(|x| (x, SystemTime::now()))
                        .collect();
                    let stream = rs2::from_iter(data);
                    let config = TimeWindowConfig {
                        window_size: Duration::from_secs(30),
                        slide_interval: Duration::from_secs(10),
                        allowed_lateness: Duration::from_secs(2),
                        watermark_delay: Duration::from_secs(5),
                    };
                    let result: Vec<_> = stream
                        .window_by_time_rs2(config, |&(_, timestamp)| timestamp)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<(i32, SystemTime)> = (0..size as i32)
                        .map(|x| (x, SystemTime::now()))
                        .collect();
                    let stream = from_iter(data);
                    let config = NewTimeWindowConfig {
                        window_size: Duration::from_secs(30),
                        slide_interval: Duration::from_secs(10),
                        allowed_lateness: Duration::from_secs(2),
                        watermark_delay: Duration::from_secs(5),
                    };
                    let result: Vec<_> = stream
                        .window_by_time_rs2_new(config, |&(_, timestamp)| timestamp)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_group_by_time(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("group_by_time");

    let analytics_sizes = &[1000, 5000, 10000];

    for &size in analytics_sizes {
        // RS2 New only (group_by_time is new feature)
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<(i32, SystemTime)> = (0..size as i32)
                        .map(|x| (x, SystemTime::now()))
                        .collect();
                    let stream = from_iter(data);
                    let result: Vec<(SystemTime, Vec<(i32, SystemTime)>)> = stream
                        .group_by_time_rs2_new(Duration::from_secs(30), |&(_, timestamp)| timestamp)
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_time_join(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("time_join");

    // Use smaller sizes for joins to avoid extremely long test times
    let join_sizes = &[100, 500, 1000];

    for &size in join_sizes {
        let half_size = size / 2;

        // RS2 Original
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &_size| {
            b.iter(|| {
                rt.block_on(async {
                    let data1: Vec<(i32, SystemTime)> = (0..half_size as i32)
                        .map(|x| (x, SystemTime::now()))
                        .collect();
                    let data2: Vec<(i32, SystemTime)> = (half_size as i32..size as i32)
                        .map(|x| (x, SystemTime::now()))
                        .collect();
                    let stream1 = rs2::from_iter(data1);
                    let stream2 = rs2::from_iter(data2);
                    let config = TimeJoinConfig {
                        window_size: Duration::from_secs(30),
                        watermark_delay: Duration::from_secs(5),
                    };
                    
                    let result: Vec<_> = stream1
                        .join_with_time_window_rs2::<(i32, SystemTime), _, _, _, i32, _, _>(
                            stream2,
                            config,
                            |&(_, timestamp)| timestamp,
                            |&(_, timestamp)| timestamp,
                            |left, right| (left, right),
                            None::<(fn(&(i32, SystemTime)) -> i32, fn(&(i32, SystemTime)) -> i32)>,
                        )
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &_size| {
            b.iter(|| {
                rt.block_on(async {
                    let data1: Vec<(i32, SystemTime)> = (0..half_size as i32)
                        .map(|x| (x, SystemTime::now()))
                        .collect();
                    let data2: Vec<(i32, SystemTime)> = (half_size as i32..size as i32)
                        .map(|x| (x, SystemTime::now()))
                        .collect();
                    let stream1 = from_iter(data1);
                    let stream2 = from_iter(data2);
                    let config = NewTimeJoinConfig {
                        window_size: Duration::from_secs(30),
                        watermark_delay: Duration::from_secs(5),
                    };
                    
                    let result: Vec<_> = stream1
                        .join_with_time_window_rs2_new(
                            stream2,
                            config,
                            |&(_, timestamp)| timestamp,
                            |&(_, timestamp)| timestamp,
                            |left, right| (left, right),
                        )
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_throttle_with_duration(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("throttle_with_duration");
    
    // Use smaller sizes since we're actually throttling now
    let throttle_sizes = &[10, 50, 100];

    for &size in throttle_sizes {
        // RS2 Original with actual throttling
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .throttle_rs2(Duration::from_millis(2)) // Actual throttling - 2ms > 1ms threshold
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New with actual throttling
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .throttle_rs2(Duration::from_millis(2)) // Actual throttling - 2ms > 1ms threshold
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

fn bench_debounce_with_duration(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("debounce_with_duration");
    
    // Use smaller sizes since we're actually debouncing now
    let debounce_sizes = &[10, 50, 100];

    for &size in debounce_sizes {
        // RS2 Original with actual debouncing
        group.bench_with_input(BenchmarkId::new("rs2", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = rs2::from_iter(data);
                    let result: Vec<i32> = stream
                        .debounce_rs2(Duration::from_micros(100)) // Actual debouncing
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });

        // RS2 New with actual debouncing
        group.bench_with_input(BenchmarkId::new("rs2_new", size), &size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let data: Vec<i32> = (0..size as i32).collect();
                    let stream = from_iter(data);
                    let result: Vec<i32> = stream
                        .debounce_rs2(Duration::from_micros(100)) // Actual debouncing
                        .collect_rs2::<Vec<_>>()
                        .await;
                    black_box(result);
                })
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_map_filter_collect,
    bench_eval_map,
    bench_take,
    bench_merge,
    bench_chunk,
    bench_throttle,
    bench_parallel_map,
    bench_par_eval_map,
    bench_zip,
    bench_scan,
    bench_sliding_window,
    bench_batch_process,
    bench_distinct_until_changed,
    bench_group_adjacent_by,
    bench_debounce,
    bench_fold,
    bench_moving_average,
    bench_sliding_window_aggregate,
    bench_sliding_count,
    bench_sliding_sum,
    bench_sliding_min_max,
    bench_time_windowing,
    bench_group_by_time,
    bench_time_join,
    bench_throttle_with_duration,
    bench_debounce_with_duration,
);
criterion_main!(benches); 