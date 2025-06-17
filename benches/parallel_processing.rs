use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rs2_stream::rs2::*;
use tokio::runtime::Runtime;
use std::time::Duration;
use sha2::{Sha256, Digest};
use serde_json::Value;

// Lightweight CPU task - should show minimal parallel benefit
async fn light_cpu_task(x: i32) -> i32 {
    // Just some basic arithmetic - very fast
    let mut result = x;
    for _ in 0..5 {
        result = result.wrapping_mul(17).wrapping_add(1);
    }
    black_box(result)
}

// Medium CPU task - should show good parallel benefits
async fn medium_cpu_task(x: i32) -> String {
    // Cryptographic hashing - moderately CPU intensive
    let mut hasher = Sha256::new();
    let input = format!("data-{}-{}", x, x * x);

    // Hash multiple times to increase CPU work
    for i in 0..10 {
        hasher.update(format!("{}-{}", input, i).as_bytes());
    }

    let result = hasher.finalize();
    black_box(format!("{:x}", result))
}

// Heavy CPU task - should show significant parallel benefits
async fn heavy_cpu_task(x: i32) -> (String, f64) {
    // Complex JSON processing + mathematical computation
    let json_data = format!(r#"{{
        "id": {},
        "data": [{}],
        "metadata": {{
            "processed": true,
            "timestamp": {},
            "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        }}
    }}"#, x, (0..50).map(|i| (i * x).to_string()).collect::<Vec<_>>().join(","), x * 1000);

    // Parse and process JSON (CPU intensive)
    let parsed: Value = serde_json::from_str(&json_data).unwrap();

    // Heavy mathematical computation
    let mut result = 0.0f64;
    for i in 0..50 {
        result += (x as f64 * i as f64).sin().cos().tan().abs();
    }

    // String processing with multiple hashing rounds
    let hash = {
        let mut hasher = Sha256::new();
        for _ in 0..50 {
            hasher.update(parsed.to_string().as_bytes());
            hasher.update(result.to_string().as_bytes());
        }
        format!("{:x}", hasher.finalize())
    };

    black_box((hash, result))
}

// I/O simulation task - should show excellent parallel benefits
async fn io_simulation_task(x: i32) -> String {
    // Simulate variable database/network delays
    let delay = match x % 10 {
        0..=2 => Duration::from_millis(5),   // Fast queries
        3..=6 => Duration::from_millis(15),  // Medium queries  
        7..=8 => Duration::from_millis(30),  // Slow queries
        _ => Duration::from_millis(50),      // Very slow queries
    };

    tokio::time::sleep(delay).await;

    // Some light processing after "I/O"
    let mut hasher = Sha256::new();
    hasher.update(format!("io-result-{}-{}", x, delay.as_millis()).as_bytes());
    black_box(format!("{:x}", hasher.finalize()))
}

// Variable workload task - demonstrates load balancing benefits
async fn variable_workload_task(x: i32) -> i32 {
    // Create highly variable workloads to test load balancing
    let work_amount = match x % 20 {
        0..=4 => 10,      // Light work (25% of tasks)
        5..=14 => 100,    // Medium work (50% of tasks)  
        15..=17 => 1000,  // Heavy work (15% of tasks)
        _ => 5000,        // Very heavy work (10% of tasks)
    };

    let mut result = x;
    for _ in 0..work_amount {
        result = result.wrapping_mul(17).wrapping_add(1);
        if work_amount > 100 {
            // Add some async yielding for heavy tasks
            if result % 100 == 0 {
                tokio::task::yield_now().await;
            }
        }
    }

    black_box(result)
}

fn bench_parallel_processing_comprehensive(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel_processing_comprehensive");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(30);

    // Test configurations: (task_name, data_size, description)
    let test_configs = vec![
        ("light_cpu", 2000, "Light CPU work - minimal parallel benefit expected"),
        ("medium_cpu", 500, "Medium CPU work - good parallel benefits"),
        ("heavy_cpu", 100, "Heavy CPU work - significant parallel benefits"),
        ("io_simulation", 200, "I/O simulation - excellent parallel benefits"),
        ("variable_workload", 400, "Variable workload - tests load balancing"),
    ];

    for (task_name, data_size, _description) in test_configs {
        // Sequential baseline
        group.bench_with_input(
            BenchmarkId::new(format!("{}_sequential", task_name), data_size),
            &(task_name, data_size),
            |b, &(task_type, size)| {
                b.to_async(&rt).iter(|| async {
                    let result = match task_type {
                        "light_cpu" => {
                            from_iter(0..size)
                                .eval_map_rs2(|x| light_cpu_task(x))
                                .map_rs2(|_| 1) // Normalize result type
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        "medium_cpu" => {
                            from_iter(0..size)
                                .eval_map_rs2(|x| medium_cpu_task(x))
                                .map_rs2(|_| 1)
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        "heavy_cpu" => {
                            from_iter(0..size)
                                .eval_map_rs2(|x| heavy_cpu_task(x))
                                .map_rs2(|_| 1)
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        "io_simulation" => {
                            from_iter(0..size)
                                .eval_map_rs2(|x| io_simulation_task(x))
                                .map_rs2(|_| 1)
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        "variable_workload" => {
                            from_iter(0..size)
                                .eval_map_rs2(|x| variable_workload_task(x))
                                .map_rs2(|_| 1)
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        _ => vec![],
                    };
                    black_box(result)
                });
            },
        );

        // Optimal concurrency levels per task type
        let concurrency_levels = match task_name {
            "light_cpu" => vec![2, 4], // Lower concurrency for light tasks
            "medium_cpu" => vec![2, 4, 8], // Moderate concurrency
            "heavy_cpu" => vec![2, 4, 8, num_cpus::get()], // Up to CPU count
            "io_simulation" => vec![8, 16, 32, 64], // High concurrency for I/O
            "variable_workload" => vec![4, 8, 16], // Medium-high for load balancing
            _ => vec![4],
        };

        for concurrency in concurrency_levels {
            // Parallel ordered processing
            group.bench_with_input(
                BenchmarkId::new(format!("{}_parallel_ordered", task_name), format!("{}_{}", data_size, concurrency)),
                &(task_name, data_size, concurrency),
                |b, &(task_type, size, conc)| {
                    b.to_async(&rt).iter(|| async {
                        let result = match task_type {
                            "light_cpu" => {
                                from_iter(0..size)
                                    .par_eval_map_rs2(conc, |x| light_cpu_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "medium_cpu" => {
                                from_iter(0..size)
                                    .par_eval_map_rs2(conc, |x| medium_cpu_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "heavy_cpu" => {
                                from_iter(0..size)
                                    .par_eval_map_rs2(conc, |x| heavy_cpu_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "io_simulation" => {
                                from_iter(0..size)
                                    .par_eval_map_rs2(conc, |x| io_simulation_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "variable_workload" => {
                                from_iter(0..size)
                                    .par_eval_map_rs2(conc, |x| variable_workload_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            _ => vec![],
                        };
                        black_box(result)
                    });
                },
            );

            // Parallel unordered processing (should be faster)
            group.bench_with_input(
                BenchmarkId::new(format!("{}_parallel_unordered", task_name), format!("{}_{}", data_size, concurrency)),
                &(task_name, data_size, concurrency),
                |b, &(task_type, size, conc)| {
                    b.to_async(&rt).iter(|| async {
                        let result = match task_type {
                            "light_cpu" => {
                                from_iter(0..size)
                                    .par_eval_map_unordered_rs2(conc, |x| light_cpu_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "medium_cpu" => {
                                from_iter(0..size)
                                    .par_eval_map_unordered_rs2(conc, |x| medium_cpu_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "heavy_cpu" => {
                                from_iter(0..size)
                                    .par_eval_map_unordered_rs2(conc, |x| heavy_cpu_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "io_simulation" => {
                                from_iter(0..size)
                                    .par_eval_map_unordered_rs2(conc, |x| io_simulation_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "variable_workload" => {
                                from_iter(0..size)
                                    .par_eval_map_unordered_rs2(conc, |x| variable_workload_task(x))
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            _ => vec![],
                        };
                        black_box(result)
                    });
                },
            );
        }
    }

    group.finish();
}

// Focused benchmark for scaling analysis
fn bench_parallel_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("parallel_scaling");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(25);

    let data_size = 100; // Fixed size for scaling test
    let max_concurrency = (num_cpus::get() * 2).min(32); // Cap at 32 to keep benchmarks reasonable

    // Test scaling from 1 to max_concurrency for different workload types
    for concurrency in [1, 2, 4, 8, 16, max_concurrency].iter().filter(|&&c| c <= max_concurrency) {
        let concurrency = *concurrency;

        // CPU-bound scaling test
        group.bench_with_input(
            BenchmarkId::new("heavy_cpu_scaling", concurrency),
            &concurrency,
            |b, &conc| {
                b.to_async(&rt).iter(|| async {
                    let result = if conc == 1 {
                        // Sequential for concurrency = 1
                        from_iter(0..data_size)
                            .eval_map_rs2(|x| heavy_cpu_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    } else {
                        // Parallel for concurrency > 1
                        from_iter(0..data_size)
                            .par_eval_map_unordered_rs2(conc, |x| heavy_cpu_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    };
                    black_box(result)
                });
            },
        );

        // I/O-bound scaling test  
        group.bench_with_input(
            BenchmarkId::new("io_simulation_scaling", concurrency),
            &concurrency,
            |b, &conc| {
                b.to_async(&rt).iter(|| async {
                    let result = if conc == 1 {
                        from_iter(0..data_size)
                            .eval_map_rs2(|x| io_simulation_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    } else {
                        from_iter(0..data_size)
                            .par_eval_map_unordered_rs2(conc, |x| io_simulation_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    };
                    black_box(result)
                });
            },
        );

        // Variable workload scaling test
        group.bench_with_input(
            BenchmarkId::new("variable_workload_scaling", concurrency),
            &concurrency,
            |b, &conc| {
                b.to_async(&rt).iter(|| async {
                    let result = if conc == 1 {
                        from_iter(0..data_size)
                            .eval_map_rs2(|x| variable_workload_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    } else {
                        from_iter(0..data_size)
                            .par_eval_map_unordered_rs2(conc, |x| variable_workload_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    };
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

// Benchmark comparing ordered vs unordered parallel processing
fn bench_ordered_vs_unordered(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("ordered_vs_unordered");
    group.measurement_time(Duration::from_secs(12));
    group.sample_size(20);

    let data_size = 200;
    let concurrency = num_cpus::get();

    // Test different task types to show when ordering matters
    let task_configs = vec![
        ("uniform_fast", "Fast uniform tasks"),
        ("uniform_slow", "Slow uniform tasks"),
        ("variable_mixed", "Mixed variable tasks"),
    ];

    for (task_type, _description) in task_configs {
        // Ordered parallel processing
        group.bench_with_input(
            BenchmarkId::new("ordered", task_type),
            &task_type,
            |b, &task| {
                b.to_async(&rt).iter(|| {
                    let task = task;
                    async move {
                        let result = match task {
                            "uniform_fast" => {
                                from_iter(0..data_size)
                                    .par_eval_map_rs2(concurrency, |x| light_cpu_task(x))
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "uniform_slow" => {
                                from_iter(0..data_size)
                                    .par_eval_map_rs2(concurrency, |x| medium_cpu_task(x))
                                    .map_rs2(|_| 1) // Map to i32 to match other arms
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "variable_mixed" => {
                                from_iter(0..data_size)
                                    .par_eval_map_rs2(concurrency, |x| variable_workload_task(x))
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            _ => vec![],
                        };
                        black_box(result)
                    }
                });
            },
        );

        // Unordered parallel processing
        group.bench_with_input(
            BenchmarkId::new("unordered", task_type),
            &task_type,
            |b, &task| {
                b.to_async(&rt).iter(|| {
                    let task = task;
                    async move {
                        let result = match task {
                            "uniform_fast" => {
                                from_iter(0..data_size)
                                    .par_eval_map_unordered_rs2(concurrency, |x| light_cpu_task(x))
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "uniform_slow" => {
                                from_iter(0..data_size)
                                    .par_eval_map_unordered_rs2(concurrency, |x| medium_cpu_task(x))
                                    .map_rs2(|_| 1) // Map to i32 to match other arms
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "variable_mixed" => {
                                from_iter(0..data_size)
                                    .par_eval_map_unordered_rs2(concurrency, |x| variable_workload_task(x))
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            _ => vec![],
                        };
                        black_box(result)
                    }
                });
            },
        );
    }

    group.finish();
}

// Benchmark optimal concurrency levels for different workloads
fn bench_concurrency_optimization(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("concurrency_optimization");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(15);

    let data_size = 150;
    let max_concurrency = num_cpus::get() * 4; // Test beyond CPU count

    // Find optimal concurrency for I/O-bound tasks
    for concurrency in [1, 2, 4, 8, 16, 32, 64].iter().filter(|&&c| c <= max_concurrency) {
        let concurrency = *concurrency;

        group.bench_with_input(
            BenchmarkId::new("io_bound_concurrency", concurrency),
            &concurrency,
            |b, &conc| {
                b.to_async(&rt).iter(|| async {
                    let result = if conc == 1 {
                        from_iter(0..data_size)
                            .eval_map_rs2(|x| io_simulation_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    } else {
                        from_iter(0..data_size)
                            .par_eval_map_unordered_rs2(conc, |x| io_simulation_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    };
                    black_box(result)
                });
            },
        );
    }

    // Find optimal concurrency for CPU-bound tasks
    let cpu_concurrency_levels: Vec<usize> = [1, 2, 4, 8, 16]
        .iter()
        .filter(|&&c| c <= max_concurrency)
        .cloned()
        .collect();

    for concurrency in cpu_concurrency_levels {
        group.bench_with_input(
            BenchmarkId::new("cpu_bound_concurrency", concurrency),
            &concurrency,
            |b, &conc| {
                b.to_async(&rt).iter(|| async {
                    let result = if conc == 1 {
                        from_iter(0..data_size)
                            .eval_map_rs2(|x| heavy_cpu_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    } else {
                        from_iter(0..data_size)
                            .par_eval_map_unordered_rs2(conc, |x| heavy_cpu_task(x))
                            .collect_rs2::<Vec<_>>()
                            .await
                    };
                    black_box(result)
                });
            },
        );
    }

    group.finish();
}

// Benchmark map_parallel_rs2 and map_parallel_with_concurrency_rs2 functions
fn bench_map_parallel_functions(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("map_parallel_functions");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(20);

    // Test configurations: (task_name, data_size, description)
    let test_configs = vec![
        ("light_cpu", 2000, "Light CPU work - minimal parallel benefit expected"),
        ("medium_cpu", 500, "Medium CPU work - good parallel benefits"),
        ("heavy_cpu", 100, "Heavy CPU work - significant parallel benefits"),
        ("io_simulation", 200, "I/O simulation - excellent parallel benefits"),
        ("variable_workload", 400, "Variable workload - tests load balancing"),
    ];

    // Synchronous versions of the async tasks for map_parallel_rs2
    let light_cpu_sync = |x: i32| -> i32 {
        let mut result = x;
        for _ in 0..5 {
            result = result.wrapping_mul(17).wrapping_add(1);
        }
        black_box(result)
    };

    let medium_cpu_sync = |x: i32| -> String {
        let mut hasher = Sha256::new();
        let input = format!("data-{}-{}", x, x * x);

        for i in 0..10 {
            hasher.update(format!("{}-{}", input, i).as_bytes());
        }

        let result = hasher.finalize();
        black_box(format!("{:x}", result))
    };

    let heavy_cpu_sync = |x: i32| -> (String, f64) {
        let json_data = format!(r#"{{
            "id": {},
            "data": [{}],
            "metadata": {{
                "processed": true,
                "timestamp": {},
                "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
            }}
        }}"#, x, (0..50).map(|i| (i * x).to_string()).collect::<Vec<_>>().join(","), x * 1000);

        let parsed: Value = serde_json::from_str(&json_data).unwrap();

        let mut result = 0.0f64;
        for i in 0..50 {
            result += (x as f64 * i as f64).sin().cos().tan().abs();
        }

        let hash = {
            let mut hasher = Sha256::new();
            for _ in 0..50 {
                hasher.update(parsed.to_string().as_bytes());
                hasher.update(result.to_string().as_bytes());
            }
            format!("{:x}", hasher.finalize())
        };

        black_box((hash, result))
    };

    // Note: We can't directly use io_simulation_task and variable_workload_task with map_parallel_rs2
    // because they're async functions. For benchmarking purposes, we'll use medium_cpu_sync instead.

    for (task_name, data_size, _description) in test_configs {
        // Skip I/O and variable workload tasks for map_parallel_rs2 since they require async
        if task_name == "io_simulation" || task_name == "variable_workload" {
            continue;
        }

        // Sequential baseline (using map_rs2)
        group.bench_with_input(
            BenchmarkId::new(format!("{}_sequential", task_name), data_size),
            &(task_name, data_size),
            |b, &(task_type, size)| {
                b.to_async(&rt).iter(|| async {
                    let result = match task_type {
                        "light_cpu" => {
                            from_iter(0..size)
                                .map_rs2(light_cpu_sync)
                                .map_rs2(|_| 1) // Normalize result type
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        "medium_cpu" => {
                            from_iter(0..size)
                                .map_rs2(medium_cpu_sync)
                                .map_rs2(|_| 1)
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        "heavy_cpu" => {
                            from_iter(0..size)
                                .map_rs2(heavy_cpu_sync)
                                .map_rs2(|_| 1)
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        _ => vec![],
                    };
                    black_box(result)
                });
            },
        );

        // map_parallel_rs2 (automatic concurrency)
        group.bench_with_input(
            BenchmarkId::new(format!("{}_map_parallel", task_name), data_size),
            &(task_name, data_size),
            |b, &(task_type, size)| {
                b.to_async(&rt).iter(|| async {
                    let result = match task_type {
                        "light_cpu" => {
                            from_iter(0..size)
                                .map_parallel_rs2(light_cpu_sync)
                                .map_rs2(|_| 1) // Normalize result type
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        "medium_cpu" => {
                            from_iter(0..size)
                                .map_parallel_rs2(medium_cpu_sync)
                                .map_rs2(|_| 1)
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        "heavy_cpu" => {
                            from_iter(0..size)
                                .map_parallel_rs2(heavy_cpu_sync)
                                .map_rs2(|_| 1)
                                .collect_rs2::<Vec<_>>()
                                .await
                        },
                        _ => vec![],
                    };
                    black_box(result)
                });
            },
        );

        // Optimal concurrency levels per task type for map_parallel_with_concurrency_rs2
        let concurrency_levels = match task_name {
            "light_cpu" => vec![2, 4], // Lower concurrency for light tasks
            "medium_cpu" => vec![2, 4, 8], // Moderate concurrency
            "heavy_cpu" => vec![2, 4, 8, num_cpus::get()], // Up to CPU count
            _ => vec![4],
        };

        for concurrency in concurrency_levels {
            // map_parallel_with_concurrency_rs2 (custom concurrency)
            group.bench_with_input(
                BenchmarkId::new(format!("{}_map_parallel_with_concurrency", task_name), format!("{}_{}", data_size, concurrency)),
                &(task_name, data_size, concurrency),
                |b, &(task_type, size, conc)| {
                    b.to_async(&rt).iter(|| async {
                        let result = match task_type {
                            "light_cpu" => {
                                from_iter(0..size)
                                    .map_parallel_with_concurrency_rs2(conc, light_cpu_sync)
                                    .map_rs2(|_| 1) // Normalize result type
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "medium_cpu" => {
                                from_iter(0..size)
                                    .map_parallel_with_concurrency_rs2(conc, medium_cpu_sync)
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            "heavy_cpu" => {
                                from_iter(0..size)
                                    .map_parallel_with_concurrency_rs2(conc, heavy_cpu_sync)
                                    .map_rs2(|_| 1)
                                    .collect_rs2::<Vec<_>>()
                                    .await
                            },
                            _ => vec![],
                        };
                        black_box(result)
                    });
                },
            );
        }
    }

    group.finish();
}

// Define a static function for heavy CPU work to avoid lifetime issues
fn heavy_cpu_work(x: i32) -> (String, f64) {
    let json_data = format!(r#"{{
        "id": {},
        "data": [{}],
        "metadata": {{
            "processed": true,
            "timestamp": {},
            "values": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        }}
    }}"#, x, (0..50).map(|i| (i * x).to_string()).collect::<Vec<_>>().join(","), x * 1000);

    let parsed: Value = serde_json::from_str(&json_data).unwrap();

    let mut result = 0.0f64;
    for i in 0..50 {
        result += (x as f64 * i as f64).sin().cos().tan().abs();
    }

    let hash = {
        let mut hasher = Sha256::new();
        for _ in 0..50 {
            hasher.update(parsed.to_string().as_bytes());
            hasher.update(result.to_string().as_bytes());
        }
        format!("{:x}", hasher.finalize())
    };

    black_box((hash, result))
}

// Compare map_parallel_rs2 with par_eval_map_rs2 for CPU-bound tasks
fn bench_map_parallel_vs_par_eval_map(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("map_parallel_vs_par_eval_map");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(20);

    let data_size = 200;
    let concurrency = num_cpus::get();

    // Sequential baseline
    group.bench_with_input(
        BenchmarkId::new("sequential", data_size),
        &data_size,
        |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter(0..size)
                    .map_rs2(heavy_cpu_work)
                    .map_rs2(|_| 1) // Normalize result type
                    .collect_rs2::<Vec<_>>()
                    .await;
                black_box(result)
            });
        },
    );

    // map_parallel_rs2 (automatic concurrency)
    group.bench_with_input(
        BenchmarkId::new("map_parallel_rs2", data_size),
        &data_size,
        |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter(0..size)
                    .map_parallel_rs2(heavy_cpu_work)
                    .map_rs2(|_| 1) // Normalize result type
                    .collect_rs2::<Vec<_>>()
                    .await;
                black_box(result)
            });
        },
    );

    // map_parallel_with_concurrency_rs2 (explicit concurrency)
    group.bench_with_input(
        BenchmarkId::new("map_parallel_with_concurrency_rs2", data_size),
        &data_size,
        |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter(0..size)
                    .map_parallel_with_concurrency_rs2(concurrency, heavy_cpu_work)
                    .map_rs2(|_| 1) // Normalize result type
                    .collect_rs2::<Vec<_>>()
                    .await;
                black_box(result)
            });
        },
    );

    // par_eval_map_rs2 (for comparison)
    group.bench_with_input(
        BenchmarkId::new("par_eval_map_rs2", data_size),
        &data_size,
        |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter(0..size)
                    .par_eval_map_rs2(concurrency, |x| async move { heavy_cpu_work(x) })
                    .map_rs2(|_| 1) // Normalize result type
                    .collect_rs2::<Vec<_>>()
                    .await;
                black_box(result)
            });
        },
    );

    // par_eval_map_unordered_rs2 (for comparison)
    group.bench_with_input(
        BenchmarkId::new("par_eval_map_unordered_rs2", data_size),
        &data_size,
        |b, &size| {
            b.to_async(&rt).iter(|| async {
                let result = from_iter(0..size)
                    .par_eval_map_unordered_rs2(concurrency, |x| async move { heavy_cpu_work(x) })
                    .map_rs2(|_| 1) // Normalize result type
                    .collect_rs2::<Vec<_>>()
                    .await;
                black_box(result)
            });
        },
    );

    group.finish();
}

criterion_group!(
    benches,
    bench_parallel_processing_comprehensive,
    bench_parallel_scaling,
    bench_ordered_vs_unordered,
    bench_concurrency_optimization,
    bench_map_parallel_functions,
    bench_map_parallel_vs_par_eval_map
);
criterion_main!(benches);
