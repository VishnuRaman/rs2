use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use rs2_stream::rs2::*;
use std::time::Duration;
use tokio::runtime::Runtime;

fn bench_backpressure_strategies(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("backpressure_strategies");
    group.measurement_time(Duration::from_secs(10));

    let data_size = 100_000;
    let strategies = vec![
        BackpressureStrategy::Block,
        BackpressureStrategy::DropOldest,
        BackpressureStrategy::DropNewest,
    ];

    // Fast producer, slow consumer scenario
    for strategy in strategies {
        group.bench_with_input(
            BenchmarkId::new("fast_producer_slow_consumer", format!("{:?}", strategy)),
            &strategy,
            |b, strategy| {
                b.to_async(&rt).iter(|| async {
                    let config = BackpressureConfig {
                        strategy: *strategy,
                        buffer_size: 1000,
                        low_watermark: Some(250),
                        high_watermark: Some(750),
                    };

                    let result = from_iter(0..data_size)
                        .auto_backpressure_with_rs2(config)
                        .eval_map_rs2(|x| async move {
                            // Simulate slow consumer
                            if x % 100 == 0 {
                                tokio::time::sleep(Duration::from_micros(10)).await;
                            }
                            black_box(x)
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

criterion_group!(benches, bench_backpressure_strategies);
criterion_main!(benches);
