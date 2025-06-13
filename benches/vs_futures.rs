use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use futures::StreamExt;
use rs2::rs2::*;
use tokio::runtime::Runtime;

fn bench_vs_futures(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("vs_futures");
    let size = 100_000;

    // RS2 implementation
    group.bench_function("rs2_map_filter", |b| {
        b.to_async(&rt).iter(|| async {
            let result = from_iter(0..size)
                .map_rs2(|x| black_box(x * 2))
                .filter_rs2(|&x| black_box(x % 4 == 0))
                .collect_rs2::<Vec<_>>()
                .await;
            black_box(result)
        });
    });

    // futures-rs implementation
    group.bench_function("futures_map_filter", |b| {
        b.to_async(&rt).iter(|| async {
            let stream = futures::stream::iter(0..size);
            let result: Vec<_> = stream
                .map(|x| black_box(x * 2))
                .filter(|&x| futures::future::ready(black_box(x % 4 == 0)))
                .collect()
                .await;
            black_box(result)
        });
    });

    group.finish();
}

criterion_group!(benches, bench_vs_futures);
criterion_main!(benches);