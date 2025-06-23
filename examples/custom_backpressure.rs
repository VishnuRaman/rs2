use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::time::Duration;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream that produces elements faster than they can be consumed
        let fast_stream = repeat(1).take(1000);

        // Configure custom backpressure
        let config = BackpressureConfig {
            strategy: BackpressureStrategy::DropNewest,
            buffer_size: 10,
            low_watermark: Some(3),
            high_watermark: Some(8),
        };

        // Apply custom backpressure
        let controlled_stream = fast_stream.auto_backpressure_with_rs2(config);

        // Process elements with a delay to simulate slow consumption
        let result = controlled_stream
            .eval_map_rs2(|x| async move {
                tokio::time::sleep(Duration::from_millis(10)).await;
                x
            })
            .collect::<Vec<_>>()
            .await;

        println!("Processed {} elements with backpressure", result.len());
    });
}
