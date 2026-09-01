use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::time::Duration;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("Creating a stream that emits a value every 500ms using tick()");

        // `tick` is a stream *constructor*, not a transformation — it takes no
        // input stream. It used to also exist as `tick_rs`, an extension method
        // that discarded its receiver entirely, so `empty().tick_rs(..)` read as
        // if the empty stream mattered when it did not.
        let stream = tick(Duration::from_millis(500), "Tick!");

        println!("Stream created. Collecting 5 values...");

        // Take only 5 items to keep the example short
        let mut count = 0;
        let mut stream = stream.take(5).boxed();

        while let Some(item) = stream.next().await {
            count += 1;
            println!("Received item {}: {}", count, item);
        }

        // Invariant: `tick` repeats forever, so `take(5)` yields exactly 5.
        assert_eq!(count, 5, "take(5) over an infinite tick must yield 5 items");
        println!("Example completed!");
    });
}
