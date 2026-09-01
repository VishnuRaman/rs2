use futures_util::stream::StreamExt;
use rs2_stream::pipe::*;
use rs2_stream::rs2::*;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let numbers = from_iter(vec![1, 2, 3, 4, 5]);

        // Create a pipe that doubles each number
        let double_pipe = map(|x: i32| x * 2);

        // Apply the pipe to the stream
        let doubled = double_pipe.apply(numbers);

        // Collect the results
        let result = doubled.collect::<Vec<_>>().await;
        println!("Doubled: {:?}", result);
        assert_eq!(result, vec![2, 4, 6, 8, 10]);

        // --- identity ----------------------------------------------------
        // `identity()` passes the stream through untouched — useful as the
        // neutral element when a pipe is assembled conditionally.
        let passthrough: Pipe<i32, i32> = identity();
        let out = passthrough
            .apply(from_iter(vec![1, 2, 3]))
            .collect::<Vec<_>>()
            .await;
        println!("\nidentity pipe -> {:?}", out);

        let double_it = false;
        let maybe_double: Pipe<i32, i32> = if double_it {
            map(|x: i32| x * 2)
        } else {
            identity()
        };
        let out = maybe_double
            .apply(from_iter(vec![1, 2, 3]))
            .collect::<Vec<_>>()
            .await;
        println!("conditional pipe (disabled) -> {:?}", out);
    });
}
