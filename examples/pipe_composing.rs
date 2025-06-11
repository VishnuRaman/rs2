use rs2::pipe::*;
use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of numbers
        let numbers = from_iter(vec![1, 2, 3, 4, 5, 6]);

        // Create pipes for different transformations
        let even_filter = filter(|&x: &i32| x % 2 == 0);
        let double_map = map(|x: i32| x * 2);

        // Compose the pipes using the compose function
        let even_then_double = compose(even_filter, double_map);

        // Apply the composed pipe to the stream
        let result_stream = even_then_double.apply(numbers);

        // Collect the results
        let result = result_stream.collect::<Vec<_>>().await;
        println!("Even numbers doubled: {:?}", result); // [4, 8, 12]

        // Alternatively, use the compose method on the Pipe
        let numbers = from_iter(vec![1, 2, 3, 4, 5, 6]);
        let even_filter = filter(|&x: &i32| x % 2 == 0);
        let double_map = map(|x: i32| x * 2);

        // Use the compose method
        let even_then_double = even_filter.compose(double_map);

        // Apply the composed pipe to the stream
        let result_stream = even_then_double.apply(numbers);

        // Collect the results
        let result = result_stream.collect::<Vec<_>>().await;
        println!("Even numbers doubled (using compose method): {:?}", result); // [4, 8, 12]
    });
}