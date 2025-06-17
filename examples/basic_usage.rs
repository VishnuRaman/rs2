use rs2_stream::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream from an iterator
        let stream = from_iter(vec![1, 2, 3, 4, 5]);

        // Apply transformations
        let result = stream
            .filter_rs2(|&x| x % 2 == 0)  // Keep only even numbers
            .map_rs2(|x| x * 2)           // Double each number
            .collect::<Vec<_>>()          // Collect into a Vec
            .await;

        println!("Result: {:?}", result);  // Output: Result: [4, 8]
    });
}