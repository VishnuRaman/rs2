use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("Demonstrating chunk_rs2 extension method");

        // Create a stream of numbers
        let stream = from_iter(1..=10);
        println!("Stream created with numbers 1 to 10");

        // Use chunk_rs2 to process the stream in chunks of 3
        println!("Chunking stream into groups of 3...");
        let chunked_stream = stream.chunk_rs2(3);

        // Process each chunk
        let mut chunk_count = 0;
        let mut chunked_stream = chunked_stream.boxed();

        while let Some(chunk) = chunked_stream.next().await {
            chunk_count += 1;
            println!("Chunk {}: {:?}", chunk_count, chunk);

            // Calculate sum of the chunk
            let sum: i32 = chunk.iter().sum();
            println!("  Sum of chunk {}: {}", chunk_count, sum);

            // Calculate average of the chunk
            let avg: f64 = sum as f64 / chunk.len() as f64;
            println!("  Average of chunk {}: {:.2}", chunk_count, avg);

            println!();
        }

        println!("Processed {} chunks", chunk_count);
        println!("Example completed!");
    });
}
