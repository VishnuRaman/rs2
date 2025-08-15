use rs2_stream::stream::{from_iter, empty, Stream, StreamExt};
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use tokio::runtime::Runtime;

// Acquire a resource - returns a path to the file
async fn acquire_resource() -> PathBuf {
    println!("Resource acquired: data.txt");
    PathBuf::from("data.txt")
}

// Release a resource
async fn release_resource(path: PathBuf) {
    println!("Resource released: {}", path.display());
}

// Use a resource
fn use_resource(path: PathBuf) -> impl Stream<Item = String> + Send + 'static {
    // Open the file and create a reader
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening file: {}", e);
            return from_iter(Vec::<String>::new());
        }
    };

    let reader = BufReader::new(file);
    let lines: Vec<String> = reader.lines().filter_map(Result::ok).collect();
    from_iter(lines)
}

fn main() {
    // Create a sample data.txt file for the example
    std::fs::write("data.txt", "Line 1\nLine 2\nLine 3\n").expect("Failed to create data.txt");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("Using bracket_rs2 extension method for resource management");

        // Use bracket_rs2 extension method to ensure resource is released
        let result = empty::<i32>()
            .bracket_rs2(
                acquire_resource(),
                |reader| use_resource(reader),
                release_resource,
            )
            .collect::<Vec<_>>()
            .await;

        println!("Lines: {:?}", result);
    });

    // Clean up the sample file
    std::fs::remove_file("data.txt").expect("Failed to remove data.txt");
}
