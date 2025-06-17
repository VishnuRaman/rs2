use rs2_stream::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::fs::File;
use std::io::{self, BufReader, BufRead};
use std::path::PathBuf;

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
fn use_resource(path: PathBuf) -> RS2Stream<String> {
    // Open the file and create a reader
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening file: {}", e);
            return empty();
        }
    };

    let reader = BufReader::new(file);
    let lines = reader.lines().filter_map(Result::ok);
    from_iter(lines)
}

fn main() {
    // Create a sample data.txt file for the example
    std::fs::write("data.txt", "Line 1\nLine 2\nLine 3\n").expect("Failed to create data.txt");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Use bracket to ensure resource is released
        let result = bracket(
            acquire_resource(),
            |reader| use_resource(reader),
            release_resource
        )
        .collect::<Vec<_>>()
        .await;

        println!("Lines: {:?}", result);
    });

    // Clean up the sample file
    std::fs::remove_file("data.txt").expect("Failed to remove data.txt");
}
