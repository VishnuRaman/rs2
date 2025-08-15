use rs2_stream::rs2::*;
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

// Use a resource - function that takes PathBuf and returns a stream
fn use_resource(path: PathBuf) -> impl rs2_stream::stream::Stream<Item = String> + Send + 'static {
    // Open the file and create a reader
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening file {}: {}", path.display(), e);
            // Return an empty stream if file can't be opened
            return from_iter_rs2(vec![]);
        }
    };

    let reader = BufReader::new(file);
    let lines: Vec<String> = reader
        .lines()
        .map(|line| line.unwrap_or_else(|_| "Error reading line".to_string()))
        .collect();

    from_iter_rs2(lines)
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🔒 Resource Management with Bracket Pattern");
        println!("==========================================");

        // Create a test file
        std::fs::write("data.txt", "Line 1\nLine 2\nLine 3\n").expect("Failed to create test file");

        // Use bracket pattern from rs2 module
        // bracket(acquire_future, use_function, release_function)
        let result: Vec<String> = bracket(
            acquire_resource(),  // Future<Output = PathBuf>
            use_resource,        // FnOnce(PathBuf) -> Stream
            |path| release_resource(path),  // FnOnce(PathBuf) -> Future<Output = ()>
        ).collect_rs2().await;

        println!("\n📄 File contents:");
        for line in result {
            println!("  {}", line);
        }

        // Clean up test file
        let _ = std::fs::remove_file("data.txt");
        
        println!("\n✅ Resource management completed successfully!");
    });
}
