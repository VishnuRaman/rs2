use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use tokio::runtime::Runtime;


/// Set by `release_resource` so the example can assert the finalizer ran —
/// that guarantee is the whole reason to use bracket.
static RELEASED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// Acquire a resource - returns a path to the file
async fn acquire_resource() -> PathBuf {
    println!("Resource acquired: data.txt");
    PathBuf::from("data.txt")
}

// Release a resource
async fn release_resource(path: PathBuf) {
    println!("Resource released: {}", path.display());
    RELEASED.store(true, std::sync::atomic::Ordering::SeqCst);
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
        println!("Using bracket_rs extension method for resource management");

        // Use bracket_rs extension method to ensure resource is released
        let result = empty::<i32>()
            .bracket_rs(
                acquire_resource(),
                |reader| use_resource(reader),
                release_resource,
            )
            .collect::<Vec<_>>()
            .await;

        println!("Lines: {:?}", result);
        // Invariants: every line is delivered, and the resource is released.
        assert_eq!(result.len(), 3, "all lines must be delivered");
        assert!(
            RELEASED.load(std::sync::atomic::Ordering::SeqCst),
            "bracket must release the resource"
        );
    });

    // Clean up the sample file
    std::fs::remove_file("data.txt").expect("Failed to remove data.txt");
}
