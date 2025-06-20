use async_stream::stream;
use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use tokio::runtime::Runtime;

// Acquire a resource - returns a path to the file
async fn acquire_resource() -> PathBuf {
    println!("Resource acquired: data.txt");
    PathBuf::from("data.txt")
}

// Release a resource with exit case information
async fn release_resource(path: PathBuf, exit_case: ExitCase<String>) {
    println!(
        "Resource released: {} with exit case: {:?}",
        path.display(),
        exit_case
    );
}

// Use a resource to create a stream of results
fn use_resource(path: PathBuf) -> RS2Stream<Result<String, String>> {
    // Open the file and create a reader
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(e) => {
            let error = format!("Error opening file: {}", e);
            println!("{}", error);
            return from_iter(vec![Err(error)]);
        }
    };

    let reader = BufReader::new(file);
    let lines = reader.lines();

    // Map each line to a Result
    stream! {
        for (i, line_result) in lines.enumerate() {
            match line_result {
                Ok(line) => {
                    // Simulate an error for demonstration purposes
                    if line.contains("ERROR") {
                        yield Err(format!("Error in line {}: {}", i + 1, line));
                    } else {
                        yield Ok(line);
                    }
                },
                Err(e) => yield Err(format!("IO error: {}", e)),
            }
        }
    }
    .boxed()
}

fn main() {
    // Create a sample data.txt file for the example
    std::fs::write("data.txt", "Line 1\nLine 2\nERROR in Line 3\nLine 4\n")
        .expect("Failed to create data.txt");

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("Demonstrating bracket_case extension method for resource management");

        // Create a stream of results
        let stream = from_iter(vec![Ok::<String, String>("Initial value".to_string())]);

        // Use bracket_case extension method to ensure resource is released
        let result = stream
            .bracket_case_rs2(
                acquire_resource(),
                |resource| use_resource(resource),
                release_resource,
            )
            .collect::<Vec<_>>()
            .await;

        println!("\nResults:");
        for (i, res) in result.iter().enumerate() {
            match res {
                Ok(value) => println!("  {}. Success: {}", i + 1, value),
                Err(error) => println!("  {}. Error: {}", i + 1, error),
            }
        }

        println!("\nNote: The resource is properly released regardless of errors in the stream");
    });

    // Clean up the sample file
    std::fs::remove_file("data.txt").expect("Failed to remove data.txt");
}
