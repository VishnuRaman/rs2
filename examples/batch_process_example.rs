use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use std::error::Error;

// Define a simple data structure for our examples
#[derive(Debug, Clone)]
struct LogEntry {
    timestamp: u64,
    level: String,
    message: String,
}

// Simulate a slow database operation
async fn save_logs_to_database(logs: Vec<LogEntry>) -> Result<String, Box<dyn Error + Send + Sync>> {
    println!("Saving batch of {} logs to database...", logs.len());
    // Simulate database latency
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(format!("Successfully saved {} logs", logs.len()))
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("\n=== Basic Batch Processing Example ===");

        // Create a stream of numbers
        let numbers = from_iter(1..=20);

        // Process numbers in batches of 5, calculating the sum of each batch
        let batch_sums = numbers
            .batch_process_rs2(5, |batch| {
                let sum: i32 = batch.iter().sum();
                vec![sum]
            })
            .collect::<Vec<_>>()
            .await;

        println!("Batch sums (batch size 5): {:?}", batch_sums);

        println!("\n=== Batch Processing with Transformation Example ===");

        // Create a stream of strings
        let words = from_iter(vec![
            "hello", "world", "batch", "processing", "example",
            "with", "rs2", "is", "efficient", "and", "powerful"
        ]);

        // Process words in batches of 3, converting each batch to uppercase
        let uppercase_batches = words
            .batch_process_rs2(3, |batch| {
                batch.into_iter()
                    .map(|word| word.to_uppercase())
                    .collect()
            })
            .collect::<Vec<_>>()
            .await;

        println!("Uppercase batches (batch size 3):");
        for (i, batch) in uppercase_batches.iter().enumerate() {
            println!("  Batch {}: {:?}", i + 1, batch);
        }

        println!("\n=== Batch Processing for Database Operations ===");

        // Create a stream of log entries
        let logs = from_iter((0..15).map(|i| LogEntry {
            timestamp: i,
            level: if i % 3 == 0 { "ERROR".to_string() } else { "INFO".to_string() },
            message: format!("Log message {}", i),
        }));

        // Process logs in batches of 4, saving each batch to a database
        let results = logs
            .batch_process_rs2(4, |batch| {
                // In a real application, we would save the batch to a database
                // For this example, we'll just return a success message
                vec![format!("Processed batch of {} logs", batch.len())]
            })
            .collect::<Vec<_>>()
            .await;

        println!("Database batch processing results:");
        for result in results {
            println!("  {}", result);
        }

        println!("\n=== Async Batch Processing Example ===");

        // Create a stream of log entries
        let logs = from_iter((0..12).map(|i| LogEntry {
            timestamp: i,
            level: if i % 3 == 0 { "ERROR".to_string() } else { "INFO".to_string() },
            message: format!("Log message {}", i),
        }));

        // First batch the logs
        let batched_logs = logs
            .chunks(3)
            .collect::<Vec<_>>()
            .await;

        // Then process each batch asynchronously
        let results = from_iter(batched_logs)
            .eval_map_rs2(|batch| async move {
                match save_logs_to_database(batch).await {
                    Ok(result) => result,
                    Err(e) => format!("Error: {}", e),
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("Async database batch processing results:");
        for result in results {
            println!("  {}", result);
        }
    });
}
