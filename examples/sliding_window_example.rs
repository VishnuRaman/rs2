use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("\n=== Basic Sliding Window Example ===");

        // Create a stream of numbers
        let numbers = from_iter(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

        // Apply a sliding window of size 3
        let windows = numbers
            .sliding_window_rs2(3)
            .collect::<Vec<_>>()
            .await;

        // Print each window
        println!("Sliding windows of size 3:");
        for (i, window) in windows.iter().enumerate() {
            println!("  Window {}: {:?}", i + 1, window);
        }

        println!("\n=== Sliding Window with Strings Example ===");

        // Create a stream of words
        let words = from_iter(vec![
            "The".to_string(),
            "quick".to_string(),
            "brown".to_string(),
            "fox".to_string(),
            "jumps".to_string(),
            "over".to_string(),
            "the".to_string(),
            "lazy".to_string(),
            "dog".to_string(),
        ]);

        // Apply a sliding window of size 4 to create phrases
        let phrases = words
            .sliding_window_rs2(4)
            .collect::<Vec<_>>()
            .await;

        // Print each phrase
        println!("Phrases (sliding windows of size 4):");
        for (i, phrase) in phrases.iter().enumerate() {
            println!("  Phrase {}: {}", i + 1, phrase.join(" "));
        }

        println!("\n=== Sliding Window for Time Series Analysis ===");

        // Create a stream of time series data (timestamp, value)
        let time_series = from_iter(vec![
            (0, 10.5),
            (1, 11.2),
            (2, 10.8),
            (3, 12.3),
            (4, 13.5),
            (5, 14.1),
            (6, 13.8),
            (7, 15.2),
            (8, 16.0),
            (9, 15.7),
        ]);

        // Apply a sliding window of size 3 for moving average calculation
        let moving_averages = time_series
            .sliding_window_rs2(3)
            .map_rs2(|window| {
                let sum: f64 = window.iter().map(|(_, value)| value).sum();
                let avg = sum / window.len() as f64;
                (window.last().unwrap().0, avg) // Use the last timestamp with the average
            })
            .collect::<Vec<_>>()
            .await;

        // Print the moving averages
        println!("Moving averages (window size 3):");
        for (timestamp, avg) in moving_averages {
            println!("  Time {}: {:.2}", timestamp, avg);
        }
    });
}