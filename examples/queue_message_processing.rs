use futures_util::stream::StreamExt;
use rs2_stream::queue::*;
use rs2_stream::rs2::*;
use std::error::Error;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};

// Define our Message type
#[derive(Debug, Clone)]
struct Message {
    id: u64,
    content: String,
    priority: Priority,
    timestamp: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum Priority {
    Low,
    Medium,
    High,
}

// Simulate message processing
async fn process_message(msg: Message) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Processing message {}: '{}'", msg.id, msg.content);

    // Simulate processing time based on priority
    let delay = match msg.priority {
        Priority::High => 5,
        Priority::Medium => 10,
        Priority::Low => 20,
    };

    sleep(Duration::from_millis(delay)).await;
    println!("Completed message {}", msg.id);
    Ok(())
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create queues for different priority levels
        let high_priority_queue = Arc::new(Queue::bounded(5));
        let medium_priority_queue = Arc::new(Queue::bounded(10));
        let low_priority_queue = Arc::new(Queue::bounded(20));

        // Create some test messages
        let messages = vec![
            Message {
                id: 1,
                content: "Critical system alert".to_string(),
                priority: Priority::High,
                timestamp: 1000,
            },
            Message {
                id: 2,
                content: "User login".to_string(),
                priority: Priority::Medium,
                timestamp: 1001,
            },
            Message {
                id: 3,
                content: "Log rotation".to_string(),
                priority: Priority::Low,
                timestamp: 1002,
            },
            Message {
                id: 4,
                content: "Security breach detected".to_string(),
                priority: Priority::High,
                timestamp: 1003,
            },
            Message {
                id: 5,
                content: "New user registration".to_string(),
                priority: Priority::Medium,
                timestamp: 1004,
            },
            Message {
                id: 6,
                content: "Daily report".to_string(),
                priority: Priority::Low,
                timestamp: 1005,
            },
        ];

        // Distribute messages to appropriate queues
        for msg in messages {
            let queue = match msg.priority {
                Priority::High => Arc::clone(&high_priority_queue),
                Priority::Medium => Arc::clone(&medium_priority_queue),
                Priority::Low => Arc::clone(&low_priority_queue),
            };

            println!(
                "Enqueueing message {}: '{}' with {:?} priority",
                msg.id, msg.content, msg.priority
            );
            queue.enqueue(msg).await.unwrap();
        }

        // Close the queues to signal no more messages
        high_priority_queue.close().await;
        medium_priority_queue.close().await;
        low_priority_queue.close().await;

        // Process messages from queues with priority
        let high_stream = high_priority_queue.dequeue();
        let medium_stream = medium_priority_queue.dequeue();
        let low_stream = low_priority_queue.dequeue();

        // Create a prioritized stream by merging the queues
        // High priority messages are processed first, then medium, then low
        let prioritized_stream = high_stream.chain(medium_stream).chain(low_stream);

        // Process messages with bounded concurrency
        let results = prioritized_stream
            .par_eval_map_rs2(2, |msg| async move {
                let result = process_message(msg.clone()).await;
                (msg, result)
            })
            .collect::<Vec<_>>()
            .await;

        // Report results
        println!("\nProcessing Summary:");
        println!("Total messages processed: {}", results.len());

        let successes = results.iter().filter(|(_, result)| result.is_ok()).count();
        let failures = results.len() - successes;

        println!("Successful: {}", successes);
        println!("Failed: {}", failures);
    });
}
