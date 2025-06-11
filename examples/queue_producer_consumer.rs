use rs2::queue::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};
use std::sync::Arc;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a shared queue
        let queue = Arc::new(Queue::bounded(10));

        // Clone for producer and consumer
        let producer_queue = Arc::clone(&queue);
        let consumer_queue = Arc::clone(&queue);

        // Spawn producer task
        let producer = tokio::spawn(async move {
            for i in 1..=20 {
                // Simulate some work
                sleep(Duration::from_millis(100)).await;

                // Enqueue item
                match producer_queue.enqueue(i).await {
                    Ok(_) => println!("Producer: Enqueued {}", i),
                    Err(e) => println!("Producer: Failed to enqueue {}: {:?}", i, e),
                }
            }

            // Close the queue when done
            producer_queue.close().await;
            println!("Producer: Done, queue closed");
        });

        // Spawn consumer task
        let consumer = tokio::spawn(async move {
            // Get dequeue stream
            let mut items = consumer_queue.dequeue();

            // Process items as they arrive
            while let Some(item) = items.next().await {
                println!("Consumer: Processing {}", item);

                // Simulate slower processing
                sleep(Duration::from_millis(200)).await;

                println!("Consumer: Finished processing {}", item);
            }

            println!("Consumer: Queue exhausted");
        });

        // Wait for both tasks to complete
        let _ = tokio::join!(producer, consumer);
    });
}