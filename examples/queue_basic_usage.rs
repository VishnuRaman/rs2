use rs2_stream::queue::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a bounded queue with capacity 5
        let queue = Queue::bounded(5);

        // Enqueue some items
        for i in 1..=3 {
            queue.enqueue(i).await.unwrap();
            println!("Enqueued: {}", i);
        }

        // Get the current queue length
        let len = queue.len().await;
        println!("Queue length: {}", len); // 3

        // Get a stream for dequeuing
        let mut dequeue_stream = queue.dequeue();

        // Dequeue and process items
        while let Some(item) = dequeue_stream.next().await {
            println!("Dequeued: {}", item);
        }
    });
}