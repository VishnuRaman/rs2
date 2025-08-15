use rs2_stream::queue::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};

#[derive(Debug, Clone)]
struct Task {
    id: u32,
    name: String,
    priority: u8,
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🚀 Starting comprehensive queue usage examples\n");

        // Example 1: Basic Queue Operations
        println!("=== Example 1: Basic Queue Operations ===");
        basic_queue_operations().await;

        // Example 2: Producer-Consumer Pattern
        println!("\n=== Example 2: Producer-Consumer Pattern ===");
        producer_consumer_pattern().await;

        // Example 3: Batch Processing
        println!("\n=== Example 3: Batch Processing ===");
        batch_processing().await;

        // Example 4: Queue with Timeout
        println!("\n=== Example 4: Queue with Timeout ===");
        queue_with_timeout().await;

        // Example 5: Task Processing Pipeline
        println!("\n=== Example 5: Task Processing Pipeline ===");
        task_processing_pipeline().await;

        println!("\n✅ All queue examples completed successfully!");
    });
}

async fn basic_queue_operations() {
    // Create a bounded queue with capacity 5
    let mut queue = Queue::bounded(5);

    // Enqueue some items
    for i in 1..=3 {
        queue.enqueue(i).await.unwrap();
        println!("Enqueued: {}", i);
    }

    // Get the current queue length
    let len = queue.len().await;
    println!("Queue length: {}", len);

    // Get a stream for dequeuing items
    let dequeue_stream = queue.stream();

    // Close the queue to stop the stream after processing existing items
    queue.close().await;

    // Dequeue and process items using stream
    let results = dequeue_stream
        .take_rs2(3) // Take only the items we enqueued
        .collect_rs2()
        .await;

    println!("Dequeued items: {:?}", results);
}

async fn producer_consumer_pattern() {
    let queue = Queue::bounded(10);

    // Create a producer task
    let mut producer_queue = queue.clone();
    let producer_task = tokio::spawn(async move {
        for i in 1..=5 {
            producer_queue.enqueue(format!("message-{}", i)).await.unwrap();
            println!("Produced: message-{}", i);
            sleep(Duration::from_millis(100)).await; // Simulate work
        }
        producer_queue.close().await;
        println!("Producer finished");
    });

    // Consumer using stream
    let consumer_task = tokio::spawn(async move {
        let results = queue.stream()
            .map_rs2(|msg| {
                println!("Consumed: {}", msg);
                msg.to_uppercase()
            })
            .collect_rs2()
            .await;
        
        println!("All processed messages: {:?}", results);
    });

    // Wait for both tasks to complete
    let _ = tokio::join!(producer_task, consumer_task);
}

async fn batch_processing() {
    let mut queue = Queue::bounded(20);

    // Enqueue multiple items
    for i in 1..=10 {
        queue.enqueue(i * 10).await.unwrap();
    }
    queue.close().await;

    // Process items in batches using batch_stream
    let batch_results = queue.batch_stream(3)
        .map_rs2(|batch| {
            let sum: i32 = batch.iter().sum();
            println!("Processing batch {:?}, sum: {}", batch, sum);
            sum
        })
        .collect_rs2()
        .await;

    println!("Batch sums: {:?}", batch_results);
}

async fn queue_with_timeout() {
    let queue = Queue::bounded(5);

    // Enqueue some items with delays
    let enqueue_task = tokio::spawn({
        let mut queue = queue.clone();
        async move {
            for i in 1..=3 {
                sleep(Duration::from_millis(200)).await;
                queue.enqueue(format!("delayed-{}", i)).await.unwrap();
                println!("Enqueued with delay: delayed-{}", i);
            }
            queue.close().await;
        }
    });

    // Process items with timeout stream
    let timeout_results = queue.timeout_stream(Duration::from_millis(500))
        .filter_map_rs2(|result| {
            match result {
                Ok(item) => {
                    println!("Received within timeout: {}", item);
                    Some(item)
                }
                Err(e) => {
                    println!("Timeout or error: {:?}", e);
                    None
                }
            }
        })
        .collect_rs2()
        .await;

    println!("Items received within timeout: {:?}", timeout_results);
    enqueue_task.await.unwrap();
}

async fn task_processing_pipeline() {
    let mut task_queue = Queue::bounded(15);

    // Create sample tasks
    let tasks = vec![
        Task { id: 1, name: "Database backup".to_string(), priority: 1 },
        Task { id: 2, name: "Send emails".to_string(), priority: 3 },
        Task { id: 3, name: "Process payments".to_string(), priority: 1 },
        Task { id: 4, name: "Generate reports".to_string(), priority: 2 },
        Task { id: 5, name: "Update inventory".to_string(), priority: 2 },
    ];

    // Enqueue tasks
    for task in tasks {
        task_queue.enqueue(task).await.unwrap();
    }
    task_queue.close().await;

    // Process tasks with priority filtering and async processing
    let processed_tasks = task_queue.stream()
        .filter_rs2(|task| task.priority <= 2) // Only high and medium priority tasks
        .filter_map_async_rs2(|task| async move {
            // Simulate async task processing
            let processing_time = match task.priority {
                1 => 100, // High priority tasks process faster
                2 => 200, // Medium priority tasks
                _ => 500, // Low priority tasks (filtered out above)
            };
            
            sleep(Duration::from_millis(processing_time)).await;
            
            println!("✅ Processed task: {} (ID: {}, Priority: {})", 
                     task.name, task.id, task.priority);
            
            Some(format!("Completed: {}", task.name))
        })
        .collect_rs2()
        .await;

    println!("\nProcessed {} high/medium priority tasks:", processed_tasks.len());
    for result in processed_tasks {
        println!("  - {}", result);
    }

    // Demonstrate queue statistics
    println!("\nFinal queue stats:");
    println!("  - Queue length: {}", task_queue.len().await);
    println!("  - Queue is empty: {}", task_queue.is_empty().await);
}
