use rs2_stream::queue::Queue;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use tokio::runtime::Runtime;
use tokio::time::{sleep, Duration};

// Define our Message type
#[derive(Debug, Clone)]
struct Message {
    id: u64,
    content: String,
    priority: Priority,
    timestamp: u64,
    processing_time: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Priority {
    Low = 1,
    Medium = 2,
    High = 3,
}

impl Priority {
    fn level(&self) -> u8 {
        match self {
            Priority::Low => 1,
            Priority::Medium => 2,
            Priority::High => 3,
        }
    }
}

// Processing result with detailed information
#[derive(Debug, Clone)]
struct ProcessingResult {
    message_id: u64,
    success: bool,
    processing_duration: Duration,
    error_message: Option<String>,
}

// Simulate message processing
async fn process_message(msg: Message) -> ProcessingResult {
    let start_time = std::time::Instant::now();
    println!("🔄 Processing message {}: '{}'", msg.id, msg.content);

    // Simulate processing time based on priority
    let delay = match msg.priority {
        Priority::High => 50,   // High priority processes faster
        Priority::Medium => 100,
        Priority::Low => 200,   // Low priority takes longer
    };

    sleep(Duration::from_millis(delay)).await;
    
    // Simulate occasional failures for demonstration
    let success = msg.id % 7 != 0; // Every 7th message fails
    
    let processing_duration = start_time.elapsed();
    
    if success {
        println!("✅ Completed message {} in {:?}", msg.id, processing_duration);
        ProcessingResult {
            message_id: msg.id,
            success: true,
            processing_duration,
            error_message: None,
        }
    } else {
        println!("❌ Failed to process message {}", msg.id);
        ProcessingResult {
            message_id: msg.id,
            success: false,
            processing_duration,
            error_message: Some("Simulated processing error".to_string()),
        }
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🚀 Starting comprehensive queue message processing example\n");

        // Example 1: Basic Priority Queue Processing
        println!("=== Example 1: Basic Priority Queue Processing ===");
        basic_priority_processing().await;

        // Example 2: Multi-Queue Processing with Merging
        println!("\n=== Example 2: Multi-Queue Processing with Merging ===");
        multi_queue_processing().await;

        // Example 3: Batch Message Processing
        println!("\n=== Example 3: Batch Message Processing ===");
        batch_message_processing().await;

        // Example 4: Real-time Message Processing Pipeline
        println!("\n=== Example 4: Real-time Message Processing Pipeline ===");
        realtime_processing_pipeline().await;

        println!("\n✅ All queue message processing examples completed!");
    });
}

async fn basic_priority_processing() {
    // Create queues for different priority levels
    let mut high_priority_queue = Queue::bounded(5);
    let mut medium_priority_queue = Queue::bounded(10);
    let mut low_priority_queue = Queue::bounded(20);

    // Create test messages
    let messages = vec![
        Message {
            id: 1,
            content: "Critical system alert".to_string(),
            priority: Priority::High,
            timestamp: 1000,
            processing_time: None,
        },
        Message {
            id: 2,
            content: "User login".to_string(),
            priority: Priority::Medium,
            timestamp: 1001,
            processing_time: None,
        },
        Message {
            id: 3,
            content: "Log rotation".to_string(),
            priority: Priority::Low,
            timestamp: 1002,
            processing_time: None,
        },
        Message {
            id: 4,
            content: "Security breach detected".to_string(),
            priority: Priority::High,
            timestamp: 1003,
            processing_time: None,
        },
    ];

    // Distribute messages to appropriate queues
    for msg in messages {
        match msg.priority {
            Priority::High => {
                println!("📨 Enqueueing HIGH priority: {}", msg.content);
                high_priority_queue.enqueue(msg).await.unwrap();
            }
            Priority::Medium => {
                println!("📨 Enqueueing MEDIUM priority: {}", msg.content);
                medium_priority_queue.enqueue(msg).await.unwrap();
            }
            Priority::Low => {
                println!("📨 Enqueueing LOW priority: {}", msg.content);
                low_priority_queue.enqueue(msg).await.unwrap();
            }
        }
    }

    // Close queues
    high_priority_queue.close().await;
    medium_priority_queue.close().await;
    low_priority_queue.close().await;

    // Process high priority messages first
    println!("\n🔥 Processing HIGH priority messages:");
    let high_results = high_priority_queue.stream()
        .filter_map_async_rs2(|msg| async move {
            let result = process_message(msg).await;
            Some(result)
        })
        .collect_rs2()
        .await;

    // Then medium priority
    println!("\n⚡ Processing MEDIUM priority messages:");
    let medium_results = medium_priority_queue.stream()
        .filter_map_async_rs2(|msg| async move {
            let result = process_message(msg).await;
            Some(result)
        })
        .collect_rs2()
        .await;

    // Finally low priority
    println!("\n📝 Processing LOW priority messages:");
    let low_results = low_priority_queue.stream()
        .filter_map_async_rs2(|msg| async move {
            let result = process_message(msg).await;
            Some(result)
        })
        .collect_rs2()
        .await;

    // Report results
    let all_results = [high_results, medium_results, low_results].concat();
    print_processing_summary(&all_results);
}

async fn multi_queue_processing() {
    let mut message_queue = Queue::bounded(50);

    // Create a mixed set of messages
    let messages = (1..=12).map(|i| {
        let priority = match i % 3 {
            0 => Priority::High,
            1 => Priority::Medium,
            _ => Priority::Low,
        };

        Message {
            id: i,
            content: format!("Message {}", i),
            priority,
            timestamp: 1000 + i,
            processing_time: None,
        }
    }).collect::<Vec<_>>();

    // Enqueue all messages
    for msg in messages {
        message_queue.enqueue(msg).await.unwrap();
    }
    message_queue.close().await;

    // Process messages with priority-based filtering and processing
    let results = message_queue.stream()
        .map_rs2(|msg| {
            // Add priority level for sorting
            (msg.priority.level(), msg)
        })
        .collect_rs2()
        .await;

    // Sort by priority (highest first)
    let mut sorted_messages = results;
    sorted_messages.sort_by(|a, b| b.0.cmp(&a.0));

    // Process sorted messages
    let processing_results = rs2_stream::stream::constructors::from_iter(sorted_messages)
        .map_rs2(|(_, msg)| msg)
        .filter_map_async_rs2(|msg| async move {
            let result = process_message(msg).await;
            Some(result)
        })
        .collect_rs2()
        .await;

    print_processing_summary(&processing_results);
}

async fn batch_message_processing() {
    let mut queue = Queue::bounded(100);

    // Generate a large number of messages
    let messages = (1..=20).map(|i| {
        let priority = match i % 4 {
            0 => Priority::High,
            1 | 2 => Priority::Medium,
            _ => Priority::Low,
        };

        Message {
            id: i,
            content: format!("Batch message {}", i),
            priority: priority.clone(),
            timestamp: 1000 + i,
            processing_time: None,
        }
    }).collect::<Vec<_>>();

    // Enqueue messages
    for msg in messages {
        queue.enqueue(msg).await.unwrap();
    }
    queue.close().await;

    // Process messages in batches
    let batch_results = queue.batch_stream(5)
        .filter_map_async_rs2(|batch| async move {
            println!("📦 Processing batch of {} messages", batch.len());
            
            let mut batch_results = Vec::new();
            for msg in batch {
                let result = process_message(msg).await;
                batch_results.push(result);
            }
            
            Some(batch_results)
        })
        .collect_rs2()
        .await;

    // Flatten batch results
    let all_results: Vec<ProcessingResult> = batch_results.into_iter().flatten().collect();
    print_processing_summary(&all_results);
}

async fn realtime_processing_pipeline() {
    let queue = Queue::bounded(10);

    // Simulate real-time message arrival
    let producer_task = tokio::spawn({
        let mut queue = queue.clone();
        async move {
            for i in 1..=8 {
                let priority = if i <= 2 { Priority::High } 
                              else if i <= 5 { Priority::Medium } 
                              else { Priority::Low };

                let msg = Message {
                    id: i,
                    content: format!("Real-time message {}", i),
                    priority,
                    timestamp: 1000 + i,
                    processing_time: None,
                };

                println!("📡 Producing: {}", msg.content);
                queue.enqueue(msg).await.unwrap();
                
                // Simulate variable arrival times
                sleep(Duration::from_millis(100)).await;
            }
            queue.close().await;
            println!("📡 Producer finished");
        }
    });

    // Process messages as they arrive with timeout handling
    let consumer_task = tokio::spawn(async move {
        let results = queue.timeout_stream(Duration::from_millis(1000))
            .filter_map_rs2(|result| {
                match result {
                    Ok(msg) => {
                        println!("📥 Received: {}", msg.content);
                        Some(msg)
                    }
                    Err(e) => {
                        println!("⏰ Timeout or error: {:?}", e);
                        None
                    }
                }
            })
            .filter_map_async_rs2(|msg| async move {
                let result = process_message(msg).await;
                Some(result)
            })
            .collect_rs2()
            .await;

        print_processing_summary(&results);
    });

    // Wait for both tasks
    let _ = tokio::join!(producer_task, consumer_task);
}

fn print_processing_summary(results: &[ProcessingResult]) {
    println!("\n📊 Processing Summary:");
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    println!("📈 Total messages processed: {}", results.len());

    let successes = results.iter().filter(|r| r.success).count();
    let failures = results.len() - successes;

    println!("✅ Successful: {}", successes);
    println!("❌ Failed: {}", failures);

    if !results.is_empty() {
        let avg_duration = results.iter()
            .map(|r| r.processing_duration.as_millis() as f64)
            .sum::<f64>() / results.len() as f64;
        
        println!("⏱️  Average processing time: {:.2}ms", avg_duration);

        // Show failed messages
        let failed_messages: Vec<_> = results.iter()
            .filter(|r| !r.success)
            .collect();

        if !failed_messages.is_empty() {
            println!("\n💥 Failed messages:");
            for result in failed_messages {
                println!("  - Message {}: {}", 
                    result.message_id, 
                    result.error_message.as_ref().unwrap_or(&"Unknown error".to_string())
                );
            }
        }
    }
    println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
}
