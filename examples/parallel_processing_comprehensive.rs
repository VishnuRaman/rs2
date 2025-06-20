use rs2_stream::rs2::*;
use serde::{Deserialize, Serialize};
use futures_util::StreamExt;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserProfile {
    id: u64,
    name: String,
    email: String,
    age: u32,
    preferences: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessedUser {
    id: u64,
    name: String,
    email: String,
    age: u32,
    preferences: Vec<String>,
    processed_at: u64,
    processing_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ApiResponse {
    user_id: u64,
    data: String,
    response_time_ms: u64,
    status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DatabaseRecord {
    id: u64,
    content: String,
    created_at: u64,
    updated_at: u64,
}

// Simulate CPU-intensive work
async fn cpu_intensive_processing(user: UserProfile) -> ProcessedUser {
    let start = Instant::now();
    
    // Simulate CPU-intensive work
    let mut _result = 0.0;
    for i in 0..100_000 {
        _result += (i as f64).sqrt();
    }
    
    // Simulate some async work
    sleep(Duration::from_millis(10)).await;
    
    let processing_time = start.elapsed().as_millis() as u64;
    
    ProcessedUser {
        id: user.id,
        name: user.name,
        email: user.email,
        age: user.age,
        preferences: user.preferences,
        processed_at: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        processing_time_ms: processing_time,
    }
}

// Simulate I/O-bound work (API calls)
async fn api_call(user_id: u64) -> ApiResponse {
    let start = Instant::now();
    
    // Simulate network delay
    sleep(Duration::from_millis(50 + (user_id % 100))).await;
    
    let response_time = start.elapsed().as_millis() as u64;
    
    ApiResponse {
        user_id,
        data: format!("API data for user {}", user_id),
        response_time_ms: response_time,
        status: "success".to_string(),
    }
}

// Simulate database operations
async fn database_operation(user_id: u64) -> DatabaseRecord {
    let start = Instant::now();
    
    // Simulate database delay
    sleep(Duration::from_millis(20 + (user_id % 50))).await;
    
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    
    DatabaseRecord {
        id: user_id,
        content: format!("Database content for user {}", user_id),
        created_at: now,
        updated_at: now,
    }
}

// Simulate file processing
async fn file_processing(user_id: u64) -> String {
    let start = Instant::now();
    
    // Simulate file I/O
    sleep(Duration::from_millis(30 + (user_id % 80))).await;
    
    format!("Processed file for user {} in {}ms", user_id, start.elapsed().as_millis())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 RS2 Comprehensive Parallel Processing Example");
    println!("================================================\n");

    // Generate test data
    let users: Vec<UserProfile> = (1..=1000)
        .map(|id| UserProfile {
            id,
            name: format!("User {}", id),
            email: format!("user{}@example.com", id),
            age: 20 + (id as u32 % 50),
            preferences: vec![
                "music".to_string(),
                "sports".to_string(),
                "technology".to_string(),
            ],
        })
        .collect();

    println!("📊 Performance Comparison: Sequential vs Parallel Processing");
    println!("-----------------------------------------------------------");

    // 1. Sequential Processing (Baseline)
    println!("\n1️⃣ Sequential Processing (Baseline)");
    let start = Instant::now();
    let sequential_results: Vec<ProcessedUser> = from_iter(users.clone())
        .eval_map_rs2(|user| Box::pin(cpu_intensive_processing(user)))
        .collect()
        .await;
    let sequential_time = start.elapsed();
    println!("   ✅ Sequential: {} users in {:?}", sequential_results.len(), sequential_time);

    // 2. Parallel Processing with Order Preservation
    println!("\n2️⃣ Parallel Processing (Ordered)");
    let start = Instant::now();
    let parallel_ordered_results: Vec<ProcessedUser> = from_iter(users.clone())
        .par_eval_map_rs2(10, |user| Box::pin(cpu_intensive_processing(user)))
        .collect()
        .await;
    let parallel_ordered_time = start.elapsed();
    println!("   ✅ Parallel (Ordered): {} users in {:?}", parallel_ordered_results.len(), parallel_ordered_time);
    println!("   📈 Speedup: {:.2}x", sequential_time.as_secs_f64() / parallel_ordered_time.as_secs_f64());

    // 3. Parallel Processing without Order (Faster)
    println!("\n3️⃣ Parallel Processing (Unordered)");
    let start = Instant::now();
    let parallel_unordered_results: Vec<ProcessedUser> = from_iter(users.clone())
        .par_eval_map_unordered_rs2(10, |user| Box::pin(cpu_intensive_processing(user)))
        .collect()
        .await;
    let parallel_unordered_time = start.elapsed();
    println!("   ✅ Parallel (Unordered): {} users in {:?}", parallel_unordered_results.len(), parallel_unordered_time);
    println!("   📈 Speedup: {:.2}x", sequential_time.as_secs_f64() / parallel_unordered_time.as_secs_f64());

    // 4. Mixed Workload Processing
    println!("\n4️⃣ Mixed Workload Processing (CPU + I/O)");
    let start = Instant::now();
    let mixed_results: Vec<(ProcessedUser, ApiResponse, DatabaseRecord)> = from_iter(users.clone())
        .par_eval_map_rs2(8, |user| {
            Box::pin(async move {
                let processed_user = cpu_intensive_processing(user.clone()).await;
                let api_response = api_call(user.id).await;
                let db_record = database_operation(user.id).await;
                (processed_user, api_response, db_record)
            })
        })
        .collect()
        .await;
    let mixed_time = start.elapsed();
    println!("   ✅ Mixed Workload: {} users in {:?}", mixed_results.len(), mixed_time);

    // 5. Pipeline Processing
    println!("\n5️⃣ Pipeline Processing");
    let start = Instant::now();
    let pipeline_results: Vec<String> = from_iter(users.clone())
        .par_eval_map_rs2(6, |user| Box::pin(cpu_intensive_processing(user)))
        .par_eval_map_rs2(4, |processed_user| Box::pin(api_call(processed_user.id)))
        .par_eval_map_rs2(8, |api_response| Box::pin(file_processing(api_response.user_id)))
        .collect()
        .await;
    let pipeline_time = start.elapsed();
    println!("   ✅ Pipeline: {} users in {:?}", pipeline_results.len(), pipeline_time);

    // 6. Adaptive Concurrency
    println!("\n6️⃣ Adaptive Concurrency (Different Concurrency Levels)");
    let concurrency_levels = vec![1, 2, 4, 8, 16, 32];
    
    for concurrency in concurrency_levels {
        let start = Instant::now();
        let results: Vec<ProcessedUser> = from_iter(users.clone())
            .par_eval_map_rs2(concurrency, |user| Box::pin(cpu_intensive_processing(user)))
            .collect()
            .await;
        let time = start.elapsed();
        println!("   🔧 Concurrency {}: {} users in {:?} ({:.2} users/sec)", 
                concurrency, results.len(), time, 
                results.len() as f64 / time.as_secs_f64());
    }

    // 7. Error Handling in Parallel Processing
    println!("\n7️⃣ Error Handling in Parallel Processing");
    let users_with_errors: Vec<UserProfile> = (1..=100)
        .map(|id| UserProfile {
            id,
            name: format!("User {}", id),
            email: format!("user{}@example.com", id),
            age: 20 + (id as u32 % 50),
            preferences: vec!["music".to_string()],
        })
        .collect();

    let error_results: Vec<Result<ProcessedUser, String>> = from_iter(users_with_errors)
        .par_eval_map_rs2(5, |user| {
            Box::pin(async move {
                if user.id % 10 == 0 {
                    // Simulate error for every 10th user
                    Err(format!("Processing failed for user {}", user.id))
                } else {
                    Ok(cpu_intensive_processing(user).await)
                }
            })
        })
        .collect()
        .await;

    let success_count = error_results.iter().filter(|r| r.is_ok()).count();
    let error_count = error_results.iter().filter(|r| r.is_err()).count();
    println!("   ✅ Success: {}, Errors: {}", success_count, error_count);

    // 8. Resource Management
    println!("\n8️⃣ Resource Management and Backpressure");
    let start = Instant::now();
    // Note: buffer_unordered is only for streams of futures, not values. Here we just collect directly.
    let resource_results: Vec<ProcessedUser> = from_iter(users.clone())
        .par_eval_map_rs2(4, |user| Box::pin(cpu_intensive_processing(user)))
        .collect()
        .await;
    let resource_time = start.elapsed();
    println!("   ✅ Resource Managed: {} users in {:?}", resource_results.len(), resource_time);

    // 9. Real-World Scenario: E-commerce Processing
    println!("\n9️⃣ Real-World Scenario: E-commerce Order Processing");
    let orders: Vec<u64> = (1..=500).collect();
    
    let start = Instant::now();
    let order_results: Vec<(u64, ApiResponse, DatabaseRecord, String)> = from_iter(orders)
        .par_eval_map_rs2(12, |order_id| {
            Box::pin(async move {
                // Simulate order processing pipeline
                let api_response = api_call(order_id).await;
                let db_record = database_operation(order_id).await;
                let file_result = file_processing(order_id).await;
                (order_id, api_response, db_record, file_result)
            })
        })
        .collect()
        .await;
    let order_time = start.elapsed();
    println!("   ✅ E-commerce Processing: {} orders in {:?}", order_results.len(), order_time);

    // 10. Performance Summary
    println!("\n📊 Performance Summary");
    println!("=====================");
    println!("Sequential Processing:     {:?}", sequential_time);
    println!("Parallel (Ordered):       {:?} ({:.2}x speedup)", 
            parallel_ordered_time, 
            sequential_time.as_secs_f64() / parallel_ordered_time.as_secs_f64());
    println!("Parallel (Unordered):     {:?} ({:.2}x speedup)", 
            parallel_unordered_time, 
            sequential_time.as_secs_f64() / parallel_unordered_time.as_secs_f64());
    println!("Mixed Workload:           {:?}", mixed_time);
    println!("Pipeline Processing:      {:?}", pipeline_time);
    println!("Resource Managed:         {:?}", resource_time);
    println!("E-commerce Processing:    {:?}", order_time);

    println!("\n🎯 Key Takeaways:");
    println!("• Parallel processing provides significant speedup for CPU-intensive tasks");
    println!("• Unordered processing is faster than ordered when order doesn't matter");
    println!("• Optimal concurrency depends on workload type (CPU vs I/O bound)");
    println!("• Error handling works seamlessly in parallel operations");
    println!("• Resource management prevents memory issues at scale");
    println!("• Pipeline processing enables complex workflows");

    println!("\n✅ Comprehensive parallel processing example completed!");
    Ok(())
} 