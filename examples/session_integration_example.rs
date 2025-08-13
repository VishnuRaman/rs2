use rs2_stream::session::{SessionBuilder, SessionPreset, set_global_session};
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::from_iter_rs2;
use rs2_stream::stream::StreamExt;
use std::time::Duration;

#[tokio::main]
async fn main() {
    println!("=== RS2 Session Integration Example ===\n");

    // Example 1: No session configuration - uses defaults
    println!("1. No Session Configuration (Uses Defaults):");
    let numbers: Vec<usize> = (1..=10).collect();
    let stream = from_iter_rs2(numbers.clone())
        .par_eval_map_rs2(Some(2), |x| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            x * 2
        });
    
    let start = std::time::Instant::now();
    let results: Vec<usize> = stream.collect().await;
    let duration = start.elapsed();
    println!("   - Explicit concurrency: 2");
    println!("   - Processed {} items in {:?}", results.len(), duration);
    println!("   - Results: {:?}", results);

    // Example 2: Set session configuration and use automatic fallback
    println!("\n2. With Session Configuration (Automatic Fallback):");
    
    // Set a development session
    let dev_session = SessionBuilder::new()
        .preset(SessionPreset::Development)
        .build();
    set_global_session(dev_session);
    
    println!("   - Session preset: Development");
    println!("   - Session concurrency: 2");
    
    let stream2 = from_iter_rs2(numbers.clone())
        .par_eval_map_rs2(None, |x| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            x * 3
        });
    
    let start = std::time::Instant::now();
    let results2: Vec<usize> = stream2.collect().await;
    let duration = start.elapsed();
    println!("   - Used session concurrency: 2");
    println!("   - Processed {} items in {:?}", results.len(), duration);
    println!("   - Results: {:?}", results2);

    // Example 3: Override session configuration with explicit parameter
    println!("\n3. Override Session Configuration (Explicit Parameter):");
    
    let stream3 = from_iter_rs2(numbers.clone())
        .par_eval_map_rs2(Some(4), |x| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            x * 4
        });
    
    let start = std::time::Instant::now();
    let results3: Vec<usize> = stream3.collect().await;
    let duration = start.elapsed();
    println!("   - Overrode session concurrency: 4");
    println!("   - Processed {} items in {:?}", results.len(), duration);
    println!("   - Results: {:?}", results3);

    // Example 4: Change session configuration and see automatic updates
    println!("\n4. Change Session Configuration (Automatic Updates):");
    
    // Switch to production session
    let prod_session = SessionBuilder::new()
        .preset(SessionPreset::Production)
        .build();
    set_global_session(prod_session);
    
    println!("   - Session preset: Production");
    println!("   - Session concurrency: 16");
    
    let stream4 = from_iter_rs2(numbers.clone())
        .par_eval_map_rs2(None, |x| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            x * 5
        });
    
    let start = std::time::Instant::now();
    let results4: Vec<usize> = stream4.collect().await;
    let duration = start.elapsed();
    println!("   - Used session concurrency: 16");
    println!("   - Processed {} items in {:?}", results.len(), duration);
    println!("   - Results: {:?}", results4);

    // Example 5: Use convenience methods
    println!("\n5. Using Convenience Methods:");
    
    // Method that explicitly uses session config
    let stream5 = from_iter_rs2(numbers.clone())
        .par_eval_map_with_session_rs2(|x| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            x * 6
        });
    
    let start = std::time::Instant::now();
    let results5: Vec<usize> = stream5.collect().await;
    let duration = start.elapsed();
    println!("   - Used par_eval_map_with_session_rs2");
    println!("   - Automatically used session concurrency: 16");
    println!("   - Processed {} items in {:?}", results.len(), duration);
    println!("   - Results: {:?}", results5);

    // Method that explicitly overrides session config
    let stream6 = from_iter_rs2(numbers.clone())
        .par_eval_map_with_concurrency_rs2(8, |x| async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            x * 7
        });
    
    let start = std::time::Instant::now();
    let results6: Vec<usize> = stream6.collect().await;
    let duration = start.elapsed();
    println!("   - Used par_eval_map_with_concurrency_rs2(8)");
    println!("   - Overrode session concurrency: 8");
    println!("   - Processed {} items in {:?}", results.len(), duration);
    println!("   - Results: {:?}", results6);

    println!("\n=== Session Integration Example Complete ===");
    println!("\nKey Benefits:");
    println!("✅ Set configuration once, use everywhere");
    println!("✅ Automatic fallback to session config when no explicit params");
    println!("✅ Easy to override when needed");
    println!("✅ Environment-specific presets (dev/prod)");
    println!("✅ No need to pass configs to every method call");
} 