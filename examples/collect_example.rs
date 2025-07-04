use rs2_stream::rs2_new::{from_iter_stream, collect_stream, CollectExt};
use rs2_stream::stream::StreamExt;

#[tokio::main]
async fn main() {
    println!("=== Collect Example with Internal Boxing ===\n");

    // Method 1: Using the collect_stream function directly
    println!("1. Using collect_stream function:");
    let stream = from_iter_stream(vec![1, 2, 3, 4, 5]);
    let result: Vec<i32> = collect_stream(stream).await;
    println!("   Collected: {:?}", result);

    // Method 2: Using the extension trait method
    println!("\n2. Using collect_into extension trait:");
    let stream = from_iter_stream(vec![6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream.collect_into().await;
    println!("   Collected: {:?}", result);

    // Method 3: Collecting into different collection types
    println!("\n3. Collecting into different collection types:");
    
    // HashSet (removes duplicates)
    use std::collections::HashSet;
    let stream = from_iter_stream(vec![1, 2, 2, 3, 3, 3, 4, 5, 5]);
    let result: HashSet<i32> = stream.collect_into().await;
    println!("   HashSet: {:?}", result);

    // BTreeSet (sorted)
    use std::collections::BTreeSet;
    let stream = from_iter_stream(vec![5, 3, 1, 4, 2, 3, 5]);
    let result: BTreeSet<i32> = stream.collect_into().await;
    println!("   BTreeSet: {:?}", result);

    // BTreeMap (key-value pairs)
    use std::collections::BTreeMap;
    let stream = from_iter_stream(vec![("b", 2), ("a", 1), ("c", 3)]);
    let result: BTreeMap<&str, i32> = stream.collect_into().await;
    println!("   BTreeMap: {:?}", result);

    // Method 4: Chaining operations before collecting
    println!("\n4. Chaining operations before collecting:");
    let stream = from_iter_stream(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let result: Vec<i32> = stream
        .filter(|&x| x % 2 == 0)  // Keep only even numbers
        .map(|x| x * 2)           // Double each number
        .take(3)                  // Take first 3
        .collect_into()           // Collect into Vec
        .await;
    println!("   Filtered, mapped, and limited: {:?}", result);

    // Method 5: Empty stream
    println!("\n5. Empty stream:");
    let stream = rs2_stream::rs2_new::empty_stream::<i32>();
    let result: Vec<i32> = collect_stream(stream).await;
    println!("   Empty stream result: {:?}", result);

    println!("\n=== Key Benefits ===");
    println!("✅ No manual boxing required");
    println!("✅ Works with any collection type that implements Default + Extend");
    println!("✅ Handles async streams properly");
    println!("✅ Zero-cost abstraction when possible");
    println!("✅ Clean, ergonomic API");
} 