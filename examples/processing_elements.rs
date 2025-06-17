use rs2_stream::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use std::collections::{HashSet, BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::error::Error;

// Define our User type for the example
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
}

// Simulate sending a notification
async fn send_notification(user: &str, message: &str) {
    println!("Sending to {}: {}", user, message);
    // Simulate network delay
    tokio::time::sleep(Duration::from_millis(50)).await;
}

// Simulate processing a user profile
async fn process_profile(user: &User) -> Result<String, Box<dyn Error + Send + Sync>> {
    println!("Processing profile for: {}", user.name);
    // Simulate processing delay
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(format!("{}'s profile processed successfully", user.name))
}

// Simulate updating user permissions
async fn update_permissions(user: &User) -> Result<String, Box<dyn Error + Send + Sync>> {
    println!("Updating permissions for: {}", user.name);
    // Simulate database delay
    tokio::time::sleep(Duration::from_millis(75)).await;
    Ok(format!("{}'s permissions updated", user.name))
}

// Simulate generating analytics for a user
async fn generate_analytics(user: &User) -> Result<String, Box<dyn Error + Send + Sync>> {
    println!("Generating analytics for: {}", user.name);
    // Simulate computation delay
    tokio::time::sleep(Duration::from_millis(150)).await;
    Ok(format!("Analytics for {} completed", user.name))
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users to process
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
            User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "moderator".to_string() },
            User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string() },
        ];

        println!("\n=== Sequential Processing Example ===");

        // Use for_each_rs2 to send notifications to each user (sequential)
        let notification_tracker = Arc::new(Mutex::new(Vec::new()));
        let tracker_clone = notification_tracker.clone();

        from_iter(users.clone())
            .filter_rs2(|user| user.active)
            .for_each_rs2(move |user| {
                let tracker = tracker_clone.clone();
                async move {
                    // Send notification
                    send_notification(&user.name, "Your account has been updated").await;

                    // Track that we sent the notification
                    let mut guard = tracker.lock().await;
                    guard.push(user.id);
                }
            })
            .await;

        let notified_users = notification_tracker.lock().await;
        println!("Notified {} users sequentially", notified_users.len());

        println!("\n=== Parallel Processing with Order Preservation ===");

        // Example 1: Process user profiles in parallel with bounded concurrency (preserving order)
        let start = std::time::Instant::now();
        let processed_profiles = from_iter(users.clone())
            .filter_rs2(|user| user.active)
            .par_eval_map_rs2(3, |user| async move {
                // Process the user profile in parallel (with 3 concurrent tasks max)
                match process_profile(&user).await {
                    Ok(result) => (user.id, result),
                    Err(_) => (user.id, format!("Failed to process {}'s profile", user.name)),
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("Processed {} profiles in parallel (ordered) in {:?}", 
                 processed_profiles.len(), start.elapsed());
        for (id, result) in &processed_profiles {
            println!("  User {}: {}", id, result);
        }

        println!("\n=== Parallel Processing without Order Preservation ===");

        // Example 2: Update user permissions in parallel without preserving order
        let start = std::time::Instant::now();
        let permission_updates = from_iter(users.clone())
            .filter_rs2(|user| user.active)
            .par_eval_map_unordered_rs2(2, |user| async move {
                // Update permissions in parallel (with 2 concurrent tasks max)
                // Results will be returned in the order they complete, not input order
                match update_permissions(&user).await {
                    Ok(result) => (user.id, result),
                    Err(_) => (user.id, format!("Failed to update {}'s permissions", user.name)),
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("Updated {} user permissions in parallel (unordered) in {:?}", 
                 permission_updates.len(), start.elapsed());
        for (id, result) in &permission_updates {
            println!("  User {}: {}", id, result);
        }

        println!("\n=== Parallel Stream Joining ===");

        // Example 3: Run multiple streams in parallel and combine their results
        let start = std::time::Instant::now();

        // Create three different streams for different user processing tasks
        let active_users = from_iter(users.clone())
            .filter_rs2(|user| user.active);

        let admin_users = from_iter(users.clone())
            .filter_rs2(|user| user.role == "admin");

        let regular_users = from_iter(users.clone())
            .filter_rs2(|user| user.role == "user");

        // Process all three streams in parallel with bounded concurrency
        let streams = vec![
            active_users.map_rs2(|user| format!("Active: {}", user.name)).boxed(),
            admin_users.map_rs2(|user| format!("Admin: {}", user.name)).boxed(),
            regular_users.map_rs2(|user| format!("Regular: {}", user.name)).boxed(),
        ];

        // Create a stream of streams and use par_join_rs2
        let combined_results = from_iter(streams)
            .par_join_rs2(3)
            .collect::<Vec<_>>()
            .await;

        println!("Processed {} streams in parallel in {:?}", 
                 combined_results.len(), start.elapsed());
        for result in &combined_results {
            println!("  {}", result);
        }

        println!("\n=== Collection Examples ===");

        // Use collect_rs2 to gather users into different collections
        // Collect into a HashSet (removes duplicates)
        let unique_roles = from_iter(users.clone())
            .map_rs2(|user| user.role)
            .collect_rs2::<HashSet<_>>()
            .await;

        println!("Unique roles: {:?}", unique_roles);

        // Collect into a BTreeMap (sorted by key)
        let users_by_id = from_iter(users.clone())
            .map_rs2(|user| (user.id, user))
            .collect_rs2::<BTreeMap<_, _>>()
            .await;

        println!("Users by ID (sorted):");
        for (id, user) in users_by_id {
            println!("  {}: {}", id, user.name);
        }
    });
}
