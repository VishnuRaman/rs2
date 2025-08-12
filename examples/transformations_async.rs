use rs2_stream::rs2::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use std::time::Duration;
use tokio::runtime::Runtime;

// Define our User type for the example
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
}

// Simulate fetching user details from a database
async fn fetch_user_details(id: u64) -> User {
    // Simulate database delay
    tokio::time::sleep(Duration::from_millis(50)).await;

    User {
        id,
        name: format!("User {}", id),
        email: format!("user{}@example.com", id),
        active: true,
        role: "user".to_string(),
    }
}

// Additional async transformation functions
async fn validate_user(user: User) -> Result<User, String> {
    // Simulate validation delay
    tokio::time::sleep(Duration::from_millis(20)).await;
    
    if user.name.is_empty() {
        Err(format!("Invalid user with ID {}: empty name", user.id))
    } else {
        Ok(user)
    }
}

async fn enrich_user_with_permissions(user: User) -> User {
    // Simulate permission lookup delay
    tokio::time::sleep(Duration::from_millis(30)).await;
    
    User {
        role: if user.id == 1 { "admin".to_string() } else { user.role },
        ..user
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("=== RS2 Async Transformations Example ===\n");

        println!("1. Basic Async Transformation with eval_map_rs2");
        
        // Create a stream of user IDs
        let user_ids = from_iter_rs2(vec![1, 2, 3, 4, 5]);

        // Use eval_map_rs2 to asynchronously fetch user details for each ID
        let users_stream = user_ids.eval_map_rs2(|id| Box::pin(async move { 
            println!("  🔍 Fetching user {}", id);
            fetch_user_details(id).await 
        }));

        let users: Vec<User> = users_stream.collect_rs2().await;

        println!("   Fetched {} users:", users.len());
        for user in &users {
            println!("     User {}: {} - {} ({})", user.id, user.name, user.email, user.role);
        }

        println!("\n2. Chained Async Transformations");

        // Create another stream and chain multiple async transformations
        let user_ids = from_iter_rs2(vec![1, 2, 3]);

        let processed_users: Vec<Result<User, String>> = user_ids
            .eval_map_rs2(|id| Box::pin(async move {
                println!("  🔍 Fetching user {}", id);
                fetch_user_details(id).await
            }))
            .eval_map_rs2(|user| Box::pin(async move {
                println!("  ✅ Validating user {}", user.id);
                validate_user(user).await
            }))
            .eval_map_rs2(|result| Box::pin(async move {
                match result {
                    Ok(user) => {
                        println!("  🔐 Enriching user {} with permissions", user.id);
                        Ok(enrich_user_with_permissions(user).await)
                    }
                    Err(e) => Err(e)
                }
            }))
            .collect_rs2()
            .await;

        println!("   Processed users:");
        for result in processed_users {
            match result {
                Ok(user) => println!("     ✅ {} - {} ({})", user.name, user.email, user.role),
                Err(e) => println!("     ❌ Error: {}", e),
            }
        }

        println!("\n3. Parallel Processing with par_eval_map");

        let user_ids = from_iter_rs2(vec![1, 2, 3, 4, 5, 6]);
        
        let start_time = std::time::Instant::now();
        
        // Use par_eval_map_rs2 for parallel processing
        let parallel_users: Vec<User> = user_ids.par_eval_map_rs2(3, |id| Box::pin(async move {
            println!("  🚀 Parallel fetching user {}", id);
            fetch_user_details(id).await
        }))
        .collect_rs2()
        .await;

        let elapsed = start_time.elapsed();
        println!("   Parallel processing completed in {:?}", elapsed);
        println!("   Fetched {} users in parallel:", parallel_users.len());
        for user in parallel_users {
            println!("     User {}: {} - {}", user.id, user.name, user.role);
        }

        println!("\n=== Example Complete ===");
        println!("\n🎯 Key Features Demonstrated:");
        println!("1. Sequential async transformations with eval_map_rs2");
        println!("2. Chained async transformations with error handling");
        println!("3. Parallel async processing with par_eval_map");
        println!("4. Result handling in async stream pipelines");
    });
}
