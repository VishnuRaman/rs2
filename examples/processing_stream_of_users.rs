use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use std::error::Error;

// Define our User type
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
}

// Simulate a database query that returns users
async fn fetch_users() -> Vec<User> {
    // In a real application, this would query a database
    vec![
        User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
        User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
        User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "moderator".to_string() },
        User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string() },
    ]
}

// Simulate sending an email to a user
async fn send_email(user: &User, message: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Sending email to {}: {}", user.email, message);
    // Simulate network delay
    tokio::time::sleep(Duration::from_millis(100)).await;
    Ok(())
}

// Simulate updating a user in the database
async fn update_user(user: User) -> Result<User, Box<dyn Error + Send + Sync>> {
    println!("Updating user: {}", user.name);
    // Simulate database delay
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(user)
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Function to create a stream of active users
        async fn get_active_users() -> RS2Stream<User> {
            // Create a stream of users
            let users = eval(fetch_users()).flat_map(|users| from_iter(users));

            // Apply backpressure to avoid overwhelming downstream systems
            let users_with_backpressure = users.auto_backpressure_rs2();

            // Process active users only
            users_with_backpressure
                .filter_rs2(|user| user.active)
                .prefetch_rs2(2)  // Prefetch to improve performance
        }

        // Group users by role
        let users_by_role = get_active_users().await
            .group_by_rs2(|user| user.role.clone())
            .collect::<Vec<_>>()
            .await;

        // Print users by role
        for (role, users) in users_by_role {
            println!("Role: {}, Count: {}", role, users.len());
            for user in users {
                println!("  - {} ({})", user.name, user.email);
            }
        }

        // Process users in parallel with bounded concurrency
        let processed_users = get_active_users().await
            .par_eval_map_rs2(3, |mut user| async move {
                // Simulate some processing
                user.name = format!("{} (Processed)", user.name);

                // Update the user in the database
                match update_user(user.clone()).await {
                    Ok(updated_user) => updated_user,
                    Err(_) => user,
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("\nProcessed {} users", processed_users.len());

        // Send emails to users with timeout
        let email_results = get_active_users().await
            .eval_map_rs2(|user| async move {
                // Add timeout to email sending
                match tokio::time::timeout(
                    Duration::from_millis(200),
                    send_email(&user, "Welcome to our platform!")
                ).await {
                    Ok(Ok(_)) => (user.id, true),
                    _ => (user.id, false),
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("\nEmail results:");
        for (user_id, success) in email_results {
            println!("User {}: {}", user_id, if success { "Email sent" } else { "Failed to send email" });
        }
    });
}
