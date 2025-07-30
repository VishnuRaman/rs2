use rs2_stream::stream::constructors::from_iter;
use rs2_stream::stream::Stream;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use rs2_stream::rs2::eval;
use std::error::Error;
use std::time::Duration;
use tokio::runtime::Runtime;

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
        User {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            active: true,
            role: "admin".to_string(),
        },
        User {
            id: 2,
            name: "Bob".to_string(),
            email: "bob@example.com".to_string(),
            active: true,
            role: "user".to_string(),
        },
        User {
            id: 3,
            name: "Charlie".to_string(),
            email: "charlie@example.com".to_string(),
            active: false,
            role: "user".to_string(),
        },
        User {
            id: 4,
            name: "Diana".to_string(),
            email: "diana@example.com".to_string(),
            active: true,
            role: "moderator".to_string(),
        },
        User {
            id: 5,
            name: "Eve".to_string(),
            email: "eve@example.com".to_string(),
            active: false,
            role: "admin".to_string(),
        },
    ]
}

// Simulate sending an email
async fn send_email(user: &User, subject: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    println!("Sending email to {}: {}", user.email, subject);
    // Simulate variable email sending time
    let delay = match user.role.as_str() {
        "admin" => 50,   // Admins get priority
        "moderator" => 100,
        _ => 150,
    };
    tokio::time::sleep(Duration::from_millis(delay)).await;
    Ok(())
}

// Simulate updating a user in database
async fn update_user(user: User) -> Result<User, Box<dyn Error + Send + Sync>> {
    println!("Updating user: {}", user.name);
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(user)
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("🚀 Starting comprehensive user stream processing example\n");
        
        // Get users from our async function
        let users = fetch_users().await;
        
        // Example 1: Group users by role
        println!("=== Example 1: Grouping Users by Role ===");
        let users_by_role = from_iter(users.clone())
            .filter_rs2(|user| user.active)
            .fold_rs2(std::collections::HashMap::new(), |mut acc, user| {
                acc.entry(user.role.clone()).or_insert_with(Vec::new).push(user);
                acc
            })
            .await;

        // Print users by role
        for (role, users) in users_by_role {
            println!("Role: {}, Count: {}", role, users.len());
            for user in users {
                println!("  - {} ({})", user.name, user.email);
            }
        }

        // Example 2: Process users in parallel with bounded concurrency
        println!("\n=== Example 2: Parallel User Processing ===");
        let processed_users = from_iter(users.clone())
            .filter_rs2(|user| user.active)
            .map_rs2(|mut user| {
                // Simulate some processing
                user.name = format!("{} (Processed)", user.name);
                user
            })
            .filter_map_async_rs2(|user| async move {
                // Update the user in the database
                match update_user(user.clone()).await {
                    Ok(updated_user) => Some(updated_user),
                    Err(_) => {
                        println!("Failed to update user: {}", user.name);
                        None
                    }
                }
            })
            .collect_rs2()
            .await;

        println!("Successfully processed {} users", processed_users.len());

        // Example 3: Send emails with timeout handling
        println!("\n=== Example 3: Email Sending with Timeouts ===");
        let email_results = from_iter(users.clone())
            .filter_rs2(|user| user.active)
            .filter_map_async_rs2(|user| async move {
                // Add timeout to email sending
                match tokio::time::timeout(
                    Duration::from_millis(200),
                    send_email(&user, "Welcome to our platform!"),
                )
                .await
                {
                    Ok(Ok(_)) => Some((user.id, user.name, true)),
                    Ok(Err(_)) => Some((user.id, user.name, false)),
                    Err(_) => {
                        println!("Timeout sending email to {}", user.name);
                        Some((user.id, user.name, false))
                    }
                }
            })
            .collect_rs2()
            .await;

        println!("Email results:");
        for (user_id, name, success) in email_results {
            println!(
                "User {} ({}): {}",
                user_id,
                name,
                if success {
                    "✅ Email sent"
                } else {
                    "❌ Failed to send email"
                }
            );
        }

        // Example 4: Advanced user analytics
        println!("\n=== Example 4: User Analytics ===");
        let analytics = from_iter(users.clone())
            .filter_rs2(|user| user.active)
            .fold_rs2((0, 0, 0), |(total, admins, moderators), user| {
                let new_total = total + 1;
                let new_admins = if user.role == "admin" { admins + 1 } else { admins };
                let new_moderators = if user.role == "moderator" { moderators + 1 } else { moderators };
                (new_total, new_admins, new_moderators)
            })
            .await;

        println!("📊 User Analytics:");
        println!("  Total active users: {}", analytics.0);
        println!("  Admin users: {}", analytics.1);
        println!("  Moderator users: {}", analytics.2);
        println!("  Regular users: {}", analytics.0 - analytics.1 - analytics.2);

        // Example 5: Stream pipeline with evaluation
        println!("\n=== Example 5: Stream Pipeline with Async Evaluation ===");
        let dynamic_stream = eval(async move {
            // Simulate dynamic user loading based on some condition
            let additional_users = vec![
                User {
                    id: 6,
                    name: "Frank".to_string(),
                    email: "frank@example.com".to_string(),
                    active: true,
                    role: "user".to_string(),
                },
                User {
                    id: 7,
                    name: "Grace".to_string(),
                    email: "grace@example.com".to_string(),
                    active: true,
                    role: "admin".to_string(),
                },
            ];
            additional_users
        });

        let dynamic_results = dynamic_stream
            .flat_map_rs2(|users| from_iter(users))
            .map_rs2(|user| format!("Dynamic user: {} ({})", user.name, user.role))
            .collect_rs2()
            .await;

        println!("Dynamic users loaded:");
        for result in dynamic_results {
            println!("  - {}", result);
        }

        println!("\n✅ User stream processing completed successfully!");
    });
}
