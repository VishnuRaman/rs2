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

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("=== RS2 Stream Combining Transformations Example ===\n");

        // Create two streams of users
        let admins = vec![
            User {
                id: 1,
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                active: true,
                role: "admin".to_string(),
            },
            User {
                id: 4,
                name: "Diana".to_string(),
                email: "diana@example.com".to_string(),
                active: true,
                role: "admin".to_string(),
            },
        ];

        let regular_users = vec![
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
                active: true,
                role: "user".to_string(),
            },
            User {
                id: 5,
                name: "Eve".to_string(),
                email: "eve@example.com".to_string(),
                active: true,
                role: "user".to_string(),
            },
        ];

        println!("1. Zip Transformation - Pairing Admins with Users");
        println!("   Admins: {}", admins.len());
        println!("   Regular Users: {}", regular_users.len());

        // Zip the streams to pair admins with users they manage
        let admin_user_pairs: Vec<(User, User)> = from_iter_rs2(admins.clone())
            .zip_rs2(from_iter_rs2(regular_users.clone()))
            .collect_rs2()
            .await;

        println!("   Admin-User Pairs:");
        for (admin, user) in &admin_user_pairs {
            println!("     👤 Admin {} manages user {}", admin.name, user.name);
        }

        println!("\n2. Zip With Transformation - Custom Management Assignments");

        // Use zip_with to create management assignments
        let assignments: Vec<String> = from_iter_rs2(admins.clone())
            .zip_with_rs2(from_iter_rs2(regular_users.clone()), |admin, user| {
                format!(
                    "🎯 {} is responsible for {}'s onboarding",
                    admin.name, user.name
                )
            })
            .collect_rs2()
            .await;

        println!("   Management Assignments:");
        for assignment in &assignments {
            println!("     {}", assignment);
        }

        println!("\n3. Merge Transformation - Combining All Users");

        // Merge streams to get all users in a single stream
        let all_users: Vec<User> = from_iter_rs2(admins.clone())
            .merge_rs2(from_iter_rs2(regular_users.clone()))
            .collect_rs2()
            .await;

        println!("   Total users after merge: {}", all_users.len());
        println!("   All users by role:");
        for user in &all_users {
            let role_icon = if user.role == "admin" { "👑" } else { "👤" };
            println!("     {} {} ({})", role_icon, user.name, user.role);
        }

        println!("\n4. Timing-based Stream Selection");

        // Create two streams with different timing using unfold_stream
        let fast_stream = unfold_stream(0, |state| async move {
            match state {
                0 => {
                    Some(("⚡ Fast response", 1))
                }
                1 => {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    Some(("⚡ Fast again", 2))
                }
                _ => None,
            }
        });

        let slow_stream = unfold_stream(0, |state| async move {
            match state {
                0 => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    Some(("🐌 Slow response", 1))
                }
                1 => {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    Some(("🐌 Slow again", 2))
                }
                _ => None,
            }
        });

        // Collect both streams to demonstrate their behavior
        println!("   Fast stream results:");
        let fast_results: Vec<&str> = fast_stream.collect_rs2().await;
        for result in &fast_results {
            println!("     {}", result);
        }

        println!("   Slow stream results:");
        let slow_results: Vec<&str> = slow_stream.collect_rs2().await;
        for result in &slow_results {
            println!("     {}", result);
        }

        println!("\n5. Advanced Stream Combining - Round Robin");

        // Create multiple streams and demonstrate round-robin combining
        let stream1 = from_iter_rs2(vec!["A1", "A2", "A3"]);
        let stream2 = from_iter_rs2(vec!["B1", "B2"]);
        let stream3 = from_iter_rs2(vec!["C1", "C2", "C3", "C4"]);

        // Use interleave to round-robin between streams
        let interleaved: Vec<&str> = stream1
            .interleave_rs2(vec![stream2, stream3])
            .collect_rs2()
            .await;

        println!("   Round-robin interleaved results:");
        for (i, item) in interleaved.iter().enumerate() {
            println!("     {}: {}", i + 1, item);
        }

        println!("\n6. Stream Concatenation");

        // Demonstrate stream concatenation
        let first_batch = from_iter_rs2(vec!["First-1", "First-2"]);
        let second_batch = from_iter_rs2(vec!["Second-1", "Second-2", "Second-3"]);

        let concatenated: Vec<&str> = first_batch
            .chain_rs2(second_batch)
            .collect_rs2()
            .await;

        println!("   Concatenated stream results:");
        for (i, item) in concatenated.iter().enumerate() {
            println!("     {}: {}", i + 1, item);
        }

        println!("\n=== Stream Combining Transformations Example Complete ===");
        println!("\n🎯 Key Features Demonstrated:");
        println!("1. Zip transformation - pairing elements from two streams");
        println!("2. Zip with transformation - custom combining logic");
        println!("3. Merge transformation - combining streams into one");
        println!("4. Timing-based streams - different execution patterns");
        println!("5. Round-robin interleaving - balanced stream mixing");
        println!("6. Stream concatenation - sequential stream joining");
    });
}
