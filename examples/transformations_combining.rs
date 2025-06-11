use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use async_stream::stream;

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
        // Create two streams of users
        let admins = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "admin".to_string() },
        ];

        let regular_users = vec![
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string() },
        ];

        // Zip the streams to pair admins with users they manage
        let admin_user_pairs = from_iter(admins.clone())
            .zip_rs2(from_iter(regular_users.clone()))
            .collect::<Vec<_>>()
            .await;

        for (admin, user) in admin_user_pairs {
            println!("Admin {} manages user {}", admin.name, user.name);
        }

        // Use zip_with to create management assignments
        let assignments = from_iter(admins.clone())
            .zip_with_rs2(from_iter(regular_users.clone()), |admin, user| {
                format!("{} is responsible for {}'s onboarding", admin.name, user.name)
            })
            .collect::<Vec<_>>()
            .await;

        for assignment in assignments {
            println!("{}", assignment);
        }

        // Merge streams to get all users in a single stream
        let all_users = from_iter(admins.clone())
            .merge_rs2(from_iter(regular_users.clone()))
            .collect::<Vec<_>>()
            .await;

        println!("Total users after merge: {}", all_users.len()); // 5

        // Create two streams with different timing
        let fast_stream = stream! {
            yield "Fast response";
            tokio::time::sleep(Duration::from_millis(50)).await;
            yield "Fast again";
        }.boxed();

        let slow_stream = stream! {
            tokio::time::sleep(Duration::from_millis(20)).await;
            yield "Slow response";
            tokio::time::sleep(Duration::from_millis(100)).await;
            yield "Slow again";
        }.boxed();

        // Use either to select whichever stream produces a value first
        let results = fast_stream
            .either_rs2(slow_stream)
            .collect::<Vec<_>>()
            .await;

        println!("Results from either: {:?}", results);
        // Should contain "Fast response", "Slow response", "Fast again", "Slow again"
    });
}