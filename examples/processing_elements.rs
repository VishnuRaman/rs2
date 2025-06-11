use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;
use std::collections::{HashSet, BTreeMap};
use std::sync::Arc;
use tokio::sync::Mutex;

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

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users to notify
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        ];

        // Use for_each_rs2 to send notifications to each user
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
        println!("Notified {} users", notified_users.len());

        // Use collect_rs2 to gather users into different collections

        // Collect into a HashSet (removes duplicates)
        let unique_roles = from_iter(users.clone())
            .map_rs2(|user| user.role)
            .collect_rs2::<HashSet<_>>()
            .await;

        println!("Unique roles: {:?}", unique_roles);

        // Collect into a BTreeMap (sorted by key)
        let users_by_id = from_iter(users)
            .map_rs2(|user| (user.id, user))
            .collect_rs2::<BTreeMap<_, _>>()
            .await;

        println!("Users by ID:");
        for (id, user) in users_by_id {
            println!("  {}: {}", id, user.name);
        }
    });
}