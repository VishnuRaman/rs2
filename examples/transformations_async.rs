use rs2::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::Duration;

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

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of user IDs
        let user_ids = from_iter(vec![1, 2, 3]);

        // Use eval_map_rs2 to asynchronously fetch user details for each ID
        let users_stream = user_ids
            .eval_map_rs2(|id| async move {
                fetch_user_details(id).await
            });

        let users = users_stream.collect::<Vec<_>>().await;

        for user in users {
            println!("Fetched user: {} ({})", user.name, user.email);
        }
    });
}