use rs2_stream::rs2::*;
use futures_util::stream::StreamExt;
use tokio::runtime::Runtime;
use std::time::{Duration, Instant};

// Define our User type for the example
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
}

// Simulate fetching a user from a database
async fn fetch_user(id: u64) -> User {
    // Simulate database delay
    tokio::time::sleep(Duration::from_millis(50)).await;

    User {
        id,
        name: "Alice".to_string(),
        email: "alice@example.com".to_string(),
        active: true,
        role: "admin".to_string(),
    }
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream by evaluating a future
        let user_stream = eval(fetch_user(1));
        let user = user_stream.collect::<Vec<_>>().await;
        println!("Fetched user: {}", user[0].name);

        // Create a stream that emits a value after a delay
        let start = Instant::now();
        let delayed_user = emit_after(
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            Duration::from_millis(100)
        );

        let user = delayed_user.collect::<Vec<_>>().await;
        let elapsed = start.elapsed();

        println!("Delayed user: {} (after {}ms)", user[0].name, elapsed.as_millis());
    });
}