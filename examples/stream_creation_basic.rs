use rs2::rs2::*;
use futures_util::stream::StreamExt;
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
        // Create a stream with a single user
        let user = User {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            active: true,
            role: "admin".to_string(),
        };

        let single_user_stream = emit(user.clone());
        let first_user = single_user_stream.collect::<Vec<_>>().await;
        println!("Single user: {:?}", first_user[0].name);

        // Create an empty stream
        let empty_stream: RS2Stream<User> = empty();
        let empty_result = empty_stream.collect::<Vec<_>>().await;
        println!("Empty stream length: {}", empty_result.len()); // 0

        // Create a stream from an iterator
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        ];

        let users_stream = from_iter(users);
        let all_users = users_stream.collect::<Vec<_>>().await;
        println!("All users: {}", all_users.len()); // 3
    });
}