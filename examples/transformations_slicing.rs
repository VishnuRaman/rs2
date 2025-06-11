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
        // Create a stream of users
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
            User { id: 4, name: "Diana".to_string(), email: "diana@example.com".to_string(), active: true, role: "moderator".to_string() },
            User { id: 5, name: "Eve".to_string(), email: "eve@example.com".to_string(), active: true, role: "user".to_string() },
        ];

        // Take the first 2 users
        let first_two_users = from_iter(users.clone())
            .take_rs2(2)
            .collect::<Vec<_>>()
            .await;

        println!("First two users: {} and {}", first_two_users[0].name, first_two_users[1].name);

        // Skip the first 3 users
        let last_two_users = from_iter(users.clone())
            .drop_rs2(3) // or .skip_rs2(3)
            .collect::<Vec<_>>()
            .await;

        println!("Last two users: {} and {}", last_two_users[0].name, last_two_users[1].name);

        // Take users while they are active
        let initial_active_users = from_iter(users.clone())
            .take_while_rs2(|user| {
                let user_clone = user.clone();
                async move { user_clone.active }
            })
            .collect::<Vec<_>>()
            .await;

        println!("Initial active users: {}", initial_active_users.len()); // 2 (stops at Charlie who is inactive)

        // Skip users while they have specific roles
        let non_standard_users = from_iter(users.clone())
            .drop_while_rs2(|user| {
                let user_clone = user.clone();
                async move { 
                    user_clone.role == "admin" || user_clone.role == "user" 
                }
            })
            .collect::<Vec<_>>()
            .await;

        println!("Non-standard role users: {}", non_standard_users.len()); // 3 (Diana, Eve)
    });
}
