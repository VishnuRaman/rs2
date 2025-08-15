use rs2_stream::rs2::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
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
        println!("=== RS2 Stream Creation Basic Example ===\n");

        // Create a stream with a single user
        let user = User {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
            active: true,
            role: "admin".to_string(),
        };

        println!("1. Single User Stream:");
        let single_user_stream = once_stream(user.clone());
        let first_user = single_user_stream.collect_rs2().await;
        println!("   Single user: {} (ID: {})", first_user[0].name, first_user[0].id);

        // Create an empty stream
        println!("\n2. Empty User Stream:");
        let empty_stream = empty_rs2::<User>();
        let empty_result = empty_stream.collect_rs2().await;
        println!("   Empty stream length: {}", empty_result.len()); // 0

        // Create a stream from an iterator
        println!("\n3. Stream from Iterator:");
        let users = vec![
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
        ];

        let users_stream = from_iter_rs2(users);
        let all_users = users_stream.collect_rs2().await;
        println!("   All users count: {}", all_users.len()); // 3
        
        for (i, user) in all_users.iter().enumerate() {
            println!("   User {}: {} - {} ({})", 
                i + 1, user.name, user.role, 
                if user.active { "active" } else { "inactive" });
        }

        println!("\n=== Stream Creation Example Complete ===");
        println!("\nKey Features Demonstrated:");
        println!("1. Single item stream creation with emit_rs2()");
        println!("2. Empty stream creation with empty_rs2::<T>()");
        println!("3. Stream creation from iterator with from_iter_rs2()");
        println!("4. Stream collection with collect_rs2()");
    });
}
