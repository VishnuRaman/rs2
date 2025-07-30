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
        println!("=== RS2 Stream Slicing Transformations Example ===\n");

        // Create a stream of users
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
                active: true,
                role: "user".to_string(),
            },
        ];

        // 1. Take the first 2 users
        println!("1. Taking First 2 Users:");
        let first_two_users = from_iter_rs2(users.clone())
            .take_rs2(2)
            .collect_rs2()
            .await;

        for (i, user) in first_two_users.iter().enumerate() {
            println!("   {}: {} ({})", i + 1, user.name, user.role);
        }

        // 2. Skip the first 3 users
        println!("\n2. Skipping First 3 Users:");
        let last_two_users = from_iter_rs2(users.clone())
            .drop_rs2(3) // or .skip_rs2(3)
            .collect_rs2()
            .await;

        for (i, user) in last_two_users.iter().enumerate() {
            println!("   {}: {} ({})", i + 1, user.name, user.role);
        }

        // 3. Take users while they are active
        println!("\n3. Taking Users While Active:");
        let initial_active_users = from_iter_rs2(users.clone())
            .take_while_rs2(|user| user.active)
            .collect_rs2()
            .await;

        println!("   Found {} active users:", initial_active_users.len());
        for user in &initial_active_users {
            println!("     - {} ({})", user.name, user.role);
        }

        // 4. Drop/Skip while a condition is true
        println!("\n4. Drop/Skip While Operations:");
        
        // Skip while using skip_while_rs2
        let skip_result: Vec<_> = from_iter_rs2(users.clone())
            .skip_while_rs2(|user| user.role == "user")
            .collect_rs2()
            .await;
        println!("   Skip while role == 'user': {} users", skip_result.len());
        for user in &skip_result {
            println!("     - {}: {} ({})", user.name, user.role, if user.active { "active" } else { "inactive" });
        }

        // Drop while using drop_while_rs2 (equivalent to skip_while)
        let drop_result: Vec<_> = from_iter_rs2(users.clone())
            .drop_while_rs2(|user| user.role == "user")
            .collect_rs2()
            .await;
        println!("   Drop while role == 'user': {} users", drop_result.len());
        for user in &drop_result {
            println!("     - {}: {} ({})", user.name, user.role, if user.active { "active" } else { "inactive" });
        }

        // 5. Demonstrate chunking for additional slicing
        println!("\n5. Chunking Users into Groups of 2:");
        let user_chunks = from_iter_rs2(users.clone())
            .chunks_rs2(2)
            .collect_rs2()
            .await;

        for (i, chunk) in user_chunks.iter().enumerate() {
            println!("   Chunk {}: {} users", i + 1, chunk.len());
            for user in chunk {
                println!("     - {} ({})", user.name, user.role);
            }
        }

        // 6. Use enumeration for indexed slicing
        println!("\n6. Enumerating Users:");
        let enumerated_users = from_iter_rs2(users.clone())
            .enumerate_rs2()
            .filter_rs2(|(index, _)| *index % 2 == 0) // Take every other user
            .map_rs2(|(index, user)| format!("#{}: {}", index, user.name))
            .collect_rs2()
            .await;

        for item in &enumerated_users {
            println!("   {}", item);
        }

        println!("\n=== Stream Slicing Transformations Example Complete ===");
        println!("\n🎯 Key Features Demonstrated:");
        println!("1. Taking first N items with take_rs2()");
        println!("2. Skipping first N items with skip_rs2()");
        println!("3. Taking items while a condition is true with take_while_rs2()");
        println!("4. Skipping/dropping items while a condition is true with skip_while_rs2() and drop_while_rs2()");
        println!("5. Stream slicing for data processing and filtering");
    });
}
