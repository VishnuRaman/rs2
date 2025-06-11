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
        ];

        let users_stream = from_iter(users);

        // Map: Transform each user to just their name
        let names_stream = users_stream
            .map_rs2(|user| user.name);

        let names = names_stream.collect::<Vec<_>>().await;
        println!("User names: {:?}", names); // ["Alice", "Bob", "Charlie"]

        // Create a new stream for filtering
        let users = vec![
            User { id: 1, name: "Alice".to_string(), email: "alice@example.com".to_string(), active: true, role: "admin".to_string() },
            User { id: 2, name: "Bob".to_string(), email: "bob@example.com".to_string(), active: true, role: "user".to_string() },
            User { id: 3, name: "Charlie".to_string(), email: "charlie@example.com".to_string(), active: false, role: "user".to_string() },
        ];

        // Filter: Keep only active users
        let active_users_stream = from_iter(users)
            .filter_rs2(|user| user.active);

        let active_users = active_users_stream.collect::<Vec<_>>().await;
        println!("Active users: {}", active_users.len()); // 2

        // Create a new stream for flat_map
        let departments = vec![
            ("Engineering", vec!["Alice", "Bob"]),
            ("Marketing", vec!["Charlie", "Diana"]),
        ];

        // Flat map: Convert departments to individual employees
        let employees_stream = from_iter(departments)
            .flat_map_rs2(|(dept, employees)| {
                // Create a stream of employees with their department
                from_iter(employees.into_iter().map(move |name| (name, dept)))
            });

        let employees = employees_stream.collect::<Vec<_>>().await;
        println!("Employees: {:?}", employees); 
        // [("Alice", "Engineering"), ("Bob", "Engineering"), ("Charlie", "Marketing"), ("Diana", "Marketing")]
    });
}