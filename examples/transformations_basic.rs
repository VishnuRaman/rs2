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
        println!("=== RS2 Basic Transformations Example ===\n");

        // Create a helper function to create sample users
        let create_sample_users = || vec![
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

        println!("1. Map Transformation - Extract User Names");
        
        // Create a stream of users
        let users = create_sample_users();
        println!("   Original users:");
        for user in &users {
            println!("     {} ({}): {} - {}", user.name, user.role, user.email, 
                    if user.active { "Active" } else { "Inactive" });
        }

        let users_stream = from_iter_rs2(users);

        // Map: Transform each user to just their name
        let names_stream = users_stream.map_rs2(|user| user.name);

        let names: Vec<String> = names_stream.collect_rs2().await;
        println!("   Extracted names: {:?}", names);

        println!("\n2. Filter Transformation - Keep Only Active Users");

        // Create a new stream for filtering
        let users = create_sample_users();
        println!("   Filtering from {} users...", users.len());

        // Filter: Keep only active users
        let active_users_stream = from_iter_rs2(users).filter_rs2(|user| user.active);

        let active_users: Vec<User> = active_users_stream.collect_rs2().await;
        println!("   Active users found: {}", active_users.len());
        for user in &active_users {
            println!("     ✅ {} ({}) - {}", user.name, user.role, user.email);
        }

        println!("\n3. Flat Map Transformation - Department to Employees");

        // Create departments with employee lists
        let departments = vec![
            ("Engineering", vec!["Alice", "Bob"]),
            ("Marketing", vec!["Charlie", "Diana"]),
            ("Sales", vec!["Eve", "Frank"]),
        ];

        println!("   Original departments:");
        for (dept, employees) in &departments {
            println!("     {}: {:?}", dept, employees);
        }

        // Flat map: Convert departments to individual employees
        let employees_stream = from_iter_rs2(departments).flat_map_rs2(|(dept, employees)| {
            // Create a stream of employees with their department
            from_iter_rs2(employees.into_iter().map(move |name| (name, dept)))
        });

        let employees: Vec<(&str, &str)> = employees_stream.collect_rs2().await;
        println!("   Flattened employee list:");
        for (name, dept) in &employees {
            println!("     {} works in {}", name, dept);
        }

        println!("\n4. Chained Transformations - Filter + Map");

        let users = create_sample_users();
        
        // Chain filter and map operations
        let active_user_emails: Vec<String> = from_iter_rs2(users)
            .filter_rs2(|user| user.active)  // Keep only active users
            .map_rs2(|user| user.email)      // Extract their emails
            .collect_rs2()
            .await;

        println!("   Active user emails:");
        for email in &active_user_emails {
            println!("     📧 {}", email);
        }

        println!("\n5. Complex Transformation - Role-based Grouping");

        let users = create_sample_users();
        
        // Transform users into role summaries
        let role_info: Vec<String> = from_iter_rs2(users)
            .map_rs2(|user| {
                let status = if user.active { "Active" } else { "Inactive" };
                format!("{}: {} ({})", user.role.to_uppercase(), user.name, status)
            })
            .collect_rs2()
            .await;

        println!("   Role-based summaries:");
        for info in &role_info {
            println!("     🏷️  {}", info);
        }

        println!("\n=== Basic Transformations Example Complete ===");
        println!("\n🎯 Key Features Demonstrated:");
        println!("1. Map transformation - extracting specific fields");
        println!("2. Filter transformation - selecting items by criteria");
        println!("3. Flat map transformation - flattening nested structures");
        println!("4. Chained transformations - combining multiple operations");
        println!("5. Complex transformations - sophisticated data reshaping");
    });
}
