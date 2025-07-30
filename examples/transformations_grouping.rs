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
        println!("=== RS2 Stream Grouping Transformations Example ===\n");

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

        // 1. Group users by role
        println!("1. Grouping Users by Role:");
        let users_by_role = from_iter_rs2(users.clone())
            .group_by_rs2(|user| user.role.clone())
            .collect_rs2()
            .await;

        for (role, role_users) in users_by_role {
            println!("   Role: {}, Count: {}", role, role_users.len());
            for user in role_users {
                println!("     - {}", user.name);
            }
        }

        // 2. Create a stream of status updates
        println!("\n2. Grouping Adjacent Status Updates:");
        let status_updates = vec![
            "online", "online", "online", "away", "away", "online", "online", "offline",
        ];

        // Group adjacent identical status updates
        let grouped_statuses = from_iter_rs2(status_updates)
            .group_adjacent_by_rs2(|&status| status)
            .collect_rs2()
            .await;

        for (status, occurrences) in grouped_statuses {
            println!(
                "   Status '{}' occurred {} consecutive times",
                status,
                occurrences.len()
            );
        }

        // 3. Filter out consecutive duplicate status updates
        println!("\n3. Distinct Until Changed Status Updates:");
        let unique_statuses = from_iter_rs2(vec![
            "online", "online", "away", "away", "online", "offline",
        ])
        .distinct_until_changed_rs2()
        .collect_rs2()
        .await;

        println!("   Unique status transitions: {:?}", unique_statuses); // ["online", "away", "online", "offline"]

        // 4. Use custom equality function to detect significant changes
        println!("\n4. Detecting Significant Metric Changes:");
        #[derive(Clone, Debug)]
        struct ServerMetrics {
            cpu: f64,
            memory: f64,
            connections: usize,
        }

        let metrics = vec![
            ServerMetrics {
                cpu: 10.5,
                memory: 45.0,
                connections: 100,
            },
            ServerMetrics {
                cpu: 11.0,
                memory: 46.0,
                connections: 102,
            }, // Small change
            ServerMetrics {
                cpu: 50.0,
                memory: 80.0,
                connections: 150,
            }, // Big change
            ServerMetrics {
                cpu: 51.0,
                memory: 81.0,
                connections: 155,
            }, // Small change
            ServerMetrics {
                cpu: 20.0,
                memory: 40.0,
                connections: 90,
            }, // Big change
        ];

        // Only emit metrics when there's a significant change
        let significant_changes = from_iter_rs2(metrics)
            .distinct_until_changed_by_rs2(|prev, curr| {
                // Consider it the same if CPU and memory changes are less than 20%
                (curr.cpu - prev.cpu).abs() < 20.0 && (curr.memory - prev.memory).abs() < 20.0
            })
            .collect_rs2()
            .await;

        println!(
            "   Number of significant metric changes: {}",
            significant_changes.len()
        ); // 3

        println!("\n=== Stream Grouping Transformations Example Complete ===");
        println!("\n🎯 Key Features Demonstrated:");
        println!("1. Stream grouping by key using group_by_rs2()");
        println!("2. Adjacent grouping using group_adjacent_by_rs2()");
        println!("3. Distinct until changed with distinct_until_changed_rs2()");
        println!("4. Custom equality detection with distinct_until_changed_by_rs2()");
        println!("5. Stream collection using collect_rs2()");
    });
}
