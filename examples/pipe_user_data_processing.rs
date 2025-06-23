use futures_util::stream::StreamExt;
use rs2_stream::pipe::*;
use rs2_stream::rs2::*;
use std::collections::HashMap;
use tokio::runtime::Runtime;

// Define our User type
#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    active: bool,
    role: String,
    login_count: u32,
}

// Define a UserStats type
#[derive(Debug, Clone, PartialEq)]
struct UserStats {
    id: u64,
    name: String,
    role: String,
    is_active: bool,
    login_frequency: &'static str,
}

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of users
        let users = vec![
            User {
                id: 1,
                name: "Alice".to_string(),
                email: "alice@example.com".to_string(),
                active: true,
                role: "admin".to_string(),
                login_count: 120,
            },
            User {
                id: 2,
                name: "Bob".to_string(),
                email: "bob@example.com".to_string(),
                active: true,
                role: "user".to_string(),
                login_count: 45,
            },
            User {
                id: 3,
                name: "Charlie".to_string(),
                email: "charlie@example.com".to_string(),
                active: false,
                role: "user".to_string(),
                login_count: 5,
            },
            User {
                id: 4,
                name: "Diana".to_string(),
                email: "diana@example.com".to_string(),
                active: true,
                role: "moderator".to_string(),
                login_count: 80,
            },
            User {
                id: 5,
                name: "Eve".to_string(),
                email: "eve@example.com".to_string(),
                active: true,
                role: "user".to_string(),
                login_count: 30,
            },
        ];

        // Create pipes for different transformations

        // 1. Filter active users
        let active_filter = filter(|user: &User| user.active);

        // 2. Transform User to UserStats
        let stats_transform = map(|user: User| {
            let frequency = match user.login_count {
                0..=10 => "Low",
                11..=50 => "Medium",
                _ => "High",
            };

            UserStats {
                id: user.id,
                name: user.name,
                role: user.role,
                is_active: user.active,
                login_frequency: frequency,
            }
        });

        // 3. Compose the pipes
        let process_users = active_filter.compose(stats_transform);

        // Apply the processing pipeline to the stream
        let user_stream = from_iter(users.clone());
        let stats_stream = process_users.apply(user_stream);

        // Collect the results
        let user_stats = stats_stream.collect::<Vec<_>>().await;

        // Print the results
        println!("User Statistics:");
        for stats in user_stats {
            println!(
                "  - {} ({}): {} usage",
                stats.name, stats.role, stats.login_frequency
            );
        }

        // Group users by login frequency using another pipe
        let group_by_frequency = map(|stats: UserStats| (stats.login_frequency, stats));

        // Apply the grouping pipe to the stats stream
        let user_stream = from_iter(users);
        let stats_stream = process_users.apply(user_stream);
        let grouped_stream = group_by_frequency.apply(stats_stream);

        // Collect and organize by frequency
        let mut frequency_groups: HashMap<&str, Vec<UserStats>> = HashMap::new();
        let grouped_stats = grouped_stream.collect::<Vec<_>>().await;

        for (frequency, stats) in grouped_stats {
            frequency_groups
                .entry(frequency)
                .or_insert_with(Vec::new)
                .push(stats);
        }

        // Print the grouped results
        println!("\nUsers by Login Frequency:");
        for (frequency, users) in &frequency_groups {
            println!("  {} Usage ({} users):", frequency, users.len());
            for user in users {
                println!("    - {} ({})", user.name, user.role);
            }
        }
    });
}
