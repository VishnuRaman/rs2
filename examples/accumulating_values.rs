use futures_util::stream::StreamExt;
use rs2_stream::rs2::*;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Create a stream of user activity events
        let user_activities = vec![
            ("Alice", 120), // User and time spent in seconds
            ("Bob", 45),
            ("Alice", 60),
            ("Charlie", 90),
            ("Bob", 30),
            ("Alice", 75),
        ];

        // Use fold_rs2 to calculate total time spent by each user
        let total_time_by_user = from_iter(user_activities.clone())
            .fold_rs2(
                std::collections::HashMap::new(),
                |mut acc, (user, time)| async move {
                    *acc.entry(user).or_insert(0) += time;
                    acc
                },
            )
            .await;

        // Alice 120+60+75 = 255, Bob 45+30 = 75, Charlie 90
        assert_eq!(total_time_by_user.get("Alice"), Some(&255));
        assert_eq!(total_time_by_user.get("Bob"), Some(&75));
        assert_eq!(total_time_by_user.get("Charlie"), Some(&90));

        println!("Total time spent by each user:");
        for (user, time) in total_time_by_user {
            println!("  - {}: {} seconds", user, time);
        }

        // Use scan_rs2 to calculate running total of time spent
        let running_total = from_iter(user_activities)
            .scan_rs2(0, |acc, (user, time)| {
                println!("Processing activity: {} spent {} seconds", user, time);
                acc + time
            })
            .collect::<Vec<_>>()
            .await;

        println!("Running total of time spent:");
        for (i, total) in running_total.iter().enumerate() {
            println!("  After activity {}: {} seconds", i + 1, total);
        }
    });
}
