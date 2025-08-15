use rs2_stream::rs2::*;
use rs2_stream::rs2_stream_ext::RS2StreamExt;
use std::collections::HashMap;
use tokio::runtime::Runtime;

fn main() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        println!("=== RS2 Accumulating Values Example ===\n");

        // Create a stream of user activity events
        let user_activities = vec![
            ("Alice", 120), // User and time spent in seconds
            ("Bob", 45),
            ("Alice", 60),
            ("Charlie", 90),
            ("Bob", 30),
            ("Alice", 75),
        ];

        // 1. Use fold_rs2 to calculate total time spent by each user
        println!("1. Calculating Total Time by User with fold_rs2:");
        let total_time_by_user = from_iter_rs2(user_activities.clone())
            .fold_rs2(
                HashMap::new(),
                |mut acc, (user, time)| {
                    *acc.entry(user).or_insert(0) += time;
                    acc
                },
            )
            .await;

        println!("   Total time spent by each user:");
        for (user, time) in &total_time_by_user {
            println!("     - {}: {} seconds", user, time);
        }

        // 2. Use scan_rs2 to calculate running total of time spent
        println!("\n2. Calculating Running Total with scan_rs2:");
        let running_total = from_iter_rs2(user_activities.clone())
            .scan_rs2(0, |acc, (user, time)| {
                println!("   Processing activity: {} spent {} seconds", user, time);
                *acc += time;
                Some(*acc)
            })
            .collect_rs2()
            .await;

        println!("   Running total of time spent:");
        for (i, total) in running_total.iter().enumerate() {
            println!("     After activity {}: {} seconds", i + 1, total);
        }

        // 3. Finding Maximum Single Session Time with reduce_rs2
        println!("3. Finding Maximum Single Session Time with reduce_rs2:");
        let max_time = from_iter_rs2(user_activities.clone())
            .map_rs2(|(_, time)| time)
            .reduce_rs2(|acc, time| std::cmp::max(acc, time))
            .await;

        match max_time {
            Some(max) => println!("   Maximum single session time: {} seconds", max),
            None => println!("   No activities found"),
        }

        // 4. Counting Total Activities:
        println!("\n4. Counting Total Activities:");
        let total_activities = from_iter_rs2(user_activities.clone()).count_rs2().await;
        println!("   Total number of activities: {}", total_activities);

        // 5. Calculating Average Session Time:
        println!("\n5. Calculating Average Session Time:");
        let (total_time, count) = from_iter_rs2(user_activities.clone())
            .fold_rs2((0, 0), |(total_time, count), (_, time)| {
                (total_time + time, count + 1)
            })
            .await;
        let average = if count > 0 { total_time as f32 / count as f32 } else { 0.0 };
        println!("   Average session time: {:.2} seconds", average);

        // 6. Finding Minimum Single Session Time with reduce_rs2
        println!("\n6. Finding Minimum Single Session Time with reduce_rs2:");
        let min_time = from_iter_rs2(user_activities.clone())
            .map_rs2(|(_, time)| time)
            .reduce_rs2(|acc, time| std::cmp::min(acc, time))
            .await;

        match min_time {
            Some(min) => println!("   Minimum single session time: {} seconds", min),
            None => println!("   No activities found"),
        }

        println!("\n=== Accumulating Values Example Complete ===");
        println!("\n🎯 Key Features Demonstrated:");
        println!("1. Complex state accumulation with fold_rs2() using HashMap");
        println!("2. Running totals with scan_rs2() for intermediate results");
        println!("3. Maximum value finding with reduce_rs2() for cleaner code");
        println!("4. Simple counting with count_rs2()");
        println!("5. Average calculation with tuple accumulation using fold_rs2()");
        println!("6. Minimum value finding with reduce_rs2() for cleaner code");
    });
}
