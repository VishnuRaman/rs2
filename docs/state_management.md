# State Management in RS2

RS2 provides state management capabilities that allow you to maintain context and remember information across stream processing operations. This is essential for building streaming applications for user session tracking, fraud detection, and real-time analytics.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Storage Backends](#storage-backends)
- [State Configurations](#state-configurations)
- [Key Extractors](#key-extractors)
- [State Access Interface](#state-access-interface)
- [Stateful Operations](#stateful-operations)
- [Examples](#examples)
- [Best Practices](#best-practices)
- [Performance Considerations](#performance-considerations)
- [Custom State Backends](#custom-state-backends)

## Overview

State management in RS2 enables you to:

- **Remember information** across multiple events
- **Track user sessions** and user behavior
- **Detect patterns** and anomalies in real-time
- **Maintain running totals** and aggregations
- **Join streams** based on shared state
- **Scale horizontally** with distributed state storage

## Quick Start

```rust
use rs2::{
    rs2_stream_ext::Rs2StreamExt,
    state::{StatefulStreamExt, StateConfigs, CustomKeyExtractor},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserEvent {
    user_id: String,
    amount: f64,
    timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserState {
    total_amount: f64,
    event_count: u64,
}

#[tokio::main]
async fn main() {
    let events = create_user_events();
    
    let config = StateConfigs::session();
    
    events
        .stateful_map_rs2(
            config,
            CustomKeyExtractor::new(|event: &UserEvent| event.user_id.clone()),
            |event, state_access| async move {
                let mut state: UserState = state_access.get().await.unwrap_or(UserState {
                    total_amount: 0.0,
                    event_count: 0,
                });

                state.total_amount += event.amount;
                state.event_count += 1;

                state_access.set(&state).await.unwrap();
                
                (event.user_id, state.total_amount, state.event_count)
            },
        )
        .for_each(|(user_id, total, count)| async {
            println!("User {}: ${:.2} total, {} events", user_id, total, count);
        })
        .await;
}
```

## Core Concepts

### State Storage

State storage is the backend that persists your state data. RS2 supports multiple storage backends:

- **In-Memory**: Fastest, but not persistent across restarts (default implementation)
- **Custom**: Implement your own storage backend by implementing the `StateStorage` trait

**Note**: The `InMemoryState` is just the default implementation provided by RS2. You can easily replace it with your own storage backend (Redis, PostgreSQL, etc.) by implementing the `StateStorage` trait and using the `with_custom_storage` method on your `StateConfig`.

### In-Memory Storage Caveat

**Important**: The `InMemoryState` backend will overwrite existing values when using the `set` method. If you need atomic read-modify-write operations (like incrementing counters), you should either:

1. **Use custom storage backends** that implement atomic operations
2. **Implement atomic logic in your application code** by reading the current value, modifying it, and then setting it back
3. **Use the state access interface** which provides atomic operations for stateful stream operations

For examples of implementing atomic operations in custom backends, see [examples/custom_storage_example.rs](../examples/custom_storage_example.rs).

## State Configurations

State configurations control how your state is stored, managed, and cleaned up. RS2 provides both predefined configurations for common use cases and a flexible builder pattern for custom configurations.

### Configuration Structure

The `StateConfig` struct contains all the settings for state management:

```rust
#[derive(Debug, Clone)]
pub struct StateConfig {
    pub storage_type: StateStorageType,    // Storage backend type
    pub ttl: Duration,                     // Time-to-live for state entries
    pub cleanup_interval: Duration,        // How often to clean up expired entries
    pub max_size: Option<usize>,           // Maximum number of entries (in-memory only)
    pub custom_storage: Option<Arc<dyn StateStorage + Send + Sync>>, // Custom storage backend
}
```

### Predefined Configurations

RS2 provides several predefined configurations optimized for different use cases:

#### 1. High Performance Configuration
```rust
let config = StateConfigs::high_performance();
```
**Use Case**: High-frequency updates, real-time processing, temporary state
- **TTL**: 1 hour
- **Cleanup Interval**: 1 minute
- **Max Size**: 10,000 entries
- **Storage**: In-memory
- **Best For**: Session tracking, rate limiting, temporary aggregations

#### 2. Session Configuration
```rust
let config = StateConfigs::session();
```
**Use Case**: User sessions, temporary user state
- **TTL**: 30 minutes
- **Cleanup Interval**: 5 minutes
- **Max Size**: 1,000 entries
- **Storage**: In-memory
- **Best For**: User sessions, temporary user preferences, short-lived state

#### 3. Short-Lived Configuration
```rust
let config = StateConfigs::short_lived();
```
**Use Case**: Very temporary state, immediate processing
- **TTL**: 5 minutes
- **Cleanup Interval**: 30 seconds
- **Max Size**: 100 entries
- **Storage**: In-memory
- **Best For**: Request-level state, immediate aggregations, temporary caching

#### 4. Long-Lived Configuration
```rust
let config = StateConfigs::long_lived();
```
**Use Case**: Persistent state, historical data, analytics
- **TTL**: 7 days
- **Cleanup Interval**: 1 hour
- **Max Size**: 100,000 entries
- **Storage**: In-memory
- **Best For**: User profiles, historical analytics, persistent aggregations

### Configuration Comparison Table

| Configuration | TTL | Cleanup Interval | Max Size | Storage | Use Case | Performance |
|---------------|-----|------------------|----------|---------|----------|-------------|
| `high_performance()` | 1 hour | 1 minute | 10,000 | In-Memory | High-frequency updates, real-time processing | ⭐⭐⭐⭐⭐ |
| `session()` | 30 minutes | 5 minutes | 1,000 | In-Memory | User sessions, temporary state | ⭐⭐⭐⭐ |
| `short_lived()` | 5 minutes | 30 seconds | 100 | In-Memory | Request-level state, immediate processing | ⭐⭐⭐⭐⭐ |
| `long_lived()` | 7 days | 1 hour | 100,000 | In-Memory | Persistent state, analytics | ⭐⭐⭐ |

### Detailed Preset Usage Examples

#### High Performance Configuration
```rust
use rs2::state::StateConfigs;

// For real-time analytics and high-frequency updates
let config = StateConfigs::high_performance();

// Use cases:
// - Rate limiting (throttle operations)
// - Real-time aggregations
// - Temporary caching
// - High-frequency event processing
// - Session tracking with frequent updates

// Example: Rate limiting with high performance
events.stateful_throttle_rs2(
    StateConfigs::high_performance(),
    CustomKeyExtractor::new(|event| event.user_id.clone()),
    100, // 100 events per window
    Duration::from_secs(60), // 1 minute window
)
```

#### Session Configuration
```rust
// For user sessions and temporary user state
let config = StateConfigs::session();

// Use cases:
// - User session management
// - Temporary user preferences
// - Shopping cart state
// - Form state persistence
// - Temporary user analytics

// Example: User session tracking
events.stateful_session_rs2(
    StateConfigs::session(),
    CustomKeyExtractor::new(|event| event.user_id.clone()),
    Duration::from_secs(1800), // 30 minute session timeout
    |event, is_new_session, state_access| async move {
        if is_new_session {
            println!("New session started for user: {}", event.user_id);
        }
        // Process event with session context
        SessionResult { event, session_id: event.session_id, is_new_session }
    },
)
```

#### Short-Lived Configuration
```rust
// For very temporary state and immediate processing
let config = StateConfigs::short_lived();

// Use cases:
// - Request-level state
// - Immediate aggregations
// - Temporary caching
// - One-time processing
// - Debugging and testing

// Example: Request-level deduplication
events.stateful_deduplicate_rs2(
    StateConfigs::short_lived(),
    CustomKeyExtractor::new(|event| {
        format!("{}:{}", event.user_id, event.request_id)
    }),
    Duration::from_secs(300), // 5 minute TTL
)
```

#### Long-Lived Configuration
```rust
// For persistent state and historical data
let config = StateConfigs::long_lived();

// Use cases:
// - User profiles
// - Historical analytics
// - Persistent aggregations
// - Long-term tracking
// - Data warehousing

// Example: User profile enrichment
events.stateful_map_rs2(
    StateConfigs::long_lived(),
    CustomKeyExtractor::new(|event| event.user_id.clone()),
    |event, state_access| async move {
        let mut profile: UserProfile = state_access.get().await.unwrap_or(UserProfile::default());
        
        // Update profile with new event data
        profile.total_purchases += 1;
        profile.total_spent += event.amount;
        profile.last_seen = event.timestamp;
        
        state_access.set(&profile).await.unwrap();
        
        // Return enriched event
        EnrichedEvent { 
            event, 
            profile: profile.clone() 
        }
    },
)
```

### Custom Configuration Builder

For fine-grained control, use the `StateConfigBuilder`:

```rust
let config = StateConfigBuilder::new()
    .storage_type(StateStorageType::InMemory)
    .ttl(Duration::from_secs(3600))        // 1 hour TTL
    .cleanup_interval(Duration::from_secs(300))  // 5 minute cleanup
    .max_size(50000)                       // 50k entry limit
    .custom_storage(my_custom_storage)     // Custom storage backend
    .build()
    .unwrap();
```

### Configuration Options Explained

#### Storage Type (`storage_type`)
```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateStorageType {
    InMemory,  // Fastest, non-persistent
    // Future storage backends will be added here
}
```

#### Time-to-Live (`ttl`)
Controls how long state entries are kept before automatic expiration:
```rust
// Short-lived (5 minutes)
.ttl(Duration::from_secs(300))

// Medium-lived (1 hour)
.ttl(Duration::from_secs(3600))

// Long-lived (24 hours)
.ttl(Duration::from_secs(86400))

// Very long-lived (7 days)
.ttl(Duration::from_secs(604800))
```

#### Cleanup Interval (`cleanup_interval`)
How often the system removes expired entries:
```rust
// Frequent cleanup (30 seconds)
.cleanup_interval(Duration::from_secs(30))

// Standard cleanup (5 minutes)
.cleanup_interval(Duration::from_secs(300))

// Infrequent cleanup (1 hour)
.cleanup_interval(Duration::from_secs(3600))
```

**Best Practice**: Set cleanup interval to 1/10th to 1/100th of your TTL for optimal performance.

#### Maximum Size (`max_size`)
Limits the number of entries in in-memory storage (simple eviction strategy):
```rust
// Small state (1k entries)
.max_size(1000)

// Medium state (10k entries)
.max_size(10000)

// Large state (100k entries)
.max_size(100000)

// Unlimited (use with caution)
.max_size(usize::MAX)
```

#### Custom Storage (`custom_storage`)
Set a custom storage backend implementing the `StateStorage` trait:
```rust
// Use a custom Redis storage backend
.custom_storage(Arc::new(RedisStorage::new(redis_client)))

// Use a custom PostgreSQL storage backend
.custom_storage(Arc::new(PostgreSQLStorage::new(db_pool)))

// Use a custom in-memory storage with special features
.custom_storage(Arc::new(CustomInMemoryStorage::new()))
```

**Note**: When using `custom_storage`, the `storage_type` is automatically set to `StateStorageType::Custom`.

### Direct StateConfig Methods

You can also configure state directly on a `StateConfig` instance:

```rust
let mut config = StateConfig::new();

// Set storage type
config = config.storage_type(StateStorageType::InMemory);

// Set TTL
config = config.ttl(Duration::from_secs(3600));

// Set cleanup interval
config = config.cleanup_interval(Duration::from_secs(300));

// Set maximum size
config = config.max_size(50000);

// Set custom storage backend
config = config.with_custom_storage(Arc::new(MyCustomStorage::new()));
```

The `with_custom_storage` method automatically sets the storage type to `StateStorageType::Custom` and stores your custom storage backend.

### Storage Creation

You can create storage instances directly from a `StateConfig`:

```rust
let config = StateConfigs::session();

// Create a Box<dyn StateStorage> instance
let storage: Box<dyn StateStorage + Send + Sync> = config.create_storage();

// Create an Arc<dyn StateStorage> instance (for sharing across threads)
let storage_arc: Arc<dyn StateStorage + Send + Sync> = config.create_storage_arc();
```

These methods handle the creation of the appropriate storage backend based on your configuration, including custom storage backends.

### Configuration Validation

The builder automatically validates your configuration:

```rust
let config = StateConfigBuilder::new()
    .ttl(Duration::from_secs(0))  // Invalid: zero TTL
    .build();  // Returns Err("TTL cannot be zero")

let config = StateConfigBuilder::new()
    .ttl(Duration::from_secs(3600))
    .cleanup_interval(Duration::from_secs(7200))  // Invalid: cleanup > TTL
    .build();  // Returns Err("Cleanup interval should be less than or equal to TTL")
```

### Configuration Best Practices

#### 1. Choose TTL Based on Use Case
```rust
// Session state: 30 minutes
let session_config = StateConfigs::session();

// User profile: 24 hours
let profile_config = StateConfigBuilder::new()
    .ttl(Duration::from_secs(86400))
    .cleanup_interval(Duration::from_secs(3600))
    .max_size(10000)
    .build()
    .unwrap();

// Analytics: 7 days
let analytics_config = StateConfigs::long_lived();
```

#### 2. Set Appropriate Cleanup Intervals
```rust
// For 1-hour TTL: cleanup every 5-10 minutes
.ttl(Duration::from_secs(3600))
.cleanup_interval(Duration::from_secs(300))

// For 24-hour TTL: cleanup every 1-2 hours
.ttl(Duration::from_secs(86400))
.cleanup_interval(Duration::from_secs(3600))
```

#### 3. Limit Memory Usage
```rust
// Always set max_size for production use
.max_size(10000)  // Prevents unbounded memory growth
```

#### 4. Monitor and Adjust
```rust
// Start with conservative settings
let config = StateConfigBuilder::new()
    .ttl(Duration::from_secs(1800))        // 30 minutes
    .cleanup_interval(Duration::from_secs(300))  // 5 minutes
    .max_size(1000)                        // 1k entries
    .build()
    .unwrap();

// Monitor usage and adjust as needed
```

## Key Extractors

Key extractors determine how state is partitioned and organized. They extract a string key from each event, which is used to group related state together.

### Key Extractor Types

#### 1. Custom Key Extractor
The most flexible option, allowing you to define custom key extraction logic:

```rust
use rs2::state::CustomKeyExtractor;

// Simple key extraction
let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| {
    event.user_id.clone()
});

// Composite key extraction
let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| {
    format!("{}:{}", event.user_id, event.category)
});

// Time-based key extraction
let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| {
    let hour = event.timestamp / 3600;
    format!("{}:{}", event.user_id, hour)
});

// Complex key extraction
let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| {
    match event.event_type.as_str() {
        "login" => format!("session:{}", event.user_id),
        "purchase" => format!("purchase:{}:{}", event.user_id, event.category),
        "view" => format!("view:{}:{}", event.user_id, event.page_id),
        _ => format!("other:{}", event.user_id),
    }
});
```

#### 2. Field Key Extractor
A simplified extractor for basic field extraction (currently simplified implementation):

```rust
use rs2::state::FieldKeyExtractor;

let key_extractor = FieldKeyExtractor::new("user_id");
```

### Key Extractor Implementation Details

#### CustomKeyExtractor
The most flexible key extractor that accepts any closure or function:

```rust
pub struct CustomKeyExtractor<F> {
    extractor: F,
}

impl<F> CustomKeyExtractor<F> {
    pub fn new(extractor: F) -> Self {
        Self { extractor }
    }
}

impl<T, F> KeyExtractor<T> for CustomKeyExtractor<F>
where
    F: Fn(&T) -> String,
{
    fn extract_key(&self, event: &T) -> String {
        (self.extractor)(event)
    }
}
```

**Usage Examples:**
```rust
// Function-based extractor
fn extract_user_key(event: &UserEvent) -> String {
    event.user_id.clone()
}
let key_extractor = CustomKeyExtractor::new(extract_user_key);

// Closure-based extractor
let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| {
    event.user_id.clone()
});

// Complex logic extractor
let key_extractor = CustomKeyExtractor::new(|event: &UserEvent| {
    if event.is_premium {
        format!("premium:{}", event.user_id)
    } else {
        format!("standard:{}", event.user_id)
    }
});
```

#### FieldKeyExtractor
A simplified extractor for basic field extraction (currently simplified implementation):

```rust
pub struct FieldKeyExtractor {
    field_name: String,
}

impl FieldKeyExtractor {
    pub fn new(field_name: &str) -> Self {
        Self {
            field_name: field_name.to_string(),
        }
    }
}

impl<T> KeyExtractor<T> for FieldKeyExtractor {
    fn extract_key(&self, _event: &T) -> String {
        // This is a simplified version - in practice you'd use reflection or serde
        // to extract the actual field value
        format!("key_{}", self.field_name)
    }
}
```

**Usage Examples:**
```rust
// Extract by field name
let key_extractor = FieldKeyExtractor::new("user_id");
let key_extractor = FieldKeyExtractor::new("category");
let key_extractor = FieldKeyExtractor::new("session_id");
```

#### Function-Based Key Extractors
You can also use plain functions that implement the `KeyExtractor` trait:

```rust
// Define a function that implements KeyExtractor
fn user_key_extractor(event: &UserEvent) -> String {
    event.user_id.clone()
}

// Use it directly in stateful operations
events.stateful_map_rs2(
    config,
    user_key_extractor,
    |event, state_access| async move {
        // Stateful processing
    },
)
```

### Key Extractor Type Comparison

| Type | Flexibility | Performance | Ease of Use | Use Case |
|------|-------------|-------------|-------------|----------|
| `CustomKeyExtractor` | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | Complex logic, custom transformations |
| `FieldKeyExtractor` | ⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | Simple field extraction |
| Function-based | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | Reusable extractors, clean code |

### Key Design Patterns

#### 1. User-Based Keys
```rust
// Single user state
CustomKeyExtractor::new(|event| event.user_id.clone())

// User + device state
CustomKeyExtractor::new(|event| {
    format!("{}:{}", event.user_id, event.device_id)
})

// User + session state
CustomKeyExtractor::new(|event| {
    format!("{}:{}", event.user_id, event.session_id)
})
```

#### 2. Time-Based Keys
```rust
// Hourly windows
CustomKeyExtractor::new(|event| {
    let hour = event.timestamp / 3600;
    format!("{}:{}", event.user_id, hour)
})

// Daily windows
CustomKeyExtractor::new(|event| {
    let day = event.timestamp / 86400;
    format!("{}:{}", event.user_id, day)
})

// Sliding windows (5-minute buckets)
CustomKeyExtractor::new(|event| {
    let bucket = event.timestamp / 300;
    format!("{}:{}", event.user_id, bucket)
})
```

#### 3. Category-Based Keys
```rust
// Product category state
CustomKeyExtractor::new(|event| {
    format!("category:{}", event.category)
})

// User + category state
CustomKeyExtractor::new(|event| {
    format!("{}:{}", event.user_id, event.category)
})

// Geographic state
CustomKeyExtractor::new(|event| {
    format!("geo:{}:{}", event.country, event.region)
})
```

#### 4. Composite Keys
```rust
// Multi-dimensional state
CustomKeyExtractor::new(|event| {
    format!("{}:{}:{}:{}", 
        event.user_id, 
        event.category, 
        event.device_type, 
        event.country
    )
})

// Hierarchical state
CustomKeyExtractor::new(|event| {
    format!("user:{}:session:{}:page:{}", 
        event.user_id, 
        event.session_id, 
        event.page_id
    )
})
```

### Key Extractor Best Practices

#### 1. Design Stable Keys
```rust
// Good: Stable, predictable keys
CustomKeyExtractor::new(|event| event.user_id.clone())

// Good: Composite but stable keys
CustomKeyExtractor::new(|event| {
    format!("{}:{}", event.user_id, event.category)
})

// Avoid: Keys that change frequently
CustomKeyExtractor::new(|event| {
    format!("{}:{}", event.user_id, event.timestamp)
})
```

#### 2. Balance Granularity
```rust
// Too coarse: All users share same state
CustomKeyExtractor::new(|_event| "global".to_string())

// Too fine: Every event has unique state
CustomKeyExtractor::new(|event| {
    format!("{}:{}:{}:{}:{}", 
        event.user_id, 
        event.timestamp, 
        event.sequence_id, 
        event.random_id, 
        event.nonce
    )
})

// Just right: User-level state
CustomKeyExtractor::new(|event| event.user_id.clone())
```

#### 3. Consider Key Distribution
```rust
// Good: Even distribution
CustomKeyExtractor::new(|event| event.user_id.clone())

// Good: Category-based distribution
CustomKeyExtractor::new(|event| event.category.clone())

// Avoid: Skewed distribution
CustomKeyExtractor::new(|event| {
    if event.user_id == "admin" {
        "admin".to_string()
    } else {
        "user".to_string()
    }
})
```

#### 4. Handle Missing Values
```rust
// Robust key extraction with fallbacks
CustomKeyExtractor::new(|event| {
    let user_id = event.user_id.as_deref().unwrap_or("anonymous");
    let category = event.category.as_deref().unwrap_or("unknown");
    format!("{}:{}", user_id, category)
})

// Error handling for required fields
CustomKeyExtractor::new(|event| {
    if event.user_id.is_empty() {
        format!("anonymous:{}", event.session_id)
    } else {
        event.user_id.clone()
    }
})
```

### Advanced Key Patterns

#### 1. Session-Aware Keys
```rust
CustomKeyExtractor::new(|event| {
    if event.is_new_session {
        format!("session:{}:{}", event.user_id, event.session_id)
    } else {
        format!("user:{}", event.user_id)
    }
})
```

#### 2. Time-Window Keys
```rust
CustomKeyExtractor::new(|event| {
    let window_size = 300; // 5 minutes
    let window_start = (event.timestamp / window_size) * window_size;
    format!("{}:{}", event.user_id, window_start)
})
```

#### 3. Hierarchical Keys
```rust
CustomKeyExtractor::new(|event| {
    match event.event_type.as_str() {
        "purchase" => format!("purchase:{}:{}", event.user_id, event.product_id),
        "view" => format!("view:{}:{}", event.user_id, event.page_id),
        "search" => format!("search:{}:{}", event.user_id, event.query_hash),
        _ => format!("other:{}", event.user_id),
    }
})
```

#### 4. Conditional Keys
```rust
CustomKeyExtractor::new(|event| {
    if event.amount > 1000.0 {
        // High-value transactions get separate state
        format!("high_value:{}", event.user_id)
    } else if event.frequency > 10 {
        // High-frequency users get separate state
        format!("high_freq:{}", event.user_id)
    } else {
        // Regular users
        format!("regular:{}", event.user_id)
    }
})
```

### Key Extractor Performance

#### 1. Efficient Key Generation
```rust
// Good: Simple string operations
CustomKeyExtractor::new(|event| event.user_id.clone())

// Good: Efficient formatting
CustomKeyExtractor::new(|event| {
    format!("{}:{}", event.user_id, event.category)
})

// Avoid: Expensive operations in key extraction
CustomKeyExtractor::new(|event| {
    // Don't do expensive operations here
    let hash = sha256::hash(&event.data); // Expensive!
    format!("{}:{}", event.user_id, hash)
})
```

#### 2. Pre-compute Keys When Possible
```rust
// If you have control over the event structure
#[derive(Debug, Clone)]
struct UserEvent {
    user_id: String,
    category: String,
    precomputed_key: String, // Pre-computed for efficiency
}

let key_extractor = CustomKeyExtractor::new(|event| {
    event.precomputed_key.clone()
});
```

### Key Extractor Examples by Use Case

#### User Session Tracking
```rust
CustomKeyExtractor::new(|event| {
    format!("session:{}:{}", event.user_id, event.session_id)
})
```

#### Fraud Detection
```rust
CustomKeyExtractor::new(|event| {
    format!("fraud:{}:{}:{}", 
        event.user_id, 
        event.ip_address, 
        event.device_fingerprint
    )
})
```

#### Real-time Analytics
```rust
CustomKeyExtractor::new(|event| {
    let hour = event.timestamp / 3600;
    format!("analytics:{}:{}", event.category, hour)
})
```

#### Rate Limiting
```rust
CustomKeyExtractor::new(|event| {
    let minute = event.timestamp / 60;
    format!("rate_limit:{}:{}", event.user_id, minute)
})
```

#### A/B Testing
```rust
CustomKeyExtractor::new(|event| {
    format!("ab_test:{}:{}", event.user_id, event.experiment_id)
})
```

### State Access Interface

The `StateAccess` interface provides methods to interact with state:

```rust
// Get current state
let state: UserState = state_access.get().await.unwrap_or(default_state);

// Set new state
state_access.set(&new_state).await.unwrap();

// Check if key exists
if state_access.exists().await {
    // Key exists
}

// Delete state
state_access.delete().await.unwrap();

// Atomic update
state_access.update(|current: Option<UserState>| {
    let mut state = current.unwrap_or(default_state);
    state.count += 1;
    state
}).await.unwrap();
```

## Storage Backends

State storage is the backend that persists your state data. RS2 supports multiple storage backends:

- **In-Memory**: Fastest, but not persistent across restarts
- **Custom**: Implement your own storage backend

### In-Memory Storage Caveat

**Important**: The `InMemoryState` backend will overwrite existing values when using the `set` method. If you need atomic read-modify-write operations (like incrementing counters), you should either:

1. **Use custom storage backends** that implement atomic operations
2. **Implement atomic logic in your application code** by reading the current value, modifying it, and then setting it back
3. **Use the state access interface** which provides atomic operations for stateful stream operations

For examples of implementing atomic operations in custom backends, see [examples/custom_storage_example.rs](../examples/custom_storage_example.rs).

## Stateful Operations

RS2 provides a comprehensive set of stateful stream operations that allow you to maintain context and state across events. Each operation is designed for specific use cases and includes proper state management.

### Available Stateful Operations

#### 1. Stateful Map
**File**: [stateful_map_example.rs](../examples/stateful_map_example.rs)

Transform events while maintaining state. This operation allows you to enrich events with historical context, track user behavior, and build complex transformations that depend on previous events.

**Use Cases**:
- User profile enrichment
- Session tracking and analytics
- Anomaly detection
- Real-time feature engineering

#### 2. Stateful Filter
**File**: [stateful_filter_example.rs](../examples/stateful_filter_example.rs)

Filter events based on state. This operation enables you to implement complex filtering logic that depends on historical data, such as rate limiting, fraud detection, and adaptive filtering.

**Use Cases**:
- Rate limiting and throttling
- Fraud detection
- Adaptive filtering based on user behavior
- Quality control and validation

#### 3. Stateful Fold
**File**: [state_management_example.rs](../examples/state_management_example.rs) (see the fold example)

Accumulate state across events. This operation is perfect for building running totals, aggregations, and maintaining complex state that evolves over time.

**Use Cases**:
- Running totals and aggregations
- Cumulative analytics
- State machines
- Complex business logic

#### 4. Stateful Window
**File**: [state_management_example.rs](../examples/state_management_example.rs) (see the window example)

Process events in sliding windows with state management. This operation combines windowing with state persistence, allowing you to maintain context across window boundaries.

**Use Cases**:
- Time-based analytics
- Sliding window aggregations
- Real-time dashboards
- Temporal pattern detection

#### 5. Stateful Join
**File**: [state_management_example.rs](../examples/state_management_example.rs) (see the join example)

Join two streams based on shared state. This operation enables complex stream correlation and event matching with stateful context.

**Use Cases**:
- Stream correlation
- Event matching
- Data enrichment from multiple sources
- Complex event processing

#### 6. Stateful Reduce
**File**: [stateful_reduce_example.rs](../examples/stateful_reduce_example.rs)

Reduce/aggregate events with state management. This operation is specifically designed for aggregations and reductions that require maintaining state across events.

**Use Cases**:
- Real-time aggregations
- Statistical calculations
- Data summarization
- Performance metrics

#### 7. Stateful Group By
**File**: [stateful_group_by_example.rs](../examples/stateful_group_by_example.rs)

Group events by key and process with state. This operation allows you to maintain separate state for each group while processing events in batches.

**Use Cases**:
- Multi-tenant processing
- Category-based analytics
- Batch processing with state
- Group-level aggregations

#### 8. Stateful Deduplicate
**File**: [stateful_deduplicate_example.rs](../examples/stateful_deduplicate_example.rs)

Remove duplicates with configurable TTL. This operation maintains a state of seen events to prevent duplicate processing.

**Use Cases**:
- Data quality assurance
- Idempotent processing
- Duplicate event filtering
- Request deduplication

#### 9. Stateful Throttle
**File**: [stateful_throttle_example.rs](../examples/stateful_throttle_example.rs)

Rate limit events with sliding windows. This operation implements sophisticated rate limiting with stateful tracking of request patterns.

**Use Cases**:
- API rate limiting
- Resource protection
- Traffic shaping
- Load balancing

#### 10. Stateful Session
**File**: [stateful_session_example.rs](../examples/stateful_session_example.rs)

Manage user sessions with timeouts. This operation tracks session state and detects new sessions based on configurable timeouts.

**Use Cases**:
- User session management
- Session-based analytics
- Authentication flows
- User journey tracking

#### 11. Stateful Pattern
**File**: [stateful_pattern_example.rs](../examples/stateful_pattern_example.rs)

Detect patterns and anomalies in real-time. This operation maintains pattern state to identify complex sequences and anomalies.

**Use Cases**:
- Fraud detection
- Security monitoring
- Business intelligence
- Anomaly detection

#### 12. Advanced Stateful Group By
**File**: [stateful_group_by_example.rs](../examples/stateful_group_by_example.rs)

Advanced grouping with configurable emission triggers. This operation provides fine-grained control over when groups are emitted.

**Parameters**:
- `max_group_size`: Emit when group reaches this size
- `group_timeout`: Emit group after this timeout
- `emit_on_key_change`: Emit previous group when key changes

**Use Cases**:
- Batch processing with size limits
- Time-based group emission
- Real-time aggregations with triggers
- Multi-tenant processing with timeouts

#### 13. Advanced Stateful Window
**File**: [stateful_window_example.rs](../examples/stateful_window_example.rs)

Advanced windowing with sliding window support and partial emission control. This operation provides sophisticated window management.

**Parameters**:
- `slide_size`: Controls sliding window behavior (None for tumbling)
- `emit_partial`: Whether to emit partial windows at stream end

**Use Cases**:
- Sliding window analytics
- Tumbling window aggregations
- Time-series analysis
- Real-time dashboards with partial results

### Comprehensive Examples

#### State Management Overview
**File**: [state_management_example.rs](../examples/state_management_example.rs)

This comprehensive example demonstrates multiple stateful operations in a single application, showing how to:
- Use different state configurations
- Implement various key extractors
- Combine multiple stateful operations
- Handle state errors and edge cases

#### Custom State Configuration
**File**: [custom_state_config_example.rs](../examples/custom_state_config_example.rs)

This example shows how to:
- Create custom state configurations
- Use different predefined configurations
- Modify existing configurations
- Optimize state management for specific use cases

### Operation Comparison

| Operation | Complexity | State Persistence | Use Case | Example File |
|-----------|------------|-------------------|----------|--------------|
| **Map** | ⭐⭐ | Medium | Event transformation | [stateful_map_example.rs](../examples/stateful_map_example.rs) |
| **Filter** | ⭐⭐ | Medium | Event filtering | [stateful_filter_example.rs](../examples/stateful_filter_example.rs) |
| **Fold** | ⭐⭐⭐ | High | State accumulation | [state_management_example.rs](../examples/state_management_example.rs) |
| **Window** | ⭐⭐⭐ | High | Time-based processing | [state_management_example.rs](../examples/state_management_example.rs) |
| **Window Advanced** | ⭐⭐⭐⭐ | High | Sliding/tumbling windows | [stateful_window_example.rs](../examples/stateful_window_example.rs) |
| **Join** | ⭐⭐⭐⭐ | High | Stream correlation | [state_management_example.rs](../examples/state_management_example.rs) |
| **Reduce** | ⭐⭐⭐ | High | Aggregations | [stateful_reduce_example.rs](../examples/stateful_reduce_example.rs) |
| **Group By** | ⭐⭐⭐ | High | Group processing | [stateful_group_by_example.rs](../examples/stateful_group_by_example.rs) |
| **Group By Advanced** | ⭐⭐⭐⭐ | High | Configurable group emission | [stateful_group_by_example.rs](../examples/stateful_group_by_example.rs) |
| **Deduplicate** | ⭐⭐ | Low | Duplicate removal | [stateful_deduplicate_example.rs](../examples/stateful_deduplicate_example.rs) |
| **Throttle** | ⭐⭐⭐ | Medium | Rate limiting | [stateful_throttle_example.rs](../examples/stateful_throttle_example.rs) |
| **Session** | ⭐⭐ | Medium | Session management | [stateful_session_example.rs](../examples/stateful_session_example.rs) |
| **Pattern** | ⭐⭐⭐⭐ | High | Pattern detection | [stateful_pattern_example.rs](../examples/stateful_pattern_example.rs) |

### Running the Examples

To run any of these examples:

```bash
# Run a specific stateful operation example
cargo run --example stateful_map_example

# Run the comprehensive state management example
cargo run --example state_management_example

# Run the custom configuration example
cargo run --example custom_state_config_example
```

### Example Structure

Each example file follows a consistent structure:

1. **Data Structures**: Define the event and state types
2. **Configuration**: Set up state management configuration
3. **Key Extractors**: Define how to partition state
4. **Stateful Operations**: Implement the specific operation
5. **Results Processing**: Handle and display results
6. **Error Handling**: Demonstrate proper error handling

### Best Practices from Examples

The example files demonstrate several best practices:

- **Proper Error Handling**: All examples show how to handle state access errors
- **Configuration Selection**: Examples show when to use different configurations
- **Key Design**: Examples demonstrate effective key extraction strategies
- **Performance Optimization**: Examples show how to optimize for different use cases
- **Real-world Scenarios**: Examples are based on common real-world use cases

For detailed implementation and usage patterns, refer to the specific example files linked above.

## Best Practices

### 1. Choose the Right Storage Configuration

- **High Performance**: Use for high-frequency updates, real-time processing
- **Session**: Use for user sessions, temporary user state
- **Short-Lived**: Use for request-level state, immediate processing
- **Long-Lived**: Use for persistent state, historical analytics

### 2. Design Efficient Keys

```rust
// Good: Simple, stable keys
CustomKeyExtractor::new(|event| event.user_id.clone())

// Good: Composite keys for complex state
CustomKeyExtractor::new(|event| {
    format!("{}:{}", event.user_id, event.category)
})

// Avoid: Keys that change frequently
CustomKeyExtractor::new(|event| {
    format!("{}:{}", event.user_id, event.timestamp)
})
```

### 3. Handle State Errors Gracefully

```rust
|event, state_access| async move {
    match state_access.get().await {
        Some(state) => {
            // Use existing state
            process_with_state(event, state)
        }
        None => {
            // Create new state
            let new_state = create_initial_state(event);
            state_access.set(&new_state).await.unwrap_or_else(|e| {
                log::warn!("Failed to save state: {}", e);
            });
            process_with_state(event, new_state)
        }
    }
}
```

### 4. Use Appropriate TTL Values

```rust
// Session state: 30 minutes
StateConfigs::session() // TTL: 30 minutes

// User profile: 24 hours
StateConfigBuilder::new()
    .storage_type(StateStorageType::InMemory)
    .ttl(Duration::from_secs(86400))  // 24 hours
    .build()

// Analytics: 7 days
StateConfigs::long_lived() // TTL: 7 days
```

### 5. Monitor State Size

```rust
// Set maximum size for in-memory storage
let config = StateConfigBuilder::new()
    .storage_type(StateStorageType::InMemory)
    .max_size(100_000) // Limit to 100k entries
    .build()
    .unwrap();
```

### 6. Plan for State Persistence

Since RS2 currently only supports in-memory storage:

- **Implement application-level persistence** for critical state
- **Use external systems** for long-term state storage
- **Design for state reconstruction** after restarts
- **Consider hybrid approaches** with external databases

## Performance Considerations

### 1. State Access Patterns

- **Batch operations**: Group multiple state updates together
- **Lazy loading**: Only load state when needed
- **Efficient key design**: Use stable, well-distributed keys

### 2. Memory Management

- **Set appropriate max_size**: Prevent unbounded memory growth
- **Use appropriate TTL**: Clean up expired state automatically
- **Monitor memory usage**: Track state size and growth patterns
- **Optimize cleanup intervals**: Balance cleanup frequency with performance

### 3. Key Distribution

- **Even distribution**: Design keys to distribute evenly
- **Avoid hotspots**: Don't use keys that create bottlenecks
- **Consider key cardinality**: Balance between too many and too few keys

### 4. In-Memory Optimization

- **TTL management**: Set appropriate TTL values to prevent memory bloat
- **Cleanup intervals**: Balance cleanup frequency with performance
- **Memory monitoring**: Track state size and growth patterns
- **LRU eviction**: Use max_size to enable automatic cleanup

## Troubleshooting

### Common Issues

1. **State not persisting**: Check TTL settings and storage backend
2. **High memory usage**: Reduce max_size or use persistent storage
3. **Slow performance**: Consider using in-memory storage or optimizing keys
4. **State corruption**: Implement proper error handling and validation

### Debugging Tips

```rust
// Enable debug logging
log::set_level(log::LevelFilter::Debug);

// Add state access logging
|event, state_access| async move {
    log::debug!("Processing event for key: {}", event.user_id);
    
    let state = state_access.get().await;
    log::debug!("Retrieved state: {:?}", state);
    
    // ... rest of processing
}
```

## Migration Guide

### From Stateless to Stateful

1. **Identify state requirements**: What data needs to be remembered?
2. **Choose storage backend**: Based on performance and persistence needs
3. **Design keys**: How to partition state effectively
4. **Implement stateful operations**: Replace stateless operations
5. **Test thoroughly**: Verify state persistence and cleanup

### From Other State Management Solutions

RS2's state management is designed to be familiar to users of other streaming frameworks:

- **Apache Flink**: Similar keyed state concepts
- **Apache Kafka Streams**: Familiar state store patterns
- **Apache Spark**: Similar stateful operations

## Custom State Backends

RS2 supports pluggable state storage backends. You can create your own custom backend by implementing the `StateStorage` trait and plugging it into the stateful stream operations. This allows you to use in-memory, Redis, or any other storage system for state management.

**How to create your own backend:**
- Implement the `StateStorage` trait for your backend (see `src/state/traits.rs`).
- Use the `with_custom_storage` or `custom_storage` method on `StateConfig` or `StateConfigBuilder` to provide your backend.
- Pass your custom config to any stateful stream operation (e.g., `stateful_map_rs2`).

For a complete example, see: [examples/custom_storage_example.rs](../examples/custom_storage_example.rs)

This example demonstrates:
- Implementing a custom in-memory backend with atomic update logic
- Simulating a Redis-like backend
- Using your backend with stateful stream operations

## Conclusion

State management is a powerful feature that enables complex streaming applications. By choosing the right storage backend, designing efficient keys, and following best practices, you can build scalable, performant stateful streaming applications with RS2.

For more examples and advanced usage patterns, see the [examples directory](../examples/) and [test suite](../tests/). 