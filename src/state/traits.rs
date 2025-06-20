use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Types of state storage backends
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateStorageType {
    InMemory,
    Custom, // For user-defined storage backends
}

/// Trait for state storage backends (object-safe version)
#[async_trait]
pub trait StateStorage {
    async fn get(&self, key: &str) -> Option<Vec<u8>>;
    async fn set(
        &self,
        key: &str,
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    async fn exists(&self, key: &str) -> bool;
    async fn clear(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// State management error types
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Validation error: {0}")]
    Validation(String),
}

pub type StateResult<T> = Result<T, StateError>;

/// Helper trait for extracting keys from events
pub trait KeyExtractor<T> {
    fn extract_key(&self, event: &T) -> String;
}

/// Default key extractor that uses a field name
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

impl<T> KeyExtractor<T> for FieldKeyExtractor 
where T: Serialize {
    fn extract_key(&self, event: &T) -> String {
        match serde_json::to_value(event) {
            Ok(value) => {
                // Support nested field paths
                let field_value = if self.field_name.contains('.') {
                    self.extract_nested_field(&value)
                } else {
                    value.get(&self.field_name)
                };
                
                match field_value {
                    Some(Value::String(s)) => s.clone(),
                    Some(Value::Number(n)) => n.to_string(),
                    Some(Value::Bool(b)) => b.to_string(),
                    Some(Value::Null) => "null".to_string(),
                    Some(Value::Array(_) | Value::Object(_)) => {
                        serde_json::to_string(field_value.unwrap())
                            .unwrap_or_else(|_| "invalid_complex_type".to_string())
                    }
                    None => format!("missing_field_{}", self.field_name),
                }
            }
            Err(e) => {
                format!("serialization_error_{}_{}", 
                    self.field_name, 
                    e.to_string().chars().take(10).collect::<String>())
            }
        }
    }
}

impl FieldKeyExtractor {
    /// Extract a nested field value using dot notation (e.g., "user.profile.id")
    fn extract_nested_field<'a>(&self, value: &'a Value) -> Option<&'a Value> {
        let parts: Vec<&str> = self.field_name.split('.').collect();
        let mut current = value;

        for part in parts {
            current = current.get(part)?;
        }

        Some(current)
    }

}

/// Custom key extractor function
#[derive(Clone)]
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
    F: Fn(&T) -> String + Clone,
{
    fn extract_key(&self, event: &T) -> String {
        (self.extractor)(event)
    }
}
