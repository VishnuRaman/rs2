use async_trait::async_trait;
use serde::Serialize;
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
///
/// Extraction can fail — a field may be missing, or the event may not
/// serialize. Returning a `Result` keeps those cases visible: an earlier
/// version returned a sentinel string like `"missing_field_user_id"`, so every
/// malformed event silently shared one state bucket and corrupted whatever
/// accumulated there.
pub trait KeyExtractor<T> {
    fn extract_key(&self, event: &T) -> StateResult<String>;
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
where
    T: Serialize,
{
    fn extract_key(&self, event: &T) -> StateResult<String> {
        let value = serde_json::to_value(event)?;

        // Support nested field paths
        let field_value = if self.field_name.contains('.') {
            self.extract_nested_field(&value)
        } else {
            value.get(&self.field_name)
        };

        match field_value {
            Some(Value::String(s)) => Ok(s.clone()),
            Some(Value::Number(n)) => Ok(n.to_string()),
            Some(Value::Bool(b)) => Ok(b.to_string()),
            Some(Value::Null) => Ok("null".to_string()),
            Some(complex @ (Value::Array(_) | Value::Object(_))) => {
                Ok(serde_json::to_string(complex)?)
            }
            None => Err(StateError::Validation(format!(
                "key field `{}` is missing from the event",
                self.field_name
            ))),
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
    fn extract_key(&self, event: &T) -> StateResult<String> {
        Ok((self.extractor)(event))
    }
}

/// Key extractor whose function can itself fail.
///
/// Use this when deriving the key can go wrong in a way the caller wants to
/// surface, rather than [`CustomKeyExtractor`], which is infallible.
#[derive(Clone)]
pub struct TryKeyExtractor<F> {
    extractor: F,
}

impl<F> TryKeyExtractor<F> {
    pub fn new(extractor: F) -> Self {
        Self { extractor }
    }
}

impl<T, F> KeyExtractor<T> for TryKeyExtractor<F>
where
    F: Fn(&T) -> StateResult<String> + Clone,
{
    fn extract_key(&self, event: &T) -> StateResult<String> {
        (self.extractor)(event)
    }
}
