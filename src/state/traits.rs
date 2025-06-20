use async_trait::async_trait;

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

impl Clone for StateError {
    fn clone(&self) -> Self {
        match self {
            StateError::Storage(s) => StateError::Storage(s.clone()),
            StateError::Validation(s) => StateError::Validation(s.clone()),
            StateError::Serialization(_) => {
                panic!("Cannot clone Serialization variant of StateError")
            }
        }
    }
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

impl<T> KeyExtractor<T> for FieldKeyExtractor {
    fn extract_key(&self, _event: &T) -> String {
        // This is a simplified version - in practice you'd use reflection or serde
        // to extract the actual field value
        format!("key_{}", self.field_name)
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
