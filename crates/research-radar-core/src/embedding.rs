//! Embedding backend trait — stub for Phase 2 readiness.
//!
//! Phase 1 stores findings in LanceDB without embeddings.
//! Phase 2 will wire in an embedding backend (OpenAI text-embedding-3-small)
//! to populate the vector column for semantic search.

use async_trait::async_trait;

/// A vector embedding (f32 array).
pub type Embedding = Vec<f32>;

/// Errors from the embedding backend.
#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("not configured")]
    NotConfigured,
}

/// Trait for embedding backends.
#[async_trait]
pub trait EmbeddingBackend: Send + Sync {
    /// Embed a single text string into a vector.
    async fn embed(&self, text: &str) -> Result<Embedding, EmbeddingError>;

    /// Embed multiple texts in a single batch call.
    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        let mut results = Vec::with_capacity(texts.len());
        for text in texts {
            results.push(self.embed(text).await?);
        }
        Ok(results)
    }

    /// Dimensionality of the embedding vectors produced by this backend.
    fn dimensions(&self) -> usize;
}

/// Stub backend that returns NotConfigured for all calls.
/// Used in Phase 1 when no embedding API key is available.
pub struct StubBackend;

#[async_trait]
impl EmbeddingBackend for StubBackend {
    async fn embed(&self, _text: &str) -> Result<Embedding, EmbeddingError> {
        Err(EmbeddingError::NotConfigured)
    }

    fn dimensions(&self) -> usize {
        1536 // text-embedding-3-small dimensionality
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stub_backend_returns_not_configured() {
        let backend = StubBackend;
        let result = backend.embed("test").await;
        assert!(matches!(result, Err(EmbeddingError::NotConfigured)));
    }

    #[test]
    fn stub_dimensions() {
        let backend = StubBackend;
        assert_eq!(backend.dimensions(), 1536);
    }
}
