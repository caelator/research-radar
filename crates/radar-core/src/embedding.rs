use crate::error::{RadarError, Result};

/// Trait for embedding backends.
#[async_trait::async_trait]
pub trait EmbeddingBackend: Send + Sync {
    /// Generate an embedding vector for the given text.
    async fn embed(&self, text: &str) -> Result<Vec<f32>>;

    /// Embedding dimensionality.
    fn dimensions(&self) -> usize;
}

/// OpenAI text-embedding-3-small backend (cheapest, most practical for v1).
pub struct OpenAIEmbeddingBackend {
    client: reqwest::Client,
    api_key: String,
    model: String,
}

impl OpenAIEmbeddingBackend {
    pub fn new(api_key: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
            model: "text-embedding-3-small".into(),
        }
    }

    pub fn with_model(api_key: String, model: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
            model,
        }
    }
}

#[async_trait::async_trait]
impl EmbeddingBackend for OpenAIEmbeddingBackend {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        let body = serde_json::json!({
            "model": self.model,
            "input": text,
        });

        let resp = self
            .client
            .post("https://api.openai.com/v1/embeddings")
            .header("Authorization", format!("Bearer {}", self.api_key))
            .json(&body)
            .send()
            .await
            .map_err(|e| RadarError::SourceTransient {
                source_name: "openai-embeddings".into(),
                message: e.to_string(),
            })?;

        if !resp.status().is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(RadarError::SourceTransient {
                source_name: "openai-embeddings".into(),
                message: format!("embedding API error: {text}"),
            });
        }

        let json: serde_json::Value = resp.json().await.map_err(|e| {
            RadarError::ScorerParse(format!("failed to parse embedding response: {e}"))
        })?;

        let embedding = json["data"][0]["embedding"]
            .as_array()
            .ok_or_else(|| RadarError::ScorerParse("missing embedding data".into()))?
            .iter()
            .map(|v| v.as_f64().unwrap_or(0.0) as f32)
            .collect();

        Ok(embedding)
    }

    fn dimensions(&self) -> usize {
        1536 // text-embedding-3-small default
    }
}

/// Mock embedding backend for testing. Returns deterministic vectors.
pub struct MockEmbeddingBackend {
    dims: usize,
}

impl MockEmbeddingBackend {
    pub fn new(dims: usize) -> Self {
        Self { dims }
    }
}

#[async_trait::async_trait]
impl EmbeddingBackend for MockEmbeddingBackend {
    async fn embed(&self, text: &str) -> Result<Vec<f32>> {
        // Simple deterministic hash-based embedding for testing
        let hash = text
            .bytes()
            .fold(0u64, |acc, b| acc.wrapping_mul(31).wrapping_add(b as u64));
        let mut vec = Vec::with_capacity(self.dims);
        let mut state = hash;
        for _ in 0..self.dims {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            vec.push(((state >> 33) as f32) / (u32::MAX as f32) - 0.5);
        }
        // Normalize
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in &mut vec {
                *v /= norm;
            }
        }
        Ok(vec)
    }

    fn dimensions(&self) -> usize {
        self.dims
    }
}

/// Build the text to embed for a research item (title + abstract).
pub fn embedding_text(title: &str, abstract_text: Option<&str>) -> String {
    match abstract_text {
        Some(abs) if !abs.is_empty() => format!("{}\n\n{}", title, abs),
        _ => title.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_embedding() {
        let backend = MockEmbeddingBackend::new(128);
        let vec = backend.embed("test text").await.unwrap();
        assert_eq!(vec.len(), 128);
        // Check it's normalized
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_mock_embedding_deterministic() {
        let backend = MockEmbeddingBackend::new(64);
        let v1 = backend.embed("same text").await.unwrap();
        let v2 = backend.embed("same text").await.unwrap();
        assert_eq!(v1, v2);
    }

    #[test]
    fn test_embedding_text() {
        assert_eq!(
            embedding_text("Title", Some("Abstract")),
            "Title\n\nAbstract"
        );
        assert_eq!(embedding_text("Title", None), "Title");
        assert_eq!(embedding_text("Title", Some("")), "Title");
    }
}
