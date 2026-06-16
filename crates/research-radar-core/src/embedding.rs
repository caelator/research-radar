//! Embedding backend trait — stub for Phase 2 readiness.
//!
//! Phase 1 stores findings in LanceDB without embeddings.
//! Phase 2 will wire in an embedding backend (OpenAI text-embedding-3-small)
//! to populate the vector column for semantic search.

use async_trait::async_trait;
use serde::Deserialize;

/// A vector embedding (f32 array).
pub type Embedding = Vec<f32>;

/// Cosine similarity of two equal-length vectors. Returns 0.0 if either is
/// empty, lengths differ, or either has zero magnitude.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }

    let mut dot = 0.0;
    let mut a_norm_sq = 0.0;
    let mut b_norm_sq = 0.0;

    for (a_value, b_value) in a.iter().zip(b.iter()) {
        dot += a_value * b_value;
        a_norm_sq += a_value * a_value;
        b_norm_sq += b_value * b_value;
    }

    if a_norm_sq <= 0.0 || b_norm_sq <= 0.0 {
        return 0.0;
    }

    let similarity = dot / (a_norm_sq.sqrt() * b_norm_sq.sqrt());
    if similarity.is_nan() {
        0.0
    } else {
        similarity
    }
}

/// Novelty = 1.0 - max cosine similarity against any prior embedding.
/// Returns 1.0 when `priors` is empty (nothing to be similar to => fully novel).
/// Result is clamped to [0.0, 1.0].
pub fn compute_novelty(candidate: &[f32], priors: &[Embedding]) -> f32 {
    if priors.is_empty() {
        return 1.0;
    }

    let max_sim = priors
        .iter()
        .map(|prior| cosine_similarity(candidate, prior))
        .fold(f32::NEG_INFINITY, f32::max);

    (1.0 - max_sim).clamp(0.0, 1.0)
}

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

/// HTTP embedding backend compatible with Voyage/OpenAI-style embeddings APIs.
pub struct HttpEmbeddingBackend {
    api_key: String,
    model: String,
    base_url: String,
    dimensions: usize,
    client: reqwest::Client,
}

impl HttpEmbeddingBackend {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            model: "voyage-3.5".to_string(),
            base_url: "https://api.voyageai.com/v1".to_string(),
            dimensions: 1024,
            client: reqwest::Client::new(),
        }
    }

    pub fn with_model(mut self, model: String) -> Self {
        self.model = model;
        self
    }

    pub fn with_base_url(mut self, url: String) -> Self {
        self.base_url = url;
        self
    }

    pub fn with_dimensions(mut self, d: usize) -> Self {
        self.dimensions = d;
        self
    }

    pub fn from_env() -> Option<Self> {
        let api_key = std::env::var("RADAR_EMBED_API_KEY").ok()?;
        if api_key.is_empty() {
            return None;
        }

        let mut backend = Self::new(api_key);
        if let Ok(model) = std::env::var("RADAR_EMBED_MODEL") {
            if !model.is_empty() {
                backend = backend.with_model(model);
            }
        }
        if let Ok(base_url) = std::env::var("RADAR_EMBED_BASE_URL") {
            if !base_url.is_empty() {
                backend = backend.with_base_url(base_url);
            }
        }
        Some(backend)
    }

    fn endpoint(&self) -> String {
        if self.base_url.trim_end_matches('/').ends_with("/embeddings") {
            self.base_url.clone()
        } else {
            format!("{}/embeddings", self.base_url.trim_end_matches('/'))
        }
    }
}

#[derive(Debug, Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

#[async_trait]
impl EmbeddingBackend for HttpEmbeddingBackend {
    async fn embed(&self, text: &str) -> Result<Embedding, EmbeddingError> {
        self.embed_batch(&[text.to_string()])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| EmbeddingError::Http("empty embedding response".into()))
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        let body = serde_json::json!({
            "model": self.model,
            "input": texts,
        });

        let resp = self
            .client
            .post(self.endpoint())
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await
            .map_err(|e| EmbeddingError::Http(e.to_string()))?;

        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let status = resp.status();
            return Err(EmbeddingError::Http(format!("rate limited: {status}")));
        }

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(EmbeddingError::Http(format!("{status}: {text}")));
        }

        let api_resp: EmbeddingsResponse = resp
            .json()
            .await
            .map_err(|e| EmbeddingError::Http(e.to_string()))?;

        Ok(api_resp
            .data
            .into_iter()
            .map(|item| item.embedding)
            .collect())
    }

    fn dimensions(&self) -> usize {
        self.dimensions
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

    #[test]
    fn cosine_identical_is_one() {
        let similarity = cosine_similarity(&[1.0, 0.0, 0.0], &[1.0, 0.0, 0.0]);
        assert!((similarity - 1.0).abs() < 1e-6);
    }

    #[test]
    fn cosine_orthogonal_is_zero() {
        let similarity = cosine_similarity(&[1.0, 0.0], &[0.0, 1.0]);
        assert!(similarity.abs() < 1e-6);
    }

    #[test]
    fn novelty_identical_prior_is_zero() {
        let novelty = compute_novelty(&[1.0, 0.0, 0.0], &[vec![1.0, 0.0, 0.0]]);
        assert!(novelty < 1e-6);
    }

    #[test]
    fn novelty_orthogonal_prior_is_one() {
        let novelty = compute_novelty(&[1.0, 0.0], &[vec![0.0, 1.0]]);
        assert!((novelty - 1.0).abs() < 1e-6);
    }

    #[test]
    fn novelty_empty_priors_is_one() {
        let novelty = compute_novelty(&[1.0, 2.0, 3.0], &[]);
        assert_eq!(novelty, 1.0);
    }

    #[test]
    fn novelty_takes_max_similarity() {
        let priors = vec![vec![0.0, 1.0], vec![1.0, 0.0]];
        let novelty = compute_novelty(&[1.0, 0.0], &priors);
        assert!(novelty < 1e-6);
    }

    async fn mock_embedding_server(
        response_body: &str,
        status: u16,
    ) -> (tokio::task::JoinHandle<()>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let body = response_body.to_string();

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let mut buf = vec![0u8; 8192];
            let _ = stream.read(&mut buf).await.unwrap();

            let resp = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(resp.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        (handle, base_url)
    }

    #[tokio::test]
    async fn http_backend_parses_canned_response_in_order() {
        let response = r#"{"data":[{"embedding":[1.0,0.0,0.0]},{"embedding":[0.0,1.0,0.0]}]}"#;
        let (server, base_url) = mock_embedding_server(response, 200).await;

        let backend = HttpEmbeddingBackend::new("k".into()).with_base_url(base_url);
        let result = backend
            .embed_batch(&["a".into(), "b".into()])
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![1.0, 0.0, 0.0]);
        assert_eq!(result[1], vec![0.0, 1.0, 0.0]);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_backend_embed_single() {
        let response = r#"{"data":[{"embedding":[0.5,0.5]}]}"#;
        let (server, base_url) = mock_embedding_server(response, 200).await;

        let backend = HttpEmbeddingBackend::new("k".into()).with_base_url(base_url);
        let result = backend.embed("x").await.unwrap();

        assert_eq!(result, vec![0.5, 0.5]);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_backend_error_status_is_http_err() {
        let (server, base_url) = mock_embedding_server(r#"{"error":"bad"}"#, 400).await;

        let backend = HttpEmbeddingBackend::new("k".into()).with_base_url(base_url);
        let err = backend.embed("x").await.unwrap_err();

        match err {
            EmbeddingError::Http(msg) => assert!(msg.contains("400")),
            other => panic!("expected Http error, got: {other:?}"),
        }

        server.await.unwrap();
    }

    #[test]
    fn from_env_none_without_key() {
        // Global env mutation can race, so the verify command runs tests with one thread.
        let old_key = std::env::var("RADAR_EMBED_API_KEY").ok();

        std::env::set_var("RADAR_EMBED_API_KEY", "");
        assert!(HttpEmbeddingBackend::from_env().is_none());
        std::env::remove_var("RADAR_EMBED_API_KEY");
        assert!(HttpEmbeddingBackend::from_env().is_none());

        if let Some(old_key) = old_key {
            std::env::set_var("RADAR_EMBED_API_KEY", old_key);
        }
    }

    #[test]
    fn endpoint_appends_embeddings() {
        let backend = HttpEmbeddingBackend::new("k".into()).with_base_url("https://x/v1".into());
        assert!(backend.endpoint().ends_with("/v1/embeddings"));

        let backend =
            HttpEmbeddingBackend::new("k".into()).with_base_url("https://x/v1/embeddings".into());
        assert_eq!(backend.endpoint(), "https://x/v1/embeddings");
    }
}
