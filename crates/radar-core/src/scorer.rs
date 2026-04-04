use crate::error::{RadarError, Result};
use crate::ranking::RankedCandidate;
use crate::types::Profile;

/// Result of scoring a single candidate via an LLM backend.
#[derive(Debug, Clone)]
pub struct ScoreResult {
    pub score: f64,                // 0.0 to 1.0
    pub reason_short: String,      // one-line summary
    pub rationale: String,         // full explanation
    pub llm_spend_microunits: u64, // cost of this call
}

/// Trait for LLM scoring backends (enables testing with mocks).
#[async_trait::async_trait]
pub trait LlmBackend: Send + Sync {
    async fn score(&self, prompt: &str) -> Result<ScoreResult>;
}

/// Mock backend for testing. Always returns a fixed score.
pub struct MockLlmBackend {
    pub default_score: f64,
    pub spend_per_call: u64,
    /// If set, the first N calls will fail with a parse error (for retry testing).
    pub fail_first_n: std::sync::atomic::AtomicU32,
}

impl MockLlmBackend {
    pub fn new(default_score: f64) -> Self {
        Self {
            default_score,
            spend_per_call: 100,
            fail_first_n: std::sync::atomic::AtomicU32::new(0),
        }
    }

    pub fn with_failures(default_score: f64, fail_first_n: u32) -> Self {
        Self {
            default_score,
            spend_per_call: 100,
            fail_first_n: std::sync::atomic::AtomicU32::new(fail_first_n),
        }
    }
}

#[async_trait::async_trait]
impl LlmBackend for MockLlmBackend {
    async fn score(&self, _prompt: &str) -> Result<ScoreResult> {
        let remaining = self
            .fail_first_n
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |v| if v > 0 { Some(v - 1) } else { None },
            )
            .unwrap_or(0);

        if remaining > 0 {
            return Err(RadarError::ScorerParse("mock parse failure".into()));
        }

        Ok(ScoreResult {
            score: self.default_score,
            reason_short: "Mock score".into(),
            rationale: "This is a mock score for testing purposes.".into(),
            llm_spend_microunits: self.spend_per_call,
        })
    }
}

/// Anthropic API backend (real implementation).
pub struct AnthropicBackend {
    pub api_key: String,
    pub model: String,
    client: reqwest::Client,
}

impl AnthropicBackend {
    pub fn new(api_key: String, model: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            api_key,
            model,
        }
    }
}

#[async_trait::async_trait]
impl LlmBackend for AnthropicBackend {
    async fn score(&self, prompt: &str) -> Result<ScoreResult> {
        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": 512,
            "messages": [{"role": "user", "content": prompt}]
        });

        let resp = self
            .client
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| RadarError::SourceTransient {
                source_name: "anthropic".into(),
                message: e.to_string(),
            })?;

        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after = resp
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(30);
            return Err(RadarError::RateLimited {
                source_name: "anthropic".into(),
                retry_after_secs: retry_after,
            });
        }

        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(RadarError::SourceTransient {
                source_name: "anthropic".into(),
                message: format!("HTTP {status}: {text}"),
            });
        }

        let resp_json: serde_json::Value = resp.json().await.map_err(|e| {
            RadarError::ScorerParse(format!("failed to parse Anthropic response: {e}"))
        })?;

        // Extract text from the content blocks
        let text = resp_json["content"]
            .as_array()
            .and_then(|arr| arr.iter().find(|b| b["type"] == "text"))
            .and_then(|b| b["text"].as_str())
            .ok_or_else(|| {
                RadarError::ScorerParse("no text content in Anthropic response".into())
            })?;

        // Extract usage for spend tracking
        let input_tokens = resp_json["usage"]["input_tokens"].as_u64().unwrap_or(0);
        let output_tokens = resp_json["usage"]["output_tokens"].as_u64().unwrap_or(0);
        // Approximate cost in microunits (1 microunit ≈ $0.000001)
        // Claude Haiku: ~$0.25/M input, ~$1.25/M output
        let spend = (input_tokens * 25 + output_tokens * 125) / 100;

        // Parse the JSON from the text — handle markdown code fences
        let json_str = extract_json(text);
        let parsed: serde_json::Value = serde_json::from_str(json_str).map_err(|e| {
            RadarError::ScorerParse(format!("invalid JSON in response: {e}\nraw: {text}"))
        })?;

        let score = parsed["score"]
            .as_f64()
            .ok_or_else(|| RadarError::ScorerParse("missing 'score' field".into()))?;
        let reason_short = parsed["reason_short"].as_str().unwrap_or("").to_string();
        let rationale = parsed["rationale"].as_str().unwrap_or("").to_string();

        Ok(ScoreResult {
            score,
            reason_short,
            rationale,
            llm_spend_microunits: spend,
        })
    }
}

/// Extract JSON from text that may be wrapped in markdown code fences.
fn extract_json(text: &str) -> &str {
    let trimmed = text.trim();
    if let Some(start) = trimmed.find('{')
        && let Some(end) = trimmed.rfind('}')
    {
        return &trimmed[start..=end];
    }
    trimmed
}

const MAX_RETRIES: u32 = 2;

/// Maximum abstract length before truncation (chars). Prevents context blowout.
const MAX_ABSTRACT_CHARS: usize = 4000;

/// Default scoring prompt template. `{title}`, `{abstract}`, and `{profile_description}`
/// are replaced at call time.
const DEFAULT_SCORING_PROMPT: &str = r#"You are a research paper relevance scorer. Rate the following paper's relevance to the research profile on a scale of 0.0 to 1.0.

Research profile: {profile_description}

Paper title: {title}
Paper abstract: {abstract}

Respond in exactly this JSON format:
{"score": <float 0.0-1.0>, "reason_short": "<one line>", "rationale": "<explanation>"}"#;

/// Main scorer that coordinates batch scoring with budget enforcement.
pub struct LlmScorer<B: LlmBackend> {
    backend: B,
    max_spend_microunits: u64,
}

impl<B: LlmBackend> LlmScorer<B> {
    pub fn new(backend: B, max_spend_microunits: u64) -> Self {
        Self {
            backend,
            max_spend_microunits,
        }
    }

    /// Score a batch of ranked candidates against a profile, enforcing budget.
    ///
    /// Returns `(candidate_index, ScoreResult)` pairs for each successfully scored candidate.
    /// Stops early if budget would be exceeded.
    pub async fn score_batch(
        &self,
        candidates: &[RankedCandidate],
        profile: &Profile,
        current_spend: u64,
    ) -> Result<Vec<(usize, ScoreResult)>> {
        // Estimate cost: assume each call costs roughly the same as the backend reports.
        // We do a conservative pre-check for the full batch.
        let estimated_per_call = 200_u64; // conservative estimate in microunits
        let estimated_batch_cost = estimated_per_call * candidates.len() as u64;

        if current_spend.saturating_add(estimated_batch_cost) > self.max_spend_microunits {
            return Err(RadarError::BudgetExhausted {
                message: format!(
                    "batch of {} candidates would exceed budget: current_spend={}, estimated_cost={}, max={}",
                    candidates.len(),
                    current_spend,
                    estimated_batch_cost,
                    self.max_spend_microunits,
                ),
            });
        }

        let prompt_template = profile
            .llm_scoring_prompt
            .as_deref()
            .unwrap_or(DEFAULT_SCORING_PROMPT);

        let profile_desc = profile.description.as_deref().unwrap_or(&profile.name);

        let mut results = Vec::with_capacity(candidates.len());
        let mut cumulative_spend = current_spend;

        for (idx, rc) in candidates.iter().enumerate() {
            let abstract_text = rc.candidate.abstract_text.as_deref().unwrap_or("");
            let truncated = truncate_abstract(abstract_text);
            let prompt = build_prompt(
                prompt_template,
                &rc.candidate.title,
                truncated,
                profile_desc,
            );

            let result = score_with_retries(&self.backend, &prompt, MAX_RETRIES).await?;
            cumulative_spend = cumulative_spend.saturating_add(result.llm_spend_microunits);

            // Check budget after each call
            if cumulative_spend > self.max_spend_microunits {
                // Include this result but stop after
                results.push((idx, result));
                return Err(RadarError::BudgetExhausted {
                    message: format!(
                        "budget exceeded mid-batch at candidate {}: spend={}, max={}",
                        idx, cumulative_spend, self.max_spend_microunits,
                    ),
                });
            }

            results.push((idx, result));
        }

        Ok(results)
    }
}

/// Truncate abstract to prevent context window blowout.
fn truncate_abstract(text: &str) -> &str {
    if text.len() <= MAX_ABSTRACT_CHARS {
        text
    } else {
        // Find a word boundary near the limit
        match text[..MAX_ABSTRACT_CHARS].rfind(' ') {
            Some(pos) => &text[..pos],
            None => &text[..MAX_ABSTRACT_CHARS],
        }
    }
}

/// Build a scoring prompt by substituting template variables.
fn build_prompt(
    template: &str,
    title: &str,
    abstract_text: &str,
    profile_description: &str,
) -> String {
    template
        .replace("{title}", title)
        .replace("{abstract}", abstract_text)
        .replace("{profile_description}", profile_description)
}

/// Call the backend with retry logic on parse failures.
async fn score_with_retries<B: LlmBackend>(
    backend: &B,
    prompt: &str,
    max_retries: u32,
) -> Result<ScoreResult> {
    let mut last_err = None;
    for _attempt in 0..=max_retries {
        match backend.score(prompt).await {
            Ok(result) => return Ok(result),
            Err(RadarError::ScorerParse(msg)) => {
                last_err = Some(RadarError::ScorerParse(msg));
                continue;
            }
            Err(e) => return Err(e), // non-parse errors are not retried
        }
    }
    Err(last_err.unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ranking::RankedCandidate;
    use crate::types::{SourceCandidate, SourceType};

    fn make_ranked(title: &str, abstract_text: &str) -> RankedCandidate {
        RankedCandidate {
            candidate: SourceCandidate {
                canonical_id: format!("test:{title}"),
                title: title.into(),
                authors: None,
                abstract_text: Some(abstract_text.into()),
                url: "http://example.com".into(),
                published_at: None,
                source_type: SourceType::Arxiv,
                aliases: vec![],
                raw_json: None,
            },
            rank_score: 0.8,
        }
    }

    fn make_profile() -> Profile {
        let mut p = Profile::new("Test Profile".into());
        p.description = Some("AI research on transformers and attention".into());
        p.keywords = vec!["transformer".into()];
        p
    }

    #[tokio::test]
    async fn test_scorer_mock_batch() {
        let backend = MockLlmBackend::new(0.85);
        let scorer = LlmScorer::new(backend, 100_000);
        let candidates = vec![
            make_ranked("Paper A", "About transformers"),
            make_ranked("Paper B", "About attention"),
        ];
        let profile = make_profile();

        let results = scorer.score_batch(&candidates, &profile, 0).await.unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, 0);
        assert_eq!(results[1].0, 1);
        assert!((results[0].1.score - 0.85).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_scorer_budget_exhaustion() {
        let backend = MockLlmBackend::new(0.5);
        // Budget is very tight: only enough for estimated cost of 0 candidates
        let scorer = LlmScorer::new(backend, 100);
        let candidates = vec![
            make_ranked("Paper A", "text"),
            make_ranked("Paper B", "text"),
            make_ranked("Paper C", "text"),
        ];
        let profile = make_profile();

        // current_spend=0, but estimated_batch_cost = 200 * 3 = 600 > 100
        let result = scorer.score_batch(&candidates, &profile, 0).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RadarError::BudgetExhausted { message } => {
                assert!(message.contains("exceed budget"));
            }
            other => panic!("Expected BudgetExhausted, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_scorer_retry_on_failure() {
        // Backend fails the first 2 calls, then succeeds. With max_retries=2 we
        // should eventually get a result.
        let backend = MockLlmBackend::with_failures(0.9, 2);
        let scorer = LlmScorer::new(backend, 100_000);
        let candidates = vec![make_ranked("Paper A", "text")];
        let profile = make_profile();

        let results = scorer.score_batch(&candidates, &profile, 0).await.unwrap();
        assert_eq!(results.len(), 1);
        assert!((results[0].1.score - 0.9).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn test_scorer_retry_exhausted() {
        // Backend fails 4 times — more than max_retries (2) + 1 initial = 3 attempts
        let backend = MockLlmBackend::with_failures(0.9, 4);
        let scorer = LlmScorer::new(backend, 100_000);
        let candidates = vec![make_ranked("Paper A", "text")];
        let profile = make_profile();

        let result = scorer.score_batch(&candidates, &profile, 0).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            RadarError::ScorerParse(_) => {}
            other => panic!("Expected ScorerParse, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_build_prompt() {
        let prompt = build_prompt(
            "Title: {title}\nAbstract: {abstract}\nProfile: {profile_description}",
            "My Paper",
            "Some abstract",
            "AI Research",
        );
        assert!(prompt.contains("My Paper"));
        assert!(prompt.contains("Some abstract"));
        assert!(prompt.contains("AI Research"));
    }

    #[test]
    fn test_extract_json() {
        assert_eq!(
            extract_json("```json\n{\"score\": 0.8}\n```"),
            "{\"score\": 0.8}"
        );
        assert_eq!(extract_json("{\"score\": 0.8}"), "{\"score\": 0.8}");
        assert_eq!(
            extract_json("Here is the result: {\"score\": 0.8} done"),
            "{\"score\": 0.8}"
        );
    }

    #[test]
    fn test_truncate_abstract() {
        let short = "Short abstract";
        assert_eq!(truncate_abstract(short), short);

        let long = "word ".repeat(2000); // 10000 chars
        let truncated = truncate_abstract(&long);
        assert!(truncated.len() <= MAX_ABSTRACT_CHARS);
    }
}
