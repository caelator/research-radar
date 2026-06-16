//! Research applicability evaluation backend.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::pricing::{cost_microunits, Usage};

#[derive(Debug, Clone)]
pub struct WeaknessContext {
    pub code: String,
    pub summary: String,
}

#[derive(Debug, Clone)]
pub struct Evaluation {
    pub applicability_hypothesis: String,
    pub suggested_experiment: String,
    pub refuted: bool,
    pub adjusted_confidence: f32,
    pub cost_microunits: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum EvaluateError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Proposal {
    pub applicability_hypothesis: String,
    pub suggested_experiment: String,
    #[serde(default, skip)]
    pub cost_microunits: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Critique {
    pub refuted: bool,
    pub reason: String,
    pub confidence_delta: f32,
    #[serde(default, skip)]
    pub cost_microunits: i64,
}

#[async_trait]
pub trait Evaluator: Send + Sync {
    async fn propose(
        &self,
        title: &str,
        summary: &str,
        content: &str,
        context: &[WeaknessContext],
    ) -> Result<Proposal, EvaluateError>;

    async fn critique(
        &self,
        title: &str,
        summary: &str,
        hypothesis: &str,
    ) -> Result<Critique, EvaluateError>;
}

pub async fn evaluate<E: Evaluator + ?Sized>(
    evaluator: &E,
    title: &str,
    summary: &str,
    content: &str,
    base_confidence: f32,
    context: &[WeaknessContext],
) -> Result<Evaluation, EvaluateError> {
    let proposal = evaluator.propose(title, summary, content, context).await?;
    let critique = evaluator
        .critique(title, summary, &proposal.applicability_hypothesis)
        .await?;

    let mut adjusted = (base_confidence + critique.confidence_delta).clamp(0.0, 1.0);
    if critique.refuted {
        adjusted = (adjusted * 0.5).clamp(0.0, 1.0);
    }

    Ok(Evaluation {
        applicability_hypothesis: proposal.applicability_hypothesis,
        suggested_experiment: proposal.suggested_experiment,
        refuted: critique.refuted,
        adjusted_confidence: adjusted,
        cost_microunits: proposal.cost_microunits + critique.cost_microunits,
    })
}

pub struct AnthropicEvaluator {
    api_key: String,
    model: String,
    client: reqwest::Client,
    base_url: String,
}

impl AnthropicEvaluator {
    pub fn new(api_key: String) -> Self {
        Self {
            api_key,
            model: "claude-sonnet-4-6".to_string(),
            client: reqwest::Client::new(),
            base_url: "https://api.anthropic.com".to_string(),
        }
    }

    pub fn with_model(mut self, model: String) -> Self {
        self.model = model;
        self
    }

    #[cfg(test)]
    fn with_base_url(mut self, url: String) -> Self {
        self.base_url = url;
        self
    }

    fn build_propose_prompt(
        title: &str,
        summary: &str,
        content: &str,
        context: &[WeaknessContext],
    ) -> String {
        let context_instruction = if context.is_empty() {
            "Hypothesize applicability to an autonomous, self-improving Rust governance system (Triumvirate).".to_string()
        } else {
            let weaknesses = context
                .iter()
                .map(|ctx| format!("[{}] {}", ctx.code, ctx.summary))
                .collect::<Vec<_>>()
                .join("\n");
            format!(
                "Target weakness context:\n{weaknesses}\nThe hypothesis MUST explicitly reference which weakness code(s) it addresses and mention the literal code string."
            )
        };

        format!(
            r#"You evaluate whether a research finding applies to a target system.

Title: {title}
Summary: {summary}
Content:
{content}

{context_instruction}

Respond with ONLY valid JSON (no markdown):
{{"applicability_hypothesis":"...","suggested_experiment":"..."}}"#,
            content = truncate(content, 2000),
        )
    }

    fn build_critique_prompt(title: &str, summary: &str, hypothesis: &str) -> String {
        format!(
            r#"You are an ADVERSARIAL critic whose job is to REFUTE the hypothesis.
Judge if it is actually applicable, non-trivial, and not already-standard.

Title: {title}
Summary: {summary}
Hypothesis: {hypothesis}

Respond with ONLY valid JSON (no markdown):
{{"refuted":<bool>,"reason":"...","confidence_delta":<float negative if weak>}}"#
        )
    }

    async fn call_anthropic<T: for<'de> Deserialize<'de>>(
        &self,
        prompt: String,
    ) -> Result<(T, i64), EvaluateError> {
        let body = serde_json::json!({
            "model": self.model,
            "max_tokens": 512,
            "messages": [{"role": "user", "content": prompt}]
        });

        let resp = self
            .client
            .post(format!("{}/v1/messages", self.base_url))
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| EvaluateError::Http(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(EvaluateError::Http(format!("{status}: {text}")));
        }

        let api_resp: AnthropicResponse = resp
            .json()
            .await
            .map_err(|e| EvaluateError::Parse(e.to_string()))?;

        let text = api_resp
            .content
            .first()
            .map(|c| c.text.as_str())
            .unwrap_or("");
        let cost = cost_microunits(&self.model, &api_resp.usage);

        let parsed = serde_json::from_str(text)
            .or_else(|_| {
                let cleaned = text
                    .trim()
                    .trim_start_matches("```json")
                    .trim_start_matches("```")
                    .trim_end_matches("```")
                    .trim();
                serde_json::from_str(cleaned)
            })
            .map_err(|e| {
                EvaluateError::Parse(format!("Failed to parse LLM output: {e}. Raw: {text}"))
            })?;

        Ok((parsed, cost))
    }
}

#[derive(Debug, Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContent>,
    #[serde(default)]
    usage: Usage,
}

#[derive(Debug, Deserialize)]
struct AnthropicContent {
    text: String,
}

#[async_trait]
impl Evaluator for AnthropicEvaluator {
    async fn propose(
        &self,
        title: &str,
        summary: &str,
        content: &str,
        context: &[WeaknessContext],
    ) -> Result<Proposal, EvaluateError> {
        let (mut proposal, cost) = self
            .call_anthropic::<Proposal>(Self::build_propose_prompt(
                title, summary, content, context,
            ))
            .await?;
        proposal.cost_microunits = cost;
        Ok(proposal)
    }

    async fn critique(
        &self,
        title: &str,
        summary: &str,
        hypothesis: &str,
    ) -> Result<Critique, EvaluateError> {
        let (mut critique, cost) = self
            .call_anthropic::<Critique>(Self::build_critique_prompt(title, summary, hypothesis))
            .await?;
        critique.cost_microunits = cost;
        Ok(critique)
    }
}

pub struct MockEvaluator;

#[async_trait]
impl Evaluator for MockEvaluator {
    async fn propose(
        &self,
        title: &str,
        _summary: &str,
        _content: &str,
        context: &[WeaknessContext],
    ) -> Result<Proposal, EvaluateError> {
        let applicability_hypothesis = if context.is_empty() {
            format!(
                "This applies to an autonomous, self-improving Rust governance system (Triumvirate): {title}"
            )
        } else {
            format!(
                "Addresses weakness {}: {} — {}",
                context[0].code, context[0].summary, title
            )
        };

        Ok(Proposal {
            applicability_hypothesis,
            suggested_experiment: format!("Run a small benchmark validating: {title}"),
            cost_microunits: 0,
        })
    }

    async fn critique(
        &self,
        _title: &str,
        _summary: &str,
        _hypothesis: &str,
    ) -> Result<Critique, EvaluateError> {
        Ok(Critique {
            refuted: false,
            reason: "Plausible and non-trivial".into(),
            confidence_delta: 0.1,
            cost_microunits: 0,
        })
    }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        s
    } else {
        let end = s[..max].rfind(' ').unwrap_or(max);
        &s[..end]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn mock_two_call_server(
        propose_body: &str,
        critique_body: &str,
    ) -> (tokio::task::JoinHandle<()>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let bodies = [propose_body.to_string(), critique_body.to_string()];

        let handle = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            for body in bodies {
                let (mut stream, _) = listener.accept().await.unwrap();

                let mut buf = vec![0u8; 8192];
                let _ = stream.read(&mut buf).await.unwrap();

                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(resp.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });

        (handle, base_url)
    }

    #[tokio::test]
    async fn mock_evaluator_empty_context_produces_hypothesis() {
        let evaluator = MockEvaluator;
        let result = evaluate(&evaluator, "title", "summary", "content", 0.6, &[])
            .await
            .unwrap();

        assert!(!result.applicability_hypothesis.is_empty());
        assert!(result.applicability_hypothesis.contains("Triumvirate"));
        assert!(!result.refuted);
        assert!((result.adjusted_confidence - 0.7).abs() < 1e-4);
    }

    #[tokio::test]
    async fn mock_evaluator_context_references_weakness_code() {
        let evaluator = MockEvaluator;
        let context = [WeaknessContext {
            code: "WK-AUTH-1".into(),
            summary: "missing token rotation".into(),
        }];

        let result = evaluate(&evaluator, "title", "summary", "content", 0.6, &context)
            .await
            .unwrap();

        assert!(result.applicability_hypothesis.contains("WK-AUTH-1"));
    }

    #[tokio::test]
    async fn refuting_critic_lowers_confidence() {
        struct RefutingEvaluator;

        #[async_trait]
        impl Evaluator for RefutingEvaluator {
            async fn propose(
                &self,
                _title: &str,
                _summary: &str,
                _content: &str,
                _context: &[WeaknessContext],
            ) -> Result<Proposal, EvaluateError> {
                Ok(Proposal {
                    applicability_hypothesis: "h".into(),
                    suggested_experiment: "e".into(),
                    cost_microunits: 0,
                })
            }

            async fn critique(
                &self,
                _title: &str,
                _summary: &str,
                _hypothesis: &str,
            ) -> Result<Critique, EvaluateError> {
                Ok(Critique {
                    refuted: true,
                    reason: "already standard".into(),
                    confidence_delta: -0.3,
                    cost_microunits: 0,
                })
            }
        }

        let result = evaluate(&RefutingEvaluator, "title", "summary", "content", 0.8, &[])
            .await
            .unwrap();

        assert!(result.refuted);
        assert!((result.adjusted_confidence - 0.25).abs() < 1e-4);
    }

    #[tokio::test]
    async fn anthropic_evaluator_two_pass_valid_flow() {
        let propose_inner = serde_json::json!({
            "applicability_hypothesis": "applies to Triumvirate governance loop",
            "suggested_experiment": "benchmark X"
        })
        .to_string();
        let critique_inner = serde_json::json!({
            "refuted": false,
            "reason": "non-trivial",
            "confidence_delta": 0.05
        })
        .to_string();
        let propose_body = serde_json::json!({
            "content":[{"type":"text","text": propose_inner}],
            "usage": {"input_tokens": 100, "output_tokens": 50}
        });
        let critique_body = serde_json::json!({
            "content":[{"type":"text","text": critique_inner}],
            "usage": {"input_tokens": 100, "output_tokens": 50}
        });

        let (server, base_url) =
            mock_two_call_server(&propose_body.to_string(), &critique_body.to_string()).await;

        let evaluator = AnthropicEvaluator::new("k".into()).with_base_url(base_url);
        let result = evaluate(&evaluator, "title", "summary", "content", 0.5, &[])
            .await
            .unwrap();

        assert_eq!(
            result.applicability_hypothesis,
            "applies to Triumvirate governance loop"
        );
        assert_eq!(result.suggested_experiment, "benchmark X");
        assert!(!result.refuted);
        assert!((result.adjusted_confidence - 0.55).abs() < 1e-4);
        assert_eq!(result.cost_microunits, 2100);

        server.await.unwrap();
    }

    #[tokio::test]
    async fn anthropic_evaluator_markdown_fallback() {
        let propose_inner = format!(
            "```json\n{}\n```",
            serde_json::json!({
                "applicability_hypothesis": "parsed from markdown",
                "suggested_experiment": "benchmark Y"
            })
        );
        let critique_inner = serde_json::json!({
            "refuted": false,
            "reason": "non-trivial",
            "confidence_delta": 0.05
        })
        .to_string();
        let propose_body = serde_json::json!({"content":[{"type":"text","text": propose_inner}]});
        let critique_body = serde_json::json!({"content":[{"type":"text","text": critique_inner}]});

        let (server, base_url) =
            mock_two_call_server(&propose_body.to_string(), &critique_body.to_string()).await;

        let evaluator = AnthropicEvaluator::new("k".into()).with_base_url(base_url);
        let result = evaluate(&evaluator, "title", "summary", "content", 0.5, &[])
            .await
            .unwrap();

        assert_eq!(result.applicability_hypothesis, "parsed from markdown");

        server.await.unwrap();
    }
}
