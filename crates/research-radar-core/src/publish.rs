use crate::evaluate::{evaluate, Evaluator, MockEvaluator, WeaknessContext};
use crate::feedback::FeedbackConfig;
use crate::finding::{Finding, UrgencyLevel};
use crate::storage::DbPool;
use crate::triumvirate::{
    resolve_weakness_link, to_weakness_contexts, PublishError, TriumviratePublisher,
    TriumvirateWeaknesses,
};

pub const DEFAULT_MIN_CONFIDENCE: f32 = 0.8;
pub const DEFAULT_MAX_PER_CYCLE: usize = 5;

#[derive(Debug, Clone)]
pub struct PublishConfig {
    pub base_url: String,
    pub project_id: String,
    pub auth_token: Option<String>,
    pub min_confidence: f32,
    pub max_per_cycle: usize,
}

impl PublishConfig {
    /// Reuse the SAME RADAR_TRIUMVIRATE_* env the feedback poller uses; None when unconfigured.
    pub fn from_env() -> Option<PublishConfig> {
        let fb = FeedbackConfig::from_env()?;
        let min_confidence = std::env::var("RADAR_PUBLISH_MIN_CONFIDENCE")
            .ok()
            .filter(|v| !v.is_empty())
            .and_then(|v| v.parse::<f32>().ok())
            .unwrap_or(DEFAULT_MIN_CONFIDENCE);
        let max_per_cycle = std::env::var("RADAR_PUBLISH_MAX_PER_CYCLE")
            .ok()
            .filter(|v| !v.is_empty())
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(DEFAULT_MAX_PER_CYCLE);
        Some(PublishConfig {
            base_url: fb.base_url,
            project_id: fb.project_id,
            auth_token: fb.auth_token,
            min_confidence,
            max_per_cycle,
        })
    }
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PublishCycleSummary {
    pub candidates: usize,
    pub published: usize,
    pub skipped_dup: usize,
    pub skipped_gate: usize,
    pub capped: bool,
    pub errors: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishCycleError {
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    #[error("fetch weaknesses error: {0}")]
    Fetch(#[from] PublishError),
}

pub fn should_publish(finding: &Finding, min_confidence: f32) -> bool {
    finding.confidence >= min_confidence && finding.urgency >= UrgencyLevel::High
}

pub struct AutoPublisher<E: Evaluator = MockEvaluator> {
    publisher: TriumviratePublisher,
    pool: DbPool,
    evaluator: E,
    min_confidence: f32,
    max_per_cycle: usize,
}

impl AutoPublisher<MockEvaluator> {
    /// Build from config with the deterministic MockEvaluator (used by the live worker
    /// when no LLM evaluator is wired, and by tests).
    pub fn from_config(config: &PublishConfig, pool: DbPool) -> Self {
        let publisher = TriumviratePublisher::new(
            config.base_url.clone(),
            config.project_id.clone(),
            config.auth_token.clone(),
        );
        Self {
            publisher,
            pool,
            evaluator: MockEvaluator,
            min_confidence: config.min_confidence,
            max_per_cycle: config.max_per_cycle,
        }
    }
}

impl<E: Evaluator> AutoPublisher<E> {
    #[cfg(test)]
    pub(crate) fn with_parts(
        publisher: TriumviratePublisher,
        pool: DbPool,
        evaluator: E,
        min_confidence: f32,
        max_per_cycle: usize,
    ) -> Self {
        Self {
            publisher,
            pool,
            evaluator,
            min_confidence,
            max_per_cycle,
        }
    }

    #[cfg(test)]
    pub(crate) fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Run one auto-publish pass over the findings produced this cycle.
    /// - fetch_weaknesses ONCE, cache; build weakness_contexts via to_weakness_contexts.
    /// - for each finding: skip if already published (dedupe by finding_id);
    ///   gate on raw confidence; run evaluate() to enrich + adjust; re-gate on adjusted
    ///   confidence; resolve_weakness_link; publish; record_published_finding on success.
    /// - enforce max_per_cycle cap. Non-fatal per-finding errors increment summary.errors.
    #[tracing::instrument(skip(self, findings), fields(input = findings.len()))]
    pub async fn publish_cycle(
        &self,
        findings: &[Finding],
    ) -> Result<PublishCycleSummary, PublishCycleError> {
        let mut summary = PublishCycleSummary::default();

        let weaknesses: TriumvirateWeaknesses = self.publisher.fetch_weaknesses().await?;
        let contexts: Vec<WeaknessContext> = to_weakness_contexts(&weaknesses);

        let already: std::collections::HashSet<String> = self.pool.published_finding_ids()?;

        for finding in findings {
            if summary.published >= self.max_per_cycle {
                summary.capped = true;
                break;
            }
            if !should_publish(finding, self.min_confidence) {
                summary.skipped_gate += 1;
                continue;
            }
            summary.candidates += 1;
            if already.contains(&finding.id) {
                summary.skipped_dup += 1;
                continue;
            }
            let content = if finding.summary.is_empty() {
                finding.title.as_str()
            } else {
                finding.summary.as_str()
            };
            let eval = match evaluate(
                &self.evaluator,
                &finding.title,
                &finding.summary,
                content,
                finding.confidence,
                &contexts,
            )
            .await
            {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("auto-publish: evaluate failed for {}: {e}", finding.id);
                    summary.errors += 1;
                    continue;
                }
            };

            let mut enriched = finding.clone();
            enriched.applicability_hypothesis = eval.applicability_hypothesis.clone();
            enriched.suggested_experiment = Some(eval.suggested_experiment.clone());
            enriched.confidence = eval.adjusted_confidence;

            if !should_publish(&enriched, self.min_confidence) {
                summary.skipped_gate += 1;
                continue;
            }

            let weakness_code: Option<String> = resolve_weakness_link(
                &enriched.domain,
                &enriched.applicability_hypothesis,
                &weaknesses,
            )
            .map(|s| s.to_string());

            match self
                .publisher
                .publish(&enriched, weakness_code.as_deref())
                .await
            {
                Ok(outcome) => {
                    if let Err(e) = self.pool.record_published_finding(
                        &enriched.id,
                        &outcome.obligation_ids,
                        enriched.source_type.as_str(),
                        &enriched.domain,
                        enriched.confidence as f64,
                        enriched.novelty_score as f64,
                        weakness_code.as_deref(),
                    ) {
                        tracing::warn!(
                            "auto-publish: record_published_finding failed for {}: {e}",
                            enriched.id
                        );
                        summary.errors += 1;
                    } else {
                        summary.published += 1;
                    }
                }
                Err(e) => {
                    tracing::warn!("auto-publish: publish failed for {}: {e}", enriched.id);
                    summary.errors += 1;
                }
            }
        }
        if summary.published >= self.max_per_cycle {
            summary.capped = true;
        }

        tracing::info!(
            published = summary.published,
            candidates = summary.candidates,
            skipped_dup = summary.skipped_dup,
            skipped_gate = summary.skipped_gate,
            capped = summary.capped,
            errors = summary.errors,
            "auto-publish: cycle complete"
        );
        Ok(summary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SourceType;
    use tokio::task::JoinHandle;

    const CANNED_WEAKNESSES_JSON: &str = r#"{"generated_at":"2026-06-15T10:00:00+00:00","open_obligations":[{"weakness_code":"gate_rejected","severity":"high","count":1,"domains":["d"],"latest_summary":"x","last_observed_at":null}],"ledger_weaknesses":[]}"#;

    async fn mock_publish_server(max_requests: usize) -> (JoinHandle<()>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let handle = tokio::spawn(async move {
            for _ in 0..max_requests {
                let (mut stream, _) = listener.accept().await.unwrap();
                use tokio::io::{AsyncReadExt, AsyncWriteExt};

                let mut buf = vec![0u8; 8192];
                let n = stream.read(&mut buf).await.unwrap();
                let request = String::from_utf8_lossy(&buf[..n]);
                let body = if request.contains("/v1/research/weaknesses") {
                    CANNED_WEAKNESSES_JSON
                } else if request.contains("/v1/research/findings") {
                    r#"{"obligation_ids":["obl-1"]}"#
                } else {
                    r#"{"error":"not found"}"#
                };
                let status = if request.contains("/v1/research/weaknesses")
                    || request.contains("/v1/research/findings")
                {
                    200
                } else {
                    404
                };
                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });

        (handle, base_url)
    }

    fn finding(id: &str, conf: f32, urgency: UrgencyLevel) -> Finding {
        let mut finding = Finding::new(
            "https://example.com/source".into(),
            "Example Source".into(),
            SourceType::Web,
            "d".into(),
            "Example finding".into(),
            "Example summary".into(),
            "Try the example change".into(),
            vec!["example".into()],
        );
        finding.id = id.into();
        finding.confidence = conf;
        finding.urgency = urgency;
        finding.novelty_score = 0.5;
        finding
    }

    async fn run_with_server(
        findings: &[Finding],
        pool: DbPool,
        min_confidence: f32,
        max_per_cycle: usize,
        max_requests: usize,
    ) -> (
        AutoPublisher<MockEvaluator>,
        PublishCycleSummary,
        JoinHandle<()>,
    ) {
        let (server, base_url) = mock_publish_server(max_requests).await;
        let publisher = TriumviratePublisher::new(base_url, "proj-x".into(), None);
        let auto = AutoPublisher::with_parts(
            publisher,
            pool,
            MockEvaluator,
            min_confidence,
            max_per_cycle,
        );
        let summary = auto.publish_cycle(findings).await.unwrap();
        (auto, summary, server)
    }

    #[tokio::test]
    async fn published_when_high_conf_high_urgency() {
        let pool = DbPool::test_pool().unwrap();
        let findings = vec![finding("f1", 0.9, UrgencyLevel::High)];
        let (auto, summary, server) = run_with_server(&findings, pool, 0.8, 5, 2).await;

        server.await.unwrap();
        assert_eq!(summary.published, 1);
        let rows = auto.pool().outcome_rows().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].finding_id, "f1");
    }

    #[tokio::test]
    async fn not_published_low_conf() {
        let pool = DbPool::test_pool().unwrap();
        let findings = vec![finding("f1", 0.7, UrgencyLevel::High)];
        let (auto, summary, server) = run_with_server(&findings, pool, 0.8, 5, 1).await;

        server.await.unwrap();
        assert_eq!(summary.published, 0);
        assert!(summary.skipped_gate >= 1);
        assert!(auto.pool().outcome_rows().unwrap().is_empty());
    }

    #[tokio::test]
    async fn not_published_medium_urgency() {
        let pool = DbPool::test_pool().unwrap();
        let findings = vec![finding("f1", 0.9, UrgencyLevel::Medium)];
        let (auto, summary, server) = run_with_server(&findings, pool, 0.8, 5, 1).await;

        server.await.unwrap();
        assert_eq!(summary.published, 0);
        assert!(auto.pool().outcome_rows().unwrap().is_empty());
    }

    #[tokio::test]
    async fn skips_already_published() {
        let pool = DbPool::test_pool().unwrap();
        pool.record_published_finding("f1", &[], "web", "d", 0.9, 0.5, None)
            .unwrap();
        let findings = vec![finding("f1", 0.9, UrgencyLevel::High)];
        let (auto, summary, server) = run_with_server(&findings, pool, 0.8, 5, 1).await;

        server.await.unwrap();
        assert_eq!(summary.published, 0);
        assert_eq!(summary.skipped_dup, 1);
        assert_eq!(auto.pool().outcome_rows().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn caps_at_max_per_cycle() {
        let pool = DbPool::test_pool().unwrap();
        let findings: Vec<_> = (0..7)
            .map(|idx| finding(&format!("f{idx}"), 0.9, UrgencyLevel::High))
            .collect();
        let (auto, summary, server) = run_with_server(&findings, pool, 0.8, 5, 6).await;

        server.await.unwrap();
        assert_eq!(summary.published, 5);
        assert!(summary.capped);
        assert_eq!(auto.pool().outcome_rows().unwrap().len(), 5);
    }

    #[test]
    fn unconfigured_is_noop() {
        std::env::remove_var("RADAR_TRIUMVIRATE_BASE_URL");
        std::env::remove_var("RADAR_TRIUMVIRATE_PROJECT_ID");
        assert!(PublishConfig::from_env().is_none());
    }
}
