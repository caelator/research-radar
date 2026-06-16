//! Triumvirate integration payloads and publisher.
//!
//! This module is deliberately wire-only: it serializes research-radar findings
//! into the ADR-0040 HTTP/JSON contract and does not depend on any Triumvirate
//! crate.

use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::evaluate::WeaknessContext;
use crate::finding::{Finding, PaperRef};

const CONTRACT_VERSION: &str = "2026-03-10";
const PRODUCER: &str = "research-radar";
const FINDING_SCHEMA_VERSION: &str = "1.0";
const MAX_ATTEMPTS: usize = 3;
const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);
const LEDGER_WEAKNESS_CODE: &str = "external_research_signal";
const MAX_LEDGER_CONTEXTS: usize = 3;

#[derive(Debug, Clone, Deserialize)]
pub struct TriumvirateWeaknesses {
    pub generated_at: String,
    #[serde(default)]
    pub open_obligations: Vec<OpenObligationGroup>,
    #[serde(default)]
    pub ledger_weaknesses: Vec<LedgerWeaknessRow>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenObligationGroup {
    pub weakness_code: String,
    pub severity: String,
    pub count: u32,
    #[serde(default)]
    pub domains: Vec<String>,
    pub latest_summary: String,
    #[serde(default)]
    pub last_observed_at: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LedgerWeaknessRow {
    pub rank: u32,
    pub weakness: String,
    pub status: String,
    pub evidence: String,
    #[serde(default)]
    pub owning_task: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationDisposition {
    Favorable,
    Unfavorable,
    Pending,
}

/// Map a Minerva ImprovementObligation status (snake_case) to a disposition.
/// Favorable: applied, closed, authorized.
/// Unfavorable: rolled_back, blocked.
/// Everything else (open, candidate_proposed, revision_requested, adjudication_pending,
/// verification_pending, or unknown) -> Pending.
pub fn classify_obligation_status(status: &str) -> ObligationDisposition {
    match status.trim().to_lowercase().as_str() {
        "applied" | "closed" | "authorized" => ObligationDisposition::Favorable,
        "rolled_back" | "blocked" => ObligationDisposition::Unfavorable,
        _ => ObligationDisposition::Pending,
    }
}

pub fn to_weakness_contexts(weaknesses: &TriumvirateWeaknesses) -> Vec<WeaknessContext> {
    let mut contexts = Vec::new();
    for group in &weaknesses.open_obligations {
        let summary = if group.domains.is_empty() {
            group.latest_summary.clone()
        } else {
            format!(
                "{} (domains: {})",
                group.latest_summary,
                group.domains.join(", ")
            )
        };
        contexts.push(WeaknessContext {
            code: group.weakness_code.clone(),
            summary,
        });
    }
    // top-ranked ledger weaknesses (lowest rank number = highest priority)
    let mut ledger: Vec<&LedgerWeaknessRow> = weaknesses.ledger_weaknesses.iter().collect();
    ledger.sort_by_key(|row| row.rank);
    for row in ledger.into_iter().take(MAX_LEDGER_CONTEXTS) {
        contexts.push(WeaknessContext {
            code: LEDGER_WEAKNESS_CODE.to_string(),
            summary: row.weakness.clone(),
        });
    }
    contexts
}

pub fn resolve_weakness_link<'a>(
    finding_domain: &str,
    eval_hypothesis: &str,
    known: &'a TriumvirateWeaknesses,
) -> Option<&'a str> {
    let domain_lower = finding_domain.to_lowercase();
    let hypo_lower = eval_hypothesis.to_lowercase();
    for group in &known.open_obligations {
        // (a) domain match
        let domain_hit = group
            .domains
            .iter()
            .any(|d| d.to_lowercase() == domain_lower);
        // (b) hypothesis mentions the code or one of its words
        let code_lower = group.weakness_code.to_lowercase();
        let code_hit = hypo_lower.contains(&code_lower)
            || code_lower
                .split('_')
                .filter(|w| w.len() > 2)
                .any(|w| hypo_lower.contains(w));
        if domain_hit || code_hit {
            return Some(group.weakness_code.as_str());
        }
    }
    None
}

/// Publishes findings to a Triumvirate ADR-0040 HTTP endpoint.
pub struct TriumviratePublisher {
    base_url: String,
    project_id: String,
    auth_token: Option<String>,
    client: reqwest::Client,
}

impl TriumviratePublisher {
    pub fn new(base_url: String, project_id: String, auth_token: Option<String>) -> Self {
        Self {
            base_url,
            project_id,
            auth_token,
            client: reqwest::Client::new(),
        }
    }

    #[cfg(test)]
    fn with_base_url(mut self, url: String) -> Self {
        self.base_url = url;
        self
    }

    pub async fn fetch_weaknesses(&self) -> Result<TriumvirateWeaknesses, PublishError> {
        let url = format!(
            "{}/v1/research/weaknesses/{}",
            self.base_url, self.project_id
        );
        let mut request = self.client.get(&url);
        if let Some(token) = &self.auth_token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = request
            .send()
            .await
            .map_err(|e| PublishError::Http(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(PublishError::Http(format!("{status}: {body}")));
        }
        let body = response
            .text()
            .await
            .map_err(|e| PublishError::Http(e.to_string()))?;
        serde_json::from_str(&body).map_err(|e| PublishError::Parse(e.to_string()))
    }

    /// GET {base}/v1/learning/{project_id}/obligations/{obligation_id}
    /// Returns the obligation `status` string from the response envelope.
    pub async fn fetch_obligation_status(
        &self,
        obligation_id: &str,
    ) -> Result<String, PublishError> {
        let url = format!(
            "{}/v1/learning/{}/obligations/{}",
            self.base_url, self.project_id, obligation_id
        );
        let mut request = self.client.get(&url);
        if let Some(token) = &self.auth_token {
            request = request.header("Authorization", format!("Bearer {token}"));
        }
        let response = request
            .send()
            .await
            .map_err(|e| PublishError::Http(e.to_string()))?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(PublishError::Http(format!("{status}: {body}")));
        }
        let body = response
            .text()
            .await
            .map_err(|e| PublishError::Http(e.to_string()))?;
        extract_obligation_status(&body)
            .ok_or_else(|| PublishError::Parse("missing obligation status".into()))
    }

    pub fn build_payload(
        &self,
        finding: &Finding,
        weakness_code: Option<&str>,
        request_id: String,
    ) -> TriumviratePayload {
        let citation = finding.cited_paper.as_ref().map(Citation::from);
        let weakness_link = match weakness_code {
            Some(code) => WeaknessLink::StrengthenExisting {
                link: "strengthen_existing",
                weakness_code: code.to_string(),
            },
            None => WeaknessLink::ProposeCapabilityGap {
                link: "propose_capability_gap",
                gap_summary: finding.title.clone(),
            },
        };

        // Prefer the LLM-evaluated applicability hypothesis (ADR-0040 phase 2);
        // fall back to the composed summary string when evaluation was not run.
        let applicability_hypothesis = if finding.applicability_hypothesis.trim().is_empty() {
            format!(
                "{} — {} (domain: {})",
                finding.summary, finding.suggested_action, finding.domain
            )
        } else {
            finding.applicability_hypothesis.clone()
        };

        // Use the real cosine-based novelty carried on the finding. A value of
        // exactly 0.0 means novelty was never computed (embeddings unavailable),
        // so fall back to the neutral 0.5 placeholder in that case only.
        let novelty_score = if finding.novelty_score == 0.0 {
            0.5_f32
        } else {
            finding.novelty_score
        };

        TriumviratePayload {
            contract_version: CONTRACT_VERSION,
            producer: PRODUCER,
            request_id,
            project_id: self.project_id.clone(),
            sent_at: Utc::now().to_rfc3339(),
            finding: TriumvirateFindingPayload {
                finding_id: finding.id.clone(),
                schema_version: FINDING_SCHEMA_VERSION,
                source_url: finding.source_url.clone(),
                source_title: finding.source_title.clone(),
                source_kind: finding.source_type.as_str(),
                citation,
                domain: finding.domain.clone(),
                title: finding.title.clone(),
                summary: finding.summary.clone(),
                suggested_action: finding.suggested_action.clone(),
                applicability_hypothesis,
                applicability_tags: finding.applicability_tags.clone(),
                confidence: finding.confidence,
                impact_weight: finding.impact_weight,
                proposed_urgency: finding.urgency.as_str(),
                novelty_score,
                weakness_link,
                discovered_at: finding.discovered_at.to_rfc3339(),
            },
        }
    }

    #[tracing::instrument(skip(self, finding), fields(finding_id = %finding.id, weakness_code = weakness_code.unwrap_or("none")))]
    pub async fn publish(
        &self,
        finding: &Finding,
        weakness_code: Option<&str>,
    ) -> Result<PublishOutcome, PublishError> {
        let request_id = Uuid::new_v4().to_string();
        let payload = self.build_payload(finding, weakness_code, request_id.clone());
        serde_json::to_value(&payload).map_err(|e| PublishError::Serialize(e.to_string()))?;

        let url = format!("{}/v1/research/findings", self.base_url);

        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self.client.post(&url);
            if let Some(token) = &self.auth_token {
                request = request.header("Authorization", format!("Bearer {token}"));
            }

            let response = request
                .json(&payload)
                .send()
                .await
                .map_err(|e| PublishError::Http(e.to_string()))?;

            let status = response.status();
            if status.is_success() {
                let body = response.text().await.unwrap_or_default();
                let obligation_ids = extract_obligation_ids(&body);
                let outcome_status = status.as_u16();
                tracing::info!(
                    status = outcome_status,
                    obligation_count = obligation_ids.len(),
                    "triumvirate: finding published"
                );
                return Ok(PublishOutcome {
                    request_id,
                    status: outcome_status,
                    obligation_ids,
                });
            }

            let should_retry =
                status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error();
            if should_retry && attempt + 1 < MAX_ATTEMPTS {
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            }

            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after_secs = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(0);
                return Err(PublishError::RateLimited { retry_after_secs });
            }

            let body = response.text().await.unwrap_or_default();
            return Err(PublishError::Http(format!("{status}: {body}")));
        }

        unreachable!("publish attempts loop always returns")
    }
}

#[derive(Debug, Clone)]
pub struct PublishOutcome {
    pub request_id: String,
    pub status: u16,
    pub obligation_ids: Vec<String>,
}

fn extract_obligation_ids(body: &str) -> Vec<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return Vec::new();
    };

    fn push_string(ids: &mut Vec<String>, value: Option<&serde_json::Value>) {
        if let Some(id) = value.and_then(|value| value.as_str()) {
            if !id.is_empty() {
                ids.push(id.to_string());
            }
        }
    }

    fn push_string_array(ids: &mut Vec<String>, value: Option<&serde_json::Value>) {
        if let Some(values) = value.and_then(|value| value.as_array()) {
            for value in values {
                push_string(ids, Some(value));
            }
        }
    }

    let mut ids = Vec::new();
    push_string_array(&mut ids, value.get("obligation_ids"));
    push_string(&mut ids, value.get("obligation_id"));

    if let Some(data) = value.get("data") {
        push_string_array(&mut ids, data.get("obligation_ids"));
        push_string(&mut ids, data.get("obligation_id"));
    }

    if let Some(obligations) = value.get("obligations").and_then(|value| value.as_array()) {
        for obligation in obligations {
            push_string(&mut ids, obligation.get("id"));
        }
    }

    ids
}

fn extract_obligation_status(body: &str) -> Option<String> {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(body) else {
        return None;
    };

    value
        .get("status")
        .or_else(|| value.get("data").and_then(|data| data.get("status")))
        .or_else(|| {
            value
                .get("obligation")
                .and_then(|obligation| obligation.get("status"))
        })
        .and_then(|status| status.as_str())
        .map(|status| status.to_string())
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("rate limited, retry after {retry_after_secs}s")]
    RateLimited { retry_after_secs: u64 },
    #[error("serialize error: {0}")]
    Serialize(String),
    #[error("parse error: {0}")]
    Parse(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct TriumviratePayload {
    contract_version: &'static str,
    producer: &'static str,
    request_id: String,
    project_id: String,
    sent_at: String,
    finding: TriumvirateFindingPayload,
}

#[derive(Debug, Clone, Serialize)]
struct TriumvirateFindingPayload {
    finding_id: String,
    schema_version: &'static str,
    source_url: String,
    source_title: String,
    source_kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    citation: Option<Citation>,
    domain: String,
    title: String,
    summary: String,
    suggested_action: String,
    applicability_hypothesis: String,
    applicability_tags: Vec<String>,
    confidence: f32,
    impact_weight: f32,
    proposed_urgency: &'static str,
    novelty_score: f32,
    weakness_link: WeaknessLink,
    discovered_at: String,
}

#[derive(Debug, Clone, Serialize)]
struct Citation {
    title: String,
    authors: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    year: Option<u16>,
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    venue: Option<String>,
}

impl From<&PaperRef> for Citation {
    fn from(paper: &PaperRef) -> Self {
        Self {
            title: paper.title.clone(),
            authors: paper.authors.clone(),
            year: paper.year,
            url: paper.url.clone(),
            venue: paper.venue.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
enum WeaknessLink {
    ProposeCapabilityGap {
        link: &'static str,
        gap_summary: String,
    },
    StrengthenExisting {
        link: &'static str,
        weakness_code: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SourceType, UrgencyLevel};

    /// Canonical ADR-0040 research-finding wire document (propose_capability_gap),
    /// with sent_at/discovered_at pinned to fixed placeholder timestamps for a stable golden.
    pub(crate) const CANONICAL_RESEARCH_FINDING_JSON: &str = r#"{
  "contract_version": "2026-03-10",
  "finding": {
    "applicability_hypothesis": "Structured concurrency bounds task lifetimes to a parent scope, eliminating orphaned tasks and making cancellation deterministic. — Refactor the agent runtime to spawn child tasks inside a JoinSet owned by the supervising scope. (domain: rust-async)",
    "applicability_tags": [
      "async-runtime",
      "tokio",
      "task-supervision"
    ],
    "citation": {
      "authors": "A. Researcher and B. Engineer",
      "title": "Structured Concurrency for Resilient Agents",
      "url": "https://arxiv.org/abs/2603.01234",
      "venue": "ICLR",
      "year": 2026
    },
    "confidence": 0.8199999928474426,
    "discovered_at": "2026-03-10T08:30:00+00:00",
    "domain": "rust-async",
    "finding_id": "finding-golden-0001",
    "impact_weight": 0.6499999761581421,
    "novelty_score": 0.5,
    "proposed_urgency": "high",
    "schema_version": "1.0",
    "source_kind": "paper",
    "source_title": "Structured Concurrency for Resilient Agents",
    "source_url": "https://arxiv.org/abs/2603.01234",
    "suggested_action": "Refactor the agent runtime to spawn child tasks inside a JoinSet owned by the supervising scope.",
    "summary": "Structured concurrency bounds task lifetimes to a parent scope, eliminating orphaned tasks and making cancellation deterministic.",
    "title": "Adopt structured concurrency for agent task supervision",
    "weakness_link": {
      "gap_summary": "Adopt structured concurrency for agent task supervision",
      "link": "propose_capability_gap"
    }
  },
  "producer": "research-radar",
  "project_id": "triumvirate",
  "request_id": "req-golden-0001",
  "sent_at": "2026-03-10T09:00:00+00:00"
}"#;

    const CANNED_WEAKNESSES_JSON: &str = r#"{
  "generated_at": "2026-06-15T10:00:00+00:00",
  "open_obligations": [
    { "weakness_code": "gate_rejected", "severity": "high", "count": 3,
      "domains": ["governance"], "latest_summary": "gate keeps rejecting proposals", "last_observed_at": "2026-06-14T09:00:00+00:00" },
    { "weakness_code": "async_orphan_tasks", "severity": "medium", "count": 1,
      "domains": ["rust-async"], "latest_summary": "orphaned tasks under load", "last_observed_at": null }
  ],
  "ledger_weaknesses": [
    { "rank": 1, "weakness": "no structured concurrency in agent runtime", "status": "open", "evidence": "incident-42", "owning_task": "TASK-009" },
    { "rank": 2, "weakness": "embedding cache thrash", "status": "open", "evidence": "metrics", "owning_task": null }
  ]
}"#;

    fn test_finding() -> Finding {
        let mut finding = Finding::new(
            "https://example.com/paper".into(),
            "Example Paper".into(),
            SourceType::Paper,
            "rust".into(),
            "Use structured concurrency".into(),
            "Structured concurrency reduces orphaned task failures.".into(),
            "Refactor task spawning around JoinSet".into(),
            vec!["async-runtime".into(), "tokio".into()],
        );
        finding.id = "finding-123".into();
        finding.confidence = 0.7;
        finding.impact_weight = 0.5;
        finding.urgency = UrgencyLevel::High;
        finding.discovered_at = chrono::DateTime::parse_from_rfc3339("2026-03-10T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        finding
    }

    fn golden_finding() -> Finding {
        let mut finding = Finding::new(
            "https://arxiv.org/abs/2603.01234".into(),
            "Structured Concurrency for Resilient Agents".into(),
            SourceType::Paper,
            "rust-async".into(),
            "Adopt structured concurrency for agent task supervision".into(),
            "Structured concurrency bounds task lifetimes to a parent scope, eliminating orphaned tasks and making cancellation deterministic.".into(),
            "Refactor the agent runtime to spawn child tasks inside a JoinSet owned by the supervising scope.".into(),
            vec![
                "async-runtime".into(),
                "tokio".into(),
                "task-supervision".into(),
            ],
        );
        finding.id = "finding-golden-0001".into();
        finding.confidence = 0.82;
        finding.impact_weight = 0.65;
        finding.urgency = UrgencyLevel::High;
        finding.discovered_at = chrono::DateTime::parse_from_rfc3339("2026-03-10T08:30:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        finding.cited_paper = Some({
            let mut p = PaperRef::new(
                "Structured Concurrency for Resilient Agents".into(),
                "A. Researcher and B. Engineer".into(),
                "https://arxiv.org/abs/2603.01234".into(),
            );
            p.year = Some(2026);
            p.venue = Some("ICLR".into());
            p
        });
        finding.applicability_hypothesis = "Structured concurrency bounds task lifetimes to a parent scope, eliminating orphaned tasks and making cancellation deterministic. — Refactor the agent runtime to spawn child tasks inside a JoinSet owned by the supervising scope. (domain: rust-async)".to_string();
        finding.novelty_score = 0.5;
        finding
    }

    /// Spin up a tiny HTTP server that returns a canned Triumvirate response.
    async fn mock_triumvirate_server(
        response_body: &str,
        status: u16,
    ) -> (tokio::task::JoinHandle<String>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");
        let body = response_body.to_string();

        let handle = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let mut buf = vec![0u8; 8192];
            let n = stream.read(&mut buf).await.unwrap();
            let request = String::from_utf8_lossy(&buf[..n]).into_owned();

            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();

            request
        });

        (handle, base_url)
    }

    #[tokio::test]
    async fn fetch_weaknesses_parses_both_arrays() {
        let (server, base_url) = mock_triumvirate_server(CANNED_WEAKNESSES_JSON, 200).await;
        let publisher = TriumviratePublisher::new(
            "http://unused.example".into(),
            "triumvirate".into(),
            Some("tok".into()),
        )
        .with_base_url(base_url);

        let weaknesses = publisher.fetch_weaknesses().await.unwrap();
        assert_eq!(weaknesses.open_obligations.len(), 2);
        assert_eq!(weaknesses.ledger_weaknesses.len(), 2);
        assert_eq!(
            weaknesses.open_obligations[0].weakness_code,
            "gate_rejected"
        );
        assert_eq!(weaknesses.ledger_weaknesses[0].rank, 1);

        let request = server.await.unwrap();
        assert!(request.to_lowercase().contains("authorization: bearer tok"));
        assert!(request.contains("/v1/research/weaknesses/triumvirate"));
    }

    #[test]
    fn classify_obligation_status_mapping() {
        assert_eq!(
            classify_obligation_status("applied"),
            ObligationDisposition::Favorable
        );
        assert_eq!(
            classify_obligation_status("closed"),
            ObligationDisposition::Favorable
        );
        assert_eq!(
            classify_obligation_status("authorized"),
            ObligationDisposition::Favorable
        );
        assert_eq!(
            classify_obligation_status("rolled_back"),
            ObligationDisposition::Unfavorable
        );
        assert_eq!(
            classify_obligation_status("blocked"),
            ObligationDisposition::Unfavorable
        );

        for status in [
            "open",
            "candidate_proposed",
            "adjudication_pending",
            "",
            "weird",
        ] {
            assert_eq!(
                classify_obligation_status(status),
                ObligationDisposition::Pending
            );
        }
    }

    #[tokio::test]
    async fn fetch_obligation_status_parses_envelope() {
        let (server, base_url) =
            mock_triumvirate_server(r#"{"data":{"status":"applied"}}"#, 200).await;
        let publisher = TriumviratePublisher::new(
            "http://unused.example".into(),
            "proj-x".into(),
            Some("tok".into()),
        )
        .with_base_url(base_url);

        let status = publisher.fetch_obligation_status("obl-9").await.unwrap();
        assert_eq!(status, "applied");

        let request = server.await.unwrap();
        assert!(request.contains("/v1/learning/proj-x/obligations/obl-9"));
        assert!(request.to_lowercase().contains("authorization: bearer tok"));
    }

    #[test]
    fn to_weakness_contexts_maps_obligations_and_ledger() {
        let weaknesses =
            serde_json::from_str::<TriumvirateWeaknesses>(CANNED_WEAKNESSES_JSON).unwrap();
        let contexts = to_weakness_contexts(&weaknesses);

        assert_eq!(contexts.len(), 4);
        assert!(contexts
            .iter()
            .any(|ctx| ctx.code == "gate_rejected" && ctx.summary.contains("governance")));
        assert!(contexts
            .iter()
            .any(|ctx| ctx.code == "external_research_signal"
                && ctx.summary.contains("structured concurrency")));
    }

    #[test]
    fn resolve_weakness_link_domain_match_and_miss() {
        let weaknesses =
            serde_json::from_str::<TriumvirateWeaknesses>(CANNED_WEAKNESSES_JSON).unwrap();

        assert_eq!(
            resolve_weakness_link("governance", "irrelevant hypothesis", &weaknesses),
            Some("gate_rejected")
        );
        assert!(
            resolve_weakness_link("astrophysics", "totally unrelated text", &weaknesses).is_none()
        );
        assert_eq!(
            resolve_weakness_link(
                "unknown-domain",
                "this finding helps with gate_rejected proposals",
                &weaknesses,
            ),
            Some("gate_rejected")
        );
    }

    #[tokio::test]
    async fn publish_emits_strengthen_existing_with_resolved_code() {
        let (server, base_url) =
            mock_triumvirate_server(r#"{"obligation_ids":["obl-1","obl-2"]}"#, 200).await;
        let publisher = TriumviratePublisher::new(base_url, "project-a".into(), None);

        let outcome = publisher
            .publish(&test_finding(), Some("gate_rejected"))
            .await
            .unwrap();
        assert_eq!(outcome.status, 200);

        let request = server.await.unwrap();
        let body = request.split("\r\n\r\n").nth(1).unwrap();
        let json = serde_json::from_str::<serde_json::Value>(body).unwrap();
        assert_eq!(
            json["finding"]["weakness_link"]["link"].as_str().unwrap(),
            "strengthen_existing"
        );
        assert_eq!(
            json["finding"]["weakness_link"]["weakness_code"]
                .as_str()
                .unwrap(),
            "gate_rejected"
        );
    }

    #[test]
    fn golden_propose_capability_gap_wire_shape() {
        let publisher = TriumviratePublisher::new(
            "https://minerva.internal".into(),
            "triumvirate".into(),
            None,
        );
        let payload = publisher.build_payload(&golden_finding(), None, "req-golden-0001".into());
        let mut value = serde_json::to_value(&payload).unwrap();
        value["sent_at"] = serde_json::Value::String("2026-03-10T09:00:00+00:00".into());
        let pretty = serde_json::to_string_pretty(&value).unwrap();
        if pretty != CANONICAL_RESEARCH_FINDING_JSON {
            println!("{pretty}");
        }
        assert_eq!(pretty, CANONICAL_RESEARCH_FINDING_JSON);
    }

    #[test]
    fn build_payload_produces_exact_json_shape() {
        let finding = test_finding();
        let publisher =
            TriumviratePublisher::new("http://example.com".into(), "project-a".into(), None);
        let payload = publisher.build_payload(&finding, None, "request-123".into());
        let json = serde_json::to_value(payload).unwrap();

        assert_eq!(json["contract_version"].as_str().unwrap(), "2026-03-10");
        assert_eq!(json["producer"].as_str().unwrap(), "research-radar");
        assert_eq!(json["project_id"].as_str().unwrap(), "project-a");
        assert_eq!(json["finding"]["finding_id"].as_str().unwrap(), finding.id);
        assert_eq!(json["finding"]["source_kind"].as_str().unwrap(), "paper");
        assert_eq!(
            json["finding"]["proposed_urgency"].as_str().unwrap(),
            finding.urgency.as_str()
        );
        assert!(!json["finding"]["applicability_hypothesis"]
            .as_str()
            .unwrap()
            .is_empty());
        assert_eq!(json["finding"]["novelty_score"].as_f64().unwrap(), 0.5);
        assert_eq!(json["finding"]["schema_version"].as_str().unwrap(), "1.0");
        assert_eq!(
            json["finding"]["weakness_link"]["link"].as_str().unwrap(),
            "propose_capability_gap"
        );
        assert_eq!(
            json["finding"]["weakness_link"]["gap_summary"]
                .as_str()
                .unwrap(),
            finding.title
        );
        assert!(json["finding"]["weakness_link"]
            .get("weakness_code")
            .is_none());

        let payload = publisher.build_payload(&finding, Some("WK-123"), "request-456".into());
        let json = serde_json::to_value(payload).unwrap();
        assert_eq!(
            json["finding"]["weakness_link"]["link"].as_str().unwrap(),
            "strengthen_existing"
        );
        assert_eq!(
            json["finding"]["weakness_link"]["weakness_code"]
                .as_str()
                .unwrap(),
            "WK-123"
        );
        assert!(json["finding"]["weakness_link"]
            .get("gap_summary")
            .is_none());
    }

    #[test]
    fn novelty_score_passes_through_real_value() {
        let mut finding = test_finding();
        finding.novelty_score = 0.83;
        let publisher =
            TriumviratePublisher::new("http://example.com".into(), "project-a".into(), None);
        let payload = publisher.build_payload(&finding, None, "req-nov".into());
        let json = serde_json::to_value(payload).unwrap();
        let novelty_score = json["finding"]["novelty_score"].as_f64().unwrap();

        assert!((novelty_score - 0.83).abs() < 1e-4);
    }

    #[test]
    fn novelty_score_zero_falls_back_to_half() {
        let finding = test_finding();
        let publisher =
            TriumviratePublisher::new("http://example.com".into(), "project-a".into(), None);
        let payload = publisher.build_payload(&finding, None, "req-nov".into());
        let json = serde_json::to_value(payload).unwrap();

        assert_eq!(json["finding"]["novelty_score"].as_f64().unwrap(), 0.5);
    }

    #[test]
    fn citation_is_omitted_when_absent_and_sparse_when_present() {
        let mut finding = test_finding();
        let publisher =
            TriumviratePublisher::new("http://example.com".into(), "project-a".into(), None);

        let payload = publisher.build_payload(&finding, None, "request-123".into());
        let json = serde_json::to_value(payload).unwrap();
        assert!(json["finding"].get("citation").is_none());

        finding.cited_paper = Some(PaperRef::new(
            "Structured Concurrency".into(),
            "A. Researcher".into(),
            "https://example.com/paper.pdf".into(),
        ));
        let payload = publisher.build_payload(&finding, None, "request-456".into());
        let json = serde_json::to_value(payload).unwrap();
        let citation = &json["finding"]["citation"];
        assert_eq!(
            citation["title"].as_str().unwrap(),
            "Structured Concurrency"
        );
        assert_eq!(citation["authors"].as_str().unwrap(), "A. Researcher");
        assert_eq!(
            citation["url"].as_str().unwrap(),
            "https://example.com/paper.pdf"
        );
        assert!(citation.get("year").is_none());
        assert!(citation.get("venue").is_none());
    }

    #[tokio::test]
    async fn publish_sends_authorization_header_when_token_is_some() {
        let (server, base_url) =
            mock_triumvirate_server(r#"{"obligation_ids":["obl-1","obl-2"]}"#, 200).await;
        let publisher = TriumviratePublisher::new(
            "http://unused.example".into(),
            "project-a".into(),
            Some("test-token".into()),
        )
        .with_base_url(base_url);

        let outcome = publisher.publish(&test_finding(), None).await.unwrap();
        assert_eq!(outcome.status, 200);

        let request = server.await.unwrap();
        assert!(request
            .to_lowercase()
            .contains("authorization: bearer test-token"));
    }

    #[tokio::test]
    async fn publish_succeeds_on_200() {
        let (server, base_url) =
            mock_triumvirate_server(r#"{"obligation_ids":["obl-1","obl-2"]}"#, 200).await;
        let publisher = TriumviratePublisher::new(base_url, "project-a".into(), None);

        let outcome = publisher.publish(&test_finding(), None).await.unwrap();
        assert_eq!(outcome.status, 200);
        assert!(!outcome.request_id.is_empty());
        assert_eq!(
            outcome.obligation_ids,
            vec!["obl-1".to_string(), "obl-2".to_string()]
        );

        server.await.unwrap();
    }

    #[test]
    fn extract_obligation_ids_handles_shapes() {
        assert_eq!(
            extract_obligation_ids(r#"{"obligation_ids":["obl-1","obl-2"]}"#),
            vec!["obl-1".to_string(), "obl-2".to_string()]
        );
        assert_eq!(
            extract_obligation_ids(r#"{"obligation_id":"obl-3"}"#),
            vec!["obl-3".to_string()]
        );
        assert_eq!(
            extract_obligation_ids(r#"{"obligations":[{"id":"obl-4"},{"id":"obl-5"}]}"#),
            vec!["obl-4".to_string(), "obl-5".to_string()]
        );
        assert!(extract_obligation_ids("").is_empty());
        assert!(extract_obligation_ids("not json").is_empty());
    }

    #[test]
    fn extract_obligation_status_shapes() {
        assert_eq!(
            extract_obligation_status(r#"{"status":"applied"}"#),
            Some("applied".to_string())
        );
        assert_eq!(
            extract_obligation_status(r#"{"data":{"status":"applied"}}"#),
            Some("applied".to_string())
        );
        assert_eq!(
            extract_obligation_status(r#"{"obligation":{"status":"applied"}}"#),
            Some("applied".to_string())
        );
        assert_eq!(extract_obligation_status(""), None);
        assert_eq!(extract_obligation_status("{}"), None);
    }

    #[tokio::test]
    async fn publish_surfaces_error_on_400() {
        let (server, base_url) = mock_triumvirate_server("{\"error\":\"bad request\"}", 400).await;
        let publisher = TriumviratePublisher::new(base_url, "project-a".into(), None);

        let err = publisher.publish(&test_finding(), None).await.unwrap_err();
        match err {
            PublishError::Http(message) => assert!(message.contains("400")),
            other => panic!("expected HTTP error, got: {other:?}"),
        }

        server.await.unwrap();
    }
}
