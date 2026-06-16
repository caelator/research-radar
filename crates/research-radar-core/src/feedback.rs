use crate::storage::{DbPool, PublishedFindingRow};
use crate::triumvirate::{classify_obligation_status, ObligationDisposition, TriumviratePublisher};
use std::collections::HashMap;

pub struct FeedbackPoller {
    publisher: TriumviratePublisher,
    pool: DbPool,
}

#[derive(Debug, Clone)]
pub struct FeedbackConfig {
    pub base_url: String,
    pub project_id: String,
    pub auth_token: Option<String>,
    pub poll_interval_secs: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PollSummary {
    pub polled: usize,
    pub accepted: usize,
    pub rejected: usize,
    pub still_pending: usize,
    pub errors: usize,
}

#[derive(Debug, thiserror::Error)]
pub enum FeedbackError {
    #[error("storage error: {0}")]
    Storage(#[from] crate::storage::StorageError),
    #[error("publish error: {0}")]
    Publish(#[from] crate::triumvirate::PublishError),
}

impl FeedbackConfig {
    pub fn from_env() -> Option<FeedbackConfig> {
        if env_truthy("RADAR_TRIUMVIRATE_DISABLE") {
            return None;
        }

        let base_url = std::env::var("RADAR_TRIUMVIRATE_BASE_URL")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "http://127.0.0.1:8400".to_string());
        let project_id = std::env::var("RADAR_TRIUMVIRATE_PROJECT_ID")
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "triumvirate".to_string());
        let poll_interval_secs = std::env::var("RADAR_FEEDBACK_POLL_SECS")
            .ok()
            .filter(|value| !value.is_empty())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(900);
        if poll_interval_secs == 0 {
            return None;
        }

        let auth_token = std::env::var("RADAR_TRIUMVIRATE_TOKEN")
            .ok()
            .filter(|value| !value.is_empty());

        Some(FeedbackConfig {
            base_url,
            project_id,
            auth_token,
            poll_interval_secs,
        })
    }
}

fn env_truthy(name: &str) -> bool {
    std::env::var(name)
        .map(|value| matches!(value.trim().to_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

impl FeedbackPoller {
    pub fn new(publisher: TriumviratePublisher, pool: DbPool) -> Self {
        Self { publisher, pool }
    }

    pub fn from_config(config: &FeedbackConfig, pool: DbPool) -> Self {
        let publisher = TriumviratePublisher::new(
            config.base_url.clone(),
            config.project_id.clone(),
            config.auth_token.clone(),
        );
        Self::new(publisher, pool)
    }

    const POLL_BATCH_LIMIT: usize = 100;

    #[cfg(test)]
    pub(crate) fn pool(&self) -> &DbPool {
        &self.pool
    }

    /// Poll Minerva for each pending published finding, reduce its obligation
    /// statuses to a single finding outcome, and persist the transition.
    #[tracing::instrument(skip(self))]
    pub async fn poll_once(&self) -> Result<PollSummary, FeedbackError> {
        let rows = self.pool.list_pending_outcomes(Self::POLL_BATCH_LIMIT)?;
        let now = chrono::Utc::now().to_rfc3339();
        let mut summary = PollSummary::default();

        for row in rows {
            summary.polled += 1;
            let mut dispositions = Vec::with_capacity(row.obligation_ids.len());

            for obligation_id in &row.obligation_ids {
                match self.publisher.fetch_obligation_status(obligation_id).await {
                    Ok(status) => dispositions.push(classify_obligation_status(&status)),
                    Err(_) => {
                        summary.errors += 1;
                        dispositions.push(ObligationDisposition::Pending);
                    }
                }
            }

            match reduce_dispositions(&dispositions) {
                ObligationDisposition::Favorable => {
                    self.pool
                        .update_finding_outcome(&row.finding_id, "accepted", &now)?;
                    summary.accepted += 1;
                }
                ObligationDisposition::Unfavorable => {
                    self.pool
                        .update_finding_outcome(&row.finding_id, "rejected", &now)?;
                    summary.rejected += 1;
                }
                ObligationDisposition::Pending => {
                    summary.still_pending += 1;
                }
            }
        }

        let transitioned = summary.accepted + summary.rejected;
        tracing::info!(
            polled = summary.polled,
            transitioned,
            accepted = summary.accepted,
            rejected = summary.rejected,
            still_pending = summary.still_pending,
            errors = summary.errors,
            "triumvirate: feedback poll complete"
        );
        Ok(summary)
    }
}

pub fn should_poll_feedback(secs_since_last_poll: u64, poll_interval_secs: u64) -> bool {
    poll_interval_secs > 0 && secs_since_last_poll >= poll_interval_secs
}

pub fn reduce_dispositions(dispositions: &[ObligationDisposition]) -> ObligationDisposition {
    if dispositions.is_empty() {
        return ObligationDisposition::Pending;
    }
    if dispositions.contains(&ObligationDisposition::Favorable) {
        return ObligationDisposition::Favorable;
    }
    if dispositions
        .iter()
        .all(|disposition| *disposition == ObligationDisposition::Unfavorable)
    {
        return ObligationDisposition::Unfavorable;
    }
    ObligationDisposition::Pending
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SourceWeight {
    pub key: String,
    pub accepted: usize,
    pub rejected: usize,
    pub pending: usize,
    pub usefulness: f64,
}

// TODO(phase-5): consume these weights in profile targeting + source selection

/// Aggregate published-finding outcomes by `source_kind` into usefulness scores.
/// usefulness = (accepted + ALPHA) / (accepted + rejected + 2*ALPHA), ALPHA = 1.0 (Laplace smoothing),
/// so a no-data source scores 0.5 (mid) and never 0/1. Pending rows are counted in `pending`
/// but excluded from the usefulness ratio. Sorted by usefulness DESC, then accepted DESC, then key ASC.
pub fn source_usefulness(rows: &[PublishedFindingRow]) -> Vec<SourceWeight> {
    aggregate(rows, |row| row.source_kind.clone())
}

/// Same aggregation keyed by `domain`.
pub fn domain_usefulness(rows: &[PublishedFindingRow]) -> Vec<SourceWeight> {
    aggregate(rows, |row| row.domain.clone())
}

fn aggregate<F: Fn(&PublishedFindingRow) -> String>(
    rows: &[PublishedFindingRow],
    key_of: F,
) -> Vec<SourceWeight> {
    const ALPHA: f64 = 1.0;

    let mut grouped: HashMap<String, SourceWeight> = HashMap::new();
    for row in rows {
        let key = key_of(row);
        let weight = grouped.entry(key.clone()).or_insert_with(|| SourceWeight {
            key,
            accepted: 0,
            rejected: 0,
            pending: 0,
            usefulness: 0.5,
        });

        match row.outcome.as_str() {
            "accepted" => weight.accepted += 1,
            "rejected" => weight.rejected += 1,
            _ => weight.pending += 1,
        }
    }

    let mut weights: Vec<_> = grouped
        .into_values()
        .map(|mut weight| {
            weight.usefulness = (weight.accepted as f64 + ALPHA)
                / (weight.accepted as f64 + weight.rejected as f64 + 2.0 * ALPHA);
            weight
        })
        .collect();

    weights.sort_by(|a, b| {
        b.usefulness
            .total_cmp(&a.usefulness)
            .then_with(|| b.accepted.cmp(&a.accepted))
            .then_with(|| a.key.cmp(&b.key))
    });
    weights
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::JoinHandle;

    async fn mock_status_server(responses: Vec<(u16, String)>) -> (JoinHandle<()>, String) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{addr}");

        let handle = tokio::spawn(async move {
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                use tokio::io::{AsyncReadExt, AsyncWriteExt};

                let mut buf = vec![0u8; 8192];
                let _ = stream.read(&mut buf).await.unwrap();

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

    fn row(source_kind: &str, domain: &str, outcome: &str) -> PublishedFindingRow {
        PublishedFindingRow {
            finding_id: format!("{source_kind}-{domain}-{outcome}"),
            obligation_ids: vec![],
            source_kind: source_kind.into(),
            domain: domain.into(),
            confidence: 0.0,
            novelty: 0.0,
            weakness_code: None,
            outcome: outcome.into(),
            published_at: "2026-01-01T00:00:00+00:00".into(),
            last_polled_at: None,
        }
    }

    #[test]
    fn should_poll_when_interval_elapsed() {
        assert!(should_poll_feedback(900, 900));
        assert!(should_poll_feedback(1000, 900));
    }

    #[test]
    fn should_not_poll_before_interval() {
        assert!(!should_poll_feedback(100, 900));
    }

    #[test]
    fn should_not_poll_when_interval_zero() {
        assert!(!should_poll_feedback(99999, 0));
    }

    #[tokio::test]
    async fn poll_once_favorable_marks_accepted() {
        let pool = DbPool::test_pool().unwrap();
        pool.record_published_finding(
            "f1",
            &["obl-1".into()],
            "arxiv",
            "alignment",
            0.8,
            0.5,
            None,
        )
        .unwrap();
        let (server, base_url) =
            mock_status_server(vec![(200, r#"{"status":"applied"}"#.into())]).await;
        let publisher = TriumviratePublisher::new(base_url, "proj-x".into(), Some("tok".into()));
        let poller = FeedbackPoller::new(publisher, pool);

        let summary = poller.poll_once().await.unwrap();

        server.await.unwrap();
        assert_eq!(summary.accepted, 1);
        let rows = poller.pool().outcome_rows().unwrap();
        let row = rows.iter().find(|row| row.finding_id == "f1").unwrap();
        assert_eq!(row.outcome, "accepted");
    }

    #[tokio::test]
    async fn poll_once_unfavorable_marks_rejected() {
        let pool = DbPool::test_pool().unwrap();
        pool.record_published_finding(
            "f1",
            &["obl-1".into()],
            "arxiv",
            "alignment",
            0.8,
            0.5,
            None,
        )
        .unwrap();
        let (server, base_url) =
            mock_status_server(vec![(200, r#"{"status":"rolled_back"}"#.into())]).await;
        let publisher = TriumviratePublisher::new(base_url, "proj-x".into(), Some("tok".into()));
        let poller = FeedbackPoller::new(publisher, pool);

        let summary = poller.poll_once().await.unwrap();

        server.await.unwrap();
        assert_eq!(summary.rejected, 1);
        let rows = poller.pool().outcome_rows().unwrap();
        let row = rows.iter().find(|row| row.finding_id == "f1").unwrap();
        assert_eq!(row.outcome, "rejected");
    }

    #[tokio::test]
    async fn poll_once_pending_stays_pending() {
        let pool = DbPool::test_pool().unwrap();
        pool.record_published_finding(
            "f1",
            &["obl-1".into()],
            "arxiv",
            "alignment",
            0.8,
            0.5,
            None,
        )
        .unwrap();
        let (server, base_url) =
            mock_status_server(vec![(200, r#"{"status":"adjudication_pending"}"#.into())]).await;
        let publisher = TriumviratePublisher::new(base_url, "proj-x".into(), Some("tok".into()));
        let poller = FeedbackPoller::new(publisher, pool);

        let summary = poller.poll_once().await.unwrap();

        server.await.unwrap();
        assert_eq!(summary.still_pending, 1);
        let rows = poller.pool().outcome_rows().unwrap();
        let row = rows.iter().find(|row| row.finding_id == "f1").unwrap();
        assert_eq!(row.outcome, "pending");
        assert_eq!(poller.pool().list_pending_outcomes(10).unwrap().len(), 1);
    }

    #[test]
    fn source_usefulness_ranks_by_outcome() {
        let rows = vec![
            row("good", "alignment", "accepted"),
            row("good", "alignment", "accepted"),
            row("good", "alignment", "accepted"),
            row("good", "alignment", "rejected"),
            row("bad", "alignment", "accepted"),
            row("bad", "alignment", "rejected"),
            row("bad", "alignment", "rejected"),
            row("bad", "alignment", "rejected"),
            row("unknown", "alignment", "pending"),
        ];

        let weights = source_usefulness(&rows);
        let good = weights.iter().find(|weight| weight.key == "good").unwrap();
        let unknown = weights
            .iter()
            .find(|weight| weight.key == "unknown")
            .unwrap();
        let bad = weights.iter().find(|weight| weight.key == "bad").unwrap();

        assert_eq!(weights[0].key, "good");
        assert!(good.usefulness > unknown.usefulness);
        assert!(unknown.usefulness > bad.usefulness);
        assert_eq!(unknown.usefulness, 0.5);
    }

    #[test]
    fn reduce_dispositions_rules() {
        assert_eq!(reduce_dispositions(&[]), ObligationDisposition::Pending);
        assert_eq!(
            reduce_dispositions(&[
                ObligationDisposition::Favorable,
                ObligationDisposition::Unfavorable
            ]),
            ObligationDisposition::Favorable
        );
        assert_eq!(
            reduce_dispositions(&[
                ObligationDisposition::Unfavorable,
                ObligationDisposition::Unfavorable
            ]),
            ObligationDisposition::Unfavorable
        );
        assert_eq!(
            reduce_dispositions(&[
                ObligationDisposition::Pending,
                ObligationDisposition::Unfavorable
            ]),
            ObligationDisposition::Pending
        );
    }
}
