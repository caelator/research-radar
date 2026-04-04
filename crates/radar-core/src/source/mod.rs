pub mod arxiv;

use crate::error::Result;
use crate::types::{SourceCandidate, SourceType};
use chrono::{DateTime, Utc};
use std::future::Future;

/// Trait for source adapters. Each source fetches items published after `since`,
/// normalizes them into `SourceCandidate`s, and respects politeness rules.
pub trait SourceAdapter: Send + Sync {
    /// Fetch items published after `since`. Returns normalized candidates.
    fn fetch(
        &self,
        categories: &[String],
        since: Option<DateTime<Utc>>,
        max_results: u32,
    ) -> impl Future<Output = Result<Vec<SourceCandidate>>> + Send;

    /// Source type identifier (e.g. "arxiv").
    fn source_type(&self) -> &'static str;
}

#[derive(Clone, Debug)]
pub struct StaticSourceAdapter {
    candidates: Vec<SourceCandidate>,
    source_type_name: &'static str,
}

impl StaticSourceAdapter {
    pub fn new(candidates: Vec<SourceCandidate>) -> Self {
        Self {
            candidates,
            source_type_name: "static",
        }
    }

    pub fn for_arxiv(keywords: &[String], categories: &[String]) -> Self {
        let joined_keywords = if keywords.is_empty() {
            "research".to_string()
        } else {
            keywords.join(", ")
        };
        let category_text = if categories.is_empty() {
            "cs.AI".to_string()
        } else {
            categories.join(", ")
        };
        let now = Utc::now();
        let items = vec![
            SourceCandidate {
                canonical_id: "mock:matched-1".into(),
                title: format!("Practical advances in {joined_keywords}"),
                authors: Some("Radar Demo".into()),
                abstract_text: Some(format!(
                    "A mock paper covering {joined_keywords} across categories {category_text}."
                )),
                url: "https://example.com/mock/matched-1".into(),
                published_at: Some(now),
                source_type: SourceType::Arxiv,
                aliases: vec![("arxiv_id".into(), "mock-matched-1".into())],
                raw_json: None,
            },
            SourceCandidate {
                canonical_id: "mock:matched-2".into(),
                title: format!("{joined_keywords} systems in production"),
                authors: Some("Radar Demo".into()),
                abstract_text: Some(
                    "A second mock paper intended to pass the keyword gate.".into(),
                ),
                url: "https://example.com/mock/matched-2".into(),
                published_at: Some(now - chrono::Duration::hours(2)),
                source_type: SourceType::Arxiv,
                aliases: vec![("arxiv_id".into(), "mock-matched-2".into())],
                raw_json: None,
            },
            SourceCandidate {
                canonical_id: "mock:noise-1".into(),
                title: "Irrelevant control sample".into(),
                authors: Some("Radar Demo".into()),
                abstract_text: Some(
                    "A mock paper that should usually fail the keyword gate.".into(),
                ),
                url: "https://example.com/mock/noise-1".into(),
                published_at: Some(now - chrono::Duration::hours(6)),
                source_type: SourceType::Arxiv,
                aliases: vec![("arxiv_id".into(), "mock-noise-1".into())],
                raw_json: None,
            },
        ];
        Self {
            candidates: items,
            source_type_name: "arxiv",
        }
    }
}

impl SourceAdapter for StaticSourceAdapter {
    fn fetch(
        &self,
        _categories: &[String],
        _since: Option<DateTime<Utc>>,
        _max_results: u32,
    ) -> impl Future<Output = Result<Vec<SourceCandidate>>> + Send {
        let candidates = self.candidates.clone();
        async move { Ok(candidates) }
    }

    fn source_type(&self) -> &'static str {
        self.source_type_name
    }
}
