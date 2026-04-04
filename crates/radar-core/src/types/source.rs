use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceType {
    Arxiv,
    SemanticScholar,
    HuggingfaceDailyPapers,
    Rss,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Arxiv => "arxiv",
            Self::SemanticScholar => "semantic_scholar",
            Self::HuggingfaceDailyPapers => "huggingface_daily_papers",
            Self::Rss => "rss",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "arxiv" => Some(Self::Arxiv),
            "semantic_scholar" => Some(Self::SemanticScholar),
            "huggingface_daily_papers" => Some(Self::HuggingfaceDailyPapers),
            "rss" => Some(Self::Rss),
            _ => None,
        }
    }
}

/// Health snapshot for a single source, exposed via source_health().
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceHealth {
    pub source_type: SourceType,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_error_category: Option<String>,
    pub consecutive_failures: u32,
    pub current_lag_secs: Option<i64>,
    pub last_gap_skipped_at: Option<DateTime<Utc>>,
    pub backoff_until: Option<DateTime<Utc>>,
}

/// Normalized item output from any source adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceCandidate {
    pub canonical_id: String,
    pub title: String,
    pub authors: Option<String>,
    pub abstract_text: Option<String>,
    pub url: String,
    pub published_at: Option<DateTime<Utc>>,
    pub source_type: SourceType,
    pub aliases: Vec<(String, String)>, // (alias_type, alias_value)
    pub raw_json: Option<String>,
}
