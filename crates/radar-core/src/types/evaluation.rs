use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    KeywordRejected,
    ScoredBelowThreshold,
    Matched,
    LlmFailed,
    SourceSkipped,
}

impl Disposition {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::KeywordRejected => "keyword_rejected",
            Self::ScoredBelowThreshold => "scored_below_threshold",
            Self::Matched => "matched",
            Self::LlmFailed => "llm_failed",
            Self::SourceSkipped => "source_skipped",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "keyword_rejected" => Some(Self::KeywordRejected),
            "scored_below_threshold" => Some(Self::ScoredBelowThreshold),
            "matched" => Some(Self::Matched),
            "llm_failed" => Some(Self::LlmFailed),
            "source_skipped" => Some(Self::SourceSkipped),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemScore {
    pub id: String,
    pub item_id: String,
    pub profile_id: String,
    pub job_id: String,
    pub disposition: Disposition,
    pub score: Option<f64>,
    pub reason_short: Option<String>,
    pub rationale: Option<String>,
    pub profile_revision_at_enqueue: i64,
    pub profile_revision_current: i64,
    pub created_at: DateTime<Utc>,
}

impl ItemScore {
    pub fn is_stale(&self) -> bool {
        self.profile_revision_at_enqueue < self.profile_revision_current
    }
}
