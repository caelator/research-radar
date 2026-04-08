//! research-radar-core — shared types and storage layer for research-radar.
//!
//! ## Data Model
//!
//! - **Source**: a URL + metadata record (paper, article, web, book)
//! - **Entry**: annotated slice of a Source (content, summary, tags, relevance score)
//! - **RadarQuery**: a search/query log entry
//! - **RadarResult**: a scored retrieval result linking a query to an entry
//! - **Finding**: evaluated research result with urgency, impact, and citation — the evolve input contract

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ─── Source ──────────────────────────────────────────────────────────

/// Kind of research source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceType {
    Paper,
    Article,
    Web,
    Book,
}

impl SourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Paper => "paper",
            Self::Article => "article",
            Self::Web => "web",
            Self::Book => "book",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "paper" => Self::Paper,
            "article" => Self::Article,
            "web" => Self::Web,
            "book" => Self::Book,
            _ => Self::Web,
        }
    }
}

/// A reference to an external source (URL + metadata).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub id: String,
    pub url: String,
    pub title: String,
    pub source_type: SourceType,
    pub added_at: DateTime<Utc>,
}

impl Source {
    pub fn new(url: String, title: String, source_type: SourceType) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            url,
            title,
            source_type,
            added_at: Utc::now(),
        }
    }
}

// ─── Entry ───────────────────────────────────────────────────────────

/// A tagged, annotated entry extracted from or associated with a Source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub id: String,
    pub source_id: String,
    /// Raw or processed content from the source.
    pub content: String,
    /// Auto- or human-generated summary.
    pub summary: Option<String>,
    /// Freeform tags for filtering and faceting.
    pub tags: Vec<String>,
    /// 0.0–1.0 relevance score for the current query context.
    pub relevance_score: f64,
    pub last_reread_at: Option<DateTime<Utc>>,
}

impl Entry {
    pub fn new(source_id: String, content: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            source_id,
            content,
            summary: None,
            tags: Vec::new(),
            relevance_score: 0.0,
            last_reread_at: None,
        }
    }
}

// ─── RadarQuery ──────────────────────────────────────────────────────

/// A logged search or query operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarQuery {
    pub id: String,
    pub query_text: String,
    pub created_at: DateTime<Utc>,
}

impl RadarQuery {
    pub fn new(query_text: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            query_text,
            created_at: Utc::now(),
        }
    }
}

// ─── RadarResult ─────────────────────────────────────────────────────

/// A retrieval result linking a query to an entry, with a relevance score.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RadarResult {
    pub id: String,
    pub query_id: String,
    pub entry_id: String,
    pub score: f64,
    pub retrieved_at: DateTime<Utc>,
}

impl RadarResult {
    pub fn new(query_id: String, entry_id: String, score: f64) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            query_id,
            entry_id,
            score,
            retrieved_at: Utc::now(),
        }
    }
}

// ─── Profile ─────────────────────────────────────────────────────────

/// A monitoring profile defining keywords, sources, and scoring preferences.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub id: String,
    pub name: String,
    pub keywords: Vec<String>,
    pub negative_keywords: Vec<String>,
    pub sources: Vec<String>,
    pub scoring_prompt: Option<String>,
    pub score_threshold: f64,
    pub max_llm_calls: u32,
    pub revision: u32,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub archived_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl Profile {
    pub fn new(name: String, keywords: Vec<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            name,
            keywords,
            negative_keywords: Vec::new(),
            sources: Vec::new(),
            scoring_prompt: None,
            score_threshold: 0.5,
            max_llm_calls: 10,
            revision: 1,
            last_seen_at: None,
            archived_at: None,
            created_at: Utc::now(),
        }
    }

    pub fn is_archived(&self) -> bool {
        self.archived_at.is_some()
    }
}

// ─── ScanJob ─────────────────────────────────────────────────────────

/// Status of a scan job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScanJobStatus {
    Pending,
    Running,
    Complete,
    Failed,
}

impl ScanJobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "running" => Self::Running,
            "complete" => Self::Complete,
            "failed" => Self::Failed,
            _ => Self::Pending,
        }
    }
}

/// A scan job triggered by profile scan_now.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanJob {
    pub id: String,
    pub profile_id: String,
    pub status: ScanJobStatus,
    pub progress: u32,
    pub total: u32,
    pub reason: Option<String>,
    pub claimed_by: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub last_progress_at: Option<DateTime<Utc>>,
    pub attempt_count: u32,
    pub profile_revision_at_enqueue: Option<u32>,
    pub llm_spend_microunits: i64,
    pub warnings_json: Option<String>,
    pub error_json: Option<String>,
    pub progress_json: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

impl ScanJob {
    pub fn new(profile_id: String, reason: Option<String>) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            profile_id,
            status: ScanJobStatus::Pending,
            progress: 0,
            total: 0,
            reason,
            claimed_by: None,
            lease_token: None,
            lease_expires_at: None,
            heartbeat_at: None,
            last_progress_at: None,
            attempt_count: 0,
            profile_revision_at_enqueue: None,
            llm_spend_microunits: 0,
            warnings_json: None,
            error_json: None,
            progress_json: None,
            created_at: Utc::now(),
            completed_at: None,
        }
    }
}

// ─── Subscription ─────────────────────────────────────────────────────

/// A notification subscription for a profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscription {
    pub id: String,
    pub profile_id: String,
    pub channel: String,
    pub config: serde_json::Value,
    pub enabled: bool,
}

impl Subscription {
    pub fn new(
        profile_id: String,
        channel: String,
        config: serde_json::Value,
        enabled: bool,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            profile_id,
            channel,
            config,
            enabled,
        }
    }
}

// ─── ScoredMatch ─────────────────────────────────────────────────────

/// An entry scored against a profile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoredMatch {
    pub entry: Entry,
    pub profile_id: String,
    pub score: f64,
    pub disposition: String,
}

// ─── SourceWatermark ─────────────────────────────────────────────────

/// Per-profile, per-source watermark for incremental fetching.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceWatermark {
    pub id: String,
    pub profile_id: String,
    pub source_type: String,
    pub source_scope_hash: String,
    pub last_fetched_at: Option<DateTime<Utc>>,
    pub last_item_published_at: Option<DateTime<Utc>>,
    pub gap_skipped: bool,
}

impl SourceWatermark {
    pub fn new(profile_id: String, source_type: String, source_scope_hash: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            profile_id,
            source_type,
            source_scope_hash,
            last_fetched_at: None,
            last_item_published_at: None,
            gap_skipped: false,
        }
    }
}

// ─── ItemAlias ──────────────────────────────────────────────────────

/// Hard-ID alias for cross-source deduplication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemAlias {
    pub id: String,
    pub item_id: String,
    pub alias_type: String,
    pub alias_value: String,
    pub source_type: String,
    pub created_at: DateTime<Utc>,
}

impl ItemAlias {
    pub fn new(
        item_id: String,
        alias_type: String,
        alias_value: String,
        source_type: String,
    ) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            item_id,
            alias_type,
            alias_value,
            source_type,
            created_at: Utc::now(),
        }
    }
}

// ─── Re-exports ──────────────────────────────────────────────────────

pub mod arxiv;
pub mod embedding;
pub mod executor;
pub mod finding;
pub mod notify;
pub mod score;
pub mod scorer;
pub mod semantic_scholar;
pub mod storage;

pub use executor::{PipelineExecutor, PipelineRun};
pub use finding::{Finding, PaperRef, UrgencyLevel};
pub use score::score_entry;
pub use scorer::{AnthropicBackend, LlmBackend, MockBackend, ScorerResult};
pub use storage::lance_store::Result as LanceResult;
pub use storage::{DbPool, RadarStore, SourceHealth, SourceHealthDetail, MAX_JOB_ATTEMPTS};
// Re-export the sqlite StorageError directly so executor can use std::result::Result<T, StorageError>
pub use crate::storage::StorageError;
