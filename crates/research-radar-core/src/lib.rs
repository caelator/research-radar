//! research-radar-core — shared types and storage layer for research-radar.
//!
//! ## Data Model
//!
//! - **Source**: a URL + metadata record (paper, article, web, book)
//! - **Entry**: annotated slice of a Source (content, summary, tags, relevance score)
//! - **RadarQuery**: a search/query log entry
//! - **RadarResult**: a scored retrieval result linking a query to an entry

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
            created_at: Utc::now(),
        }
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
    pub fn new(profile_id: String, channel: String, config: serde_json::Value, enabled: bool) -> Self {
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

// ─── Re-exports ──────────────────────────────────────────────────────

pub mod executor;
pub mod score;
pub mod storage;
pub use executor::{PipelineExecutor, PipelineRun};
pub use score::score_entry;
pub use storage::{DbPool, SourceHealth, StorageError};
