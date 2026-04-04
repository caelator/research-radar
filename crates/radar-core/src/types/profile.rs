use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub keywords: Vec<String>,
    pub negative_keywords: Vec<String>,
    pub sources: Vec<String>,
    pub llm_scoring_prompt: Option<String>,
    pub score_threshold: f64,
    pub max_llm_calls_per_scan: u32,
    pub revision: i64,
    pub last_seen_at: Option<DateTime<Utc>>,
    pub archived_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl Profile {
    pub fn new(name: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4().to_string(),
            name,
            description: None,
            keywords: Vec::new(),
            negative_keywords: Vec::new(),
            sources: vec!["arxiv".to_string()],
            llm_scoring_prompt: None,
            score_threshold: 0.7,
            max_llm_calls_per_scan: 20,
            revision: 1,
            last_seen_at: None,
            archived_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn is_archived(&self) -> bool {
        self.archived_at.is_some()
    }

    pub fn is_ready(&self) -> bool {
        !self.name.is_empty() && !self.keywords.is_empty() && !self.sources.is_empty()
    }

    pub fn missing_fields(&self) -> Vec<&'static str> {
        let mut missing = Vec::new();
        if self.name.is_empty() {
            missing.push("name");
        }
        if self.keywords.is_empty() {
            missing.push("keywords");
        }
        if self.sources.is_empty() {
            missing.push("sources");
        }
        missing
    }
}
