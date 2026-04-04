use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: String,
    pub canonical_id: String,
    pub title: String,
    pub authors: Option<String>,
    pub abstract_text: Option<String>,
    pub url: String,
    pub published_at: Option<DateTime<Utc>>,
    pub source_type: String,
    pub raw_json: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl Item {
    pub fn new(canonical_id: String, title: String, url: String, source_type: String) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            canonical_id,
            title,
            authors: None,
            abstract_text: None,
            url,
            published_at: None,
            source_type,
            raw_json: None,
            created_at: Utc::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ItemAlias {
    pub item_id: String,
    pub alias_type: String, // "arxiv_id", "doi", "semantic_scholar_id", "url"
    pub alias_value: String,
}
