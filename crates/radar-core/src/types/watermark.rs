use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Per-profile, per-source watermark for incremental fetching.
/// Keyed by (profile_id, source_type, source_scope_hash).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceWatermark {
    pub profile_id: String,
    pub source_type: String,
    pub source_scope_hash: String,
    pub high_watermark: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
