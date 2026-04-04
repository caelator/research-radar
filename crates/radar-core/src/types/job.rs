use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Claimed,
    Processing,
    Completed,
    CompletedWithWarnings,
    Failed,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Claimed => "claimed",
            Self::Processing => "processing",
            Self::Completed => "completed",
            Self::CompletedWithWarnings => "completed_with_warnings",
            Self::Failed => "failed",
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "queued" => Some(Self::Queued),
            "claimed" => Some(Self::Claimed),
            "processing" => Some(Self::Processing),
            "completed" => Some(Self::Completed),
            "completed_with_warnings" => Some(Self::CompletedWithWarnings),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::CompletedWithWarnings | Self::Failed
        )
    }

    pub fn is_active(&self) -> bool {
        matches!(self, Self::Queued | Self::Claimed | Self::Processing)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanJob {
    pub job_id: String,
    pub profile_id: String,
    pub profile_snapshot_json: String,
    pub status: JobStatus,
    pub claimed_by: Option<String>,
    pub lease_token: Option<String>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub last_progress_at: Option<DateTime<Utc>>,
    pub attempt_count: i32,
    pub profile_revision_at_enqueue: i64,
    pub source_scope_hash: Option<String>,
    pub reason: Option<String>,
    pub llm_usage_json: Option<String>,
    pub llm_spend_microunits: i64,
    pub warnings_json: Option<String>,
    pub error_json: Option<String>,
    pub progress_json: Option<String>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

impl ScanJob {
    pub fn new(
        profile_id: String,
        profile_snapshot_json: String,
        profile_revision: i64,
        reason: Option<String>,
    ) -> Self {
        let now = Utc::now();
        Self {
            job_id: Uuid::new_v4().to_string(),
            profile_id,
            profile_snapshot_json,
            status: JobStatus::Queued,
            claimed_by: None,
            lease_token: None,
            lease_expires_at: None,
            heartbeat_at: None,
            last_progress_at: None,
            attempt_count: 0,
            profile_revision_at_enqueue: profile_revision,
            source_scope_hash: None,
            reason,
            llm_usage_json: None,
            llm_spend_microunits: 0,
            warnings_json: None,
            error_json: None,
            progress_json: None,
            created_at: now,
            started_at: None,
            finished_at: None,
        }
    }

    pub fn new_lease_token() -> String {
        Uuid::new_v4().to_string()
    }

    pub fn profile_snapshot(&self) -> serde_json::Result<crate::types::Profile> {
        serde_json::from_str(&self.profile_snapshot_json)
    }
}
