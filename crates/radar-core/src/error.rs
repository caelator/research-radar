use thiserror::Error;

/// Unified error taxonomy for radar-core, radar-mcp, and radar-cli.
#[derive(Debug, Error)]
pub enum RadarError {
    // Source errors
    #[error("transient source error for {source_name}: {message}")]
    SourceTransient {
        source_name: String,
        message: String,
    },

    #[error("source schema drift for {source_name}: {message}")]
    SourceSchemaDrift {
        source_name: String,
        message: String,
    },

    #[error("rate limited by {source_name}: retry after {retry_after_secs}s")]
    RateLimited {
        source_name: String,
        retry_after_secs: u64,
    },

    // Storage errors
    #[error("storage conflict: {0}")]
    StorageConflict(String),

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("database integrity check failed: {message}")]
    DbIntegrityFailed { message: String },

    // Scoring errors
    #[error("scorer parse error: {0}")]
    ScorerParse(String),

    #[error("budget exhausted: {message}")]
    BudgetExhausted { message: String },

    // Scan lifecycle errors
    #[error("gap skipped for {source_name}: backlog beyond catch-up window")]
    GapSkipped { source_name: String },

    #[error("lease liveness lost for job {job_id}")]
    LeaseLivenessLost { job_id: String },

    #[error("executor unavailable: {message}")]
    ExecutorUnavailable { message: String },

    // Profile errors
    #[error("profile archived: {profile_id}")]
    ProfileArchived { profile_id: String },

    #[error("profile not ready: {message}")]
    ProfileNotReady { message: String },

    #[error("profile not found: {profile_id}")]
    ProfileNotFound { profile_id: String },

    // General
    #[error("not found: {0}")]
    NotFound(String),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, RadarError>;
