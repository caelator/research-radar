/// V1 schema for research.radar SQLite database.
pub const SCHEMA_VERSION: i32 = 2;

pub const CREATE_TABLES: &str = r#"
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS profiles (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    keywords_json TEXT NOT NULL DEFAULT '[]',
    negative_keywords_json TEXT NOT NULL DEFAULT '[]',
    sources_json TEXT NOT NULL DEFAULT '["arxiv"]',
    llm_scoring_prompt TEXT,
    score_threshold REAL NOT NULL DEFAULT 0.7,
    max_llm_calls_per_scan INTEGER NOT NULL DEFAULT 20,
    revision INTEGER NOT NULL DEFAULT 1,
    last_seen_at TEXT,
    archived_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS scan_jobs (
    job_id TEXT PRIMARY KEY NOT NULL,
    profile_id TEXT NOT NULL REFERENCES profiles(id),
    profile_snapshot_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    claimed_by TEXT,
    lease_token TEXT,
    lease_expires_at TEXT,
    heartbeat_at TEXT,
    last_progress_at TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    profile_revision_at_enqueue INTEGER NOT NULL,
    source_scope_hash TEXT,
    reason TEXT,
    llm_usage_json TEXT,
    llm_spend_microunits INTEGER NOT NULL DEFAULT 0,
    warnings_json TEXT,
    error_json TEXT,
    progress_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_scan_jobs_profile_status ON scan_jobs(profile_id, status);
CREATE INDEX IF NOT EXISTS idx_scan_jobs_status ON scan_jobs(status);

CREATE TABLE IF NOT EXISTS executor_heartbeats (
    worker_id TEXT PRIMARY KEY NOT NULL,
    heartbeat_at TEXT NOT NULL,
    started_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS items (
    id TEXT PRIMARY KEY NOT NULL,
    canonical_id TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    authors TEXT,
    abstract_text TEXT,
    url TEXT NOT NULL,
    published_at TEXT,
    source_type TEXT NOT NULL,
    raw_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_items_canonical ON items(canonical_id);

CREATE TABLE IF NOT EXISTS item_aliases (
    item_id TEXT NOT NULL REFERENCES items(id),
    alias_type TEXT NOT NULL,
    alias_value TEXT NOT NULL,
    PRIMARY KEY (item_id, alias_type, alias_value)
);
CREATE INDEX IF NOT EXISTS idx_item_aliases_value ON item_aliases(alias_type, alias_value);

CREATE TABLE IF NOT EXISTS item_scores (
    id TEXT PRIMARY KEY NOT NULL,
    item_id TEXT NOT NULL REFERENCES items(id),
    profile_id TEXT NOT NULL REFERENCES profiles(id),
    job_id TEXT NOT NULL REFERENCES scan_jobs(job_id),
    disposition TEXT NOT NULL,
    score REAL,
    reason_short TEXT,
    rationale TEXT,
    profile_revision_at_enqueue INTEGER NOT NULL,
    profile_revision_current INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_item_scores_profile ON item_scores(profile_id, created_at);
CREATE INDEX IF NOT EXISTS idx_item_scores_item_profile ON item_scores(item_id, profile_id);

CREATE TABLE IF NOT EXISTS notifications (
    id TEXT PRIMARY KEY NOT NULL,
    profile_id TEXT NOT NULL REFERENCES profiles(id),
    item_id TEXT NOT NULL REFERENCES items(id),
    channel TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    sent_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_notifications_idempotency
    ON notifications(profile_id, item_id, channel);

CREATE TABLE IF NOT EXISTS source_watermarks (
    profile_id TEXT NOT NULL REFERENCES profiles(id),
    source_type TEXT NOT NULL,
    source_scope_hash TEXT NOT NULL,
    high_watermark TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (profile_id, source_type, source_scope_hash)
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY NOT NULL,
    profile_id TEXT NOT NULL REFERENCES profiles(id),
    channel TEXT NOT NULL,
    channel_config TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_subscriptions_profile_channel
    ON subscriptions(profile_id, channel);
"#;
