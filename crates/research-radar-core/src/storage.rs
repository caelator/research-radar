//! LanceDB + SQLite storage layer for research-radar-core.
//!
//! Two stores with distinct responsibilities:
//!
//! - **`DbPool`** (synchronous, SQLite): existing pipeline compatibility.
//!   All tables from the original schema are preserved. Safe for `executor.rs`,
//!   `mcp_server.rs`, and `main.rs`.
//!
//! - **`RadarStore`** (async, LanceDB): the evolve loop contract.
//!   Uses typed Arrow columns for filterable Finding fields. Safe for
//!   concurrent reads and writes from multiple processes.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::PathBuf;
use uuid::Uuid;

// Re-exports for backward compatibility.
pub use self::lance_store::RadarStore;
pub use self::sqlite::DbPool;
pub use self::sqlite::{SourceHealth, StorageError};

// ─── SQLite (existing pipeline) ─────────────────────────────────────────────

mod sqlite {
    use super::*;

    #[derive(Debug, thiserror::Error)]
    pub enum StorageError {
        #[error("SQLite error: {0}")]
        Sqlite(#[from] rusqlite::Error),
        #[error("IO error: {0}")]
        Io(#[from] std::io::Error),
        #[error("not found: {0}")]
        NotFound(String),
        #[error("serialization error: {0}")]
        Serde(#[from] serde_json::Error),
    }

    pub type Result<T> = std::result::Result<T, StorageError>;

    /// Wraps a rusqlite Connection. Used by the existing sync pipeline.
    pub struct DbPool {
        pub(crate) conn: Connection,
    }

    impl DbPool {
        /// Open (or create) the SQLite database at `~/.research-radar/data.db`.
        pub fn init() -> Result<Self> {
            let db_path = Self::db_path()?;
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let conn = Connection::open(&db_path)?;
            conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
            let pool = Self { conn };
            pool.run_migrations()?;
            Ok(pool)
        }

        fn db_path() -> Result<PathBuf> {
            let home = dirs::home_dir().ok_or_else(|| {
                StorageError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "cannot resolve home directory",
                ))
            })?;
            Ok(home.join(".research-radar/data.db"))
        }

        /// In-memory pool for tests.
        pub fn test_pool() -> Result<Self> {
            let conn = Connection::open_in_memory()?;
            conn.execute_batch("PRAGMA journal_mode=WAL;")?;
            let pool = Self { conn };
            pool.run_migrations()?;
            Ok(pool)
        }

        pub(crate) fn run_migrations(&self) -> Result<()> {
            self.conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS sources (
                    id          TEXT PRIMARY KEY,
                    url         TEXT NOT NULL,
                    title       TEXT NOT NULL,
                    source_type TEXT NOT NULL DEFAULT 'web',
                    added_at    TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS entries (
                    id               TEXT PRIMARY KEY,
                    source_id        TEXT NOT NULL REFERENCES sources(id),
                    content          TEXT NOT NULL,
                    summary          TEXT,
                    tags             TEXT NOT NULL DEFAULT '[]',
                    relevance_score  REAL NOT NULL DEFAULT 0.0,
                    last_reread_at   TEXT
                );
                CREATE TABLE IF NOT EXISTS queries (
                    id          TEXT PRIMARY KEY,
                    query_text  TEXT NOT NULL,
                    created_at  TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS results (
                    id           TEXT PRIMARY KEY,
                    query_id     TEXT NOT NULL REFERENCES queries(id),
                    entry_id     TEXT NOT NULL REFERENCES entries(id),
                    score        REAL NOT NULL,
                    retrieved_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS profiles (
                    id                TEXT PRIMARY KEY,
                    name              TEXT NOT NULL,
                    keywords          TEXT NOT NULL DEFAULT '[]',
                    negative_keywords TEXT NOT NULL DEFAULT '[]',
                    sources           TEXT NOT NULL DEFAULT '[]',
                    scoring_prompt    TEXT,
                    score_threshold   REAL NOT NULL DEFAULT 0.5,
                    max_llm_calls     INTEGER NOT NULL DEFAULT 10,
                    created_at        TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scan_jobs (
                    id            TEXT PRIMARY KEY,
                    profile_id    TEXT NOT NULL REFERENCES profiles(id),
                    status        TEXT NOT NULL DEFAULT 'pending',
                    progress      INTEGER NOT NULL DEFAULT 0,
                    total         INTEGER NOT NULL DEFAULT 0,
                    reason        TEXT,
                    created_at    TEXT NOT NULL,
                    completed_at  TEXT
                );
                CREATE TABLE IF NOT EXISTS subscriptions (
                    id          TEXT PRIMARY KEY,
                    profile_id  TEXT NOT NULL REFERENCES profiles(id),
                    channel     TEXT NOT NULL,
                    config      TEXT NOT NULL DEFAULT '{}',
                    enabled     INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS item_scores (
                    id          TEXT PRIMARY KEY,
                    entry_id    TEXT NOT NULL REFERENCES entries(id),
                    profile_id  TEXT NOT NULL REFERENCES profiles(id),
                    score       REAL NOT NULL,
                    disposition TEXT NOT NULL DEFAULT 'new',
                    UNIQUE(entry_id, profile_id)
                );
                CREATE INDEX IF NOT EXISTS idx_entries_source_id ON entries(source_id);
                CREATE INDEX IF NOT EXISTS idx_results_query_id  ON results(query_id);
                CREATE INDEX IF NOT EXISTS idx_results_entry_id  ON results(entry_id);
                CREATE INDEX IF NOT EXISTS idx_scan_jobs_profile ON scan_jobs(profile_id);
                CREATE INDEX IF NOT EXISTS idx_subscriptions_profile ON subscriptions(profile_id);

                CREATE TABLE IF NOT EXISTS notifications (
                    id          TEXT PRIMARY KEY,
                    profile_id  TEXT NOT NULL REFERENCES profiles(id),
                    item_id     TEXT NOT NULL,
                    channel     TEXT NOT NULL,
                    sent_at     TEXT NOT NULL,
                    UNIQUE(profile_id, item_id, channel)
                );
                CREATE INDEX IF NOT EXISTS idx_notifications_profile ON notifications(profile_id);
                "#,
            )?;
            Ok(())
        }

        // ─── Source ─────────────────────────────────────────────

        pub fn insert_source(&self, source: &crate::Source) -> Result<String> {
            self.conn.execute(
                "INSERT INTO sources (id, url, title, source_type, added_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    source.id,
                    source.url,
                    source.title,
                    source.source_type.as_str(),
                    source.added_at.to_rfc3339(),
                ],
            )?;
            Ok(source.id.clone())
        }

        pub fn get_source(&self, id: &str) -> Result<Option<crate::Source>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, url, title, source_type, added_at FROM sources WHERE id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_source(row)?))
            } else {
                Ok(None)
            }
        }

        fn row_to_source(row: &rusqlite::Row) -> std::result::Result<crate::Source, StorageError> {
            use crate::SourceType;
            let added_str: String = row.get(4)?;
            let added_at = DateTime::parse_from_rfc3339(&added_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            Ok(crate::Source {
                id: row.get(0)?,
                url: row.get(1)?,
                title: row.get(2)?,
                source_type: SourceType::from_str(&row.get::<_, String>(3)?),
                added_at,
            })
        }

        pub fn list_sources(&self, limit: usize) -> Result<Vec<crate::Source>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, url, title, source_type, added_at FROM sources ORDER BY added_at DESC LIMIT ?1",
            )?;
            let mut sources = Vec::new();
            let mut rows = stmt.query(params![limit as i64])?;
            while let Some(row) = rows.next()? {
                sources.push(Self::row_to_source(row)?);
            }
            Ok(sources)
        }

        pub fn count_sources(&self) -> Result<usize> {
            let count: i64 = self
                .conn
                .query_row("SELECT COUNT(*) FROM sources", [], |row| row.get(0))?;
            Ok(count as usize)
        }

        // ─── Entry ─────────────────────────────────────────────

        pub fn insert_entry(&self, entry: &crate::Entry) -> Result<String> {
            let tags_json = serde_json::to_string(&entry.tags)?;
            self.conn.execute(
                "INSERT INTO entries (id, source_id, content, summary, tags, relevance_score, last_reread_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    entry.id,
                    entry.source_id,
                    entry.content,
                    entry.summary,
                    tags_json,
                    entry.relevance_score,
                    entry.last_reread_at.map(|dt| dt.to_rfc3339()),
                ],
            )?;
            Ok(entry.id.clone())
        }

        pub fn get_entry(&self, id: &str) -> Result<Option<crate::Entry>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, source_id, content, summary, tags, relevance_score, last_reread_at \
                 FROM entries WHERE id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_entry(row)?))
            } else {
                Ok(None)
            }
        }

        fn row_to_entry(row: &rusqlite::Row) -> rusqlite::Result<crate::Entry> {
            let tags_str: String = row.get(4)?;
            let last_reread_str: Option<String> = row.get(6)?;
            let last_reread_at = last_reread_str
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc));
            let tags: Vec<String> = serde_json::from_str(&tags_str).unwrap_or_default();
            Ok(crate::Entry {
                id: row.get(0)?,
                source_id: row.get(1)?,
                content: row.get(2)?,
                summary: row.get(3)?,
                tags,
                relevance_score: row.get(5)?,
                last_reread_at,
            })
        }

        pub fn search_entries(&self, query: &str, top_k: usize) -> Result<Vec<crate::Entry>> {
            let pattern = format!("%{query}%");
            let mut stmt = self.conn.prepare(
                "SELECT id, source_id, content, summary, tags, relevance_score, last_reread_at \
                 FROM entries WHERE content LIKE ?1 OR summary LIKE ?1 \
                 ORDER BY relevance_score DESC LIMIT ?2",
            )?;
            let rows = stmt.query_map(params![pattern, top_k as i64], Self::row_to_entry)?;
            let mut entries = Vec::new();
            for row in rows {
                entries.push(row?);
            }
            Ok(entries)
        }

        /// List entries, optionally filtered by source IDs.
        pub fn list_entries(&self, source_ids: Option<&[String]>) -> Result<Vec<crate::Entry>> {
            match source_ids {
                Some(ids) if !ids.is_empty() => {
                    let placeholders: Vec<String> = ids
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format!("?{}", i + 1))
                        .collect();
                    let sql = format!(
                        "SELECT id, source_id, content, summary, tags, relevance_score, last_reread_at \
                         FROM entries WHERE source_id IN ({}) ORDER BY relevance_score DESC LIMIT 1000",
                        placeholders.join(", ")
                    );
                    let params: Vec<&dyn rusqlite::ToSql> =
                        ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
                    let mut stmt = self.conn.prepare(&sql)?;
                    let mut entries = Vec::new();
                    let mut rows = stmt.query(params.as_slice())?;
                    while let Some(row) = rows.next()? {
                        entries.push(Self::row_to_entry(row)?);
                    }
                    Ok(entries)
                }
                _ => {
                    let mut stmt = self.conn.prepare(
                        "SELECT id, source_id, content, summary, tags, relevance_score, last_reread_at \
                         FROM entries ORDER BY relevance_score DESC LIMIT 1000",
                    )?;
                    let mut entries = Vec::new();
                    let mut rows = stmt.query([])?;
                    while let Some(row) = rows.next()? {
                        entries.push(Self::row_to_entry(row)?);
                    }
                    Ok(entries)
                }
            }
        }

        /// Fetch sources by their IDs.
        pub fn list_sources_by_ids(&self, ids: &[String]) -> Result<Vec<crate::Source>> {
            if ids.is_empty() {
                return Ok(Vec::new());
            }
            let placeholders: Vec<String> = ids
                .iter()
                .enumerate()
                .map(|(i, _)| format!("?{}", i + 1))
                .collect();
            let sql = format!(
                "SELECT id, url, title, source_type, added_at FROM sources WHERE id IN ({})",
                placeholders.join(", ")
            );
            let params: Vec<&dyn rusqlite::ToSql> =
                ids.iter().map(|s| s as &dyn rusqlite::ToSql).collect();
            let mut stmt = self.conn.prepare(&sql)?;
            let mut sources = Vec::new();
            let mut rows = stmt.query(params.as_slice())?;
            while let Some(row) = rows.next()? {
                sources.push(Self::row_to_source(row)?);
            }
            Ok(sources)
        }

        /// Update the relevance score for an entry.
        pub fn update_entry_relevance(&self, entry_id: &str, relevance_score: f64) -> Result<()> {
            self.conn.execute(
                "UPDATE entries SET relevance_score = ?2 WHERE id = ?1",
                params![entry_id, relevance_score],
            )?;
            Ok(())
        }

        // ─── Query logging ───────────────────────────────────

        pub fn log_query(&self, query: &crate::RadarQuery) -> Result<String> {
            self.conn.execute(
                "INSERT INTO queries (id, query_text, created_at) VALUES (?1, ?2, ?3)",
                params![query.id, query.query_text, query.created_at.to_rfc3339()],
            )?;
            Ok(query.id.clone())
        }

        pub fn insert_result(&self, result: &crate::RadarResult) -> Result<String> {
            self.conn.execute(
                "INSERT INTO results (id, query_id, entry_id, score, retrieved_at) VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    result.id,
                    result.query_id,
                    result.entry_id,
                    result.score,
                    result.retrieved_at.to_rfc3339(),
                ],
            )?;
            Ok(result.id.clone())
        }

        // ─── Profile ─────────────────────────────────────────

        pub fn insert_profile(&self, profile: &crate::Profile) -> Result<String> {
            let keywords_json = serde_json::to_string(&profile.keywords)?;
            let neg_keywords_json = serde_json::to_string(&profile.negative_keywords)?;
            let sources_json = serde_json::to_string(&profile.sources)?;
            self.conn.execute(
                "INSERT INTO profiles (id, name, keywords, negative_keywords, sources, \
                 scoring_prompt, score_threshold, max_llm_calls, created_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    profile.id,
                    profile.name,
                    keywords_json,
                    neg_keywords_json,
                    sources_json,
                    profile.scoring_prompt,
                    profile.score_threshold,
                    profile.max_llm_calls,
                    profile.created_at.to_rfc3339(),
                ],
            )?;
            Ok(profile.id.clone())
        }

        pub fn get_profile(&self, id: &str) -> Result<Option<crate::Profile>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, name, keywords, negative_keywords, sources, scoring_prompt, \
                 score_threshold, max_llm_calls, created_at FROM profiles WHERE id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_profile(row)?))
            } else {
                Ok(None)
            }
        }

        fn row_to_profile(
            row: &rusqlite::Row,
        ) -> std::result::Result<crate::Profile, StorageError> {
            let keywords_str: String = row.get(2)?;
            let neg_keywords_str: String = row.get(3)?;
            let sources_str: String = row.get(4)?;
            let created_str: String = row.get(8)?;
            let created_at = DateTime::parse_from_rfc3339(&created_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            Ok(crate::Profile {
                id: row.get(0)?,
                name: row.get(1)?,
                keywords: serde_json::from_str(&keywords_str).unwrap_or_default(),
                negative_keywords: serde_json::from_str(&neg_keywords_str).unwrap_or_default(),
                sources: serde_json::from_str(&sources_str).unwrap_or_default(),
                scoring_prompt: row.get(5)?,
                score_threshold: row.get(6)?,
                max_llm_calls: row.get(7)?,
                created_at,
            })
        }

        pub fn list_profiles(&self) -> Result<Vec<crate::Profile>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, name, keywords, negative_keywords, sources, scoring_prompt, \
                 score_threshold, max_llm_calls, created_at FROM profiles ORDER BY created_at DESC",
            )?;
            let mut profiles = Vec::new();
            let mut rows = stmt.query([])?;
            while let Some(row) = rows.next()? {
                profiles.push(Self::row_to_profile(row)?);
            }
            Ok(profiles)
        }

        pub fn update_profile(&self, profile: &crate::Profile) -> Result<()> {
            let keywords_json = serde_json::to_string(&profile.keywords)?;
            let neg_keywords_json = serde_json::to_string(&profile.negative_keywords)?;
            let sources_json = serde_json::to_string(&profile.sources)?;
            self.conn.execute(
                "UPDATE profiles SET name = ?2, keywords = ?3, negative_keywords = ?4, \
                 sources = ?5, scoring_prompt = ?6, score_threshold = ?7, max_llm_calls = ?8 \
                 WHERE id = ?1",
                params![
                    profile.id,
                    profile.name,
                    keywords_json,
                    neg_keywords_json,
                    sources_json,
                    profile.scoring_prompt,
                    profile.score_threshold,
                    profile.max_llm_calls,
                ],
            )?;
            Ok(())
        }

        pub fn delete_profile(&self, id: &str) -> Result<()> {
            // Cascade delete related scan_jobs first, then subscriptions, then item_scores.
            self.conn
                .execute("DELETE FROM scan_jobs WHERE profile_id = ?1", params![id])?;
            self.conn.execute(
                "DELETE FROM subscriptions WHERE profile_id = ?1",
                params![id],
            )?;
            self.conn
                .execute("DELETE FROM item_scores WHERE profile_id = ?1", params![id])?;
            self.conn
                .execute("DELETE FROM profiles WHERE id = ?1", params![id])?;
            Ok(())
        }

        // ─── ScanJob ────────────────────────────────────────

        pub fn enqueue_job(
            &self,
            profile_id: &str,
            reason: Option<String>,
        ) -> Result<crate::ScanJob> {
            if let Some(job) = self.get_active_scan_job(profile_id)? {
                return Ok(job);
            }
            let job = crate::ScanJob::new(profile_id.to_string(), reason);
            self.insert_scan_job(&job)?;
            Ok(job)
        }

        pub fn insert_scan_job(&self, job: &crate::ScanJob) -> Result<String> {
            self.conn.execute(
                "INSERT INTO scan_jobs (id, profile_id, status, progress, total, reason, created_at, completed_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    job.id,
                    job.profile_id,
                    job.status.as_str(),
                    job.progress,
                    job.total,
                    job.reason,
                    job.created_at.to_rfc3339(),
                    job.completed_at.map(|dt| dt.to_rfc3339()),
                ],
            )?;
            Ok(job.id.clone())
        }

        pub fn get_scan_job(&self, id: &str) -> Result<Option<crate::ScanJob>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, profile_id, status, progress, total, reason, created_at, completed_at \
                 FROM scan_jobs WHERE id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_scan_job(row)?))
            } else {
                Ok(None)
            }
        }

        fn row_to_scan_job(
            row: &rusqlite::Row,
        ) -> std::result::Result<crate::ScanJob, StorageError> {
            let status_str: String = row.get(2)?;
            let created_str: String = row.get(6)?;
            let completed_str: Option<String> = row.get(7)?;
            let created_at = DateTime::parse_from_rfc3339(&created_str)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());
            let completed_at = completed_str
                .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                .map(|dt| dt.with_timezone(&Utc));
            Ok(crate::ScanJob {
                id: row.get(0)?,
                profile_id: row.get(1)?,
                status: crate::ScanJobStatus::from_str(&status_str),
                progress: row.get::<_, i64>(3)? as u32,
                total: row.get::<_, i64>(4)? as u32,
                reason: row.get(5)?,
                created_at,
                completed_at,
            })
        }

        pub fn claim_scan_job(&self, job_id: &str) -> Result<Option<crate::ScanJob>> {
            if let Some(job) = self.get_scan_job(job_id)? {
                if job.status == crate::ScanJobStatus::Pending {
                    self.conn.execute(
                        "UPDATE scan_jobs SET status = 'running' WHERE id = ?1 AND status = 'pending'",
                        params![job_id],
                    )?;
                    return self.get_scan_job(job_id);
                }
            }
            Ok(None)
        }

        pub fn fail_scan_job(&self, job_id: &str) -> Result<()> {
            self.conn.execute(
                "UPDATE scan_jobs SET status = 'failed', completed_at = ?2 WHERE id = ?1",
                params![job_id, Utc::now().to_rfc3339()],
            )?;
            Ok(())
        }

        pub fn update_scan_job(&self, job: &crate::ScanJob) -> Result<()> {
            self.conn.execute(
                "UPDATE scan_jobs SET status = ?2, progress = ?3, total = ?4, completed_at = ?5 WHERE id = ?1",
                params![
                    job.id,
                    job.status.as_str(),
                    job.progress,
                    job.total,
                    job.completed_at.map(|dt| dt.to_rfc3339()),
                ],
            )?;
            Ok(())
        }

        pub fn list_scan_jobs(
            &self,
            profile_id: &str,
            limit: usize,
        ) -> Result<Vec<crate::ScanJob>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, profile_id, status, progress, total, reason, created_at, completed_at \
                 FROM scan_jobs WHERE profile_id = ?1 ORDER BY created_at DESC LIMIT ?2",
            )?;
            let mut jobs = Vec::new();
            let mut rows = stmt.query(params![profile_id, limit as i64])?;
            while let Some(row) = rows.next()? {
                jobs.push(Self::row_to_scan_job(row)?);
            }
            Ok(jobs)
        }

        pub fn get_active_scan_job(&self, profile_id: &str) -> Result<Option<crate::ScanJob>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, profile_id, status, progress, total, reason, created_at, completed_at \
                 FROM scan_jobs WHERE profile_id = ?1 AND status IN ('pending', 'running') \
                 ORDER BY created_at DESC LIMIT 1",
            )?;
            let mut rows = stmt.query(params![profile_id])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_scan_job(row)?))
            } else {
                Ok(None)
            }
        }

        pub fn claim_next_scan_job(&self) -> Result<Option<crate::ScanJob>> {
            let mut stmt = self.conn.prepare(
                "SELECT id FROM scan_jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1",
            )?;
            let mut rows = stmt.query([])?;
            if let Some(row) = rows.next()? {
                let id: String = row.get(0)?;
                drop(rows);
                self.claim_scan_job(&id)
            } else {
                Ok(None)
            }
        }

        // ─── Subscription ───────────────────────────────────

        pub fn insert_subscription(&self, sub: &crate::Subscription) -> Result<String> {
            let config_json = serde_json::to_string(&sub.config)?;
            self.conn.execute(
                "INSERT INTO subscriptions (id, profile_id, channel, config, enabled) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    sub.id,
                    sub.profile_id,
                    sub.channel,
                    config_json,
                    sub.enabled as i32,
                ],
            )?;
            Ok(sub.id.clone())
        }

        pub fn get_subscription(&self, id: &str) -> Result<Option<crate::Subscription>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, profile_id, channel, config, enabled FROM subscriptions WHERE id = ?1",
            )?;
            let mut rows = stmt.query(params![id])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_subscription(row)?))
            } else {
                Ok(None)
            }
        }

        pub fn get_subscription_by_profile_channel(
            &self,
            profile_id: &str,
            channel: &str,
        ) -> Result<Option<crate::Subscription>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, profile_id, channel, config, enabled FROM subscriptions \
                 WHERE profile_id = ?1 AND channel = ?2 LIMIT 1",
            )?;
            let mut rows = stmt.query(params![profile_id, channel])?;
            if let Some(row) = rows.next()? {
                Ok(Some(Self::row_to_subscription(row)?))
            } else {
                Ok(None)
            }
        }

        pub fn update_subscription(&self, sub: &crate::Subscription) -> Result<()> {
            let config_json = serde_json::to_string(&sub.config)?;
            self.conn.execute(
                "UPDATE subscriptions SET config = ?2, enabled = ?3 WHERE id = ?1",
                params![sub.id, config_json, sub.enabled as i32],
            )?;
            Ok(())
        }

        fn row_to_subscription(
            row: &rusqlite::Row,
        ) -> std::result::Result<crate::Subscription, StorageError> {
            let config_str: String = row.get(3)?;
            let enabled_i32: i32 = row.get(4)?;
            Ok(crate::Subscription {
                id: row.get(0)?,
                profile_id: row.get(1)?,
                channel: row.get(2)?,
                config: serde_json::from_str(&config_str)
                    .unwrap_or(serde_json::Value::Object(Default::default())),
                enabled: enabled_i32 != 0,
            })
        }

        pub fn upsert_item_score(
            &self,
            entry_id: &str,
            profile_id: &str,
            score: f64,
            disposition: &str,
        ) -> Result<String> {
            let id = Uuid::new_v4().to_string();
            self.conn.execute(
                "INSERT INTO item_scores (id, entry_id, profile_id, score, disposition) \
                 VALUES (?1, ?2, ?3, ?4, ?5) \
                 ON CONFLICT(entry_id, profile_id) DO UPDATE SET score = ?4, disposition = ?5",
                params![id, entry_id, profile_id, score, disposition],
            )?;
            Ok(id)
        }

        pub fn get_items_by_profile(
            &self,
            profile_id: &str,
            disposition: Option<&str>,
            min_score: Option<f64>,
            limit: usize,
            offset: usize,
        ) -> Result<Vec<crate::ScoredMatch>> {
            let mut sql = String::from(
                "SELECT e.id, e.source_id, e.content, e.summary, e.tags, e.relevance_score, e.last_reread_at, \
                 i.score, i.disposition FROM item_scores i JOIN entries e ON e.id = i.entry_id \
                 WHERE i.profile_id = ?1",
            );
            let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(profile_id.to_string())];
            if let Some(disp) = disposition {
                sql.push_str(" AND i.disposition = ?2");
                params_vec.push(Box::new(disp.to_string()));
            }
            if let Some(ms) = min_score {
                let idx = params_vec.len() + 1;
                sql.push_str(&format!(" AND i.score >= ?{idx}"));
                params_vec.push(Box::new(ms));
            }
            let limit_idx = params_vec.len() + 1;
            let offset_idx = params_vec.len() + 2;
            sql.push_str(&format!(
                " ORDER BY i.score DESC LIMIT ?{limit_idx} OFFSET ?{offset_idx}"
            ));
            params_vec.push(Box::new(limit as i64));
            params_vec.push(Box::new(offset as i64));
            let params_refs: Vec<&dyn rusqlite::ToSql> =
                params_vec.iter().map(|p| p.as_ref()).collect();

            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_refs.as_slice(), |row| {
                let entry = crate::Entry {
                    id: row.get(0)?,
                    source_id: row.get(1)?,
                    content: row.get(2)?,
                    summary: row.get(3)?,
                    tags: serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                    relevance_score: row.get(5)?,
                    last_reread_at: row
                        .get::<_, Option<String>>(6)?
                        .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                        .map(|dt| dt.with_timezone(&Utc)),
                };
                let score: f64 = row.get(7)?;
                let disposition: String = row.get(8)?;
                Ok(crate::ScoredMatch {
                    entry,
                    profile_id: profile_id.to_string(),
                    score,
                    disposition,
                })
            })?;
            let mut matches = Vec::new();
            for row in rows {
                matches.push(row?);
            }
            Ok(matches)
        }

        // ─── Notifications ──────────────────────────────────

        /// Record that a notification was sent for a (profile, item, channel) tuple.
        pub fn record_notification(
            &self,
            profile_id: &str,
            item_id: &str,
            channel: &str,
        ) -> Result<()> {
            let id = Uuid::new_v4().to_string();
            self.conn.execute(
                "INSERT OR IGNORE INTO notifications (id, profile_id, item_id, channel, sent_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    id,
                    profile_id,
                    item_id,
                    channel,
                    chrono::Utc::now().to_rfc3339(),
                ],
            )?;
            Ok(())
        }

        /// Get the set of item IDs that have already been notified for a profile + channel.
        pub fn get_notified_items(
            &self,
            profile_id: &str,
            channel: &str,
        ) -> Result<std::collections::HashSet<String>> {
            let mut stmt = self.conn.prepare(
                "SELECT item_id FROM notifications WHERE profile_id = ?1 AND channel = ?2",
            )?;
            let mut rows = stmt.query(params![profile_id, channel])?;
            let mut set = std::collections::HashSet::new();
            while let Some(row) = rows.next()? {
                let item_id: String = row.get(0)?;
                set.insert(item_id);
            }
            Ok(set)
        }

        /// Get enabled subscriptions for a profile.
        pub fn get_enabled_subscriptions(
            &self,
            profile_id: &str,
        ) -> Result<Vec<crate::Subscription>> {
            let mut stmt = self.conn.prepare(
                "SELECT id, profile_id, channel, config, enabled FROM subscriptions \
                 WHERE profile_id = ?1 AND enabled = 1",
            )?;
            let mut subs = Vec::new();
            let mut rows = stmt.query(params![profile_id])?;
            while let Some(row) = rows.next()? {
                subs.push(Self::row_to_subscription(row)?);
            }
            Ok(subs)
        }

        pub fn get_source_health(&self, source_type: Option<&str>) -> Result<Vec<SourceHealth>> {
            let base_sql =
                "SELECT s.source_type, MAX(s.added_at) AS last_scan, COUNT(e.id) AS items_count, \
                 COALESCE(AVG(e.relevance_score), 0.0) AS avg_relevance \
                 FROM sources s LEFT JOIN entries e ON e.source_id = s.id";
            let grouped_sql = " GROUP BY s.source_type ORDER BY s.source_type ASC";

            let mut health = Vec::new();
            match source_type {
                Some(kind) => {
                    let sql = format!("{base_sql} WHERE s.source_type = ?1{grouped_sql}");
                    let mut stmt = self.conn.prepare(&sql)?;
                    let mut rows = stmt.query(params![kind])?;
                    while let Some(row) = rows.next()? {
                        let items_count: i64 = row.get(2)?;
                        let avg_relevance: f64 = row.get(3)?;
                        health.push(SourceHealth {
                            source_type: row.get(0)?,
                            status: if items_count > 0 {
                                "ready".into()
                            } else {
                                "empty".into()
                            },
                            last_scan: row.get(1)?,
                            items_count: items_count as u64,
                            avg_relevance,
                        });
                    }
                }
                None => {
                    let sql = format!("{base_sql}{grouped_sql}");
                    let mut stmt = self.conn.prepare(&sql)?;
                    let mut rows = stmt.query([])?;
                    while let Some(row) = rows.next()? {
                        let items_count: i64 = row.get(2)?;
                        let avg_relevance: f64 = row.get(3)?;
                        health.push(SourceHealth {
                            source_type: row.get(0)?,
                            status: if items_count > 0 {
                                "ready".into()
                            } else {
                                "empty".into()
                            },
                            last_scan: row.get(1)?,
                            items_count: items_count as u64,
                            avg_relevance,
                        });
                    }
                }
            }
            Ok(health)
        }
    }

    /// Health status for a source.
    #[derive(Debug, Clone, serde::Serialize)]
    pub struct SourceHealth {
        pub source_type: String,
        pub status: String,
        pub last_scan: Option<String>,
        pub items_count: u64,
        pub avg_relevance: f64,
    }

    // ─── Tests ────────────────────────────────────────────────

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{
            Entry, Profile, RadarQuery, RadarResult, ScanJob, ScanJobStatus, Source, SourceType,
            Subscription,
        };

        fn memory_pool() -> DbPool {
            let conn = Connection::open_in_memory().unwrap();
            conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
            let pool = DbPool { conn };
            pool.run_migrations().unwrap();
            pool
        }

        #[test]
        fn insert_and_get_source() {
            let pool = memory_pool();
            let src = Source::new(
                "https://example.com".into(),
                "Example".into(),
                SourceType::Web,
            );
            pool.insert_source(&src).unwrap();
            let fetched = pool.get_source(&src.id).unwrap().unwrap();
            assert_eq!(fetched.url, "https://example.com");
        }

        #[test]
        fn insert_and_get_entry() {
            let pool = memory_pool();
            let src = Source::new(
                "https://example.com".into(),
                "Example".into(),
                SourceType::Web,
            );
            pool.insert_source(&src).unwrap();
            let mut entry = Entry::new(src.id.clone(), "AI safety research paper".into());
            entry.tags = vec!["ai".into(), "safety".into()];
            pool.insert_entry(&entry).unwrap();
            let fetched = pool.get_entry(&entry.id).unwrap().unwrap();
            assert_eq!(fetched.tags, vec!["ai", "safety"]);
        }

        #[test]
        fn search_entries() {
            let pool = memory_pool();
            let src = Source::new(
                "https://example.com".into(),
                "Example".into(),
                SourceType::Web,
            );
            pool.insert_source(&src).unwrap();
            let entry = Entry::new(src.id.clone(), "Transformers for NLP.".into());
            pool.insert_entry(&entry).unwrap();
            let results = pool.search_entries("transformers", 5).unwrap();
            assert_eq!(results.len(), 1);
        }

        #[test]
        fn log_query_and_result() {
            let pool = memory_pool();
            let src = Source::new(
                "https://example.com".into(),
                "Example".into(),
                SourceType::Web,
            );
            pool.insert_source(&src).unwrap();
            let entry = Entry::new(src.id.clone(), "AI content".into());
            pool.insert_entry(&entry).unwrap();
            let q = RadarQuery::new("AI".into());
            pool.log_query(&q).unwrap();
            let result = RadarResult::new(q.id.clone(), entry.id.clone(), 0.92);
            pool.insert_result(&result).unwrap();
            let entries = pool.search_entries("AI", 5).unwrap();
            assert_eq!(entries.len(), 1);
        }

        #[test]
        fn insert_and_get_profile() {
            let pool = memory_pool();
            let profile = Profile::new("AI Research".into(), vec!["AI".into(), "ML".into()]);
            pool.insert_profile(&profile).unwrap();
            let fetched = pool.get_profile(&profile.id).unwrap().unwrap();
            assert_eq!(fetched.name, "AI Research");
            assert_eq!(fetched.keywords, vec!["AI", "ML"]);
        }

        #[test]
        fn scan_job_pending() {
            let pool = memory_pool();
            let profile = Profile::new("Test".into(), vec!["test".into()]);
            pool.insert_profile(&profile).unwrap();
            let job = ScanJob::new(profile.id.clone(), Some("test".into()));
            pool.insert_scan_job(&job).unwrap();
            let active = pool.get_active_scan_job(&profile.id).unwrap().unwrap();
            assert_eq!(active.status, ScanJobStatus::Pending);
        }

        #[test]
        fn claim_scan_job() {
            let pool = memory_pool();
            let profile = Profile::new("Test".into(), vec!["test".into()]);
            pool.insert_profile(&profile).unwrap();
            let job = ScanJob::new(profile.id.clone(), None);
            pool.insert_scan_job(&job).unwrap();
            let claimed = pool.claim_scan_job(&job.id).unwrap().unwrap();
            assert_eq!(claimed.status, ScanJobStatus::Running);
        }

        #[test]
        fn subscription_insert() {
            let pool = memory_pool();
            let profile = Profile::new("Test".into(), vec!["test".into()]);
            pool.insert_profile(&profile).unwrap();
            let sub = Subscription::new(
                profile.id.clone(),
                "email".into(),
                serde_json::json!({"address": "test@example.com"}),
                true,
            );
            pool.insert_subscription(&sub).unwrap();
            let fetched = pool.get_subscription(&sub.id).unwrap().unwrap();
            assert_eq!(fetched.channel, "email");
            assert!(fetched.enabled);
        }

        #[test]
        fn upsert_item_score() {
            let pool = memory_pool();
            let profile = Profile::new("Test".into(), vec!["AI".into()]);
            pool.insert_profile(&profile).unwrap();
            let src = Source::new(
                "https://example.com".into(),
                "Example".into(),
                SourceType::Web,
            );
            pool.insert_source(&src).unwrap();
            let entry = Entry::new(src.id.clone(), "AI safety paper".into());
            pool.insert_entry(&entry).unwrap();
            pool.upsert_item_score(&entry.id, &profile.id, 0.85, "new")
                .unwrap();
            let matches = pool
                .get_items_by_profile(&profile.id, None, Some(0.5), 10, 0)
                .unwrap();
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].score, 0.85);
        }
    }
}

// ─── LanceDB (evolve loop) ─────────────────────────────────────────────────

pub mod lance_store {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BooleanArray, Float32Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use chrono::{DateTime, Utc};
    use futures::TryStreamExt;
    use lancedb::arrow::SendableRecordBatchStream;
    use lancedb::connection::Connection;
    use lancedb::query::{ExecutableQuery, QueryBase};
    use lancedb::table::Table;

    use crate::finding::{PaperRef, UrgencyLevel};
    use crate::Finding;

    #[derive(Debug, thiserror::Error)]
    pub enum LanceError {
        #[error("LanceDB error: {0}")]
        Lance(#[from] lancedb::error::Error),
        #[error("IO error: {0}")]
        Io(#[from] std::io::Error),
        #[error("not found: {0}")]
        NotFound(String),
        #[error("serialization error: {0}")]
        Serde(#[from] serde_json::Error),
        #[error("arrow error: {0}")]
        Arrow(#[from] arrow::error::ArrowError),
    }

    pub type Result<T> = std::result::Result<T, LanceError>;

    /// LanceDB-backed store. The evolve loop contract.
    ///
    /// Stores Finding records with typed Arrow columns for efficient filtering by
    /// urgency, confidence, impact_weight, and is_actionable.
    ///
    /// Database path: `~/.research-radar/lance/` (separate from SQLite).
    pub struct RadarStore {
        conn: Connection,
    }

    impl RadarStore {
        /// Open (or create) the LanceDB store at `~/.research-radar/lance/`.
        pub async fn init() -> Result<Self> {
            let db_path = Self::db_path()?;
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            let uri = db_path.to_str().ok_or_else(|| {
                LanceError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "db path is not valid UTF-8",
                ))
            })?;
            let conn = lancedb::connection::connect(uri)
                .execute()
                .await
                .map_err(LanceError::Lance)?;
            let store = Self { conn };
            store.init_tables().await?;
            Ok(store)
        }

        fn db_path() -> Result<PathBuf> {
            let home = dirs::home_dir().ok_or_else(|| {
                LanceError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "cannot resolve home directory",
                ))
            })?;
            Ok(home.join(".research-radar/lance"))
        }

        async fn init_tables(&self) -> Result<()> {
            self.create_findings_table().await?;
            Ok(())
        }

        // ─── Findings ─────────────────────────────────────────

        fn findings_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("source_url", DataType::Utf8, false),
                Field::new("source_title", DataType::Utf8, false),
                Field::new("source_type", DataType::Utf8, false),
                Field::new("domain", DataType::Utf8, false),
                Field::new("title", DataType::Utf8, false),
                Field::new("summary", DataType::Utf8, false),
                Field::new("confidence", DataType::Float32, false),
                Field::new("impact_weight", DataType::Float32, false),
                Field::new("urgency", DataType::Utf8, false),
                Field::new("priority_score", DataType::Float32, false),
                Field::new("is_actionable", DataType::Boolean, false),
                Field::new("suggested_action", DataType::Utf8, false),
                Field::new("applicability_tags", DataType::Utf8, false),
                Field::new("cited_paper", DataType::Utf8, true),
                Field::new("discovered_at", DataType::Int64, false),
                Field::new("related_entry_ids", DataType::Utf8, false),
                Field::new("schema_version", DataType::Utf8, false),
            ]))
        }

        async fn create_findings_table(&self) -> Result<()> {
            match self
                .conn
                .create_empty_table("findings", Self::findings_schema())
                .execute()
                .await
            {
                Ok(_) => Ok(()),
                Err(e) if e.to_string().contains("already exists") => Ok(()),
                Err(e) => Err(LanceError::Lance(e)),
            }
        }

        async fn findings_table(&self) -> Result<Table> {
            Ok(self.conn.open_table("findings").execute().await?)
        }

        /// Insert a Finding. Returns the finding id.
        pub async fn insert_finding(&self, finding: &Finding) -> Result<String> {
            let batch = self.findings_to_batch(std::iter::once(finding))?;
            let table = self.findings_table().await?;
            table.add(batch).execute().await?;
            Ok(finding.id.clone())
        }

        /// Retrieve a Finding by id.
        pub async fn get_finding(&self, id: &str) -> Result<Option<Finding>> {
            let table = self.findings_table().await?;
            let filter = format!("id = '{id}'");
            let results = table.query().only_if(&filter).execute().await?;
            let findings = self.batch_to_findings(results).await?;
            Ok(findings.into_iter().next())
        }

        /// List all findings, most recent first.
        pub async fn list_findings(&self, limit: usize) -> Result<Vec<Finding>> {
            let table = self.findings_table().await?;
            let results = table.query().limit(limit).execute().await?;
            self.batch_to_findings(results).await
        }

        /// List findings filtered by urgency level.
        pub async fn list_findings_by_urgency(
            &self,
            urgency: UrgencyLevel,
            limit: usize,
        ) -> Result<Vec<Finding>> {
            let table = self.findings_table().await?;
            let filter = format!("urgency = '{}'", urgency.as_str());
            let results = table
                .query()
                .only_if(&filter)
                .limit(limit)
                .execute()
                .await?;
            self.batch_to_findings(results).await
        }

        /// List actionable findings (confidence >= 0.4, urgency != Low).
        pub async fn list_actionable_findings(&self, limit: usize) -> Result<Vec<Finding>> {
            let table = self.findings_table().await?;
            let results = table
                .query()
                .only_if("is_actionable = true")
                .limit(limit)
                .execute()
                .await?;
            self.batch_to_findings(results).await
        }

        // ─── Arrow conversion ─────────────────────────────────

        fn findings_to_batch<'a>(
            &self,
            findings: impl Iterator<Item = &'a Finding>,
        ) -> Result<RecordBatch> {
            let mut ids = Vec::new();
            let mut source_urls = Vec::new();
            let mut source_titles = Vec::new();
            let mut source_types = Vec::new();
            let mut domains = Vec::new();
            let mut titles = Vec::new();
            let mut summaries = Vec::new();
            let mut confidences = Vec::new();
            let mut impact_weights = Vec::new();
            let mut urgencies = Vec::new();
            let mut priority_scores = Vec::new();
            let mut is_actionables = Vec::new();
            let mut suggested_actions = Vec::new();
            let mut applicability_tags = Vec::new();
            let mut cited_papers = Vec::new();
            let mut discovered_ats = Vec::new();
            let mut related_entry_ids = Vec::new();
            let mut schema_versions = Vec::new();

            for f in findings {
                ids.push(f.id.clone());
                source_urls.push(f.source_url.clone());
                source_titles.push(f.source_title.clone());
                source_types.push(f.source_type.as_str().to_string());
                domains.push(f.domain.clone());
                titles.push(f.title.clone());
                summaries.push(f.summary.clone());
                confidences.push(f.confidence);
                impact_weights.push(f.impact_weight);
                urgencies.push(f.urgency.as_str().to_string());
                priority_scores.push(f.priority_score());
                is_actionables.push(f.is_actionable());
                suggested_actions.push(f.suggested_action.clone());
                applicability_tags
                    .push(serde_json::to_string(&f.applicability_tags).unwrap_or_default());
                cited_papers.push(
                    f.cited_paper
                        .as_ref()
                        .map(|p| serde_json::to_string(p).unwrap_or_default())
                        .unwrap_or_default(),
                );
                discovered_ats.push(f.discovered_at.timestamp_millis());
                related_entry_ids
                    .push(serde_json::to_string(&f.related_entry_ids).unwrap_or_default());
                schema_versions.push(f.schema_version.clone());
            }

            RecordBatch::try_new(
                Self::findings_schema(),
                vec![
                    Arc::new(StringArray::from(ids)) as ArrayRef,
                    Arc::new(StringArray::from(source_urls)),
                    Arc::new(StringArray::from(source_titles)),
                    Arc::new(StringArray::from(source_types)),
                    Arc::new(StringArray::from(domains)),
                    Arc::new(StringArray::from(titles)),
                    Arc::new(StringArray::from(summaries)),
                    Arc::new(Float32Array::from(confidences)),
                    Arc::new(Float32Array::from(impact_weights)),
                    Arc::new(StringArray::from(urgencies)),
                    Arc::new(Float32Array::from(priority_scores)),
                    Arc::new(BooleanArray::from(is_actionables)),
                    Arc::new(StringArray::from(suggested_actions)),
                    Arc::new(StringArray::from(applicability_tags)),
                    Arc::new(StringArray::from(cited_papers)),
                    Arc::new(Int64Array::from(discovered_ats)),
                    Arc::new(StringArray::from(related_entry_ids)),
                    Arc::new(StringArray::from(schema_versions)),
                ],
            )
            .map_err(LanceError::Arrow)
        }

        async fn batch_to_findings(
            &self,
            stream: SendableRecordBatchStream,
        ) -> Result<Vec<Finding>> {
            use arrow::record_batch::RecordBatch;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            let mut findings = Vec::new();
            for batch in &batches {
                findings.extend(self.record_batch_to_findings(batch)?);
            }
            Ok(findings)
        }

        fn record_batch_to_findings(&self, batch: &RecordBatch) -> Result<Vec<Finding>> {
            use arrow_array::StringArray;

            let id_arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let source_url_arr = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let source_title_arr = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let source_type_arr = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let domain_arr = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let title_arr = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let summary_arr = batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let confidence_arr = batch
                .column(7)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let impact_weight_arr = batch
                .column(8)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let urgency_arr = batch
                .column(9)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let _priority_score_arr = batch
                .column(10)
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap();
            let _is_actionable_arr = batch
                .column(11)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            let suggested_action_arr = batch
                .column(12)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let applicability_tags_arr = batch
                .column(13)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let cited_paper_arr = batch
                .column(14)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let discovered_at_arr = batch
                .column(15)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let related_entry_ids_arr = batch
                .column(16)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let schema_version_arr = batch
                .column(17)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let num_rows = batch.num_rows();
            let mut findings = Vec::with_capacity(num_rows);

            for i in 0..num_rows {
                let cited_paper_raw = cited_paper_arr.value(i);
                let cited_paper: Option<PaperRef> = if cited_paper_raw.is_empty() {
                    None
                } else {
                    serde_json::from_str(cited_paper_raw).ok()
                };

                let applicability_tags: Vec<String> =
                    serde_json::from_str(applicability_tags_arr.value(i)).unwrap_or_default();

                let related_entry_ids: Vec<String> =
                    serde_json::from_str(related_entry_ids_arr.value(i)).unwrap_or_default();

                let discovered_at_ms = discovered_at_arr.value(i);
                let discovered_at =
                    DateTime::from_timestamp_millis(discovered_at_ms).unwrap_or_else(Utc::now);

                findings.push(Finding {
                    id: id_arr.value(i).to_string(),
                    source_url: source_url_arr.value(i).to_string(),
                    source_title: source_title_arr.value(i).to_string(),
                    source_type: crate::SourceType::from_str(source_type_arr.value(i)),
                    domain: domain_arr.value(i).to_string(),
                    title: title_arr.value(i).to_string(),
                    summary: summary_arr.value(i).to_string(),
                    confidence: confidence_arr.value(i),
                    impact_weight: impact_weight_arr.value(i),
                    urgency: UrgencyLevel::from_str(urgency_arr.value(i)),
                    suggested_action: suggested_action_arr.value(i).to_string(),
                    applicability_tags,
                    cited_paper,
                    discovered_at,
                    related_entry_ids,
                    schema_version: schema_version_arr.value(i).to_string(),
                });
            }

            Ok(findings)
        }
    }
}
