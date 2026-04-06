//! SQLite storage layer for research-radar-core.
//!
//! Database path: `~/.research-radar/data.db` (auto-created).
//! WAL mode is enabled for concurrent read performance.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::PathBuf;

use crate::{Entry, Profile, RadarQuery, RadarResult, ScanJob, ScanJobStatus, ScoredMatch, Source, SourceType, Subscription};
use uuid::Uuid;

// ─── Error ───────────────────────────────────────────────────────────

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

// ─── DbPool ──────────────────────────────────────────────────────────

/// Wraps a rusqlite Connection with typed helpers and auto-migration.
pub struct DbPool {
    pub(crate) conn: Connection,
}

impl DbPool {
    /// Open (or create) the database at `~/.research-radar/data.db`.
    /// Creates the parent directory if missing, runs migrations, enables WAL.
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

    /// Returns the canonical database path, expanding `~`.
    fn db_path() -> Result<PathBuf> {
        let home = dirs::home_dir().ok_or_else(|| {
            StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "cannot resolve home directory",
            ))
        })?;
        Ok(home.join(".research-radar/data.db"))
    }

    /// Run CREATE TABLE IF NOT EXISTS for all schema tables.
    /// Returns an in-memory DbPool for use in tests.
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
            CREATE INDEX IF NOT EXISTS idx_item_scores_entry  ON item_scores(entry_id);
            CREATE INDEX IF NOT EXISTS idx_item_scores_profile ON item_scores(profile_id);
            "#,
        )?;
        Ok(())
    }

    // ─── Source operations ───────────────────────────────────────────

    /// Insert a new Source. Returns the source id.
    pub fn insert_source(&self, source: &Source) -> Result<String> {
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

    /// Retrieve a Source by id.
    pub fn get_source(&self, id: &str) -> Result<Option<Source>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, url, title, source_type, added_at FROM sources WHERE id = ?1")?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::row_to_source(row)?))
        } else {
            Ok(None)
        }
    }

    fn row_to_source(row: &rusqlite::Row) -> Result<Source> {
        let added_str: String = row.get(4)?;
        let added_at = DateTime::parse_from_rfc3339(&added_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        Ok(Source {
            id: row.get(0)?,
            url: row.get(1)?,
            title: row.get(2)?,
            source_type: SourceType::from_str(&row.get::<_, String>(3)?),
            added_at,
        })
    }

    // ─── Entry operations ─────────────────────────────────────────────

    /// Insert a new Entry. Returns the entry id.
    pub fn insert_entry(&self, entry: &Entry) -> Result<String> {
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

    /// Retrieve an Entry by id.
    pub fn get_entry(&self, id: &str) -> Result<Option<Entry>> {
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

    fn row_to_entry(row: &rusqlite::Row) -> rusqlite::Result<Entry> {
        let tags_str: String = row.get(4)?;
        let last_reread_str: Option<String> = row.get(6)?;
        let last_reread_at = last_reread_str
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc));
        let tags: Vec<String> = serde_json::from_str(&tags_str).unwrap_or_default();
        Ok(Entry {
            id: row.get(0)?,
            source_id: row.get(1)?,
            content: row.get(2)?,
            summary: row.get(3)?,
            tags,
            relevance_score: row.get(5)?,
            last_reread_at,
        })
    }

    pub fn list_entries(&self, source_ids: Option<&[String]>) -> Result<Vec<Entry>> {
        let mut entries = Vec::new();

        if let Some(source_ids) = source_ids {
            if source_ids.is_empty() {
                return Ok(entries);
            }

            let placeholders = (0..source_ids.len())
                .map(|idx| format!("?{}", idx + 1))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT id, source_id, content, summary, tags, relevance_score, last_reread_at \
                 FROM entries WHERE source_id IN ({}) ORDER BY relevance_score DESC, id ASC",
                placeholders
            );
            let params: Vec<&dyn rusqlite::ToSql> = source_ids
                .iter()
                .map(|id| id as &dyn rusqlite::ToSql)
                .collect();
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params.as_slice(), Self::row_to_entry)?;
            for row in rows {
                entries.push(row?);
            }
        } else {
            let mut stmt = self.conn.prepare(
                "SELECT id, source_id, content, summary, tags, relevance_score, last_reread_at \
                 FROM entries ORDER BY relevance_score DESC, id ASC",
            )?;
            let rows = stmt.query_map([], Self::row_to_entry)?;
            for row in rows {
                entries.push(row?);
            }
        }

        Ok(entries)
    }

    pub fn update_entry_relevance(&self, entry_id: &str, score: f64) -> Result<()> {
        self.conn.execute(
            "UPDATE entries SET relevance_score = ?2, last_reread_at = ?3 WHERE id = ?1",
            params![entry_id, score, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// Simple keyword search over entries (LIKE-based). Returns up to `top_k` results
    /// ordered by relevance_score DESC.
    ///
    /// In a future phase this will delegate to a proper vector store.
    pub fn search_entries(&self, query: &str, top_k: usize) -> Result<Vec<Entry>> {
        let pattern = format!("%{query}%");
        let mut stmt = self.conn.prepare(
            "SELECT id, source_id, content, summary, tags, relevance_score, last_reread_at \
             FROM entries \
             WHERE content LIKE ?1 OR summary LIKE ?1 \
             ORDER BY relevance_score DESC \
             LIMIT ?2",
        )?;
        let rows = stmt.query_map(params![pattern, top_k as i64], Self::row_to_entry)?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    // ─── Query logging ────────────────────────────────────────────────

    /// Log a query. Returns the query id.
    pub fn log_query(&self, query: &RadarQuery) -> Result<String> {
        self.conn.execute(
            "INSERT INTO queries (id, query_text, created_at) VALUES (?1, ?2, ?3)",
            params![query.id, query.query_text, query.created_at.to_rfc3339()],
        )?;
        Ok(query.id.clone())
    }

    /// Record a retrieval result.
    pub fn insert_result(&self, result: &RadarResult) -> Result<String> {
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

    // ─── Source listing ────────────────────────────────────────────────

    /// List all sources, ordered by most recently added.
    pub fn list_sources(&self, limit: usize) -> Result<Vec<Source>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, url, title, source_type, added_at FROM sources \
             ORDER BY added_at DESC LIMIT ?1",
        )?;
        let mut sources = Vec::new();
        let mut rows = stmt.query(params![limit as i64])?;
        while let Some(row) = rows.next()? {
            sources.push(Self::row_to_source(row)?);
        }
        Ok(sources)
    }

    pub fn list_sources_by_ids(&self, ids: &[String]) -> Result<Vec<Source>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders = (0..ids.len())
            .map(|idx| format!("?{}", idx + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT id, url, title, source_type, added_at FROM sources WHERE id IN ({}) ORDER BY added_at DESC",
            placeholders
        );
        let params: Vec<&dyn rusqlite::ToSql> = ids.iter().map(|id| id as &dyn rusqlite::ToSql).collect();
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query(params.as_slice())?;
        let mut sources = Vec::new();
        while let Some(row) = rows.next()? {
            sources.push(Self::row_to_source(row)?);
        }
        Ok(sources)
    }

    /// Count total sources in the database.
    pub fn count_sources(&self) -> Result<usize> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM sources", [], |row| row.get(0))?;
        Ok(count as usize)
    }

    // ─── Profile operations ──────────────────────────────────────────

    /// Insert a new Profile. Returns the profile id.
    pub fn insert_profile(&self, profile: &Profile) -> Result<String> {
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

    /// Retrieve a Profile by id.
    pub fn get_profile(&self, id: &str) -> Result<Option<Profile>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, keywords, negative_keywords, sources, scoring_prompt, \
             score_threshold, max_llm_calls, created_at \
             FROM profiles WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::row_to_profile(row)?))
        } else {
            Ok(None)
        }
    }

    fn row_to_profile(row: &rusqlite::Row) -> Result<Profile> {
        let keywords_str: String = row.get(2)?;
        let neg_keywords_str: String = row.get(3)?;
        let sources_str: String = row.get(4)?;
        let created_str: String = row.get(8)?;
        let created_at = DateTime::parse_from_rfc3339(&created_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        Ok(Profile {
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

    /// Update a Profile. Only non-None fields are updated.
    pub fn update_profile(&self, profile: &Profile) -> Result<()> {
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

    /// List all profiles.
    pub fn list_profiles(&self) -> Result<Vec<Profile>> {
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

    // ─── ScanJob operations ──────────────────────────────────────────

    pub fn enqueue_job(&self, profile_id: &str, reason: Option<String>) -> Result<ScanJob> {
        if let Some(job) = self.get_active_scan_job(profile_id)? {
            return Ok(job);
        }

        let job = ScanJob::new(profile_id.to_string(), reason);
        self.insert_scan_job(&job)?;
        Ok(job)
    }

    /// Insert a new ScanJob. Returns the job id.
    pub fn insert_scan_job(&self, job: &ScanJob) -> Result<String> {
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

    /// Retrieve a ScanJob by id.
    pub fn get_scan_job(&self, id: &str) -> Result<Option<ScanJob>> {
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

    fn row_to_scan_job(row: &rusqlite::Row) -> Result<ScanJob> {
        let status_str: String = row.get(2)?;
        let created_str: String = row.get(6)?;
        let completed_str: Option<String> = row.get(7)?;
        let created_at = DateTime::parse_from_rfc3339(&created_str)
            .map(|dt| dt.with_timezone(&Utc))
            .unwrap_or_else(|_| Utc::now());
        let completed_at = completed_str
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|dt| dt.with_timezone(&Utc));
        Ok(ScanJob {
            id: row.get(0)?,
            profile_id: row.get(1)?,
            status: ScanJobStatus::from_str(&status_str),
            progress: row.get::<_, i64>(3)? as u32,
            total: row.get::<_, i64>(4)? as u32,
            reason: row.get(5)?,
            created_at,
            completed_at,
        })
    }

    pub fn claim_next_scan_job(&self) -> Result<Option<ScanJob>> {
        let mut stmt = self.conn.prepare(
            "SELECT id FROM scan_jobs WHERE status = 'pending' ORDER BY created_at ASC LIMIT 1",
        )?;
        let mut rows = stmt.query([])?;
        let Some(row) = rows.next()? else {
            return Ok(None);
        };
        let id: String = row.get(0)?;
        self.claim_scan_job(&id)
    }

    pub fn claim_scan_job(&self, job_id: &str) -> Result<Option<ScanJob>> {
        let job = match self.get_scan_job(job_id)? {
            Some(job) => job,
            None => return Ok(None),
        };

        if job.status != ScanJobStatus::Pending {
            return Ok(None);
        }

        self.conn.execute(
            "UPDATE scan_jobs SET status = 'running' WHERE id = ?1 AND status = 'pending'",
            params![job_id],
        )?;
        self.get_scan_job(job_id)
    }

    pub fn fail_scan_job(&self, job_id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE scan_jobs SET status = 'failed', completed_at = ?2 WHERE id = ?1",
            params![job_id, Utc::now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// Update a ScanJob.
    pub fn update_scan_job(&self, job: &ScanJob) -> Result<()> {
        self.conn.execute(
            "UPDATE scan_jobs SET status = ?2, progress = ?3, total = ?4, completed_at = ?5 \
             WHERE id = ?1",
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

    /// List scan jobs for a profile, most recent first.
    pub fn list_scan_jobs(&self, profile_id: &str, limit: usize) -> Result<Vec<ScanJob>> {
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

    /// Find the most recent pending/running job for a profile, if any.
    pub fn get_active_scan_job(&self, profile_id: &str) -> Result<Option<ScanJob>> {
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

    // ─── Subscription operations ─────────────────────────────────────

    /// Insert a new Subscription. Returns the subscription id.
    pub fn insert_subscription(&self, sub: &Subscription) -> Result<String> {
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

    /// Retrieve a Subscription by id.
    pub fn get_subscription(&self, id: &str) -> Result<Option<Subscription>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, profile_id, channel, config, enabled \
             FROM subscriptions WHERE id = ?1",
        )?;
        let mut rows = stmt.query(params![id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::row_to_subscription(row)?))
        } else {
            Ok(None)
        }
    }

    fn row_to_subscription(row: &rusqlite::Row) -> Result<Subscription> {
        let config_str: String = row.get(3)?;
        let enabled_i32: i32 = row.get(4)?;
        Ok(Subscription {
            id: row.get(0)?,
            profile_id: row.get(1)?,
            channel: row.get(2)?,
            config: serde_json::from_str(&config_str).unwrap_or(serde_json::Value::Object(Default::default())),
            enabled: enabled_i32 != 0,
        })
    }

    /// Update a Subscription.
    pub fn update_subscription(&self, sub: &Subscription) -> Result<()> {
        let config_json = serde_json::to_string(&sub.config)?;
        self.conn.execute(
            "UPDATE subscriptions SET channel = ?2, config = ?3, enabled = ?4 WHERE id = ?1",
            params![
                sub.id,
                sub.channel,
                config_json,
                sub.enabled as i32,
            ],
        )?;
        Ok(())
    }

    /// Get the subscription for a profile + channel, if it exists.
    pub fn get_subscription_by_profile_channel(
        &self,
        profile_id: &str,
        channel: &str,
    ) -> Result<Option<Subscription>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, profile_id, channel, config, enabled \
             FROM subscriptions WHERE profile_id = ?1 AND channel = ?2",
        )?;
        let mut rows = stmt.query(params![profile_id, channel])?;
        if let Some(row) = rows.next()? {
            Ok(Some(Self::row_to_subscription(row)?))
        } else {
            Ok(None)
        }
    }

    // ─── ItemScore operations ─────────────────────────────────────────

    /// Upsert an item score (insert or update on conflict).
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

    /// Get scored matches for a profile, optionally filtered by disposition and min_score.
    pub fn get_items_by_profile(
        &self,
        profile_id: &str,
        disposition: Option<&str>,
        min_score: Option<f64>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<ScoredMatch>> {
        let mut sql = String::from(
            "SELECT e.id, e.source_id, e.content, e.summary, e.tags, e.relevance_score, e.last_reread_at, \
             i.score, i.disposition \
             FROM item_scores i \
             JOIN entries e ON e.id = i.entry_id \
             WHERE i.profile_id = ?1",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![Box::new(profile_id.to_string())];

        if let Some(disp) = disposition {
            sql.push_str(" AND i.disposition = ?2");
            params_vec.push(Box::new(disp.to_string()));
        }
        if let Some(ms) = min_score {
            let idx = params_vec.len() + 1;
            sql.push_str(&format!(" AND i.score >= ?{}", idx));
            params_vec.push(Box::new(ms));
        }

        sql.push_str(&format!(" ORDER BY i.score DESC LIMIT ?{} OFFSET ?{}",
            params_vec.len() + 1, params_vec.len() + 2));
        params_vec.push(Box::new(limit as i64));
        params_vec.push(Box::new(offset as i64));

        let params_refs: Vec<&dyn rusqlite::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            let entry = Entry {
                id: row.get(0)?,
                source_id: row.get(1)?,
                content: row.get(2)?,
                summary: row.get(3)?,
                tags: serde_json::from_str(&row.get::<_, String>(4)?).unwrap_or_default(),
                relevance_score: row.get(5)?,
                last_reread_at: row.get::<_, Option<String>>(6)?
                    .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
                    .map(|dt| dt.with_timezone(&Utc)),
            };
            let score: f64 = row.get(7)?;
            let disposition: String = row.get(8)?;
            Ok(ScoredMatch {
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

    // ─── Source health ───────────────────────────────────────────────

    /// Returns per-source health stats.
    pub fn get_source_health(&self, source_type: Option<&str>) -> Result<Vec<SourceHealth>> {
        let sql = if source_type.is_some() {
            "SELECT s.source_type, COUNT(DISTINCT e.id) as items_count, \
             COALESCE(AVG(i.score), 0.0) as avg_relevance, MAX(e.last_reread_at) as last_scan \
             FROM sources s \
             LEFT JOIN entries e ON e.source_id = s.id \
             LEFT JOIN item_scores i ON i.entry_id = e.id \
             WHERE s.source_type = ?1 \
             GROUP BY s.id, s.source_type"
        } else {
            "SELECT s.source_type, COUNT(DISTINCT e.id) as items_count, \
             COALESCE(AVG(i.score), 0.0) as avg_relevance, MAX(e.last_reread_at) as last_scan \
             FROM sources s \
             LEFT JOIN entries e ON e.source_id = s.id \
             LEFT JOIN item_scores i ON i.entry_id = e.id \
             GROUP BY s.id, s.source_type"
        };

        let mut results = Vec::new();
        if let Some(st) = source_type {
            let mut stmt = self.conn.prepare(sql)?;
            let rows = stmt.query_map(params![st], |row| {
                let items_count: i64 = row.get(1)?;
                let avg_relevance: f64 = row.get(2)?;
                let last_scan_str: Option<String> = row.get(3)?;
                Ok(SourceHealth {
                    source_type: st.to_string(),
                    status: if items_count > 0 { "healthy".to_string() } else { "empty".to_string() },
                    last_scan: last_scan_str,
                    items_count: items_count as u64,
                    avg_relevance,
                })
            })?;
            for row in rows {
                results.push(row?);
            }
        } else {
            let mut stmt = self.conn.prepare(sql)?;
            let rows = stmt.query_map([], |row| {
                let source_type_str: String = row.get(0)?;
                let items_count: i64 = row.get(1)?;
                let avg_relevance: f64 = row.get(2)?;
                let last_scan_str: Option<String> = row.get(3)?;
                Ok(SourceHealth {
                    source_type: source_type_str,
                    status: if items_count > 0 { "healthy".to_string() } else { "empty".to_string() },
                    last_scan: last_scan_str,
                    items_count: items_count as u64,
                    avg_relevance,
                })
            })?;
            for row in rows {
                results.push(row?);
            }
        }
        Ok(results)
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

#[cfg(test)]
mod tests {
    use super::*;

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
        let src = Source::new("https://example.com".into(), "Example".into(), SourceType::Web);
        pool.insert_source(&src).unwrap();
        let fetched = pool.get_source(&src.id).unwrap().unwrap();
        assert_eq!(fetched.url, "https://example.com");
        assert_eq!(fetched.title, "Example");
    }

    #[test]
    fn insert_and_get_entry() {
        let pool = memory_pool();
        let src = Source::new("https://example.com".into(), "Example".into(), SourceType::Web);
        pool.insert_source(&src).unwrap();

        let mut entry = Entry::new(src.id.clone(), "Some content about AI safety.".into());
        entry.summary = Some("A short summary.".into());
        entry.tags = vec!["ai".into(), "safety".into()];
        entry.relevance_score = 0.85;

        pool.insert_entry(&entry).unwrap();
        let fetched = pool.get_entry(&entry.id).unwrap().unwrap();
        assert_eq!(fetched.content, "Some content about AI safety.");
        assert_eq!(fetched.tags, vec!["ai", "safety"]);
    }

    #[test]
    fn search_entries_finds_keyword() {
        let pool = memory_pool();
        let src = Source::new("https://example.com".into(), "Example".into(), SourceType::Web);
        pool.insert_source(&src).unwrap();

        let entry = Entry::new(src.id.clone(), "Transformers architecture for NLP.".into());
        pool.insert_entry(&entry).unwrap();

        let results = pool.search_entries("transformers", 5).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content, "Transformers architecture for NLP.");
    }

    #[test]
    fn log_query_and_insert_result() {
        let pool = memory_pool();
        let src = Source::new("https://example.com".into(), "Example".into(), SourceType::Web);
        pool.insert_source(&src).unwrap();
        let entry = Entry::new(src.id.clone(), "Content about query".into());
        pool.insert_entry(&entry).unwrap();

        let q = RadarQuery::new("my query".into());
        pool.log_query(&q).unwrap();

        let result = RadarResult::new(q.id.clone(), entry.id.clone(), 0.92);
        pool.insert_result(&result).unwrap();

        let entries = pool.search_entries("query", 5).unwrap();
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn list_sources_returns_newest_first() {
        let pool = memory_pool();
        // Insert two sources (second one added last, so should appear first in list).
        let src1 = Source::new("https://first.com".into(), "First Source".into(), SourceType::Paper);
        let src2 = Source::new("https://second.com".into(), "Second Source".into(), SourceType::Article);
        pool.insert_source(&src1).unwrap();
        pool.insert_source(&src2).unwrap();

        let sources = pool.list_sources(10).unwrap();
        assert_eq!(sources.len(), 2);
        // Most recently added first
        assert_eq!(sources[0].id, src2.id);
        assert_eq!(sources[1].id, src1.id);
    }

    #[test]
    fn list_sources_respects_limit() {
        let pool = memory_pool();
        for i in 0..5 {
            let src = Source::new(
                format!("https://example{i}.com"),
                format!("Source {i}"),
                SourceType::Web,
            );
            pool.insert_source(&src).unwrap();
        }

        let sources = pool.list_sources(3).unwrap();
        assert_eq!(sources.len(), 3);
    }

    #[test]
    fn count_sources_returns_total() {
        let pool = memory_pool();
        assert_eq!(pool.count_sources().unwrap(), 0);

        let src = Source::new("https://example.com".into(), "Example".into(), SourceType::Web);
        pool.insert_source(&src).unwrap();
        assert_eq!(pool.count_sources().unwrap(), 1);

        let src2 = Source::new("https://other.com".into(), "Other".into(), SourceType::Paper);
        pool.insert_source(&src2).unwrap();
        assert_eq!(pool.count_sources().unwrap(), 2);
    }

    #[test]
    fn search_records_radar_result_per_entry() {
        let pool = memory_pool();
        let src = Source::new("https://example.com".into(), "Example".into(), SourceType::Web);
        pool.insert_source(&src).unwrap();

        // Insert two entries
        let entry1 = Entry::new(src.id.clone(), "AI safety research paper".into());
        let entry2 = Entry::new(src.id.clone(), "AI alignment techniques".into());
        pool.insert_entry(&entry1).unwrap();
        pool.insert_entry(&entry2).unwrap();

        let q = RadarQuery::new("AI".into());
        let query_id = pool.log_query(&q).unwrap();
        let entries = pool.search_entries("AI", 10).unwrap();
        assert_eq!(entries.len(), 2);

        // Record a result for each entry
        for entry in &entries {
            let result = RadarResult::new(query_id.clone(), entry.id.clone(), entry.relevance_score);
            pool.insert_result(&result).unwrap();
        }

        // Verify both results were recorded
        let all_entries = pool.search_entries("AI", 10).unwrap();
        assert_eq!(all_entries.len(), 2);
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
    fn profile_update() {
        let pool = memory_pool();
        let mut profile = Profile::new("AI Research".into(), vec!["AI".into()]);
        pool.insert_profile(&profile).unwrap();
        profile.name = "Updated Name".into();
        profile.keywords = vec!["AI".into(), "Safety".into()];
        pool.update_profile(&profile).unwrap();
        let fetched = pool.get_profile(&profile.id).unwrap().unwrap();
        assert_eq!(fetched.name, "Updated Name");
        assert_eq!(fetched.keywords, vec!["AI", "Safety"]);
    }

    #[test]
    fn scan_job_insert_and_get() {
        let pool = memory_pool();
        let profile = Profile::new("Test".into(), vec!["test".into()]);
        pool.insert_profile(&profile).unwrap();
        let job = ScanJob::new(profile.id.clone(), Some("test reason".into()));
        pool.insert_scan_job(&job).unwrap();
        let fetched = pool.get_scan_job(&job.id).unwrap().unwrap();
        assert_eq!(fetched.profile_id, profile.id);
        assert_eq!(fetched.status, ScanJobStatus::Pending);
    }

    #[test]
    fn subscription_insert_and_get() {
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
    fn get_active_scan_job_returns_pending() {
        let pool = memory_pool();
        let profile = Profile::new("Test".into(), vec!["test".into()]);
        pool.insert_profile(&profile).unwrap();
        let job = ScanJob::new(profile.id.clone(), None);
        pool.insert_scan_job(&job).unwrap();
        let active = pool.get_active_scan_job(&profile.id).unwrap().unwrap();
        assert_eq!(active.id, job.id);
    }

    #[test]
    fn get_items_by_profile_with_scores() {
        let pool = memory_pool();
        let profile = Profile::new("Test".into(), vec!["AI".into()]);
        pool.insert_profile(&profile).unwrap();
        let src = Source::new("https://example.com".into(), "Example".into(), SourceType::Web);
        pool.insert_source(&src).unwrap();
        let entry = Entry::new(src.id.clone(), "AI safety paper content".into());
        pool.insert_entry(&entry).unwrap();

        pool.upsert_item_score(&entry.id, &profile.id, 0.85, "new").unwrap();
        let matches = pool.get_items_by_profile(&profile.id, None, Some(0.5), 10, 0).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].score, 0.85);
    }

    #[test]
    fn source_health_returns_stats() {
        let pool = memory_pool();
        let src = Source::new("https://example.com".into(), "Example".into(), SourceType::Web);
        pool.insert_source(&src).unwrap();
        let entry = Entry::new(src.id.clone(), "AI content".into());
        pool.insert_entry(&entry).unwrap();

        let health = pool.get_source_health(None).unwrap();
        assert!(!health.is_empty());
    }

    #[test]
    fn subscription_update() {
        let pool = memory_pool();
        let profile = Profile::new("Test".into(), vec!["test".into()]);
        pool.insert_profile(&profile).unwrap();
        let mut sub = Subscription::new(
            profile.id.clone(),
            "email".into(),
            serde_json::json!({"address": "test@example.com"}),
            true,
        );
        pool.insert_subscription(&sub).unwrap();
        sub.enabled = false;
        pool.update_subscription(&sub).unwrap();
        let fetched = pool.get_subscription(&sub.id).unwrap().unwrap();
        assert!(!fetched.enabled);
    }
}
