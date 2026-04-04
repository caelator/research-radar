use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;

use crate::db::schema;
use crate::error::{RadarError, Result};
use crate::types::*;
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};

/// Compute a deterministic scope hash from a sorted list of source strings.
/// Returns a 16-char hex string derived from the default hasher.
pub fn compute_source_scope_hash(sources: &[String]) -> String {
    let mut sorted: Vec<&str> = sources.iter().map(|s| s.as_str()).collect();
    sorted.sort();
    let mut hasher = DefaultHasher::new();
    for s in &sorted {
        s.hash(&mut hasher);
    }
    format!("{:016x}", hasher.finish())
}

/// Primary database handle for radar. Wraps a rusqlite Connection with
/// WAL mode, busy timeout, and migration tracking.
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Open (or create) the database at `path`. Runs WAL pragma, busy timeout,
    /// quick_check, and applies migrations if needed. Backs up before migrating.
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;

        // WAL mode for concurrent readers
        conn.pragma_update(None, "journal_mode", "wal")?;
        // 5 second busy timeout
        conn.pragma_update(None, "busy_timeout", 5000)?;
        // Foreign keys
        conn.pragma_update(None, "foreign_keys", "ON")?;

        let store = Self { conn };
        store.quick_check()?;
        store.migrate(path)?;
        Ok(store)
    }

    /// Open an in-memory database (for testing).
    pub fn open_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        let store = Self { conn };
        store.apply_schema()?;
        Ok(store)
    }

    /// Run SQLite quick_check. Returns error if corruption detected.
    pub fn quick_check(&self) -> Result<()> {
        let result: String = self
            .conn
            .query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if result != "ok" {
            return Err(RadarError::DbIntegrityFailed {
                message: format!("quick_check returned: {result}"),
            });
        }
        Ok(())
    }

    /// Run full integrity_check. More thorough than quick_check.
    pub fn integrity_check(&self) -> Result<()> {
        let result: String = self
            .conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if result != "ok" {
            return Err(RadarError::DbIntegrityFailed {
                message: format!("integrity_check returned: {result}"),
            });
        }
        Ok(())
    }

    fn current_schema_version(&self) -> Result<Option<i32>> {
        // Check if schema_version table exists
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_version')",
            [],
            |row| row.get(0),
        )?;
        if !exists {
            return Ok(None);
        }
        let version: Option<i32> = self
            .conn
            .query_row("SELECT MAX(version) FROM schema_version", [], |row| {
                row.get(0)
            })
            .optional()?
            .flatten();
        Ok(version)
    }

    fn migrate(&self, path: &Path) -> Result<()> {
        let current = self.current_schema_version()?;
        match current {
            Some(v) if v >= schema::SCHEMA_VERSION => return Ok(()),
            Some(_) => {
                // Backup before migration
                self.backup(path)?;
            }
            None => {} // Fresh DB, no backup needed
        }
        self.apply_schema()?;
        Ok(())
    }

    fn apply_schema(&self) -> Result<()> {
        self.conn.execute_batch(schema::CREATE_TABLES)?;
        if !self.column_exists("scan_jobs", "profile_snapshot_json")? {
            self.conn.execute_batch(
                "ALTER TABLE scan_jobs ADD COLUMN profile_snapshot_json TEXT NOT NULL DEFAULT '{}';",
            )?;
        }
        // Upsert schema version
        self.conn.execute(
            "INSERT INTO schema_version (version) VALUES (?1)",
            params![schema::SCHEMA_VERSION],
        )?;
        Ok(())
    }

    fn column_exists(&self, table: &str, column: &str) -> Result<bool> {
        let pragma = format!("PRAGMA table_info({table})");
        let mut stmt = self.conn.prepare(&pragma)?;
        let columns = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for name in columns {
            if name? == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Create a backup of the database file.
    pub fn backup(&self, db_path: &Path) -> Result<()> {
        let backup_path =
            db_path.with_extension(format!("backup-{}", Utc::now().format("%Y%m%d%H%M%S")));
        std::fs::copy(db_path, &backup_path)
            .map_err(|e| RadarError::Other(format!("backup failed: {e}")))?;
        tracing::info!("backed up database to {}", backup_path.display());
        Ok(())
    }

    /// Vacuum the database.
    pub fn vacuum(&self) -> Result<()> {
        self.conn.execute_batch("VACUUM")?;
        Ok(())
    }

    // ─── Profile helpers ───────────────────────────────────────────

    pub fn insert_profile(&self, profile: &Profile) -> Result<()> {
        self.conn.execute(
            "INSERT INTO profiles (id, name, description, keywords_json, negative_keywords_json, sources_json, llm_scoring_prompt, score_threshold, max_llm_calls_per_scan, revision, last_seen_at, archived_at, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            params![
                profile.id,
                profile.name,
                profile.description,
                serde_json::to_string(&profile.keywords)?,
                serde_json::to_string(&profile.negative_keywords)?,
                serde_json::to_string(&profile.sources)?,
                profile.llm_scoring_prompt,
                profile.score_threshold,
                profile.max_llm_calls_per_scan,
                profile.revision,
                profile.last_seen_at.map(|t| t.to_rfc3339()),
                profile.archived_at.map(|t| t.to_rfc3339()),
                profile.created_at.to_rfc3339(),
                profile.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_profile(&self, id: &str) -> Result<Profile> {
        self.conn
            .query_row(
                "SELECT id, name, description, keywords_json, negative_keywords_json, sources_json, llm_scoring_prompt, score_threshold, max_llm_calls_per_scan, revision, last_seen_at, archived_at, created_at, updated_at FROM profiles WHERE id = ?1",
                params![id],
                |row| Ok(row_to_profile(row)),
            )
            .optional()?
            .ok_or_else(|| RadarError::ProfileNotFound {
                profile_id: id.to_string(),
            })?
    }

    pub fn get_profile_by_name(&self, name: &str) -> Result<Profile> {
        self.conn
            .query_row(
                "SELECT id, name, description, keywords_json, negative_keywords_json, sources_json, llm_scoring_prompt, score_threshold, max_llm_calls_per_scan, revision, last_seen_at, archived_at, created_at, updated_at FROM profiles WHERE name = ?1",
                params![name],
                |row| Ok(row_to_profile(row)),
            )
            .optional()?
            .ok_or_else(|| RadarError::ProfileNotFound {
                profile_id: name.to_string(),
            })?
    }

    pub fn list_active_profiles(&self) -> Result<Vec<Profile>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, name, description, keywords_json, negative_keywords_json, sources_json, llm_scoring_prompt, score_threshold, max_llm_calls_per_scan, revision, last_seen_at, archived_at, created_at, updated_at FROM profiles WHERE archived_at IS NULL"
        )?;
        let rows = stmt
            .query_map([], |row| Ok(row_to_profile(row)))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut profiles = Vec::new();
        for r in rows {
            profiles.push(r?);
        }
        Ok(profiles)
    }

    pub fn update_profile(&self, profile: &Profile) -> Result<()> {
        let rows = self.conn.execute(
            "UPDATE profiles SET name = ?2, description = ?3, keywords_json = ?4, negative_keywords_json = ?5, sources_json = ?6, llm_scoring_prompt = ?7, score_threshold = ?8, max_llm_calls_per_scan = ?9, revision = ?10, last_seen_at = ?11, archived_at = ?12, updated_at = ?13 WHERE id = ?1 AND revision < ?10",
            params![
                profile.id,
                profile.name,
                profile.description,
                serde_json::to_string(&profile.keywords)?,
                serde_json::to_string(&profile.negative_keywords)?,
                serde_json::to_string(&profile.sources)?,
                profile.llm_scoring_prompt,
                profile.score_threshold,
                profile.max_llm_calls_per_scan,
                profile.revision,
                profile.last_seen_at.map(|t| t.to_rfc3339()),
                profile.archived_at.map(|t| t.to_rfc3339()),
                profile.updated_at.to_rfc3339(),
            ],
        )?;
        if rows == 0 {
            return Err(RadarError::StorageConflict(
                "profile revision conflict or not found".to_string(),
            ));
        }
        Ok(())
    }

    // ─── Scan Job helpers ──────────────────────────────────────────

    pub fn insert_job(&self, job: &ScanJob) -> Result<()> {
        self.conn.execute(
            "INSERT INTO scan_jobs (job_id, profile_id, profile_snapshot_json, status, claimed_by, lease_token, lease_expires_at, heartbeat_at, last_progress_at, attempt_count, profile_revision_at_enqueue, source_scope_hash, reason, llm_usage_json, llm_spend_microunits, warnings_json, error_json, progress_json, created_at, started_at, finished_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
            params![
                job.job_id,
                job.profile_id,
                job.profile_snapshot_json,
                job.status.as_str(),
                job.claimed_by,
                job.lease_token,
                job.lease_expires_at.map(|t| t.to_rfc3339()),
                job.heartbeat_at.map(|t| t.to_rfc3339()),
                job.last_progress_at.map(|t| t.to_rfc3339()),
                job.attempt_count,
                job.profile_revision_at_enqueue,
                job.source_scope_hash,
                job.reason,
                job.llm_usage_json,
                job.llm_spend_microunits,
                job.warnings_json,
                job.error_json,
                job.progress_json,
                job.created_at.to_rfc3339(),
                job.started_at.map(|t| t.to_rfc3339()),
                job.finished_at.map(|t| t.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    pub fn get_job(&self, job_id: &str) -> Result<ScanJob> {
        self.conn
            .query_row(
                "SELECT job_id, profile_id, profile_snapshot_json, status, claimed_by, lease_token, lease_expires_at, heartbeat_at, last_progress_at, attempt_count, profile_revision_at_enqueue, source_scope_hash, reason, llm_usage_json, llm_spend_microunits, warnings_json, error_json, progress_json, created_at, started_at, finished_at FROM scan_jobs WHERE job_id = ?1",
                params![job_id],
                |row| Ok(row_to_job(row)),
            )
            .optional()?
            .ok_or_else(|| RadarError::NotFound(format!("job {job_id}")))?
    }

    /// Find an active (queued/claimed/processing) job for a profile.
    pub fn find_active_job_for_profile(&self, profile_id: &str) -> Result<Option<ScanJob>> {
        let job = self
            .conn
            .query_row(
                "SELECT job_id, profile_id, profile_snapshot_json, status, claimed_by, lease_token, lease_expires_at, heartbeat_at, last_progress_at, attempt_count, profile_revision_at_enqueue, source_scope_hash, reason, llm_usage_json, llm_spend_microunits, warnings_json, error_json, progress_json, created_at, started_at, finished_at FROM scan_jobs WHERE profile_id = ?1 AND status IN ('queued', 'claimed', 'processing') ORDER BY created_at DESC LIMIT 1",
                params![profile_id],
                |row| Ok(row_to_job(row)),
            )
            .optional()?;
        match job {
            Some(j) => Ok(Some(j?)),
            None => Ok(None),
        }
    }

    /// Atomically claim a queued or expired job. Returns the claimed job or None.
    pub fn claim_job(&self, worker_id: &str, lease_duration_secs: i64) -> Result<Option<ScanJob>> {
        let lease_token = ScanJob::new_lease_token();
        let now = Utc::now();
        let lease_expires = now + chrono::Duration::seconds(lease_duration_secs);

        let rows = self.conn.execute(
            "UPDATE scan_jobs SET status = 'claimed', claimed_by = ?1, lease_token = ?2, lease_expires_at = ?3, heartbeat_at = ?4, started_at = COALESCE(started_at, ?4), attempt_count = attempt_count + 1
             WHERE job_id = (
                 SELECT job_id FROM scan_jobs
                 WHERE status = 'queued' OR (status IN ('claimed', 'processing') AND lease_expires_at < ?4)
                 ORDER BY created_at ASC LIMIT 1
             )",
            params![
                worker_id,
                lease_token,
                lease_expires.to_rfc3339(),
                now.to_rfc3339(),
            ],
        )?;

        if rows == 0 {
            return Ok(None);
        }

        // Fetch the claimed job
        let job = self.conn.query_row(
            "SELECT job_id, profile_id, profile_snapshot_json, status, claimed_by, lease_token, lease_expires_at, heartbeat_at, last_progress_at, attempt_count, profile_revision_at_enqueue, source_scope_hash, reason, llm_usage_json, llm_spend_microunits, warnings_json, error_json, progress_json, created_at, started_at, finished_at FROM scan_jobs WHERE lease_token = ?1",
            params![lease_token],
            |row| Ok(row_to_job(row)),
        )?;

        Ok(Some(job?))
    }

    /// Renew lease (heartbeat). Fenced on lease_token.
    pub fn renew_lease(
        &self,
        job_id: &str,
        lease_token: &str,
        lease_duration_secs: i64,
    ) -> Result<bool> {
        let now = Utc::now();
        let lease_expires = now + chrono::Duration::seconds(lease_duration_secs);
        let rows = self.conn.execute(
            "UPDATE scan_jobs SET lease_expires_at = ?1, heartbeat_at = ?2 WHERE job_id = ?3 AND lease_token = ?4",
            params![lease_expires.to_rfc3339(), now.to_rfc3339(), job_id, lease_token],
        )?;
        Ok(rows > 0)
    }

    /// Complete a job. Fenced on lease_token.
    #[allow(clippy::too_many_arguments)]
    pub fn complete_job(
        &self,
        job_id: &str,
        lease_token: &str,
        status: JobStatus,
        warnings_json: Option<&str>,
        error_json: Option<&str>,
        llm_usage_json: Option<&str>,
        llm_spend_microunits: i64,
    ) -> Result<bool> {
        let now = Utc::now();
        let rows = self.conn.execute(
            "UPDATE scan_jobs SET status = ?1, finished_at = ?2, warnings_json = ?3, error_json = ?4, llm_usage_json = ?5, llm_spend_microunits = ?6 WHERE job_id = ?7 AND lease_token = ?8",
            params![
                status.as_str(),
                now.to_rfc3339(),
                warnings_json,
                error_json,
                llm_usage_json,
                llm_spend_microunits,
                job_id,
                lease_token,
            ],
        )?;
        Ok(rows > 0)
    }

    /// Update job progress. Fenced on lease_token.
    pub fn update_job_progress(
        &self,
        job_id: &str,
        lease_token: &str,
        status: JobStatus,
        progress_json: Option<&str>,
    ) -> Result<bool> {
        let now = Utc::now();
        let rows = self.conn.execute(
            "UPDATE scan_jobs SET status = ?1, last_progress_at = ?2, progress_json = ?3 WHERE job_id = ?4 AND lease_token = ?5",
            params![status.as_str(), now.to_rfc3339(), progress_json, job_id, lease_token],
        )?;
        Ok(rows > 0)
    }

    // ─── Item helpers ──────────────────────────────────────────────

    /// Insert an item, returning the existing item if canonical_id already exists.
    pub fn upsert_item(&self, item: &Item) -> Result<String> {
        // Try to find existing
        let existing: Option<String> = self
            .conn
            .query_row(
                "SELECT id FROM items WHERE canonical_id = ?1",
                params![item.canonical_id],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(id) = existing {
            return Ok(id);
        }

        self.conn.execute(
            "INSERT INTO items (id, canonical_id, title, authors, abstract_text, url, published_at, source_type, raw_json, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                item.id,
                item.canonical_id,
                item.title,
                item.authors,
                item.abstract_text,
                item.url,
                item.published_at.map(|t| t.to_rfc3339()),
                item.source_type,
                item.raw_json,
                item.created_at.to_rfc3339(),
            ],
        )?;
        Ok(item.id.clone())
    }

    pub fn get_item(&self, id: &str) -> Result<Item> {
        self.conn
            .query_row(
                "SELECT id, canonical_id, title, authors, abstract_text, url, published_at, source_type, raw_json, created_at FROM items WHERE id = ?1",
                params![id],
                |row| Ok(row_to_item(row)),
            )
            .optional()?
            .ok_or_else(|| RadarError::NotFound(format!("item {id}")))?
    }

    /// Look up an item by any of its aliases.
    pub fn find_item_by_alias(
        &self,
        alias_type: &str,
        alias_value: &str,
    ) -> Result<Option<String>> {
        let id: Option<String> = self
            .conn
            .query_row(
                "SELECT item_id FROM item_aliases WHERE alias_type = ?1 AND alias_value = ?2",
                params![alias_type, alias_value],
                |row| row.get(0),
            )
            .optional()?;
        Ok(id)
    }

    pub fn insert_alias(&self, alias: &ItemAlias) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO item_aliases (item_id, alias_type, alias_value) VALUES (?1, ?2, ?3)",
            params![alias.item_id, alias.alias_type, alias.alias_value],
        )?;
        Ok(())
    }

    // ─── Evaluation (ItemScore) helpers ────────────────────────────

    pub fn insert_score(&self, score: &ItemScore) -> Result<()> {
        self.conn.execute(
            "INSERT INTO item_scores (id, item_id, profile_id, job_id, disposition, score, reason_short, rationale, profile_revision_at_enqueue, profile_revision_current, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                score.id,
                score.item_id,
                score.profile_id,
                score.job_id,
                score.disposition.as_str(),
                score.score,
                score.reason_short,
                score.rationale,
                score.profile_revision_at_enqueue,
                score.profile_revision_current,
                score.created_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn list_matches(
        &self,
        profile_id: &str,
        min_score: Option<f64>,
        since: Option<DateTime<Utc>>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<(ItemScore, Item)>> {
        let min_score = min_score.unwrap_or(0.0);
        let since_str = since.unwrap_or(DateTime::UNIX_EPOCH).to_rfc3339();

        let mut stmt = self.conn.prepare(
            "SELECT s.id, s.item_id, s.profile_id, s.job_id, s.disposition, s.score, s.reason_short, s.rationale, s.profile_revision_at_enqueue, s.profile_revision_current, s.created_at,
                    i.id, i.canonical_id, i.title, i.authors, i.abstract_text, i.url, i.published_at, i.source_type, i.raw_json, i.created_at
             FROM item_scores s
             JOIN items i ON s.item_id = i.id
             WHERE s.profile_id = ?1 AND s.disposition = 'matched' AND (s.score IS NULL OR s.score >= ?2) AND s.created_at >= ?3
             ORDER BY s.created_at DESC
             LIMIT ?4 OFFSET ?5"
        )?;

        let rows = stmt
            .query_map(
                params![profile_id, min_score, since_str, limit, offset],
                |row| {
                    let score = row_to_score(row, 0);
                    let item = row_to_item_at(row, 11);
                    Ok((score, item))
                },
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let mut results = Vec::new();
        for (s, i) in rows {
            results.push((s?, i?));
        }
        Ok(results)
    }

    /// Count unread matches for a profile (created after last_seen_at).
    pub fn count_unread(&self, profile_id: &str) -> Result<u32> {
        // Get last_seen_at from profile
        let last_seen: Option<String> = self
            .conn
            .query_row(
                "SELECT last_seen_at FROM profiles WHERE id = ?1",
                params![profile_id],
                |row| row.get(0),
            )
            .optional()?
            .flatten();

        let since = last_seen.as_deref().unwrap_or("1970-01-01T00:00:00+00:00");
        let count: u32 = self.conn.query_row(
            "SELECT COUNT(*) FROM item_scores WHERE profile_id = ?1 AND disposition = 'matched' AND created_at > ?2",
            params![profile_id, since],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    // ─── Notification helpers ──────────────────────────────────────

    pub fn insert_notification(&self, notif: &Notification) -> Result<()> {
        self.conn.execute(
            "INSERT OR IGNORE INTO notifications (id, profile_id, item_id, channel, status, error_message, attempt_count, created_at, sent_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                notif.id,
                notif.profile_id,
                notif.item_id,
                notif.channel,
                notif.status.as_str(),
                notif.error_message,
                notif.attempt_count,
                notif.created_at.to_rfc3339(),
                notif.sent_at.map(|t| t.to_rfc3339()),
            ],
        )?;
        Ok(())
    }

    pub fn mark_notification_sent(&self, id: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE notifications SET status = 'sent', sent_at = ?1 WHERE id = ?2",
            params![Utc::now().to_rfc3339(), id],
        )?;
        Ok(())
    }

    pub fn mark_notification_failed(&self, id: &str, error: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE notifications SET status = 'failed', error_message = ?1, attempt_count = attempt_count + 1 WHERE id = ?2",
            params![error, id],
        )?;
        Ok(())
    }

    // ─── Watermark helpers ─────────────────────────────────────────

    pub fn get_watermark(
        &self,
        profile_id: &str,
        source_type: &str,
        source_scope_hash: &str,
    ) -> Result<Option<SourceWatermark>> {
        let wm = self
            .conn
            .query_row(
                "SELECT profile_id, source_type, source_scope_hash, high_watermark, updated_at FROM source_watermarks WHERE profile_id = ?1 AND source_type = ?2 AND source_scope_hash = ?3",
                params![profile_id, source_type, source_scope_hash],
                |row| {
                    Ok(SourceWatermark {
                        profile_id: row.get(0)?,
                        source_type: row.get(1)?,
                        source_scope_hash: row.get(2)?,
                        high_watermark: parse_dt(&row.get::<_, String>(3)?),
                        updated_at: parse_dt(&row.get::<_, String>(4)?),
                    })
                },
            )
            .optional()?;
        Ok(wm)
    }

    pub fn upsert_watermark(&self, wm: &SourceWatermark) -> Result<()> {
        self.conn.execute(
            "INSERT INTO source_watermarks (profile_id, source_type, source_scope_hash, high_watermark, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(profile_id, source_type, source_scope_hash) DO UPDATE SET high_watermark = ?4, updated_at = ?5",
            params![
                wm.profile_id,
                wm.source_type,
                wm.source_scope_hash,
                wm.high_watermark.to_rfc3339(),
                wm.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    // ─── Subscription helpers ──────────────────────────────────────

    pub fn upsert_subscription(&self, sub: &Subscription) -> Result<()> {
        self.conn.execute(
            "INSERT INTO subscriptions (id, profile_id, channel, channel_config, enabled, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(profile_id, channel) DO UPDATE SET channel_config = ?4, enabled = ?5, updated_at = ?7",
            params![
                sub.id,
                sub.profile_id,
                sub.channel,
                sub.channel_config,
                sub.enabled,
                sub.created_at.to_rfc3339(),
                sub.updated_at.to_rfc3339(),
            ],
        )?;
        Ok(())
    }

    pub fn get_subscriptions_for_profile(&self, profile_id: &str) -> Result<Vec<Subscription>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, profile_id, channel, channel_config, enabled, created_at, updated_at FROM subscriptions WHERE profile_id = ?1 AND enabled = 1"
        )?;
        let rows = stmt
            .query_map(params![profile_id], |row| {
                Ok(Subscription {
                    id: row.get(0)?,
                    profile_id: row.get(1)?,
                    channel: row.get(2)?,
                    channel_config: row.get(3)?,
                    enabled: row.get(4)?,
                    created_at: parse_dt(&row.get::<_, String>(5)?),
                    updated_at: parse_dt(&row.get::<_, String>(6)?),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    // ─── Scan enqueue ──────────────────────────────────────────────

    /// Enqueue a scan job for the given profile.
    ///
    /// Returns `(job, reused)` where `reused` is true if an active job was found
    /// and returned instead of creating a new one.
    ///
    /// Checks:
    /// 1. Profile must not be archived.
    /// 2. Profile must be "ready" (has keywords or a scoring prompt).
    /// 3. Unless `force`, reuses an existing active job for the profile.
    pub fn enqueue_job(
        &self,
        profile_id: &str,
        sources: &[String],
        reason: &str,
        force: bool,
    ) -> Result<(ScanJob, bool)> {
        let profile = self.get_profile(profile_id)?;
        self.conn.execute_batch("BEGIN IMMEDIATE TRANSACTION")?;

        let result: Result<(ScanJob, bool)> = (|| {
            if profile.is_archived() {
                return Err(RadarError::ProfileArchived {
                    profile_id: profile_id.to_string(),
                });
            }

            // Ready = has keywords or a scoring prompt
            if profile.keywords.is_empty() && profile.llm_scoring_prompt.is_none() {
                return Err(RadarError::ProfileNotReady {
                    message: format!(
                        "profile '{}' needs keywords or a scoring prompt before scanning",
                        profile.name,
                    ),
                });
            }

            let scope_hash = compute_source_scope_hash(sources);

            // Reuse an active job unless forced
            if !force {
                let existing_job_id: Option<String> = self
                .conn
                .query_row(
                    "SELECT job_id FROM scan_jobs WHERE profile_id = ?1 AND status IN ('queued', 'claimed', 'processing') ORDER BY created_at DESC LIMIT 1",
                    params![profile_id],
                    |row| row.get(0),
                )
                .optional()?;
                if let Some(job_id) = existing_job_id {
                    let job = self.get_job(&job_id)?;
                    return Ok((job, true));
                }
            }

            let mut snapshot = profile.clone();
            snapshot.sources = sources.to_vec();
            let snapshot_json = serde_json::to_string(&snapshot)?;
            let mut job = ScanJob::new(
                profile_id.to_string(),
                snapshot_json,
                profile.revision,
                Some(reason.to_string()),
            );
            job.source_scope_hash = Some(scope_hash);

            self.conn.execute(
            "INSERT INTO scan_jobs (job_id, profile_id, profile_snapshot_json, status, claimed_by, lease_token, lease_expires_at, heartbeat_at, last_progress_at, attempt_count, profile_revision_at_enqueue, source_scope_hash, reason, llm_usage_json, llm_spend_microunits, warnings_json, error_json, progress_json, created_at, started_at, finished_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)",
            params![
                job.job_id,
                job.profile_id,
                job.profile_snapshot_json,
                job.status.as_str(),
                job.claimed_by,
                job.lease_token,
                job.lease_expires_at.map(|t| t.to_rfc3339()),
                job.heartbeat_at.map(|t| t.to_rfc3339()),
                job.last_progress_at.map(|t| t.to_rfc3339()),
                job.attempt_count,
                job.profile_revision_at_enqueue,
                job.source_scope_hash,
                job.reason,
                job.llm_usage_json,
                job.llm_spend_microunits,
                job.warnings_json,
                job.error_json,
                job.progress_json,
                job.created_at.to_rfc3339(),
                job.started_at.map(|t| t.to_rfc3339()),
                job.finished_at.map(|t| t.to_rfc3339()),
            ],
        )?;
            Ok((job, false))
        })();

        match result {
            Ok(value) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn enqueue_scan(
        &self,
        profile_id: &str,
        sources: &[String],
        reason: &str,
        force: bool,
    ) -> Result<(ScanJob, bool)> {
        self.enqueue_job(profile_id, sources, reason, force)
    }

    pub fn record_executor_heartbeat(&self, worker_id: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        self.conn.execute(
            "INSERT INTO executor_heartbeats (worker_id, heartbeat_at, started_at, updated_at)
             VALUES (?1, ?2, ?2, ?2)
             ON CONFLICT(worker_id) DO UPDATE SET heartbeat_at = excluded.heartbeat_at, updated_at = excluded.updated_at",
            params![worker_id, now],
        )?;
        Ok(())
    }

    pub fn latest_executor_heartbeat(&self) -> Result<Option<DateTime<Utc>>> {
        let heartbeat: Option<String> = self
            .conn
            .query_row(
                "SELECT heartbeat_at FROM executor_heartbeats ORDER BY heartbeat_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        Ok(heartbeat.as_deref().map(parse_dt))
    }

    pub fn trigger_scan(
        &self,
        profile_id: &str,
        sources: &[String],
        reason: &str,
        force: bool,
        heartbeat_ttl_secs: i64,
    ) -> Result<(ScanJob, bool)> {
        let fresh = self
            .latest_executor_heartbeat()?
            .map(|ts| (Utc::now() - ts).num_seconds() <= heartbeat_ttl_secs)
            .unwrap_or(false);
        if !fresh {
            return Err(RadarError::ExecutorUnavailable {
                message: format!("no executor heartbeat within the last {heartbeat_ttl_secs}s"),
            });
        }
        self.enqueue_job(profile_id, sources, reason, force)
    }

    // ─── Activity helpers ──────────────────────────────────────────

    pub fn acknowledge_activity(&self, profile_id: &str, up_to: DateTime<Utc>) -> Result<()> {
        self.conn.execute(
            "UPDATE profiles SET last_seen_at = ?1 WHERE id = ?2",
            params![up_to.to_rfc3339(), profile_id],
        )?;
        Ok(())
    }

    // ─── Spend tracking ────────────────────────────────────────────

    /// Sum LLM spend across all jobs in the given time window.
    pub fn total_spend_since(&self, since: DateTime<Utc>) -> Result<i64> {
        let total: i64 = self.conn.query_row(
            "SELECT COALESCE(SUM(llm_spend_microunits), 0) FROM scan_jobs WHERE created_at >= ?1",
            params![since.to_rfc3339()],
            |row| row.get(0),
        )?;
        Ok(total)
    }
}

// ─── Row mapping helpers ───────────────────────────────────────────

fn parse_dt(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| DateTime::UNIX_EPOCH)
}

fn parse_dt_opt(s: &Option<String>) -> Option<DateTime<Utc>> {
    s.as_ref().map(|s| parse_dt(s))
}

fn row_to_profile(row: &rusqlite::Row) -> Result<Profile> {
    Ok(Profile {
        id: row.get(0)?,
        name: row.get(1)?,
        description: row.get(2)?,
        keywords: serde_json::from_str(&row.get::<_, String>(3)?)?,
        negative_keywords: serde_json::from_str(&row.get::<_, String>(4)?)?,
        sources: serde_json::from_str(&row.get::<_, String>(5)?)?,
        llm_scoring_prompt: row.get(6)?,
        score_threshold: row.get(7)?,
        max_llm_calls_per_scan: row.get(8)?,
        revision: row.get(9)?,
        last_seen_at: parse_dt_opt(&row.get(10)?),
        archived_at: parse_dt_opt(&row.get(11)?),
        created_at: parse_dt(&row.get::<_, String>(12)?),
        updated_at: parse_dt(&row.get::<_, String>(13)?),
    })
}

fn row_to_job(row: &rusqlite::Row) -> Result<ScanJob> {
    Ok(ScanJob {
        job_id: row.get(0)?,
        profile_id: row.get(1)?,
        profile_snapshot_json: row.get(2)?,
        status: JobStatus::from_str(&row.get::<_, String>(3)?).unwrap_or(JobStatus::Failed),
        claimed_by: row.get(4)?,
        lease_token: row.get(5)?,
        lease_expires_at: parse_dt_opt(&row.get(6)?),
        heartbeat_at: parse_dt_opt(&row.get(7)?),
        last_progress_at: parse_dt_opt(&row.get(8)?),
        attempt_count: row.get(9)?,
        profile_revision_at_enqueue: row.get(10)?,
        source_scope_hash: row.get(11)?,
        reason: row.get(12)?,
        llm_usage_json: row.get(13)?,
        llm_spend_microunits: row.get(14)?,
        warnings_json: row.get(15)?,
        error_json: row.get(16)?,
        progress_json: row.get(17)?,
        created_at: parse_dt(&row.get::<_, String>(18)?),
        started_at: parse_dt_opt(&row.get(19)?),
        finished_at: parse_dt_opt(&row.get(20)?),
    })
}

fn row_to_item(row: &rusqlite::Row) -> Result<Item> {
    row_to_item_at(row, 0)
}

fn row_to_item_at(row: &rusqlite::Row, offset: usize) -> Result<Item> {
    Ok(Item {
        id: row.get(offset)?,
        canonical_id: row.get(offset + 1)?,
        title: row.get(offset + 2)?,
        authors: row.get(offset + 3)?,
        abstract_text: row.get(offset + 4)?,
        url: row.get(offset + 5)?,
        published_at: parse_dt_opt(&row.get(offset + 6)?),
        source_type: row.get(offset + 7)?,
        raw_json: row.get(offset + 8)?,
        created_at: parse_dt(&row.get::<_, String>(offset + 9)?),
    })
}

fn row_to_score(row: &rusqlite::Row, offset: usize) -> Result<ItemScore> {
    Ok(ItemScore {
        id: row.get(offset)?,
        item_id: row.get(offset + 1)?,
        profile_id: row.get(offset + 2)?,
        job_id: row.get(offset + 3)?,
        disposition: Disposition::from_str(&row.get::<_, String>(offset + 4)?)
            .unwrap_or(Disposition::LlmFailed),
        score: row.get(offset + 5)?,
        reason_short: row.get(offset + 6)?,
        rationale: row.get(offset + 7)?,
        profile_revision_at_enqueue: row.get(offset + 8)?,
        profile_revision_current: row.get(offset + 9)?,
        created_at: parse_dt(&row.get::<_, String>(offset + 10)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn test_open_memory() {
        let store = Store::open_memory().unwrap();
        store.quick_check().unwrap();
    }

    #[test]
    fn test_profile_crud() {
        let store = Store::open_memory().unwrap();
        let mut profile = Profile::new("test-ai-research".to_string());
        profile.keywords = vec!["transformer".to_string(), "attention".to_string()];

        store.insert_profile(&profile).unwrap();

        let fetched = store.get_profile(&profile.id).unwrap();
        assert_eq!(fetched.name, "test-ai-research");
        assert_eq!(fetched.keywords, vec!["transformer", "attention"]);
        assert_eq!(fetched.revision, 1);

        // Update with revision bump
        let mut updated = fetched;
        updated.revision = 2;
        updated.keywords.push("llm".to_string());
        updated.updated_at = Utc::now();
        store.update_profile(&updated).unwrap();

        let fetched2 = store.get_profile(&profile.id).unwrap();
        assert_eq!(fetched2.revision, 2);
        assert_eq!(fetched2.keywords.len(), 3);
    }

    #[test]
    fn test_profile_revision_conflict() {
        let store = Store::open_memory().unwrap();
        let profile = Profile::new("conflict-test".to_string());
        store.insert_profile(&profile).unwrap();

        // Try to update with same revision (not higher)
        let mut stale = profile.clone();
        stale.revision = 1; // same as current
        let result = store.update_profile(&stale);
        assert!(result.is_err());
    }

    #[test]
    fn test_job_lifecycle() {
        let store = Store::open_memory().unwrap();
        let profile = Profile::new("job-test".to_string());
        store.insert_profile(&profile).unwrap();

        let job = ScanJob::new(
            profile.id.clone(),
            "{}".to_string(),
            1,
            Some("test".to_string()),
        );
        store.insert_job(&job).unwrap();

        // Find active job
        let active = store.find_active_job_for_profile(&profile.id).unwrap();
        assert!(active.is_some());
        assert_eq!(active.unwrap().job_id, job.job_id);

        // Claim
        let claimed = store.claim_job("worker-1", 300).unwrap();
        assert!(claimed.is_some());
        let claimed = claimed.unwrap();
        assert_eq!(claimed.status, JobStatus::Claimed);

        // Renew lease
        let renewed = store
            .renew_lease(
                &claimed.job_id,
                claimed.lease_token.as_deref().unwrap(),
                300,
            )
            .unwrap();
        assert!(renewed);

        // Complete
        let completed = store
            .complete_job(
                &claimed.job_id,
                claimed.lease_token.as_deref().unwrap(),
                JobStatus::Completed,
                None,
                None,
                None,
                0,
            )
            .unwrap();
        assert!(completed);

        let final_job = store.get_job(&claimed.job_id).unwrap();
        assert_eq!(final_job.status, JobStatus::Completed);
        assert!(final_job.finished_at.is_some());
    }

    #[test]
    fn test_lease_fencing() {
        let store = Store::open_memory().unwrap();
        let profile = Profile::new("fence-test".to_string());
        store.insert_profile(&profile).unwrap();

        let job = ScanJob::new(profile.id.clone(), "{}".to_string(), 1, None);
        store.insert_job(&job).unwrap();

        let claimed = store.claim_job("worker-1", 300).unwrap().unwrap();

        // Try to complete with wrong lease token — should be fenced
        let fenced = store
            .complete_job(
                &claimed.job_id,
                "wrong-token",
                JobStatus::Completed,
                None,
                None,
                None,
                0,
            )
            .unwrap();
        assert!(!fenced);

        // Correct token works
        let ok = store
            .complete_job(
                &claimed.job_id,
                claimed.lease_token.as_deref().unwrap(),
                JobStatus::Completed,
                None,
                None,
                None,
                0,
            )
            .unwrap();
        assert!(ok);
    }

    #[test]
    fn test_item_dedup() {
        let store = Store::open_memory().unwrap();
        let item = Item::new(
            "arxiv:2401.00001".to_string(),
            "Test Paper".to_string(),
            "https://arxiv.org/abs/2401.00001".to_string(),
            "arxiv".to_string(),
        );
        let id1 = store.upsert_item(&item).unwrap();

        // Insert again with same canonical_id — should return existing
        let mut item2 = Item::new(
            "arxiv:2401.00001".to_string(),
            "Test Paper v2".to_string(),
            "https://arxiv.org/abs/2401.00001".to_string(),
            "arxiv".to_string(),
        );
        item2.id = Uuid::new_v4().to_string(); // different internal id
        let id2 = store.upsert_item(&item2).unwrap();

        assert_eq!(id1, id2);
    }

    #[test]
    fn test_alias_lookup() {
        let store = Store::open_memory().unwrap();
        let item = Item::new(
            "arxiv:2401.00001".to_string(),
            "Test Paper".to_string(),
            "https://arxiv.org/abs/2401.00001".to_string(),
            "arxiv".to_string(),
        );
        let item_id = store.upsert_item(&item).unwrap();

        store
            .insert_alias(&ItemAlias {
                item_id: item_id.clone(),
                alias_type: "doi".to_string(),
                alias_value: "10.1234/test".to_string(),
            })
            .unwrap();

        let found = store.find_item_by_alias("doi", "10.1234/test").unwrap();
        assert_eq!(found, Some(item_id));
    }

    #[test]
    fn test_enqueue_reuses_active_job() {
        let store = Store::open_memory().unwrap();
        let mut profile = Profile::new("reuse-test".to_string());
        profile.keywords = vec!["transformers".to_string()];
        store.insert_profile(&profile).unwrap();

        let sources = vec!["arxiv".to_string()];
        let (job1, reused1) = store
            .enqueue_scan(&profile.id, &sources, "first", false)
            .unwrap();
        assert!(!reused1);

        // Second enqueue without force should reuse
        let (job2, reused2) = store
            .enqueue_scan(&profile.id, &sources, "second", false)
            .unwrap();
        assert!(reused2);
        assert_eq!(job1.job_id, job2.job_id);
    }

    #[test]
    fn test_enqueue_force_creates_new() {
        let store = Store::open_memory().unwrap();
        let mut profile = Profile::new("force-test".to_string());
        profile.keywords = vec!["llm".to_string()];
        store.insert_profile(&profile).unwrap();

        let sources = vec!["arxiv".to_string()];
        let (job1, reused1) = store
            .enqueue_scan(&profile.id, &sources, "first", false)
            .unwrap();
        assert!(!reused1);

        // Force should create a new job even though one is active
        let (job2, reused2) = store
            .enqueue_scan(&profile.id, &sources, "forced", true)
            .unwrap();
        assert!(!reused2);
        assert_ne!(job1.job_id, job2.job_id);
    }

    #[test]
    fn test_enqueue_rejects_archived_profile() {
        let store = Store::open_memory().unwrap();
        let mut profile = Profile::new("archived-test".to_string());
        profile.keywords = vec!["test".to_string()];
        profile.archived_at = Some(Utc::now());
        store.insert_profile(&profile).unwrap();

        let sources = vec!["arxiv".to_string()];
        let result = store.enqueue_scan(&profile.id, &sources, "attempt", false);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, RadarError::ProfileArchived { .. }));
    }

    #[test]
    fn test_trigger_scan_requires_fresh_executor_heartbeat() {
        let store = Store::open_memory().unwrap();
        let mut profile = Profile::new("heartbeat-test".to_string());
        profile.keywords = vec!["test".to_string()];
        store.insert_profile(&profile).unwrap();

        let sources = vec!["arxiv".to_string()];
        let err = store
            .trigger_scan(&profile.id, &sources, "attempt", false, 60)
            .unwrap_err();
        assert!(matches!(err, RadarError::ExecutorUnavailable { .. }));

        store.record_executor_heartbeat("worker-1").unwrap();
        let (_job, reused) = store
            .trigger_scan(&profile.id, &sources, "attempt", false, 60)
            .unwrap();
        assert!(!reused);
    }

    #[test]
    fn test_watermark_upsert() {
        let store = Store::open_memory().unwrap();
        let profile = Profile::new("wm-test".to_string());
        store.insert_profile(&profile).unwrap();

        let now = Utc::now();
        let wm = SourceWatermark {
            profile_id: profile.id.clone(),
            source_type: "arxiv".to_string(),
            source_scope_hash: "cs.AI".to_string(),
            high_watermark: now,
            updated_at: now,
        };
        store.upsert_watermark(&wm).unwrap();

        let fetched = store
            .get_watermark(&profile.id, "arxiv", "cs.AI")
            .unwrap()
            .unwrap();
        assert_eq!(fetched.source_type, "arxiv");

        // Update watermark
        let later = now + chrono::Duration::hours(1);
        let wm2 = SourceWatermark {
            high_watermark: later,
            updated_at: later,
            ..wm
        };
        store.upsert_watermark(&wm2).unwrap();

        let fetched2 = store
            .get_watermark(&profile.id, "arxiv", "cs.AI")
            .unwrap()
            .unwrap();
        assert!(fetched2.high_watermark > fetched.high_watermark);
    }
}
