//! SQLite storage layer for research-radar-core.
//!
//! Database path: `~/.research-radar/data.db` (auto-created).
//! WAL mode is enabled for concurrent read performance.

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection};
use std::path::PathBuf;

use crate::{Entry, RadarQuery, RadarResult, Source, SourceType};

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
    conn: Connection,
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
    fn run_migrations(&self) -> Result<()> {
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

            CREATE INDEX IF NOT EXISTS idx_entries_source_id ON entries(source_id);
            CREATE INDEX IF NOT EXISTS idx_results_query_id   ON results(query_id);
            CREATE INDEX IF NOT EXISTS idx_results_entry_id   ON results(entry_id);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
}
