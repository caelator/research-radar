//! Pipeline executor — the end-to-end scan loop.
//!
//! Stages: fetch → normalize → dedup → keyword filter → rank → LLM score → persist → notify.

use chrono::Utc;
use std::collections::HashSet;
use std::sync::Arc;

use crate::arxiv;
use crate::github;
use crate::notify;
use crate::openalex;
use crate::rustsec;
use crate::scorer::{LlmBackend, MockBackend, ScorerResult};
use crate::semantic_scholar;
use crate::{
    score_entry, DbPool, Entry, Finding, Profile, RadarStore, ScanJobStatus, ScoredMatch,
    StorageError, UrgencyLevel,
};

const DEFAULT_LLM_CALL_MICROUNITS: i64 = 1000;

pub fn llm_budget_microunits() -> Option<i64> {
    let value = std::env::var("RADAR_LLM_BUDGET_MICROUNITS").ok()?;
    if value.is_empty() {
        return None;
    }
    value.parse::<i64>().ok()
}

fn budget_allows(spent: i64, budget: Option<i64>, call_cost: i64) -> bool {
    budget.is_none_or(|budget| spent + call_cost <= budget)
}

fn tokio_block_on<F: std::future::Future>(fut: F) -> F::Output {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
        Err(_) => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("tokio runtime")
            .block_on(fut),
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PipelineRun {
    pub job_id: String,
    pub profile_id: String,
    pub candidates: usize,
    pub deduped: usize,
    pub scored: usize,
    pub accepted: usize,
    pub notified: usize,
    pub arxiv_fetched: usize,
    pub s2_fetched: usize,
    pub oa_fetched: usize,
    pub rustsec_fetched: usize,
    pub github_fetched: usize,
    pub llm_budget_remaining: Option<i64>,
}

pub struct PipelineExecutor {
    scorer: Arc<dyn LlmBackend>,
    discord_webhook_url: Option<String>,
    /// If true, skips external API calls (arXiv, Semantic Scholar, OpenAlex,
    /// RustSec, GitHub) in tests and offline runs.
    #[allow(dead_code)] // Read only in test-executor code paths.
    disable_external_fetches: bool,
    /// Optional explicit LanceDB directory. When set, `persist_findings`
    /// writes here instead of resolving `~/.research-radar/lance` from the
    /// process-global `HOME` env var. Used by tests to avoid env-var races.
    lance_store_path: Option<std::path::PathBuf>,
}

impl Default for PipelineExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineExecutor {
    pub fn new() -> Self {
        Self {
            scorer: Arc::new(MockBackend),
            discord_webhook_url: None,
            disable_external_fetches: false,
            lance_store_path: None,
        }
    }

    /// Create an executor that does not make external API calls.
    ///
    /// All fetch stages (arXiv, Semantic Scholar, OpenAlex) are skipped — the
    /// pipeline only processes pre-existing entries in the database. This is
    /// used by tests and by callers that want an offline run.
    pub fn test_executor() -> Self {
        Self {
            scorer: Arc::new(MockBackend),
            discord_webhook_url: None,
            disable_external_fetches: true,
            lance_store_path: None,
        }
    }

    /// Set an explicit LanceDB directory for `persist_findings` (tests only).
    ///
    /// Avoids mutating the process-global `HOME` env var, which is unsafe
    /// under concurrent test execution.
    #[cfg(test)]
    pub fn with_lance_store_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        self.lance_store_path = Some(path.into());
        self
    }

    pub fn with_scorer(scorer: Arc<dyn LlmBackend>) -> Self {
        Self {
            scorer,
            discord_webhook_url: None,
            disable_external_fetches: false,
            lance_store_path: None,
        }
    }

    pub fn with_discord_webhook_url(mut self, url: Option<String>) -> Self {
        self.discord_webhook_url = url;
        self
    }

    /// Override the scorer on an existing executor (e.g. a `test_executor`).
    /// Keeps all other settings (such as `disable_external_fetches`) intact.
    pub fn with_scorer_override(mut self, scorer: Arc<dyn LlmBackend>) -> Self {
        self.scorer = scorer;
        self
    }

    pub fn run_next(&self, pool: &DbPool) -> Result<Option<PipelineRun>, StorageError> {
        let mut job = match pool.claim_next_scan_job()? {
            Some(job) => job,
            None => return Ok(None),
        };

        match self.execute_job(pool, &mut job) {
            Ok(run) => Ok(Some(run)),
            Err(err) => {
                pool.fail_scan_job(&job.id)?;
                Err(err)
            }
        }
    }

    pub fn execute_job_by_id(
        &self,
        pool: &DbPool,
        job_id: &str,
    ) -> Result<PipelineRun, StorageError> {
        let mut job = pool
            .claim_scan_job(job_id)?
            .ok_or_else(|| StorageError::NotFound(format!("scan job not claimable: {job_id}")))?;
        match self.execute_job(pool, &mut job) {
            Ok(run) => Ok(run),
            Err(err) => {
                pool.fail_scan_job(&job.id)?;
                Err(err)
            }
        }
    }

    /// Send a heartbeat to keep the lease alive between pipeline stages.
    fn heartbeat(&self, pool: &DbPool, job: &crate::ScanJob) {
        if let Some(ref token) = job.lease_token {
            match pool.heartbeat_job(&job.id, token) {
                Ok(true) => {}
                Ok(false) => {
                    tracing::warn!("heartbeat: lease lost for job {} (token mismatch)", job.id);
                }
                Err(e) => {
                    tracing::warn!("heartbeat failed for job {}: {e}", job.id);
                }
            }
        }
    }

    fn execute_job(
        &self,
        pool: &DbPool,
        job: &mut crate::ScanJob,
    ) -> Result<PipelineRun, StorageError> {
        let profile = pool.get_profile(&job.profile_id)?.ok_or_else(|| {
            StorageError::NotFound(format!("profile {} not found", job.profile_id))
        })?;

        // Guard: reject archived profiles
        if profile.is_archived() {
            tracing::warn!(
                "profile '{}' is archived — aborting scan job {}",
                profile.name,
                job.id
            );
            if let Some(ref token) = job.lease_token {
                pool.complete_job_fenced(&job.id, token, ScanJobStatus::Failed)?;
            } else {
                pool.fail_scan_job(&job.id)?;
            }
            return Ok(PipelineRun {
                job_id: job.id.clone(),
                profile_id: profile.id,
                ..Default::default()
            });
        }

        // Stage 1a: Fetch from arXiv (if keywords present, source not circuit-broken)
        let arxiv_fetched = if pool.is_source_circuit_broken("arxiv") {
            tracing::info!(
                "arXiv circuit breaker open — skipping fetch for '{}'",
                profile.name
            );
            0
        } else {
            self.fetch_arxiv(pool, &profile)
        };
        self.heartbeat(pool, job);

        // Stage 1b: Fetch from Semantic Scholar (if not circuit-broken)
        let s2_fetched = if pool.is_source_circuit_broken("semantic_scholar") {
            tracing::info!(
                "Semantic Scholar circuit breaker open — skipping fetch for '{}'",
                profile.name
            );
            0
        } else {
            self.fetch_s2(pool, &profile)
        };
        self.heartbeat(pool, job);

        // Stage 1c: Fetch from OpenAlex (if not circuit-broken)
        let oa_fetched = if pool.is_source_circuit_broken("openalex") {
            tracing::info!(
                "OpenAlex circuit breaker open — skipping fetch for '{}'",
                profile.name
            );
            0
        } else {
            self.fetch_openalex(pool, &profile)
        };
        self.heartbeat(pool, job);

        // Stage 1d: Fetch from RustSec (if not circuit-broken)
        let rustsec_fetched = if pool.is_source_circuit_broken("rustsec") {
            tracing::info!(
                "RustSec circuit breaker open — skipping fetch for '{}'",
                profile.name
            );
            0
        } else {
            self.fetch_rustsec(pool, &profile)
        };
        self.heartbeat(pool, job);

        // Stage 1e: Fetch from GitHub (if not circuit-broken)
        let github_fetched = if pool.is_source_circuit_broken("github") {
            tracing::info!(
                "GitHub circuit breaker open — skipping fetch for '{}'",
                profile.name
            );
            0
        } else {
            self.fetch_github(pool, &profile)
        };
        self.heartbeat(pool, job);

        // Stage 2: Gather candidates
        let mut candidates = self.fetch_candidates(pool, &profile)?;
        let candidate_count = candidates.len();

        // Stage 3: Dedup
        let deduped = self.dedup_candidates(&mut candidates);

        // Stage 4: Keyword filter + rank
        let ranked = self.rank_candidates(&profile, candidates);
        self.heartbeat(pool, job);

        // Stage 5: LLM score the top candidates (bounded by max_llm_calls and budget)
        let budget = llm_budget_microunits();
        let mut spent = job.llm_spend_microunits;
        let (scored, budget_warning) = self.llm_score(&profile, &ranked, &mut spent, budget);
        job.llm_spend_microunits = spent;
        if let Some(reason) = budget_warning {
            let mut warnings = job
                .warnings_json
                .as_deref()
                .and_then(|json| serde_json::from_str::<Vec<String>>(json).ok())
                .unwrap_or_default();
            warnings.push(reason.clone());
            job.warnings_json = serde_json::to_string(&warnings).ok();
            tracing::warn!("{reason}");
        }
        // Merge LLM-refined scores back into ranked candidates so downstream
        // stages (persist, findings, notify) use the best-available score.
        let llm_map: std::collections::HashMap<String, &ScorerResult> = scored
            .iter()
            .map(|(id, r)| (id.clone(), r))
            .collect();
        let ranked = self.merge_llm_scores(ranked, &llm_map, &profile);
        self.heartbeat(pool, job);

        // Stage 6: Persist scores
        let accepted = self.persist_scores(pool, &profile, &ranked)?;

        // Stage 7: Persist findings to LanceDB
        self.persist_findings(pool, &profile, &ranked)?;
        self.heartbeat(pool, job);

        // Stage 8: Notify
        let notified = self.notify(pool, &profile, &ranked);

        // Mark job complete — use fenced write if lease is available
        job.total = ranked.len() as u32;
        job.progress = ranked.len() as u32;
        job.status = ScanJobStatus::Complete;
        job.completed_at = Some(Utc::now());
        if let Some(ref token) = job.lease_token.clone() {
            let fenced = pool.complete_job_fenced_full(job, token)?;
            if !fenced {
                tracing::warn!(
                    "lease lost for job {} — another worker may have taken over",
                    job.id
                );
            }
        } else {
            pool.update_scan_job(job)?;
        }

        Ok(PipelineRun {
            job_id: job.id.clone(),
            profile_id: profile.id,
            candidates: candidate_count,
            deduped,
            scored: ranked.len(),
            accepted,
            notified,
            arxiv_fetched,
            s2_fetched,
            oa_fetched,
            rustsec_fetched,
            github_fetched,
            llm_budget_remaining: budget.map(|b| b - job.llm_spend_microunits),
        })
    }

    /// Fetch papers from arXiv and insert as sources + entries.
    ///
    /// Uses alias-based dedup (arxiv_id) instead of fragile text search,
    /// and updates watermarks for incremental fetching.
    fn fetch_arxiv(&self, pool: &DbPool, profile: &Profile) -> usize {
        use crate::{ItemAlias, SourceWatermark};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        if profile.keywords.is_empty() {
            return 0;
        }

        if self.disable_external_fetches {
            return 0;
        }

        // Compute a stable scope hash from the profile's sorted keywords
        let scope_hash = {
            let mut sorted = profile.keywords.clone();
            sorted.sort();
            let mut h = DefaultHasher::new();
            sorted.hash(&mut h);
            format!("{:016x}", h.finish())
        };

        // Load watermark for this profile+arxiv+scope
        let watermark = pool
            .get_watermark(&profile.id, "arxiv", &scope_hash)
            .ok()
            .flatten();

        let papers = match tokio_block_on(arxiv::fetch_arxiv_papers(profile, 20)) {
            Ok(papers) => {
                let _ = pool.upsert_source_health("arxiv", true, None);
                papers
            }
            Err(e) => {
                let err_str = e.to_string();
                tracing::warn!("arXiv fetch failed: {err_str}");
                let _ = pool.upsert_source_health("arxiv", false, Some(&err_str));
                // Set rate limit backoff if we detect a rate-limit response
                if err_str.contains("429") || err_str.to_lowercase().contains("rate limit") {
                    let backoff = chrono::Utc::now() + chrono::Duration::minutes(10);
                    let _ = pool.set_rate_limit_until("arxiv", backoff);
                    tracing::warn!("arXiv rate-limited — backoff until {backoff}");
                }
                return 0;
            }
        };

        let mut inserted = 0;
        let mut newest_published: Option<chrono::DateTime<chrono::Utc>> = None;

        for paper in &papers {
            // Skip papers older than watermark (incremental fetch)
            if let Some(ref wm) = watermark {
                if let Some(last_pub) = wm.last_item_published_at {
                    if paper.published <= last_pub {
                        continue;
                    }
                }
            }

            // Alias-based dedup: check if this arxiv_id was already ingested
            if let Ok(Some(_)) = pool.find_by_alias("arxiv_id", &paper.arxiv_id) {
                continue;
            }

            let (source, entry) = arxiv::paper_to_source_entry(paper);
            if pool.insert_source(&source).is_ok() && pool.insert_entry(&entry).is_ok() {
                // Register the arxiv_id alias for future dedup
                let alias = ItemAlias::new(
                    entry.id.clone(),
                    "arxiv_id".into(),
                    paper.arxiv_id.clone(),
                    "arxiv".into(),
                );
                let _ = pool.insert_alias(&alias);
                inserted += 1;

                // Track the newest paper we inserted
                match newest_published {
                    Some(cur) if paper.published > cur => {
                        newest_published = Some(paper.published);
                    }
                    None => {
                        newest_published = Some(paper.published);
                    }
                    _ => {}
                }
            }
        }

        // Update watermark with the latest published date
        if let Some(newest) = newest_published {
            let mut wm = watermark.unwrap_or_else(|| {
                SourceWatermark::new(profile.id.clone(), "arxiv".into(), scope_hash.clone())
            });
            wm.last_fetched_at = Some(Utc::now());
            // Only advance the watermark forward
            let should_advance = wm.last_item_published_at.is_none_or(|prev| newest > prev);
            if should_advance {
                wm.last_item_published_at = Some(newest);
            }
            let _ = pool.upsert_watermark(&wm);
        } else if watermark.is_none() && !papers.is_empty() {
            // First run, no new inserts but papers exist — record fetch time
            let mut wm =
                SourceWatermark::new(profile.id.clone(), "arxiv".into(), scope_hash.clone());
            wm.last_fetched_at = Some(Utc::now());
            let _ = pool.upsert_watermark(&wm);
        }

        if inserted > 0 {
            tracing::info!(
                "arXiv: fetched {inserted} new papers for profile '{}'",
                profile.name
            );
        }
        inserted
    }

    /// Fetch papers from Semantic Scholar and insert as sources + entries.
    ///
    /// Uses alias-based dedup (s2_paper_id and cross-ref arxiv_id) and
    /// watermark tracking for incremental fetching.
    fn fetch_s2(&self, pool: &DbPool, profile: &Profile) -> usize {
        use crate::{ItemAlias, SourceWatermark};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        if profile.keywords.is_empty() {
            return 0;
        }

        if self.disable_external_fetches {
            return 0;
        }

        let scope_hash = {
            let mut sorted = profile.keywords.clone();
            sorted.sort();
            let mut h = DefaultHasher::new();
            sorted.hash(&mut h);
            format!("s2_{:016x}", h.finish())
        };

        let watermark = pool
            .get_watermark(&profile.id, "semantic_scholar", &scope_hash)
            .ok()
            .flatten();

        let papers = match tokio_block_on(semantic_scholar::fetch_s2_papers(profile, 20)) {
            Ok(papers) => {
                let _ = pool.upsert_source_health("semantic_scholar", true, None);
                papers
            }
            Err(e) => {
                let err_str = e.to_string();
                tracing::warn!("Semantic Scholar fetch failed: {err_str}");
                let _ = pool.upsert_source_health("semantic_scholar", false, Some(&err_str));
                if err_str.contains("429") || err_str.to_lowercase().contains("rate limit") {
                    let backoff = chrono::Utc::now() + chrono::Duration::minutes(10);
                    let _ = pool.set_rate_limit_until("semantic_scholar", backoff);
                    tracing::warn!("Semantic Scholar rate-limited — backoff until {backoff}");
                }
                return 0;
            }
        };

        let mut inserted = 0;
        let mut newest_published: Option<chrono::DateTime<chrono::Utc>> = None;

        for paper in &papers {
            // Skip papers older than watermark
            if let Some(ref wm) = watermark {
                if let (Some(last_pub), Some(pub_date)) =
                    (wm.last_item_published_at, paper.publication_date)
                {
                    if pub_date <= last_pub {
                        continue;
                    }
                }
            }

            // Alias-based dedup: check S2 paper ID
            if let Ok(Some(_)) = pool.find_by_alias("s2_paper_id", &paper.paper_id) {
                continue;
            }

            // Cross-source dedup: if this paper has an arxiv_id, check that too
            if let Some(ref arxiv_id) = paper.external_ids.arxiv_id {
                if let Ok(Some(_)) = pool.find_by_alias("arxiv_id", arxiv_id) {
                    continue;
                }
            }

            let (source, entry) = semantic_scholar::paper_to_source_entry(paper);
            if pool.insert_source(&source).is_ok() && pool.insert_entry(&entry).is_ok() {
                // Register S2 paper ID alias
                let alias = ItemAlias::new(
                    entry.id.clone(),
                    "s2_paper_id".into(),
                    paper.paper_id.clone(),
                    "semantic_scholar".into(),
                );
                let _ = pool.insert_alias(&alias);

                // Also register arxiv alias if available (cross-source dedup)
                if let Some(ref arxiv_id) = paper.external_ids.arxiv_id {
                    let arxiv_alias = ItemAlias::new(
                        entry.id.clone(),
                        "arxiv_id".into(),
                        arxiv_id.clone(),
                        "semantic_scholar".into(),
                    );
                    let _ = pool.insert_alias(&arxiv_alias);
                }

                // Register DOI alias if available
                if let Some(ref doi) = paper.external_ids.doi {
                    let doi_alias = ItemAlias::new(
                        entry.id.clone(),
                        "doi".into(),
                        doi.clone(),
                        "semantic_scholar".into(),
                    );
                    let _ = pool.insert_alias(&doi_alias);
                }

                inserted += 1;

                if let Some(pub_date) = paper.publication_date {
                    match newest_published {
                        Some(cur) if pub_date > cur => {
                            newest_published = Some(pub_date);
                        }
                        None => {
                            newest_published = Some(pub_date);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Update watermark
        if let Some(newest) = newest_published {
            let mut wm = watermark.unwrap_or_else(|| {
                SourceWatermark::new(
                    profile.id.clone(),
                    "semantic_scholar".into(),
                    scope_hash.clone(),
                )
            });
            wm.last_fetched_at = Some(Utc::now());
            let should_advance = wm.last_item_published_at.is_none_or(|prev| newest > prev);
            if should_advance {
                wm.last_item_published_at = Some(newest);
            }
            let _ = pool.upsert_watermark(&wm);
        } else if watermark.is_none() && !papers.is_empty() {
            let mut wm = SourceWatermark::new(
                profile.id.clone(),
                "semantic_scholar".into(),
                scope_hash.clone(),
            );
            wm.last_fetched_at = Some(Utc::now());
            let _ = pool.upsert_watermark(&wm);
        }

        if inserted > 0 {
            tracing::info!(
                "S2: fetched {inserted} new papers for profile '{}'",
                profile.name
            );
        }
        inserted
    }

    /// Fetch works from OpenAlex and insert as sources + entries.
    ///
    /// Uses alias-based dedup (openalex_id and cross-ref arxiv_id/doi) and
    /// watermark tracking for incremental fetching.
    fn fetch_openalex(&self, pool: &DbPool, profile: &Profile) -> usize {
        use crate::{ItemAlias, SourceWatermark};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        if profile.keywords.is_empty() {
            return 0;
        }

        if self.disable_external_fetches {
            return 0;
        }

        let scope_hash = {
            let mut sorted = profile.keywords.clone();
            sorted.sort();
            let mut h = DefaultHasher::new();
            sorted.hash(&mut h);
            format!("oa_{:016x}", h.finish())
        };

        let watermark = pool
            .get_watermark(&profile.id, "openalex", &scope_hash)
            .ok()
            .flatten();

        let works = match tokio_block_on(openalex::fetch_oa_works(profile, 20)) {
            Ok(works) => {
                let _ = pool.upsert_source_health("openalex", true, None);
                works
            }
            Err(e) => {
                let err_str = e.to_string();
                tracing::warn!("OpenAlex fetch failed: {err_str}");
                let _ = pool.upsert_source_health("openalex", false, Some(&err_str));
                if err_str.contains("429") || err_str.to_lowercase().contains("rate limit") {
                    let backoff = chrono::Utc::now() + chrono::Duration::minutes(5);
                    let _ = pool.set_rate_limit_until("openalex", backoff);
                    tracing::warn!("OpenAlex rate-limited — backoff until {backoff}");
                }
                return 0;
            }
        };

        let mut inserted = 0;
        let mut newest_published: Option<chrono::DateTime<chrono::Utc>> = None;

        for work in &works {
            // Skip works older than watermark
            if let Some(ref wm) = watermark {
                if let (Some(last_pub), Some(pub_date)) =
                    (wm.last_item_published_at, work.publication_date)
                {
                    if pub_date <= last_pub {
                        continue;
                    }
                }
            }

            // Alias-based dedup: check OpenAlex ID
            if let Ok(Some(_)) = pool.find_by_alias("openalex_id", &work.openalex_id) {
                continue;
            }

            // Cross-source dedup: check arxiv_id if present
            if let Some(ref arxiv_id) = work.arxiv_id {
                if let Ok(Some(_)) = pool.find_by_alias("arxiv_id", arxiv_id) {
                    continue;
                }
            }

            // Cross-source dedup: check DOI if present
            if let Some(ref doi) = work.doi {
                if let Ok(Some(_)) = pool.find_by_alias("doi", doi) {
                    continue;
                }
            }

            let (source, entry) = openalex::work_to_source_entry(work);
            if pool.insert_source(&source).is_ok() && pool.insert_entry(&entry).is_ok() {
                // Register OpenAlex ID alias
                let alias = ItemAlias::new(
                    entry.id.clone(),
                    "openalex_id".into(),
                    work.openalex_id.clone(),
                    "openalex".into(),
                );
                let _ = pool.insert_alias(&alias);

                // Register arxiv alias if available
                if let Some(ref arxiv_id) = work.arxiv_id {
                    let arxiv_alias = ItemAlias::new(
                        entry.id.clone(),
                        "arxiv_id".into(),
                        arxiv_id.clone(),
                        "openalex".into(),
                    );
                    let _ = pool.insert_alias(&arxiv_alias);
                }

                // Register DOI alias if available
                if let Some(ref doi) = work.doi {
                    let doi_alias = ItemAlias::new(
                        entry.id.clone(),
                        "doi".into(),
                        doi.clone(),
                        "openalex".into(),
                    );
                    let _ = pool.insert_alias(&doi_alias);
                }

                inserted += 1;

                if let Some(pub_date) = work.publication_date {
                    match newest_published {
                        Some(cur) if pub_date > cur => {
                            newest_published = Some(pub_date);
                        }
                        None => {
                            newest_published = Some(pub_date);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Update watermark
        if let Some(newest) = newest_published {
            let mut wm = watermark.unwrap_or_else(|| {
                SourceWatermark::new(profile.id.clone(), "openalex".into(), scope_hash.clone())
            });
            wm.last_fetched_at = Some(Utc::now());
            let should_advance = wm.last_item_published_at.is_none_or(|prev| newest > prev);
            if should_advance {
                wm.last_item_published_at = Some(newest);
            }
            let _ = pool.upsert_watermark(&wm);
        } else if watermark.is_none() && !works.is_empty() {
            let mut wm =
                SourceWatermark::new(profile.id.clone(), "openalex".into(), scope_hash.clone());
            wm.last_fetched_at = Some(Utc::now());
            let _ = pool.upsert_watermark(&wm);
        }

        if inserted > 0 {
            tracing::info!(
                "OpenAlex: fetched {inserted} new works for profile '{}'",
                profile.name
            );
        }
        inserted
    }

    /// Maximum number of candidate entries to evaluate per scan.
    ///
    /// Bounds the work done in a single scan so that a large historical corpus
    /// doesn't make each scan O(corpus). Newly-fetched entries are prioritized
    /// (they have the highest rowid).
    const MAX_CANDIDATES_PER_SCAN: usize = 500;
    /// Fetch advisories from RustSec and insert as sources + entries.
    ///
    /// Uses alias-based dedup (rustsec_id and cross-ref cve) and watermark
    /// tracking for incremental fetching.
    fn fetch_rustsec(&self, pool: &DbPool, profile: &Profile) -> usize {
        use crate::{ItemAlias, SourceWatermark};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        #[cfg(test)]
        if self.disable_external_fetches {
            return 0;
        }

        let scope_hash = {
            let mut sorted = profile.keywords.clone();
            sorted.sort();
            let mut h = DefaultHasher::new();
            sorted.hash(&mut h);
            format!("rustsec_{:016x}", h.finish())
        };

        let watermark = pool
            .get_watermark(&profile.id, "rustsec", &scope_hash)
            .ok()
            .flatten();

        let advisories = match tokio_block_on(rustsec::fetch_rustsec_advisories(profile, 20)) {
            Ok(advisories) => {
                let _ = pool.upsert_source_health("rustsec", true, None);
                advisories
            }
            Err(e) => {
                let err_str = e.to_string();
                tracing::warn!("RustSec fetch failed: {err_str}");
                let _ = pool.upsert_source_health("rustsec", false, Some(&err_str));
                if err_str.contains("429") || err_str.to_lowercase().contains("rate limit") {
                    let backoff = chrono::Utc::now() + chrono::Duration::minutes(10);
                    let _ = pool.set_rate_limit_until("rustsec", backoff);
                    tracing::warn!("RustSec rate-limited — backoff until {backoff}");
                }
                return 0;
            }
        };

        let mut inserted = 0;
        let mut newest_published: Option<chrono::DateTime<chrono::Utc>> = None;

        for adv in &advisories {
            // Skip advisories older than watermark
            if let Some(ref wm) = watermark {
                if let (Some(last_pub), Some(pub_date)) = (wm.last_item_published_at, adv.published)
                {
                    if pub_date <= last_pub {
                        continue;
                    }
                }
            }

            // Alias-based dedup: check RustSec advisory ID
            if let Ok(Some(_)) = pool.find_by_alias("rustsec_id", &adv.advisory_id) {
                continue;
            }

            // Cross-source dedup: check CVE aliases
            if adv.aliases.iter().any(|alias| {
                alias.starts_with("CVE-") && matches!(pool.find_by_alias("cve", alias), Ok(Some(_)))
            }) {
                continue;
            }

            let (source, entry) = rustsec::advisory_to_source_entry(adv);
            if pool.insert_source(&source).is_ok() && pool.insert_entry(&entry).is_ok() {
                // Register RustSec advisory ID alias
                let alias = ItemAlias::new(
                    entry.id.clone(),
                    "rustsec_id".into(),
                    adv.advisory_id.clone(),
                    "rustsec".into(),
                );
                let _ = pool.insert_alias(&alias);

                // Register CVE aliases for cross-source dedup
                for cve in adv.aliases.iter().filter(|alias| alias.starts_with("CVE-")) {
                    let cve_alias = ItemAlias::new(
                        entry.id.clone(),
                        "cve".into(),
                        cve.clone(),
                        "rustsec".into(),
                    );
                    let _ = pool.insert_alias(&cve_alias);
                }

                inserted += 1;

                if let Some(pub_date) = adv.published {
                    match newest_published {
                        Some(cur) if pub_date > cur => {
                            newest_published = Some(pub_date);
                        }
                        None => {
                            newest_published = Some(pub_date);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Update watermark
        if let Some(newest) = newest_published {
            let mut wm = watermark.unwrap_or_else(|| {
                SourceWatermark::new(profile.id.clone(), "rustsec".into(), scope_hash.clone())
            });
            wm.last_fetched_at = Some(Utc::now());
            let should_advance = wm.last_item_published_at.is_none_or(|prev| newest > prev);
            if should_advance {
                wm.last_item_published_at = Some(newest);
            }
            let _ = pool.upsert_watermark(&wm);
        } else if watermark.is_none() && !advisories.is_empty() {
            let mut wm =
                SourceWatermark::new(profile.id.clone(), "rustsec".into(), scope_hash.clone());
            wm.last_fetched_at = Some(Utc::now());
            let _ = pool.upsert_watermark(&wm);
        }

        if inserted > 0 {
            tracing::info!(
                "RustSec: fetched {inserted} new advisories for profile '{}'",
                profile.name
            );
        }
        inserted
    }

    /// Fetch releases from GitHub and insert as sources + entries.
    ///
    /// Uses alias-based dedup (github_release) and watermark tracking for
    /// incremental fetching.
    fn fetch_github(&self, pool: &DbPool, profile: &Profile) -> usize {
        use crate::{ItemAlias, SourceWatermark};
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        if profile.keywords.is_empty() {
            return 0;
        }

        #[cfg(test)]
        if self.disable_external_fetches {
            return 0;
        }

        let scope_hash = {
            let mut sorted = profile.keywords.clone();
            sorted.sort();
            let mut h = DefaultHasher::new();
            sorted.hash(&mut h);
            format!("github_{:016x}", h.finish())
        };

        let watermark = pool
            .get_watermark(&profile.id, "github", &scope_hash)
            .ok()
            .flatten();

        let releases = match tokio_block_on(github::fetch_github_releases(profile, 20)) {
            Ok(releases) => {
                let _ = pool.upsert_source_health("github", true, None);
                releases
            }
            Err(e) => {
                let err_str = e.to_string();
                tracing::warn!("GitHub fetch failed: {err_str}");
                let _ = pool.upsert_source_health("github", false, Some(&err_str));
                if err_str.contains("429") || err_str.to_lowercase().contains("rate limit") {
                    let backoff = chrono::Utc::now() + chrono::Duration::minutes(10);
                    let _ = pool.set_rate_limit_until("github", backoff);
                    tracing::warn!("GitHub rate-limited — backoff until {backoff}");
                }
                return 0;
            }
        };

        let mut inserted = 0;
        let mut newest_published: Option<chrono::DateTime<chrono::Utc>> = None;

        for rel in &releases {
            // Skip releases older than watermark
            if let Some(ref wm) = watermark {
                if let (Some(last_pub), Some(pub_date)) =
                    (wm.last_item_published_at, rel.published_at)
                {
                    if pub_date <= last_pub {
                        continue;
                    }
                }
            }

            let release_key = format!("{}@{}", rel.repo_full_name, rel.tag_name);
            if let Ok(Some(_)) = pool.find_by_alias("github_release", &release_key) {
                continue;
            }

            let (source, entry) = github::release_to_source_entry(rel);
            if pool.insert_source(&source).is_ok() && pool.insert_entry(&entry).is_ok() {
                let alias = ItemAlias::new(
                    entry.id.clone(),
                    "github_release".into(),
                    release_key,
                    "github".into(),
                );
                let _ = pool.insert_alias(&alias);

                inserted += 1;

                if let Some(pub_date) = rel.published_at {
                    match newest_published {
                        Some(cur) if pub_date > cur => {
                            newest_published = Some(pub_date);
                        }
                        None => {
                            newest_published = Some(pub_date);
                        }
                        _ => {}
                    }
                }
            }
        }

        // Update watermark
        if let Some(newest) = newest_published {
            let mut wm = watermark.unwrap_or_else(|| {
                SourceWatermark::new(profile.id.clone(), "github".into(), scope_hash.clone())
            });
            wm.last_fetched_at = Some(Utc::now());
            let should_advance = wm.last_item_published_at.is_none_or(|prev| newest > prev);
            if should_advance {
                wm.last_item_published_at = Some(newest);
            }
            let _ = pool.upsert_watermark(&wm);
        } else if watermark.is_none() && !releases.is_empty() {
            let mut wm =
                SourceWatermark::new(profile.id.clone(), "github".into(), scope_hash.clone());
            wm.last_fetched_at = Some(Utc::now());
            let _ = pool.upsert_watermark(&wm);
        }

        if inserted > 0 {
            tracing::info!(
                "GitHub: fetched {inserted} new releases for profile '{}'",
                profile.name
            );
        }
        inserted
    }

    fn fetch_candidates(
        &self,
        pool: &DbPool,
        profile: &Profile,
    ) -> Result<Vec<Entry>, StorageError> {
        // Ensure sources without entries get a placeholder entry so they can be
        // scored. When the profile lists explicit sources, only those are
        // considered; otherwise all sources are considered (user-added sources
        // should not be silently skipped on first scan).
        let sources = if profile.sources.is_empty() {
            pool.list_sources(pool.count_sources()?)?
        } else {
            pool.list_sources_by_ids(&profile.sources)?
        };
        for source in &sources {
            let source_entries = pool.list_entries(Some(std::slice::from_ref(&source.id)))?;
            if source_entries.is_empty() {
                let content = format!("{} {}", source.title, source.url);
                let mut entry = Entry::new(source.id.clone(), content);
                entry.summary = Some(format!("Fetched from {}", source.url));
                pool.insert_entry(&entry)?;
            }
        }

        // Incremental scan: only gather entries not yet scored for this profile.
        // This makes each scan O(new entries) instead of O(total corpus).
        let entries = pool.list_unscored_entries(&profile.id, Self::MAX_CANDIDATES_PER_SCAN)?;
        Ok(entries)
    }

    fn dedup_candidates(&self, entries: &mut Vec<Entry>) -> usize {
        let mut seen = HashSet::new();
        entries.retain(|entry| seen.insert(normalize_text(&entry.content)));
        entries.len()
    }

    fn rank_candidates(&self, profile: &Profile, entries: Vec<Entry>) -> Vec<ScoredMatch> {
        let mut ranked: Vec<ScoredMatch> = entries
            .into_iter()
            .map(|entry| {
                let score = score_entry(&entry, profile);
                let disposition = if score >= profile.score_threshold {
                    "new"
                } else {
                    "filtered"
                };
                ScoredMatch {
                    entry,
                    profile_id: profile.id.clone(),
                    score,
                    disposition: disposition.to_string(),
                }
            })
            .collect();

        ranked.sort_by(|a, b| b.score.total_cmp(&a.score));
        ranked
    }

    /// Run LLM scoring on the top keyword-matched candidates.
    ///
    /// Returns results with cost tracking. Only the top candidates above the
    /// keyword-score threshold are sent to the (expensive) LLM, bounded by
    /// `max_llm_calls` and the configured budget.
    fn llm_score(
        &self,
        profile: &Profile,
        ranked: &[ScoredMatch],
        spent: &mut i64,
        budget: Option<i64>,
    ) -> (Vec<(String, ScorerResult)>, Option<String>) {
        let above_threshold: Vec<&ScoredMatch> = ranked
            .iter()
            .filter(|m| m.score >= profile.score_threshold)
            .take(profile.max_llm_calls as usize)
            .collect();

        if above_threshold.is_empty() {
            return (Vec::new(), None);
        }

        let mut results: Vec<(String, ScorerResult)> = Vec::new();
        let mut budget_exhausted = None;
        for scored in above_threshold {
            if !budget_allows(*spent, budget, DEFAULT_LLM_CALL_MICROUNITS) {
                let budget_value = budget.unwrap();
                budget_exhausted = Some(format!(
                    "LLM budget exhausted: spent {} of {budget_value} microunits",
                    *spent
                ));
                break;
            }
            match tokio_block_on(self.scorer.score(&scored.entry, profile)) {
                Ok(result) => {
                    let real = if result.cost_microunits > 0 {
                        result.cost_microunits
                    } else {
                        DEFAULT_LLM_CALL_MICROUNITS
                    };
                    *spent += real;
                    results.push((scored.entry.id.clone(), result));
                }
                Err(e) => {
                    tracing::warn!("LLM scoring failed for entry {}: {e}", scored.entry.id);
                    results.push((
                        scored.entry.id.clone(),
                        ScorerResult {
                            score: scored.score,
                            reason: format!("LLM scoring failed: {e}"),
                            rationale: String::new(),
                            disposition: "llm_failed".to_string(),
                            cost_microunits: 0,
                        },
                    ));
                    *spent += DEFAULT_LLM_CALL_MICROUNITS;
                }
            }
        }
        (results, budget_exhausted)
    }

    fn persist_scores(
        &self,
        pool: &DbPool,
        profile: &Profile,
        ranked: &[ScoredMatch],
    ) -> Result<usize, StorageError> {
        let mut accepted = 0;
        for scored in ranked {
            // Skip entries with a zero score — they have no signal and don't
            // need a row in item_scores. This avoids redundant writes for the
            // (typically large) tail of irrelevant candidates.
            if scored.score == 0.0 {
                continue;
            }

            pool.upsert_item_score(
                &scored.entry.id,
                &profile.id,
                scored.score,
                &scored.disposition,
            )?;
            pool.update_entry_relevance(&scored.entry.id, scored.score)?;
            if scored.score >= profile.score_threshold {
                accepted += 1;
            }
        }
        Ok(accepted)
    }

    /// Merge LLM-refined scores back into the ranked candidates.
    ///
    /// For each candidate that received an LLM score, replace its keyword score
    /// with the LLM score and update its disposition. Candidates not sent to the
    /// LLM keep their keyword score. The result is re-sorted by final score.
    fn merge_llm_scores(
        &self,
        mut ranked: Vec<ScoredMatch>,
        llm_results: &std::collections::HashMap<String, &ScorerResult>,
        profile: &Profile,
    ) -> Vec<ScoredMatch> {
        for sm in ranked.iter_mut() {
            if let Some(llm) = llm_results.get(&sm.entry.id) {
                sm.score = llm.score;
                sm.disposition = if llm.score >= profile.score_threshold {
                    "matched"
                } else {
                    "scored_below_threshold"
                }
                .to_string();
            }
        }
        ranked.sort_by(|a, b| b.score.total_cmp(&a.score));
        ranked
    }

    fn persist_findings(
        &self,
        pool: &DbPool,
        profile: &Profile,
        ranked: &[ScoredMatch],
    ) -> Result<(), StorageError> {
        let accepted: Vec<&ScoredMatch> = ranked
            .iter()
            .filter(|scored| scored.score >= profile.score_threshold)
            .collect();
        if accepted.is_empty() {
            return Ok(());
        }

        let store = tokio_block_on(async {
            match &self.lance_store_path {
                Some(path) => RadarStore::init_at(path).await,
                None => RadarStore::init().await,
            }
        })
        .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string())))?;
        let embed_backend = crate::embedding::active_embedding_backend();
        let priors: Vec<Vec<f32>> = match &embed_backend {
            Some(_) => tokio_block_on(store.fetch_finding_embeddings(1000)).unwrap_or_default(),
            None => Vec::new(),
        };

        for scored in accepted {
            let source = pool.get_source(&scored.entry.source_id)?.ok_or_else(|| {
                StorageError::NotFound(format!("source {} not found", scored.entry.source_id))
            })?;
            let mut finding = Finding::new(
                source.url.clone(),
                source.title.clone(),
                source.source_type,
                profile.name.to_lowercase().replace(' ', "-"),
                source.title.clone(),
                scored
                    .entry
                    .summary
                    .clone()
                    .unwrap_or_else(|| scored.entry.content.clone()),
                format!(
                    "Review '{}' findings for profile '{}'",
                    source.title, profile.name
                ),
                profile.keywords.clone(),
            );
            finding.confidence = scored.score as f32;
            finding.impact_weight = scored.score as f32;
            finding.urgency = urgency_for_score(scored.score);
            finding.related_entry_ids = vec![scored.entry.id.clone()];
            finding.suggested_action = format!(
                "Incorporate or review source '{}' against profile '{}' keywords",
                source.title, profile.name
            );
            match &embed_backend {
                Some(backend) => {
                    let text = format!("{}\n{}", finding.title, finding.summary);
                    match tokio_block_on(backend.embed(&text)) {
                        Ok(vec) => {
                            let novelty = crate::embedding::compute_novelty(&vec, &priors);
                            finding.novelty_score = novelty;
                            tokio_block_on(
                                store.insert_finding_with_embedding(&finding, Some(&vec)),
                            )
                            .map_err(|err| {
                                StorageError::Io(std::io::Error::other(err.to_string()))
                            })?;
                        }
                        Err(e) => {
                            tracing::warn!("embedding failed for finding {}: {e}", finding.id);
                            tokio_block_on(store.insert_finding(&finding)).map_err(|err| {
                                StorageError::Io(std::io::Error::other(err.to_string()))
                            })?;
                        }
                    }
                }
                None => {
                    tokio_block_on(store.insert_finding(&finding))
                        .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string())))?;
                }
            }
        }

        Ok(())
    }

    /// Send Discord notifications for accepted matches.
    fn notify(&self, pool: &DbPool, profile: &Profile, ranked: &[ScoredMatch]) -> usize {
        let subs = match pool.get_enabled_subscriptions(&profile.id) {
            Ok(subs) => subs,
            Err(e) => {
                tracing::warn!("failed to get subscriptions: {e}");
                return 0;
            }
        };

        let discord_sub = subs.iter().find(|s| s.channel == "discord");
        let subscription_webhook =
            discord_sub.and_then(|sub| sub.config.get("webhook_url").and_then(|v| v.as_str()));

        // Fall back to DISCORD_WEBHOOK_URL env var if no subscription webhook is configured.
        let webhook_url = subscription_webhook
            .map(String::from)
            .or_else(|| self.discord_webhook_url.clone());

        let webhook_url = match webhook_url {
            Some(url) => url,
            None => return 0,
        };

        match tokio_block_on(notify::notify_discord(pool, profile, ranked, &webhook_url)) {
            Ok(result) => {
                if result.sent > 0 {
                    tracing::info!(
                        "Discord: sent {} notifications for profile '{}'",
                        result.sent,
                        profile.name
                    );
                }
                result.sent
            }
            Err(e) => {
                tracing::warn!("Discord notification failed: {e}");
                0
            }
        }
    }
}

fn urgency_for_score(score: f64) -> UrgencyLevel {
    if score >= 0.95 {
        UrgencyLevel::Critical
    } else if score >= 0.8 {
        UrgencyLevel::High
    } else if score >= 0.5 {
        UrgencyLevel::Medium
    } else {
        UrgencyLevel::Low
    }
}

fn normalize_text(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scorer::ScorerError;
    use crate::{DbPool, Profile, Source, SourceType};
    use async_trait::async_trait;

    #[test]
    fn budget_allows_under_budget_permits() {
        assert!(budget_allows(0, Some(2000), 1000));
        assert!(budget_allows(1000, Some(2000), 1000));
    }

    #[test]
    fn budget_allows_over_budget_skips() {
        assert!(!budget_allows(2000, Some(2000), 1000));
        assert!(!budget_allows(1500, Some(2000), 1000));
    }

    #[test]
    fn budget_allows_none_is_unlimited() {
        assert!(budget_allows(999999, None, 1000));
    }

    struct FixedCostBackend {
        cost_microunits: i64,
    }

    #[async_trait]
    impl LlmBackend for FixedCostBackend {
        async fn score(
            &self,
            _entry: &Entry,
            _profile: &Profile,
        ) -> Result<ScorerResult, ScorerError> {
            Ok(ScorerResult {
                score: 0.9,
                reason: "fixed".into(),
                rationale: "fixed cost backend".into(),
                disposition: "matched".into(),
                cost_microunits: self.cost_microunits,
            })
        }
    }

    #[test]
    fn llm_score_accrues_real_cost_and_gates_on_real_spend() {
        let mut profile = Profile::new("AI".into(), vec!["AI".into()]);
        profile.max_llm_calls = 3;
        profile.score_threshold = 0.5;

        let ranked: Vec<ScoredMatch> = (0..3)
            .map(|idx| ScoredMatch {
                entry: Entry::new("src".into(), format!("AI safety entry {idx}")),
                profile_id: profile.id.clone(),
                score: 0.9,
                disposition: "new".into(),
            })
            .collect();

        let executor = PipelineExecutor::with_scorer(Arc::new(FixedCostBackend {
            cost_microunits: 4000,
        }));
        let mut spent = 0;
        let (results, warning) = executor.llm_score(&profile, &ranked, &mut spent, Some(8500));

        assert_eq!(results.len(), 2);
        assert_eq!(spent, 8000);
        assert_eq!(
            warning.as_deref(),
            Some("LLM budget exhausted: spent 8000 of 8500 microunits")
        );
    }

    #[test]
    fn executor_claims_and_completes_job() {
        let _home_guard = crate::storage::HOME_ENV_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let pool = DbPool::test_pool().unwrap();
        let profile = Profile::new("AI".into(), vec!["AI".into(), "safety".into()]);
        pool.insert_profile(&profile).unwrap();

        let source = Source::new(
            "https://example.com".into(),
            "AI Safety Example".into(),
            SourceType::Web,
        );
        pool.insert_source(&source).unwrap();
        let entry = Entry::new(source.id.clone(), "AI safety update".into());
        pool.insert_entry(&entry).unwrap();

        let job = pool.enqueue_job(&profile.id, Some("test".into())).unwrap();
        let run = PipelineExecutor::test_executor()
            .run_next(&pool)
            .unwrap()
            .unwrap();
        assert_eq!(run.job_id, job.id);
        // The manually-added entry should be accepted
        assert!(run.accepted >= 1);

        let stored = pool.get_scan_job(&job.id).unwrap().unwrap();
        assert_eq!(stored.status, ScanJobStatus::Complete);
        assert!(stored.progress >= 1);
        assert!(stored.total >= 1);
    }

    #[test]
    fn executor_creates_entries_from_sources_when_needed() {
        let _home_guard = crate::storage::HOME_ENV_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let pool = DbPool::test_pool().unwrap();
        let profile = Profile::new("Rust".into(), vec!["rust".into()]);
        pool.insert_profile(&profile).unwrap();
        let source = Source::new(
            "https://example.com/rust".into(),
            "Rust release notes".into(),
            SourceType::Article,
        );
        pool.insert_source(&source).unwrap();
        let job = pool.enqueue_job(&profile.id, None).unwrap();

        let run = PipelineExecutor::test_executor()
            .execute_job_by_id(&pool, &job.id)
            .unwrap();
        // At least the manually-added source created an entry
        assert!(run.candidates >= 1);
        assert!(run.scored >= 1);

        let matches = pool
            .get_items_by_profile(&profile.id, None, None, 100, 0)
            .unwrap();
        assert!(!matches.is_empty());
    }

    #[test]
    fn executor_with_mock_scorer() {
        let _home_guard = crate::storage::HOME_ENV_LOCK
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let pool = DbPool::test_pool().unwrap();
        let profile = Profile::new("AI".into(), vec!["AI".into(), "safety".into()]);
        pool.insert_profile(&profile).unwrap();

        let source = Source::new(
            "https://example.com".into(),
            "AI Safety Research".into(),
            SourceType::Paper,
        );
        pool.insert_source(&source).unwrap();
        let entry = Entry::new(source.id.clone(), "AI safety alignment research".into());
        pool.insert_entry(&entry).unwrap();

        let executor = PipelineExecutor::test_executor();
        let job = pool.enqueue_job(&profile.id, None).unwrap();
        let run = executor.execute_job_by_id(&pool, &job.id).unwrap();
        assert!(run.accepted > 0);
    }

    #[test]
    fn notification_idempotency() {
        let pool = DbPool::test_pool().unwrap();
        let profile = Profile::new("Test".into(), vec!["test".into()]);
        pool.insert_profile(&profile).unwrap();

        pool.record_notification(&profile.id, "item1", "discord")
            .unwrap();
        pool.record_notification(&profile.id, "item1", "discord")
            .unwrap(); // should not error (INSERT OR IGNORE)

        let notified = pool.get_notified_items(&profile.id, "discord").unwrap();
        assert!(notified.contains("item1"));
        assert_eq!(notified.len(), 1);
    }

    /// Incremental scanning: a second scan over the same entries must not
    /// re-score them. The candidate count of the second run should be 0.
    #[test]
    fn incremental_scan_skips_already_scored_entries() {
        let pool = DbPool::test_pool().unwrap();
        let profile = Profile::new("AI".into(), vec!["AI".into()]);
        pool.insert_profile(&profile).unwrap();

        let source = Source::new(
            "https://example.com/incr".into(),
            "AI Research".into(),
            SourceType::Paper,
        );
        pool.insert_source(&source).unwrap();
        let entry = Entry::new(source.id.clone(), "AI alignment research".into());
        pool.insert_entry(&entry).unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let executor = PipelineExecutor::test_executor()
            .with_lance_store_path(tmp.path().join("lance"));

        // First scan: should find and score the entry.
        let _job1 = pool.enqueue_job(&profile.id, None).unwrap();
        let run1 = executor.run_next(&pool).unwrap().unwrap();
        assert!(run1.candidates >= 1, "first scan must find the entry");
        assert!(run1.accepted >= 1, "first scan must accept the entry");

        // Second scan: no new entries → zero candidates.
        let _job2 = pool.enqueue_job(&profile.id, None).unwrap();
        let run2 = executor.run_next(&pool).unwrap().unwrap();
        assert_eq!(
            run2.candidates, 0,
            "second scan must not re-score already-scored entries"
        );
        assert_eq!(run2.accepted, 0);
    }

    /// Zero-score entries must not get a row in item_scores.
    #[test]
    fn zero_score_entries_are_not_persisted() {
        let pool = DbPool::test_pool().unwrap();
        let profile = Profile::new("Rust".into(), vec!["rust".into()]);
        pool.insert_profile(&profile).unwrap();

        // An entry that matches keywords.
        let src_match = Source::new(
            "https://example.com/match".into(),
            "Rust Safety".into(),
            SourceType::Paper,
        );
        pool.insert_source(&src_match).unwrap();
        pool.insert_entry(&Entry::new(
            src_match.id.clone(),
            "rust memory safety".into(),
        ))
        .unwrap();

        // An entry that does NOT match any keyword.
        let src_no = Source::new(
            "https://example.com/nomatch".into(),
            "Gardening".into(),
            SourceType::Article,
        );
        pool.insert_source(&src_no).unwrap();
        pool.insert_entry(&Entry::new(src_no.id.clone(), "tomato growing tips".into()))
            .unwrap();

        let _job = pool.enqueue_job(&profile.id, None).unwrap();
        let executor = PipelineExecutor::test_executor();
        let _run = executor.run_next(&pool).unwrap().unwrap();

        // Only the matching entry should be in item_scores.
        let items = pool
            .get_items_by_profile(&profile.id, None, None, 100, 0)
            .unwrap();
        assert_eq!(items.len(), 1, "only matching entry should be scored");
        assert!(items[0].entry.content.contains("rust"));
    }

    /// LLM scores must override keyword scores in the persisted result.
    #[test]
    fn llm_score_overrides_keyword_score() {
        use crate::scorer::{LlmBackend, ScorerError, ScorerResult};
        use async_trait::async_trait;

        /// A mock LLM backend that always returns a fixed high score,
        /// regardless of keyword overlap.
        struct HighScoreBackend;

        #[async_trait]
        impl LlmBackend for HighScoreBackend {
            async fn score(
                &self,
                _entry: &Entry,
                _profile: &Profile,
            ) -> Result<ScorerResult, ScorerError> {
                Ok(ScorerResult {
                    score: 0.99,
                    reason: "LLM override".into(),
                    rationale: "mock".into(),
                    disposition: "matched".into(),
                    cost_microunits: 0,
                })
            }
        }

        let pool = DbPool::test_pool().unwrap();
        // Use a threshold above the keyword score but below the LLM score.
        let mut profile = Profile::new("Test".into(), vec!["AI".into()]);
        profile.score_threshold = 0.8;
        profile.max_llm_calls = 10;
        pool.insert_profile(&profile).unwrap();

        let source = Source::new(
            "https://example.com/llm".into(),
            "AI Paper".into(),
            SourceType::Paper,
        );
        pool.insert_source(&source).unwrap();
        // Keyword score for single keyword "AI" = 1.0, but we verify the LLM
        // score (0.99) is what gets persisted, not some other value.
        pool.insert_entry(&Entry::new(source.id.clone(), "AI research".into()))
            .unwrap();

        let tmp = tempfile::TempDir::new().unwrap();
        let executor = PipelineExecutor::test_executor()
            .with_scorer_override(Arc::new(HighScoreBackend))
            .with_lance_store_path(tmp.path().join("lance"));
        let _job = pool.enqueue_job(&profile.id, None).unwrap();
        let _run = executor.run_next(&pool).unwrap().unwrap();
        assert!(_run.accepted >= 1);

        let items = pool
            .get_items_by_profile(&profile.id, None, None, 100, 0)
            .unwrap();
        assert!(!items.is_empty());
        // The persisted score must be the LLM score, not the keyword score.
        assert!(
            (items[0].score - 0.99).abs() < 0.001,
            "LLM score (0.99) must be persisted, got {}",
            items[0].score
        );
    }
}
