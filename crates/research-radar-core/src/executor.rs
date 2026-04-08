//! Pipeline executor — the end-to-end scan loop.
//!
//! Stages: fetch → normalize → dedup → keyword filter → rank → LLM score → persist → notify.

use chrono::Utc;
use std::collections::HashSet;
use std::sync::Arc;

use crate::arxiv;
use crate::notify;
use crate::scorer::{LlmBackend, MockBackend, ScorerResult};
use crate::semantic_scholar;
use crate::{
    score_entry, DbPool, Entry, Finding, Profile, RadarStore, ScanJobStatus, ScoredMatch,
    StorageError, UrgencyLevel,
};

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
}

pub struct PipelineExecutor {
    scorer: Arc<dyn LlmBackend>,
    discord_webhook_url: Option<String>,
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
        }
    }

    pub fn with_scorer(scorer: Arc<dyn LlmBackend>) -> Self {
        Self {
            scorer,
            discord_webhook_url: None,
        }
    }

    pub fn with_discord_webhook_url(mut self, url: Option<String>) -> Self {
        self.discord_webhook_url = url;
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

        // Stage 1a: Fetch from arXiv (if keywords present)
        let arxiv_fetched = self.fetch_arxiv(pool, &profile);

        // Stage 1b: Fetch from Semantic Scholar
        let s2_fetched = self.fetch_s2(pool, &profile);

        // Stage 2: Gather candidates
        let mut candidates = self.fetch_candidates(pool, &profile)?;
        let candidate_count = candidates.len();

        // Stage 3: Dedup
        let deduped = self.dedup_candidates(&mut candidates);

        // Stage 4: Keyword filter + rank
        let ranked = self.rank_candidates(&profile, candidates);

        // Stage 5: LLM score the top candidates (bounded by max_llm_calls)
        let scored = self.llm_score(&profile, &ranked);

        // Stage 6: Persist scores
        let accepted = self.persist_scores(pool, &profile, &ranked, &scored)?;

        // Stage 7: Persist findings to LanceDB
        self.persist_findings(pool, &profile, &ranked)?;

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
                tracing::warn!("arXiv fetch failed: {e}");
                let _ = pool.upsert_source_health("arxiv", false, Some(&e.to_string()));
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
            let should_advance = wm
                .last_item_published_at
                .map_or(true, |prev| newest > prev);
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
                tracing::warn!("Semantic Scholar fetch failed: {e}");
                let _ =
                    pool.upsert_source_health("semantic_scholar", false, Some(&e.to_string()));
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
            let should_advance = wm
                .last_item_published_at
                .map_or(true, |prev| newest > prev);
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

    fn fetch_candidates(
        &self,
        pool: &DbPool,
        profile: &Profile,
    ) -> Result<Vec<Entry>, StorageError> {
        // Ensure all sources have at least one entry
        let sources = if profile.sources.is_empty() {
            pool.list_sources(pool.count_sources()?)?
        } else {
            pool.list_sources_by_ids(&profile.sources)?
        };

        for source in &sources {
            let source_entries = pool.list_entries(Some(&[source.id.clone()]))?;
            if source_entries.is_empty() {
                let content = format!("{} {}", source.title, source.url);
                let mut entry = Entry::new(source.id.clone(), content);
                entry.summary = Some(format!("Fetched from {}", source.url));
                pool.insert_entry(&entry)?;
            }
        }

        // Now gather all entries
        let entries = if profile.sources.is_empty() {
            pool.list_entries(None)?
        } else {
            pool.list_entries(Some(&profile.sources))?
        };

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
    fn llm_score(&self, profile: &Profile, ranked: &[ScoredMatch]) -> Vec<ScorerResult> {
        let above_threshold: Vec<&ScoredMatch> = ranked
            .iter()
            .filter(|m| m.score >= profile.score_threshold)
            .take(profile.max_llm_calls as usize)
            .collect();

        if above_threshold.is_empty() {
            return Vec::new();
        }

        let mut results = Vec::new();
        for scored in above_threshold {
            match tokio_block_on(self.scorer.score(&scored.entry, profile)) {
                Ok(result) => results.push(result),
                Err(e) => {
                    tracing::warn!("LLM scoring failed for entry {}: {e}", scored.entry.id);
                    results.push(ScorerResult {
                        score: scored.score,
                        reason: format!("LLM scoring failed: {e}"),
                        rationale: String::new(),
                        disposition: "llm_failed".to_string(),
                    });
                }
            }
        }
        results
    }

    fn persist_scores(
        &self,
        pool: &DbPool,
        profile: &Profile,
        ranked: &[ScoredMatch],
        _llm_results: &[ScorerResult],
    ) -> Result<usize, StorageError> {
        let mut accepted = 0;
        for scored in ranked {
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

        let store = tokio_block_on(RadarStore::init())
            .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string())))?;

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
            tokio_block_on(store.insert_finding(&finding))
                .map_err(|err| StorageError::Io(std::io::Error::other(err.to_string())))?;
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
    use crate::{DbPool, Profile, Source, SourceType};

    #[test]
    fn executor_claims_and_completes_job() {
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
        let run = PipelineExecutor::new().run_next(&pool).unwrap().unwrap();
        assert_eq!(run.job_id, job.id);
        // arXiv may contribute additional matches
        assert!(run.accepted >= 1);

        let stored = pool.get_scan_job(&job.id).unwrap().unwrap();
        assert_eq!(stored.status, ScanJobStatus::Complete);
        assert!(stored.progress >= 1);
        assert!(stored.total >= 1);
    }

    #[test]
    fn executor_creates_entries_from_sources_when_needed() {
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

        let run = PipelineExecutor::new()
            .execute_job_by_id(&pool, &job.id)
            .unwrap();
        // At least the manually-added source; arXiv may add more
        assert!(run.candidates >= 1);
        assert!(run.scored >= 1);

        let matches = pool
            .get_items_by_profile(&profile.id, None, None, 100, 0)
            .unwrap();
        assert!(!matches.is_empty());
    }

    #[test]
    fn executor_with_mock_scorer() {
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

        let executor = PipelineExecutor::with_scorer(Arc::new(MockBackend));
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
}
