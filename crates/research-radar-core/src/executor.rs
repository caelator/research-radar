use chrono::Utc;
use std::collections::HashSet;

use crate::{score_entry, DbPool, Entry, Profile, ScanJobStatus, ScoredMatch, StorageError};

#[derive(Debug, Clone, Default)]
pub struct PipelineExecutor;

#[derive(Debug, Clone, PartialEq)]
pub struct PipelineRun {
    pub job_id: String,
    pub profile_id: String,
    pub candidates: usize,
    pub deduped: usize,
    pub scored: usize,
    pub accepted: usize,
}

impl PipelineExecutor {
    pub fn new() -> Self {
        Self
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

    pub fn execute_job_by_id(&self, pool: &DbPool, job_id: &str) -> Result<PipelineRun, StorageError> {
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

    fn execute_job(&self, pool: &DbPool, job: &mut crate::ScanJob) -> Result<PipelineRun, StorageError> {
        let profile = pool
            .get_profile(&job.profile_id)?
            .ok_or_else(|| StorageError::NotFound(format!("profile {} not found", job.profile_id)))?;

        let mut candidates = self.fetch_candidates(pool, &profile)?;
        let candidate_count = candidates.len();
        let deduped = self.dedup_candidates(&mut candidates);
        let ranked = self.rank_candidates(&profile, candidates);
        let accepted = self.persist_scores(pool, &profile, &ranked)?;

        job.total = ranked.len() as u32;
        job.progress = ranked.len() as u32;
        job.status = ScanJobStatus::Complete;
        job.completed_at = Some(Utc::now());
        pool.update_scan_job(job)?;

        Ok(PipelineRun {
            job_id: job.id.clone(),
            profile_id: profile.id,
            candidates: candidate_count,
            deduped,
            scored: ranked.len(),
            accepted,
        })
    }

    fn fetch_candidates(&self, pool: &DbPool, profile: &Profile) -> Result<Vec<Entry>, StorageError> {
        let mut entries = if profile.sources.is_empty() {
            pool.list_entries(None)?
        } else {
            pool.list_entries(Some(&profile.sources))?
        };

        if entries.is_empty() {
            let sources = if profile.sources.is_empty() {
                pool.list_sources(pool.count_sources()?)?
            } else {
                pool.list_sources_by_ids(&profile.sources)?
            };

            for source in sources {
                let content = format!("{} {}", source.title, source.url);
                let mut entry = Entry::new(source.id.clone(), content);
                entry.summary = Some(format!("Fetched from {}", source.url));
                pool.insert_entry(&entry)?;
                entries.push(entry);
            }
        }

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

    fn persist_scores(
        &self,
        pool: &DbPool,
        profile: &Profile,
        ranked: &[ScoredMatch],
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
}

fn normalize_text(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ").to_lowercase()
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

        let source = Source::new("https://example.com".into(), "AI Safety Example".into(), SourceType::Web);
        pool.insert_source(&source).unwrap();
        let entry = Entry::new(source.id.clone(), "AI safety update".into());
        pool.insert_entry(&entry).unwrap();

        let job = pool.enqueue_job(&profile.id, Some("test".into())).unwrap();
        let run = PipelineExecutor::new().run_next(&pool).unwrap().unwrap();
        assert_eq!(run.job_id, job.id);
        assert_eq!(run.accepted, 1);

        let stored = pool.get_scan_job(&job.id).unwrap().unwrap();
        assert_eq!(stored.status, ScanJobStatus::Complete);
        assert_eq!(stored.progress, 1);
        assert_eq!(stored.total, 1);
    }

    #[test]
    fn executor_creates_entries_from_sources_when_needed() {
        let pool = DbPool::test_pool().unwrap();
        let profile = Profile::new("Rust".into(), vec!["rust".into()]);
        pool.insert_profile(&profile).unwrap();
        let source = Source::new("https://example.com/rust".into(), "Rust release notes".into(), SourceType::Article);
        pool.insert_source(&source).unwrap();
        let job = pool.enqueue_job(&profile.id, None).unwrap();

        let run = PipelineExecutor::new().execute_job_by_id(&pool, &job.id).unwrap();
        assert_eq!(run.candidates, 1);
        assert_eq!(run.scored, 1);

        let matches = pool.get_items_by_profile(&profile.id, None, None, 10, 0).unwrap();
        assert_eq!(matches.len(), 1);
    }
}
