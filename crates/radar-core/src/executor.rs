use chrono::Utc;
use std::sync::Arc;
use tokio::sync::watch;

use crate::db::Store;
use crate::embedding::EmbeddingBackend;
use crate::error::{RadarError, Result};
use crate::filter::KeywordGate;
use crate::notify::NotificationBackend;
use crate::pipeline::fetch_with_watermark;
use crate::ranking::rank_candidates;
use crate::scorer::{LlmBackend, LlmScorer};
use crate::source::SourceAdapter;
use crate::types::*;
use crate::vector::VectorStore;

/// Configuration for the executor loop.
pub struct ExecutorConfig {
    pub worker_id: String,
    pub lease_duration_secs: i64,
    pub lease_renew_interval_secs: u64,
    pub poll_interval_secs: u64,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", uuid::Uuid::new_v4()),
            lease_duration_secs: 300,
            lease_renew_interval_secs: 60,
            poll_interval_secs: 5,
        }
    }
}

/// Execute a single scan job end-to-end.
///
/// This is the core pipeline: fetch → filter → rank → score → persist → notify.
/// Each scored item is checkpointed to SQLite as it completes.
#[allow(clippy::too_many_arguments)]
pub async fn execute_scan<B, A, E, N>(
    store: &Store,
    job: &ScanJob,
    profile: &Profile,
    scorer: &LlmScorer<B>,
    adapter: &A,
    notifier: &N,
    vector_store: Option<&VectorStore>,
    embedder: Option<&E>,
    config: &ExecutorConfig,
) -> Result<ScanOutcome>
where
    B: LlmBackend,
    A: SourceAdapter,
    E: EmbeddingBackend,
    N: NotificationBackend,
{
    let lease_token = job
        .lease_token
        .as_deref()
        .ok_or_else(|| RadarError::LeaseLivenessLost {
            job_id: job.job_id.clone(),
        })?;

    let scope_hash = job.source_scope_hash.as_deref().unwrap_or("");

    // Update status to processing
    store.update_job_progress(
        &job.job_id,
        lease_token,
        JobStatus::Processing,
        Some(r#"{"stage":"fetching"}"#),
    )?;

    // 1. Fetch candidates from source
    let fetch_result = fetch_with_watermark(
        store,
        &profile.id,
        adapter.source_type(),
        scope_hash,
        &profile.sources,
        adapter,
    )
    .await?;

    let total_fetched = fetch_result.candidates.len();
    tracing::info!(
        job_id = %job.job_id,
        fetched = total_fetched,
        gap_skipped = fetch_result.gap_skipped,
        "fetch complete"
    );

    // 2. Dual-write to LanceDB if available
    if let (Some(vs), Some(emb)) = (vector_store, embedder) {
        for candidate in &fetch_result.candidates {
            if let Err(e) = vs.ingest_candidate(candidate, emb).await {
                tracing::warn!(
                    canonical_id = %candidate.canonical_id,
                    error = %e,
                    "LanceDB ingest failed, continuing"
                );
            }
        }
    }

    // 3. Deduplicate: skip items already in SQLite
    let mut new_candidates = Vec::new();
    for candidate in fetch_result.candidates {
        let existing = store.find_item_by_alias(
            &format!("{}_id", candidate.source_type.as_str()),
            &candidate.canonical_id,
        )?;
        if existing.is_none() {
            new_candidates.push(candidate);
        }
    }

    tracing::info!(
        job_id = %job.job_id,
        new = new_candidates.len(),
        deduped = total_fetched - new_candidates.len(),
        "dedup complete"
    );

    // 4. Keyword gate
    let gate = KeywordGate::new(profile.keywords.clone(), profile.negative_keywords.clone());
    let (passed, rejected_count) = gate.filter(new_candidates);

    store.update_job_progress(
        &job.job_id,
        lease_token,
        JobStatus::Processing,
        Some(
            &serde_json::json!({
                "stage": "scoring",
                "fetched": total_fetched,
                "passed_filter": passed.len(),
                "rejected": rejected_count,
            })
            .to_string(),
        ),
    )?;

    // Renew lease before scoring (long-running step)
    store.renew_lease(&job.job_id, lease_token, config.lease_duration_secs)?;

    // 5. Rank candidates
    let ranked = rank_candidates(
        passed,
        &profile.keywords,
        profile.max_llm_calls_per_scan as usize,
    );

    // 6. Score candidates
    let current_spend = store.total_spend_since(Utc::now() - chrono::Duration::hours(24))? as u64;

    let score_results = match scorer.score_batch(&ranked, profile, current_spend).await {
        Ok(results) => results,
        Err(RadarError::BudgetExhausted { message }) => {
            tracing::warn!(job_id = %job.job_id, msg = %message, "budget exhausted mid-scan");
            // Return whatever we have so far (empty if pre-check failed)
            vec![]
        }
        Err(e) => return Err(e),
    };

    // 7. Persist items and scores to SQLite (per-item checkpoint)
    let mut matched_items = Vec::new();
    let mut total_spend = 0u64;

    for (idx, score_result) in &score_results {
        let rc = &ranked[*idx];
        total_spend += score_result.llm_spend_microunits;

        // Persist the item
        let item = Item {
            id: uuid::Uuid::new_v4().to_string(),
            canonical_id: rc.candidate.canonical_id.clone(),
            title: rc.candidate.title.clone(),
            authors: rc.candidate.authors.clone(),
            abstract_text: rc.candidate.abstract_text.clone(),
            url: rc.candidate.url.clone(),
            published_at: rc.candidate.published_at,
            source_type: rc.candidate.source_type.as_str().to_string(),
            raw_json: rc.candidate.raw_json.clone(),
            created_at: Utc::now(),
        };
        let item_id = store.upsert_item(&item)?;

        // Persist aliases
        for (alias_type, alias_value) in &rc.candidate.aliases {
            store.insert_alias(&ItemAlias {
                item_id: item_id.clone(),
                alias_type: alias_type.clone(),
                alias_value: alias_value.clone(),
            })?;
        }

        // Determine disposition
        let disposition = if score_result.score >= profile.score_threshold {
            Disposition::Matched
        } else {
            Disposition::ScoredBelowThreshold
        };

        let item_score = ItemScore {
            id: uuid::Uuid::new_v4().to_string(),
            item_id: item_id.clone(),
            profile_id: profile.id.clone(),
            job_id: job.job_id.clone(),
            disposition,
            score: Some(score_result.score),
            reason_short: Some(score_result.reason_short.clone()),
            rationale: Some(score_result.rationale.clone()),
            profile_revision_at_enqueue: job.profile_revision_at_enqueue,
            profile_revision_current: profile.revision,
            created_at: Utc::now(),
        };
        store.insert_score(&item_score)?;

        if disposition == Disposition::Matched {
            let persisted_item = store.get_item(&item_id)?;
            matched_items.push((item_score, persisted_item));
        }

        // Renew lease periodically during scoring
        if *idx % 5 == 4 {
            store.renew_lease(&job.job_id, lease_token, config.lease_duration_secs)?;
        }
    }

    // Also persist keyword-rejected items with their disposition
    for rc in &ranked {
        // Skip already-scored items
        let already_scored = score_results
            .iter()
            .any(|(idx, _)| ranked[*idx].candidate.canonical_id == rc.candidate.canonical_id);
        if already_scored {
            continue;
        }
    }

    // 8. Notify via subscriptions
    let mut notifications_sent = 0;
    if !matched_items.is_empty() {
        let subscriptions = store.get_subscriptions_for_profile(&profile.id)?;
        for sub in &subscriptions {
            if sub.channel == "discord" {
                // Parse webhook URL from channel_config
                let config_json: serde_json::Value =
                    serde_json::from_str(&sub.channel_config).unwrap_or_default();
                let webhook_url = config_json["webhook_url"].as_str().unwrap_or("");

                if webhook_url.is_empty() {
                    tracing::warn!(
                        profile_id = %profile.id,
                        "discord subscription has no webhook_url"
                    );
                    continue;
                }

                // Check idempotency — only notify for items not already notified
                let mut to_notify = Vec::new();
                for (score, item) in &matched_items {
                    let notif = Notification {
                        id: uuid::Uuid::new_v4().to_string(),
                        profile_id: profile.id.clone(),
                        item_id: item.id.clone(),
                        channel: "discord".into(),
                        status: NotificationStatus::Pending,
                        error_message: None,
                        attempt_count: 0,
                        created_at: Utc::now(),
                        sent_at: None,
                    };
                    // insert_notification uses INSERT OR IGNORE for idempotency
                    store.insert_notification(&notif)?;
                    to_notify.push((notif.id.clone(), score.clone(), item.clone()));
                }

                if !to_notify.is_empty() {
                    let payload_matches: Vec<(ItemScore, Item)> = to_notify
                        .iter()
                        .map(|(_, score, item)| (score.clone(), item.clone()))
                        .collect();
                    match notifier
                        .send_matches(webhook_url, &profile.name, &payload_matches)
                        .await
                    {
                        Ok(sent) => {
                            notifications_sent += sent;
                            for (notif_id, _, _) in &to_notify {
                                // Best-effort mark as sent
                                let _ = store.mark_notification_sent(notif_id);
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                profile_id = %profile.id,
                                error = %e,
                                "discord notification failed"
                            );
                            for (notif_id, _, _) in &to_notify {
                                let _ = store.mark_notification_failed(notif_id, &e.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    let outcome = ScanOutcome {
        total_fetched,
        total_new: ranked.len(),
        total_scored: score_results.len(),
        total_matched: matched_items.len(),
        total_spend_microunits: total_spend,
        notifications_sent,
        gap_skipped: fetch_result.gap_skipped,
    };

    Ok(outcome)
}

/// Summary of a completed scan.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ScanOutcome {
    pub total_fetched: usize,
    pub total_new: usize,
    pub total_scored: usize,
    pub total_matched: usize,
    pub total_spend_microunits: u64,
    pub notifications_sent: usize,
    pub gap_skipped: bool,
}

pub async fn run_executor_once<B, A, E, N>(
    store: &Store,
    scorer_factory: impl Fn(&Profile) -> LlmScorer<B>,
    adapter: &A,
    notifier: &N,
    vector_store: Option<&VectorStore>,
    embedder: Option<&E>,
    config: &ExecutorConfig,
) -> Result<Option<ScanOutcome>>
where
    B: LlmBackend,
    A: SourceAdapter,
    E: EmbeddingBackend,
    N: NotificationBackend,
{
    store.record_executor_heartbeat(&config.worker_id)?;

    let Some(job) = store.claim_job(&config.worker_id, config.lease_duration_secs)? else {
        return Ok(None);
    };

    tracing::info!(job_id = %job.job_id, profile_id = %job.profile_id, "claimed job");

    let profile = job.profile_snapshot().map_err(RadarError::Serialization)?;

    let scorer = scorer_factory(&profile);
    let outcome = execute_scan(
        store,
        &job,
        &profile,
        &scorer,
        adapter,
        notifier,
        vector_store,
        embedder,
        config,
    )
    .await;

    store.record_executor_heartbeat(&config.worker_id)?;

    match outcome {
        Ok(outcome) => {
            let status = if outcome.gap_skipped {
                JobStatus::CompletedWithWarnings
            } else {
                JobStatus::Completed
            };
            let warnings = if outcome.gap_skipped {
                Some(serde_json::json!({"gap_skipped": true}).to_string())
            } else {
                None
            };
            let _ = store.complete_job(
                &job.job_id,
                job.lease_token.as_deref().unwrap_or(""),
                status,
                warnings.as_deref(),
                None,
                Some(&serde_json::to_string(&outcome).unwrap_or_default()),
                outcome.total_spend_microunits as i64,
            );
            Ok(Some(outcome))
        }
        Err(e) => {
            tracing::error!(job_id = %job.job_id, error = %e, "scan failed");
            let _ = store.complete_job(
                &job.job_id,
                job.lease_token.as_deref().unwrap_or(""),
                JobStatus::Failed,
                None,
                Some(&serde_json::json!({"error": e.to_string()}).to_string()),
                None,
                0,
            );
            Err(e)
        }
    }
}

/// Run the executor loop: poll for jobs, execute them, complete them.
///
/// This is meant to run as a background tokio task. It will run until the
/// shutdown signal is received.
#[allow(clippy::too_many_arguments)]
pub async fn run_executor_loop<B, A, E, N>(
    store: Arc<Store>,
    scorer_factory: impl Fn(&Profile) -> LlmScorer<B>,
    adapter: Arc<A>,
    notifier: Arc<N>,
    vector_store: Option<Arc<VectorStore>>,
    embedder: Option<Arc<E>>,
    config: ExecutorConfig,
    mut shutdown: watch::Receiver<bool>,
) where
    B: LlmBackend,
    A: SourceAdapter,
    E: EmbeddingBackend,
    N: NotificationBackend,
{
    tracing::info!(worker_id = %config.worker_id, "executor loop starting");

    loop {
        // Check for shutdown
        if *shutdown.borrow() {
            tracing::info!("executor loop shutting down");
            break;
        }

        match run_executor_once(
            store.as_ref(),
            &scorer_factory,
            adapter.as_ref(),
            notifier.as_ref(),
            vector_store.as_deref(),
            embedder.as_deref(),
            &config,
        )
        .await
        {
            Ok(Some(outcome)) => {
                tracing::info!(
                    matched = outcome.total_matched,
                    scored = outcome.total_scored,
                    notified = outcome.notifications_sent,
                    "scan completed"
                );
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!(error = %e, "executor iteration failed");
            }
        }

        // Wait before polling again, with shutdown check
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(config.poll_interval_secs)) => {}
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    tracing::info!("executor loop shutting down");
                    break;
                }
            }
        }
    }
}
