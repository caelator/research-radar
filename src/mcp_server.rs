//! JSON-RPC 2.0 MCP server over stdio with background scan worker.
//!
//! Reads JSON-RPC requests from stdin, writes responses to stdout.
//! On startup, spawns a Tokio background task that polls the scan-job
//! queue and processes jobs asynchronously so `scan_poll` tracks real progress.

use research_radar_core::{
    AnthropicBackend, DbPool, PipelineExecutor, Profile, ScanJobStatus, ScoredMatch, SourceHealth,
    Subscription,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

// ─── Worker health tracking ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct WorkerHealthSnapshot {
    pub started_at: String,
    pub last_poll_at: Option<String>,
    pub last_job_completed_at: Option<String>,
    pub jobs_completed: u64,
    pub jobs_failed: u64,
    pub jobs_dead_lettered: u64,
    pub consecutive_failures: u32,
    pub is_processing: bool,
    pub uptime_seconds: i64,
}

#[derive(Debug)]
struct WorkerHealth {
    started_at: chrono::DateTime<chrono::Utc>,
    last_poll_at: Option<chrono::DateTime<chrono::Utc>>,
    last_job_completed_at: Option<chrono::DateTime<chrono::Utc>>,
    jobs_completed: u64,
    jobs_failed: u64,
    jobs_dead_lettered: u64,
    consecutive_failures: u32,
    is_processing: bool,
}

impl WorkerHealth {
    fn new() -> Self {
        Self {
            started_at: chrono::Utc::now(),
            last_poll_at: None,
            last_job_completed_at: None,
            jobs_completed: 0,
            jobs_failed: 0,
            jobs_dead_lettered: 0,
            consecutive_failures: 0,
            is_processing: false,
        }
    }

    fn snapshot(&self) -> WorkerHealthSnapshot {
        let now = chrono::Utc::now();
        WorkerHealthSnapshot {
            started_at: self.started_at.to_rfc3339(),
            last_poll_at: self.last_poll_at.map(|dt| dt.to_rfc3339()),
            last_job_completed_at: self.last_job_completed_at.map(|dt| dt.to_rfc3339()),
            jobs_completed: self.jobs_completed,
            jobs_failed: self.jobs_failed,
            jobs_dead_lettered: self.jobs_dead_lettered,
            consecutive_failures: self.consecutive_failures,
            is_processing: self.is_processing,
            uptime_seconds: (now - self.started_at).num_seconds(),
        }
    }
}

// ─── JSON-RPC types ────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    pub params: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i32,
    pub message: String,
}

impl JsonRpcResponse {
    fn success(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: Some(result),
            error: None,
        }
    }

    fn error(id: Value, code: i32, message: impl Into<String>) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            id,
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.into(),
            }),
        }
    }
}

// ─── Tool input/output types (flattened for JSON-RPC params/result) ─

#[derive(Debug, Deserialize)]
pub struct ProfileCreateInput {
    pub name: String,
    pub keywords: Vec<String>,
    #[serde(default)]
    pub negative_keywords: Option<Vec<String>>,
    #[serde(default)]
    pub sources: Option<Vec<String>>,
    #[serde(default)]
    pub scoring_prompt: Option<String>,
    #[serde(default)]
    pub score_threshold: Option<f64>,
    #[serde(default)]
    pub max_llm_calls: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct ProfileUpdateInput {
    pub profile_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub keywords: Option<Vec<String>>,
    #[serde(default)]
    pub negative_keywords: Option<Vec<String>>,
    #[serde(default)]
    pub scoring_prompt: Option<String>,
    #[serde(default)]
    pub score_threshold: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct ScanNowInput {
    pub profile_id: String,
    #[serde(default)]
    pub force: Option<bool>,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ScanPollInput {
    pub job_id: String,
}

#[derive(Debug, Deserialize)]
pub struct MatchesListInput {
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub disposition: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub offset: Option<u32>,
    #[serde(default)]
    pub min_score: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct MatchGetInput {
    pub item_id: String,
    #[serde(default)]
    pub profile_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SubscriptionSetInput {
    pub profile_id: String,
    pub channel: String,
    pub config: Value,
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct SourceHealthInput {
    #[serde(default)]
    pub source_type: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ProfileGetInput {
    pub profile_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ProfileDeleteInput {
    pub profile_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ScanHistoryInput {
    #[serde(default)]
    pub profile_id: Option<String>,
    #[serde(default)]
    pub limit: Option<u32>,
}

// ─── Background scan worker ────────────────────────────────────────

/// Spawn a background Tokio task that polls for pending scan jobs and
/// executes them. Runs until `shutdown` is set to `true`.
///
/// Opens its own DbPool so the main stdio thread retains exclusive
/// ownership of its connection.
/// Default interval between poll cycles (seconds).
const WORKER_POLL_INTERVAL_SECS: u64 = 300;

/// Interval between auto-enqueue cycles (seconds).
/// We auto-enqueue less frequently than we poll so burst processing
/// doesn't re-enqueue while jobs are still pending.
const WORKER_ENQUEUE_INTERVAL_SECS: u64 = 600;

fn spawn_scan_worker(
    shutdown: Arc<AtomicBool>,
    health: Arc<Mutex<WorkerHealth>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Open a dedicated DB connection for the worker
        let pool = match DbPool::init() {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("scan worker: failed to open database: {e}");
                return;
            }
        };
        let executor = build_executor();

        tracing::info!("scan worker started");

        let mut last_enqueue = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(WORKER_ENQUEUE_INTERVAL_SECS))
            .unwrap_or_else(std::time::Instant::now);

        while !shutdown.load(Ordering::Relaxed) {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;

            if let Ok(mut h) = health.lock() {
                h.last_poll_at = Some(chrono::Utc::now());
            }

            // ── Reclaim expired leases ─────────────────────────
            match pool.reclaim_expired_leases() {
                Ok((reclaimed, dead_lettered)) => {
                    if reclaimed > 0 {
                        tracing::info!("scan worker: reclaimed {reclaimed} expired lease(s)");
                    }
                    if dead_lettered > 0 {
                        tracing::warn!(
                            "scan worker: dead-lettered {dead_lettered} job(s) (exceeded max attempts)"
                        );
                        if let Ok(mut h) = health.lock() {
                            h.jobs_dead_lettered += dead_lettered as u64;
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("scan worker: lease reclamation failed: {e}");
                }
            }

            // ── Auto-enqueue all active profiles periodically ──
            if last_enqueue.elapsed()
                >= std::time::Duration::from_secs(WORKER_ENQUEUE_INTERVAL_SECS)
            {
                last_enqueue = std::time::Instant::now();
                match pool.list_profiles() {
                    Ok(profiles) => {
                        let active: Vec<_> =
                            profiles.into_iter().filter(|p| !p.is_archived()).collect();
                        for profile in &active {
                            if let Err(e) =
                                pool.enqueue_job(&profile.id, Some("auto-enqueue".into()))
                            {
                                tracing::warn!(
                                    "scan worker: failed to enqueue for '{}': {e}",
                                    profile.name
                                );
                            }
                        }
                        if !active.is_empty() {
                            tracing::info!(
                                "scan worker: auto-enqueued for {} active profile(s)",
                                active.len()
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!("scan worker: failed to list profiles: {e}");
                    }
                }
            }

            // ── Process all available jobs in one burst ─────────
            let mut consecutive_failures = 0u32;
            if let Ok(mut h) = health.lock() {
                h.is_processing = true;
            }
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                match executor.run_next(&pool) {
                    Ok(Some(run)) => {
                        consecutive_failures = 0;
                        if let Ok(mut h) = health.lock() {
                            h.jobs_completed += 1;
                            h.consecutive_failures = 0;
                            h.last_job_completed_at = Some(chrono::Utc::now());
                        }
                        tracing::info!(
                            "scan worker: completed job {} for profile {} — {} accepted",
                            run.job_id,
                            run.profile_id,
                            run.accepted
                        );
                    }
                    Ok(None) => break, // no more pending jobs
                    Err(e) => {
                        consecutive_failures += 1;
                        if let Ok(mut h) = health.lock() {
                            h.jobs_failed += 1;
                            h.consecutive_failures = consecutive_failures;
                        }
                        tracing::warn!("scan worker: job failed ({consecutive_failures}): {e}");
                        if consecutive_failures >= 5 {
                            tracing::error!(
                                "scan worker: too many consecutive failures, backing off"
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                            break;
                        }
                    }
                }
            }
            if let Ok(mut h) = health.lock() {
                h.is_processing = false;
            }

            // ── Sleep until next poll cycle ─────────────────────
            // Sleep in 2-second increments so shutdown is responsive
            let deadline =
                tokio::time::Instant::now() + std::time::Duration::from_secs(WORKER_POLL_INTERVAL_SECS);
            while tokio::time::Instant::now() < deadline && !shutdown.load(Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
        tracing::info!("scan worker shutting down");
    })
}

fn build_executor() -> PipelineExecutor {
    let discord_webhook_url = std::env::var("DISCORD_WEBHOOK_URL")
        .ok()
        .filter(|u| !u.is_empty());
    let executor = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(key) if !key.is_empty() => {
            PipelineExecutor::with_scorer(Arc::new(AnthropicBackend::new(key)))
        }
        _ => PipelineExecutor::new(),
    };
    executor.with_discord_webhook_url(discord_webhook_url)
}

// ─── Server implementation ──────────────────────────────────────────

/// Start the MCP server: spawn background scan worker, then read
/// JSON-RPC requests from stdin and respond to stdout.
pub fn run_mcp_server(pool: &DbPool) -> io::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

    rt.block_on(async {
        let shutdown = Arc::new(AtomicBool::new(false));
        let worker_health = Arc::new(Mutex::new(WorkerHealth::new()));
        let worker_handle = spawn_scan_worker(Arc::clone(&shutdown), Arc::clone(&worker_health));

        // Run the stdio loop directly. This blocks the async runtime's
        // main thread, but that's fine — the scan worker runs on the
        // Tokio thread pool via `tokio::spawn`.
        let stdio_result = run_stdio_loop(pool, &worker_health);

        // Shutdown the background worker gracefully
        shutdown.store(true, Ordering::Relaxed);
        let _ = worker_handle.await;

        stdio_result
    })
}

fn run_stdio_loop(pool: &DbPool, worker_health: &Arc<Mutex<WorkerHealth>>) -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let mut handle = stdin.lock();

    loop {
        let mut line = String::new();
        match handle.read_line(&mut line) {
            Ok(0) => break, // EOF
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }

        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        let req: JsonRpcRequest = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(e) => {
                let resp = JsonRpcResponse::error(Value::Null, -32700, format!("Parse error: {e}"));
                let out = serde_json::to_string(&resp).unwrap_or_default();
                writeln!(stdout, "{}", out)?;
                stdout.flush()?;
                continue;
            }
        };

        let resp = handle_request(pool, req, worker_health);
        let out = serde_json::to_string(&resp).unwrap_or_default();
        writeln!(stdout, "{}", out)?;
        stdout.flush()?;
    }

    Ok(())
}

fn handle_request(
    pool: &DbPool,
    req: JsonRpcRequest,
    worker_health: &Arc<Mutex<WorkerHealth>>,
) -> JsonRpcResponse {
    let id = req.id.clone();

    let result = match req.method.as_str() {
        "profile_create" => handle_profile_create(pool, req.params),
        "profile_update" => handle_profile_update(pool, req.params),
        "profile_list" => handle_profile_list(pool),
        "profile_get" => handle_profile_get(pool, req.params),
        "profile_delete" => handle_profile_delete(pool, req.params),
        "scan_now" => handle_scan_now(pool, req.params),
        "scan_poll" => handle_scan_poll(pool, req.params),
        "scan_history" => handle_scan_history(pool, req.params),
        "matches_list" => handle_matches_list(pool, req.params),
        "match_get" => handle_match_get(pool, req.params),
        "subscription_set" => handle_subscription_set(pool, req.params),
        "source_health" => handle_source_health(pool, req.params),
        "worker_health" => handle_worker_health(worker_health),
        _ => {
            return JsonRpcResponse::error(id, -32601, "Method not found");
        }
    };

    match result {
        Ok(v) => JsonRpcResponse::success(id, v),
        Err(e) => JsonRpcResponse::error(id, -32600, e),
    }
}

// ─── Tool handlers ─────────────────────────────────────────────────

fn handle_profile_create(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: ProfileCreateInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    let mut profile = Profile::new(input.name, input.keywords);
    if let Some(nk) = input.negative_keywords {
        profile.negative_keywords = nk;
    }
    if let Some(srcs) = input.sources {
        profile.sources = srcs;
    }
    profile.scoring_prompt = input.scoring_prompt;
    if let Some(t) = input.score_threshold {
        profile.score_threshold = t;
    }
    if let Some(m) = input.max_llm_calls {
        profile.max_llm_calls = m;
    }

    pool.insert_profile(&profile).map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "profile_id": profile.id }))
}

fn handle_profile_update(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: ProfileUpdateInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    let mut profile = pool
        .get_profile(&input.profile_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Profile not found".to_string())?;

    if let Some(name) = input.name {
        profile.name = name;
    }
    if let Some(keywords) = input.keywords {
        profile.keywords = keywords;
    }
    if let Some(nk) = input.negative_keywords {
        profile.negative_keywords = nk;
    }
    if let Some(prompt) = input.scoring_prompt {
        profile.scoring_prompt = Some(prompt);
    }
    if let Some(t) = input.score_threshold {
        profile.score_threshold = t;
    }

    pool.update_profile(&profile).map_err(|e| e.to_string())?;

    serde_json::to_value(&profile).map_err(|e| e.to_string())
}

fn handle_scan_now(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: ScanNowInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    // Verify profile exists
    pool.get_profile(&input.profile_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Profile not found".to_string())?;

    // Check for existing active job
    if input.force != Some(true) {
        if let Some(active) = pool
            .get_active_scan_job(&input.profile_id)
            .map_err(|e| e.to_string())?
        {
            return Ok(serde_json::json!({
                "job_id": active.id,
                "reused": true
            }));
        }
    }

    let job = if input.force == Some(true) {
        let job = research_radar_core::ScanJob::new(input.profile_id.clone(), input.reason);
        pool.insert_scan_job(&job).map_err(|e| e.to_string())?;
        job
    } else {
        pool.enqueue_job(&input.profile_id, input.reason)
            .map_err(|e| e.to_string())?
    };

    Ok(serde_json::json!({
        "job_id": job.id,
        "reused": job.status != ScanJobStatus::Pending
    }))
}

fn handle_scan_poll(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: ScanPollInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    let job = pool
        .get_scan_job(&input.job_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Job not found".to_string())?;

    let results_summary = match job.status {
        ScanJobStatus::Complete => "Scan complete.".to_string(),
        ScanJobStatus::Failed => "Scan failed.".to_string(),
        ScanJobStatus::Pending => "Scan is pending.".to_string(),
        ScanJobStatus::Running => format!("Running: {}/{} processed.", job.progress, job.total),
    };

    Ok(serde_json::json!({
        "status": job.status.as_str(),
        "progress": job.progress,
        "total": job.total,
        "results_summary": results_summary
    }))
}

fn handle_matches_list(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: MatchesListInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    let profile_id = input
        .profile_id
        .as_deref()
        .ok_or_else(|| "profile_id is required".to_string())?;

    let limit = input.limit.unwrap_or(20).min(100) as usize;
    let offset = input.offset.unwrap_or(0) as usize;
    let min_score = input.min_score;
    let disposition = input.disposition.as_deref();

    let matches = pool
        .get_items_by_profile(profile_id, disposition, min_score, limit, offset)
        .map_err(|e| e.to_string())?;

    let items: Vec<Value> = matches
        .into_iter()
        .map(|m: ScoredMatch| {
            serde_json::json!({
                "item_id": m.entry.id,
                "score": m.score,
                "disposition": m.disposition,
                "entry": m.entry
            })
        })
        .collect();

    Ok(serde_json::json!({ "items": items, "count": items.len() }))
}

fn handle_match_get(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: MatchGetInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    let entry = pool
        .get_entry(&input.item_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Item not found".to_string())?;

    let mut result = serde_json::json!({ "entry": entry });

    if let Some(profile_id) = &input.profile_id {
        let matches = pool
            .get_items_by_profile(profile_id, None, None, 1, 0)
            .map_err(|e| e.to_string())?;

        if let Some(m) = matches.into_iter().find(|m| m.entry.id == input.item_id) {
            result["score"] = serde_json::json!(m.score);
            result["disposition"] = serde_json::json!(m.disposition);
        }
    }

    Ok(result)
}

fn handle_subscription_set(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: SubscriptionSetInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    // Verify profile exists
    pool.get_profile(&input.profile_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Profile not found".to_string())?;

    // Check if subscription already exists for this profile + channel
    if let Some(mut existing) = pool
        .get_subscription_by_profile_channel(&input.profile_id, &input.channel)
        .map_err(|e| e.to_string())?
    {
        existing.config = input.config;
        existing.enabled = input.enabled;
        pool.update_subscription(&existing)
            .map_err(|e| e.to_string())?;
        return Ok(serde_json::json!({ "subscription_id": existing.id }));
    }

    let sub = Subscription::new(input.profile_id, input.channel, input.config, input.enabled);
    pool.insert_subscription(&sub).map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "subscription_id": sub.id }))
}

fn handle_source_health(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: SourceHealthInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    let health: Vec<SourceHealth> = pool
        .get_source_health(input.source_type.as_deref())
        .map_err(|e| e.to_string())?;

    let sources: Vec<Value> = health
        .into_iter()
        .map(|h: SourceHealth| {
            serde_json::json!({
                "source_type": h.source_type,
                "status": h.status,
                "last_scan": h.last_scan,
                "items_count": h.items_count,
                "avg_relevance": h.avg_relevance
            })
        })
        .collect();

    Ok(serde_json::json!({ "sources": sources }))
}

fn handle_worker_health(worker_health: &Arc<Mutex<WorkerHealth>>) -> Result<Value, String> {
    let snapshot = worker_health
        .lock()
        .map_err(|e| format!("health lock poisoned: {e}"))?
        .snapshot();
    serde_json::to_value(&snapshot).map_err(|e| e.to_string())
}

fn handle_profile_list(pool: &DbPool) -> Result<Value, String> {
    let profiles = pool.list_profiles().map_err(|e| e.to_string())?;
    let items: Vec<Value> = profiles
        .into_iter()
        .map(|p| serde_json::to_value(&p).unwrap_or(Value::Null))
        .collect();
    Ok(serde_json::json!({ "profiles": items, "count": items.len() }))
}

fn handle_profile_get(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: ProfileGetInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    let profile = pool
        .get_profile(&input.profile_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Profile not found".to_string())?;

    serde_json::to_value(&profile).map_err(|e| e.to_string())
}

fn handle_profile_delete(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: ProfileDeleteInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    pool.get_profile(&input.profile_id)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "Profile not found".to_string())?;

    pool.delete_profile(&input.profile_id)
        .map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "deleted": true }))
}

fn handle_scan_history(pool: &DbPool, params: Option<Value>) -> Result<Value, String> {
    let input: ScanHistoryInput = serde_json::from_value(params.unwrap_or(Value::Null))
        .map_err(|e| format!("Invalid params: {e}"))?;

    let limit = input.limit.unwrap_or(20).min(100) as usize;

    let jobs = match input.profile_id {
        Some(ref pid) => pool.list_scan_jobs(pid, limit).map_err(|e| e.to_string())?,
        None => pool
            .list_recent_scan_jobs(limit)
            .map_err(|e| e.to_string())?,
    };

    let items: Vec<Value> = jobs
        .into_iter()
        .map(|j| {
            serde_json::json!({
                "job_id": j.id,
                "profile_id": j.profile_id,
                "status": j.status.as_str(),
                "progress": j.progress,
                "total": j.total,
                "reason": j.reason,
                "attempt_count": j.attempt_count,
                "created_at": j.created_at.to_rfc3339(),
                "completed_at": j.completed_at.map(|dt| dt.to_rfc3339()),
            })
        })
        .collect();

    Ok(serde_json::json!({ "jobs": items, "count": items.len() }))
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use research_radar_core::SourceType;

    fn test_pool() -> DbPool {
        DbPool::test_pool().expect("failed to create test pool")
    }

    fn test_worker_health() -> Arc<Mutex<WorkerHealth>> {
        Arc::new(Mutex::new(WorkerHealth::new()))
    }

    fn rpc_call(pool: &DbPool, method: &str, params: Value) -> JsonRpcResponse {
        let health = test_worker_health();
        let req = JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id: Value::Null,
            method: method.into(),
            params: Some(params),
        };
        handle_request(pool, req, &health)
    }

    #[test]
    fn test_profile_create_and_get() {
        let pool = test_pool();
        let resp = rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({
                "name": "AI Research",
                "keywords": ["AI", "safety", "ML"]
            }),
        );
        assert!(resp.error.is_none());
        let result = resp.result.as_ref().unwrap();
        let profile_id = result["profile_id"].as_str().unwrap();

        let resp2 = rpc_call(
            &pool,
            "profile_update",
            serde_json::json!({
                "profile_id": profile_id,
                "name": "Updated AI Research"
            }),
        );
        assert!(resp2.error.is_none());
        let result2 = resp2.result.as_ref().unwrap();
        assert_eq!(result2["name"].as_str().unwrap(), "Updated AI Research");
    }

    #[test]
    fn test_profile_update() {
        let pool = test_pool();
        let resp = rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({
                "name": "Original",
                "keywords": ["test"]
            }),
        );
        let result = resp.result.as_ref().unwrap();
        let profile_id = result["profile_id"].as_str().unwrap();

        let resp2 = rpc_call(
            &pool,
            "profile_update",
            serde_json::json!({
                "profile_id": profile_id,
                "keywords": ["test", "updated"],
                "score_threshold": 0.7
            }),
        );
        assert!(resp2.error.is_none());
        let result2 = resp2.result.as_ref().unwrap();
        assert_eq!(result2["keywords"].as_array().unwrap().len(), 2);
        assert_eq!(result2["score_threshold"].as_f64().unwrap(), 0.7);
    }

    #[test]
    fn test_scan_now_and_poll() {
        let pool = test_pool();

        let create_resp = rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({"name": "Test Profile", "keywords": ["test"]}),
        );
        let result = create_resp.result.as_ref().unwrap();
        let profile_id = result["profile_id"].as_str().unwrap();

        let scan_resp = rpc_call(
            &pool,
            "scan_now",
            serde_json::json!({"profile_id": profile_id}),
        );
        assert!(scan_resp.error.is_none());
        let scan_result = scan_resp.result.as_ref().unwrap();
        let job_id = scan_result["job_id"].as_str().unwrap();
        assert_eq!(scan_result["reused"], false);

        let scan_resp2 = rpc_call(
            &pool,
            "scan_now",
            serde_json::json!({"profile_id": profile_id}),
        );
        let scan_result2 = scan_resp2.result.as_ref().unwrap();
        assert_eq!(scan_result2["job_id"].as_str().unwrap(), job_id);
        assert_eq!(scan_result2["reused"], true);

        let poll_resp = rpc_call(&pool, "scan_poll", serde_json::json!({"job_id": job_id}));
        assert!(poll_resp.error.is_none());
        let poll_result = poll_resp.result.as_ref().unwrap();
        assert_eq!(poll_result["status"].as_str().unwrap(), "pending");
    }

    #[test]
    fn test_matches_list() {
        let pool = test_pool();

        // Create profile
        let create_resp = rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({"name": "Test", "keywords": ["AI"]}),
        );
        let result = create_resp.result.as_ref().unwrap();
        let profile_id = result["profile_id"].as_str().unwrap();

        // Add a source and entry
        let src = research_radar_core::Source::new(
            "https://example.com".into(),
            "Example".into(),
            SourceType::Web,
        );
        pool.insert_source(&src).unwrap();
        let entry =
            research_radar_core::Entry::new(src.id.clone(), "AI safety research paper".into());
        pool.insert_entry(&entry).unwrap();
        pool.upsert_item_score(&entry.id, profile_id, 0.85, "new")
            .unwrap();

        // List matches
        let resp = rpc_call(
            &pool,
            "matches_list",
            serde_json::json!({"profile_id": profile_id}),
        );
        assert!(resp.error.is_none());
        let resp_result = resp.result.as_ref().unwrap();
        let items = resp_result["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["score"].as_f64().unwrap(), 0.85);
    }

    #[test]
    fn test_subscription_set() {
        let pool = test_pool();

        // Create profile
        let create_resp = rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({"name": "Test", "keywords": ["test"]}),
        );
        let result = create_resp.result.as_ref().unwrap();
        let profile_id = result["profile_id"].as_str().unwrap();

        // Set subscription
        let resp = rpc_call(
            &pool,
            "subscription_set",
            serde_json::json!({
                "profile_id": profile_id,
                "channel": "email",
                "config": {"address": "test@example.com"},
                "enabled": true
            }),
        );
        assert!(resp.error.is_none());
        let resp_result = resp.result.as_ref().unwrap();
        let sub_id = resp_result["subscription_id"].as_str().unwrap();
        assert!(!sub_id.is_empty());

        // Update subscription
        let resp2 = rpc_call(
            &pool,
            "subscription_set",
            serde_json::json!({
                "profile_id": profile_id,
                "channel": "email",
                "config": {"address": "new@example.com"},
                "enabled": false
            }),
        );
        assert!(resp2.error.is_none());
        let resp2_result = resp2.result.as_ref().unwrap();
        assert_eq!(resp2_result["subscription_id"].as_str().unwrap(), sub_id);
    }

    #[test]
    fn test_source_health() {
        let pool = test_pool();
        let src = research_radar_core::Source::new(
            "https://example.com".into(),
            "Example".into(),
            SourceType::Web,
        );
        pool.insert_source(&src).unwrap();
        let entry = research_radar_core::Entry::new(src.id.clone(), "AI content".into());
        pool.insert_entry(&entry).unwrap();

        let resp = rpc_call(&pool, "source_health", serde_json::json!({}));
        assert!(resp.error.is_none());
        let resp_result = resp.result.as_ref().unwrap();
        let sources = resp_result["sources"].as_array().unwrap();
        assert!(!sources.is_empty());
    }

    #[test]
    fn test_profile_list() {
        let pool = test_pool();
        // Create two profiles
        rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({"name": "A", "keywords": ["a"]}),
        );
        rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({"name": "B", "keywords": ["b"]}),
        );
        let resp = rpc_call(&pool, "profile_list", serde_json::json!({}));
        assert!(resp.error.is_none());
        let result = resp.result.as_ref().unwrap();
        assert_eq!(result["count"].as_u64().unwrap(), 2);
        assert_eq!(result["profiles"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_profile_get() {
        let pool = test_pool();
        let create_resp = rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({"name": "Lookup", "keywords": ["x"]}),
        );
        let pid = create_resp.result.as_ref().unwrap()["profile_id"]
            .as_str()
            .unwrap()
            .to_string();

        let resp = rpc_call(
            &pool,
            "profile_get",
            serde_json::json!({"profile_id": pid}),
        );
        assert!(resp.error.is_none());
        let result = resp.result.as_ref().unwrap();
        assert_eq!(result["name"].as_str().unwrap(), "Lookup");
    }

    #[test]
    fn test_profile_delete() {
        let pool = test_pool();
        let create_resp = rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({"name": "ToDelete", "keywords": ["x"]}),
        );
        let pid = create_resp.result.as_ref().unwrap()["profile_id"]
            .as_str()
            .unwrap()
            .to_string();

        let resp = rpc_call(
            &pool,
            "profile_delete",
            serde_json::json!({"profile_id": pid}),
        );
        assert!(resp.error.is_none());
        assert_eq!(resp.result.as_ref().unwrap()["deleted"], true);

        // Verify it's gone
        let get_resp = rpc_call(
            &pool,
            "profile_get",
            serde_json::json!({"profile_id": pid}),
        );
        assert!(get_resp.error.is_some());
    }

    #[test]
    fn test_scan_history() {
        let pool = test_pool();
        let create_resp = rpc_call(
            &pool,
            "profile_create",
            serde_json::json!({"name": "History", "keywords": ["h"]}),
        );
        let pid = create_resp.result.as_ref().unwrap()["profile_id"]
            .as_str()
            .unwrap()
            .to_string();

        // Enqueue a job
        rpc_call(
            &pool,
            "scan_now",
            serde_json::json!({"profile_id": pid}),
        );

        // Query history by profile
        let resp = rpc_call(
            &pool,
            "scan_history",
            serde_json::json!({"profile_id": pid}),
        );
        assert!(resp.error.is_none());
        let result = resp.result.as_ref().unwrap();
        assert_eq!(result["count"].as_u64().unwrap(), 1);

        // Query all history
        let resp2 = rpc_call(&pool, "scan_history", serde_json::json!({}));
        assert!(resp2.error.is_none());
        assert!(resp2.result.as_ref().unwrap()["count"].as_u64().unwrap() >= 1);
    }

    #[test]
    fn test_worker_health_endpoint() {
        let pool = test_pool();
        let resp = rpc_call(&pool, "worker_health", serde_json::json!({}));
        assert!(resp.error.is_none());
        let result = resp.result.as_ref().unwrap();
        assert!(result["started_at"].as_str().is_some());
        assert_eq!(result["jobs_completed"].as_u64().unwrap(), 0);
        assert_eq!(result["jobs_failed"].as_u64().unwrap(), 0);
        assert_eq!(result["is_processing"].as_bool().unwrap(), false);
        assert!(result["uptime_seconds"].as_i64().unwrap() >= 0);
    }

    #[test]
    fn test_method_not_found() {
        let pool = test_pool();
        let resp = rpc_call(&pool, "unknown_method", serde_json::json!({}));
        assert!(resp.error.is_some());
        assert_eq!(resp.error.unwrap().code, -32601);
    }
}
