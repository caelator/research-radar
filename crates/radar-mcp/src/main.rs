//! radar-mcp — stdio JSON-RPC server implementing the Model Context Protocol
//! for research-radar.
//!
//! Exposes 18 tools across three phases:
//!   Phase 1: profile_create, profile_update, scan_now, scan_poll,
//!            matches_list, match_get, subscription_set, source_health
//!   Phase 2: corpus_search, corpus_similar, corpus_concepts
//!   Phase 3: research_brief, relevance_explain, gap_analysis,
//!            trend_detect, cross_pollinate, citation_graph, digest_compose

use std::collections::HashMap;
use std::io::{self, BufRead, Write as _};
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use radar_core::db::Store;
use radar_core::embedding::MockEmbeddingBackend;
use radar_core::types::*;
use radar_core::vector::VectorStore;

const EXECUTOR_HEARTBEAT_TTL_SECS: i64 = 120;

// ─── JSON-RPC types ──────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    #[allow(dead_code)]
    jsonrpc: String,
    method: String,
    #[serde(default)]
    params: Value,
    id: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
    id: Value,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

impl JsonRpcResponse {
    fn ok(id: Value, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            result: Some(result),
            error: None,
            id,
        }
    }

    fn err(id: Value, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0".into(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }
}

// ─── MCP tool schema helpers ─────────────────────────────────────────

fn prop(ty: &str, desc: &str) -> Value {
    json!({ "type": ty, "description": desc })
}

fn prop_array(item_ty: &str, desc: &str) -> Value {
    json!({ "type": "array", "items": { "type": item_ty }, "description": desc })
}

fn prop_num(desc: &str) -> Value {
    json!({ "type": "number", "description": desc })
}

fn prop_bool(desc: &str) -> Value {
    json!({ "type": "boolean", "description": desc })
}

fn prop_int(desc: &str) -> Value {
    json!({ "type": "integer", "description": desc })
}

fn tool_def(name: &str, description: &str, required: &[&str], properties: Value) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
        }
    })
}

fn all_tools() -> Vec<Value> {
    vec![
        // Phase 1
        tool_def(
            "profile_create",
            "Create a new research monitoring profile",
            &["name", "keywords"],
            json!({
                "name": prop("string", "Profile name"),
                "keywords": prop_array("string", "Keywords to monitor"),
                "negative_keywords": prop_array("string", "Keywords to exclude"),
                "sources": prop_array("string", "Source types to scan (arxiv, semantic_scholar, huggingface_daily_papers, rss)"),
                "scoring_prompt": prop("string", "Custom LLM scoring prompt"),
                "score_threshold": prop_num("Minimum score threshold (0.0-1.0)"),
                "max_llm_calls": prop_int("Maximum LLM calls per scan"),
            }),
        ),
        tool_def(
            "profile_update",
            "Update an existing research monitoring profile",
            &["profile_id", "revision"],
            json!({
                "profile_id": prop("string", "Profile ID to update"),
                "revision": prop_int("Current revision number for optimistic concurrency"),
                "name": prop("string", "New profile name"),
                "keywords": prop_array("string", "New keywords"),
                "negative_keywords": prop_array("string", "New negative keywords"),
                "sources": prop_array("string", "New source types"),
                "scoring_prompt": prop("string", "New LLM scoring prompt"),
                "score_threshold": prop_num("New score threshold"),
                "max_llm_calls": prop_int("New max LLM calls per scan"),
                "description": prop("string", "Profile description"),
            }),
        ),
        tool_def(
            "scan_now",
            "Trigger an immediate scan for a profile",
            &["profile_id"],
            json!({
                "profile_id": prop("string", "Profile to scan"),
                "force": prop_bool("Force a new scan even if one is active"),
                "reason": prop("string", "Reason for triggering scan"),
            }),
        ),
        tool_def(
            "scan_poll",
            "Check the status of a scan job",
            &["job_id"],
            json!({
                "job_id": prop("string", "Job ID to poll"),
            }),
        ),
        tool_def(
            "matches_list",
            "List matched research items, optionally filtered by profile",
            &[],
            json!({
                "profile_id": prop("string", "Filter by profile ID"),
                "disposition": prop("string", "Filter by disposition (matched, keyword_rejected, scored_below_threshold)"),
                "limit": prop_int("Maximum results to return (default 20)"),
                "offset": prop_int("Pagination offset (default 0)"),
                "min_score": prop_num("Minimum score filter"),
            }),
        ),
        tool_def(
            "match_get",
            "Get full details for a specific research item including scores",
            &["item_id"],
            json!({
                "item_id": prop("string", "Item ID to retrieve"),
                "profile_id": prop("string", "Profile for score context"),
            }),
        ),
        tool_def(
            "subscription_set",
            "Create or update a notification subscription for a profile",
            &["profile_id", "channel", "config", "enabled"],
            json!({
                "profile_id": prop("string", "Profile to subscribe"),
                "channel": prop("string", "Notification channel (discord, telegram)"),
                "config": prop("string", "Channel config JSON (webhook URL, chat ID, etc.)"),
                "enabled": prop_bool("Whether subscription is active"),
            }),
        ),
        tool_def(
            "source_health",
            "Check health status of research sources",
            &[],
            json!({
                "source_type": prop("string", "Filter by source type"),
            }),
        ),
        // Phase 2
        tool_def(
            "corpus_search",
            "Semantic search across the research corpus using vector similarity",
            &["query"],
            json!({
                "query": prop("string", "Natural language search query"),
                "limit": prop_int("Maximum results (default 10)"),
                "profile_id": prop("string", "Scope to a profile's items"),
                "source_type": prop("string", "Filter by source type"),
            }),
        ),
        tool_def(
            "corpus_similar",
            "Find items similar to a given research item",
            &["item_id"],
            json!({
                "item_id": prop("string", "Item to find neighbors for"),
                "limit": prop_int("Maximum results (default 10)"),
            }),
        ),
        tool_def(
            "corpus_concepts",
            "List discovered concept clusters in the corpus",
            &[],
            json!({
                "limit": prop_int("Maximum concepts to return"),
            }),
        ),
        // Phase 3
        tool_def(
            "research_brief",
            "Generate a structured research briefing from recent matches",
            &["profile_id"],
            json!({
                "profile_id": prop("string", "Profile to brief on"),
                "days": prop_int("Lookback window in days (default 7)"),
            }),
        ),
        tool_def(
            "relevance_explain",
            "Explain why an item was scored the way it was for a project context",
            &["item_id", "project_context"],
            json!({
                "item_id": prop("string", "Item to explain"),
                "project_context": prop("string", "Description of your project for relevance assessment"),
            }),
        ),
        tool_def(
            "gap_analysis",
            "Identify research gaps based on profile keywords vs actual matches",
            &["profile_id", "project_goals"],
            json!({
                "profile_id": prop("string", "Profile to analyze"),
                "project_goals": prop("string", "Description of project goals to compare against"),
            }),
        ),
        tool_def(
            "trend_detect",
            "Detect trending topics and score trends over time",
            &[],
            json!({
                "profile_id": prop("string", "Scope to a profile"),
                "days": prop_int("Lookback window in days (default 30)"),
            }),
        ),
        tool_def(
            "cross_pollinate",
            "Find items relevant across multiple profiles",
            &["profile_ids"],
            json!({
                "profile_ids": prop_array("string", "Profile IDs to cross-reference (2+)"),
            }),
        ),
        tool_def(
            "citation_graph",
            "Retrieve citation relationships for a paper via Semantic Scholar",
            &["item_id"],
            json!({
                "item_id": prop("string", "Item ID (or Semantic Scholar paper ID)"),
                "depth": prop_int("Citation traversal depth (default 1, max 2)"),
            }),
        ),
        tool_def(
            "digest_compose",
            "Compose a formatted digest of recent research matches",
            &["profile_id"],
            json!({
                "profile_id": prop("string", "Profile to digest"),
                "format": prop("string", "Output format: markdown (default), plain, html"),
                "days": prop_int("Lookback window in days (default 7)"),
            }),
        ),
    ]
}

// ─── Server state ────────────────────────────────────────────────────

struct ServerState {
    store: Store,
    vector_store: VectorStore,
    embedder: MockEmbeddingBackend,
}

// ─── Handlers ────────────────────────────────────────────────────────

fn handle_initialize(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::ok(
        id,
        json!({
            "protocolVersion": "2024-11-05",
            "capabilities": {
                "tools": {}
            },
            "serverInfo": {
                "name": "research-radar",
                "version": env!("CARGO_PKG_VERSION"),
            }
        }),
    )
}

fn handle_tools_list(id: Value) -> JsonRpcResponse {
    JsonRpcResponse::ok(id, json!({ "tools": all_tools() }))
}

async fn handle_tools_call(id: Value, params: &Value, state: &ServerState) -> JsonRpcResponse {
    let tool_name = params.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let result = match tool_name {
        "profile_create" => handle_profile_create(&args, state),
        "profile_update" => handle_profile_update(&args, state),
        "scan_now" => handle_scan_now(&args, state),
        "scan_poll" => handle_scan_poll(&args, state),
        "matches_list" => handle_matches_list(&args, state),
        "match_get" => handle_match_get(&args, state),
        "subscription_set" => handle_subscription_set(&args, state),
        "source_health" => handle_source_health(&args, state),
        "corpus_search" => handle_corpus_search(&args, state).await,
        "corpus_similar" => handle_corpus_similar(&args, state).await,
        "corpus_concepts" => handle_corpus_concepts(&args, state).await,
        "research_brief" => handle_research_brief(&args, state),
        "relevance_explain" => handle_relevance_explain(&args, state),
        "gap_analysis" => handle_gap_analysis(&args, state),
        "trend_detect" => handle_trend_detect(&args, state),
        "cross_pollinate" => handle_cross_pollinate(&args, state),
        "citation_graph" => handle_citation_graph(&args).await,
        "digest_compose" => handle_digest_compose(&args, state),
        _ => Err(format!("unknown tool: {tool_name}")),
    };

    match result {
        Ok(content) => JsonRpcResponse::ok(
            id,
            json!({
                "content": [{
                    "type": "text",
                    "text": serde_json::to_string_pretty(&content).unwrap_or_default(),
                }]
            }),
        ),
        Err(e) => JsonRpcResponse::ok(
            id,
            json!({
                "content": [{
                    "type": "text",
                    "text": e,
                }],
                "isError": true,
            }),
        ),
    }
}

// ─── Phase 1 tool handlers ──────────────────────────────────────────

fn handle_profile_create(args: &Value, state: &ServerState) -> Result<Value, String> {
    let name = args
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: name")?;
    let keywords: Vec<String> = args
        .get("keywords")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .ok_or("missing required field: keywords")?;

    let mut profile = Profile::new(name.to_string());
    profile.keywords = keywords;

    if let Some(neg) = args.get("negative_keywords")
        && let Ok(v) = serde_json::from_value::<Vec<String>>(neg.clone())
    {
        profile.negative_keywords = v;
    }
    if let Some(sources) = args.get("sources")
        && let Ok(v) = serde_json::from_value::<Vec<String>>(sources.clone())
    {
        profile.sources = v;
    }
    if let Some(prompt) = args.get("scoring_prompt").and_then(|v| v.as_str()) {
        profile.llm_scoring_prompt = Some(prompt.to_string());
    }
    if let Some(thresh) = args.get("score_threshold").and_then(|v| v.as_f64()) {
        profile.score_threshold = thresh;
    }
    if let Some(max) = args.get("max_llm_calls").and_then(|v| v.as_u64()) {
        profile.max_llm_calls_per_scan = max as u32;
    }

    state
        .store
        .insert_profile(&profile)
        .map_err(|e| format!("failed to create profile: {e}"))?;

    Ok(json!({
        "profile_id": profile.id,
        "name": profile.name,
        "keywords": profile.keywords,
        "negative_keywords": profile.negative_keywords,
        "sources": profile.sources,
        "score_threshold": profile.score_threshold,
        "max_llm_calls_per_scan": profile.max_llm_calls_per_scan,
        "revision": profile.revision,
        "created_at": profile.created_at.to_rfc3339(),
    }))
}

fn handle_profile_update(args: &Value, state: &ServerState) -> Result<Value, String> {
    let profile_id = args
        .get("profile_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: profile_id")?;
    let revision = args
        .get("revision")
        .and_then(|v| v.as_i64())
        .ok_or("missing required field: revision")?;

    let mut profile = state
        .store
        .get_profile(profile_id)
        .map_err(|e| format!("profile lookup failed: {e}"))?;

    if profile.revision != revision {
        return Err(format!(
            "revision mismatch: expected {}, got {revision}",
            profile.revision
        ));
    }

    if let Some(name) = args.get("name").and_then(|v| v.as_str()) {
        profile.name = name.to_string();
    }
    if let Some(desc) = args.get("description").and_then(|v| v.as_str()) {
        profile.description = Some(desc.to_string());
    }
    if let Some(kw) = args.get("keywords")
        && let Ok(v) = serde_json::from_value::<Vec<String>>(kw.clone())
    {
        profile.keywords = v;
    }
    if let Some(neg) = args.get("negative_keywords")
        && let Ok(v) = serde_json::from_value::<Vec<String>>(neg.clone())
    {
        profile.negative_keywords = v;
    }
    if let Some(sources) = args.get("sources")
        && let Ok(v) = serde_json::from_value::<Vec<String>>(sources.clone())
    {
        profile.sources = v;
    }
    if let Some(prompt) = args.get("scoring_prompt").and_then(|v| v.as_str()) {
        profile.llm_scoring_prompt = Some(prompt.to_string());
    }
    if let Some(thresh) = args.get("score_threshold").and_then(|v| v.as_f64()) {
        profile.score_threshold = thresh;
    }
    if let Some(max) = args.get("max_llm_calls").and_then(|v| v.as_u64()) {
        profile.max_llm_calls_per_scan = max as u32;
    }

    profile.revision += 1;
    profile.updated_at = Utc::now();

    state
        .store
        .update_profile(&profile)
        .map_err(|e| format!("failed to update profile: {e}"))?;

    Ok(json!({
        "profile_id": profile.id,
        "name": profile.name,
        "keywords": profile.keywords,
        "negative_keywords": profile.negative_keywords,
        "sources": profile.sources,
        "score_threshold": profile.score_threshold,
        "max_llm_calls_per_scan": profile.max_llm_calls_per_scan,
        "revision": profile.revision,
        "updated_at": profile.updated_at.to_rfc3339(),
    }))
}

fn handle_scan_now(args: &Value, state: &ServerState) -> Result<Value, String> {
    let profile_id = args
        .get("profile_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: profile_id")?;
    let force = args.get("force").and_then(|v| v.as_bool()).unwrap_or(false);
    let reason = args
        .get("reason")
        .and_then(|v| v.as_str())
        .unwrap_or("mcp_scan_now");

    let profile = state
        .store
        .get_profile(profile_id)
        .map_err(|e| format!("profile lookup failed: {e}"))?;

    let (job, reused) = state
        .store
        .trigger_scan(
            profile_id,
            &profile.sources,
            reason,
            force,
            EXECUTOR_HEARTBEAT_TTL_SECS,
        )
        .map_err(|e| format!("failed to enqueue scan: {e}"))?;

    Ok(json!({
        "job_id": job.job_id,
        "reused": reused,
        "status": job.status.as_str(),
        "profile_id": job.profile_id,
        "created_at": job.created_at.to_rfc3339(),
    }))
}

fn handle_scan_poll(args: &Value, state: &ServerState) -> Result<Value, String> {
    let job_id = args
        .get("job_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: job_id")?;

    let job = state
        .store
        .get_job(job_id)
        .map_err(|e| format!("job lookup failed: {e}"))?;

    let progress: Option<Value> = job
        .progress_json
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok());

    Ok(json!({
        "job_id": job.job_id,
        "status": job.status.as_str(),
        "progress": progress,
        "created_at": job.created_at.to_rfc3339(),
        "started_at": job.started_at.map(|t| t.to_rfc3339()),
        "finished_at": job.finished_at.map(|t| t.to_rfc3339()),
        "warnings": job.warnings_json,
        "error": job.error_json,
    }))
}

fn handle_matches_list(args: &Value, state: &ServerState) -> Result<Value, String> {
    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(20) as u32;
    let offset = args.get("offset").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    let min_score = args.get("min_score").and_then(|v| v.as_f64());

    // If profile_id given, list matches for that profile.
    // Otherwise list for all active profiles.
    let profile_ids: Vec<String> =
        if let Some(pid) = args.get("profile_id").and_then(|v| v.as_str()) {
            vec![pid.to_string()]
        } else {
            state
                .store
                .list_active_profiles()
                .map_err(|e| format!("failed to list profiles: {e}"))?
                .into_iter()
                .map(|p| p.id)
                .collect()
        };

    let mut all_matches = Vec::new();
    for pid in &profile_ids {
        let matches = state
            .store
            .list_matches(pid, min_score, None, limit, offset)
            .map_err(|e| format!("failed to list matches: {e}"))?;
        for (score, item) in matches {
            all_matches.push(json!({
                "item_id": item.id,
                "canonical_id": item.canonical_id,
                "title": item.title,
                "authors": item.authors,
                "url": item.url,
                "source_type": item.source_type,
                "published_at": item.published_at.map(|t| t.to_rfc3339()),
                "score": score.score,
                "disposition": score.disposition.as_str(),
                "reason_short": score.reason_short,
                "profile_id": score.profile_id,
                "scored_at": score.created_at.to_rfc3339(),
            }));
        }
    }

    Ok(json!({
        "matches": all_matches,
        "count": all_matches.len(),
        "limit": limit,
        "offset": offset,
    }))
}

fn handle_match_get(args: &Value, state: &ServerState) -> Result<Value, String> {
    let item_id = args
        .get("item_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: item_id")?;

    let item = state
        .store
        .get_item(item_id)
        .map_err(|e| format!("item lookup failed: {e}"))?;

    // If profile_id is given, fetch scores for that profile
    let scores = if let Some(pid) = args.get("profile_id").and_then(|v| v.as_str()) {
        let matches = state
            .store
            .list_matches(pid, None, None, 100, 0)
            .map_err(|e| format!("failed to list scores: {e}"))?;
        matches
            .into_iter()
            .filter(|(_, i)| i.id == item_id)
            .map(|(s, _)| {
                json!({
                    "profile_id": s.profile_id,
                    "score": s.score,
                    "disposition": s.disposition.as_str(),
                    "reason_short": s.reason_short,
                    "rationale": s.rationale,
                    "job_id": s.job_id,
                    "stale": s.is_stale(),
                    "scored_at": s.created_at.to_rfc3339(),
                })
            })
            .collect::<Vec<_>>()
    } else {
        vec![]
    };

    Ok(json!({
        "item": {
            "id": item.id,
            "canonical_id": item.canonical_id,
            "title": item.title,
            "authors": item.authors,
            "abstract_text": item.abstract_text,
            "url": item.url,
            "source_type": item.source_type,
            "published_at": item.published_at.map(|t| t.to_rfc3339()),
            "created_at": item.created_at.to_rfc3339(),
        },
        "scores": scores,
    }))
}

fn handle_subscription_set(args: &Value, state: &ServerState) -> Result<Value, String> {
    let profile_id = args
        .get("profile_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: profile_id")?;
    let channel = args
        .get("channel")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: channel")?;
    let config = args
        .get("config")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: config")?;
    let enabled = args
        .get("enabled")
        .and_then(|v| v.as_bool())
        .ok_or("missing required field: enabled")?;

    // Validate channel config is valid JSON
    let _: Value =
        serde_json::from_str(config).map_err(|e| format!("config must be valid JSON: {e}"))?;

    let now = Utc::now();
    let sub = Subscription {
        id: Uuid::new_v4().to_string(),
        profile_id: profile_id.to_string(),
        channel: channel.to_string(),
        channel_config: config.to_string(),
        enabled,
        created_at: now,
        updated_at: now,
    };

    state
        .store
        .upsert_subscription(&sub)
        .map_err(|e| format!("failed to set subscription: {e}"))?;

    Ok(json!({
        "subscription_id": sub.id,
        "profile_id": sub.profile_id,
        "channel": sub.channel,
        "enabled": sub.enabled,
    }))
}

fn handle_source_health(args: &Value, _state: &ServerState) -> Result<Value, String> {
    let filter = args.get("source_type").and_then(|v| v.as_str());

    let all_sources = [
        "arxiv",
        "semantic_scholar",
        "huggingface_daily_papers",
        "rss",
    ];
    let sources: Vec<&str> = if let Some(f) = filter {
        if all_sources.contains(&f) {
            vec![f]
        } else {
            return Err(format!("unknown source type: {f}"));
        }
    } else {
        all_sources.to_vec()
    };

    // Return basic health info; in production this would query actual health state.
    let health: Vec<Value> = sources
        .iter()
        .map(|s| {
            json!({
                "source_type": s,
                "status": "ok",
                "last_success_at": null,
                "consecutive_failures": 0,
                "current_lag_secs": null,
                "backoff_until": null,
            })
        })
        .collect();

    Ok(json!({ "sources": health }))
}

// ─── Phase 2 tool handlers ──────────────────────────────────────────

async fn handle_corpus_search(args: &Value, state: &ServerState) -> Result<Value, String> {
    let query = args
        .get("query")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: query")?;
    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

    let results = state
        .vector_store
        .search(query, &state.embedder, limit)
        .await
        .map_err(|e| format!("vector search failed: {e}"))?;

    let items: Vec<Value> = results
        .into_iter()
        .map(|r| {
            json!({
                "canonical_id": r.canonical_id,
                "title": r.title,
                "abstract_text": r.abstract_text,
                "distance": r.distance,
            })
        })
        .collect();

    Ok(json!({
        "results": items,
        "count": items.len(),
        "query": query,
    }))
}

async fn handle_corpus_similar(args: &Value, state: &ServerState) -> Result<Value, String> {
    let item_id = args
        .get("item_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: item_id")?;
    let limit = args.get("limit").and_then(|v| v.as_u64()).unwrap_or(10) as usize;

    // item_id here is a canonical_id for the vector store
    let results = state
        .vector_store
        .find_similar(item_id, &state.embedder, limit)
        .await
        .map_err(|e| format!("similar search failed: {e}"))?;

    let items: Vec<Value> = results
        .into_iter()
        .map(|r| {
            json!({
                "canonical_id": r.canonical_id,
                "title": r.title,
                "abstract_text": r.abstract_text,
                "distance": r.distance,
            })
        })
        .collect();

    Ok(json!({
        "item_id": item_id,
        "similar": items,
        "count": items.len(),
    }))
}

async fn handle_corpus_concepts(args: &Value, state: &ServerState) -> Result<Value, String> {
    let limit = args
        .get("limit")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize);

    let mut concepts = state
        .vector_store
        .list_concepts()
        .await
        .map_err(|e| format!("concept listing failed: {e}"))?;

    if let Some(limit) = limit {
        concepts.truncate(limit);
    }

    let items: Vec<Value> = concepts
        .into_iter()
        .map(|c| {
            let item_ids: Vec<String> = serde_json::from_str(&c.item_ids_json).unwrap_or_default();
            json!({
                "concept_id": c.concept_id,
                "label": c.label,
                "item_count": item_ids.len(),
                "item_ids": item_ids,
            })
        })
        .collect();

    Ok(json!({
        "concepts": items,
        "count": items.len(),
    }))
}

// ─── Phase 3 tool handlers ──────────────────────────────────────────

fn handle_research_brief(args: &Value, state: &ServerState) -> Result<Value, String> {
    let profile_id = args
        .get("profile_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: profile_id")?;
    let days = args.get("days").and_then(|v| v.as_i64()).unwrap_or(7);

    let profile = state
        .store
        .get_profile(profile_id)
        .map_err(|e| format!("profile lookup failed: {e}"))?;

    let since = Utc::now() - Duration::days(days);
    let matches = state
        .store
        .list_matches(profile_id, None, Some(since), 100, 0)
        .map_err(|e| format!("failed to list matches: {e}"))?;

    let total = matches.len();
    let avg_score = if total > 0 {
        let sum: f64 = matches.iter().filter_map(|(s, _)| s.score).sum();
        let scored_count = matches.iter().filter(|(s, _)| s.score.is_some()).count();
        if scored_count > 0 {
            sum / scored_count as f64
        } else {
            0.0
        }
    } else {
        0.0
    };

    // Group by source type
    let mut by_source: HashMap<String, usize> = HashMap::new();
    for (_, item) in &matches {
        *by_source.entry(item.source_type.clone()).or_default() += 1;
    }

    // Top matches
    let mut scored: Vec<_> = matches.iter().filter(|(s, _)| s.score.is_some()).collect();
    scored.sort_by(|a, b| {
        b.0.score
            .unwrap_or(0.0)
            .partial_cmp(&a.0.score.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let top: Vec<Value> = scored
        .iter()
        .take(5)
        .map(|(s, i)| {
            json!({
                "title": i.title,
                "score": s.score,
                "reason": s.reason_short,
                "url": i.url,
            })
        })
        .collect();

    let unread = state
        .store
        .count_unread(profile_id)
        .map_err(|e| format!("count unread failed: {e}"))?;

    Ok(json!({
        "profile": profile.name,
        "period_days": days,
        "total_matches": total,
        "unread_count": unread,
        "average_score": (avg_score * 100.0).round() / 100.0,
        "by_source": by_source,
        "top_matches": top,
    }))
}

fn handle_relevance_explain(args: &Value, state: &ServerState) -> Result<Value, String> {
    let item_id = args
        .get("item_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: item_id")?;
    let project_context = args
        .get("project_context")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: project_context")?;

    let item = state
        .store
        .get_item(item_id)
        .map_err(|e| format!("item lookup failed: {e}"))?;

    // Return stored rationale from any existing scores as the explanation basis.
    // In production, this would invoke an LLM for a contextual explanation.
    Ok(json!({
        "item_id": item.id,
        "title": item.title,
        "project_context": project_context,
        "explanation": format!(
            "Item '{}' discusses {}. Relevance to your project context ('{}') \
             depends on overlap with the paper's core contributions in {}.",
            item.title,
            item.abstract_text.as_deref().unwrap_or("(no abstract)"),
            project_context,
            item.source_type,
        ),
    }))
}

fn handle_gap_analysis(args: &Value, state: &ServerState) -> Result<Value, String> {
    let profile_id = args
        .get("profile_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: profile_id")?;
    let project_goals = args
        .get("project_goals")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: project_goals")?;

    let profile = state
        .store
        .get_profile(profile_id)
        .map_err(|e| format!("profile lookup failed: {e}"))?;

    let since = Utc::now() - Duration::days(30);
    let matches = state
        .store
        .list_matches(profile_id, None, Some(since), 200, 0)
        .map_err(|e| format!("failed to list matches: {e}"))?;

    // Check which keywords appear in matched item titles/abstracts
    let mut keyword_hits: HashMap<String, usize> = HashMap::new();
    for kw in &profile.keywords {
        keyword_hits.insert(kw.clone(), 0);
    }
    for (_, item) in &matches {
        let text = format!(
            "{} {}",
            item.title.to_lowercase(),
            item.abstract_text.as_deref().unwrap_or("").to_lowercase()
        );
        for kw in &profile.keywords {
            if text.contains(&kw.to_lowercase()) {
                *keyword_hits.entry(kw.clone()).or_default() += 1;
            }
        }
    }

    let covered: Vec<_> = keyword_hits
        .iter()
        .filter(|(_, count)| **count > 0)
        .map(|(kw, count)| json!({ "keyword": kw, "match_count": count }))
        .collect();

    let gaps: Vec<_> = keyword_hits
        .iter()
        .filter(|(_, count)| **count == 0)
        .map(|(kw, _)| kw.clone())
        .collect();

    Ok(json!({
        "profile": profile.name,
        "project_goals": project_goals,
        "total_matches_30d": matches.len(),
        "keyword_coverage": covered,
        "gaps": gaps,
        "recommendation": if gaps.is_empty() {
            "All profile keywords are represented in recent matches."
        } else {
            "Some keywords have no matches. Consider broadening source types or adjusting keywords."
        },
    }))
}

fn handle_trend_detect(args: &Value, state: &ServerState) -> Result<Value, String> {
    let days = args.get("days").and_then(|v| v.as_i64()).unwrap_or(30);

    let profiles = if let Some(pid) = args.get("profile_id").and_then(|v| v.as_str()) {
        vec![
            state
                .store
                .get_profile(pid)
                .map_err(|e| format!("profile lookup failed: {e}"))?,
        ]
    } else {
        state
            .store
            .list_active_profiles()
            .map_err(|e| format!("failed to list profiles: {e}"))?
    };

    let since = Utc::now() - Duration::days(days);
    let mut weekly_counts: HashMap<String, usize> = HashMap::new();
    let mut score_trend: Vec<(String, f64)> = Vec::new();

    for profile in &profiles {
        let matches = state
            .store
            .list_matches(&profile.id, None, Some(since), 500, 0)
            .map_err(|e| format!("failed to list matches: {e}"))?;

        for (score_rec, _) in &matches {
            let week = score_rec.created_at.format("%Y-W%W").to_string();
            *weekly_counts.entry(week.clone()).or_default() += 1;
            if let Some(s) = score_rec.score {
                score_trend.push((week, s));
            }
        }
    }

    // Compute weekly average scores
    let mut weekly_scores: HashMap<String, (f64, usize)> = HashMap::new();
    for (week, score) in &score_trend {
        let entry = weekly_scores.entry(week.clone()).or_default();
        entry.0 += score;
        entry.1 += 1;
    }
    let weekly_avgs: HashMap<String, f64> = weekly_scores
        .into_iter()
        .map(|(week, (sum, count))| (week, (sum / count as f64 * 100.0).round() / 100.0))
        .collect();

    Ok(json!({
        "period_days": days,
        "weekly_match_counts": weekly_counts,
        "weekly_average_scores": weekly_avgs,
        "total_matches": weekly_counts.values().sum::<usize>(),
    }))
}

fn handle_cross_pollinate(args: &Value, state: &ServerState) -> Result<Value, String> {
    let profile_ids: Vec<String> = args
        .get("profile_ids")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .ok_or("missing required field: profile_ids (array of strings)")?;

    if profile_ids.len() < 2 {
        return Err("cross_pollinate requires at least 2 profile IDs".into());
    }

    let since = Utc::now() - Duration::days(30);

    // Collect item IDs matched per profile
    let mut profile_items: HashMap<String, HashMap<String, (f64, Value)>> = HashMap::new();
    for pid in &profile_ids {
        let profile = state
            .store
            .get_profile(pid)
            .map_err(|e| format!("profile lookup failed: {e}"))?;
        let matches = state
            .store
            .list_matches(pid, None, Some(since), 200, 0)
            .map_err(|e| format!("failed to list matches: {e}"))?;

        let mut items_map = HashMap::new();
        for (score_rec, item) in matches {
            items_map.insert(
                item.id.clone(),
                (
                    score_rec.score.unwrap_or(0.0),
                    json!({
                        "item_id": item.id,
                        "title": item.title,
                        "url": item.url,
                    }),
                ),
            );
        }
        profile_items.insert(profile.name.clone(), items_map);
    }

    // Find items appearing in 2+ profiles
    let mut item_profiles: HashMap<String, Vec<(String, f64, Value)>> = HashMap::new();
    for (profile_name, items) in &profile_items {
        for (item_id, (score, info)) in items {
            item_profiles.entry(item_id.clone()).or_default().push((
                profile_name.clone(),
                *score,
                info.clone(),
            ));
        }
    }

    let cross_relevant: Vec<Value> = item_profiles
        .into_iter()
        .filter(|(_, profiles)| profiles.len() >= 2)
        .map(|(_, profiles)| {
            let info = &profiles[0].2;
            let profile_scores: Vec<Value> = profiles
                .iter()
                .map(|(name, score, _)| json!({ "profile": name, "score": score }))
                .collect();
            json!({
                "item": info,
                "relevant_to": profile_scores,
            })
        })
        .collect();

    Ok(json!({
        "cross_relevant_items": cross_relevant,
        "count": cross_relevant.len(),
    }))
}

async fn handle_citation_graph(args: &Value) -> Result<Value, String> {
    let item_id = args
        .get("item_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: item_id")?;
    let depth = args
        .get("depth")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
        .min(2) as usize;

    // Query Semantic Scholar API
    let url = format!(
        "https://api.semanticscholar.org/graph/v1/paper/{}?fields=title,citations.title,citations.paperId,references.title,references.paperId",
        item_id,
    );

    let client = reqwest::Client::new();
    let resp = client
        .get(&url)
        .header("User-Agent", "research-radar/0.1")
        .send()
        .await
        .map_err(|e| format!("Semantic Scholar request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("Semantic Scholar API returned {status}: {body}"));
    }

    let paper: Value = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse Semantic Scholar response: {e}"))?;

    let title = paper
        .get("title")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let citations: Vec<Value> = paper
        .get("citations")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|c| {
                    Some(json!({
                        "paperId": c.get("paperId")?.as_str()?,
                        "title": c.get("title")?.as_str()?,
                    }))
                })
                .collect()
        })
        .unwrap_or_default();

    let references: Vec<Value> = paper
        .get("references")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|r| {
                    Some(json!({
                        "paperId": r.get("paperId")?.as_str()?,
                        "title": r.get("title")?.as_str()?,
                    }))
                })
                .collect()
        })
        .unwrap_or_default();

    // For depth=2, we would recursively fetch citations of citations.
    // For now, we return the first level and note the depth capability.
    Ok(json!({
        "paper_id": item_id,
        "title": title,
        "depth": depth,
        "citations": citations,
        "citation_count": citations.len(),
        "references": references,
        "reference_count": references.len(),
        "note": if depth > 1 {
            "Depth >1 returns first-level results; recursive traversal is planned."
        } else {
            ""
        },
    }))
}

fn handle_digest_compose(args: &Value, state: &ServerState) -> Result<Value, String> {
    let profile_id = args
        .get("profile_id")
        .and_then(|v| v.as_str())
        .ok_or("missing required field: profile_id")?;
    let format = args
        .get("format")
        .and_then(|v| v.as_str())
        .unwrap_or("markdown");
    let days = args.get("days").and_then(|v| v.as_i64()).unwrap_or(7);

    let profile = state
        .store
        .get_profile(profile_id)
        .map_err(|e| format!("profile lookup failed: {e}"))?;

    let since = Utc::now() - Duration::days(days);
    let matches = state
        .store
        .list_matches(profile_id, None, Some(since), 50, 0)
        .map_err(|e| format!("failed to list matches: {e}"))?;

    // Sort by score descending
    let mut sorted: Vec<_> = matches.into_iter().collect();
    sorted.sort_by(|a, b| {
        b.0.score
            .unwrap_or(0.0)
            .partial_cmp(&a.0.score.unwrap_or(0.0))
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let digest = match format {
        "plain" => {
            let mut out = format!(
                "Research Digest: {} ({} days)\n{}\n\n",
                profile.name,
                days,
                "=".repeat(40),
            );
            if sorted.is_empty() {
                out.push_str("No matches in this period.\n");
            }
            for (i, (score, item)) in sorted.iter().enumerate() {
                out.push_str(&format!(
                    "{}. {} (score: {:.2})\n   {}\n   {}\n\n",
                    i + 1,
                    item.title,
                    score.score.unwrap_or(0.0),
                    score.reason_short.as_deref().unwrap_or(""),
                    item.url,
                ));
            }
            out
        }
        "html" => {
            let mut out = format!(
                "<h1>Research Digest: {}</h1>\n<p>Period: {} days</p>\n<ol>\n",
                profile.name, days,
            );
            for (score, item) in &sorted {
                out.push_str(&format!(
                    "<li><strong><a href=\"{}\">{}</a></strong> (score: {:.2})<br/>{}</li>\n",
                    item.url,
                    item.title,
                    score.score.unwrap_or(0.0),
                    score.reason_short.as_deref().unwrap_or(""),
                ));
            }
            out.push_str("</ol>\n");
            out
        }
        _ => {
            // markdown (default)
            let mut out = format!(
                "# Research Digest: {}\n\n*Period: {} days*\n\n",
                profile.name, days
            );
            if sorted.is_empty() {
                out.push_str("No matches in this period.\n");
            }
            for (i, (score, item)) in sorted.iter().enumerate() {
                out.push_str(&format!(
                    "{}. **[{}]({})** (score: {:.2})\\\n   {}\n\n",
                    i + 1,
                    item.title,
                    item.url,
                    score.score.unwrap_or(0.0),
                    score.reason_short.as_deref().unwrap_or(""),
                ));
            }
            out
        }
    };

    Ok(json!({
        "profile": profile.name,
        "format": format,
        "period_days": days,
        "item_count": sorted.len(),
        "digest": digest,
    }))
}

// ─── Main ────────────────────────────────────────────────────────────

fn parse_args() -> (PathBuf, PathBuf) {
    let args: Vec<String> = std::env::args().collect();
    let mut db_path = PathBuf::from(
        std::env::var("RADAR_DB_PATH").unwrap_or_else(|_| dirs_or_default("radar.db")),
    );
    let mut lance_path = PathBuf::from(
        std::env::var("RADAR_LANCE_PATH").unwrap_or_else(|_| dirs_or_default("lance")),
    );

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--db-path" if i + 1 < args.len() => {
                db_path = PathBuf::from(&args[i + 1]);
                i += 2;
            }
            "--lance-path" if i + 1 < args.len() => {
                lance_path = PathBuf::from(&args[i + 1]);
                i += 2;
            }
            _ => {
                i += 1;
            }
        }
    }

    (db_path, lance_path)
}

fn dirs_or_default(name: &str) -> String {
    if let Some(home) = std::env::var_os("HOME") {
        let dir = PathBuf::from(home).join(".radar");
        format!("{}/{name}", dir.display())
    } else {
        format!(".radar/{name}")
    }
}

#[tokio::main]
#[allow(clippy::arc_with_non_send_sync)]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("radar_mcp=info".parse().unwrap()),
        )
        .with_writer(io::stderr)
        .init();

    let (db_path, lance_path) = parse_args();

    // Ensure parent directories exist
    if let Some(parent) = db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Some(parent) = lance_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    tracing::info!(
        db = %db_path.display(),
        lance = %lance_path.display(),
        "starting radar-mcp server",
    );

    let store = Store::open(&db_path).unwrap_or_else(|e| {
        eprintln!("fatal: failed to open store at {}: {e}", db_path.display());
        std::process::exit(1);
    });

    let vector_store = VectorStore::open(&lance_path, 1536)
        .await
        .unwrap_or_else(|e| {
            eprintln!(
                "fatal: failed to open vector store at {}: {e}",
                lance_path.display()
            );
            std::process::exit(1);
        });

    let embedder = MockEmbeddingBackend::new(1536);

    let state = Arc::new(ServerState {
        store,
        vector_store,
        embedder,
    });

    let stdin = io::stdin();
    let stdout = io::stdout();

    for line in stdin.lock().lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                tracing::error!("stdin read error: {e}");
                break;
            }
        };

        let line = line.trim().to_string();
        if line.is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(e) => {
                let resp = JsonRpcResponse::err(Value::Null, -32700, format!("parse error: {e}"));
                let out = serde_json::to_string(&resp).unwrap();
                let mut stdout = stdout.lock();
                let _ = writeln!(stdout, "{out}");
                let _ = stdout.flush();
                continue;
            }
        };

        let id = request.id.clone().unwrap_or(Value::Null);

        let response = match request.method.as_str() {
            "initialize" => handle_initialize(id),
            "notifications/initialized" => continue, // no response needed
            "tools/list" => handle_tools_list(id),
            "tools/call" => handle_tools_call(id, &request.params, &state).await,
            _ => JsonRpcResponse::err(id, -32601, format!("method not found: {}", request.method)),
        };

        let out = serde_json::to_string(&response).unwrap();
        let mut stdout = stdout.lock();
        let _ = writeln!(stdout, "{out}");
        let _ = stdout.flush();
    }

    tracing::info!("radar-mcp server shutting down");
}
