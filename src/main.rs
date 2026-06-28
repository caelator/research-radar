//! research-radar — CLI for tracking and searching research sources.
//!
//! ## Usage
//!
//! ```sh
//! research-radar add <url>          # Add a source URL to the radar
//! research-radar search <query>      # Search entries by keyword
//! research-radar list               # List all sources
//! research-radar profile create     # Create a monitoring profile
//! research-radar profile list       # List all profiles
//! research-radar mcp                # Start the MCP JSON-RPC server
//! ```

use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use research_radar_core::{
    AnthropicBackend, DbPool, PipelineExecutor, Profile, RadarQuery, RadarResult, RadarStore,
    Source, SourceType,
};
use std::sync::Arc;

mod mcp_server;

#[derive(Parser)]
#[command(
    name = "research-radar",
    about = "research.radar — track and search research sources"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Add a new source URL to the radar.
    Add {
        /// The URL of the source.
        url: String,

        /// Title for the source (optional; derived from URL if omitted).
        #[arg(short, long)]
        title: Option<String>,

        /// Source type: paper, article, web, book (default: web).
        #[arg(short, long, default_value = "web")]
        source_type: String,
    },

    /// Search entries by keyword query.
    Search {
        /// The search query string.
        query: String,

        /// Maximum number of results to return (default: 10).
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },

    /// List all stored sources.
    List {
        /// Maximum number of sources to show (default: 50).
        #[arg(short, long, default_value = "50")]
        limit: usize,
    },

    /// Show the database path being used.
    DbPath,

    /// Process one pending scan job synchronously.
    ScanOnce,

    /// Run the scan worker loop — processes all pending jobs, then exits.
    ScanWorker {
        /// Process all active profiles (enqueue + run).
        #[arg(long)]
        all_active_profiles: bool,

        /// Keep running and poll for new jobs every N seconds.
        #[arg(long)]
        poll_interval: Option<u64>,
    },

    /// Manage monitoring profiles.
    #[command(subcommand)]
    Profile(ProfileCommands),

    /// Start the MCP JSON-RPC server (stdio).
    Mcp,

    /// Auto-create or update a monitoring profile from a project directory.
    ///
    /// Scans the project's manifest files (Cargo.toml, pyproject.toml,
    /// package.json, go.mod) and source code to derive keywords, then creates
    /// a research-radar profile.
    AutoProfile {
        /// Path to the project directory (default: current directory).
        #[arg(short, long, default_value = ".")]
        dir: String,

        /// Override the profile name (default: derived from directory name).
        #[arg(short, long)]
        name: Option<String>,

        /// Print the derived profile as JSON without creating it (dry-run).
        #[arg(long)]
        dry_run: bool,
    },

    /// Export actionable findings to a JSON file (for self-harness integration).
    ExportFindings {
        /// Output file path (default: stdout).
        #[arg(short, long)]
        out: Option<String>,

        /// Maximum number of findings to export (default: 20).
        #[arg(short, long, default_value = "20")]
        limit: usize,

        /// Minimum confidence threshold (default: 0.0 = no filter).
        #[arg(long, default_value = "0.0")]
        min_confidence: f64,
    },
}

#[derive(Parser)]
enum ProfileCommands {
    /// Create a new monitoring profile.
    Create {
        /// Profile name.
        #[arg(long)]
        name: String,

        /// Comma-separated keywords to monitor.
        #[arg(long)]
        keywords: String,

        /// Comma-separated negative keywords (exclusions).
        #[arg(long)]
        negative_keywords: Option<String>,

        /// Minimum relevance score threshold (0.0–1.0, default: 0.5).
        #[arg(long)]
        score_threshold: Option<f64>,

        /// Maximum LLM calls per scan (default: 10).
        #[arg(long)]
        max_llm_calls: Option<u32>,
    },

    /// List all profiles.
    List,

    /// Delete a profile by ID.
    Delete {
        /// Profile ID to delete.
        #[arg(long)]
        id: String,
    },

    /// Get a profile by ID (JSON output).
    Get {
        /// Profile ID to retrieve.
        #[arg(long)]
        id: String,
    },
}

fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_writer(std::io::stderr),
        )
        .init();

    let cli = Cli::parse();

    // Open the database (creates ~/.research-radar/ if needed).
    let pool = match DbPool::init() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: failed to open database: {e}");
            std::process::exit(1);
        }
    };

    match cli.command {
        Commands::Add {
            url,
            title,
            source_type,
        } => {
            let src = Source::new(
                url.clone(),
                title.unwrap_or_else(|| url.clone()),
                SourceType::from_str(&source_type),
            );
            match pool.insert_source(&src) {
                Ok(id) => {
                    println!("added source {id}");
                    println!("  url   : {}", src.url);
                    println!("  title : {}", src.title);
                    println!("  type  : {}", src.source_type.as_str());
                }
                Err(e) => {
                    eprintln!("error: failed to insert source: {e}");
                    std::process::exit(1);
                }
            }
        }

        Commands::Search { query, limit } => {
            let q = RadarQuery::new(query.clone());
            let query_id = match pool.log_query(&q) {
                Ok(id) => id,
                Err(e) => {
                    eprintln!("warning: failed to log query: {e}");
                    String::new()
                }
            };

            let entries = match pool.search_entries(&query, limit) {
                Ok(results) => results,
                Err(e) => {
                    eprintln!("error: search failed: {e}");
                    std::process::exit(1);
                }
            };

            // Record each result so we can later analyze retrieval quality.
            if !query_id.is_empty() {
                for entry in &entries {
                    let result =
                        RadarResult::new(query_id.clone(), entry.id.clone(), entry.relevance_score);
                    if let Err(e) = pool.insert_result(&result) {
                        eprintln!(
                            "warning: failed to record result for entry {}: {e}",
                            entry.id
                        );
                    }
                }
            }

            if entries.is_empty() {
                println!("no entries found for \"{query}\"");
            } else {
                println!("found {} entry(ies):\n", entries.len());
                for entry in &entries {
                    let summary = entry.summary.as_deref().unwrap_or("(no summary)");
                    println!("  [{:.2}] {}", entry.relevance_score, entry.id);
                    println!("  content : {}", truncate(&entry.content, 120));
                    println!("  summary : {}", truncate(summary, 120));
                    if !entry.tags.is_empty() {
                        println!("  tags    : {}", entry.tags.join(", "));
                    }
                    println!();
                }
            }
        }

        Commands::List { limit } => {
            let total = pool.count_sources().unwrap_or(0);
            let sources = pool.list_sources(limit).unwrap_or_default();

            if sources.is_empty() {
                println!("no sources stored yet — add one with `research-radar add <url>`");
                return;
            }

            println!("{} source(s) stored (showing up to {limit}):\n", total);
            for src in &sources {
                println!("  [{}] {}", src.source_type.as_str(), src.id);
                println!("  title  : {}", truncate(&src.title, 80));
                println!("  url    : {}", truncate(&src.url, 80));
                println!("  added  : {}", src.added_at.format("%Y-%m-%d"));
                println!();
            }
        }

        Commands::DbPath => {
            let home = dirs::home_dir().unwrap_or_default();
            println!("{}/.research-radar/data.db", home.display());
        }

        Commands::ScanOnce => {
            let executor = build_executor();
            match executor.run_next(&pool) {
                Ok(Some(run)) => print_run(&run),
                Ok(None) => println!("no pending scan jobs"),
                Err(e) => {
                    eprintln!("error: scan-once failed: {e}");
                    std::process::exit(1);
                }
            }
        }

        Commands::ScanWorker {
            all_active_profiles,
            poll_interval,
        } => {
            let executor = build_executor();

            loop {
                // If --all-active-profiles, enqueue jobs for all profiles
                if all_active_profiles {
                    match pool.list_profiles() {
                        Ok(profiles) => {
                            // Seed default profile on first run when no profiles exist.
                            if profiles.is_empty() {
                                let default_keywords = vec![
                                    "AI".into(),
                                    "machine learning".into(),
                                    "software engineering".into(),
                                    "programming languages".into(),
                                    "compilers".into(),
                                    "systems programming".into(),
                                    "Rust".into(),
                                    "formal methods".into(),
                                ];
                                let mut default_profile =
                                    Profile::new("default".into(), default_keywords);
                                default_profile.score_threshold = 0.4;
                                default_profile.max_llm_calls = 10;
                                if let Err(e) = pool.insert_profile(&default_profile) {
                                    eprintln!("error: failed to create default profile: {e}");
                                    std::process::exit(1);
                                }
                                println!(
                                    "no profiles found — created default profile with id {}",
                                    default_profile.id
                                );
                                if let Err(e) = pool
                                    .enqueue_job(&default_profile.id, Some("scan-worker".into()))
                                {
                                    eprintln!(
                                        "warning: failed to enqueue for default profile: {e}"
                                    );
                                }
                            } else {
                                for profile in &profiles {
                                    if let Err(e) =
                                        pool.enqueue_job(&profile.id, Some("scan-worker".into()))
                                    {
                                        eprintln!(
                                            "warning: failed to enqueue for {}: {e}",
                                            profile.name
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("error: failed to list profiles: {e}");
                            std::process::exit(1);
                        }
                    }
                }

                // Process all pending jobs — continue on individual failures
                let mut total_runs = 0;
                let mut failures = 0;
                loop {
                    match executor.run_next(&pool) {
                        Ok(Some(run)) => {
                            print_run(&run);
                            total_runs += 1;
                        }
                        Ok(None) => break,
                        Err(e) => {
                            eprintln!("warning: scan job failed: {e}");
                            failures += 1;
                            // Continue to next job instead of stopping
                            if failures >= 5 {
                                eprintln!("error: too many consecutive failures, stopping");
                                break;
                            }
                        }
                    }
                }

                if total_runs == 0 {
                    println!("no pending scan jobs");
                } else {
                    println!("processed {total_runs} scan job(s)");
                }

                match poll_interval {
                    Some(secs) => {
                        println!("sleeping {secs}s before next poll...");
                        std::thread::sleep(std::time::Duration::from_secs(secs));
                    }
                    None => break,
                }
            }
        }

        Commands::Profile(cmd) => match cmd {
            ProfileCommands::Create {
                name,
                keywords,
                negative_keywords,
                score_threshold,
                max_llm_calls,
            } => {
                let keywords: Vec<String> =
                    keywords.split(',').map(|s| s.trim().to_string()).collect();
                let negative_keywords: Vec<String> = negative_keywords
                    .map(|nk| nk.split(',').map(|s| s.trim().to_string()).collect())
                    .unwrap_or_default();
                let mut profile = Profile::new(name, keywords);
                profile.negative_keywords = negative_keywords;
                if let Some(t) = score_threshold {
                    profile.score_threshold = t;
                }
                if let Some(m) = max_llm_calls {
                    profile.max_llm_calls = m;
                }
                match pool.insert_profile(&profile) {
                    Ok(id) => {
                        println!("created profile {id}");
                        println!("  name              : {}", profile.name);
                        println!("  keywords          : {}", profile.keywords.join(", "));
                        println!(
                            "  negative_keywords : {}",
                            profile.negative_keywords.join(", ")
                        );
                        println!("  score_threshold   : {}", profile.score_threshold);
                        println!("  max_llm_calls     : {}", profile.max_llm_calls);
                    }
                    Err(e) => {
                        eprintln!("error: failed to create profile: {e}");
                        std::process::exit(1);
                    }
                }
            }

            ProfileCommands::List => match pool.list_profiles() {
                Ok(profiles) => {
                    if profiles.is_empty() {
                        println!("no profiles — create one with `research-radar profile create`");
                        return;
                    }
                    println!("{} profile(s):\n", profiles.len());
                    for p in &profiles {
                        println!("  [{}] {}", p.id, p.name);
                        println!("  keywords          : {}", p.keywords.join(", "));
                        println!("  negative_keywords : {}", p.negative_keywords.join(", "));
                        println!("  score_threshold   : {}", p.score_threshold);
                        println!("  max_llm_calls     : {}", p.max_llm_calls);
                        println!("  created_at        : {}", p.created_at.format("%Y-%m-%d"));
                        println!();
                    }
                }
                Err(e) => {
                    eprintln!("error: failed to list profiles: {e}");
                    std::process::exit(1);
                }
            },

            ProfileCommands::Delete { id } => match pool.get_profile(&id) {
                Ok(None) => {
                    eprintln!("error: profile not found: {id}");
                    std::process::exit(1);
                }
                Ok(Some(profile)) => {
                    if let Err(e) = pool.delete_profile(&id) {
                        eprintln!("error: failed to delete profile {}: {e}", profile.name);
                        std::process::exit(1);
                    }
                    println!("deleted profile '{}' ({id})", profile.name);
                }
                Err(e) => {
                    eprintln!("error: failed to look up profile: {e}");
                    std::process::exit(1);
                }
            },

            ProfileCommands::Get { id } => match pool.get_profile(&id) {
                Ok(None) => {
                    eprintln!("error: profile not found: {id}");
                    std::process::exit(1);
                }
                Ok(Some(profile)) => {
                    let json = serde_json::to_string_pretty(&profile).unwrap_or_else(|_| {
                        eprintln!("error: failed to serialize profile to JSON");
                        std::process::exit(1);
                    });
                    println!("{json}");
                }
                Err(e) => {
                    eprintln!("error: failed to look up profile: {e}");
                    std::process::exit(1);
                }
            },
        },

        Commands::Mcp => {
            if let Err(e) = mcp_server::run_mcp_server(&pool) {
                eprintln!("MCP server error: {e}");
                std::process::exit(1);
            }
        }

        Commands::AutoProfile {
            dir,
            name,
            dry_run,
        } => {
            match auto_profile(&pool, &dir, name, dry_run) {
                Ok(profile_id) => {
                    if !dry_run {
                        println!("created profile {profile_id}");
                    }
                }
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }

        Commands::ExportFindings {
            out,
            limit,
            min_confidence,
        } => {
            let findings_json = match export_findings(limit, min_confidence) {
                Ok(json) => json,
                Err(e) => {
                    eprintln!("error: failed to export findings: {e}");
                    std::process::exit(1);
                }
            };
            match &out {
                Some(path) => match std::fs::write(path, &findings_json) {
                    Ok(()) => eprintln!("exported findings to {path}"),
                    Err(e) => {
                        eprintln!("error: failed to write output file: {e}");
                        std::process::exit(1);
                    }
                },
                None => println!("{findings_json}"),
            }
        }
    }
}

/// Extract research-relevant keywords from a project directory.
///
/// Parses manifest files (Cargo.toml, pyproject.toml, package.json, go.mod)
/// and scans .rs/.py/.ts/.go/.js source files for domain-significant terms.
fn extract_project_keywords(dir: &str) -> Result<(Vec<String>, String), String> {
    let dir = std::path::Path::new(dir);
    if !dir.is_dir() {
        return Err(format!("'{}' is not a directory", dir.display()));
    }

    let mut keywords = std::collections::BTreeSet::new();
    let dir_canonical = std::fs::canonicalize(dir).unwrap_or_else(|_| std::path::PathBuf::from(dir));
    let project_name = dir_canonical
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("project")
        .to_string();

    // ── Cargo.toml (section-aware, direct deps only) ──
    let cargo = dir.join("Cargo.toml");
    if cargo.exists() {
        let text = std::fs::read_to_string(&cargo).unwrap_or_default();
        let mut in_deps = false;
        for line in text.lines() {
            let trimmed = line.trim();
            // Track section headers
            if trimmed.starts_with('[') {
                in_deps = trimmed.contains("dependencies");
                continue;
            }
            if !in_deps {
                continue;
            }
            // Dependency lines: "lancedb = ..." or "tokio = { workspace = true }"
            if let Some(eq_pos) = trimmed.find('=') {
                let name = trimmed[..eq_pos].trim();
                if !name.is_empty() && !name.starts_with('[') && !name.starts_with('#') {
                    keywords.insert(name.to_string());
                }
            }
        }
    }

    // ── pyproject.toml ──
    let pyproject = dir.join("pyproject.toml");
    if pyproject.exists() {
        let text = std::fs::read_to_string(&pyproject).map_err(|e| e.to_string())?;
        for line in text.lines() {
            let trimmed = line.trim();
            // dependencies = ["foo>=1.0", "bar"]
            if trimmed.starts_with('"') && trimmed.contains(">=") || trimmed.contains("==") || trimmed.contains("~=") {
                let name = trimmed.trim_start_matches('"').split(|c: char| c == '>' || c == '=' || c == '~' || c == '<').next().unwrap_or("").trim();
                if !name.is_empty() {
                    keywords.insert(name.to_string());
                }
            }
        }
    }

    // ── package.json ──
    let pkg = dir.join("package.json");
    if pkg.exists() {
        let text = std::fs::read_to_string(&pkg).map_err(|e| e.to_string())?;
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) {
            if let Some(deps) = json.get("dependencies").and_then(|d| d.as_object()) {
                for key in deps.keys() {
                    keywords.insert(key.clone());
                }
            }
            if let Some(deps) = json.get("devDependencies").and_then(|d| d.as_object()) {
                for key in deps.keys() {
                    keywords.insert(key.clone());
                }
            }
        }
    }

    // ── go.mod ──
    let gomod = dir.join("go.mod");
    if gomod.exists() {
        let text = std::fs::read_to_string(&gomod).map_err(|e| e.to_string())?;
        for line in text.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("require ") || trimmed.contains('\t') {
                for part in trimmed.split_whitespace() {
                    // Extract the module path last segment
                    if part.contains('/') && !part.starts_with("require") {
                        if let Some(last) = part.rsplit('/').next() {
                            if !last.is_empty() && !last.chars().next().unwrap().is_ascii_digit() {
                                keywords.insert(last.to_string());
                            }
                        }
                    }
                }
            }
        }
    }

    // ── README/AGENTS headings — extract domain terms from headings only ──
    for fname in &["README.md", "AGENTS.md", "CLAUDE.md"] {
        let path = dir.join(fname);
        if path.exists() {
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            for line in text.lines() {
                let trimmed = line.trim();
                // Only extract from markdown headings (# or ##) — these are domain-significant
                if !trimmed.starts_with('#') {
                    continue;
                }
                let lower = trimmed.trim_start_matches('#').trim().to_lowercase();
                for word in lower.split_whitespace() {
                    let clean: String = word.chars().filter(|c| c.is_alphanumeric() || *c == '-' || *c == '_').collect();
                    if clean.len() >= 4 && !is_common_word(&clean) {
                        keywords.insert(clean);
                    }
                }
            }
        }
    }

    // Filter out generic build/tooling noise that isn't useful for research monitoring
    let noise = [
        "serde", "serde_json", "anyhow", "thiserror", "tracing", "tracing-subscriber",
        "libc", "cfg-if", "once_cell", "lazy_static", "regex", "base64", "hex",
        "url", "percent-encoding", "mime", "log", "env_logger",
        "pytest", "setuptools", "wheel", "pip", "flake8", "mypy", "black",
        "eslint", "prettier", "typescript", "jest", "webpack", "vite", "babel",
        "build", "hatchling", "cryptography",
    ];
    let mut filtered: Vec<String> = keywords
        .into_iter()
        .filter(|k| !noise.contains(&k.as_str()) && k.len() >= 3)
        .collect();

    // Sort by length descending (more specific keywords first) then alphabetically
    filtered.sort_by(|a, b| b.len().cmp(&a.len()).then(a.cmp(b)));
    // Cap at 25 keywords
    filtered.truncate(25);

    if filtered.is_empty() {
        return Err("could not extract any keywords from this project".into());
    }

    Ok((filtered, project_name))
}

fn is_common_word(word: &str) -> bool {
    const COMMON: &[&str] = &[
        "this", "that", "with", "from", "have", "been", "will", "would", "could",
        "should", "there", "their", "about", "which", "when", "what", "they",
        "them", "then", "than", "these", "those", "some", "such", "only", "also",
        "into", "over", "under", "more", "most", "very", "just", "like", "make",
        "made", "your", "here", "must", "does", "done", "each", "both", "first",
        "last", "next", "project", "code", "file", "data", "test", "tool", "tools",
        "using", "used", "uses", "readme", "license", "install", "usage", "docs",
        "repository", "following", "example", "examples", "default", "config",
        "configuration", "build", "script", "scripts", "output", "input", "error",
        "warning", "status", "model", "based", "library", "package", "version",
        "description", "setup", "start", "stop", "run", "running", "source",
    ];
    COMMON.contains(&word)
}

/// Create or update a research-radar profile from a project directory.
fn auto_profile(
    pool: &DbPool,
    dir: &str,
    name_override: Option<String>,
    dry_run: bool,
) -> Result<String, String> {
    let (keywords, project_name) = extract_project_keywords(dir)?;
    let profile_name = name_override.unwrap_or(project_name);

    let mut profile = Profile::new(profile_name, keywords.clone());
    profile.score_threshold = 0.3; // Lower threshold — we want broad coverage for auto-profiles

    if dry_run {
        let json = serde_json::to_string_pretty(&serde_json::json!({
            "name": profile.name,
            "keywords": profile.keywords,
            "score_threshold": profile.score_threshold,
            "keyword_count": profile.keywords.len(),
        })).map_err(|e| e.to_string())?;
        println!("{json}");
        return Ok(String::new());
    }

    pool.insert_profile(&profile).map_err(|e| e.to_string())?;
    Ok(profile.id)
}

/// Export actionable findings as a JSON array for external consumers
/// (e.g. self-harness memory sources).
fn export_findings(

    limit: usize,
    min_confidence: f64,
) -> Result<String, Box<dyn std::error::Error>> {
    let store = tokio_block_on(RadarStore::init())?;
    let findings = tokio_block_on(store.list_actionable_findings(limit))?;

    let filtered: Vec<_> = findings
        .into_iter()
        .filter(|f| f.confidence as f64 >= min_confidence)
        .map(|f| {
            serde_json::json!({
                "id": f.id,
                "title": f.title,
                "summary": f.summary,
                "source_url": f.source_url,
                "source_title": f.source_title,
                "source_type": f.source_type.as_str(),
                "domain": f.domain,
                "confidence": f.confidence,
                "urgency": f.urgency.as_str(),
                "suggested_action": f.suggested_action,
                "novelty_score": f.novelty_score,
                "applicability_hypothesis": f.applicability_hypothesis,
                "discovered_at": f.discovered_at.to_rfc3339(),
            })
        })
        .collect();

    Ok(serde_json::to_string_pretty(&serde_json::json!({
        "source": "research-radar",
        "exported_at": chrono::Utc::now().to_rfc3339(),
        "count": filtered.len(),
        "findings": filtered,
    }))?)
}

fn tokio_block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
        .block_on(fut)
}

fn build_executor() -> PipelineExecutor {
    let discord_webhook_url = std::env::var("DISCORD_WEBHOOK_URL")
        .ok()
        .filter(|u| !u.is_empty());
    let executor = match std::env::var("ANTHROPIC_API_KEY") {
        Ok(key) if !key.is_empty() => {
            PipelineExecutor::with_scorer(Arc::new(AnthropicBackend::new(key)))
        }
        _ => PipelineExecutor::new(), // uses MockBackend (keyword scoring)
    };
    executor.with_discord_webhook_url(discord_webhook_url)
}

fn print_run(run: &research_radar_core::PipelineRun) {
    println!(
        "processed job {} for profile {}",
        run.job_id, run.profile_id
    );
    println!("  arxiv      : {} new papers", run.arxiv_fetched);
    println!("  s2         : {} new papers", run.s2_fetched);
    println!("  openalex   : {} new works", run.oa_fetched);
    println!("  candidates : {}", run.candidates);
    println!("  deduped    : {}", run.deduped);
    println!("  scored     : {}", run.scored);
    println!("  accepted   : {}", run.accepted);
    println!("  notified   : {}", run.notified);
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let cut = s[..max.saturating_sub(3)]
            .rfind(' ')
            .unwrap_or(max.saturating_sub(3));
        format!("{}...", &s[..cut])
    }
}
