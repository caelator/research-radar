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
    AnthropicBackend, DbPool, PipelineExecutor, Profile, RadarQuery, RadarResult, Source,
    SourceType,
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
    }
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
