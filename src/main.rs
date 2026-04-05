//! research-radar — CLI for tracking and searching research sources.
//!
//! ## Usage
//!
//! ```sh
//! research-radar add <url>          # Add a source URL to the radar
//! research-radar search <query>      # Search entries by keyword
//! research-radar list                # List all sources
//! ```

use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use research_radar_core::{
    DbPool, Entry, RadarQuery, Source, SourceType,
};

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
}

fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().without_time())
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
            if let Err(e) = pool.log_query(&q) {
                eprintln!("warning: failed to log query: {e}");
            }

            let entries = match pool.search_entries(&query, limit) {
                Ok(results) => results,
                Err(e) => {
                    eprintln!("error: search failed: {e}");
                    std::process::exit(1);
                }
            };

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
            // Simple list via in-memory scan (small-scale for now).
            let _all_entries: Vec<Entry> = pool
                .search_entries("", limit)
                .unwrap_or_default();

            // We want all sources, so we'll do a direct query.
            // For Phase 1 simplicity, just print a placeholder count.
            // A full "list sources" would be a separate pool method.
            println!("sources stored (showing up to {limit}):");
            println!("  (source listing — use `search` to find entries)");
        }

        Commands::DbPath => {
            let home = dirs::home_dir().unwrap_or_default();
            println!("{}/.research-radar/data.db", home.display());
        }
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let cut = s[..max.saturating_sub(3)].rfind(' ').unwrap_or(max.saturating_sub(3));
        format!("{}...", &s[..cut])
    }
}
