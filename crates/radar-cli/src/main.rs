use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use radar_core::db::Store;
use radar_core::embedding::{EmbeddingBackend, MockEmbeddingBackend};
use radar_core::executor::{ExecutorConfig, run_executor_loop, run_executor_once};
use radar_core::notify::DiscordNotifier;
use radar_core::scorer::{LlmScorer, MockLlmBackend};
use radar_core::source::{StaticSourceAdapter, arxiv::ArxivAdapter};
use radar_core::types::{Profile, Subscription};
use radar_core::vector::VectorStore;
use tokio::sync::watch;

#[derive(Parser)]
#[command(name = "radar", about = "research.radar — AI research monitoring")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the Phase 1 demo flow once: create or load a profile, enqueue a job,
    /// execute exactly one job with mock backends, then exit.
    ScanOnce {
        /// Existing profile to scan. If omitted, a profile is created or updated.
        #[arg(long)]
        profile_id: Option<String>,

        /// Profile name to create or update when `--profile-id` is not provided.
        #[arg(long, default_value = "research-radar-demo")]
        name: String,

        /// arXiv categories to search (e.g. cs.AI cs.CL cs.LG)
        #[arg(
            short,
            long,
            value_delimiter = ',',
            default_value = "cs.AI,cs.CL,cs.LG"
        )]
        categories: Vec<String>,

        /// Keywords to filter results (title/abstract match). Comma-separated.
        #[arg(short, long, value_delimiter = ',')]
        keywords: Vec<String>,

        /// Negative keywords to reject. Comma-separated.
        #[arg(short, long, value_delimiter = ',')]
        negative: Vec<String>,

        /// Path to the SQLite database
        #[arg(long, env = "RADAR_DB_PATH", default_value = "~/.radar/radar.db")]
        db_path: String,

        /// Path to the LanceDB directory
        #[arg(long, env = "RADAR_LANCE_PATH", default_value = "~/.radar/lance")]
        lance_path: String,

        /// Optional Discord webhook to exercise notification delivery.
        #[arg(long)]
        webhook_url: Option<String>,

        /// Embedding dimensions for the local mock embedder.
        #[arg(long, default_value = "128")]
        embedding_dims: usize,

        /// Max LLM spend in microunits per run.
        #[arg(long, default_value = "1000000")]
        max_spend: u64,
    },

    /// Run the scan-worker executor loop (polls for jobs, executes scans)
    ScanWorker {
        /// Path to the SQLite database
        #[arg(long, env = "RADAR_DB_PATH", default_value = "~/.radar/radar.db")]
        db_path: String,

        /// Path to the LanceDB directory
        #[arg(long, env = "RADAR_LANCE_PATH", default_value = "~/.radar/lance")]
        lance_path: String,

        /// Worker ID (defaults to a random UUID)
        #[arg(long)]
        worker_id: Option<String>,

        /// Lease duration in seconds
        #[arg(long, default_value = "300")]
        lease_duration: i64,

        /// Poll interval in seconds
        #[arg(long, default_value = "5")]
        poll_interval: u64,

        /// Embedding dimensions
        #[arg(long, default_value = "1536")]
        embedding_dims: usize,

        /// Max LLM spend in microunits per scorer
        #[arg(long, default_value = "1000000")]
        max_spend: u64,
    },

    /// Backfill embeddings for matched items into LanceDB
    Backfill {
        /// Profile ID to backfill items for
        #[arg(long)]
        profile_id: String,

        /// Path to the SQLite database
        #[arg(long, env = "RADAR_DB_PATH", default_value = "~/.radar/radar.db")]
        db_path: String,

        /// Path to the LanceDB directory
        #[arg(long, env = "RADAR_LANCE_PATH", default_value = "~/.radar/lance")]
        lance_path: String,

        /// Embedding dimensions
        #[arg(long, default_value = "1536")]
        embedding_dims: usize,
    },
}

fn expand_tilde(path: &str) -> PathBuf {
    if let Some(stripped) = path.strip_prefix("~/")
        && let Some(home) = dirs::home_dir()
    {
        return home.join(stripped);
    }
    PathBuf::from(path)
}

#[tokio::main]
#[allow(clippy::arc_with_non_send_sync)]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::ScanOnce {
            profile_id,
            name,
            categories,
            keywords,
            negative,
            db_path,
            lance_path,
            webhook_url,
            embedding_dims,
            max_spend,
        } => {
            let db_path = expand_tilde(&db_path);
            let lance_path = expand_tilde(&lance_path);
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent).expect("failed to create db directory");
            }
            std::fs::create_dir_all(&lance_path).expect("failed to create lance directory");

            let store = Store::open(&db_path).expect("failed to open database");
            let vector_store = VectorStore::open(&lance_path, embedding_dims)
                .await
                .expect("failed to open LanceDB");
            let embedder = MockEmbeddingBackend::new(embedding_dims);
            let notifier = DiscordNotifier::new();
            let adapter = StaticSourceAdapter::for_arxiv(&keywords, &categories);

            let profile = if let Some(profile_id) = profile_id {
                store
                    .get_profile(&profile_id)
                    .expect("failed to load profile")
            } else {
                match store.get_profile_by_name(&name) {
                    Ok(mut existing) => {
                        existing.keywords = keywords.clone();
                        existing.negative_keywords = negative.clone();
                        existing.sources = categories.clone();
                        existing.revision += 1;
                        existing.updated_at = chrono::Utc::now();
                        store
                            .update_profile(&existing)
                            .expect("failed to update profile");
                        existing
                    }
                    Err(_) => {
                        let mut created = Profile::new(name.clone());
                        created.keywords = keywords.clone();
                        created.negative_keywords = negative.clone();
                        created.sources = categories.clone();
                        store
                            .insert_profile(&created)
                            .expect("failed to create profile");
                        created
                    }
                }
            };

            if let Some(webhook_url) = webhook_url {
                let now = chrono::Utc::now();
                let sub = Subscription {
                    id: uuid::Uuid::new_v4().to_string(),
                    profile_id: profile.id.clone(),
                    channel: "discord".into(),
                    channel_config: serde_json::json!({ "webhook_url": webhook_url }).to_string(),
                    enabled: true,
                    created_at: now,
                    updated_at: now,
                };
                store
                    .upsert_subscription(&sub)
                    .expect("failed to upsert subscription");
            }

            let (job, reused) = store
                .enqueue_job(&profile.id, &profile.sources, "scan-once", false)
                .expect("failed to enqueue job");
            let config = ExecutorConfig {
                worker_id: format!("scan-once-{}", uuid::Uuid::new_v4()),
                lease_duration_secs: 300,
                lease_renew_interval_secs: 60,
                poll_interval_secs: 1,
            };
            let outcome = run_executor_once(
                &store,
                move |_profile| LlmScorer::new(MockLlmBackend::new(0.85), max_spend),
                &adapter,
                &notifier,
                Some(&vector_store),
                Some(&embedder),
                &config,
            )
            .await
            .expect("executor failed")
            .expect("no queued job was available");
            let final_job = store.get_job(&job.job_id).expect("failed to reload job");

            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "profile_id": profile.id,
                    "job_id": job.job_id,
                    "reused": reused,
                    "status": final_job.status.as_str(),
                    "matched": outcome.total_matched,
                    "scored": outcome.total_scored,
                    "fetched": outcome.total_fetched,
                    "notifications_sent": outcome.notifications_sent,
                    "vector_items": vector_store.item_count().await.unwrap_or(0),
                }))
                .unwrap()
            );
        }
        Commands::ScanWorker {
            db_path,
            lance_path,
            worker_id,
            lease_duration,
            poll_interval,
            embedding_dims,
            max_spend,
        } => {
            let db_path = expand_tilde(&db_path);
            let lance_path = expand_tilde(&lance_path);

            // Ensure parent directories exist
            if let Some(parent) = db_path.parent() {
                std::fs::create_dir_all(parent).expect("failed to create db directory");
            }
            std::fs::create_dir_all(&lance_path).expect("failed to create lance directory");

            let store = Arc::new(Store::open(&db_path).expect("failed to open database"));

            let vector_store = match VectorStore::open(&lance_path, embedding_dims).await {
                Ok(vs) => Some(Arc::new(vs)),
                Err(e) => {
                    tracing::warn!("LanceDB unavailable, running without vector store: {e}");
                    None
                }
            };

            // For now, use mock backends. Real backends require API keys via env vars.
            let embedder = Some(Arc::new(MockEmbeddingBackend::new(embedding_dims)));
            let adapter = Arc::new(ArxivAdapter::new());
            let notifier = Arc::new(DiscordNotifier::new());

            let config = ExecutorConfig {
                worker_id: worker_id.unwrap_or_else(|| format!("worker-{}", uuid::Uuid::new_v4())),
                lease_duration_secs: lease_duration,
                lease_renew_interval_secs: (lease_duration as u64) / 5,
                poll_interval_secs: poll_interval,
            };

            let (shutdown_tx, shutdown_rx) = watch::channel(false);

            // Handle SIGINT/SIGTERM for graceful shutdown
            let shutdown_tx_clone = shutdown_tx.clone();
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("shutdown signal received");
                let _ = shutdown_tx_clone.send(true);
            });

            eprintln!(
                "research.radar scan-worker starting (worker_id={})",
                config.worker_id
            );
            eprintln!("  db: {}", db_path.display());
            eprintln!("  lance: {}", lance_path.display());
            eprintln!("  poll_interval: {}s", poll_interval);

            run_executor_loop(
                store,
                move |_profile| LlmScorer::new(MockLlmBackend::new(0.5), max_spend),
                adapter,
                notifier,
                vector_store,
                embedder,
                config,
                shutdown_rx,
            )
            .await;

            eprintln!("scan-worker stopped");
        }
        Commands::Backfill {
            profile_id,
            db_path,
            lance_path,
            embedding_dims,
        } => {
            let db_path = expand_tilde(&db_path);
            let lance_path = expand_tilde(&lance_path);

            let store = Store::open(&db_path).expect("failed to open database");
            let vector_store = VectorStore::open(&lance_path, embedding_dims)
                .await
                .expect("failed to open LanceDB");
            let embedder = MockEmbeddingBackend::new(embedding_dims);

            eprintln!("Backfilling embeddings for profile {profile_id}...");

            let mut offset: u32 = 0;
            let batch_size: u32 = 100;
            let mut total = 0;

            loop {
                let matches = store
                    .list_matches(&profile_id, None, None, batch_size, offset)
                    .expect("failed to list matches");

                if matches.is_empty() {
                    break;
                }

                for (_score, item) in &matches {
                    let published = item.published_at.map(|dt| dt.to_rfc3339());
                    let text = radar_core::embedding::embedding_text(
                        &item.title,
                        item.abstract_text.as_deref(),
                    );
                    let embedding = embedder.embed(&text).await.expect("embedding failed");

                    if let Err(e) = vector_store
                        .upsert_item(
                            &item.canonical_id,
                            &item.title,
                            item.abstract_text.as_deref(),
                            &item.source_type,
                            published.as_deref(),
                            &embedding,
                        )
                        .await
                    {
                        tracing::warn!(
                            canonical_id = %item.canonical_id,
                            error = %e,
                            "backfill item failed"
                        );
                    } else {
                        total += 1;
                    }
                }

                offset += batch_size;
                eprint!("\r  backfilled {total} items...");
            }

            eprintln!("\nBackfill complete: {total} items ingested into LanceDB");
            let count = vector_store.item_count().await.unwrap_or(0);
            eprintln!("LanceDB item count: {count}");
        }
    }
}
