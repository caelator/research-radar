use chrono::Utc;
use radar_core::db::Store;
use radar_core::embedding::MockEmbeddingBackend;
use radar_core::executor::{ExecutorConfig, run_executor_once};
use radar_core::notify::MockNotifier;
use radar_core::scorer::{LlmScorer, MockLlmBackend};
use radar_core::source::StaticSourceAdapter;
use radar_core::types::{Profile, SourceCandidate, SourceType, Subscription};
use radar_core::vector::VectorStore;

#[tokio::test]
async fn full_pipeline_persists_matches_and_attempts_notification() {
    let store = Store::open_memory().unwrap();
    let mut profile = Profile::new("phase1-e2e".into());
    profile.keywords = vec!["transformers".into(), "agents".into()];
    profile.sources = vec!["cs.AI".into()];
    store.insert_profile(&profile).unwrap();

    let now = Utc::now();
    let subscription = Subscription {
        id: uuid::Uuid::new_v4().to_string(),
        profile_id: profile.id.clone(),
        channel: "discord".into(),
        channel_config: serde_json::json!({
            "webhook_url": "mock://discord/phase1"
        })
        .to_string(),
        enabled: true,
        created_at: now,
        updated_at: now,
    };
    store.upsert_subscription(&subscription).unwrap();

    let adapter = StaticSourceAdapter::new(vec![SourceCandidate {
        canonical_id: "mock:paper-1".into(),
        title: "Transformers for agentic systems".into(),
        authors: Some("Radar Test".into()),
        abstract_text: Some("A mock paper about transformers and agents.".into()),
        url: "https://example.com/paper-1".into(),
        published_at: Some(now),
        source_type: SourceType::Arxiv,
        aliases: vec![("arxiv_id".into(), "mock-paper-1".into())],
        raw_json: None,
    }]);
    let embedder = MockEmbeddingBackend::new(64);
    let vector_store = VectorStore::open_temp(64).await.unwrap();
    let notifier = MockNotifier::default();

    let (job, reused) = store
        .enqueue_job(&profile.id, &profile.sources, "integration-test", false)
        .unwrap();
    assert!(!reused);

    let config = ExecutorConfig {
        worker_id: "phase1-test-worker".into(),
        lease_duration_secs: 300,
        lease_renew_interval_secs: 60,
        poll_interval_secs: 1,
    };

    let outcome = run_executor_once(
        &store,
        |_profile| LlmScorer::new(MockLlmBackend::new(0.91), 1_000_000),
        &adapter,
        &notifier,
        Some(&vector_store),
        Some(&embedder),
        &config,
    )
    .await
    .unwrap()
    .unwrap();

    assert_eq!(outcome.total_fetched, 1);
    assert_eq!(outcome.total_scored, 1);
    assert_eq!(outcome.total_matched, 1);
    assert_eq!(outcome.notifications_sent, 1);

    let final_job = store.get_job(&job.job_id).unwrap();
    assert_eq!(final_job.status.as_str(), "completed");

    let matches = store.list_matches(&profile.id, None, None, 10, 0).unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(vector_store.item_count().await.unwrap(), 1);

    let batches = notifier.sent_batches();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].0, "mock://discord/phase1");
    assert_eq!(batches[0].1, "phase1-e2e");
    assert_eq!(batches[0].2, 1);
}
