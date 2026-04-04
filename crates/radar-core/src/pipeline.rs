use chrono::{Duration, Utc};

use crate::db::Store;
use crate::error::Result;
use crate::source::SourceAdapter;
use crate::types::{SourceCandidate, SourceWatermark};

/// Maximum lookback window when no watermark exists or scope has changed.
const MAX_CATCHUP_WINDOW_DAYS: i64 = 14;

/// Overlap lookback applied to watermarks for dedup safety.
const OVERLAP_LOOKBACK_HOURS: i64 = 2;

/// Result of a fetch-with-watermark cycle.
pub struct FetchResult {
    pub candidates: Vec<SourceCandidate>,
    pub new_watermark: String,
    pub gap_skipped: bool,
}

/// Fetch candidates from a source with watermark tracking.
///
/// - Loads existing watermark for (profile_id, source_type, scope_hash).
/// - If no watermark exists, sets `since = now - MAX_CATCHUP_WINDOW` and flags
///   `gap_skipped` if the profile was created before that window.
/// - If `scope_hash` differs from the stored watermark, resets to MAX_CATCHUP_WINDOW.
/// - Applies overlap lookback window for dedup safety.
/// - Returns candidates and the new watermark position.
pub async fn fetch_with_watermark<A: SourceAdapter>(
    store: &Store,
    profile_id: &str,
    source_type: &str,
    scope_hash: &str,
    categories: &[String],
    adapter: &A,
) -> Result<FetchResult> {
    let now = Utc::now();
    let catchup_floor = now - Duration::days(MAX_CATCHUP_WINDOW_DAYS);

    let existing_wm = store.get_watermark(profile_id, source_type, scope_hash)?;

    let (since, gap_skipped) = match &existing_wm {
        Some(wm) => {
            // Watermark exists and scope matches — apply overlap lookback
            let since = wm.high_watermark - Duration::hours(OVERLAP_LOOKBACK_HOURS);
            (since, false)
        }
        None => {
            // Check if there is a watermark for this profile+source with a different scope hash.
            // If so, the scope changed and we reset.
            // Either way, use the catchup floor.
            let profile = store.get_profile(profile_id)?;
            let gap_skipped = profile.created_at < catchup_floor;
            (catchup_floor, gap_skipped)
        }
    };

    let candidates = adapter.fetch(categories, Some(since), 200).await?;

    let new_watermark = now.to_rfc3339();

    // Persist the new watermark position
    store.upsert_watermark(&SourceWatermark {
        profile_id: profile_id.to_string(),
        source_type: source_type.to_string(),
        source_scope_hash: scope_hash.to_string(),
        high_watermark: now,
        updated_at: now,
    })?;

    Ok(FetchResult {
        candidates,
        new_watermark,
        gap_skipped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Store;
    use crate::error::Result;
    use crate::types::{Profile, SourceCandidate, SourceType, SourceWatermark};
    use chrono::{DateTime, Duration, Utc};
    use std::future::Future;

    /// A stub adapter that returns a fixed set of candidates.
    struct StubAdapter {
        candidates: Vec<SourceCandidate>,
    }

    impl StubAdapter {
        fn new(candidates: Vec<SourceCandidate>) -> Self {
            Self { candidates }
        }

        fn empty() -> Self {
            Self::new(vec![])
        }

        fn with_one() -> Self {
            Self::new(vec![SourceCandidate {
                canonical_id: "test:stub-1".into(),
                title: "Stub Paper".into(),
                authors: None,
                abstract_text: None,
                url: "https://example.com/stub".into(),
                published_at: Some(Utc::now()),
                source_type: SourceType::Arxiv,
                aliases: vec![],
                raw_json: None,
            }])
        }
    }

    impl crate::source::SourceAdapter for StubAdapter {
        fn fetch(
            &self,
            _categories: &[String],
            _since: Option<DateTime<Utc>>,
            _max_results: u32,
        ) -> impl Future<Output = Result<Vec<SourceCandidate>>> + Send {
            let candidates = self.candidates.clone();
            async move { Ok(candidates) }
        }

        fn source_type(&self) -> &'static str {
            "stub"
        }
    }

    fn make_ready_profile() -> Profile {
        let mut p = Profile::new("pipeline-test".to_string());
        p.keywords = vec!["test".to_string()];
        p
    }

    #[tokio::test]
    async fn test_watermark_advances_after_fetch() {
        let store = Store::open_memory().unwrap();
        let profile = make_ready_profile();
        store.insert_profile(&profile).unwrap();

        let adapter = StubAdapter::with_one();
        let scope_hash = "abc123";

        // First fetch — no watermark yet
        let result = fetch_with_watermark(&store, &profile.id, "stub", scope_hash, &[], &adapter)
            .await
            .unwrap();
        assert_eq!(result.candidates.len(), 1);

        let wm1 = store
            .get_watermark(&profile.id, "stub", scope_hash)
            .unwrap()
            .expect("watermark should exist after fetch");

        // Second fetch — watermark should advance
        let _result2 = fetch_with_watermark(&store, &profile.id, "stub", scope_hash, &[], &adapter)
            .await
            .unwrap();

        let wm2 = store
            .get_watermark(&profile.id, "stub", scope_hash)
            .unwrap()
            .expect("watermark should still exist");

        assert!(wm2.high_watermark >= wm1.high_watermark);
    }

    #[tokio::test]
    async fn test_scope_change_resets_watermark() {
        let store = Store::open_memory().unwrap();
        let profile = make_ready_profile();
        store.insert_profile(&profile).unwrap();

        let adapter = StubAdapter::empty();

        // Set up a watermark with scope "old_scope"
        let old_time = Utc::now() - Duration::hours(48);
        store
            .upsert_watermark(&SourceWatermark {
                profile_id: profile.id.clone(),
                source_type: "stub".to_string(),
                source_scope_hash: "old_scope".to_string(),
                high_watermark: old_time,
                updated_at: old_time,
            })
            .unwrap();

        // Fetch with a different scope hash — should reset to catchup window
        let result = fetch_with_watermark(&store, &profile.id, "stub", "new_scope", &[], &adapter)
            .await
            .unwrap();

        // The new watermark should be recent (not from old_time)
        let new_wm = store
            .get_watermark(&profile.id, "stub", "new_scope")
            .unwrap()
            .expect("new scope watermark should exist");

        // old watermark should still be there, untouched
        let old_wm = store
            .get_watermark(&profile.id, "stub", "old_scope")
            .unwrap()
            .expect("old scope watermark should remain");
        assert_eq!(old_wm.high_watermark, old_time);

        // New watermark should be much more recent
        assert!(new_wm.high_watermark > old_time + Duration::hours(40));
        assert!(!result.gap_skipped);
    }

    #[tokio::test]
    async fn test_no_watermark_uses_catchup_window() {
        let store = Store::open_memory().unwrap();
        // Create a profile with a creation time well before the catchup window
        let mut profile = make_ready_profile();
        profile.created_at = Utc::now() - Duration::days(30);
        store.insert_profile(&profile).unwrap();

        let adapter = StubAdapter::empty();

        let result = fetch_with_watermark(&store, &profile.id, "stub", "scope1", &[], &adapter)
            .await
            .unwrap();

        // Profile was created 30 days ago, catchup window is 14 days -> gap_skipped should be true
        assert!(result.gap_skipped);
    }
}
