//! Mock scorer for Phase 1 — keyword-overlap based scoring.
//!
//! In production this will delegate to an LLM. For Phase 1 we score
//! entries by counting how many of the profile's keywords appear in
//! the entry's content or summary.

use crate::{Entry, Profile};

/// Score an entry against a profile by keyword overlap.
///
/// Returns a score between 0.0 and 1.0:
/// - 0.0 if any negative keyword matches or the entry has no keywords to match
/// - (matched_keywords / total_keywords) otherwise
pub fn score_entry(entry: &Entry, profile: &Profile) -> f64 {
    let content_lower = entry.content.to_lowercase();
    let summary_lower = entry.summary.as_deref().unwrap_or("").to_lowercase();

    // Check negative keywords first — immediate disqualification.
    for nk in &profile.negative_keywords {
        let nk_lower = nk.to_lowercase();
        if content_lower.contains(&nk_lower) || summary_lower.contains(&nk_lower) {
            return 0.0;
        }
    }

    if profile.keywords.is_empty() {
        return 0.0;
    }

    let matches: usize = profile
        .keywords
        .iter()
        .filter(|kw| {
            let kw_lower = kw.to_lowercase();
            content_lower.contains(&kw_lower) || summary_lower.contains(&kw_lower)
        })
        .count();

    (matches as f64) / (profile.keywords.len() as f64)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(content: &str, summary: Option<&str>) -> Entry {
        Entry {
            id: uuid::Uuid::new_v4().to_string(),
            source_id: "src1".to_string(),
            content: content.to_string(),
            summary: summary.map(String::from),
            tags: vec![],
            relevance_score: 0.0,
            last_reread_at: None,
        }
    }

    fn make_profile(keywords: Vec<&str>, negative_keywords: Vec<&str>) -> Profile {
        Profile {
            id: uuid::Uuid::new_v4().to_string(),
            name: "Test".to_string(),
            keywords: keywords.into_iter().map(String::from).collect(),
            negative_keywords: negative_keywords.into_iter().map(String::from).collect(),
            sources: vec![],
            scoring_prompt: None,
            score_threshold: 0.5,
            max_llm_calls: 10,
            revision: 1,
            last_seen_at: None,
            archived_at: None,
            created_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn score_entry_full_match() {
        let entry = make_entry("AI safety research is important", None);
        let profile = make_profile(vec!["AI", "safety"], vec![]);
        assert_eq!(score_entry(&entry, &profile), 1.0);
    }

    #[test]
    fn score_entry_partial_match() {
        let entry = make_entry("AI research methods", None);
        let profile = make_profile(vec!["AI", "safety", "alignment"], vec![]);
        // 1 out of 3 keywords = 0.333...
        let score = score_entry(&entry, &profile);
        assert!((score - 1.0 / 3.0).abs() < 0.001);
    }

    #[test]
    fn score_entry_negative_keyword() {
        let entry = make_entry("AI safety research", None);
        let profile = make_profile(vec!["AI", "safety"], vec!["research"]);
        assert_eq!(score_entry(&entry, &profile), 0.0);
    }

    #[test]
    fn score_entry_empty_keywords() {
        let entry = make_entry("Some content", None);
        let profile = make_profile(vec![], vec![]);
        assert_eq!(score_entry(&entry, &profile), 0.0);
    }

    #[test]
    fn score_entry_no_match() {
        let entry = make_entry("Gardening tips for beginners", None);
        let profile = make_profile(vec!["AI", "ML"], vec![]);
        assert_eq!(score_entry(&entry, &profile), 0.0);
    }

    #[test]
    fn score_entry_matches_summary() {
        let entry = make_entry("Some general content here", Some("This is about AI safety"));
        let profile = make_profile(vec!["AI", "safety"], vec![]);
        // Both keywords appear in summary
        assert_eq!(score_entry(&entry, &profile), 1.0);
    }

    #[test]
    fn score_entry_case_insensitive() {
        // "AI" appears as a proper substring (case-insensitive)
        let entry = make_entry("artificial intelligence (AI) research", None);
        let profile = make_profile(vec!["AI"], vec![]);
        assert_eq!(score_entry(&entry, &profile), 1.0);
    }
}
