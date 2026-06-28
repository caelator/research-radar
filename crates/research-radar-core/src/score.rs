//! Keyword-overlap scorer.
//!
//! Scores entries by counting how many of the profile's keywords appear in the
//! entry's content or summary. Matching is **token-based**, not substring-based:
//! a keyword matches only when it appears as a whole word (respecting word
//! boundaries), so a keyword like `"AI"` does not match inside `"rain"`,
//! `"detail"`, or `"email"`.

use crate::{Entry, Profile};

/// Return true if `haystack` contains `needle` as a whole-word match,
/// case-insensitively.
///
/// A "word" is a maximal run of Unicode alphanumeric characters (plus `'` and
/// `-`, to handle tokens like `state-of-the-art` and `don't`). This means
/// `"AI"` matches the standalone token in `"an AI model"` but not inside
/// `"rain"` or `"email"`.
fn contains_word(haystack_lower: &str, needle_lower: &str) -> bool {
    // Single-word or multi-word keyword: every component word must appear as
    // a standalone token. We split the needle on whitespace and require all
    // parts to be present in order-independent fashion is too loose for
    // multi-word phrases, so we also support the phrase as a contiguous
    // sequence of tokens.

    // For single-word keywords, do exact token membership.
    let needle_words: Vec<&str> = needle_lower.split_whitespace().collect();

    if needle_words.is_empty() {
        return false;
    }

    // Tokenize the haystack into lowercase words.
    let tokens: Vec<&str> = tokenize(haystack_lower).collect();

    if needle_words.len() == 1 {
        return tokens.contains(&needle_words[0]);
    }

    // Multi-word keyword: check if the sequence appears contiguously in tokens.
    tokens
        .windows(needle_words.len())
        .any(|window| window == needle_words.as_slice())
}

/// Split text into lowercase word tokens.
///
/// A word is a maximal run of alphanumeric characters, apostrophes, or hyphens.
/// This keeps compound tokens like `"tokio-based"` and `"don't"` intact while
/// dropping punctuation.
fn tokenize(s: &str) -> impl Iterator<Item = &str> {
    s.split(|c: char| !(c.is_alphanumeric() || c == '\'' || c == '-'))
        .filter(|tok| !tok.is_empty())
}

/// Weighted keyword-overlap score.
///
/// Each keyword contributes up to 1.0 / N to the total score (where N is the
/// number of keywords). A keyword that matches earns its full share, plus a
/// small frequency bonus when it appears multiple times (capped so a single
/// keyword can contribute at most ~1.4× its base share). Keywords that match
/// in *both* content and summary get an additional boost, reflecting that the
/// entry discusses the topic prominently.
///
/// Returns a score between 0.0 and 1.0 (clamped):
/// - 0.0 if any negative keyword matches or the profile has no keywords
/// - otherwise, the sum of per-keyword contributions
pub fn score_entry(entry: &Entry, profile: &Profile) -> f64 {
    let content_lower = entry.content.to_lowercase();
    let summary_lower = entry.summary.as_deref().unwrap_or("").to_lowercase();

    // Check negative keywords first — immediate disqualification.
    for nk in &profile.negative_keywords {
        let nk_lower = nk.to_lowercase();
        if contains_word(&content_lower, &nk_lower)
            || contains_word(&summary_lower, &nk_lower)
        {
            return 0.0;
        }
    }

    if profile.keywords.is_empty() {
        return 0.0;
    }

    let n = profile.keywords.len() as f64;
    let base_share = 1.0 / n;

    let total: f64 = profile
        .keywords
        .iter()
        .map(|kw| {
            let kw_lower = kw.to_lowercase();
            let in_content = contains_word(&content_lower, &kw_lower);
            let in_summary = contains_word(&summary_lower, &kw_lower);

            if !in_content && !in_summary {
                return 0.0;
            }

            // Frequency bonus: count occurrences in content (capped at 3).
            let freq = if in_content {
                count_word(&content_lower, &kw_lower).min(3) as f64
            } else {
                0.0
            };
            let freq_bonus = (freq - 1.0).max(0.0) * 0.1;

            // Dual-field boost: matching in both content and summary signals
            // stronger relevance.
            let dual_boost = if in_content && in_summary { 0.15 } else { 0.0 };

            // Cap each keyword's contribution at ~1.4× its base share.
            let contribution = base_share * (1.0 + freq_bonus + dual_boost);
            contribution.min(base_share * 1.4)
        })
        .sum();

    total.clamp(0.0, 1.0)
}

/// Count how many times `needle` appears as a whole word in `haystack_lower`.
fn count_word(haystack_lower: &str, needle_lower: &str) -> usize {
    let needle_words: Vec<&str> = needle_lower.split_whitespace().collect();
    if needle_words.is_empty() {
        return 0;
    }
    let tokens: Vec<&str> = tokenize(haystack_lower).collect();
    if needle_words.len() == 1 {
        return tokens.iter().filter(|t| **t == needle_words[0]).count();
    }
    tokens
        .windows(needle_words.len())
        .filter(|w| *w == needle_words.as_slice())
        .count()
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
        // "AI" appears as a proper standalone token (case-insensitive)
        let entry = make_entry("artificial intelligence (AI) research", None);
        let profile = make_profile(vec!["AI"], vec![]);
        assert_eq!(score_entry(&entry, &profile), 1.0);
    }

    // ── New tests: tokenization correctness ────────────────────────

    #[test]
    fn substring_false_positive_eliminated() {
        // "AI" must NOT match inside "rain", "detail", or "email".
        let entry = make_entry("Heavy rain damaged the email detail report", None);
        let profile = make_profile(vec!["AI"], vec![]);
        assert_eq!(
            score_entry(&entry, &profile),
            0.0,
            "tokenized matching must not match 'AI' inside other words"
        );
    }

    #[test]
    fn keyword_matches_as_standalone_token() {
        let entry = make_entry("This paper presents an AI approach to safety", None);
        let profile = make_profile(vec!["AI"], vec![]);
        assert_eq!(score_entry(&entry, &profile), 1.0);
    }

    #[test]
    fn multi_word_keyword_phrase_match() {
        let entry = make_entry("Advances in machine learning for safety", None);
        let profile = make_profile(vec!["machine learning"], vec![]);
        assert_eq!(score_entry(&entry, &profile), 1.0);
    }

    #[test]
    fn multi_word_keyword_partial_does_not_match() {
        // "machine learning" as a phrase should not match "machine" alone.
        let entry = make_entry("This machine is broken", None);
        let profile = make_profile(vec!["machine learning"], vec![]);
        assert_eq!(score_entry(&entry, &profile), 0.0);
    }

    #[test]
    fn hyphenated_tokens_preserved() {
        let entry = make_entry("A state-of-the-art tokio-based runtime", None);
        let profile = make_profile(vec!["tokio-based"], vec![]);
        assert_eq!(score_entry(&entry, &profile), 1.0);
    }

    #[test]
    fn negative_keyword_word_boundary() {
        // Negative keyword "rust" should not trigger on "trust" or "frustrating".
        let entry = make_entry("A frustrating trust exercise without rust", None);
        let profile = make_profile(vec!["trust"], vec!["rust"]);
        assert_eq!(score_entry(&entry, &profile), 0.0);
    }

    // ── Weighted scoring tests ──────────────────────────────────────

    #[test]
    fn dual_field_match_scores_higher_than_single_field() {
        // A keyword that appears in both content and summary should score
        // higher than one that appears in content only. Use a multi-keyword
        // profile so the base share is < 1.0 (otherwise clamping hides the
        // difference).
        let single = make_entry("AI research paper about systems", None);
        let dual = make_entry("AI research paper about systems", Some("AI and systems"));
        let profile = make_profile(vec!["AI", "systems", "compilers"], vec![]);
        let s_single = score_entry(&single, &profile);
        let s_dual = score_entry(&dual, &profile);
        assert!(
            s_dual > s_single,
            "dual-field match ({s_dual}) must outrank single-field ({s_single})"
        );
    }

    #[test]
    fn repeated_keyword_gets_frequency_bonus() {
        // A keyword that appears multiple times signals stronger relevance.
        // Use a multi-keyword profile so base share < 1.0.
        let sparse = make_entry("AI is interesting for compilers", None);
        let dense = make_entry("AI AI AI AI breakthrough in compilers", None);
        let profile = make_profile(vec!["AI", "compilers", "systems"], vec![]);
        let s_sparse = score_entry(&sparse, &profile);
        let s_dense = score_entry(&dense, &profile);
        assert!(
            s_dense > s_sparse,
            "frequent keyword ({s_dense}) must outrank sparse ({s_sparse})"
        );
    }

    #[test]
    fn score_never_exceeds_one() {
        // Even with many keywords all matching in both fields with high freq,
        // the total must be clamped to 1.0.
        let entry = make_entry(
            "AI AI AI safety safety safety alignment alignment alignment",
            Some("AI safety alignment research"),
        );
        let profile = make_profile(vec!["AI", "safety", "alignment"], vec![]);
        let score = score_entry(&entry, &profile);
        assert!(
            score <= 1.0 && score > 0.0,
            "score must be in (0, 1], got {score}"
        );
    }
}
