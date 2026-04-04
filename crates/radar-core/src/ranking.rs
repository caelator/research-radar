use crate::types::SourceCandidate;

/// A candidate annotated with a deterministic rank score.
#[derive(Debug, Clone)]
pub struct RankedCandidate {
    pub candidate: SourceCandidate,
    pub rank_score: f64, // composite of recency + keyword density
}

/// Deterministic ranking: sort keyword-passed candidates by recency + keyword density,
/// truncate to `max_llm_calls` limit. Guarantees bounded LLM calls per scan.
pub fn rank_candidates(
    candidates: Vec<SourceCandidate>,
    keywords: &[String],
    max_llm_calls: usize,
) -> Vec<RankedCandidate> {
    if candidates.is_empty() {
        return Vec::new();
    }

    let keywords_lower: Vec<String> = keywords.iter().map(|k| k.to_lowercase()).collect();

    // Compute recency bounds for normalization
    let (min_ts, max_ts) = recency_bounds(&candidates);
    let span = max_ts - min_ts;

    let mut ranked: Vec<RankedCandidate> = candidates
        .into_iter()
        .map(|c| {
            let kd = keyword_density(&c, &keywords_lower);
            let rs = recency_score(&c, min_ts, span);
            let composite = 0.6 * kd + 0.4 * rs;
            RankedCandidate {
                candidate: c,
                rank_score: composite,
            }
        })
        .collect();

    // Sort descending by rank_score, stable sort for determinism
    ranked.sort_by(|a, b| {
        b.rank_score
            .partial_cmp(&a.rank_score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    ranked.truncate(max_llm_calls);
    ranked
}

/// Fraction of keywords matched in title + abstract.
fn keyword_density(candidate: &SourceCandidate, keywords_lower: &[String]) -> f64 {
    if keywords_lower.is_empty() {
        return 0.0;
    }
    let title_lower = candidate.title.to_lowercase();
    let abstract_lower = candidate
        .abstract_text
        .as_deref()
        .unwrap_or("")
        .to_lowercase();

    let matched = keywords_lower
        .iter()
        .filter(|kw| title_lower.contains(kw.as_str()) || abstract_lower.contains(kw.as_str()))
        .count();

    matched as f64 / keywords_lower.len() as f64
}

/// Normalize published_at to 0.0..1.0 within the batch. Candidates without
/// a timestamp get 0.0 (oldest).
fn recency_score(candidate: &SourceCandidate, min_ts: i64, span: i64) -> f64 {
    if span == 0 {
        return 0.5; // all same timestamp (or all missing)
    }
    let ts = candidate
        .published_at
        .map(|dt| dt.timestamp())
        .unwrap_or(min_ts);
    (ts - min_ts) as f64 / span as f64
}

/// Returns (min_timestamp, max_timestamp) across the batch.
fn recency_bounds(candidates: &[SourceCandidate]) -> (i64, i64) {
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    let mut any = false;

    for c in candidates {
        if let Some(dt) = c.published_at {
            let ts = dt.timestamp();
            if ts < min_ts {
                min_ts = ts;
            }
            if ts > max_ts {
                max_ts = ts;
            }
            any = true;
        }
    }

    if !any {
        return (0, 0);
    }
    (min_ts, max_ts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SourceType;
    use chrono::{DateTime, TimeZone, Utc};

    fn make_candidate(
        title: &str,
        abstract_text: &str,
        published_at: Option<DateTime<Utc>>,
    ) -> SourceCandidate {
        SourceCandidate {
            canonical_id: format!("test:{title}"),
            title: title.into(),
            authors: None,
            abstract_text: Some(abstract_text.into()),
            url: "http://example.com".into(),
            published_at,
            source_type: SourceType::Arxiv,
            aliases: vec![],
            raw_json: None,
        }
    }

    #[test]
    fn test_ranking_respects_limit() {
        let kw = vec!["transformer".into()];
        let candidates: Vec<_> = (0..10)
            .map(|i| {
                make_candidate(
                    &format!("Transformer Paper {i}"),
                    "About transformers",
                    Some(Utc::now()),
                )
            })
            .collect();

        let ranked = rank_candidates(candidates, &kw, 3);
        assert_eq!(ranked.len(), 3);
    }

    #[test]
    fn test_ranking_stable_with_same_input() {
        let kw = vec!["attention".into(), "transformer".into()];
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap();

        let make_batch = || {
            vec![
                make_candidate("Attention in Transformers", "Both keywords here", Some(t1)),
                make_candidate("Only Attention", "No other keyword", Some(t2)),
                make_candidate("Only Transformer", "Transformer stuff", Some(t1)),
            ]
        };

        let r1 = rank_candidates(make_batch(), &kw, 10);
        let r2 = rank_candidates(make_batch(), &kw, 10);

        for (a, b) in r1.iter().zip(r2.iter()) {
            assert_eq!(a.candidate.canonical_id, b.candidate.canonical_id);
            assert!((a.rank_score - b.rank_score).abs() < f64::EPSILON);
        }
    }

    #[test]
    fn test_ranking_keyword_density() {
        let kw = vec!["attention".into(), "transformer".into(), "scaling".into()];
        let t = Utc.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap();

        let candidates = vec![
            make_candidate(
                "Attention and Transformer Scaling",
                "All three keywords",
                Some(t),
            ),
            make_candidate("Only Attention", "Single keyword", Some(t)),
        ];

        let ranked = rank_candidates(candidates, &kw, 10);
        // The candidate with all 3 keywords should rank higher
        assert!(ranked[0].rank_score > ranked[1].rank_score);
        assert!(ranked[0].candidate.title.contains("Scaling"));
    }

    #[test]
    fn test_ranking_empty_input() {
        let ranked = rank_candidates(vec![], &["test".to_string()], 5);
        assert!(ranked.is_empty());
    }

    #[test]
    fn test_ranking_limit_larger_than_input() {
        let kw = vec!["test".into()];
        let candidates = vec![make_candidate(
            "Test paper",
            "about testing",
            Some(Utc::now()),
        )];
        let ranked = rank_candidates(candidates, &kw, 100);
        assert_eq!(ranked.len(), 1);
    }
}
