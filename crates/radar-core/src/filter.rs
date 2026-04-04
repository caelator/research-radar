use crate::types::SourceCandidate;

/// Field-aware keyword gate. Checks if a candidate matches any of the positive
/// keywords in title or abstract, and rejects if it matches negative keywords.
pub struct KeywordGate {
    pub keywords: Vec<String>,
    pub negative_keywords: Vec<String>,
}

impl KeywordGate {
    pub fn new(keywords: Vec<String>, negative_keywords: Vec<String>) -> Self {
        Self {
            keywords: keywords.into_iter().map(|k| k.to_lowercase()).collect(),
            negative_keywords: negative_keywords
                .into_iter()
                .map(|k| k.to_lowercase())
                .collect(),
        }
    }

    /// Returns true if the candidate passes the keyword gate:
    /// - Must match at least one positive keyword in title or abstract
    /// - Must NOT match any negative keyword
    pub fn passes(&self, candidate: &SourceCandidate) -> bool {
        let title_lower = candidate.title.to_lowercase();
        let abstract_lower = candidate
            .abstract_text
            .as_deref()
            .unwrap_or("")
            .to_lowercase();

        // Check negative keywords first (reject fast)
        for neg in &self.negative_keywords {
            if title_lower.contains(neg.as_str()) || abstract_lower.contains(neg.as_str()) {
                return false;
            }
        }

        // If no positive keywords specified, everything passes
        if self.keywords.is_empty() {
            return true;
        }

        // Must match at least one positive keyword
        for kw in &self.keywords {
            if title_lower.contains(kw.as_str()) || abstract_lower.contains(kw.as_str()) {
                return true;
            }
        }

        false
    }

    /// Filter a list of candidates, returning only those that pass the gate.
    /// Also returns the count of rejected candidates.
    pub fn filter(&self, candidates: Vec<SourceCandidate>) -> (Vec<SourceCandidate>, usize) {
        let total = candidates.len();
        let passed: Vec<_> = candidates.into_iter().filter(|c| self.passes(c)).collect();
        let rejected = total - passed.len();
        (passed, rejected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SourceType;

    fn make_candidate(title: &str, abstract_text: &str) -> SourceCandidate {
        SourceCandidate {
            canonical_id: "test:1".into(),
            title: title.into(),
            authors: None,
            abstract_text: Some(abstract_text.into()),
            url: "http://example.com".into(),
            published_at: None,
            source_type: SourceType::Arxiv,
            aliases: vec![],
            raw_json: None,
        }
    }

    #[test]
    fn test_positive_match_title() {
        let gate = KeywordGate::new(vec!["transformer".into()], vec![]);
        let c = make_candidate("A New Transformer Architecture", "Some abstract.");
        assert!(gate.passes(&c));
    }

    #[test]
    fn test_positive_match_abstract() {
        let gate = KeywordGate::new(vec!["attention".into()], vec![]);
        let c = make_candidate("Some Paper", "We study attention mechanisms.");
        assert!(gate.passes(&c));
    }

    #[test]
    fn test_negative_reject() {
        let gate = KeywordGate::new(vec!["language model".into()], vec!["medical".into()]);
        let c = make_candidate(
            "Language Model for Medical Diagnosis",
            "We apply language models to medical imaging.",
        );
        assert!(!gate.passes(&c));
    }

    #[test]
    fn test_no_match() {
        let gate = KeywordGate::new(vec!["quantum".into()], vec![]);
        let c = make_candidate("A New Transformer Architecture", "We study attention.");
        assert!(!gate.passes(&c));
    }

    #[test]
    fn test_empty_keywords_passes_all() {
        let gate = KeywordGate::new(vec![], vec![]);
        let c = make_candidate("Anything", "Whatever");
        assert!(gate.passes(&c));
    }
}
