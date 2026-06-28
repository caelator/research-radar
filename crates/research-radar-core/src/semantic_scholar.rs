//! Semantic Scholar source adapter — fetch papers from the S2 API.
//!
//! Uses the public Semantic Scholar Academic Graph API to search for papers
//! matching profile keywords. Respects rate limits (100 req/5min for
//! unauthenticated, higher with API key).

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{Entry, Profile, Source, SourceType};

/// A paper fetched from Semantic Scholar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S2Paper {
    pub paper_id: String,
    pub title: String,
    pub abstract_text: String,
    pub authors: Vec<String>,
    pub year: Option<u32>,
    pub venue: Option<String>,
    pub citation_count: Option<u32>,
    pub fields_of_study: Vec<String>,
    pub publication_date: Option<DateTime<Utc>>,
    pub external_ids: S2ExternalIds,
    pub url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct S2ExternalIds {
    pub arxiv_id: Option<String>,
    pub doi: Option<String>,
    pub corpus_id: Option<String>,
}

/// Errors from Semantic Scholar fetching.
#[derive(Debug, thiserror::Error)]
pub enum S2Error {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("rate limited")]
    RateLimited,
}

/// Response envelope from S2 relevance search.
#[derive(Debug, Deserialize)]
struct S2SearchResponse {
    #[serde(default)]
    data: Vec<S2SearchResult>,
}

/// Individual result from S2 search.
#[derive(Debug, Deserialize)]
struct S2SearchResult {
    #[serde(rename = "paperId")]
    paper_id: Option<String>,
    title: Option<String>,
    #[serde(rename = "abstract")]
    abstract_text: Option<String>,
    #[serde(default)]
    authors: Vec<S2Author>,
    year: Option<u32>,
    venue: Option<String>,
    #[serde(rename = "citationCount")]
    citation_count: Option<u32>,
    #[serde(rename = "fieldsOfStudy")]
    fields_of_study: Option<Vec<String>>,
    #[serde(rename = "publicationDate")]
    publication_date: Option<String>,
    #[serde(rename = "externalIds")]
    external_ids: Option<S2RawExternalIds>,
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct S2Author {
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct S2RawExternalIds {
    #[serde(rename = "ArXiv")]
    arxiv: Option<String>,
    #[serde(rename = "DOI")]
    doi: Option<String>,
    #[serde(rename = "CorpusId")]
    corpus_id: Option<serde_json::Value>,
}

const S2_API_BASE: &str = "https://api.semanticscholar.org/graph/v1";
const S2_FIELDS: &str = "paperId,title,abstract,authors,year,venue,citationCount,fieldsOfStudy,publicationDate,externalIds,url";

/// Fetch papers from Semantic Scholar matching the profile's keywords.
///
/// Returns up to `max_results` papers. Uses the relevance search endpoint.
pub async fn fetch_s2_papers(
    profile: &Profile,
    max_results: usize,
) -> Result<Vec<S2Paper>, S2Error> {
    if profile.keywords.is_empty() {
        return Ok(Vec::new());
    }

    let query = build_query(profile);
    if query.is_empty() {
        return Ok(Vec::new());
    }

    let limit = max_results.min(100);
    let url = format!(
        "{}/paper/search?query={}&limit={}&fields={}",
        S2_API_BASE,
        urlencoded(&query),
        limit,
        S2_FIELDS,
    );

    let client = crate::http_client().map_err(|e| S2Error::Http(e.to_string()))?;
    let mut req = client
        .get(&url)
        .header("User-Agent", "research-radar/0.1");

    // Use API key if available for higher rate limits
    if let Ok(key) = std::env::var("S2_API_KEY") {
        if !key.is_empty() {
            req = req.header("x-api-key", key);
        }
    }

    let resp = req.send().await.map_err(|e| S2Error::Http(e.to_string()))?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(S2Error::RateLimited);
    }

    if !resp.status().is_success() {
        return Err(S2Error::Http(format!(
            "Semantic Scholar returned {}",
            resp.status()
        )));
    }

    let body: S2SearchResponse = resp
        .json()
        .await
        .map_err(|e| S2Error::Parse(e.to_string()))?;

    let papers = body.data.into_iter().filter_map(convert_result).collect();

    Ok(papers)
}

/// Convert an S2Paper into a Source + Entry pair for storage.
pub fn paper_to_source_entry(paper: &S2Paper) -> (Source, Entry) {
    let source = Source {
        id: uuid::Uuid::new_v4().to_string(),
        url: paper.url.clone(),
        title: paper.title.clone(),
        source_type: SourceType::Paper,
        added_at: Utc::now(),
    };

    let venue_str = paper
        .venue
        .as_deref()
        .filter(|v| !v.is_empty())
        .map(|v| format!("\nVenue: {v}"))
        .unwrap_or_default();

    let content = format!(
        "{}\n\nAuthors: {}\nFields: {}{}",
        paper.abstract_text,
        paper.authors.join(", "),
        paper.fields_of_study.join(", "),
        venue_str,
    );

    let mut entry = Entry::new(source.id.clone(), content);
    entry.summary = Some(paper.abstract_text.clone());
    entry.tags = paper.fields_of_study.clone();

    (source, entry)
}

fn build_query(profile: &Profile) -> String {
    // S2 search uses natural language queries — join keywords with spaces
    let positive = profile.keywords.join(" ");

    // S2 doesn't have explicit ANDNOT, but negative keywords can be
    // prepended with "-" for basic exclusion
    if profile.negative_keywords.is_empty() {
        positive
    } else {
        let negatives: Vec<String> = profile
            .negative_keywords
            .iter()
            .map(|nk| format!("-{nk}"))
            .collect();
        format!("{positive} {}", negatives.join(" "))
    }
}

fn convert_result(r: S2SearchResult) -> Option<S2Paper> {
    let paper_id = r.paper_id.filter(|s| !s.is_empty())?;
    let title = r.title.filter(|s| !s.is_empty())?;

    let publication_date = r.publication_date.and_then(|d| {
        NaiveDate::parse_from_str(&d, "%Y-%m-%d")
            .ok()
            .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap().and_utc())
    });

    let external_ids = r
        .external_ids
        .map(|ext| S2ExternalIds {
            arxiv_id: ext.arxiv,
            doi: ext.doi,
            corpus_id: ext.corpus_id.and_then(|v| match v {
                serde_json::Value::Number(n) => Some(n.to_string()),
                serde_json::Value::String(s) => Some(s),
                _ => None,
            }),
        })
        .unwrap_or_default();

    let url = r
        .url
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| format!("https://www.semanticscholar.org/paper/{paper_id}"));

    Some(S2Paper {
        paper_id,
        title,
        abstract_text: r.abstract_text.unwrap_or_default(),
        authors: r.authors.into_iter().filter_map(|a| a.name).collect(),
        year: r.year,
        venue: r.venue.filter(|v| !v.is_empty()),
        citation_count: r.citation_count,
        fields_of_study: r.fields_of_study.unwrap_or_default(),
        publication_date,
        external_ids,
        url,
    })
}

fn urlencoded(s: &str) -> String {
    s.replace(' ', "%20")
        .replace('"', "%22")
        .replace('(', "%28")
        .replace(')', "%29")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_query_simple() {
        let profile = Profile::new(
            "AI".into(),
            vec!["machine learning".into(), "safety".into()],
        );
        let query = build_query(&profile);
        assert_eq!(query, "machine learning safety");
    }

    #[test]
    fn build_query_with_negative() {
        let mut profile = Profile::new("AI".into(), vec!["machine learning".into()]);
        profile.negative_keywords = vec!["biology".into()];
        let query = build_query(&profile);
        assert!(query.contains("-biology"));
        assert!(query.contains("machine learning"));
    }

    #[test]
    fn build_query_empty() {
        let profile = Profile::new("Empty".into(), vec![]);
        let query = build_query(&profile);
        assert!(query.is_empty());
    }

    #[test]
    fn convert_result_minimal() {
        let result = S2SearchResult {
            paper_id: Some("abc123".into()),
            title: Some("Test Paper".into()),
            abstract_text: Some("A test abstract.".into()),
            authors: vec![S2Author {
                name: Some("Author One".into()),
            }],
            year: Some(2024),
            venue: Some("NeurIPS".into()),
            citation_count: Some(42),
            fields_of_study: Some(vec!["Computer Science".into()]),
            publication_date: Some("2024-01-15".into()),
            external_ids: Some(S2RawExternalIds {
                arxiv: Some("2401.12345".into()),
                doi: None,
                corpus_id: None,
            }),
            url: Some("https://semanticscholar.org/paper/abc123".into()),
        };

        let paper = convert_result(result).unwrap();
        assert_eq!(paper.paper_id, "abc123");
        assert_eq!(paper.title, "Test Paper");
        assert_eq!(paper.authors, vec!["Author One"]);
        assert_eq!(paper.external_ids.arxiv_id, Some("2401.12345".into()));
        assert!(paper.publication_date.is_some());
    }

    #[test]
    fn convert_result_missing_required() {
        let result = S2SearchResult {
            paper_id: None,
            title: Some("Test".into()),
            abstract_text: None,
            authors: vec![],
            year: None,
            venue: None,
            citation_count: None,
            fields_of_study: None,
            publication_date: None,
            external_ids: None,
            url: None,
        };
        assert!(convert_result(result).is_none());
    }

    #[test]
    fn paper_to_source_entry_conversion() {
        let paper = S2Paper {
            paper_id: "abc123".into(),
            title: "Test Paper".into(),
            abstract_text: "A test abstract.".into(),
            authors: vec!["Author One".into()],
            year: Some(2024),
            venue: Some("NeurIPS".into()),
            citation_count: Some(42),
            fields_of_study: vec!["Computer Science".into()],
            publication_date: Some(Utc::now()),
            external_ids: S2ExternalIds::default(),
            url: "https://semanticscholar.org/paper/abc123".into(),
        };

        let (source, entry) = paper_to_source_entry(&paper);
        assert_eq!(source.source_type, SourceType::Paper);
        assert_eq!(entry.source_id, source.id);
        assert!(entry.content.contains("A test abstract."));
        assert!(entry.content.contains("NeurIPS"));
        assert_eq!(entry.tags, vec!["Computer Science"]);
    }
}
