//! OpenAlex source adapter — fetch works from the OpenAlex API.
//!
//! OpenAlex is a free, open catalog of ~250M scholarly works. The API requires
//! no authentication; providing a `mailto` parameter enters the polite pool
//! with higher rate limits (10 req/s vs 1 req/s).
//!
//! Docs: https://docs.openalex.org

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{Entry, Profile, Source, SourceType};

/// A work fetched from OpenAlex.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OaWork {
    pub openalex_id: String,
    pub doi: Option<String>,
    pub title: String,
    pub abstract_text: String,
    pub authors: Vec<String>,
    pub publication_date: Option<DateTime<Utc>>,
    pub cited_by_count: u32,
    pub concepts: Vec<String>,
    pub source_name: Option<String>,
    pub work_type: Option<String>,
    pub arxiv_id: Option<String>,
    pub url: String,
}

/// Errors from OpenAlex fetching.
#[derive(Debug, thiserror::Error)]
pub enum OaError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("rate limited")]
    RateLimited,
}

// ─── API response types ────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct OaSearchResponse {
    #[serde(default)]
    results: Vec<OaRawWork>,
}

#[derive(Debug, Deserialize)]
struct OaRawWork {
    id: Option<String>,
    doi: Option<String>,
    title: Option<String>,
    #[serde(rename = "abstract_inverted_index")]
    abstract_inverted_index: Option<serde_json::Value>,
    #[serde(default)]
    authorships: Vec<OaAuthorship>,
    publication_date: Option<String>,
    cited_by_count: Option<u32>,
    #[serde(default)]
    concepts: Vec<OaConcept>,
    primary_location: Option<OaLocation>,
    #[serde(rename = "type")]
    work_type: Option<String>,
    ids: Option<OaIds>,
}

#[derive(Debug, Deserialize)]
struct OaAuthorship {
    author: Option<OaAuthor>,
}

#[derive(Debug, Deserialize)]
struct OaAuthor {
    display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OaConcept {
    display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OaLocation {
    source: Option<OaSource>,
}

#[derive(Debug, Deserialize)]
struct OaSource {
    display_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OaIds {
    openalex: Option<String>,
    doi: Option<String>,
}

const OA_API_BASE: &str = "https://api.openalex.org";

/// Fetch works from OpenAlex matching the profile's keywords.
///
/// Returns up to `max_results` works. Uses the /works search endpoint
/// with relevance sorting.
pub async fn fetch_oa_works(
    profile: &Profile,
    max_results: usize,
) -> Result<Vec<OaWork>, OaError> {
    if profile.keywords.is_empty() {
        return Ok(Vec::new());
    }

    let query = build_query(profile);
    if query.is_empty() {
        return Ok(Vec::new());
    }

    let limit = max_results.min(50); // OpenAlex per_page max is 200, keep conservative
    let mut url = format!(
        "{}/works?search={}&per_page={}&sort=relevance_score:desc&filter=has_abstract:true",
        OA_API_BASE,
        urlencoded(&query),
        limit,
    );

    // Enter the polite pool if we have a contact email
    if let Ok(email) = std::env::var("OPENALEX_EMAIL") {
        if !email.is_empty() {
            url.push_str(&format!("&mailto={}", urlencoded(&email)));
        }
    }

    let resp = reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "research-radar/0.1")
        .send()
        .await
        .map_err(|e| OaError::Http(e.to_string()))?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(OaError::RateLimited);
    }

    if !resp.status().is_success() {
        return Err(OaError::Http(format!(
            "OpenAlex returned {}",
            resp.status()
        )));
    }

    let body: OaSearchResponse = resp
        .json()
        .await
        .map_err(|e| OaError::Parse(e.to_string()))?;

    let works = body
        .results
        .into_iter()
        .filter_map(convert_result)
        .collect();

    Ok(works)
}

/// Convert an OaWork into a Source + Entry pair for storage.
pub fn work_to_source_entry(work: &OaWork) -> (Source, Entry) {
    let source = Source {
        id: uuid::Uuid::new_v4().to_string(),
        url: work.url.clone(),
        title: work.title.clone(),
        source_type: SourceType::Paper,
        added_at: Utc::now(),
    };

    let venue_str = work
        .source_name
        .as_deref()
        .filter(|v| !v.is_empty())
        .map(|v| format!("\nVenue: {v}"))
        .unwrap_or_default();

    let content = format!(
        "{}\n\nAuthors: {}\nTopics: {}{}",
        work.abstract_text,
        work.authors.join(", "),
        work.concepts.join(", "),
        venue_str,
    );

    let mut entry = Entry::new(source.id.clone(), content);
    entry.summary = Some(work.abstract_text.clone());
    entry.tags = work.concepts.clone();

    (source, entry)
}

fn build_query(profile: &Profile) -> String {
    // OpenAlex search is natural-language. Join keywords with spaces.
    let positive = profile.keywords.join(" ");
    // OpenAlex doesn't support negation in search, so negative keywords
    // are handled later at the filtering/ranking stage.
    positive
}

/// Reconstruct abstract text from OpenAlex's inverted index format.
///
/// OpenAlex stores abstracts as `{"word": [pos1, pos2], ...}`. We reconstruct
/// the original text by inverting back to position order.
fn reconstruct_abstract(inverted_index: &serde_json::Value) -> Option<String> {
    let obj = inverted_index.as_object()?;
    if obj.is_empty() {
        return None;
    }

    let mut words: Vec<(usize, &str)> = Vec::new();
    for (word, positions) in obj {
        if let Some(arr) = positions.as_array() {
            for pos in arr {
                if let Some(idx) = pos.as_u64() {
                    words.push((idx as usize, word.as_str()));
                }
            }
        }
    }
    words.sort_by_key(|(idx, _)| *idx);

    if words.is_empty() {
        return None;
    }

    let text: Vec<&str> = words.into_iter().map(|(_, w)| w).collect();
    Some(text.join(" "))
}

fn convert_result(r: OaRawWork) -> Option<OaWork> {
    let openalex_id = r.id.filter(|s| !s.is_empty())?;
    let title = r.title.filter(|s| !s.is_empty())?;

    let abstract_text = r
        .abstract_inverted_index
        .as_ref()
        .and_then(reconstruct_abstract)
        .unwrap_or_default();

    let publication_date = r.publication_date.and_then(|d| {
        NaiveDate::parse_from_str(&d, "%Y-%m-%d")
            .ok()
            .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap().and_utc())
    });

    // Extract arXiv ID from DOI if it's an arXiv DOI, or from the OpenAlex IDs
    let doi_raw = r.doi.or_else(|| r.ids.as_ref().and_then(|ids| ids.doi.clone()));
    let doi = doi_raw.as_deref().map(normalize_doi).map(String::from);
    let arxiv_id = doi
        .as_deref()
        .and_then(extract_arxiv_from_doi)
        .map(String::from);

    let url = doi
        .as_ref()
        .map(|d| format!("https://doi.org/{d}"))
        .unwrap_or_else(|| openalex_id.clone());

    let source_name = r
        .primary_location
        .and_then(|loc| loc.source)
        .and_then(|s| s.display_name);

    Some(OaWork {
        openalex_id,
        doi,
        title,
        abstract_text,
        authors: r
            .authorships
            .into_iter()
            .filter_map(|a| a.author.and_then(|au| au.display_name))
            .collect(),
        publication_date,
        cited_by_count: r.cited_by_count.unwrap_or(0),
        concepts: r
            .concepts
            .into_iter()
            .filter_map(|c| c.display_name)
            .take(5) // Keep top 5 concepts to avoid noise
            .collect(),
        source_name,
        work_type: r.work_type,
        arxiv_id,
        url,
    })
}

/// Normalize DOI by stripping the https://doi.org/ prefix if present.
fn normalize_doi(doi: &str) -> &str {
    doi.strip_prefix("https://doi.org/")
        .or_else(|| doi.strip_prefix("http://doi.org/"))
        .unwrap_or(doi)
}

/// Extract arXiv ID from a DOI like "10.48550/arxiv.2401.12345".
fn extract_arxiv_from_doi(doi: &str) -> Option<&str> {
    let lower = doi.to_lowercase();
    if lower.starts_with("10.48550/arxiv.") {
        Some(&doi["10.48550/arxiv.".len()..])
    } else {
        None
    }
}

fn urlencoded(s: &str) -> String {
    s.replace(' ', "%20")
        .replace('"', "%22")
        .replace('(', "%28")
        .replace(')', "%29")
        .replace('&', "%26")
        .replace('@', "%40")
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
    fn build_query_empty() {
        let profile = Profile::new("Empty".into(), vec![]);
        let query = build_query(&profile);
        assert!(query.is_empty());
    }

    #[test]
    fn reconstruct_abstract_from_inverted_index() {
        let index = serde_json::json!({
            "The": [0],
            "quick": [1],
            "brown": [2],
            "fox": [3]
        });
        let text = reconstruct_abstract(&index).unwrap();
        assert_eq!(text, "The quick brown fox");
    }

    #[test]
    fn reconstruct_abstract_repeated_words() {
        let index = serde_json::json!({
            "the": [0, 4],
            "cat": [1, 5],
            "sat": [2],
            "on": [3]
        });
        let text = reconstruct_abstract(&index).unwrap();
        assert_eq!(text, "the cat sat on the cat");
    }

    #[test]
    fn reconstruct_abstract_empty() {
        let index = serde_json::json!({});
        assert!(reconstruct_abstract(&index).is_none());
    }

    #[test]
    fn reconstruct_abstract_null() {
        let index = serde_json::Value::Null;
        assert!(reconstruct_abstract(&index).is_none());
    }

    #[test]
    fn normalize_doi_strips_prefix() {
        assert_eq!(normalize_doi("https://doi.org/10.1234/test"), "10.1234/test");
        assert_eq!(normalize_doi("http://doi.org/10.1234/test"), "10.1234/test");
        assert_eq!(normalize_doi("10.1234/test"), "10.1234/test");
    }

    #[test]
    fn extract_arxiv_id_from_doi() {
        assert_eq!(
            extract_arxiv_from_doi("10.48550/arxiv.2401.12345"),
            Some("2401.12345")
        );
        assert_eq!(extract_arxiv_from_doi("10.1234/other"), None);
    }

    #[test]
    fn convert_result_minimal() {
        let result = OaRawWork {
            id: Some("https://openalex.org/W123".into()),
            doi: Some("https://doi.org/10.1234/test".into()),
            title: Some("Test Paper".into()),
            abstract_inverted_index: Some(serde_json::json!({
                "A": [0],
                "test": [1],
                "abstract.": [2]
            })),
            authorships: vec![OaAuthorship {
                author: Some(OaAuthor {
                    display_name: Some("Author One".into()),
                }),
            }],
            publication_date: Some("2024-01-15".into()),
            cited_by_count: Some(42),
            concepts: vec![OaConcept {
                display_name: Some("Computer Science".into()),
            }],
            primary_location: Some(OaLocation {
                source: Some(OaSource {
                    display_name: Some("NeurIPS".into()),
                }),
            }),
            work_type: Some("article".into()),
            ids: None,
        };

        let work = convert_result(result).unwrap();
        assert_eq!(work.openalex_id, "https://openalex.org/W123");
        assert_eq!(work.doi, Some("10.1234/test".into()));
        assert_eq!(work.title, "Test Paper");
        assert_eq!(work.abstract_text, "A test abstract.");
        assert_eq!(work.authors, vec!["Author One"]);
        assert_eq!(work.cited_by_count, 42);
        assert_eq!(work.concepts, vec!["Computer Science"]);
        assert_eq!(work.source_name, Some("NeurIPS".into()));
        assert!(work.publication_date.is_some());
    }

    #[test]
    fn convert_result_missing_required() {
        let result = OaRawWork {
            id: None,
            doi: None,
            title: Some("Test".into()),
            abstract_inverted_index: None,
            authorships: vec![],
            publication_date: None,
            cited_by_count: None,
            concepts: vec![],
            primary_location: None,
            work_type: None,
            ids: None,
        };
        assert!(convert_result(result).is_none());
    }

    #[test]
    fn convert_result_arxiv_doi() {
        let result = OaRawWork {
            id: Some("https://openalex.org/W456".into()),
            doi: Some("https://doi.org/10.48550/arxiv.2401.99999".into()),
            title: Some("ArXiv Paper".into()),
            abstract_inverted_index: Some(serde_json::json!({"Hello": [0]})),
            authorships: vec![],
            publication_date: None,
            cited_by_count: None,
            concepts: vec![],
            primary_location: None,
            work_type: None,
            ids: None,
        };

        let work = convert_result(result).unwrap();
        assert_eq!(work.arxiv_id, Some("2401.99999".into()));
    }

    #[test]
    fn work_to_source_entry_conversion() {
        let work = OaWork {
            openalex_id: "https://openalex.org/W123".into(),
            doi: Some("10.1234/test".into()),
            title: "Test Paper".into(),
            abstract_text: "A test abstract.".into(),
            authors: vec!["Author One".into()],
            publication_date: Some(Utc::now()),
            cited_by_count: 42,
            concepts: vec!["Computer Science".into()],
            source_name: Some("NeurIPS".into()),
            work_type: Some("article".into()),
            arxiv_id: None,
            url: "https://doi.org/10.1234/test".into(),
        };

        let (source, entry) = work_to_source_entry(&work);
        assert_eq!(source.source_type, SourceType::Paper);
        assert_eq!(entry.source_id, source.id);
        assert!(entry.content.contains("A test abstract."));
        assert!(entry.content.contains("NeurIPS"));
        assert_eq!(entry.tags, vec!["Computer Science"]);
    }
}
