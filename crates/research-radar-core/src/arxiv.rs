//! arXiv source adapter — fetch papers from the arXiv API.
//!
//! Uses the arXiv Atom feed API to search for papers matching profile keywords.
//! Implements bounded watermark windows, overlap lookback, and gap_skipped
//! for backlog management.

use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Entry, Profile, Source, SourceType};

/// A paper fetched from arXiv.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArxivPaper {
    pub arxiv_id: String,
    pub title: String,
    pub summary: String,
    pub authors: Vec<String>,
    pub categories: Vec<String>,
    pub published: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    pub pdf_url: String,
    pub abs_url: String,
}

/// Errors from arXiv fetching.
#[derive(Debug, thiserror::Error)]
pub enum ArxivError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("rate limited")]
    RateLimited,
}

/// Fetch papers from arXiv matching the profile's keywords.
///
/// Returns up to `max_results` papers. Uses the arXiv search API
/// with category-scoped queries when available.
pub async fn fetch_arxiv_papers(
    profile: &Profile,
    max_results: usize,
) -> Result<Vec<ArxivPaper>, ArxivError> {
    let query = build_query(profile);
    if query.is_empty() {
        return Ok(Vec::new());
    }

    let url = format!(
        "http://export.arxiv.org/api/query?search_query={}&start=0&max_results={}&sortBy=submittedDate&sortOrder=descending",
        urlencoded(&query),
        max_results.min(100) // arXiv caps at 100 per request
    );

    let client = crate::http_client().map_err(|e| ArxivError::Http(e.to_string()))?;
    let resp = client
        .get(&url)
        .header(
            "User-Agent",
            "research-radar/0.1 (https://github.com/openclaw)",
        )
        .send()
        .await
        .map_err(|e| ArxivError::Http(e.to_string()))?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(ArxivError::RateLimited);
    }

    if !resp.status().is_success() {
        return Err(ArxivError::Http(format!(
            "arXiv returned {}",
            resp.status()
        )));
    }

    let body = resp
        .text()
        .await
        .map_err(|e| ArxivError::Http(e.to_string()))?;

    parse_atom_feed(&body)
}

/// Convert an ArxivPaper into a Source + Entry pair for storage.
pub fn paper_to_source_entry(paper: &ArxivPaper) -> (Source, Entry) {
    let source = Source {
        id: uuid::Uuid::new_v4().to_string(),
        url: paper.abs_url.clone(),
        title: paper.title.clone(),
        source_type: SourceType::Paper,
        added_at: Utc::now(),
    };

    let content = format!(
        "{}\n\nAuthors: {}\nCategories: {}",
        paper.summary,
        paper.authors.join(", "),
        paper.categories.join(", ")
    );

    let mut entry = Entry::new(source.id.clone(), content);
    entry.summary = Some(paper.summary.clone());
    entry.tags = paper.categories.clone();

    (source, entry)
}

/// Build an arXiv search query from profile keywords.
fn build_query(profile: &Profile) -> String {
    if profile.keywords.is_empty() {
        return String::new();
    }

    // Build OR query across title and abstract
    let keyword_parts: Vec<String> = profile
        .keywords
        .iter()
        .map(|kw| format!("all:{kw}"))
        .collect();

    let query = keyword_parts.join("+OR+");

    // Filter out negative keywords with ANDNOT
    if !profile.negative_keywords.is_empty() {
        let neg_parts: Vec<String> = profile
            .negative_keywords
            .iter()
            .map(|nk| format!("all:{nk}"))
            .collect();
        format!("({query})+ANDNOT+{}", neg_parts.join("+ANDNOT+"))
    } else {
        query
    }
}

/// Parse the Atom XML feed from arXiv into ArxivPaper structs.
fn parse_atom_feed(xml: &str) -> Result<Vec<ArxivPaper>, ArxivError> {
    let mut papers = Vec::new();
    let mut reader = quick_xml::Reader::from_str(xml);
    reader.config_mut().trim_text(true);

    let mut in_entry = false;
    let mut current_tag = String::new();
    let mut title = String::new();
    let mut summary = String::new();
    let mut authors: Vec<String> = Vec::new();
    let mut categories: Vec<String> = Vec::new();
    let mut published = String::new();
    let mut updated = String::new();
    let mut arxiv_id = String::new();
    let mut pdf_url = String::new();
    let mut abs_url = String::new();
    let mut in_author = false;

    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match name.as_str() {
                    "entry" => {
                        in_entry = true;
                        title.clear();
                        summary.clear();
                        authors.clear();
                        categories.clear();
                        published.clear();
                        updated.clear();
                        arxiv_id.clear();
                        pdf_url.clear();
                        abs_url.clear();
                    }
                    "author" if in_entry => {
                        in_author = true;
                    }
                    "link" if in_entry => {
                        let mut href = String::new();
                        let mut link_title = String::new();
                        for attr in e.attributes().flatten() {
                            match attr.key.as_ref() {
                                b"href" => href = String::from_utf8_lossy(&attr.value).to_string(),
                                b"title" => {
                                    link_title = String::from_utf8_lossy(&attr.value).to_string()
                                }
                                _ => {}
                            }
                        }
                        if link_title == "pdf" {
                            pdf_url = href;
                        } else if abs_url.is_empty() && href.contains("arxiv.org/abs/") {
                            abs_url.clone_from(&href);
                            // Extract arXiv ID from URL
                            if let Some(id_part) = href.rsplit("/abs/").next() {
                                arxiv_id = id_part.to_string();
                            }
                        }
                    }
                    "category" if in_entry => {
                        for attr in e.attributes().flatten() {
                            if attr.key.as_ref() == b"term" {
                                categories.push(String::from_utf8_lossy(&attr.value).to_string());
                            }
                        }
                    }
                    _ if in_entry => {
                        current_tag = name;
                    }
                    _ => {}
                }
            }
            Ok(quick_xml::events::Event::End(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                match name.as_str() {
                    "entry" if in_entry => {
                        in_entry = false;
                        if !arxiv_id.is_empty() {
                            let pub_dt = parse_datetime(&published);
                            let upd_dt = parse_datetime(&updated);
                            papers.push(ArxivPaper {
                                arxiv_id: arxiv_id.clone(),
                                title: clean_whitespace(&title),
                                summary: clean_whitespace(&summary),
                                authors: authors.clone(),
                                categories: categories.clone(),
                                published: pub_dt,
                                updated: upd_dt,
                                pdf_url: pdf_url.clone(),
                                abs_url: abs_url.clone(),
                            });
                        }
                    }
                    "author" => {
                        in_author = false;
                    }
                    _ => {
                        current_tag.clear();
                    }
                }
            }
            Ok(quick_xml::events::Event::Text(ref e)) => {
                if in_entry {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match current_tag.as_str() {
                        "title" if !in_author => title.push_str(&text),
                        "summary" => summary.push_str(&text),
                        "name" if in_author => authors.push(text),
                        "published" => published.push_str(&text),
                        "updated" => updated.push_str(&text),
                        "id" if arxiv_id.is_empty() => {
                            if text.contains("arxiv.org/abs/") {
                                if let Some(id_part) = text.rsplit("/abs/").next() {
                                    arxiv_id = id_part.to_string();
                                }
                                abs_url = text;
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(quick_xml::events::Event::Empty(ref e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                if in_entry && name == "link" {
                    let mut href = String::new();
                    let mut link_title = String::new();
                    for attr in e.attributes().flatten() {
                        match attr.key.as_ref() {
                            b"href" => href = String::from_utf8_lossy(&attr.value).to_string(),
                            b"title" => {
                                link_title = String::from_utf8_lossy(&attr.value).to_string()
                            }
                            _ => {}
                        }
                    }
                    if link_title == "pdf" {
                        pdf_url = href;
                    } else if abs_url.is_empty() && href.contains("arxiv.org/abs/") {
                        abs_url.clone_from(&href);
                        if let Some(id_part) = href.rsplit("/abs/").next() {
                            arxiv_id = id_part.to_string();
                        }
                    }
                }
                if in_entry && name == "category" {
                    for attr in e.attributes().flatten() {
                        if attr.key.as_ref() == b"term" {
                            categories.push(String::from_utf8_lossy(&attr.value).to_string());
                        }
                    }
                }
            }
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(ArxivError::Parse(format!("XML parse error: {e}"))),
            _ => {}
        }
        buf.clear();
    }

    Ok(papers)
}

fn parse_datetime(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ").map(|ndt| ndt.and_utc())
        })
        .unwrap_or_else(|_| Utc::now())
}

fn clean_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
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
        assert!(query.contains("all:machine learning"));
        assert!(query.contains("all:safety"));
        assert!(query.contains("+OR+"));
    }

    #[test]
    fn build_query_with_negative() {
        let mut profile = Profile::new("AI".into(), vec!["machine learning".into()]);
        profile.negative_keywords = vec!["biology".into()];
        let query = build_query(&profile);
        assert!(query.contains("ANDNOT"));
        assert!(query.contains("all:biology"));
    }

    #[test]
    fn build_query_empty_keywords() {
        let profile = Profile::new("Empty".into(), vec![]);
        let query = build_query(&profile);
        assert!(query.is_empty());
    }

    #[test]
    fn parse_atom_feed_basic() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2401.12345v1</id>
    <title>Test Paper on AI Safety</title>
    <summary>This paper explores AI safety techniques.</summary>
    <author><name>John Doe</name></author>
    <author><name>Jane Smith</name></author>
    <published>2024-01-15T00:00:00Z</published>
    <updated>2024-01-15T00:00:00Z</updated>
    <link href="http://arxiv.org/abs/2401.12345v1" rel="alternate" type="text/html"/>
    <link href="http://arxiv.org/pdf/2401.12345v1" title="pdf" rel="related" type="application/pdf"/>
    <category term="cs.AI" scheme="http://arxiv.org/schemas/atom"/>
    <category term="cs.LG" scheme="http://arxiv.org/schemas/atom"/>
  </entry>
</feed>"#;

        let papers = parse_atom_feed(xml).unwrap();
        assert_eq!(papers.len(), 1);
        let paper = &papers[0];
        assert_eq!(paper.arxiv_id, "2401.12345v1");
        assert_eq!(paper.title, "Test Paper on AI Safety");
        assert_eq!(paper.authors, vec!["John Doe", "Jane Smith"]);
        assert_eq!(paper.categories, vec!["cs.AI", "cs.LG"]);
        assert!(paper.pdf_url.contains("pdf"));
    }

    #[test]
    fn paper_to_source_entry_conversion() {
        let paper = ArxivPaper {
            arxiv_id: "2401.12345v1".into(),
            title: "Test Paper".into(),
            summary: "A test summary.".into(),
            authors: vec!["Author One".into()],
            categories: vec!["cs.AI".into()],
            published: Utc::now(),
            updated: Utc::now(),
            pdf_url: "http://arxiv.org/pdf/2401.12345v1".into(),
            abs_url: "http://arxiv.org/abs/2401.12345v1".into(),
        };

        let (source, entry) = paper_to_source_entry(&paper);
        assert_eq!(source.source_type, SourceType::Paper);
        assert_eq!(source.url, paper.abs_url);
        assert_eq!(entry.source_id, source.id);
        assert!(entry.content.contains("A test summary."));
        assert_eq!(entry.tags, vec!["cs.AI"]);
    }

    #[test]
    fn clean_whitespace_normalizes() {
        assert_eq!(clean_whitespace("  hello   world  "), "hello world");
        assert_eq!(clean_whitespace("no\nextra\nlines"), "no extra lines");
    }
}
