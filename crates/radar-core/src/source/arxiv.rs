use chrono::{DateTime, NaiveDateTime, Utc};
use quick_xml::Reader;
use quick_xml::events::Event;
use reqwest::Client;
use tracing::{debug, warn};

use super::SourceAdapter;
use crate::error::{RadarError, Result};
use crate::types::{SourceCandidate, SourceType};

/// arXiv API source adapter. Fetches from the arXiv Atom API.
pub struct ArxivAdapter {
    client: Client,
}

impl ArxivAdapter {
    pub fn new() -> Self {
        Self {
            client: Client::builder()
                .user_agent("research-radar/0.1 (https://github.com/openclaw)")
                .build()
                .expect("failed to build HTTP client"),
        }
    }

    /// Build an arXiv API query URL.
    /// categories: e.g. ["cs.AI", "cs.CL", "cs.LG"]
    /// keywords: search terms to AND with categories
    fn build_url(
        &self,
        categories: &[String],
        keywords: &[String],
        max_results: u32,
        start: u32,
    ) -> String {
        // Build category filter: cat:cs.AI OR cat:cs.CL OR ...
        let cat_query = if categories.is_empty() {
            String::new()
        } else {
            let cats: Vec<String> = categories.iter().map(|c| format!("cat:{c}")).collect();
            format!("({})", cats.join("+OR+"))
        };

        // Build keyword filter for title+abstract: (ti:X OR abs:X) AND (ti:Y OR abs:Y) ...
        let kw_query = if keywords.is_empty() {
            String::new()
        } else {
            let parts: Vec<String> = keywords
                .iter()
                .map(|kw| {
                    let encoded = kw.replace(' ', "+");
                    format!("(ti:%22{encoded}%22+OR+abs:%22{encoded}%22)")
                })
                .collect();
            // OR the keywords together (any match is interesting)
            format!("({})", parts.join("+OR+"))
        };

        let search_query = match (cat_query.is_empty(), kw_query.is_empty()) {
            (true, true) => "all:electron".to_string(), // fallback
            (false, true) => cat_query,
            (true, false) => kw_query,
            (false, false) => format!("{cat_query}+AND+{kw_query}"),
        };

        format!(
            "http://export.arxiv.org/api/query?search_query={search_query}&start={start}&max_results={max_results}&sortBy=submittedDate&sortOrder=descending"
        )
    }
}

impl Default for ArxivAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceAdapter for ArxivAdapter {
    async fn fetch(
        &self,
        categories: &[String],
        _since: Option<DateTime<Utc>>,
        max_results: u32,
    ) -> Result<Vec<SourceCandidate>> {
        // For the initial implementation, we search by category.
        // Keywords are handled at a higher level (keyword gate), but we can
        // pass empty keywords here to get broad category results.
        let url = self.build_url(categories, &[], max_results, 0);
        debug!("arXiv fetch: {url}");

        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| RadarError::SourceTransient {
                source_name: "arxiv".into(),
                message: e.to_string(),
            })?;

        if !resp.status().is_success() {
            return Err(RadarError::SourceTransient {
                source_name: "arxiv".into(),
                message: format!("HTTP {}", resp.status()),
            });
        }

        let body = resp.text().await.map_err(|e| RadarError::SourceTransient {
            source_name: "arxiv".into(),
            message: e.to_string(),
        })?;

        parse_arxiv_atom(&body)
    }

    fn source_type(&self) -> &'static str {
        "arxiv"
    }
}

/// Parse arXiv Atom XML into SourceCandidates.
fn parse_arxiv_atom(xml: &str) -> Result<Vec<SourceCandidate>> {
    let mut reader = Reader::from_str(xml);
    let mut candidates = Vec::new();

    // State machine for parsing
    let mut in_entry = false;
    let mut current_tag = String::new();
    let mut title = String::new();
    let mut summary = String::new();
    let mut id_url = String::new();
    let mut published = String::new();
    let mut authors: Vec<String> = Vec::new();
    let mut in_author = false;
    let mut categories: Vec<String> = Vec::new();
    let mut links: Vec<String> = Vec::new();

    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) | Ok(Event::Empty(ref e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                if local == "entry" {
                    in_entry = true;
                    title.clear();
                    summary.clear();
                    id_url.clear();
                    published.clear();
                    authors.clear();
                    categories.clear();
                    links.clear();
                } else if in_entry {
                    current_tag = local.clone();
                    if local == "author" {
                        in_author = true;
                    }
                    if local == "category"
                        && let Some(term) = e
                            .attributes()
                            .filter_map(|a| a.ok())
                            .find(|a| String::from_utf8_lossy(a.key.as_ref()) == "term")
                    {
                        categories.push(String::from_utf8_lossy(&term.value).to_string());
                    }
                    if local == "link" {
                        let mut href = String::new();
                        let mut link_type = String::new();
                        for attr in e.attributes().filter_map(|a| a.ok()) {
                            let key = String::from_utf8_lossy(attr.key.as_ref()).to_string();
                            let val = String::from_utf8_lossy(&attr.value).to_string();
                            if key == "href" {
                                href = val;
                            } else if key == "type" {
                                link_type = val;
                            }
                        }
                        if link_type == "text/html" || link_type.is_empty() {
                            links.push(href);
                        }
                    }
                }
            }
            Ok(Event::Text(ref e)) => {
                if in_entry {
                    let text = e.unescape().unwrap_or_default().to_string();
                    match current_tag.as_str() {
                        "title" if !in_author => {
                            title.push_str(&text);
                        }
                        "summary" => {
                            summary.push_str(&text);
                        }
                        "id" if !in_author => {
                            id_url.push_str(&text);
                        }
                        "published" => {
                            published.push_str(&text);
                        }
                        "name" if in_author => {
                            authors.push(text.trim().to_string());
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::End(ref e)) => {
                let local = String::from_utf8_lossy(e.local_name().as_ref()).to_string();
                if local == "entry" {
                    in_entry = false;

                    // Extract arXiv ID from the URL
                    let arxiv_id = extract_arxiv_id(&id_url);
                    let canonical_id = format!("arxiv:{arxiv_id}");

                    // Clean up title (remove newlines)
                    let title_clean = title.split_whitespace().collect::<Vec<_>>().join(" ");

                    // Clean up abstract
                    let abstract_clean = summary.split_whitespace().collect::<Vec<_>>().join(" ");

                    let published_at = parse_arxiv_date(&published);

                    let url = if !links.is_empty() {
                        links[0].clone()
                    } else {
                        format!("https://arxiv.org/abs/{arxiv_id}")
                    };

                    let mut aliases = vec![("arxiv_id".to_string(), arxiv_id.clone())];
                    // Add category aliases
                    for cat in &categories {
                        aliases.push(("arxiv_category".to_string(), cat.clone()));
                    }

                    if !title_clean.is_empty() {
                        candidates.push(SourceCandidate {
                            canonical_id,
                            title: title_clean,
                            authors: if authors.is_empty() {
                                None
                            } else {
                                Some(authors.join(", "))
                            },
                            abstract_text: if abstract_clean.is_empty() {
                                None
                            } else {
                                Some(abstract_clean)
                            },
                            url,
                            published_at,
                            source_type: SourceType::Arxiv,
                            aliases,
                            raw_json: None,
                        });
                    }
                } else if local == "author" {
                    in_author = false;
                }
                current_tag.clear();
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                warn!("arXiv XML parse error: {e}");
                break;
            }
            _ => {}
        }
        buf.clear();
    }

    debug!("arXiv parsed {} entries", candidates.len());
    Ok(candidates)
}

/// Extract the arXiv paper ID from a URL like http://arxiv.org/abs/2401.12345v1
fn extract_arxiv_id(url: &str) -> String {
    url.rsplit('/')
        .next()
        .unwrap_or(url)
        .trim_end_matches(|c: char| c == 'v' || c.is_ascii_digit())
        .to_string()
        // If we stripped too much (e.g. the whole ID was digits), use the raw segment
        .chars()
        .next()
        .map(|_| {
            let segment = url.rsplit('/').next().unwrap_or(url);
            // Remove version suffix like v1, v2
            if let Some(idx) = segment.rfind('v')
                && segment[idx + 1..].chars().all(|c| c.is_ascii_digit())
                && idx > 0
            {
                return segment[..idx].to_string();
            }
            segment.to_string()
        })
        .unwrap_or_else(|| url.to_string())
}

fn parse_arxiv_date(s: &str) -> Option<DateTime<Utc>> {
    // arXiv dates: 2024-01-15T18:00:00Z
    DateTime::parse_from_rfc3339(s.trim())
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%dT%H:%M:%S")
                .ok()
                .map(|ndt| ndt.and_utc())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_arxiv_id() {
        assert_eq!(
            extract_arxiv_id("http://arxiv.org/abs/2401.12345v1"),
            "2401.12345"
        );
        assert_eq!(
            extract_arxiv_id("http://arxiv.org/abs/2401.12345"),
            "2401.12345"
        );
    }

    #[test]
    fn test_parse_atom_entry() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <entry>
    <id>http://arxiv.org/abs/2401.00001v1</id>
    <title>Test Paper on Transformers</title>
    <summary>This paper studies attention mechanisms in large language models.</summary>
    <published>2024-01-15T18:00:00Z</published>
    <author><name>Alice Smith</name></author>
    <author><name>Bob Jones</name></author>
    <category term="cs.CL" />
    <category term="cs.AI" />
    <link href="http://arxiv.org/abs/2401.00001v1" type="text/html" />
  </entry>
</feed>"#;

        let candidates = parse_arxiv_atom(xml).unwrap();
        assert_eq!(candidates.len(), 1);
        let c = &candidates[0];
        assert_eq!(c.canonical_id, "arxiv:2401.00001");
        assert_eq!(c.title, "Test Paper on Transformers");
        assert!(
            c.abstract_text
                .as_ref()
                .unwrap()
                .contains("attention mechanisms")
        );
        assert_eq!(c.authors.as_ref().unwrap(), "Alice Smith, Bob Jones");
        assert!(c.published_at.is_some());
        assert_eq!(c.aliases.len(), 3); // arxiv_id + 2 categories
    }
}
