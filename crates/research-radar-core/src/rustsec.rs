//! RustSec source adapter — fetch security advisories from the RustSec feed.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use crate::{Entry, Profile, Source, SourceType};

/// A security advisory fetched from the RustSec advisory database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RustSecAdvisory {
    pub advisory_id: String,
    pub package: String,
    pub title: String,
    pub description: String,
    pub url: String,
    pub patched_versions: Vec<String>,
    pub unaffected_versions: Vec<String>,
    pub cvss: Option<String>,
    pub aliases: Vec<String>,
    pub categories: Vec<String>,
    pub published: Option<DateTime<Utc>>,
}

#[derive(Debug, thiserror::Error)]
pub enum RustSecError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("rate limited")]
    RateLimited,
}

#[derive(Debug, Deserialize)]
struct RustSecFeed {
    #[serde(default)]
    advisories: Vec<RustSecRawEntry>,
}

#[derive(Debug, Deserialize)]
struct RustSecRawEntry {
    #[serde(default)]
    advisory: RustSecRawAdvisory,
    versions: Option<RustSecRawVersions>,
}

#[derive(Debug, Default, Deserialize)]
struct RustSecRawAdvisory {
    id: Option<String>,
    package: Option<String>,
    title: Option<String>,
    description: Option<String>,
    date: Option<String>,
    url: Option<String>,
    #[serde(default)]
    categories: Vec<String>,
    #[serde(default)]
    aliases: Vec<String>,
    cvss: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct RustSecRawVersions {
    #[serde(default)]
    patched: Vec<String>,
    #[serde(default)]
    unaffected: Vec<String>,
}

const RUSTSEC_FEED_URL: &str = "https://rustsec.org/advisories.json";

pub async fn fetch_rustsec_advisories(
    profile: &Profile,
    max_results: usize,
) -> Result<Vec<RustSecAdvisory>, RustSecError> {
    let resp = reqwest::Client::new()
        .get(RUSTSEC_FEED_URL)
        .header("User-Agent", "research-radar/0.1")
        .send()
        .await
        .map_err(|e| RustSecError::Http(e.to_string()))?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return Err(RustSecError::RateLimited);
    }

    if !resp.status().is_success() {
        return Err(RustSecError::Http(format!(
            "RustSec returned {}",
            resp.status()
        )));
    }

    let body: RustSecFeed = resp
        .json()
        .await
        .map_err(|e| RustSecError::Parse(e.to_string()))?;

    let mut advisories: Vec<RustSecAdvisory> = body
        .advisories
        .into_iter()
        .filter_map(convert_entry)
        .filter(|adv| matches_profile(adv, profile))
        .collect();

    advisories.truncate(max_results);
    Ok(advisories)
}

pub fn advisory_to_source_entry(adv: &RustSecAdvisory) -> (Source, Entry) {
    let title = if adv.title.is_empty() {
        adv.advisory_id.clone()
    } else {
        adv.title.clone()
    };

    let source = Source {
        id: uuid::Uuid::new_v4().to_string(),
        url: adv.url.clone(),
        title,
        source_type: SourceType::Web,
        added_at: Utc::now(),
    };

    let content = format!(
        "{}\n\nPackage: {}\nPatched: {}\nAliases: {}\nCategories: {}",
        adv.description,
        adv.package,
        adv.patched_versions.join(", "),
        adv.aliases.join(", "),
        adv.categories.join(", "),
    );

    let mut entry = Entry::new(source.id.clone(), content);
    entry.summary = Some(if adv.description.is_empty() {
        source.title.clone()
    } else {
        adv.description.clone()
    });
    entry.tags = vec!["security".into(), "cve".into(), "advisory".into()];
    entry.tags.extend(adv.categories.clone());

    (source, entry)
}

fn convert_entry(raw: RustSecRawEntry) -> Option<RustSecAdvisory> {
    let advisory_id = raw.advisory.id.filter(|s| !s.is_empty())?;
    let package = raw.advisory.package.filter(|s| !s.is_empty())?;
    let versions = raw.versions.unwrap_or_default();

    let published = raw.advisory.date.and_then(|d| {
        NaiveDate::parse_from_str(&d, "%Y-%m-%d")
            .ok()
            .map(|nd| nd.and_hms_opt(0, 0, 0).unwrap().and_utc())
    });

    let url = raw
        .advisory
        .url
        .filter(|u| !u.is_empty())
        .unwrap_or_else(|| format!("https://rustsec.org/advisories/{advisory_id}.html"));

    Some(RustSecAdvisory {
        advisory_id,
        package,
        title: raw.advisory.title.unwrap_or_default(),
        description: raw.advisory.description.unwrap_or_default(),
        url,
        patched_versions: versions.patched,
        unaffected_versions: versions.unaffected,
        cvss: raw.advisory.cvss,
        aliases: raw.advisory.aliases,
        categories: raw.advisory.categories,
        published,
    })
}

fn matches_profile(adv: &RustSecAdvisory, profile: &Profile) -> bool {
    if profile.keywords.is_empty() {
        return true;
    }

    let haystack = format!(
        "{} {} {} {}",
        adv.title,
        adv.description,
        adv.package,
        adv.categories.join(" "),
    )
    .to_lowercase();

    profile
        .keywords
        .iter()
        .map(|keyword| keyword.to_lowercase())
        .any(|keyword| haystack.contains(&keyword))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FEED_JSON: &str = r#"{
        "advisories": [
            {
                "advisory": {
                    "id": "RUSTSEC-2024-0001",
                    "package": "some-crate",
                    "title": "Code execution in some-crate",
                    "description": "A test vulnerability.",
                    "date": "2024-01-15",
                    "url": "https://rustsec.org/advisories/RUSTSEC-2024-0001.html",
                    "categories": ["code-execution"],
                    "aliases": ["CVE-2024-1234"],
                    "cvss": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H"
                },
                "versions": { "patched": [">=1.2.3"], "unaffected": ["<1.0.0"] }
            },
            {
                "advisory": {
                    "id": "RUSTSEC-2024-0002",
                    "package": "memory-crate",
                    "title": "Memory corruption",
                    "description": "A second test vulnerability.",
                    "date": "2024-02-20",
                    "categories": ["memory-corruption"],
                    "aliases": ["GHSA-abcd-efgh-ijkl"]
                },
                "versions": { "patched": [">=2.0.0"], "unaffected": [] }
            }
        ]
    }"#;

    #[test]
    fn parse_feed_basic() {
        let feed: RustSecFeed = serde_json::from_str(FEED_JSON).unwrap();
        let advisories: Vec<RustSecAdvisory> = feed
            .advisories
            .into_iter()
            .filter_map(convert_entry)
            .collect();

        assert_eq!(advisories.len(), 2);
        assert_eq!(advisories[0].advisory_id, "RUSTSEC-2024-0001");
        assert_eq!(advisories[0].package, "some-crate");
        assert!(advisories[0].aliases.contains(&"CVE-2024-1234".into()));
        assert!(advisories[0].categories.contains(&"code-execution".into()));
    }

    #[test]
    fn convert_entry_missing_required() {
        let raw = RustSecRawEntry {
            advisory: RustSecRawAdvisory {
                id: Some(String::new()),
                package: Some("some-crate".into()),
                ..Default::default()
            },
            versions: None,
        };

        assert!(convert_entry(raw).is_none());
    }

    #[test]
    fn advisory_to_source_entry_conversion() {
        let adv = RustSecAdvisory {
            advisory_id: "RUSTSEC-2024-0001".into(),
            package: "some-crate".into(),
            title: "Code execution in some-crate".into(),
            description: "A test vulnerability.".into(),
            url: "https://rustsec.org/advisories/RUSTSEC-2024-0001.html".into(),
            patched_versions: vec![">=1.2.3".into()],
            unaffected_versions: vec!["<1.0.0".into()],
            cvss: None,
            aliases: vec!["CVE-2024-1234".into()],
            categories: vec!["code-execution".into()],
            published: Some(Utc::now()),
        };

        let (source, entry) = advisory_to_source_entry(&adv);
        assert_eq!(source.source_type, SourceType::Web);
        assert!(entry.tags.contains(&"security".into()));
        assert!(entry.tags.contains(&"cve".into()));
        assert!(entry.content.contains("some-crate"));
        assert!(entry.content.contains("CVE-2024-1234"));
    }

    #[test]
    fn keyword_filter_applies() {
        let feed: RustSecFeed = serde_json::from_str(FEED_JSON).unwrap();
        let advisories: Vec<RustSecAdvisory> = feed
            .advisories
            .into_iter()
            .filter_map(convert_entry)
            .collect();

        let profile = Profile::new("Security".into(), vec!["memory".into()]);
        let matches: Vec<&RustSecAdvisory> = advisories
            .iter()
            .filter(|adv| matches_profile(adv, &profile))
            .collect();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].advisory_id, "RUSTSEC-2024-0002");

        let empty_profile = Profile::new("All".into(), vec![]);
        let all: Vec<&RustSecAdvisory> = advisories
            .iter()
            .filter(|adv| matches_profile(adv, &empty_profile))
            .collect();
        assert_eq!(all.len(), 2);
    }
}
