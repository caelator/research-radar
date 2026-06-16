//! GitHub source adapter — fetch notable release signals from repositories.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Entry, Profile, Source, SourceType};

/// A notable GitHub signal (release/tag) for a repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubRelease {
    pub repo_full_name: String,
    pub tag_name: String,
    pub name: String,
    pub body: String,
    pub html_url: String,
    pub published_at: Option<DateTime<Utc>>,
    pub prerelease: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum GithubError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("rate limited")]
    RateLimited,
}

#[derive(Debug, Deserialize)]
struct GithubSearchResponse {
    #[serde(default)]
    items: Vec<GithubRepoItem>,
}

#[derive(Debug, Deserialize)]
struct GithubRepoItem {
    full_name: Option<String>,
    #[allow(dead_code)]
    releases_url: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GithubReleaseRaw {
    pub tag_name: Option<String>,
    pub name: Option<String>,
    pub body: Option<String>,
    pub html_url: Option<String>,
    pub published_at: Option<String>,
    pub prerelease: Option<bool>,
}

const GITHUB_API_BASE: &str = "https://api.github.com";

pub async fn fetch_github_releases(
    profile: &Profile,
    max_results: usize,
) -> Result<Vec<GithubRelease>, GithubError> {
    if profile.keywords.is_empty() {
        return Ok(Vec::new());
    }

    let query = profile.keywords.join("+");
    if query.is_empty() {
        return Ok(Vec::new());
    }

    let per_page = max_results.clamp(1, 100);
    let url = format!(
        "{}/search/repositories?q={}&sort=updated&per_page={}",
        GITHUB_API_BASE,
        urlencoded(&query),
        per_page,
    );

    let client = reqwest::Client::new();
    let body: GithubSearchResponse = github_get(client.get(&url))
        .await?
        .json()
        .await
        .map_err(|e| GithubError::Parse(e.to_string()))?;

    let mut releases = Vec::new();
    for repo in body.items {
        if releases.len() >= max_results {
            break;
        }

        let Some(full_name) = repo.full_name.filter(|s| !s.is_empty()) else {
            continue;
        };
        let release_url = format!(
            "{}/repos/{}/releases?per_page=1",
            GITHUB_API_BASE,
            urlencoded(&full_name)
        );
        let raw: Vec<GithubReleaseRaw> = github_get(client.get(&release_url))
            .await?
            .json()
            .await
            .map_err(|e| GithubError::Parse(e.to_string()))?;

        if let Some(rel) = raw
            .into_iter()
            .next()
            .and_then(|raw| convert_release(&full_name, raw))
        {
            releases.push(rel);
        }
    }

    Ok(releases)
}

pub fn convert_release(repo_full_name: &str, raw: GithubReleaseRaw) -> Option<GithubRelease> {
    let tag_name = raw.tag_name.filter(|s| !s.is_empty())?;
    let html_url = raw.html_url.filter(|s| !s.is_empty())?;
    let published_at = raw.published_at.and_then(|d| {
        DateTime::parse_from_rfc3339(&d)
            .ok()
            .map(|dt| dt.with_timezone(&Utc))
    });

    Some(GithubRelease {
        repo_full_name: repo_full_name.into(),
        tag_name,
        name: raw.name.unwrap_or_default(),
        body: raw.body.unwrap_or_default(),
        html_url,
        published_at,
        prerelease: raw.prerelease.unwrap_or(false),
    })
}

pub fn release_to_source_entry(rel: &GithubRelease) -> (Source, Entry) {
    let title = if rel.name.is_empty() {
        format!("{} {}", rel.repo_full_name, rel.tag_name)
    } else {
        rel.name.clone()
    };

    let source = Source {
        id: uuid::Uuid::new_v4().to_string(),
        url: rel.html_url.clone(),
        title,
        source_type: SourceType::Web,
        added_at: Utc::now(),
    };

    let content = format!(
        "{}\n\nRepo: {}\nTag: {}\nPrerelease: {}",
        rel.body, rel.repo_full_name, rel.tag_name, rel.prerelease
    );

    let mut entry = Entry::new(source.id.clone(), content);
    entry.summary = Some(if rel.name.is_empty() {
        rel.body.clone()
    } else {
        rel.name.clone()
    });
    entry.tags = vec!["github".into(), "release".into()];
    if rel.prerelease {
        entry.tags.push("prerelease".into());
    }

    (source, entry)
}

async fn github_get(req: reqwest::RequestBuilder) -> Result<reqwest::Response, GithubError> {
    let mut req = req
        .header("User-Agent", "research-radar/0.1")
        .header("Accept", "application/vnd.github+json");

    if let Ok(token) = std::env::var("GITHUB_TOKEN") {
        if !token.is_empty() {
            req = req.header("Authorization", format!("Bearer {token}"));
        }
    }

    let resp = req
        .send()
        .await
        .map_err(|e| GithubError::Http(e.to_string()))?;

    if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
        || resp.status() == reqwest::StatusCode::FORBIDDEN
    {
        return Err(GithubError::RateLimited);
    }

    if !resp.status().is_success() {
        return Err(GithubError::Http(format!(
            "GitHub returned {}",
            resp.status()
        )));
    }

    Ok(resp)
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
    fn convert_release_basic() {
        let raw: GithubReleaseRaw = serde_json::from_str(
            r#"{
                "tag_name": "v1.2.3",
                "name": "Release 1.2.3",
                "body": "Release notes",
                "html_url": "https://github.com/owner/name/releases/tag/v1.2.3",
                "published_at": "2026-06-15T12:34:56Z",
                "prerelease": false
            }"#,
        )
        .unwrap();

        let rel = convert_release("owner/name", raw).unwrap();
        assert_eq!(rel.tag_name, "v1.2.3");
        assert_eq!(rel.repo_full_name, "owner/name");
        assert!(rel.published_at.is_some());
        assert!(!rel.prerelease);
    }

    #[test]
    fn convert_release_missing_required() {
        let missing_tag = GithubReleaseRaw {
            tag_name: None,
            name: None,
            body: None,
            html_url: Some("https://github.com/owner/name/releases/tag/v1.2.3".into()),
            published_at: None,
            prerelease: None,
        };
        assert!(convert_release("owner/name", missing_tag).is_none());

        let missing_url = GithubReleaseRaw {
            tag_name: Some("v1.2.3".into()),
            name: None,
            body: None,
            html_url: Some(String::new()),
            published_at: None,
            prerelease: None,
        };
        assert!(convert_release("owner/name", missing_url).is_none());
    }

    #[test]
    fn parse_search_response() {
        let resp: GithubSearchResponse =
            serde_json::from_str(r#"{"items":[{"full_name":"a/b"},{"full_name":"c/d"}]}"#).unwrap();
        assert_eq!(resp.items.len(), 2);
        assert_eq!(resp.items[0].full_name, Some("a/b".into()));
    }

    #[test]
    fn release_to_source_entry_conversion() {
        let rel = GithubRelease {
            repo_full_name: "owner/name".into(),
            tag_name: "v1.2.3".into(),
            name: "Release 1.2.3".into(),
            body: "Release notes".into(),
            html_url: "https://github.com/owner/name/releases/tag/v1.2.3".into(),
            published_at: Some(Utc::now()),
            prerelease: false,
        };

        let (source, entry) = release_to_source_entry(&rel);
        assert_eq!(source.source_type, SourceType::Web);
        assert!(entry.tags.contains(&"github".into()));
        assert!(entry.content.contains("owner/name"));
        assert!(entry.content.contains("v1.2.3"));
    }

    #[test]
    fn prerelease_tag_present() {
        let rel = GithubRelease {
            repo_full_name: "owner/name".into(),
            tag_name: "v2.0.0-rc.1".into(),
            name: "Release candidate".into(),
            body: "Release notes".into(),
            html_url: "https://github.com/owner/name/releases/tag/v2.0.0-rc.1".into(),
            published_at: None,
            prerelease: true,
        };

        let (_, entry) = release_to_source_entry(&rel);
        assert!(entry.tags.contains(&"prerelease".into()));
    }
}
