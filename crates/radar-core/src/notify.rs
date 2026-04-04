use crate::error::{RadarError, Result};
use crate::types::{Item, ItemScore};

/// Maximum embeds per Discord webhook request.
const MAX_EMBEDS_PER_REQUEST: usize = 10;

/// Maximum characters for embed description.
const MAX_EMBED_DESC: usize = 4096;

/// Maximum characters for embed field value.
const MAX_FIELD_VALUE: usize = 1024;

/// Discord webhook notifier with batching and truncation.
pub struct DiscordNotifier {
    client: reqwest::Client,
}

#[async_trait::async_trait]
pub trait NotificationBackend: Send + Sync {
    async fn send_matches(
        &self,
        destination: &str,
        profile_name: &str,
        matches: &[(ItemScore, Item)],
    ) -> Result<usize>;
}

impl DiscordNotifier {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    /// Send scored matches to a Discord webhook. Batches into max 10 embeds per request.
    /// Returns the number of embeds sent.
    pub async fn send_matches(
        &self,
        webhook_url: &str,
        profile_name: &str,
        matches: &[(ItemScore, Item)],
    ) -> Result<usize> {
        if matches.is_empty() {
            return Ok(0);
        }

        let embeds: Vec<serde_json::Value> = matches
            .iter()
            .map(|(score, item)| build_embed(profile_name, score, item))
            .collect();

        let mut sent = 0;
        for chunk in embeds.chunks(MAX_EMBEDS_PER_REQUEST) {
            let payload = serde_json::json!({
                "content": if sent == 0 {
                    format!("**research.radar** — {} new matches for **{}**", matches.len(), profile_name)
                } else {
                    String::new()
                },
                "embeds": chunk,
            });

            let resp = self
                .client
                .post(webhook_url)
                .json(&payload)
                .send()
                .await
                .map_err(|e| RadarError::SourceTransient {
                    source_name: "discord".into(),
                    message: e.to_string(),
                })?;

            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                let retry_after = resp
                    .headers()
                    .get("retry-after")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(5);
                tokio::time::sleep(std::time::Duration::from_secs(retry_after)).await;
                // Retry once
                self.client
                    .post(webhook_url)
                    .json(&payload)
                    .send()
                    .await
                    .map_err(|e| RadarError::SourceTransient {
                        source_name: "discord".into(),
                        message: e.to_string(),
                    })?;
            } else if !resp.status().is_success() {
                let text = resp.text().await.unwrap_or_default();
                return Err(RadarError::SourceTransient {
                    source_name: "discord".into(),
                    message: format!("webhook failed: {text}"),
                });
            }

            sent += chunk.len();
        }

        Ok(sent)
    }
}

impl Default for DiscordNotifier {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl NotificationBackend for DiscordNotifier {
    async fn send_matches(
        &self,
        destination: &str,
        profile_name: &str,
        matches: &[(ItemScore, Item)],
    ) -> Result<usize> {
        self.send_matches(destination, profile_name, matches).await
    }
}

#[derive(Clone, Default)]
pub struct MockNotifier {
    sent_batches: std::sync::Arc<std::sync::Mutex<Vec<(String, String, usize)>>>,
}

impl MockNotifier {
    pub fn sent_batches(&self) -> Vec<(String, String, usize)> {
        self.sent_batches.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl NotificationBackend for MockNotifier {
    async fn send_matches(
        &self,
        destination: &str,
        profile_name: &str,
        matches: &[(ItemScore, Item)],
    ) -> Result<usize> {
        self.sent_batches.lock().unwrap().push((
            destination.to_string(),
            profile_name.to_string(),
            matches.len(),
        ));
        Ok(matches.len())
    }
}

fn build_embed(profile_name: &str, score: &ItemScore, item: &Item) -> serde_json::Value {
    let desc = item
        .abstract_text
        .as_deref()
        .unwrap_or("No abstract available.");
    let desc_truncated = truncate(desc, MAX_EMBED_DESC);

    let score_val = score.score.unwrap_or(0.0);
    let color = score_to_color(score_val);

    let mut fields = vec![serde_json::json!({
        "name": "Score",
        "value": format!("{:.2}", score_val),
        "inline": true,
    })];

    if let Some(ref reason) = score.reason_short {
        fields.push(serde_json::json!({
            "name": "Why",
            "value": truncate(reason, MAX_FIELD_VALUE),
            "inline": true,
        }));
    }

    if let Some(ref authors) = item.authors {
        fields.push(serde_json::json!({
            "name": "Authors",
            "value": truncate(authors, MAX_FIELD_VALUE),
            "inline": false,
        }));
    }

    serde_json::json!({
        "title": truncate(&item.title, 256),
        "url": item.url,
        "description": desc_truncated,
        "color": color,
        "fields": fields,
        "footer": {
            "text": format!("Profile: {} | Source: {}", profile_name, item.source_type),
        },
    })
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let cut = s[..max.saturating_sub(3)]
            .rfind(' ')
            .unwrap_or(max.saturating_sub(3));
        format!("{}...", &s[..cut])
    }
}

/// Map score 0.0-1.0 to a Discord embed color (red→yellow→green gradient).
fn score_to_color(score: f64) -> u32 {
    let clamped = score.clamp(0.0, 1.0);
    if clamped >= 0.8 {
        0x2ecc71 // green
    } else if clamped >= 0.6 {
        0xf39c12 // orange
    } else {
        0xe74c3c // red
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_short() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn test_truncate_long() {
        let long = "word ".repeat(100);
        let result = truncate(&long, 20);
        assert!(result.len() <= 20);
        assert!(result.ends_with("..."));
    }

    #[test]
    fn test_score_to_color() {
        assert_eq!(score_to_color(0.9), 0x2ecc71);
        assert_eq!(score_to_color(0.7), 0xf39c12);
        assert_eq!(score_to_color(0.3), 0xe74c3c);
    }

    #[test]
    fn test_build_embed() {
        let score = ItemScore {
            id: "s1".into(),
            item_id: "i1".into(),
            profile_id: "p1".into(),
            job_id: "j1".into(),
            disposition: crate::types::Disposition::Matched,
            score: Some(0.85),
            reason_short: Some("Highly relevant".into()),
            rationale: Some("Detailed rationale".into()),
            profile_revision_at_enqueue: 1,
            profile_revision_current: 1,
            created_at: chrono::Utc::now(),
        };
        let item = Item::new(
            "test:1".into(),
            "Test Paper Title".into(),
            "https://example.com".into(),
            "arxiv".into(),
        );
        let embed = build_embed("My Profile", &score, &item);
        assert_eq!(embed["title"], "Test Paper Title");
        assert_eq!(embed["color"], 0x2ecc71);
    }
}
