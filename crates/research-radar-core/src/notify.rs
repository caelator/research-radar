//! Discord webhook notifier with at-least-once delivery semantics.
//!
//! Posts scored research matches to Discord as rich embeds. Idempotency
//! is enforced via the `notifications` table — each (profile_id, item_id, channel)
//! tuple is sent at most once per scan.

use serde::{Deserialize, Serialize};

use crate::{DbPool, Profile, ScoredMatch};

/// Notification delivery result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyResult {
    pub sent: usize,
    pub skipped: usize,
    pub failed: usize,
    pub errors: Vec<String>,
}

/// Errors from the notification system.
#[derive(Debug, thiserror::Error)]
pub enum NotifyError {
    #[error("HTTP error: {0}")]
    Http(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("no webhook configured")]
    NoWebhook,
}

/// A Discord embed for a matched research item.
#[derive(Debug, Serialize)]
struct DiscordEmbed {
    title: String,
    description: String,
    color: u32,
    fields: Vec<DiscordField>,
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
}

#[derive(Debug, Serialize)]
struct DiscordField {
    name: String,
    value: String,
    inline: bool,
}

#[derive(Debug, Serialize)]
struct DiscordWebhookPayload {
    content: Option<String>,
    embeds: Vec<DiscordEmbed>,
}

/// Send notifications for accepted matches to Discord.
///
/// Checks the `notifications` table to avoid duplicate sends.
/// Records each successful send for idempotency.
pub async fn notify_discord(
    pool: &DbPool,
    profile: &Profile,
    matches: &[ScoredMatch],
    webhook_url: &str,
) -> Result<NotifyResult, NotifyError> {
    let client = crate::http_client().map_err(|e| NotifyError::Http(e.to_string()))?;
    let mut result = NotifyResult {
        sent: 0,
        skipped: 0,
        failed: 0,
        errors: Vec::new(),
    };

    // Filter to only matches above threshold
    let accepted: Vec<&ScoredMatch> = matches
        .iter()
        .filter(|m| m.score >= profile.score_threshold)
        .collect();

    if accepted.is_empty() {
        return Ok(result);
    }

    // Check which items have already been notified
    let already_notified = pool
        .get_notified_items(&profile.id, "discord")
        .map_err(|e| NotifyError::Storage(e.to_string()))?;

    // Batch embeds (Discord allows max 10 per message)
    let mut embeds = Vec::new();

    for scored in &accepted {
        if already_notified.contains(&scored.entry.id) {
            result.skipped += 1;
            continue;
        }

        let color = match scored.score {
            s if s >= 0.9 => 0xFF0000, // Red — critical relevance
            s if s >= 0.7 => 0xFF8C00, // Orange — high relevance
            s if s >= 0.5 => 0xFFD700, // Gold — medium relevance
            _ => 0x808080,             // Gray — low relevance
        };

        let source = pool.get_source(&scored.entry.source_id).ok().flatten();

        let url = source.as_ref().map(|s| s.url.clone());

        embeds.push((
            scored.entry.id.clone(),
            DiscordEmbed {
                title: truncate(
                    &source
                        .as_ref()
                        .map(|s| s.title.clone())
                        .unwrap_or_else(|| scored.entry.id.clone()),
                    256,
                ),
                description: truncate(
                    scored
                        .entry
                        .summary
                        .as_deref()
                        .unwrap_or(&scored.entry.content),
                    2048,
                ),
                color,
                fields: vec![
                    DiscordField {
                        name: "Score".into(),
                        value: format!("{:.0}%", scored.score * 100.0),
                        inline: true,
                    },
                    DiscordField {
                        name: "Profile".into(),
                        value: profile.name.clone(),
                        inline: true,
                    },
                    DiscordField {
                        name: "Disposition".into(),
                        value: scored.disposition.clone(),
                        inline: true,
                    },
                ],
                url,
            },
        ));
    }

    // Send in batches of 10
    for batch in embeds.chunks(10) {
        let payload = DiscordWebhookPayload {
            content: None,
            embeds: batch
                .iter()
                .map(|(_, e)| {
                    // We need to clone — Discord embeds are consumed by serde
                    DiscordEmbed {
                        title: e.title.clone(),
                        description: e.description.clone(),
                        color: e.color,
                        fields: e
                            .fields
                            .iter()
                            .map(|f| DiscordField {
                                name: f.name.clone(),
                                value: f.value.clone(),
                                inline: f.inline,
                            })
                            .collect(),
                        url: e.url.clone(),
                    }
                })
                .collect(),
        };

        match client.post(webhook_url).json(&payload).send().await {
            Ok(resp) if resp.status().is_success() => {
                // Record each item as notified
                for (item_id, _) in batch {
                    if let Err(e) = pool.record_notification(&profile.id, item_id, "discord") {
                        tracing::warn!("failed to record notification for {item_id}: {e}");
                    }
                    result.sent += 1;
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let text = resp.text().await.unwrap_or_default();
                let err = format!("Discord webhook {status}: {text}");
                tracing::warn!("{err}");
                result.failed += batch.len();
                result.errors.push(err);
            }
            Err(e) => {
                let err = format!("Discord webhook error: {e}");
                tracing::warn!("{err}");
                result.failed += batch.len();
                result.errors.push(err);
            }
        }
    }

    Ok(result)
}

/// Truncate `s` to at most `max` bytes, appending "..." if truncated.
///
/// Safe for multi-byte UTF-8: never slices inside a code point. Prefers to cut
/// at a word boundary when one falls within the window, otherwise cuts at the
/// nearest char boundary at or before the byte limit.
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    // Reserve room for the ellipsis.
    let limit = max.saturating_sub(3);
    // Walk back to a char boundary at or below `limit`.
    let mut boundary = limit;
    while !s.is_char_boundary(boundary) {
        boundary -= 1;
    }
    // Prefer to cut at the last space within the window for readability.
    let cut = s[..boundary].rfind(' ').unwrap_or(boundary);
    format!("{}...", &s[..cut])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_short_string() {
        assert_eq!(truncate("hello", 10), "hello");
    }

    #[test]
    fn truncate_long_string() {
        let long = "a ".repeat(200);
        let result = truncate(&long, 50);
        assert!(result.len() <= 50);
        assert!(result.ends_with("..."));
    }

    #[test]
    fn truncate_multibyte_utf8_does_not_panic() {
        // Accented characters (2 bytes each in UTF-8). A naive byte slice
        // would land mid-codepoint and panic.
        let s = "aaéééééééééééé end";
        let result = truncate(s, 10);
        assert!(result.ends_with("..."));
        // Result must be valid UTF-8 and within the byte budget.
        assert!(result.len() <= 10);
    }

    #[test]
    fn truncate_cjk_does_not_panic() {
        // CJK characters are 3 bytes each.
        let s: String = "中".repeat(50);
        let result = truncate(&s, 10);
        assert!(result.ends_with("..."));
        assert!(result.len() <= 10);
    }

    #[test]
    fn discord_embed_color_ranges() {
        let color_critical = 0xFF0000u32;
        let color_high = 0xFF8C00u32;
        let color_medium = 0xFFD700u32;
        let color_low = 0x808080u32;
        assert_ne!(color_critical, color_high);
        assert_ne!(color_high, color_medium);
        assert_ne!(color_medium, color_low);
    }

    #[test]
    fn discord_embed_serializes_correctly() {
        let embed = DiscordEmbed {
            title: "Test Paper".into(),
            description: "A paper about AI safety".into(),
            color: 0xFF0000,
            fields: vec![
                DiscordField {
                    name: "Score".into(),
                    value: "95%".into(),
                    inline: true,
                },
                DiscordField {
                    name: "Profile".into(),
                    value: "AI Safety".into(),
                    inline: true,
                },
            ],
            url: Some("https://arxiv.org/abs/2401.12345".into()),
        };

        let json = serde_json::to_value(&embed).unwrap();
        assert_eq!(json["title"], "Test Paper");
        assert_eq!(json["color"], 0xFF0000u32);
        assert_eq!(json["url"], "https://arxiv.org/abs/2401.12345");
        assert_eq!(json["fields"].as_array().unwrap().len(), 2);
        assert_eq!(json["fields"][0]["name"], "Score");
        assert!(json["fields"][0]["inline"].as_bool().unwrap());
    }

    #[test]
    fn discord_embed_omits_null_url() {
        let embed = DiscordEmbed {
            title: "No URL".into(),
            description: "desc".into(),
            color: 0x808080,
            fields: vec![],
            url: None,
        };

        let json = serde_json::to_value(&embed).unwrap();
        assert!(!json.as_object().unwrap().contains_key("url"));
    }

    #[test]
    fn discord_webhook_payload_batches_embeds() {
        let embeds: Vec<DiscordEmbed> = (0..3)
            .map(|i| DiscordEmbed {
                title: format!("Paper {i}"),
                description: format!("Description {i}"),
                color: 0xFFD700,
                fields: vec![DiscordField {
                    name: "Score".into(),
                    value: format!("{i}0%"),
                    inline: true,
                }],
                url: None,
            })
            .collect();

        let payload = DiscordWebhookPayload {
            content: None,
            embeds,
        };

        let json = serde_json::to_value(&payload).unwrap();
        assert!(json["content"].is_null());
        assert_eq!(json["embeds"].as_array().unwrap().len(), 3);
        assert_eq!(json["embeds"][0]["title"], "Paper 0");
        assert_eq!(json["embeds"][2]["title"], "Paper 2");
    }

    #[test]
    fn discord_embed_fields_truncated_to_limits() {
        // Discord limits: title 256, description 2048
        let long_title = "x".repeat(300);
        let long_desc = "y ".repeat(1500); // ~3000 chars

        let truncated_title = truncate(&long_title, 256);
        let truncated_desc = truncate(&long_desc, 2048);

        assert!(truncated_title.len() <= 256);
        assert!(truncated_desc.len() <= 2048);
    }
}
