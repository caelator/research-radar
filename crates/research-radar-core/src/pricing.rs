use serde::Deserialize;

static UNKNOWN_MODEL_WARN: std::sync::Once = std::sync::Once::new();

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Usage {
    #[serde(default)]
    pub input_tokens: u64,
    #[serde(default)]
    pub output_tokens: u64,
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    #[serde(default)]
    pub cache_read_input_tokens: u64,
}

pub fn cost_microunits(model: &str, usage: &Usage) -> i64 {
    let (base_input_rate, output_rate) = match model {
        "claude-sonnet-4-6" => (3_i64, 15_i64),
        "claude-opus-4-8" => (5_i64, 25_i64),
        "claude-haiku-4-5" => (1_i64, 5_i64),
        unknown => {
            UNKNOWN_MODEL_WARN.call_once(|| {
                tracing::warn!(
                    model = unknown,
                    "unknown Anthropic model for pricing; falling back to claude-sonnet-4-6 rates"
                );
            });
            (3_i64, 15_i64)
        }
    };

    (usage.input_tokens as i64 * base_input_rate)
        + (usage.output_tokens as i64 * output_rate)
        + (usage.cache_read_input_tokens as f64 * base_input_rate as f64 * 0.1).round() as i64
        + (usage.cache_creation_input_tokens as f64 * base_input_rate as f64 * 1.25).round() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sonnet_input_and_output_cost() {
        let usage = Usage {
            input_tokens: 1000,
            output_tokens: 500,
            ..Default::default()
        };
        assert_eq!(cost_microunits("claude-sonnet-4-6", &usage), 10500);
    }

    #[test]
    fn opus_input_and_output_cost() {
        let usage = Usage {
            input_tokens: 2000,
            output_tokens: 1000,
            ..Default::default()
        };
        assert_eq!(cost_microunits("claude-opus-4-8", &usage), 35000);
    }

    #[test]
    fn haiku_input_and_output_cost() {
        let usage = Usage {
            input_tokens: 1000,
            output_tokens: 1000,
            ..Default::default()
        };
        assert_eq!(cost_microunits("claude-haiku-4-5", &usage), 6000);
    }

    #[test]
    fn unknown_model_falls_back_to_sonnet_cost() {
        let usage = Usage {
            input_tokens: 1000,
            output_tokens: 500,
            ..Default::default()
        };
        assert_eq!(cost_microunits("gpt-foo", &usage), 10500);
    }

    #[test]
    fn sonnet_cache_read_cost_rounds() {
        let usage = Usage {
            cache_read_input_tokens: 1000,
            ..Default::default()
        };
        assert_eq!(cost_microunits("claude-sonnet-4-6", &usage), 300);
    }

    #[test]
    fn zero_usage_is_free() {
        assert_eq!(cost_microunits("claude-sonnet-4-6", &Usage::default()), 0);
    }
}
