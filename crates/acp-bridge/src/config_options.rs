//! Helpers for ACP `configOptions` (`session/new`, `session/load`,
//! `session/resume` and `session/set_config_option` all return them).

use alleycat_codex_proto as p;
use serde_json::Value;
use tracing::warn;

pub const MODEL: &str = "model";
pub const THOUGHT_LEVEL: &str = "thought_level";

/// `configOptions` array of an ACP response (empty when absent).
pub fn extract(response: &Value) -> Vec<Value> {
    response
        .get("configOptions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

pub fn find<'a>(options: &'a [Value], id: &str) -> Option<&'a Value> {
    options
        .iter()
        .find(|o| o.get("id").and_then(Value::as_str) == Some(id))
}

pub fn current_value(options: &[Value], id: &str) -> Option<String> {
    find(options, id)?
        .get("currentValue")?
        .as_str()
        .map(str::to_string)
}

pub fn values(options: &[Value], id: &str) -> Vec<String> {
    find(options, id)
        .and_then(|o| o.get("options"))
        .and_then(Value::as_array)
        .map(|opts| {
            opts.iter()
                .filter_map(|o| o.get("value").and_then(Value::as_str).map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Codex reasoning effort → ACP `thought_level` value.
pub fn thought_level_for(effort: p::ReasoningEffort) -> &'static str {
    match effort {
        p::ReasoningEffort::None | p::ReasoningEffort::Minimal | p::ReasoningEffort::Low => "low",
        p::ReasoningEffort::Medium => "medium",
        p::ReasoningEffort::High => "high",
        p::ReasoningEffort::XHigh | p::ReasoningEffort::Max => "xhigh",
    }
}

/// ACP `thought_level` value → codex reasoning effort.
pub fn effort_for(level: &str) -> Option<p::ReasoningEffort> {
    match level {
        "low" => Some(p::ReasoningEffort::Low),
        "medium" => Some(p::ReasoningEffort::Medium),
        "high" => Some(p::ReasoningEffort::High),
        "xhigh" => Some(p::ReasoningEffort::XHigh),
        _ => None,
    }
}

/// `(configId, value)` pairs to send so the session matches the request.
/// Values the agent does not offer are skipped (and logged): the turn
/// runs with the agent's current value instead of failing.
pub fn pending_changes(
    options: &[Value],
    model: Option<&str>,
    effort: Option<p::ReasoningEffort>,
) -> Vec<(&'static str, String)> {
    let mut out = Vec::new();
    if let Some(model) = model {
        if wants(options, MODEL, model) {
            out.push((MODEL, model.to_string()));
        }
    }
    if let Some(effort) = effort {
        let level = thought_level_for(effort);
        if wants(options, THOUGHT_LEVEL, level) {
            out.push((THOUGHT_LEVEL, level.to_string()));
        }
    }
    out
}

fn wants(options: &[Value], id: &str, value: &str) -> bool {
    let allowed = values(options, id);
    if allowed.is_empty() {
        return false;
    }
    if !allowed.iter().any(|v| v == value) {
        warn!(
            config_id = id,
            value, "agent does not offer this value; keeping its current one"
        );
        return false;
    }
    current_value(options, id).as_deref() != Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn options() -> Vec<serde_json::Value> {
        vec![
            json!({"id": "model", "currentValue": "m/a", "options": [{"value": "m/a"}, {"value": "m/b"}]}),
            json!({"id": "thought_level", "currentValue": "low", "options": [{"value": "low"}, {"value": "medium"}, {"value": "high"}, {"value": "xhigh"}]}),
        ]
    }

    #[test]
    fn effort_maps_to_thought_level() {
        use p::ReasoningEffort as E;
        let cases = [
            (E::None, "low"),
            (E::Minimal, "low"),
            (E::Low, "low"),
            (E::Medium, "medium"),
            (E::High, "high"),
            (E::XHigh, "xhigh"),
            (E::Max, "xhigh"),
        ];
        for (effort, level) in cases {
            assert_eq!(thought_level_for(effort), level, "{effort:?}");
        }
        assert_eq!(effort_for("xhigh"), Some(E::XHigh));
        assert_eq!(effort_for("turbo"), None);
    }

    #[test]
    fn pending_changes_only_includes_allowed_differences() {
        let opts = options();
        assert!(pending_changes(&opts, Some("m/a"), Some(p::ReasoningEffort::Low)).is_empty());
        assert_eq!(
            pending_changes(&opts, Some("m/b"), Some(p::ReasoningEffort::High)),
            vec![
                (MODEL, "m/b".to_string()),
                (THOUGHT_LEVEL, "high".to_string())
            ]
        );
        assert!(pending_changes(&opts, Some("placeholder"), None).is_empty());
        assert!(pending_changes(&[], Some("m/b"), Some(p::ReasoningEffort::High)).is_empty());
    }

    #[test]
    fn current_value_and_values() {
        let opts = options();
        assert_eq!(current_value(&opts, MODEL).as_deref(), Some("m/a"));
        assert_eq!(
            values(&opts, THOUGHT_LEVEL),
            vec!["low", "medium", "high", "xhigh"]
        );
        assert_eq!(extract(&json!({"configOptions": opts.clone()})), opts);
        assert!(extract(&json!({})).is_empty());
    }
}
