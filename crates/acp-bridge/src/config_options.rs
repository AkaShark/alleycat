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

/// Codex reasoning effort → ACP `thought_level` value for a model that
/// offers `offered`. The phone's top level is `xhigh`; on models whose top
/// level is `max` (and that have no `xhigh`) it becomes `max`.
pub fn thought_level_for(effort: p::ReasoningEffort, offered: &[String]) -> String {
    let level = match effort {
        p::ReasoningEffort::None | p::ReasoningEffort::Minimal | p::ReasoningEffort::Low => "low",
        p::ReasoningEffort::Medium => "medium",
        p::ReasoningEffort::High => "high",
        p::ReasoningEffort::XHigh | p::ReasoningEffort::Max => {
            let has = |v: &str| offered.iter().any(|o| o == v);
            if !has("xhigh") && has("max") {
                "max"
            } else {
                "xhigh"
            }
        }
    };
    level.to_string()
}

/// ACP `thought_level` value → codex reasoning effort. `max` maps to
/// `xhigh`: the phone drops efforts it does not know, and it has no `max`.
pub fn effort_for(level: &str) -> Option<p::ReasoningEffort> {
    match level {
        "low" => Some(p::ReasoningEffort::Low),
        "medium" => Some(p::ReasoningEffort::Medium),
        "high" => Some(p::ReasoningEffort::High),
        "xhigh" | "max" => Some(p::ReasoningEffort::XHigh),
        _ => None,
    }
}

/// Codex efforts (with the agent's display names) for a `thought_level`
/// option. `max` is left out when the model also offers `xhigh`, since
/// both would map to the phone's `xhigh`.
pub fn efforts_in(option: &Value) -> Vec<(p::ReasoningEffort, String)> {
    let opts = option
        .get("options")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let has_xhigh = opts
        .iter()
        .any(|o| o.get("value").and_then(Value::as_str) == Some("xhigh"));
    opts.iter()
        .filter_map(|o| {
            let value = o.get("value").and_then(Value::as_str)?;
            if value == "max" && has_xhigh {
                return None;
            }
            let name = o.get("name").and_then(Value::as_str).unwrap_or(value);
            Some((effort_for(value)?, name.to_string()))
        })
        .collect()
}

/// Model to switch to so the session matches the request, if any. Values
/// the agent does not offer are skipped (and logged): the turn runs with
/// the agent's current value instead of failing.
pub fn model_change(options: &[Value], model: Option<&str>) -> Option<String> {
    let model = model?;
    wants(options, MODEL, model).then(|| model.to_string())
}

/// Thinking level to switch to, checked against the levels the session's
/// current model offers (so apply a model change first).
pub fn effort_change(options: &[Value], effort: Option<p::ReasoningEffort>) -> Option<String> {
    let level = thought_level_for(effort?, &values(options, THOUGHT_LEVEL));
    wants(options, THOUGHT_LEVEL, &level).then_some(level)
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
        let full: Vec<String> = ["low", "medium", "high", "xhigh"]
            .map(String::from)
            .to_vec();
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
            assert_eq!(thought_level_for(effort, &full), level, "{effort:?}");
        }
        let max_only: Vec<String> = ["high", "max"].map(String::from).to_vec();
        assert_eq!(thought_level_for(E::XHigh, &max_only), "max");
        let both: Vec<String> = ["high", "xhigh", "max"].map(String::from).to_vec();
        assert_eq!(thought_level_for(E::XHigh, &both), "xhigh");
        assert_eq!(effort_for("xhigh"), Some(E::XHigh));
        assert_eq!(effort_for("max"), Some(E::XHigh));
        assert_eq!(effort_for("turbo"), None);
    }

    #[test]
    fn efforts_in_folds_max_into_xhigh() {
        use p::ReasoningEffort as E;
        let max_only = json!({"options": [{"value": "high", "name": "High"}, {"value": "max", "name": "Max"}]});
        assert_eq!(
            efforts_in(&max_only),
            vec![(E::High, "High".to_string()), (E::XHigh, "Max".to_string())]
        );
        let both = json!({"options": [{"value": "xhigh"}, {"value": "max"}]});
        assert_eq!(efforts_in(&both), vec![(E::XHigh, "xhigh".to_string())]);
        assert!(efforts_in(&json!({})).is_empty());
    }

    #[test]
    fn changes_only_include_allowed_differences() {
        let opts = options();
        assert_eq!(model_change(&opts, Some("m/a")), None);
        assert_eq!(model_change(&opts, Some("m/b")).as_deref(), Some("m/b"));
        assert_eq!(model_change(&opts, Some("placeholder")), None);
        assert_eq!(model_change(&[], Some("m/b")), None);
        assert_eq!(effort_change(&opts, Some(p::ReasoningEffort::Low)), None);
        assert_eq!(
            effort_change(&opts, Some(p::ReasoningEffort::High)).as_deref(),
            Some("high")
        );
        assert_eq!(effort_change(&[], Some(p::ReasoningEffort::High)), None);
        assert_eq!(effort_change(&opts, None), None);
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
