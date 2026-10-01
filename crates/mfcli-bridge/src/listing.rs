//! Multi-project `thread/list` for mfcli: request filters, merging and the
//! codex `Thread` shape. Sessions themselves come from [`crate::storage`].

use std::collections::HashSet;

use serde_json::{Value, json};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcpSession {
    pub session_id: String,
    pub cwd: String,
    pub title: String,
    pub updated_at_ms: i64,
}

/// `cwd` filter of a codex `thread/list` request (string or array).
pub fn requested_cwds(params: &Value) -> Vec<String> {
    match params.get("cwd") {
        Some(Value::String(cwd)) if cwd.starts_with('/') => vec![cwd.clone()],
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(Value::as_str)
            .filter(|c| c.starts_with('/'))
            .map(str::to_string)
            .collect(),
        _ => Vec::new(),
    }
}

/// Newest first, one entry per session id, truncated to `limit`.
pub fn merge(mut sessions: Vec<AcpSession>, limit: Option<usize>) -> Vec<AcpSession> {
    sessions.sort_by(|a, b| {
        b.updated_at_ms
            .cmp(&a.updated_at_ms)
            .then_with(|| a.session_id.cmp(&b.session_id))
    });
    let mut seen = HashSet::new();
    sessions.retain(|s| seen.insert(s.session_id.clone()));
    if let Some(limit) = limit {
        sessions.truncate(limit);
    }
    sessions
}

pub fn to_thread(s: &AcpSession, agent: &str) -> Value {
    json!({
        "id": s.session_id,
        "sessionId": s.session_id,
        "forkedFromId": null,
        "preview": s.title,
        "ephemeral": false,
        "modelProvider": agent,
        "createdAt": s.updated_at_ms,
        "updatedAt": s.updated_at_ms,
        "status": {"type": "idle"},
        "path": "",
        "cwd": s.cwd,
        "cliVersion": "",
        "source": "appServer",
        "threadSource": null,
        "agentNickname": null,
        "agentRole": null,
        "gitInfo": null,
        "name": s.title,
        "turns": [],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requested_cwds_accepts_string_and_array() {
        assert_eq!(requested_cwds(&json!({"cwd": "/a"})), vec!["/a"]);
        assert_eq!(
            requested_cwds(&json!({"cwd": ["/a", "rel", "/b"]})),
            vec!["/a", "/b"]
        );
        assert!(requested_cwds(&json!({"cwd": "rel"})).is_empty());
        assert!(requested_cwds(&json!({})).is_empty());
    }

    #[test]
    fn merge_dedupes_sorts_and_limits() {
        let a = AcpSession {
            session_id: "a".into(),
            cwd: "/p".into(),
            title: "a".into(),
            updated_at_ms: 1,
        };
        let b = AcpSession {
            session_id: "b".into(),
            cwd: "/p".into(),
            title: "b".into(),
            updated_at_ms: 3,
        };
        let a_newer = AcpSession {
            updated_at_ms: 5,
            ..a.clone()
        };
        let merged = merge(vec![a, b.clone(), a_newer.clone()], None);
        assert_eq!(merged, vec![a_newer.clone(), b]);
        assert_eq!(merge(merged, Some(1)), vec![a_newer]);
    }
}
