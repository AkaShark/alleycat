//! Multi-project `thread/list` for mfcli: its `session/list` only returns
//! sessions when given a `cwd`, so the bridge asks once per known project.

use std::collections::{BTreeSet, HashSet};

use alleycat_acp_bridge::acp_client::AcpClient;
use async_trait::async_trait;
use serde_json::{Value, json};
use tracing::warn;

/// Upper bound on `session/list` pages per project (guards against an
/// agent that keeps returning a cursor).
pub const MAX_PAGES: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcpSession {
    pub session_id: String,
    pub cwd: String,
    pub title: String,
    pub updated_at_ms: i64,
}

#[async_trait]
pub trait SessionLister: Send + Sync {
    async fn list_page(&self, cwd: &str, cursor: Option<&str>) -> anyhow::Result<Value>;
}

#[async_trait]
impl SessionLister for AcpClient {
    async fn list_page(&self, cwd: &str, cursor: Option<&str>) -> anyhow::Result<Value> {
        let mut params = json!({"cwd": cwd});
        if let Some(cursor) = cursor {
            params["cursor"] = json!(cursor);
        }
        self.send_request("session/list", params).await
    }
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

pub fn parse_page(page: &Value, requested_cwd: &str) -> (Vec<AcpSession>, Option<String>) {
    let sessions = page
        .get("sessions")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|s| {
                    let session_id = s.get("sessionId")?.as_str()?.to_string();
                    let cwd = s
                        .get("cwd")
                        .and_then(Value::as_str)
                        .filter(|c| c.starts_with('/'))
                        .unwrap_or(requested_cwd)
                        .to_string();
                    let title = s
                        .get("title")
                        .and_then(Value::as_str)
                        .filter(|t| !t.is_empty())
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("Session {session_id}"));
                    let updated_at_ms = match s.get("updatedAt") {
                        Some(Value::String(text)) => chrono::DateTime::parse_from_rfc3339(text)
                            .map(|d| d.timestamp_millis())
                            .unwrap_or(0),
                        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
                        _ => 0,
                    };
                    Some(AcpSession {
                        session_id,
                        cwd,
                        title,
                        updated_at_ms,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let next = page
        .get("nextCursor")
        .and_then(Value::as_str)
        .filter(|c| !c.is_empty())
        .map(str::to_string);
    (sessions, next)
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

/// Ask `lister` for every page of every cwd; a failing cwd is skipped.
pub async fn collect_sessions(
    lister: &dyn SessionLister,
    cwds: &BTreeSet<String>,
) -> Vec<AcpSession> {
    let mut out = Vec::new();
    for cwd in cwds {
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let page = match lister.list_page(cwd, cursor.as_deref()).await {
                Ok(page) => page,
                Err(err) => {
                    warn!(cwd, error = %err, "mfcli session/list failed for project; skipping");
                    break;
                }
            };
            let (mut items, next) = parse_page(&page, cwd);
            out.append(&mut items);
            match next {
                Some(next) if cursor.as_deref() != Some(next.as_str()) => cursor = Some(next),
                _ => break,
            }
        }
    }
    out
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
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct FakeLister {
        pages: HashMap<String, Vec<Value>>,
        calls: Mutex<usize>,
    }

    #[async_trait]
    impl SessionLister for FakeLister {
        async fn list_page(&self, cwd: &str, cursor: Option<&str>) -> anyhow::Result<Value> {
            *self.calls.lock().unwrap() += 1;
            if cwd == "/broken" {
                anyhow::bail!("Internal error: boom");
            }
            if cwd == "/loop" {
                return Ok(json!({"sessions": [], "nextCursor": "same"}));
            }
            let idx: usize = cursor.map(|c| c.parse().unwrap()).unwrap_or(0);
            Ok(self
                .pages
                .get(cwd)
                .and_then(|p| p.get(idx))
                .cloned()
                .unwrap_or(json!({"sessions": []})))
        }
    }

    fn s(id: &str, cwd: &str, updated: &str) -> Value {
        json!({"sessionId": id, "cwd": cwd, "title": format!("t-{id}"), "updatedAt": updated})
    }

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
    fn parse_page_falls_back_to_requested_cwd_and_title() {
        let page = json!({"sessions": [{"sessionId": "x", "updatedAt": "2026-09-30T10:00:00Z"}], "nextCursor": null});
        let (items, next) = parse_page(&page, "/p");
        assert_eq!(items[0].cwd, "/p");
        assert_eq!(items[0].title, "Session x");
        assert_eq!(items[0].updated_at_ms, 1_790_762_400_000);
        assert_eq!(next, None);
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

    #[tokio::test]
    async fn collect_sessions_follows_pages() {
        let lister = FakeLister {
            pages: HashMap::from([(
                "/p".to_string(),
                vec![
                    json!({"sessions": [s("1", "/p", "2026-09-30T10:00:00Z")], "nextCursor": "1"}),
                    json!({"sessions": [s("2", "/p", "2026-09-30T11:00:00Z")], "nextCursor": null}),
                ],
            )]),
            calls: Mutex::new(0),
        };
        let cwds = BTreeSet::from(["/p".to_string()]);
        let ids: Vec<_> = collect_sessions(&lister, &cwds)
            .await
            .into_iter()
            .map(|s| s.session_id)
            .collect();
        assert_eq!(ids, vec!["1", "2"]);
    }

    #[tokio::test]
    async fn collect_sessions_skips_failing_cwd() {
        let lister = FakeLister {
            pages: HashMap::from([(
                "/ok".to_string(),
                vec![json!({"sessions": [s("1", "/ok", "2026-09-30T10:00:00Z")]})],
            )]),
            calls: Mutex::new(0),
        };
        let cwds = BTreeSet::from(["/broken".to_string(), "/ok".to_string()]);
        let ids: Vec<_> = collect_sessions(&lister, &cwds)
            .await
            .into_iter()
            .map(|s| s.session_id)
            .collect();
        assert_eq!(ids, vec!["1"]);
    }

    #[tokio::test]
    async fn collect_sessions_stops_on_repeated_cursor() {
        let lister = FakeLister {
            pages: HashMap::new(),
            calls: Mutex::new(0),
        };
        let cwds = BTreeSet::from(["/loop".to_string()]);
        collect_sessions(&lister, &cwds).await;
        assert_eq!(*lister.calls.lock().unwrap(), 2);
    }
}
