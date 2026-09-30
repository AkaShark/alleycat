//! MyFlicker (`mfcli`) bridge: ACP over `mfcli acp`.
//!
//! All mfcli-specific behavior lives here; `acp-bridge` stays generic.
//! - launch: `<bin> acp`, with the client advertising no fs/terminal
//!   support (mfcli runs its own tools on this machine) and model
//!   discovery enabled (mfcli does not persist prompt-less sessions);
//! - `thread/list`: mfcli's `session/list` needs a `cwd`, so the bridge
//!   lists every known project (`~/.codeflicker/data.json` + its own cwd
//!   index + the request's cwd) on the secondary process and merges;
//! - `thread/start` / `thread/fork` / `thread/resume` / `turn/start`: keep
//!   the cwd index current and give the generic bridge a session's real
//!   cwd; an unknown cwd is an error, never a silent `/`.

pub mod index;
pub mod listing;
pub mod projects;

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use alleycat_acp_bridge::AcpBridge;
use alleycat_bridge_core::{Bridge, Conn, JsonRpcError, ProcessLauncher, error_codes};
use anyhow::{Context, Result};
use async_trait::async_trait;
use serde_json::{Value, json};

use crate::index::CwdIndex;
use crate::listing::SessionLister;

#[derive(Debug, Clone)]
pub struct MfcliPaths {
    /// mfcli's `data.json` (project directories).
    pub data_json: PathBuf,
    /// Where the bridge keeps its `sessionId → cwd` index.
    pub index_file: PathBuf,
}

impl MfcliPaths {
    pub fn default_for(state_dir: &Path) -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/tmp"));
        Self {
            data_json: home.join(".codeflicker/data.json"),
            index_file: state_dir.join("mfcli-sessions.json"),
        }
    }
}

pub struct MfcliBridge {
    inner: Arc<AcpBridge>,
    index: CwdIndex,
    data_json: PathBuf,
}

fn internal(message: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: error_codes::INTERNAL_ERROR,
        message: message.into(),
        data: None,
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn thread_id(params: &Value) -> Option<String> {
    params
        .get("threadId")
        .and_then(Value::as_str)
        .map(str::to_string)
}

impl MfcliBridge {
    pub fn new(inner: Arc<AcpBridge>, paths: MfcliPaths) -> Self {
        Self {
            inner,
            index: CwdIndex::load(paths.index_file),
            data_json: paths.data_json,
        }
    }

    /// Build the bridge. Nothing is spawned until the first phone connects,
    /// so a missing `mfcli` binary does not fail the daemon.
    pub async fn build(
        bin: impl Into<PathBuf>,
        paths: MfcliPaths,
        launcher: Arc<dyn ProcessLauncher>,
    ) -> Result<Arc<Self>> {
        let inner = AcpBridge::builder()
            .agent_bin(bin)
            .agent_args(vec!["acp".to_string()])
            .client_capabilities(
                json!({"fs": {"readTextFile": false, "writeTextFile": false}, "terminal": false}),
            )
            .discover_models(true)
            .pool_capacity(8)
            .launcher(launcher)
            .build()
            .await
            .context("building inner AcpBridge for mfcli")?;
        Ok(Arc::new(Self::new(inner, paths)))
    }

    /// A session's cwd without asking mfcli: the generic bridge's record,
    /// then the request's `cwd`, then the index. `/` (the placeholder for an
    /// unknown cwd) never counts.
    fn known_cwd(&self, session_id: &str, params: &Value) -> Option<String> {
        self.inner
            .session_cwd(session_id)
            .or_else(|| {
                params
                    .get("cwd")
                    .and_then(Value::as_str)
                    .filter(|c| c.starts_with('/') && *c != "/")
                    .map(str::to_string)
            })
            .or_else(|| self.index.cwd_for(session_id))
    }

    /// Find a session's project by listing every known project (refreshes
    /// the index as a side effect).
    async fn resolve_cwd(&self, lister: &dyn SessionLister, session_id: &str) -> Option<String> {
        let cwds = self.candidate_cwds().await;
        let sessions = listing::collect_sessions(lister, &cwds).await;
        self.index.record_all(
            sessions
                .iter()
                .map(|s| (s.session_id.clone(), s.cwd.clone(), s.updated_at_ms)),
        );
        sessions
            .into_iter()
            .find(|s| s.session_id == session_id)
            .map(|s| s.cwd)
    }

    /// The session's real cwd. mfcli runs tools in the session's cwd, so an
    /// unknown cwd is an error rather than a silent `/`.
    async fn cwd_for_session(
        &self,
        ctx: &Conn,
        session_id: &str,
        params: &Value,
    ) -> Result<String, JsonRpcError> {
        if let Some(cwd) = self.known_cwd(session_id, params) {
            return Ok(cwd);
        }
        let client = self
            .inner
            .ensure_aux_client(ctx)
            .await
            .map_err(|e| internal(format!("Failed to get mfcli ACP client: {e}")))?;
        self.resolve_cwd(client.as_ref(), session_id)
            .await
            .ok_or_else(|| {
                internal(format!(
                    "MyFlicker could not find the project directory of session {session_id}; \
                     refresh the task list and try again"
                ))
            })
    }

    /// Project directories to list: mfcli's own projects plus every cwd
    /// the index has seen.
    async fn candidate_cwds(&self) -> BTreeSet<String> {
        let data_json = self.data_json.clone();
        let mut cwds: BTreeSet<String> =
            tokio::task::spawn_blocking(move || projects::project_dirs(&data_json))
                .await
                .unwrap_or_default()
                .into_iter()
                .collect();
        cwds.extend(self.index.cwds());
        cwds
    }

    async fn thread_list(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let requested = listing::requested_cwds(&params);
        let limit = params
            .get("limit")
            .and_then(Value::as_u64)
            .map(|n| n as usize);
        let cwds: BTreeSet<String> = if requested.is_empty() {
            self.candidate_cwds().await
        } else {
            requested.into_iter().collect()
        };
        let client = self
            .inner
            .ensure_aux_client(ctx)
            .await
            .map_err(|e| internal(format!("Failed to get mfcli ACP client: {e}")))?;
        let sessions = listing::merge(
            listing::collect_sessions(client.as_ref(), &cwds).await,
            limit,
        );
        self.index.record_all(
            sessions
                .iter()
                .map(|s| (s.session_id.clone(), s.cwd.clone(), s.updated_at_ms)),
        );
        let agent = ctx.session().agent.to_string();
        let data: Vec<Value> = sessions
            .iter()
            .map(|s| listing::to_thread(s, &agent))
            .collect();
        Ok(json!({"data": data, "nextCursor": null, "backwardsCursor": null}))
    }

    async fn thread_resume(&self, ctx: &Conn, mut params: Value) -> Result<Value, JsonRpcError> {
        let Some(id) = thread_id(&params) else {
            return self.inner.dispatch(ctx, "thread/resume", params).await;
        };
        let cwd = self.cwd_for_session(ctx, &id, &params).await?;
        if let Some(obj) = params.as_object_mut() {
            obj.insert("cwd".to_string(), json!(cwd));
        }
        let response = self.inner.dispatch(ctx, "thread/resume", params).await?;
        self.index.record_all([(id, cwd, now_ms())]);
        Ok(response)
    }

    /// `turn/start` on a session the generic bridge has no cwd for (daemon
    /// restart, thread never opened in this process): find the cwd first
    /// so a restore never resumes the session in `/`.
    async fn turn_start(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        if let Some(id) = thread_id(&params)
            && self.inner.session_cwd(&id).is_none()
        {
            let cwd = self.cwd_for_session(ctx, &id, &params).await?;
            self.inner.set_session_cwd(&id, &cwd);
        }
        self.inner.dispatch(ctx, "turn/start", params).await
    }

    /// `thread/start` / `thread/fork`: record the new session's cwd.
    async fn thread_create(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        let cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .map(str::to_string);
        let response = self.inner.dispatch(ctx, method, params).await?;
        if let (Some(id), Some(cwd)) = (response.pointer("/thread/id").and_then(Value::as_str), cwd)
        {
            self.index.record_all([(id.to_string(), cwd, now_ms())]);
        }
        Ok(response)
    }
}

#[async_trait]
impl Bridge for MfcliBridge {
    async fn initialize(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        self.inner.initialize(ctx, params).await
    }

    async fn dispatch(
        &self,
        ctx: &Conn,
        method: &str,
        params: Value,
    ) -> Result<Value, JsonRpcError> {
        match method {
            "thread/list" => self.thread_list(ctx, params).await,
            "thread/resume" => self.thread_resume(ctx, params).await,
            "thread/start" | "thread/fork" => self.thread_create(ctx, method, params).await,
            "turn/start" => self.turn_start(ctx, params).await,
            _ => self.inner.dispatch(ctx, method, params).await,
        }
    }

    async fn notification(&self, ctx: &Conn, method: &str, params: Value) {
        self.inner.notification(ctx, method, params).await;
    }

    async fn shutdown(&self) {
        self.inner.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alleycat_bridge_core::LocalLauncher;

    fn paths(dir: &Path) -> MfcliPaths {
        MfcliPaths {
            data_json: dir.join("data.json"),
            index_file: dir.join("idx.json"),
        }
    }

    #[tokio::test]
    async fn build_succeeds_when_binary_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let bridge = MfcliBridge::build(
            "/nonexistent/mfcli",
            paths(dir.path()),
            Arc::new(LocalLauncher),
        )
        .await;
        assert!(bridge.is_ok());
    }

    struct OneProjectLister;

    #[async_trait]
    impl SessionLister for OneProjectLister {
        async fn list_page(&self, cwd: &str, _cursor: Option<&str>) -> anyhow::Result<Value> {
            if cwd == "/p" {
                Ok(
                    json!({"sessions": [{"sessionId": "s9", "cwd": "/p", "title": "t", "updatedAt": "2026-09-30T10:00:00Z"}]}),
                )
            } else {
                Ok(json!({"sessions": []}))
            }
        }
    }

    #[tokio::test]
    async fn known_cwd_prefers_inner_then_request_then_index() {
        let dir = tempfile::tempdir().unwrap();
        let p = paths(dir.path());
        CwdIndex::load(p.index_file.clone()).record_all([(
            "s1".to_string(),
            "/from-index".to_string(),
            1,
        )]);
        let bridge = MfcliBridge::build("/nonexistent/mfcli", p, Arc::new(LocalLauncher))
            .await
            .unwrap();
        assert_eq!(
            bridge.known_cwd("s1", &json!({})).as_deref(),
            Some("/from-index")
        );
        assert_eq!(
            bridge
                .known_cwd("s1", &json!({"cwd": "/from-request"}))
                .as_deref(),
            Some("/from-request")
        );
        assert_eq!(
            bridge.known_cwd("s1", &json!({"cwd": "/"})).as_deref(),
            Some("/from-index")
        );
        bridge.inner.set_session_cwd("s1", "/from-inner");
        assert_eq!(
            bridge
                .known_cwd("s1", &json!({"cwd": "/from-request"}))
                .as_deref(),
            Some("/from-inner")
        );
        assert_eq!(bridge.known_cwd("nobody", &json!({"cwd": "/"})), None);
    }

    #[tokio::test]
    async fn resolve_cwd_finds_session_by_listing_projects() {
        let dir = tempfile::tempdir().unwrap();
        let p = paths(dir.path());
        std::fs::write(&p.data_json, r#"{"projects": {"/p": {}, "/q": {}}}"#).unwrap();
        let bridge = MfcliBridge::build("/nonexistent/mfcli", p, Arc::new(LocalLauncher))
            .await
            .unwrap();
        assert_eq!(
            bridge.resolve_cwd(&OneProjectLister, "s9").await.as_deref(),
            Some("/p")
        );
        assert_eq!(bridge.index.cwd_for("s9").as_deref(), Some("/p"));
        assert_eq!(bridge.resolve_cwd(&OneProjectLister, "missing").await, None);
    }
}
