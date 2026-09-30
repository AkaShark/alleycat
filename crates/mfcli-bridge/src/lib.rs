//! MyFlicker (`mfcli`) bridge: ACP over `mfcli acp`.
//!
//! All mfcli-specific behavior lives here; `acp-bridge` stays generic.
//! - launch: `<bin> acp`, with the client advertising no fs/terminal
//!   support (mfcli runs its own tools on this machine) and model
//!   discovery enabled (mfcli does not persist prompt-less sessions);
//! - `thread/list`: mfcli's `session/list` needs a `cwd`, so the bridge
//!   lists every known project (`~/.codeflicker/data.json` + its own cwd
//!   index + the request's cwd) on the secondary process and merges;
//! - `thread/start` / `thread/resume` / `turn/start`: keep the cwd index
//!   current and seed the generic bridge with a session's real cwd.

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

    /// Give the generic bridge a session's real cwd before it restores the
    /// session in a fresh process (daemon restart, first turn on a thread
    /// the phone never resumed in this process).
    fn seed_session_cwd(&self, params: &Value) {
        if let Some(id) = thread_id(params) {
            if self.inner.session_cwd(&id).is_none() {
                if let Some(cwd) = self.index.cwd_for(&id) {
                    self.inner.set_session_cwd(&id, &cwd);
                }
            }
        }
    }

    async fn thread_list(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let requested = listing::requested_cwds(&params);
        let limit = params
            .get("limit")
            .and_then(Value::as_u64)
            .map(|n| n as usize);
        let cwds: BTreeSet<String> = if requested.is_empty() {
            let data_json = self.data_json.clone();
            let mut cwds: BTreeSet<String> =
                tokio::task::spawn_blocking(move || projects::project_dirs(&data_json))
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .collect();
            cwds.extend(self.index.cwds());
            cwds
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
        let id = thread_id(&params);
        let has_cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .is_some_and(|c| c.starts_with('/'));
        if let (Some(id), false) = (&id, has_cwd) {
            if let (Some(cwd), Some(obj)) = (self.index.cwd_for(id), params.as_object_mut()) {
                obj.insert("cwd".to_string(), json!(cwd));
            }
        }
        let cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .map(str::to_string);
        let response = self.inner.dispatch(ctx, "thread/resume", params).await?;
        if let (Some(id), Some(cwd)) = (id, cwd) {
            self.index.record_all([(id, cwd, now_ms())]);
        }
        Ok(response)
    }

    async fn thread_start(&self, ctx: &Conn, params: Value) -> Result<Value, JsonRpcError> {
        let cwd = params
            .get("cwd")
            .and_then(Value::as_str)
            .map(str::to_string);
        let response = self.inner.dispatch(ctx, "thread/start", params).await?;
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
            "thread/start" => self.thread_start(ctx, params).await,
            "turn/start" => {
                self.seed_session_cwd(&params);
                self.inner.dispatch(ctx, method, params).await
            }
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

    #[tokio::test]
    async fn seeds_inner_cwd_from_index() {
        let dir = tempfile::tempdir().unwrap();
        let p = paths(dir.path());
        CwdIndex::load(p.index_file.clone()).record_all([(
            "s1".to_string(),
            "/proj".to_string(),
            1,
        )]);
        let bridge = MfcliBridge::build("/nonexistent/mfcli", p, Arc::new(LocalLauncher))
            .await
            .unwrap();
        bridge.seed_session_cwd(&json!({"threadId": "s1"}));
        assert_eq!(bridge.inner.session_cwd("s1").as_deref(), Some("/proj"));
        bridge.seed_session_cwd(&json!({"threadId": "unknown"}));
        assert_eq!(bridge.inner.session_cwd("unknown"), None);
    }
}
