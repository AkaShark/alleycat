//! Shared helpers for acp-bridge integration tests.
//!
//! Each test builds its own bridge pointed at the `fake-acp-agent` binary
//! with a private log + config file, so tests in one binary can run in
//! parallel without sharing env vars.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::Arc;

use alleycat_acp_bridge::{AcpBridge, AcpBridgeBuilder};
use alleycat_bridge_core::session::Session;
use alleycat_bridge_core::{Bridge, Conn, JsonRpcError};
use serde_json::{Value, json};

pub fn fake_agent_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_fake-acp-agent"))
}

pub struct Harness {
    pub bridge: Arc<AcpBridge>,
    pub session: Arc<Session>,
    pub dir: tempfile::TempDir,
}

impl Harness {
    pub async fn new(config: Value) -> Self {
        Self::with(config, |b| b).await
    }

    pub async fn with(
        config: Value,
        customize: impl FnOnce(AcpBridgeBuilder) -> AcpBridgeBuilder,
    ) -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let config_path = dir.path().join("config.json");
        std::fs::write(&config_path, config.to_string()).expect("write config");
        std::fs::create_dir_all(dir.path().join("project")).expect("project dir");
        let log_path = dir.path().join("frames.jsonl");
        let builder = AcpBridge::builder()
            .agent_bin(fake_agent_path())
            .agent_args(vec![
                "--log".into(),
                log_path.display().to_string(),
                "--config".into(),
                config_path.display().to_string(),
            ]);
        let bridge = customize(builder).build().await.expect("build bridge");
        let session = Arc::new(Session::new("fake", "test-node".into(), 64, 1 << 20));
        Self {
            bridge,
            session,
            dir,
        }
    }

    pub fn conn(&self) -> Conn {
        Conn::from_session(Arc::clone(&self.session))
    }

    /// Absolute project directory tests pass as `cwd`.
    pub fn project_dir(&self) -> String {
        self.dir.path().join("project").display().to_string()
    }

    pub async fn initialize(&self) -> Value {
        self.bridge
            .initialize(
                &self.conn(),
                json!({"clientInfo": {"name": "test", "version": "0"}}),
            )
            .await
            .expect("initialize")
    }

    pub async fn call(&self, method: &str, params: Value) -> Result<Value, JsonRpcError> {
        self.bridge.dispatch(&self.conn(), method, params).await
    }

    pub async fn start_thread(&self) -> String {
        let started = self
            .call("thread/start", json!({"cwd": self.project_dir()}))
            .await
            .expect("thread/start");
        started["thread"]["id"]
            .as_str()
            .expect("thread id")
            .to_string()
    }

    /// Every line the fake agent received, across all spawned processes.
    pub fn frames(&self) -> Vec<Value> {
        read_frames(&self.dir.path().join("frames.jsonl"))
    }

    pub fn methods(&self) -> Vec<String> {
        self.frames()
            .iter()
            .filter_map(|f| f.get("method").and_then(Value::as_str).map(str::to_string))
            .collect()
    }

    pub fn last_frame(&self, method: &str) -> Value {
        self.frames()
            .into_iter()
            .filter(|f| f["method"] == method)
            .last()
            .unwrap_or_else(|| panic!("agent never received {method}"))
    }
}

fn read_frames(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

pub fn text_input(text: &str) -> Value {
    json!([{"type": "text", "text": text}])
}
