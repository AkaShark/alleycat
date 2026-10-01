//! Live checks against the real `mfcli` (not run by default; need mfcli
//! installed and logged in; no model calls):
//! `MFCLI_LIVE_CWD=/abs/project cargo test -p alleycat-mfcli-bridge --test live_mfcli -- --ignored --nocapture`

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::launch_environment::UserEnvironmentLauncher;
use alleycat_bridge_core::session::Session;
use alleycat_bridge_core::{Bridge, Conn, LocalLauncher, ProcessLauncher};
use alleycat_mfcli_bridge::{MfcliBridge, MfcliPaths};
use serde_json::{Value, json};

async fn live_bridge(state: &std::path::Path) -> (Arc<MfcliBridge>, Conn) {
    let launcher: Arc<dyn ProcessLauncher> =
        Arc::new(UserEnvironmentLauncher::new(Arc::new(LocalLauncher)));
    let bridge = MfcliBridge::build("mfcli", MfcliPaths::default_for(state), launcher)
        .await
        .unwrap();
    let conn = Conn::from_session(Arc::new(Session::new("mfcli", "live".into(), 64, 1 << 20)));
    tokio::time::timeout(
        Duration::from_secs(30),
        bridge.initialize(
            &conn,
            json!({"clientInfo": {"name": "live", "version": "0"}}),
        ),
    )
    .await
    .expect("initialize timed out")
    .expect("initialize");
    (bridge, conn)
}

/// mfcli's saved default model and thinking level.
fn saved_default() -> Value {
    let home = std::env::var("HOME").unwrap();
    let text =
        std::fs::read_to_string(format!("{home}/.codeflicker/config.json")).unwrap_or_default();
    let config: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
    json!({"model": config["model"], "thinkingLevel": config["thinkingLevel"]})
}

#[tokio::test]
#[ignore = "live: needs a logged-in mfcli"]
async fn starts_a_thread_in_a_project_directory() {
    let cwd = std::env::var("MFCLI_LIVE_CWD").expect("set MFCLI_LIVE_CWD");
    let state = tempfile::tempdir().unwrap();
    let (bridge, conn) = live_bridge(state.path()).await;
    let started = tokio::time::timeout(
        Duration::from_secs(60),
        bridge.dispatch(&conn, "thread/start", json!({"cwd": cwd})),
    )
    .await
    .expect("thread/start timed out")
    .unwrap();
    println!(
        "thread/start model={} id={}",
        started["model"], started["thread"]["id"]
    );
}

#[tokio::test]
#[ignore = "live: needs a logged-in mfcli"]
async fn model_list_has_levels_per_model_and_keeps_the_default() {
    let before = saved_default();
    let state = tempfile::tempdir().unwrap();
    let (bridge, conn) = live_bridge(state.path()).await;
    let list = tokio::time::timeout(
        Duration::from_secs(120),
        bridge.dispatch(&conn, "model/list", json!({})),
    )
    .await
    .expect("model/list timed out")
    .unwrap();

    let mut level_sets = BTreeSet::new();
    for model in list["data"].as_array().unwrap() {
        let levels: Vec<&str> = model["supportedReasoningEfforts"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|e| e["reasoningEffort"].as_str())
            .collect();
        println!("{:40} {levels:?}", model["id"].as_str().unwrap_or(""));
        level_sets.insert(levels.join(","));
    }
    assert!(level_sets.len() > 1, "every model got the same levels");
    assert_eq!(
        saved_default(),
        before,
        "discovery changed the saved default"
    );
}
