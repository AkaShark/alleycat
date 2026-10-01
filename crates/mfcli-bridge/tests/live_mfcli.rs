//! Live check against the real `mfcli` (not run by default; needs mfcli
//! installed and logged in, makes no model calls):
//! `MFCLI_LIVE_CWD=/abs/project cargo test -p alleycat-mfcli-bridge --test live_mfcli -- --ignored --nocapture`

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::launch_environment::UserEnvironmentLauncher;
use alleycat_bridge_core::session::Session;
use alleycat_bridge_core::{Bridge, Conn, LocalLauncher, ProcessLauncher};
use alleycat_mfcli_bridge::{MfcliBridge, MfcliPaths};
use serde_json::json;

#[tokio::test]
#[ignore = "live: needs a logged-in mfcli"]
async fn starts_a_thread_in_a_project_directory() {
    let _ = tracing_subscriber_init();
    let cwd = std::env::var("MFCLI_LIVE_CWD").expect("set MFCLI_LIVE_CWD");
    let state = tempfile::tempdir().unwrap();
    let launcher: Arc<dyn ProcessLauncher> =
        Arc::new(UserEnvironmentLauncher::new(Arc::new(LocalLauncher)));
    let bridge = MfcliBridge::build("mfcli", MfcliPaths::default_for(state.path()), launcher)
        .await
        .unwrap();
    let conn = Conn::from_session(Arc::new(Session::new("mfcli", "live".into(), 64, 1 << 20)));
    let init = tokio::time::timeout(
        Duration::from_secs(30),
        bridge.initialize(&conn, json!({"clientInfo": {"name": "live", "version": "0"}})),
    )
    .await
    .expect("initialize timed out");
    println!("initialize: {}", init.is_ok());
    let started = tokio::time::timeout(
        Duration::from_secs(60),
        bridge.dispatch(&conn, "thread/start", json!({"cwd": cwd})),
    )
    .await
    .expect("thread/start timed out")
    .unwrap();
    println!("thread/start model={} id={}", started["model"], started["thread"]["id"]);
    let _ = PathBuf::new();
}

fn tracing_subscriber_init() -> Option<()> {
    None
}
