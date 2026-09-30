mod support;

use std::sync::Arc;
use std::time::{Duration, Instant};

use alleycat_bridge_core::Bridge;
use serde_json::json;
use support::{Harness, text_input};

fn sessions_config(delay_ms: u64) -> serde_json::Value {
    json!({
        "prompt_delay_ms": delay_ms,
        "sessions_by_cwd": {
            "/tmp/proj-a": [{"sessionId": "a1", "cwd": "/tmp/proj-a", "title": "A one", "updatedAt": "2026-09-30T10:00:00Z"}]
        }
    })
}

#[tokio::test]
async fn thread_list_passes_cwd_and_keeps_it() {
    let h = Harness::new(sessions_config(0)).await;
    h.initialize().await;
    let list = h
        .call("thread/list", json!({"cwd": "/tmp/proj-a"}))
        .await
        .unwrap();
    assert_eq!(list["data"][0]["id"], "a1");
    assert_eq!(list["data"][0]["cwd"], "/tmp/proj-a");
    assert_eq!(h.last_frame("session/list")["params"]["cwd"], "/tmp/proj-a");
}

#[tokio::test]
async fn thread_list_does_not_wait_for_running_prompt() {
    let h = Harness::new(sessions_config(1500)).await;
    h.initialize().await;
    let sid = h.start_thread().await;

    let bridge = Arc::clone(&h.bridge);
    let conn = h.conn();
    let turn = tokio::spawn(async move {
        bridge
            .dispatch(
                &conn,
                "turn/start",
                json!({"threadId": sid, "input": text_input("slow")}),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let started = Instant::now();
    let list = h
        .call("thread/list", json!({"cwd": "/tmp/proj-a"}))
        .await
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "list waited {:?}",
        started.elapsed()
    );
    assert_eq!(list["data"][0]["id"], "a1");
    turn.await.unwrap().unwrap();
}
