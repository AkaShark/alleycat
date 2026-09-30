mod support;

use std::time::Duration;

use serde_json::json;
use support::{Harness, text_input};

fn tail_after_nth(methods: &[String], name: &str, n: usize) -> Vec<String> {
    let idx = methods
        .iter()
        .enumerate()
        .filter(|(_, m)| *m == name)
        .nth(n)
        .map(|(i, _)| i)
        .unwrap_or_else(|| panic!("no occurrence #{n} of {name} in {methods:?}"));
    methods[idx..].to_vec()
}

#[tokio::test]
async fn reconnect_initialize_is_not_resent_to_live_process() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    h.initialize().await;
    assert_eq!(h.methods().iter().filter(|m| *m == "initialize").count(), 1);
}

#[tokio::test]
async fn respawned_process_is_initialized_and_session_restored() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    h.call(
        "turn/start",
        json!({"threadId": sid, "input": text_input("one")}),
    )
    .await
    .unwrap();

    h.bridge.recycle_process(&h.conn()).await;
    h.call(
        "turn/start",
        json!({"threadId": sid, "input": text_input("two")}),
    )
    .await
    .unwrap();

    assert_eq!(
        tail_after_nth(&h.methods(), "initialize", 1),
        vec!["initialize", "session/resume", "session/prompt"]
    );
    assert_eq!(
        h.last_frame("session/resume")["params"]["cwd"],
        json!(h.project_dir())
    );
}

#[tokio::test]
async fn crashed_process_is_replaced() {
    let h = Harness::new(json!({"exit_after_prompts": 1})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    h.call(
        "turn/start",
        json!({"threadId": sid, "input": text_input("one")}),
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await; // let the reader see EOF

    h.call(
        "turn/start",
        json!({"threadId": sid, "input": text_input("two")}),
    )
    .await
    .unwrap();
    assert_eq!(h.methods().iter().filter(|m| *m == "initialize").count(), 2);
}

#[tokio::test]
async fn thread_resume_marks_session_loaded() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    h.call(
        "thread/resume",
        json!({"threadId": "old-1", "cwd": h.project_dir()}),
    )
    .await
    .unwrap();
    h.call(
        "turn/start",
        json!({"threadId": "old-1", "input": text_input("hi")}),
    )
    .await
    .unwrap();
    let methods = h.methods();
    assert_eq!(
        tail_after_nth(&methods, "session/load", 0),
        vec!["session/load", "session/prompt"]
    );
}

#[tokio::test]
async fn turn_start_uses_request_cwd_when_session_unknown() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    h.call(
        "turn/start",
        json!({"threadId": "from-disk", "cwd": h.project_dir(), "input": text_input("hi")}),
    )
    .await
    .unwrap();
    assert_eq!(
        h.last_frame("session/resume")["params"]["cwd"],
        json!(h.project_dir())
    );
}

#[tokio::test]
async fn agent_without_restore_capability_prompts_directly() {
    let h = Harness::new(json!({"capabilities": {"loadSession": false}})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    h.bridge.recycle_process(&h.conn()).await;
    let err = h
        .call(
            "turn/start",
            json!({"threadId": sid, "input": text_input("hi")}),
        )
        .await
        .unwrap_err();
    assert!(err.message.contains("not found"), "got: {}", err.message);
    assert_eq!(
        tail_after_nth(&h.methods(), "initialize", 1),
        vec!["initialize", "session/prompt"]
    );
}
