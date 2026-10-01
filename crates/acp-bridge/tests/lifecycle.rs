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
    h.run_turn(json!({"threadId": sid, "input": text_input("one")}))
        .await;

    h.bridge.recycle_process(&h.conn()).await;
    h.run_turn(json!({"threadId": sid, "input": text_input("two")}))
        .await;

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
    h.run_turn(json!({"threadId": sid, "input": text_input("one")}))
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await; // let the reader see EOF

    h.run_turn(json!({"threadId": sid, "input": text_input("two")}))
        .await;
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
    h.run_turn(json!({"threadId": "old-1", "input": text_input("hi")}))
        .await;
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
    h.run_turn(json!({"threadId": "from-disk", "cwd": h.project_dir(), "input": text_input("hi")}))
        .await;
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
    // The turn starts, then fails: the prompt error ends it.
    let turn = h
        .run_turn(json!({"threadId": sid, "input": text_input("hi")}))
        .await;
    assert_eq!(turn["turn"]["status"], "failed");
    let message = turn["turn"]["error"]["message"]
        .as_str()
        .unwrap_or_default();
    assert!(message.contains("not found"), "got: {message}");
    let status = h
        .notifications()
        .into_iter()
        .rev()
        .find(|f| f["method"] == "thread/status/changed")
        .expect("thread/status/changed");
    assert_eq!(status["params"]["status"]["type"], "idle");
    assert_eq!(
        tail_after_nth(&h.methods(), "initialize", 1),
        vec!["initialize", "session/prompt"]
    );
}
