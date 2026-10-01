mod support;

use serde_json::json;
use support::{Harness, text_input};

#[tokio::test]
async fn thread_start_reports_agent_current_model() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let started = h
        .call("thread/start", json!({"cwd": h.project_dir()}))
        .await
        .unwrap();
    assert_eq!(started["model"], json!("fake/alpha"));
}

#[tokio::test]
async fn thread_start_applies_requested_model() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let started = h
        .call(
            "thread/start",
            json!({"cwd": h.project_dir(), "model": "fake/beta"}),
        )
        .await
        .unwrap();
    assert_eq!(started["model"], json!("fake/beta"));
}

#[tokio::test]
async fn turn_start_switches_model_and_effort_before_prompting() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    h.run_turn(
        json!({"threadId": sid, "input": text_input("hi"), "model": "fake/beta", "effort": "high"}),
    )
    .await;
    let frames = h.frames();
    let tail: Vec<_> = frames.iter().rev().take(3).rev().collect();
    assert_eq!(tail[0]["method"], "session/set_config_option");
    assert_eq!(tail[0]["params"]["configId"], "model");
    assert_eq!(tail[0]["params"]["value"], "fake/beta");
    assert_eq!(tail[1]["params"]["configId"], "thought_level");
    assert_eq!(tail[1]["params"]["value"], "high");
    assert_eq!(tail[2]["method"], "session/prompt");
}

#[tokio::test]
async fn unchanged_values_send_nothing() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    h.run_turn(
        json!({"threadId": sid, "input": text_input("hi"), "model": "fake/alpha", "effort": "low"}),
    )
    .await;
    assert!(!h.methods().iter().any(|m| m == "session/set_config_option"));
}

#[tokio::test]
async fn unknown_model_is_skipped_and_max_effort_maps_to_xhigh() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    h.run_turn(
        json!({"threadId": sid, "input": text_input("hi"), "model": "fake", "effort": "max"}),
    )
    .await;
    let sets: Vec<_> = h
        .frames()
        .into_iter()
        .filter(|f| f["method"] == "session/set_config_option")
        .collect();
    assert_eq!(sets.len(), 1);
    assert_eq!(sets[0]["params"]["configId"], "thought_level");
    assert_eq!(sets[0]["params"]["value"], "xhigh");
}

#[tokio::test]
async fn thread_resume_reports_agent_current_model() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let resumed = h
        .call(
            "thread/resume",
            json!({"threadId": "old-1", "cwd": h.project_dir()}),
        )
        .await
        .unwrap();
    assert_eq!(resumed["model"], json!("fake/alpha"));
}
