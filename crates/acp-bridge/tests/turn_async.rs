//! `turn/start` answers as soon as the turn is running, like Codex: the
//! prompt runs in the background and the turn ends with `turn/completed`.
//! A phone waits for the `turn/start` answer before it opens a new task,
//! and it reads no events while a request is pending.

mod support;

use std::time::{Duration, Instant};

use serde_json::json;
use support::{Harness, text_input};

#[tokio::test]
async fn turn_start_returns_while_the_prompt_runs() {
    let h = Harness::new(json!({"prompt_delay_ms": 1500})).await;
    h.initialize().await;
    let sid = h.start_thread().await;

    let started = Instant::now();
    let response = h
        .call(
            "turn/start",
            json!({"threadId": sid, "input": text_input("hi")}),
        )
        .await
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "turn/start waited {:?}",
        started.elapsed()
    );
    assert_eq!(response["turn"]["status"], "inProgress");

    let turn = h.wait_turn_completed(&response["turn"]["id"]).await;
    assert_eq!(turn["status"], "completed");
    assert!(turn["items"].to_string().contains("\"ok\""), "{turn}");
}

#[tokio::test]
async fn thread_resume_during_a_turn_returns_the_running_turn() {
    let h = Harness::new(json!({"prompt_delay_ms": 1500})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    let response = h
        .call(
            "turn/start",
            json!({"threadId": sid, "input": text_input("hi")}),
        )
        .await
        .unwrap();
    let turn_id = response["turn"]["id"].clone();

    let started = Instant::now();
    let resumed = h
        .call(
            "thread/resume",
            json!({"threadId": sid, "cwd": h.project_dir()}),
        )
        .await
        .unwrap();
    assert!(
        started.elapsed() < Duration::from_millis(1000),
        "thread/resume waited {:?}",
        started.elapsed()
    );
    assert_eq!(resumed["thread"]["status"]["type"], "active");
    let running = resumed["thread"]["turns"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(running["id"], turn_id);
    assert_eq!(running["status"], "inProgress");
    assert_eq!(running["items"][0]["type"], "userMessage");
    assert!(!h.methods().iter().any(|m| m == "session/load"));

    h.wait_turn_completed(&turn_id).await;
    let resumed = h
        .call(
            "thread/resume",
            json!({"threadId": sid, "cwd": h.project_dir()}),
        )
        .await
        .unwrap();
    assert_eq!(resumed["thread"]["status"]["type"], "idle");
    let last = resumed["thread"]["turns"]
        .as_array()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    assert_eq!(last["id"], turn_id);
    assert_eq!(last["status"], "completed");
}

#[tokio::test]
async fn thread_stays_active_until_its_last_turn_ends() {
    let h = Harness::new(json!({"prompt_delay_ms": 300})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    let mut turn_ids = Vec::new();
    for text in ["one", "two"] {
        let response = h
            .call(
                "turn/start",
                json!({"threadId": sid, "input": text_input(text)}),
            )
            .await
            .unwrap();
        turn_ids.push(response["turn"]["id"].clone());
    }
    for id in &turn_ids {
        assert_eq!(h.wait_turn_completed(id).await["status"], "completed");
    }

    // Idle comes after the second turn ends, not after the first.
    let frames = h.notifications();
    let position = |pred: &dyn Fn(&serde_json::Value) -> bool| {
        frames.iter().position(|f| pred(f)).expect("frame")
    };
    let idle = position(&|f| {
        f["method"] == "thread/status/changed" && f["params"]["status"]["type"] == "idle"
    });
    let second_completed =
        position(&|f| f["method"] == "turn/completed" && f["params"]["turn"]["id"] == turn_ids[1]);
    assert!(
        idle > second_completed,
        "idle at {idle}, second turn ended at {second_completed}"
    );
}
