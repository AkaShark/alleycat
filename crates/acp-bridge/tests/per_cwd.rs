//! `process_per_cwd`: agents such as mfcli work in their process directory,
//! so every project gets its own process.

mod support;

use serde_json::json;
use support::{Harness, text_input};

fn home() -> String {
    let home = std::env::var("HOME").unwrap();
    std::fs::canonicalize(home).unwrap().display().to_string()
}

async fn start_in(h: &Harness, cwd: &str) -> String {
    let started = h.call("thread/start", json!({"cwd": cwd})).await.unwrap();
    started["thread"]["id"].as_str().unwrap().to_string()
}

async fn turn_text(h: &Harness, sid: &str) -> String {
    let turn = h
        .call(
            "turn/start",
            json!({"threadId": sid, "input": text_input("pwd")}),
        )
        .await
        .unwrap();
    turn["turn"]["items"].to_string()
}

#[tokio::test]
async fn each_project_runs_in_its_own_process() {
    let h = Harness::with(json!({"echo_cwd": true}), |b| b.process_per_cwd(true)).await;
    let a = h.make_dir("proj-a");
    let b = h.make_dir("proj-b");
    h.initialize().await;
    let sid_a = start_in(&h, &a).await;
    let sid_b = start_in(&h, &b).await;

    assert!(
        turn_text(&h, &sid_a)
            .await
            .contains(&format!("ok from {a}"))
    );
    assert!(
        turn_text(&h, &sid_b)
            .await
            .contains(&format!("ok from {b}"))
    );
    let spawned = h.spawned_cwds();
    assert!(spawned.contains(&a), "{spawned:?}");
    assert!(spawned.contains(&b), "{spawned:?}");
    assert!(!spawned.iter().any(|c| c == "/"), "{spawned:?}");
}

#[tokio::test]
async fn without_a_project_the_home_process_is_used_not_root() {
    let h = Harness::with(json!({"echo_cwd": true}), |b| b.process_per_cwd(true)).await;
    h.initialize().await;
    let started = h.call("thread/start", json!({})).await.unwrap();
    let sid = started["thread"]["id"].as_str().unwrap().to_string();
    assert!(
        turn_text(&h, &sid)
            .await
            .contains(&format!("ok from {}", home()))
    );
    assert_eq!(h.spawned_cwds(), vec![home()]);
}

#[tokio::test]
async fn default_mode_keeps_one_process_per_connection() {
    let h = Harness::new(json!({"echo_cwd": true})).await;
    let a = h.make_dir("proj-a");
    let b = h.make_dir("proj-b");
    h.initialize().await;
    start_in(&h, &a).await;
    start_in(&h, &b).await;
    assert_eq!(h.spawned_cwds().len(), 1);
}

#[tokio::test]
async fn recycle_restarts_the_project_process_and_resumes() {
    let h = Harness::with(json!({"echo_cwd": true}), |b| b.process_per_cwd(true)).await;
    let a = h.make_dir("proj-a");
    h.initialize().await;
    let sid = start_in(&h, &a).await;
    h.bridge.recycle_process(&h.conn()).await;
    assert!(turn_text(&h, &sid).await.contains(&format!("ok from {a}")));
    assert_eq!(h.spawned_cwds().iter().filter(|c| **c == a).count(), 2);
    assert_eq!(
        h.last_frame("session/resume")["params"]["sessionId"],
        json!(sid)
    );
}
