//! Regression tests for the whole-branch review findings.

mod support;

use std::sync::Arc;
use std::time::Duration;

use alleycat_bridge_core::session::Session;
use alleycat_bridge_core::{Bridge, Conn};
use serde_json::json;
use support::{Harness, text_input};

fn tail_after_last(methods: &[String], name: &str) -> Vec<String> {
    let idx = methods
        .iter()
        .rposition(|m| m == name)
        .unwrap_or_else(|| panic!("no {name} in {methods:?}"));
    methods[idx..].to_vec()
}

#[tokio::test]
async fn forked_thread_is_prompted_without_restore() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    let fork = h
        .call(
            "thread/fork",
            json!({"threadId": sid, "cwd": h.project_dir()}),
        )
        .await
        .unwrap();
    let fork_id = fork["thread"]["id"].as_str().unwrap().to_string();
    h.call(
        "turn/start",
        json!({"threadId": fork_id, "input": text_input("hi")}),
    )
    .await
    .unwrap();
    assert_eq!(
        tail_after_last(&h.methods(), "session/new"),
        vec!["session/new", "session/prompt"]
    );
    assert_eq!(
        h.bridge.session_cwd(&fork_id).as_deref(),
        Some(h.project_dir().as_str())
    );
}

#[tokio::test]
async fn concurrent_first_use_spawns_one_secondary_process() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let (a, b) = tokio::join!(
        h.call("thread/list", json!({"cwd": "/tmp/none"})),
        h.call("thread/list", json!({"cwd": "/tmp/none"}))
    );
    a.unwrap();
    b.unwrap();
    // one primary + one secondary process
    assert_eq!(h.methods().iter().filter(|m| *m == "initialize").count(), 2);
}

#[tokio::test]
async fn root_placeholder_cwd_is_not_recorded() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    h.call("thread/resume", json!({"threadId": "no-cwd"}))
        .await
        .unwrap();
    assert_eq!(h.last_frame("session/load")["params"]["cwd"], "/");
    assert_eq!(h.bridge.session_cwd("no-cwd"), None);
}

#[tokio::test]
async fn load_only_agent_restores_with_session_load_and_drops_replay() {
    let h = Harness::new(json!({"capabilities": {"loadSession": true}})).await;
    h.initialize().await;
    let sid = h.start_thread().await;
    h.bridge.recycle_process(&h.conn()).await;
    let turn = h
        .call(
            "turn/start",
            json!({"threadId": sid, "input": text_input("hi")}),
        )
        .await
        .unwrap();
    assert_eq!(
        tail_after_last(&h.methods(), "initialize"),
        vec!["initialize", "session/load", "session/prompt"]
    );
    let items = turn["turn"]["items"].to_string();
    assert!(!items.contains("earlier answer"), "replay leaked: {items}");
}

#[tokio::test]
async fn busy_process_is_not_evicted_as_idle() {
    let h = Harness::with(json!({"prompt_delay_ms": 2500}), |b| {
        b.idle_ttl(Duration::from_secs(1))
    })
    .await;
    h.initialize().await;
    let sid = h.start_thread().await;

    let bridge = Arc::clone(&h.bridge);
    let conn = h.conn();
    let turn = tokio::spawn(async move {
        bridge
            .dispatch(
                &conn,
                "turn/start",
                json!({"threadId": sid, "input": text_input("long")}),
            )
            .await
    });
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // A second phone connecting spawns a process, which runs eviction.
    let other = Conn::from_session(Arc::new(Session::new(
        "fake",
        "other-node".into(),
        64,
        1 << 20,
    )));
    h.bridge
        .initialize(&other, json!({"clientInfo": {"name": "t", "version": "0"}}))
        .await
        .unwrap();

    turn.await
        .unwrap()
        .expect("long turn must survive idle eviction");
}
