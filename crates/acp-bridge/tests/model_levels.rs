//! Agents whose thinking levels depend on the model (mfcli): the catalog
//! reports each model's own levels, discovery leaves the agent on the model
//! and level it started with, and a turn that switches model checks the
//! requested level against the new model.

mod support;

use serde_json::{Value, json};
use support::{Harness, text_input};

fn per_model_agent() -> Value {
    json!({
        "default_thought": "medium",
        "models": [
            {"value": "m/alpha", "levels": ["low", "medium", "high", "max"]},
            {"value": "m/beta", "levels": ["high", "max"]},
            {"value": "m/gamma", "levels": []},
            {"value": "m/delta", "levels": ["low", "high", "xhigh", "max"]}
        ]
    })
}

fn efforts(model: &Value) -> Vec<String> {
    model["supportedReasoningEfforts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["reasoningEffort"].as_str().unwrap().to_string())
        .collect()
}

fn model<'a>(list: &'a Value, id: &str) -> &'a Value {
    list["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["id"] == id)
        .unwrap_or_else(|| panic!("model {id} missing from {list}"))
}

fn config_sets(h: &Harness) -> Vec<(String, String)> {
    h.frames()
        .into_iter()
        .filter(|f| f["method"] == "session/set_config_option")
        .map(|f| {
            (
                f["params"]["configId"].as_str().unwrap().to_string(),
                f["params"]["value"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[tokio::test]
async fn catalog_reports_each_models_own_levels() {
    let h = Harness::with(per_model_agent(), |b| b.discover_models(true)).await;
    h.initialize().await;
    let list = h.call("model/list", json!({})).await.unwrap();

    let alpha = model(&list, "m/alpha");
    // The phone has no `max` (it drops unknown levels), so a model's `max`
    // is offered as `xhigh`...
    assert_eq!(efforts(alpha), vec!["low", "medium", "high", "xhigh"]);
    assert_eq!(alpha["defaultReasoningEffort"], "medium");
    assert_eq!(alpha["isDefault"], json!(true));

    let beta = model(&list, "m/beta");
    assert_eq!(efforts(beta), vec!["high", "xhigh"]);
    assert_eq!(beta["defaultReasoningEffort"], "high");

    assert!(efforts(model(&list, "m/gamma")).is_empty());

    // ...unless the model has a real `xhigh` too; then `max` is left out.
    assert_eq!(
        efforts(model(&list, "m/delta")),
        vec!["low", "high", "xhigh"]
    );
}

#[tokio::test]
async fn discovery_leaves_the_starting_model_and_level() {
    let h = Harness::with(per_model_agent(), |b| b.discover_models(true)).await;
    h.initialize().await;
    h.call("model/list", json!({})).await.unwrap();

    // mfcli saves every switch as the user's default, so the last two
    // switches must put back what the session started with.
    let sets = config_sets(&h);
    let tail = &sets[sets.len() - 2..];
    assert_eq!(
        tail,
        &[
            ("model".to_string(), "m/alpha".to_string()),
            ("thought_level".to_string(), "medium".to_string()),
        ]
    );
}

#[tokio::test]
async fn turn_checks_effort_against_the_new_model() {
    let h = Harness::new(per_model_agent()).await;
    h.initialize().await;
    let sid = h.start_thread().await;

    // beta has no xhigh: the phone's top level becomes beta's `max`.
    h.run_turn(
        json!({"threadId": sid, "input": text_input("one"), "model": "m/beta", "effort": "xhigh"}),
    )
    .await;
    assert_eq!(
        config_sets(&h),
        vec![
            ("model".to_string(), "m/beta".to_string()),
            ("thought_level".to_string(), "max".to_string()),
        ]
    );

    // beta has no `low`, but the turn switches to alpha, which does.
    h.run_turn(
        json!({"threadId": sid, "input": text_input("two"), "model": "m/alpha", "effort": "low"}),
    )
    .await;
    assert_eq!(
        &config_sets(&h)[2..],
        &[
            ("model".to_string(), "m/alpha".to_string()),
            ("thought_level".to_string(), "low".to_string()),
        ]
    );
}

#[tokio::test]
async fn catalog_is_complete_when_a_thread_came_first() {
    // On the phone, reconnecting resumes threads before it lists models,
    // so the catalog is already known when model/list arrives.
    let h = Harness::with(per_model_agent(), |b| b.discover_models(true)).await;
    h.initialize().await;
    h.start_thread().await;
    let list = h.call("model/list", json!({})).await.unwrap();
    assert_eq!(efforts(model(&list, "m/beta")), vec!["high", "xhigh"]);
    assert!(efforts(model(&list, "m/gamma")).is_empty());

    // The walk happens once, not on every model/list.
    let walked = config_sets(&h).len();
    h.call("model/list", json!({})).await.unwrap();
    assert_eq!(config_sets(&h).len(), walked);
}

#[tokio::test]
async fn thread_start_waits_for_discovery() {
    // mfcli keeps one saved default, which every switch rewrites: a thread
    // started mid-walk would begin on whichever model the walk is on.
    let mut config = per_model_agent();
    config["config_delay_ms"] = json!(100);
    let h = Harness::with(config, |b| b.discover_models(true)).await;
    h.initialize().await;
    let start_later = async {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        h.call("thread/start", json!({"cwd": h.project_dir()}))
            .await
            .unwrap()
    };
    let (list, _) = tokio::join!(h.call("model/list", json!({})), start_later);
    list.unwrap();

    let frames = h.frames();
    let thread_new = frames
        .iter()
        .position(|f| f["method"] == "session/new" && f["params"]["cwd"] == json!(h.project_dir()))
        .expect("thread's session/new");
    let last_switch = frames
        .iter()
        .rposition(|f| f["method"] == "session/set_config_option")
        .expect("discovery switches");
    assert!(
        thread_new > last_switch,
        "thread/start ran during discovery: session/new at {thread_new}, last switch at {last_switch}"
    );
}

#[tokio::test]
async fn without_discovery_every_model_keeps_the_shared_levels() {
    // Devin / Grok: no discovery, so a session on a model without levels
    // must not give that one model an empty list while the rest differ.
    let mut config = per_model_agent();
    config["models"] = json!([
        {"value": "m/gamma", "levels": []},
        {"value": "m/alpha", "levels": ["low", "high"]}
    ]);
    let h = Harness::new(config).await;
    h.initialize().await;
    h.start_thread().await;
    let list = h.call("model/list", json!({})).await.unwrap();
    for id in ["m/gamma", "m/alpha"] {
        assert_eq!(efforts(model(&list, id)), vec!["medium"], "{id}");
    }
}
