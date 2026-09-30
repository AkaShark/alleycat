mod support;

use serde_json::{Value, json};
use support::Harness;

fn ids(list: &Value) -> Vec<String> {
    list["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn placeholder_model_when_discovery_is_off() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let list = h.call("model/list", json!({})).await.unwrap();
    assert_eq!(ids(&list), vec!["fake"]);
}

#[tokio::test]
async fn discovery_fetches_catalog_before_any_thread() {
    let h = Harness::with(json!({}), |b| b.discover_models(true)).await;
    h.initialize().await;
    let list = h.call("model/list", json!({})).await.unwrap();
    assert_eq!(ids(&list), vec!["fake/alpha", "fake/beta"]);
    assert_eq!(list["data"][0]["isDefault"], json!(true));
    let home = std::env::var("HOME").unwrap();
    assert_eq!(h.last_frame("session/new")["params"]["cwd"], json!(home));
}

#[tokio::test]
async fn efforts_and_image_follow_the_agent() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    h.start_thread().await;
    let list = h.call("model/list", json!({})).await.unwrap();
    let model = &list["data"][0];
    let efforts: Vec<_> = model["supportedReasoningEfforts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["reasoningEffort"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(efforts, vec!["low", "medium", "high", "xhigh"]);
    assert_eq!(model["defaultReasoningEffort"], "low");
    assert!(
        model["inputModalities"]
            .as_array()
            .unwrap()
            .contains(&json!("image"))
    );
}

#[tokio::test]
async fn no_image_modality_without_capability() {
    let h = Harness::new(
        json!({"capabilities": {"loadSession": true, "promptCapabilities": {"image": false}}}),
    )
    .await;
    h.initialize().await;
    h.start_thread().await;
    let list = h.call("model/list", json!({})).await.unwrap();
    assert_eq!(list["data"][0]["inputModalities"], json!(["text"]));
}
