mod support;

use serde_json::json;
use support::Harness;

#[tokio::test]
async fn initialize_sends_integer_protocol_version() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let init = h.last_frame("initialize");
    assert_eq!(init["params"]["protocolVersion"], json!(1));
    assert_eq!(
        init["params"]["clientCapabilities"]["terminal"],
        json!(true)
    );
    assert_eq!(
        init["params"]["clientCapabilities"]["fs"]["writeTextFile"],
        json!(true)
    );
}

#[tokio::test]
async fn client_capabilities_are_configurable() {
    let caps = json!({"fs": {"readTextFile": false, "writeTextFile": false}, "terminal": false});
    let h = Harness::with(json!({}), |b| b.client_capabilities(caps.clone())).await;
    h.initialize().await;
    assert_eq!(
        h.last_frame("initialize")["params"]["clientCapabilities"],
        caps
    );
}

#[tokio::test]
async fn agent_error_details_reach_the_caller() {
    let h = Harness::new(json!({"fail": {"session/new": "quota exhausted"}})).await;
    h.initialize().await;
    let err = h
        .call("thread/start", json!({"cwd": h.project_dir()}))
        .await
        .unwrap_err();
    assert!(
        err.message.contains("quota exhausted"),
        "got: {}",
        err.message
    );
}
