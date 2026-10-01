//! Item ids must stay unique across the turns of one thread: the phone
//! upserts items by id, so a reused id overwrites an earlier turn's item.

mod support;

use std::collections::HashSet;

use serde_json::{Value, json};
use support::{Harness, text_input};

fn started_item_ids(frames: &[Value]) -> Vec<String> {
    frames
        .iter()
        .filter(|f| f["method"] == "item/started")
        .filter_map(|f| f["params"]["item"]["id"].as_str().map(str::to_string))
        .collect()
}

#[tokio::test]
async fn item_ids_are_unique_across_turns() {
    let h = Harness::new(json!({})).await;
    h.initialize().await;
    let thread_id = h.start_thread().await;
    for text in ["first", "second"] {
        h.call(
            "turn/start",
            json!({"threadId": thread_id, "input": text_input(text)}),
        )
        .await
        .expect("turn/start");
    }

    let ids = started_item_ids(&h.notifications());
    assert_eq!(ids.len(), 4, "two user + two agent items: {ids:?}");
    let unique: HashSet<&String> = ids.iter().collect();
    assert_eq!(unique.len(), ids.len(), "duplicate item ids: {ids:?}");
}

#[tokio::test]
async fn replayed_history_has_unique_ids() {
    let h = Harness::new(json!({"history": [["q1", "a1"], ["q2", "a2"], ["q3", "a3"]]})).await;
    h.initialize().await;
    let resumed = h
        .call(
            "thread/resume",
            json!({"threadId": "old-1", "cwd": h.project_dir()}),
        )
        .await
        .expect("thread/resume");
    let turns = resumed["thread"]["turns"].as_array().expect("turns");
    assert_eq!(turns.len(), 3, "{turns:?}");
    let turn_ids: HashSet<&str> = turns.iter().filter_map(|t| t["id"].as_str()).collect();
    assert_eq!(turn_ids.len(), 3, "duplicate turn ids: {turns:?}");
    let item_ids: Vec<&str> = turns
        .iter()
        .flat_map(|t| t["items"].as_array().into_iter().flatten())
        .filter_map(|i| i["id"].as_str())
        .collect();
    assert_eq!(item_ids.len(), 6, "{item_ids:?}");
    let unique: HashSet<&&str> = item_ids.iter().collect();
    assert_eq!(
        unique.len(),
        item_ids.len(),
        "duplicate item ids: {item_ids:?}"
    );
}
