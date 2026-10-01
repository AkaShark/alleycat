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
