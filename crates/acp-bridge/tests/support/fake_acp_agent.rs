//! Test-only ACP agent for acp-bridge integration tests.
//!
//! Mirrors the mfcli (`mfcli acp`) behaviour verified on 2026-09-30:
//! integer `protocolVersion`, `Agent not initialized` before
//! `initialize`, a per-process session table (`Session X not found`),
//! `configOptions` with `currentValue`, and `session/list` filtered by cwd.
//!
//! argv: `--log <path>` appends every inbound line; `--config <path>` is a
//! JSON object with optional keys:
//! - `sessions_by_cwd`: `{ "/abs": [ {sessionId, cwd, title, updatedAt} ] }`
//! - `page_size`: session/list page size (default: everything in one page)
//! - `prompt_delay_ms`: sleep inside session/prompt
//! - `exit_after_prompts`: exit after this many completed prompts
//! - `capabilities`: replaces the default `agentCapabilities`
//! - `fail`: `{ "<method>": "<details>" }` answers that method with an error

use std::collections::{HashMap, HashSet};
use std::io::{self, BufRead, Write};
use std::time::Duration;

use serde_json::{Value, json};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let log = arg(&args, "--log");
    let config = arg(&args, "--config")
        .and_then(|p| std::fs::read_to_string(p).ok())
        .and_then(|s| serde_json::from_str::<Value>(&s).ok())
        .unwrap_or_else(|| json!({}));
    let mut agent = FakeAgent::new(config);
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        if line.trim().is_empty() {
            continue;
        }
        if let Some(path) = &log {
            append(path, &line);
        }
        let Ok(frame) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        agent.handle(frame);
        if agent.should_exit {
            break;
        }
    }
}

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1).cloned())
}

fn append(path: &str, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open fake agent log");
    writeln!(file, "{line}").expect("write fake agent log");
}

fn out(value: Value) {
    let mut stdout = io::stdout().lock();
    writeln!(stdout, "{value}").expect("write stdout");
    stdout.flush().expect("flush stdout");
}

fn ok(id: &Value, result: Value) {
    out(json!({"jsonrpc": "2.0", "id": id, "result": result}));
}

fn err(id: &Value, code: i64, details: &str) {
    let message = if code == -32602 {
        "Invalid params"
    } else {
        "Internal error"
    };
    out(json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message, "data": {"details": details}},
    }));
}

fn update(session_id: &str, update: Value) {
    out(json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {"sessionId": session_id, "update": update},
    }));
}

struct FakeAgent {
    config: Value,
    initialized: bool,
    sessions: HashSet<String>,
    session_config: HashMap<String, HashMap<String, String>>,
    next_session: u64,
    prompts_done: u64,
    should_exit: bool,
}

impl FakeAgent {
    fn new(config: Value) -> Self {
        Self {
            config,
            initialized: false,
            sessions: HashSet::new(),
            session_config: HashMap::new(),
            next_session: 0,
            prompts_done: 0,
            should_exit: false,
        }
    }

    fn current(&self, session_id: &str, config_id: &str, default: &str) -> String {
        self.session_config
            .get(session_id)
            .and_then(|c| c.get(config_id))
            .cloned()
            .unwrap_or_else(|| default.to_string())
    }

    fn config_options(&self, session_id: &str) -> Value {
        json!([
            {
                "id": "model", "name": "Model", "category": "model", "type": "select",
                "currentValue": self.current(session_id, "model", "fake/alpha"),
                "options": [
                    {"name": "Fake Alpha", "value": "fake/alpha"},
                    {"name": "Fake Beta", "value": "fake/beta"}
                ]
            },
            {
                "id": "thought_level", "name": "Thinking", "category": "thought_level", "type": "select",
                "currentValue": self.current(session_id, "thought_level", "low"),
                "options": [
                    {"name": "Low", "value": "low"},
                    {"name": "Medium", "value": "medium"},
                    {"name": "High", "value": "high"},
                    {"name": "Xhigh", "value": "xhigh"}
                ]
            }
        ])
    }

    fn handle(&mut self, frame: Value) {
        let Some(id) = frame.get("id").cloned() else {
            return; // notification (e.g. session/cancel): logged only
        };
        let Some(method) = frame.get("method").and_then(Value::as_str) else {
            return;
        };
        let method = method.to_string();
        let params = frame.get("params").cloned().unwrap_or_else(|| json!({}));

        if method == "initialize" {
            if !params.get("protocolVersion").is_some_and(Value::is_u64) {
                return err(
                    &id,
                    -32602,
                    "protocolVersion: expected number, received string",
                );
            }
            self.initialized = true;
            let caps = self.config.get("capabilities").cloned().unwrap_or_else(|| {
                json!({
                    "loadSession": true,
                    "sessionCapabilities": {"list": {}, "resume": {}},
                    "promptCapabilities": {"image": true, "embeddedContext": true}
                })
            });
            return ok(
                &id,
                json!({"protocolVersion": 1, "agentCapabilities": caps}),
            );
        }
        if !self.initialized {
            return err(&id, -32603, "Agent not initialized");
        }
        if let Some(details) = self
            .config
            .pointer(&format!("/fail/{}", method.replace('/', "~1")))
        {
            return err(&id, -32603, details.as_str().unwrap_or("failure"));
        }
        let session_id = params
            .get("sessionId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        match method.as_str() {
            "session/new" => {
                self.next_session += 1;
                let sid = format!("fake-{}-{}", std::process::id(), self.next_session);
                self.sessions.insert(sid.clone());
                ok(
                    &id,
                    json!({"sessionId": sid, "configOptions": self.config_options(&sid)}),
                );
            }
            "session/load" => {
                self.sessions.insert(session_id.clone());
                update(
                    &session_id,
                    json!({"sessionUpdate": "user_message_chunk", "content": {"type": "text", "text": "earlier question"}}),
                );
                update(
                    &session_id,
                    json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "earlier answer"}}),
                );
                ok(
                    &id,
                    json!({"configOptions": self.config_options(&session_id)}),
                );
            }
            "session/resume" => {
                self.sessions.insert(session_id.clone());
                ok(
                    &id,
                    json!({"configOptions": self.config_options(&session_id)}),
                );
            }
            "session/list" => {
                let cwd = params.get("cwd").and_then(Value::as_str).unwrap_or("");
                let all: Vec<Value> = self
                    .config
                    .pointer("/sessions_by_cwd")
                    .and_then(|m| m.get(cwd))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let start: usize = params
                    .get("cursor")
                    .and_then(Value::as_str)
                    .and_then(|c| c.parse().ok())
                    .unwrap_or(0);
                let size = self
                    .config
                    .get("page_size")
                    .and_then(Value::as_u64)
                    .map(|n| n as usize)
                    .unwrap_or(usize::MAX);
                let start = start.min(all.len());
                let end = start.saturating_add(size).min(all.len());
                let next = (end < all.len()).then(|| end.to_string());
                ok(
                    &id,
                    json!({"sessions": all[start..end], "nextCursor": next}),
                );
            }
            "session/set_config_option" => {
                let config_id = params.get("configId").and_then(Value::as_str).unwrap_or("");
                let value = params.get("value").and_then(Value::as_str).unwrap_or("");
                let options = self.config_options(&session_id);
                let allowed = options
                    .as_array()
                    .into_iter()
                    .flatten()
                    .find(|o| o["id"] == config_id)
                    .and_then(|o| o["options"].as_array())
                    .is_some_and(|opts| opts.iter().any(|o| o["value"] == value));
                if !allowed {
                    return err(
                        &id,
                        -32602,
                        &format!("invalid value {value} for {config_id}"),
                    );
                }
                self.session_config
                    .entry(session_id.clone())
                    .or_default()
                    .insert(config_id.to_string(), value.to_string());
                ok(
                    &id,
                    json!({"configOptions": self.config_options(&session_id)}),
                );
            }
            "session/prompt" => {
                if !self.sessions.contains(&session_id) {
                    return err(&id, -32603, &format!("Session {session_id} not found"));
                }
                if let Some(ms) = self.config.get("prompt_delay_ms").and_then(Value::as_u64) {
                    std::thread::sleep(Duration::from_millis(ms));
                }
                update(
                    &session_id,
                    json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "ok"}}),
                );
                ok(&id, json!({"stopReason": "end_turn"}));
                self.prompts_done += 1;
                if self
                    .config
                    .get("exit_after_prompts")
                    .and_then(Value::as_u64)
                    == Some(self.prompts_done)
                {
                    self.should_exit = true;
                }
            }
            other => err(&id, -32601, &format!("Method not found: {other}")),
        }
    }
}
