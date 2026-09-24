use serde::{Deserialize, Serialize};

pub const PROTOCOL_VERSION: u32 = 1;
pub const ALLEYCAT_ALPN: &[u8] = b"alleycat/1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PairPayload {
    pub v: u32,
    pub node_id: String,
    pub token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relay: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgentWire {
    Websocket,
    Jsonl,
}

impl AgentWire {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Websocket => "websocket",
            Self::Jsonl => "jsonl",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentInfo {
    pub name: String,
    pub display_name: String,
    pub wire: AgentWire,
    pub available: bool,
    /// UI-facing presentation hints. Optional so older alleycat daemons
    /// continue to round-trip without this field; new clients render
    /// generic fallbacks when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub presentation: Option<AgentPresentation>,
    /// Behavioral capability flags that let clients branch on
    /// agent-specific behavior (reasoning-effort lock, visible mode
    /// allowlist, transport eligibility) without hardcoding agent names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<AgentCapabilities>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AgentPresentation {
    /// Longer/formal title used in headers, e.g. "Factory Droid" while
    /// `display_name` is "Droid". Falls back to `display_name` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Whether the BETA badge should be shown next to this agent.
    #[serde(default)]
    pub is_beta: bool,
    /// Ascending sort key. Ties broken by `name`.
    #[serde(default)]
    pub sort_order: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Alternate lowercase names that should also resolve to this agent
    /// (back-compat for clients that persisted a different alias).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct AgentCapabilities {
    /// Amp behavior: reasoning effort is locked once the thread has any
    /// activity. Clients hide / disable the effort selector when true.
    #[serde(default)]
    pub locks_reasoning_effort_after_activity: bool,
    /// Allowlist of mode names the model selector should show. `None`
    /// means no filtering; show all modes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub visible_modes: Option<Vec<String>>,
    /// Agent can be reached via the SSH bridge bootstrap path
    /// (Claude / Pi / Opencode / Codex today).
    #[serde(default)]
    pub supports_ssh_bridge: bool,
    /// Codex-only: the agent speaks the `codex app-server` wire and can
    /// be dialed directly on its TCP port without going through Alleycat.
    #[serde(default)]
    pub uses_direct_codex_port: bool,
    /// Whether a client may send per-thread approval/sandbox overrides and
    /// expect the runtime to enforce them. False for bridges that launch in a
    /// fixed/yolo-like mode or whose upstream agent has no thread permission
    /// profile system.
    #[serde(default)]
    pub supports_thread_permission_overrides: bool,
    /// Whether thread snapshots from this agent contain authoritative
    /// effective approval/sandbox policies. False means clients should not
    /// hydrate or imply permissions from missing/placeholder values.
    #[serde(default)]
    pub reports_effective_thread_permissions: bool,
}

/// Resume hint sent on `Connect` when a reconnecting client wants to
/// reattach to an existing session for `(client_node_id, agent)`. The server
/// keys on the iroh `remote_node_id`, so the client doesn't need to (and
/// can't) carry its own identity here. `last_seq` is the highest seq the
/// client successfully observed on the prior attachment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Resume {
    pub last_seq: u64,
}

/// What `Connect` resolved to on the server side.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttachKind {
    /// No prior session existed; one was minted for this client.
    Fresh,
    /// Prior session reattached and the resume cursor was within the replay
    /// window. The drainer will replay missed frames before going live.
    Resumed,
    /// Prior session existed but the cursor predates the ring floor. The
    /// client must reload state from authoritative storage (e.g. `thread/read`)
    /// before treating this stream as caught up; backlog replay is empty.
    DriftReload,
}

impl From<alleycat_bridge_core::session::AttachKind> for AttachKind {
    fn from(value: alleycat_bridge_core::session::AttachKind) -> Self {
        match value {
            alleycat_bridge_core::session::AttachKind::Fresh => Self::Fresh,
            alleycat_bridge_core::session::AttachKind::Resumed => Self::Resumed,
            alleycat_bridge_core::session::AttachKind::DriftReload => Self::DriftReload,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SessionInfo {
    pub attached: AttachKind,
    pub current_seq: u64,
    pub floor_seq: u64,
}

/// Feature flag advertised in [`HostInfo::features`] by hosts that implement
/// `push_subscribe` / `push_unsubscribe`.
pub const FEATURE_PUSH_V1: &str = "push.v1";

/// Host-level capabilities, attached to `list_agents` responses. Absent on
/// hosts that predate it; clients treat that as "no optional features".
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostInfo {
    #[serde(default)]
    pub features: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push: Option<HostPushInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostPushInfo {
    pub enabled: bool,
    /// Agents whose turn terminal state this host can observe in its current
    /// mode. Empty when push is disabled.
    #[serde(default)]
    pub agents: Vec<String>,
}

/// Where the Worker should deliver the notification (`push_subscribe`).
/// Enum-like fields are plain strings so a bad value yields a `bad_request`
/// response instead of an undecodable first frame.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushTargetWire {
    /// `ios` | `android`.
    pub platform: String,
    /// `sandbox` | `production`; iOS only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apns_environment: Option<String>,
    /// Push token sealed to the Worker (base64url, spec §5.5). Opaque to the
    /// host: stored and forwarded as-is, never parsed or logged.
    pub sealed: String,
}

impl std::fmt::Debug for PushTargetWire {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PushTargetWire")
            .field("platform", &self.platform)
            .field("apns_environment", &self.apns_environment)
            .field("sealed", &format_args!("<{} bytes>", self.sealed.len()))
            .finish()
    }
}

/// Device-signed authorization for one subscription (`push_subscribe`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushGrantWire {
    pub device_id: String,
    pub issued: u64,
    pub expires: u64,
    pub nonce: String,
    pub signature: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    ListAgents {
        v: u32,
        token: String,
    },
    RestartAgent {
        v: u32,
        token: String,
        agent: String,
    },
    Connect {
        v: u32,
        token: String,
        agent: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        resume: Option<Resume>,
    },
    PushSubscribe {
        v: u32,
        token: String,
        agent: String,
        thread_id: String,
        turn_id: String,
        target: PushTargetWire,
        grant: PushGrantWire,
    },
    PushUnsubscribe {
        v: u32,
        token: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        agent: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thread_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        turn_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        all: Option<bool>,
    },
}

impl Request {
    pub fn version(&self) -> u32 {
        match self {
            Self::ListAgents { v, .. }
            | Self::RestartAgent { v, .. }
            | Self::Connect { v, .. }
            | Self::PushSubscribe { v, .. }
            | Self::PushUnsubscribe { v, .. } => *v,
        }
    }

    pub fn token(&self) -> &str {
        match self {
            Self::ListAgents { token, .. }
            | Self::RestartAgent { token, .. }
            | Self::Connect { token, .. }
            | Self::PushSubscribe { token, .. }
            | Self::PushUnsubscribe { token, .. } => token,
        }
    }
}

/// Whether the host has already registered the subscription with the Worker.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushSubscriptionState {
    Pending,
    Registered,
}

/// Terminal notification type (`interrupted` is reported as `failed`).
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PushTerminalType {
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushTerminalWire {
    #[serde(rename = "type")]
    pub kind: PushTerminalType,
    pub occurred_at: u64,
}

/// `push` field of `push_subscribe` / `push_unsubscribe` responses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum PushResponse {
    Subscribe {
        subscription: PushSubscriptionState,
        /// `null` while the turn is still running (or not yet known).
        terminal: Option<PushTerminalWire>,
    },
    Unsubscribe {
        removed: u32,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub v: u32,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<Vec<AgentInfo>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<HostInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push: Option<PushResponse>,
}

impl Response {
    fn base(ok: bool) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            ok,
            agents: None,
            session: None,
            error: None,
            host: None,
            push: None,
        }
    }

    pub fn ok() -> Self {
        Self::base(true)
    }

    pub fn ok_with_session(session: SessionInfo) -> Self {
        Self {
            session: Some(session),
            ..Self::base(true)
        }
    }

    pub fn agents_with_host(agents: Vec<AgentInfo>, host: HostInfo) -> Self {
        Self {
            agents: Some(agents),
            host: Some(host),
            ..Self::base(true)
        }
    }

    pub fn push(push: PushResponse) -> Self {
        Self {
            push: Some(push),
            ..Self::base(true)
        }
    }

    pub fn error(error: impl Into<String>) -> Self {
        Self {
            error: Some(error.into()),
            ..Self::base(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn subscribe_json() -> serde_json::Value {
        json!({
            "op": "push_subscribe",
            "v": 1,
            "token": "tok",
            "agent": "codex",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
            "target": {
                "platform": "ios",
                "apns_environment": "production",
                "sealed": "AQGsAbIgnoY1T7hTI3td"
            },
            "grant": {
                "device_id": "8139770ea87d175f56a35466c34c7ecccb8d8a91b4ee37a25df60f5b8fc9b394",
                "issued": 1790300000u64,
                "expires": 1790386400u64,
                "nonce": "ffeeddccbbaa99887766554433221100",
                "signature": "c0bb"
            }
        })
    }

    #[test]
    fn push_subscribe_parses_snake_case_wire() {
        let request: Request = serde_json::from_value(subscribe_json()).unwrap();
        assert_eq!(request.version(), 1);
        assert_eq!(request.token(), "tok");
        let Request::PushSubscribe {
            agent,
            thread_id,
            turn_id,
            target,
            grant,
            ..
        } = request
        else {
            panic!("expected push_subscribe");
        };
        assert_eq!(agent, "codex");
        assert_eq!(thread_id, "thread-1");
        assert_eq!(turn_id, "turn-1");
        assert_eq!(target.platform, "ios");
        assert_eq!(target.apns_environment.as_deref(), Some("production"));
        assert_eq!(target.sealed, "AQGsAbIgnoY1T7hTI3td");
        // The sealed blob never shows up in logs.
        assert!(!format!("{target:?}").contains("AQGsAbIgnoY1T7hTI3td"));
        assert_eq!(grant.issued, 1790300000);
        assert_eq!(grant.nonce, "ffeeddccbbaa99887766554433221100");
    }

    #[test]
    fn push_subscribe_round_trips_and_android_omits_environment() {
        let mut value = subscribe_json();
        value["target"] = json!({"platform": "android", "sealed": "AQG-android_blob"});
        let request: Request = serde_json::from_value(value.clone()).unwrap();
        let back = serde_json::to_value(&request).unwrap();
        assert_eq!(back, value);
        assert!(back["target"].get("apns_environment").is_none());
    }

    #[test]
    fn push_unsubscribe_fields_are_optional() {
        let request: Request =
            serde_json::from_value(json!({"op": "push_unsubscribe", "v": 1, "token": "t"}))
                .unwrap();
        assert!(matches!(
            request,
            Request::PushUnsubscribe {
                agent: None,
                thread_id: None,
                turn_id: None,
                all: None,
                ..
            }
        ));
        let request: Request = serde_json::from_value(json!({
            "op": "push_unsubscribe", "v": 1, "token": "t", "all": true
        }))
        .unwrap();
        assert!(matches!(
            request,
            Request::PushUnsubscribe {
                all: Some(true),
                ..
            }
        ));
    }

    #[test]
    fn unknown_op_still_fails_to_decode() {
        // Old hosts close the stream on unknown ops; new hosts must keep that
        // behavior for ops they don't know either.
        let err = serde_json::from_value::<Request>(json!({"op": "push_future", "v": 1}));
        assert!(err.is_err());
    }

    #[test]
    fn list_agents_response_carries_host_info() {
        let response = Response::agents_with_host(
            Vec::new(),
            HostInfo {
                features: vec![FEATURE_PUSH_V1.to_string()],
                push: Some(HostPushInfo {
                    enabled: true,
                    agents: vec!["codex".into(), "claude".into()],
                }),
            },
        );
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(
            value,
            json!({
                "v": 1,
                "ok": true,
                "agents": [],
                "host": {
                    "features": ["push.v1"],
                    "push": {"enabled": true, "agents": ["codex", "claude"]}
                }
            })
        );
        let back: Response = serde_json::from_value(value).unwrap();
        assert_eq!(back.host.unwrap().push.unwrap().agents.len(), 2);
    }

    #[test]
    fn list_agents_response_without_host_still_parses() {
        // What an old host sends.
        let response: Response =
            serde_json::from_value(json!({"v": 1, "ok": true, "agents": []})).unwrap();
        assert!(response.host.is_none());
        assert!(response.push.is_none());
    }

    #[test]
    fn old_client_ignores_new_response_fields() {
        // The response struct as it existed before push.v1.
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct OldResponse {
            v: u32,
            ok: bool,
            #[serde(default)]
            agents: Option<Vec<AgentInfo>>,
            #[serde(default)]
            session: Option<SessionInfo>,
            #[serde(default)]
            error: Option<String>,
        }
        let response = Response::agents_with_host(
            Vec::new(),
            HostInfo {
                features: vec![FEATURE_PUSH_V1.to_string()],
                push: None,
            },
        );
        let old: OldResponse =
            serde_json::from_str(&serde_json::to_string(&response).unwrap()).unwrap();
        assert!(old.ok);
        assert_eq!(old.agents.unwrap().len(), 0);
    }

    #[test]
    fn push_subscribe_response_serializes_null_terminal() {
        let pending = Response::push(PushResponse::Subscribe {
            subscription: PushSubscriptionState::Pending,
            terminal: None,
        });
        assert_eq!(
            serde_json::to_value(&pending).unwrap(),
            json!({"v": 1, "ok": true, "push": {"subscription": "pending", "terminal": null}})
        );
        let done = Response::push(PushResponse::Subscribe {
            subscription: PushSubscriptionState::Registered,
            terminal: Some(PushTerminalWire {
                kind: PushTerminalType::Failed,
                occurred_at: 1790300123,
            }),
        });
        let value = serde_json::to_value(&done).unwrap();
        assert_eq!(
            value["push"],
            json!({"subscription": "registered", "terminal": {"type": "failed", "occurred_at": 1790300123u64}})
        );
        let back: Response = serde_json::from_value(value).unwrap();
        assert_eq!(back.push, done.push);
    }

    #[test]
    fn push_unsubscribe_response_round_trips() {
        let response = Response::push(PushResponse::Unsubscribe { removed: 2 });
        let value = serde_json::to_value(&response).unwrap();
        assert_eq!(value["push"], json!({"removed": 2}));
        let back: Response = serde_json::from_value(value).unwrap();
        assert_eq!(back.push, Some(PushResponse::Unsubscribe { removed: 2 }));
    }

    #[test]
    fn error_response_shape_is_unchanged() {
        assert_eq!(
            serde_json::to_value(Response::error("push_unsupported")).unwrap(),
            json!({"v": 1, "ok": false, "error": "push_unsupported"})
        );
    }
}
