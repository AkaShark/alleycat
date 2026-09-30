//! Host-reported turn completion notifications (design spec
//! `2026-09-24-host-push-notifications-design.md`, §6).
//!
//! Paired devices subscribe to a turn over the existing iroh channel
//! (`push_subscribe`, authorized by a device-signed grant). The host watches
//! for the turn's authoritative terminal state — the Codex side-channel
//! watcher ([`codex`]) or the bridge session tap ([`bridge`]) — and reports it
//! to the push Worker through a persistent, host-signed outbox ([`store`],
//! [`client`], [`signing`]). The Worker never sees conversation content: only
//! ids and the terminal type.

pub mod bridge;
pub mod client;
pub mod codex;
pub mod signing;
pub mod store;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwap;
use iroh::SecretKey;
use rand::Rng;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::HostConfig;
use crate::protocol::{
    AgentInfo, FEATURE_PUSH_V1, HostInfo, HostPushInfo, PushGrantWire, PushResponse,
    PushSubscriptionState, PushTargetWire, PushTerminalType, PushTerminalWire,
};

use self::client::{Delivery, WorkerTarget, parse_worker_url};
use self::codex::{CodexConnector, CodexWatcherHandle, WatcherTiming};
use self::signing::{GrantClaims, random_hex32, verify_grant};
use self::store::{
    ApnsEnvironment, EventBody, FailureReason, OutboxAction, OutboxItem, OutboxPolicy, Platform,
    PushData, PushFiles, RecordedTerminal, Subscription, SubscriptionTerminal, Terminal,
    TerminalKind, TurnKey,
};

/// Bridge agents whose `turn/completed` frames flow through a bridge-core
/// session (shell has no turns).
pub const BRIDGE_PUSH_AGENTS: &[&str] = &[
    "claude", "pi", "opencode", "amp", "droid", "hermes", "devin", "grok", "mfcli",
];

const MAX_SUBSCRIPTIONS: usize = 1000;
const MAX_ID_BYTES: usize = 128;
const MAX_AGENT_LEN: usize = 64;
/// Sealed targets are ~420 base64url characters; the Worker caps bodies at
/// 16 KB.
const MAX_SEALED_TARGET_LEN: usize = 4096;
/// How long `push_subscribe` waits for the Codex watcher's first verdict.
const CODEX_FIRST_CHECK_WAIT: Duration = Duration::from_secs(2);
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(60);

pub mod error_codes {
    pub const UNSUPPORTED: &str = "push_unsupported";
    pub const BAD_REQUEST: &str = "bad_request";
    pub const INVALID_GRANT: &str = "invalid_grant";
}

/// Error returned to the phone as `Response::error(code)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushError {
    pub code: &'static str,
    pub detail: String,
}

impl PushError {
    fn new(code: &'static str, detail: impl Into<String>) -> Self {
        Self {
            code,
            detail: detail.into(),
        }
    }

    fn unsupported(detail: impl Into<String>) -> Self {
        Self::new(error_codes::UNSUPPORTED, detail)
    }

    fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(error_codes::BAD_REQUEST, detail)
    }

    fn invalid_grant(detail: impl Into<String>) -> Self {
        Self::new(error_codes::INVALID_GRANT, detail)
    }
}

/// A terminal state observed by a watcher.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalReport {
    pub agent: String,
    pub thread_id: String,
    pub turn_id: String,
    pub terminal: Terminal,
}

impl TerminalReport {
    fn key(&self) -> TurnKey {
        (
            self.agent.clone(),
            self.thread_id.clone(),
            self.turn_id.clone(),
        )
    }
}

pub fn terminal_channel() -> (
    mpsc::UnboundedSender<TerminalReport>,
    mpsc::UnboundedReceiver<TerminalReport>,
) {
    mpsc::unbounded_channel()
}

pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn unix_now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Turn timestamps are unix seconds; tolerate millisecond values.
pub fn normalize_unix_secs(value: &Value) -> Option<u64> {
    let raw = value
        .as_u64()
        .or_else(|| value.as_f64().map(|f| f as u64))?;
    Some(if raw > 100_000_000_000 {
        raw / 1000
    } else {
        raw
    })
}

/// Map a codex-wire `TurnStatus` to a terminal (`None` for `inProgress`).
pub fn terminal_from_turn_status(status: &str, completed_at: Option<&Value>) -> Option<Terminal> {
    let at = completed_at
        .and_then(normalize_unix_secs)
        .filter(|&secs| secs > 0)
        .unwrap_or_else(unix_now);
    match status {
        "completed" => Some(Terminal::completed(at)),
        "failed" => Some(Terminal::failed(FailureReason::Error, at)),
        "interrupted" => Some(Terminal::failed(FailureReason::Interrupted, at)),
        _ => None,
    }
}

/// Snapshot for `push_status` / `status --json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushStatus {
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_host: Option<String>,
    pub subscriptions: usize,
    pub outbox_depth: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_success_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_at: Option<u64>,
}

pub struct SubscribeRequest {
    pub agent: String,
    pub thread_id: String,
    pub turn_id: String,
    pub target: PushTargetWire,
    pub grant: PushGrantWire,
}

pub struct UnsubscribeRequest {
    pub agent: Option<String>,
    pub thread_id: Option<String>,
    pub turn_id: Option<String>,
    pub all: bool,
}

/// Everything needed to start the push service.
pub struct PushParams {
    pub config: Arc<ArcSwap<HostConfig>>,
    pub secret_key: SecretKey,
    pub files: PushFiles,
    /// `App::push_worker_url`, used when `[push] worker_url` is unset.
    pub app_worker_url: Option<String>,
    pub reports_tx: mpsc::UnboundedSender<TerminalReport>,
    pub reports_rx: mpsc::UnboundedReceiver<TerminalReport>,
    /// `None` when Codex terminal states can't be observed (Stdio mode,
    /// codex unavailable, non-unix UDS modes).
    pub codex: Option<Arc<dyn CodexConnector>>,
    pub policy: OutboxPolicy,
    pub watcher_timing: WatcherTiming,
}

#[derive(Debug, Default, Clone)]
struct RuntimeStatus {
    last_success_at: Option<u64>,
    last_error: Option<String>,
    last_error_at: Option<u64>,
    config_error: Option<String>,
}

struct PushState {
    data: PushData,
    /// Outbox item currently being sent (not persisted).
    in_flight: Option<String>,
}

struct Effective {
    enabled: bool,
    target: Option<WorkerTarget>,
}

enum Job {
    Register {
        item_id: String,
        sub: Box<Subscription>,
    },
    Event {
        item_id: String,
        subscription: String,
        body: EventBody,
    },
    Revoke {
        item_id: String,
        subscription: Option<String>,
        remote_id: String,
    },
}

impl Job {
    fn item_id(&self) -> &str {
        match self {
            Self::Register { item_id, .. }
            | Self::Event { item_id, .. }
            | Self::Revoke { item_id, .. } => item_id,
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Register { .. } => "register",
            Self::Event { .. } => "event",
            Self::Revoke { .. } => "revoke",
        }
    }
}

/// `POST /v2/subscriptions` body (spec §7.1, v2).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct RegisterBody<'a> {
    device_id: &'a str,
    platform: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    apns_environment: Option<&'static str>,
    sealed_target: &'a str,
    agent: &'a str,
    thread_id: &'a str,
    turn_id: &'a str,
    issued_at: u64,
    expires_at: u64,
    grant_nonce: &'a str,
    grant_signature: &'a str,
}

impl<'a> RegisterBody<'a> {
    fn from_subscription(sub: &'a Subscription) -> Self {
        Self {
            device_id: &sub.device_id,
            platform: sub.platform.as_str(),
            apns_environment: sub.apns_environment.map(ApnsEnvironment::as_str),
            sealed_target: &sub.sealed_target,
            agent: &sub.agent,
            thread_id: &sub.thread_id,
            turn_id: &sub.turn_id,
            issued_at: sub.issued_at,
            expires_at: sub.expires_at,
            grant_nonce: &sub.grant_nonce,
            grant_signature: &sub.grant_signature,
        }
    }
}

struct PushCore {
    config: Arc<ArcSwap<HostConfig>>,
    app_worker_url: Option<String>,
    secret_key: SecretKey,
    host_id: String,
    files: PushFiles,
    policy: OutboxPolicy,
    state: Mutex<PushState>,
    wake: Notify,
    http: reqwest::Client,
    codex: Option<CodexWatcherHandle>,
    runtime: std::sync::Mutex<RuntimeStatus>,
}

/// Cheap-to-clone handle used by the iroh handler and the control socket.
#[derive(Clone)]
pub struct PushService {
    core: Arc<PushCore>,
    control: Arc<ServiceControl>,
}

struct ServiceControl {
    shutdown_tx: watch::Sender<bool>,
    delivery: std::sync::Mutex<Option<JoinHandle<()>>>,
    background: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl PushService {
    pub async fn start(params: PushParams) -> Self {
        let PushParams {
            config,
            secret_key,
            files,
            app_worker_url,
            reports_tx,
            reports_rx,
            codex,
            policy,
            watcher_timing,
        } = params;
        let now = unix_now();
        let mut data = files.load(now).await;
        if reconcile(&mut data, now) {
            persist_all(&files, &data).await;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut background = Vec::new();
        let codex_handle = codex.map(|connector| {
            let (handle, task) = codex::spawn_codex_watcher(
                connector,
                reports_tx.clone(),
                shutdown_rx.clone(),
                watcher_timing,
            );
            background.push(task);
            handle
        });

        // Never follow redirects: the signature covers the original origin
        // and path, and the headers must not leak to another host.
        let http = reqwest::Client::builder()
            .user_agent(format!(
                "{}/{} push",
                crate::binary_name(),
                crate::binary_version()
            ))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|error| {
                warn!("building push HTTP client failed ({error}); using defaults without a user agent");
                reqwest::Client::builder()
                    .redirect(reqwest::redirect::Policy::none())
                    .build()
                    .unwrap_or_default()
            });

        let host_id = secret_key.public().to_string();
        let core = Arc::new(PushCore {
            config,
            app_worker_url,
            secret_key,
            host_id,
            files,
            policy,
            state: Mutex::new(PushState {
                data,
                in_flight: None,
            }),
            wake: Notify::new(),
            http,
            codex: codex_handle,
            runtime: std::sync::Mutex::new(RuntimeStatus::default()),
        });

        core.housekeeping().await;
        core.resume_pending().await;

        background.push(tokio::spawn(run_reports(
            Arc::clone(&core),
            reports_rx,
            shutdown_rx.clone(),
        )));
        let delivery = tokio::spawn(run_delivery(Arc::clone(&core), shutdown_rx));

        let status = core.effective();
        info!(
            enabled = status.enabled,
            worker = status
                .target
                .as_ref()
                .and_then(WorkerTarget::host)
                .as_deref()
                .unwrap_or("<none>"),
            codex = core.codex.is_some(),
            "push service started"
        );

        Self {
            core,
            control: Arc::new(ServiceControl {
                shutdown_tx,
                delivery: std::sync::Mutex::new(Some(delivery)),
                background: std::sync::Mutex::new(background),
            }),
        }
    }

    /// `host` field for `list_agents` responses.
    pub fn host_info(&self, agents: &[AgentInfo]) -> HostInfo {
        self.core.host_info(agents)
    }

    pub async fn subscribe(
        &self,
        node_id: &str,
        request: SubscribeRequest,
    ) -> Result<PushResponse, PushError> {
        self.core.subscribe(node_id, request).await
    }

    pub async fn unsubscribe(
        &self,
        node_id: &str,
        request: UnsubscribeRequest,
    ) -> Result<PushResponse, PushError> {
        self.core
            .unsubscribe(node_id, request)
            .await
            .map(|removed| PushResponse::Unsubscribe { removed })
    }

    pub async fn status(&self) -> PushStatus {
        self.core.status().await
    }

    /// Re-read `[push]` now (after `reload`).
    pub fn config_changed(&self) {
        self.core.wake.notify_one();
    }

    /// Stop watchers, give the outbox one bounded flush of due items, then
    /// abort whatever is left. Pending items stay on disk for the next start.
    pub async fn shutdown(&self, deadline: Duration) {
        let _ = self.control.shutdown_tx.send(true);
        let delivery = self.control.delivery.lock().unwrap().take();
        if let Some(delivery) = delivery {
            let abort = delivery.abort_handle();
            if tokio::time::timeout(deadline, delivery).await.is_err() {
                warn!("push outbox flush did not finish in time; aborting");
                abort.abort();
            }
        }
        for task in self.control.background.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

async fn run_reports(
    core: Arc<PushCore>,
    mut reports: mpsc::UnboundedReceiver<TerminalReport>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            report = reports.recv() => match report {
                Some(report) => core.record_terminal(report).await,
                None => break,
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    // Bridge terminals have no re-check path: record what is
                    // already queued so the events persist for the next run.
                    while let Ok(report) = reports.try_recv() {
                        core.record_terminal(report).await;
                    }
                    break;
                }
            }
        }
    }
}

async fn run_delivery(core: Arc<PushCore>, mut shutdown: watch::Receiver<bool>) {
    let mut next_housekeeping = tokio::time::Instant::now() + HOUSEKEEPING_INTERVAL;
    loop {
        if *shutdown.borrow() {
            break;
        }
        if tokio::time::Instant::now() >= next_housekeeping {
            core.housekeeping().await;
            next_housekeeping = tokio::time::Instant::now() + HOUSEKEEPING_INTERVAL;
        }
        let effective = core.effective();
        if effective.enabled
            && let Some(target) = effective.target.as_ref()
            && core.delivery_step(target).await
        {
            continue;
        }
        let wake_at = core.next_wake(next_housekeeping, effective.enabled).await;
        tokio::select! {
            _ = core.wake.notified() => {}
            _ = tokio::time::sleep_until(wake_at) => {}
            changed = shutdown.changed() => {
                // A dropped sender (service dropped without shutdown) must
                // not turn this into a busy loop.
                if changed.is_err() {
                    break;
                }
            }
        }
    }
    core.flush().await;
}

impl PushCore {
    fn effective(&self) -> Effective {
        let cfg = self.config.load();
        let raw = match cfg.push.worker_url.as_deref() {
            Some(url) => Some(url.to_string()),
            None => self.app_worker_url.clone(),
        };
        let raw = raw.filter(|url| !url.trim().is_empty());
        let target = raw.and_then(|url| match parse_worker_url(&url) {
            Ok(base_url) => {
                self.runtime.lock().unwrap().config_error = None;
                Some(WorkerTarget {
                    base_url,
                    timeout: Duration::from_secs(cfg.push.request_timeout_secs.max(1)),
                })
            }
            Err(error) => {
                let mut runtime = self.runtime.lock().unwrap();
                if runtime.config_error.as_deref() != Some(error.as_str()) {
                    warn!("push disabled: {error}");
                    runtime.config_error = Some(error);
                }
                None
            }
        });
        Effective {
            enabled: cfg.push.enabled && target.is_some(),
            target,
        }
    }

    fn agent_supported(&self, agent: &str) -> bool {
        if !self.effective().enabled {
            return false;
        }
        let cfg = self.config.load();
        match agent {
            codex::CODEX_AGENT => cfg.agents.codex.enabled && self.codex.is_some(),
            other => BRIDGE_PUSH_AGENTS.contains(&other) && cfg.agents.enabled_by_name(other),
        }
    }

    fn host_info(&self, agents: &[AgentInfo]) -> HostInfo {
        let enabled = self.effective().enabled;
        let agents = if enabled {
            agents
                .iter()
                .filter(|agent| agent.available && self.agent_supported(&agent.name))
                .map(|agent| agent.name.clone())
                .collect()
        } else {
            Vec::new()
        };
        HostInfo {
            features: vec![FEATURE_PUSH_V1.to_string()],
            push: Some(HostPushInfo { enabled, agents }),
        }
    }

    async fn status(&self) -> PushStatus {
        let effective = self.effective();
        let (subscriptions, outbox_depth) = {
            let state = self.state.lock().await;
            (
                state.data.subscriptions.len(),
                state.data.outbox.items.len(),
            )
        };
        let runtime = self.runtime.lock().unwrap().clone();
        PushStatus {
            enabled: effective.enabled,
            worker_host: effective.target.as_ref().and_then(WorkerTarget::host),
            subscriptions,
            outbox_depth,
            last_success_at: runtime.last_success_at,
            last_error: runtime.last_error.or(runtime.config_error),
            last_error_at: runtime.last_error_at,
        }
    }

    fn note_success(&self) {
        self.runtime.lock().unwrap().last_success_at = Some(unix_now());
    }

    fn note_error(&self, error: &str) {
        let mut runtime = self.runtime.lock().unwrap();
        runtime.last_error = Some(error.to_string());
        runtime.last_error_at = Some(unix_now());
    }

    // --- subscribe / unsubscribe ------------------------------------------

    async fn subscribe(
        &self,
        node_id: &str,
        request: SubscribeRequest,
    ) -> Result<PushResponse, PushError> {
        // Malformed agent names are a bad request, not "unsupported".
        check_agent(&request.agent)?;
        if !self.agent_supported(&request.agent) {
            return Err(PushError::unsupported(format!(
                "push is not available for agent `{}`",
                request.agent
            )));
        }
        let Some(target) = self.effective().target else {
            return Err(PushError::unsupported("push worker is not configured"));
        };
        let now = unix_now();
        let valid = validate_subscribe(&request, node_id, &self.host_id, &target.origin(), now)?;
        let key: TurnKey = (
            request.agent.clone(),
            request.thread_id.clone(),
            request.turn_id.clone(),
        );

        let (sub_id, known_terminal) = {
            let mut state = self.state.lock().await;
            let in_flight = state.in_flight.clone();
            let data = &mut state.data;
            let existing = data
                .subscriptions
                .values()
                .find(|s| {
                    !s.revoking
                        && s.device_id == node_id
                        && s.matches_turn(&request.agent, &request.thread_id, &request.turn_id)
                })
                .cloned();
            // Sealing is randomized, so an identical blob means the very
            // same request was retried; anything else replaces the old grant.
            let reuse = existing
                .as_ref()
                .is_some_and(|s| s.sealed_target == request.target.sealed);
            if reuse {
                let existing = existing.expect("checked above");
                (existing.id.clone(), existing.terminal.map(|t| t.terminal))
            } else {
                if let Some(old) = existing {
                    // New grant/target: retire the old registration first
                    // (the Worker dedups on the subscription key).
                    retire_subscription(data, &old.id, in_flight.as_deref(), now, &self.policy);
                }
                if data.subscriptions.values().filter(|s| !s.revoking).count() >= MAX_SUBSCRIPTIONS
                    && let Some(oldest) = data
                        .subscriptions
                        .values()
                        .filter(|s| !s.revoking)
                        .min_by_key(|s| s.created_at)
                        .map(|s| s.id.clone())
                {
                    warn!("push subscription cap reached; evicting the oldest subscription");
                    drop_subscription(data, &oldest, in_flight.as_deref());
                }
                let sub = Subscription {
                    id: format!("hs_{}", random_hex32()),
                    device_id: node_id.to_string(),
                    agent: request.agent.clone(),
                    thread_id: request.thread_id.clone(),
                    turn_id: request.turn_id.clone(),
                    platform: valid.platform,
                    sealed_target: request.target.sealed.clone(),
                    apns_environment: valid.environment,
                    issued_at: request.grant.issued,
                    expires_at: request.grant.expires,
                    grant_nonce: request.grant.nonce.clone(),
                    grant_signature: request.grant.signature.clone(),
                    created_at: now,
                    remote_id: None,
                    register_attempted: false,
                    revoking: false,
                    terminal: None,
                };
                let sub_id = sub.id.clone();
                let chain = sub.chain();
                data.subscriptions.insert(sub_id.clone(), sub);
                enqueue(
                    data,
                    OutboxItem::new(
                        chain,
                        OutboxAction::Register {
                            subscription: sub_id.clone(),
                        },
                        now,
                    ),
                    &self.policy,
                    in_flight.as_deref(),
                );
                // Finished before we subscribed? (spec: 15-minute memory)
                let recent = data.recent.get(&key).cloned();
                if let Some(recent) = recent.as_ref() {
                    attach_terminal(
                        data,
                        &sub_id,
                        recent,
                        now,
                        &self.policy,
                        in_flight.as_deref(),
                    );
                }
                self.persist(&state.data, false).await;
                info!(
                    agent = %request.agent,
                    thread = %request.thread_id,
                    turn = %request.turn_id,
                    device = %short(node_id),
                    known_terminal = recent.is_some(),
                    "push subscription accepted"
                );
                (sub_id, recent.map(|r| r.terminal))
            }
        };
        self.wake.notify_one();

        if known_terminal.is_none()
            && request.agent == codex::CODEX_AGENT
            && let Some(codex) = self.codex.as_ref()
            && let Some(terminal) = codex
                .track_and_wait(&request.thread_id, &request.turn_id, CODEX_FIRST_CHECK_WAIT)
                .await
        {
            self.record_terminal(TerminalReport {
                agent: request.agent.clone(),
                thread_id: request.thread_id.clone(),
                turn_id: request.turn_id.clone(),
                terminal,
            })
            .await;
        }

        Ok(self.subscription_response(&sub_id, &key).await)
    }

    async fn subscription_response(&self, sub_id: &str, key: &TurnKey) -> PushResponse {
        let state = self.state.lock().await;
        let (subscription, terminal) = match state.data.subscriptions.get(sub_id) {
            Some(sub) => (
                if sub.remote_id.is_some() {
                    PushSubscriptionState::Registered
                } else {
                    PushSubscriptionState::Pending
                },
                sub.terminal.as_ref().map(|t| t.terminal),
            ),
            // Gone already: delivered (terminal remembered) or dropped
            // (e.g. rejected by the Worker).
            None => match state.data.recent.get(key) {
                Some(recent) => (PushSubscriptionState::Registered, Some(recent.terminal)),
                None => (PushSubscriptionState::Pending, None),
            },
        };
        PushResponse::Subscribe {
            subscription,
            terminal: terminal.map(|t| PushTerminalWire {
                kind: match t.kind {
                    TerminalKind::Completed => PushTerminalType::Completed,
                    TerminalKind::Failed => PushTerminalType::Failed,
                },
                occurred_at: t.occurred_at,
            }),
        }
    }

    async fn unsubscribe(
        &self,
        node_id: &str,
        request: UnsubscribeRequest,
    ) -> Result<u32, PushError> {
        if !request.all
            && request.agent.is_none()
            && request.thread_id.is_none()
            && request.turn_id.is_none()
        {
            return Err(PushError::bad_request(
                "push_unsubscribe needs `all: true` or at least one of agent/thread_id/turn_id",
            ));
        }
        let now = unix_now();
        let mut untrack = Vec::new();
        let removed = {
            let mut state = self.state.lock().await;
            let in_flight = state.in_flight.clone();
            let data = &mut state.data;
            let matched: Vec<String> = data
                .subscriptions
                .values()
                .filter(|s| {
                    !s.revoking
                        && s.device_id == node_id
                        && (request.all
                            || (request.agent.as_deref().is_none_or(|a| a == s.agent)
                                && request
                                    .thread_id
                                    .as_deref()
                                    .is_none_or(|t| t == s.thread_id)
                                && request.turn_id.as_deref().is_none_or(|t| t == s.turn_id)))
                })
                .map(|s| s.id.clone())
                .collect();
            for id in &matched {
                if let Some(sub) = data.subscriptions.get(id) {
                    untrack.push(sub.turn_key());
                }
                retire_subscription(data, id, in_flight.as_deref(), now, &self.policy);
            }
            if !matched.is_empty() {
                self.persist(&state.data, false).await;
            }
            untrack.retain(|key| !has_live_waiter(&state.data, key));
            matched.len() as u32
        };
        for key in untrack {
            self.codex_untrack(&key);
        }
        if removed > 0 {
            info!(device = %short(node_id), removed, "push subscriptions revoked");
            self.wake.notify_one();
        }
        Ok(removed)
    }

    fn codex_untrack(&self, key: &TurnKey) {
        if key.0 == codex::CODEX_AGENT
            && let Some(codex) = self.codex.as_ref()
        {
            codex.untrack(&key.1, &key.2);
        }
    }

    // --- terminal states --------------------------------------------------

    async fn record_terminal(&self, report: TerminalReport) {
        let now = unix_now();
        let key = report.key();
        let attached = {
            let mut state = self.state.lock().await;
            let in_flight = state.in_flight.clone();
            let data = &mut state.data;
            let (record, is_new) = match data.recent.get(&key) {
                Some(existing) => (existing.clone(), false),
                None => {
                    let record = RecordedTerminal {
                        agent: report.agent.clone(),
                        thread_id: report.thread_id.clone(),
                        turn_id: report.turn_id.clone(),
                        terminal: report.terminal,
                        event_id: format!("evt_{}", random_hex32()),
                        recorded_at: now,
                    };
                    data.recent.insert(record.clone());
                    (record, true)
                }
            };
            let targets: Vec<String> = data
                .subscriptions
                .values()
                .filter(|s| !s.revoking && s.terminal.is_none() && s.turn_key() == key)
                .map(|s| s.id.clone())
                .collect();
            for id in &targets {
                attach_terminal(data, id, &record, now, &self.policy, in_flight.as_deref());
            }
            if is_new && let Err(error) = self.files.save_recent(&state.data.recent).await {
                warn!("saving push recent terminals failed: {error:#}");
            }
            if !targets.is_empty() {
                self.persist(&state.data, false).await;
            }
            targets.len()
        };
        if attached > 0 {
            info!(
                agent = %report.agent,
                thread = %report.thread_id,
                turn = %report.turn_id,
                kind = ?report.terminal.kind,
                subscriptions = attached,
                "turn terminal queued for push"
            );
            self.wake.notify_one();
        } else {
            debug!(agent = %report.agent, turn = %report.turn_id, "turn terminal recorded (no subscribers)");
        }
    }

    /// After a restart: hand unfinished subscriptions back to the watchers.
    async fn resume_pending(&self) {
        let pending: Vec<TurnKey> = {
            let state = self.state.lock().await;
            let mut keys: Vec<TurnKey> = state
                .data
                .subscriptions
                .values()
                .filter(|s| !s.revoking && s.terminal.is_none())
                .map(Subscription::turn_key)
                .collect();
            keys.sort();
            keys.dedup();
            keys
        };
        for key in pending {
            let recent = self.state.lock().await.data.recent.get(&key).cloned();
            if let Some(recent) = recent {
                self.record_terminal(TerminalReport {
                    agent: recent.agent,
                    thread_id: recent.thread_id,
                    turn_id: recent.turn_id,
                    terminal: recent.terminal,
                })
                .await;
            } else if key.0 == codex::CODEX_AGENT
                && let Some(codex) = self.codex.as_ref()
            {
                codex.track(&key.1, &key.2);
            }
        }
    }

    // --- outbox -------------------------------------------------------------

    async fn persist(&self, data: &PushData, recent: bool) {
        if let Err(error) = self.files.save_subscriptions(&data.subscriptions).await {
            warn!("saving push subscriptions failed: {error:#}");
        }
        if let Err(error) = self.files.save_outbox(&data.outbox).await {
            warn!("saving push outbox failed: {error:#}");
        }
        if recent && let Err(error) = self.files.save_recent(&data.recent).await {
            warn!("saving push recent terminals failed: {error:#}");
        }
    }

    async fn next_wake(
        &self,
        housekeeping: tokio::time::Instant,
        enabled: bool,
    ) -> tokio::time::Instant {
        if !enabled {
            return housekeeping;
        }
        let state = self.state.lock().await;
        match state.data.outbox.next_wake_ms() {
            Some(at_ms) => {
                let delay = Duration::from_millis(at_ms.saturating_sub(unix_now_ms()));
                housekeeping.min(tokio::time::Instant::now() + delay)
            }
            None => housekeeping,
        }
    }

    /// Send one due item. Returns false when nothing is due.
    async fn delivery_step(&self, target: &WorkerTarget) -> bool {
        let Some(job) = self.take_job().await else {
            return false;
        };
        let delivery = self.execute(&job, target).await;
        self.apply(job, delivery).await;
        true
    }

    async fn take_job(&self) -> Option<Job> {
        let now_ms = unix_now_ms();
        let now = now_ms / 1000;
        let mut state = self.state.lock().await;
        let mut dirty = false;
        let job = loop {
            let Some(item) = state.data.outbox.next_due(now_ms).cloned() else {
                break None;
            };
            let data = &mut state.data;
            let job = match &item.action {
                OutboxAction::Register { subscription } => {
                    match data.subscriptions.get(subscription) {
                        // A revoking subscription still re-registers if an
                        // earlier attempt may have reached the Worker: that
                        // is how the revoke learns which id to delete.
                        Some(sub)
                            if sub.remote_id.is_none()
                                && (!sub.revoking || sub.register_attempted) =>
                        {
                            Some(Job::Register {
                                item_id: item.id.clone(),
                                sub: Box::new(sub.clone()),
                            })
                        }
                        _ => None,
                    }
                }
                OutboxAction::Event {
                    subscription,
                    event,
                } => match data.subscriptions.get(subscription) {
                    Some(sub) if !sub.revoking => match sub.remote_id {
                        Some(_) => Some(Job::Event {
                            item_id: item.id.clone(),
                            subscription: subscription.clone(),
                            body: event.clone(),
                        }),
                        None => {
                            // Register first (should only happen after a crash).
                            data.outbox.insert_before_subscription(OutboxItem::new(
                                sub.chain(),
                                OutboxAction::Register {
                                    subscription: subscription.clone(),
                                },
                                now,
                            ));
                            dirty = true;
                            continue;
                        }
                    },
                    _ => None,
                },
                OutboxAction::Revoke {
                    subscription,
                    remote_id,
                } => {
                    let remote = remote_id.clone().or_else(|| {
                        subscription
                            .as_ref()
                            .and_then(|id| data.subscriptions.get(id))
                            .and_then(|sub| sub.remote_id.clone())
                    });
                    match remote {
                        Some(remote_id) => Some(Job::Revoke {
                            item_id: item.id.clone(),
                            subscription: subscription.clone(),
                            remote_id,
                        }),
                        None => {
                            // Never registered: nothing to delete remotely.
                            if let Some(id) = subscription {
                                data.subscriptions.remove(id);
                            }
                            None
                        }
                    }
                }
            };
            match job {
                Some(job) => break Some(job),
                None => {
                    data.outbox.remove(&item.id);
                    dirty = true;
                }
            }
        };
        state.in_flight = job.as_ref().map(|job| job.item_id().to_string());
        if dirty {
            self.persist(&state.data, false).await;
        }
        job
    }

    async fn execute(&self, job: &Job, target: &WorkerTarget) -> Delivery {
        let now = unix_now();
        match job {
            Job::Register { sub, .. } => {
                let body = match serde_json::to_vec(&RegisterBody::from_subscription(sub)) {
                    Ok(body) => body,
                    Err(error) => {
                        return Delivery::Permanent {
                            status: None,
                            error: format!("serializing register body: {error}"),
                        };
                    }
                };
                client::send(
                    &self.http,
                    &self.secret_key,
                    target,
                    Method::POST,
                    "/v2/subscriptions",
                    Some(body),
                    now,
                )
                .await
            }
            Job::Event { body, .. } => {
                let body = match serde_json::to_vec(body) {
                    Ok(body) => body,
                    Err(error) => {
                        return Delivery::Permanent {
                            status: None,
                            error: format!("serializing event body: {error}"),
                        };
                    }
                };
                client::send(
                    &self.http,
                    &self.secret_key,
                    target,
                    Method::POST,
                    "/v2/events",
                    Some(body),
                    now,
                )
                .await
            }
            Job::Revoke { remote_id, .. } => {
                if !is_safe_remote_id(remote_id) {
                    return Delivery::Permanent {
                        status: None,
                        error: "refusing to revoke a malformed subscription id".into(),
                    };
                }
                client::send(
                    &self.http,
                    &self.secret_key,
                    target,
                    Method::DELETE,
                    &format!("/v2/subscriptions/{remote_id}"),
                    None,
                    now,
                )
                .await
            }
        }
    }

    async fn apply(&self, job: Job, delivery: Delivery) {
        let now_ms = unix_now_ms();
        let now = now_ms / 1000;
        let mut state = self.state.lock().await;
        state.in_flight = None;
        let label = job.label();
        // Turns whose subscriptions were dropped here; the Codex watcher can
        // stop tracking them unless someone else still waits.
        let mut dropped: Vec<TurnKey> = Vec::new();
        let data = &mut state.data;
        match delivery {
            Delivery::Success { body, .. } => match job {
                Job::Register { item_id, sub } => {
                    data.outbox.remove(&item_id);
                    match body
                        .get("subscriptionId")
                        .and_then(Value::as_str)
                        .filter(|id| is_safe_remote_id(id))
                    {
                        Some(remote_id) => {
                            match data.subscriptions.get_mut(&sub.id) {
                                Some(local) => local.remote_id = Some(remote_id.to_string()),
                                None => {
                                    // Dropped while in flight: delete it remotely.
                                    enqueue(
                                        data,
                                        OutboxItem::new(
                                            sub.chain(),
                                            OutboxAction::Revoke {
                                                subscription: None,
                                                remote_id: Some(remote_id.to_string()),
                                            },
                                            now,
                                        ),
                                        &self.policy,
                                        None,
                                    );
                                }
                            }
                            debug!(subscription = %remote_id, "push subscription registered");
                            self.note_success();
                        }
                        None => {
                            warn!(
                                "push worker accepted a registration without a subscriptionId; dropping it"
                            );
                            dropped.extend(drop_subscription(data, &sub.id, None));
                            self.note_error("register response missing subscriptionId");
                        }
                    }
                }
                Job::Event {
                    item_id,
                    subscription,
                    body: event,
                } => {
                    data.outbox.remove(&item_id);
                    drop_subscription(data, &subscription, None);
                    // One POST covers every registered subscription of the
                    // turn; retire the ones the Worker reported on.
                    let reported: Vec<String> = body
                        .get("results")
                        .and_then(Value::as_array)
                        .map(|results| {
                            results
                                .iter()
                                .filter_map(|r| r.get("subscriptionId").and_then(Value::as_str))
                                .map(str::to_string)
                                .collect()
                        })
                        .unwrap_or_default();
                    let covered: Vec<String> = data
                        .subscriptions
                        .values()
                        .filter(|s| {
                            s.remote_id.as_ref().is_some_and(|r| reported.contains(r))
                                && s.terminal
                                    .as_ref()
                                    .is_some_and(|t| t.event_id == event.event_id)
                        })
                        .map(|s| s.id.clone())
                        .collect();
                    for id in covered {
                        drop_subscription(data, &id, None);
                    }
                    let matched = body.get("matched").and_then(|m| m.as_u64()).unwrap_or(0);
                    info!(event = %event.event_id, matched, "push event delivered to worker");
                    self.note_success();
                }
                Job::Revoke {
                    item_id,
                    subscription,
                    ..
                } => {
                    data.outbox.remove(&item_id);
                    if let Some(id) = subscription {
                        drop_subscription(data, &id, None);
                    }
                    self.note_success();
                }
            },
            Delivery::Retry {
                status,
                retry_after,
                error,
                maybe_delivered,
            } => {
                let item_id = job.item_id().to_string();
                if maybe_delivered
                    && let Job::Register { sub, .. } = &job
                    && let Some(local) = data.subscriptions.get_mut(&sub.id)
                {
                    local.register_attempted = true;
                }
                let attempts = data
                    .outbox
                    .get(&item_id)
                    .map(|i| i.attempts + 1)
                    .unwrap_or(1);
                let jitter: f64 = rand::thread_rng().r#gen();
                let delay = self.policy.retry_delay(attempts, retry_after, jitter);
                data.outbox
                    .schedule_retry(&item_id, now_ms, delay, error.clone());
                if status == Some(429) || (status == Some(503) && retry_after.is_some()) {
                    let pause = retry_after
                        .unwrap_or(delay)
                        .min(self.policy.max_retry_after);
                    data.outbox.paused_until_ms = now_ms + pause.as_millis() as u64;
                }
                warn!(
                    action = label,
                    attempts,
                    retry_in_ms = delay.as_millis() as u64,
                    "push delivery failed; will retry: {error}"
                );
                self.note_error(&error);
            }
            Delivery::Permanent { status, error } => {
                let item_id = job.item_id().to_string();
                data.outbox.remove(&item_id);
                let id = match job {
                    Job::Register { sub, .. } => Some(sub.id.clone()),
                    Job::Event { subscription, .. } => Some(subscription),
                    Job::Revoke { subscription, .. } => subscription,
                };
                if let Some(id) = id {
                    dropped.extend(drop_subscription(data, &id, None));
                }
                if label == "revoke" && status == Some(404) {
                    debug!("push subscription already gone on the worker");
                } else {
                    warn!(
                        action = label,
                        "push delivery rejected permanently; dropped: {error}"
                    );
                    self.note_error(&error);
                }
            }
        }
        self.persist(&state.data, false).await;
        dropped.retain(|key| !has_live_waiter(&state.data, key));
        drop(state);
        for key in dropped {
            self.codex_untrack(&key);
        }
    }

    /// Shutdown: deliver what is due right now, within a short budget.
    async fn flush(&self) {
        let effective = self.effective();
        let (true, Some(target)) = (effective.enabled, effective.target) else {
            return;
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, self.delivery_step(&target)).await {
                Ok(true) => continue,
                _ => break,
            }
        }
    }

    /// Expire subscriptions, age out outbox items, forget old terminals.
    async fn housekeeping(&self) {
        let now = unix_now();
        let mut untrack = Vec::new();
        {
            let mut state = self.state.lock().await;
            let in_flight = state.in_flight.clone();
            let data = &mut state.data;
            let mut changed = false;

            let expired: Vec<String> = data
                .subscriptions
                .values()
                .filter(|s| s.expires_at <= now)
                .map(|s| s.id.clone())
                .collect();
            for id in &expired {
                if let Some(sub) = data.subscriptions.get(id) {
                    untrack.push(sub.turn_key());
                }
                drop_subscription(data, id, in_flight.as_deref());
                changed = true;
            }
            if !expired.is_empty() {
                info!(count = expired.len(), "push subscriptions expired");
            }

            for item in data.outbox.prune_expired(now, self.policy.max_age) {
                warn!(
                    action = item.action.label(),
                    attempts = item.attempts,
                    "push outbox item expired; dropped"
                );
                if matches!(
                    item.action,
                    OutboxAction::Register { .. } | OutboxAction::Event { .. }
                ) && let Some(id) = item.action.subscription()
                {
                    let id = id.to_string();
                    untrack.extend(drop_subscription(data, &id, in_flight.as_deref()));
                }
                changed = true;
            }

            let recent_changed = data.recent.prune(now);
            if changed || recent_changed {
                self.persist(&state.data, recent_changed).await;
            }
            untrack.retain(|key| !has_live_waiter(&state.data, key));
        }
        for key in untrack {
            self.codex_untrack(&key);
        }
    }
}

/// Queue an outbox item, dropping the subscriptions of anything evicted by
/// the queue cap.
fn enqueue(data: &mut PushData, item: OutboxItem, policy: &OutboxPolicy, in_flight: Option<&str>) {
    for evicted in data.outbox.push(item, policy.max_items) {
        warn!(
            action = evicted.action.label(),
            "push outbox full; evicted the oldest item"
        );
        // Whatever the evicted item was for can no longer complete; the
        // Worker expires its side.
        if let Some(id) = evicted.action.subscription() {
            let id = id.to_string();
            drop_subscription(data, &id, in_flight);
        }
    }
}

/// Forget a subscription locally together with its queued items (except the
/// in-flight one, which `apply` reconciles). Returns the turn it watched.
fn drop_subscription(data: &mut PushData, id: &str, in_flight: Option<&str>) -> Option<TurnKey> {
    let removed = data.subscriptions.remove(id);
    data.outbox.remove_for_subscription(id, in_flight);
    removed.map(|sub| sub.turn_key())
}

/// Stop notifying for a subscription. If the Worker never saw it (and no
/// register is in flight) it is simply forgotten; otherwise a `revoke` is
/// queued behind whatever is in flight on its chain.
fn retire_subscription(
    data: &mut PushData,
    id: &str,
    in_flight: Option<&str>,
    now: u64,
    policy: &OutboxPolicy,
) {
    let Some(sub) = data.subscriptions.get(id).cloned() else {
        return;
    };
    let register_in_flight = in_flight.is_some_and(|flight| {
        data.outbox.get(flight).is_some_and(|item| {
            item.action
                == OutboxAction::Register {
                    subscription: id.to_string(),
                }
        })
    });
    // Never sent to the Worker: forgetting it locally is enough.
    if sub.remote_id.is_none() && !register_in_flight && !sub.register_attempted {
        drop_subscription(data, id, in_flight);
        return;
    }
    // Keep a queued register retry when an earlier attempt may have reached
    // the Worker (e.g. timed out): it returns the existing id, which the
    // revoke queued behind it then deletes.
    let keep_register = sub.remote_id.is_none() && sub.register_attempted;
    data.outbox.items.retain(|item| {
        Some(item.id.as_str()) == in_flight
            || item.action.subscription() != Some(id)
            || (keep_register && item.action.label() == "register")
    });
    if let Some(local) = data.subscriptions.get_mut(id) {
        local.revoking = true;
    }
    enqueue(
        data,
        OutboxItem::new(
            sub.chain(),
            OutboxAction::Revoke {
                subscription: Some(id.to_string()),
                remote_id: None,
            },
            now,
        ),
        policy,
        in_flight,
    );
}

fn attach_terminal(
    data: &mut PushData,
    id: &str,
    record: &RecordedTerminal,
    now: u64,
    policy: &OutboxPolicy,
    in_flight: Option<&str>,
) {
    let Some(sub) = data.subscriptions.get_mut(id) else {
        return;
    };
    if sub.terminal.is_some() || sub.revoking {
        return;
    }
    sub.terminal = Some(SubscriptionTerminal {
        terminal: record.terminal,
        event_id: record.event_id.clone(),
    });
    let chain = sub.chain();
    let body = EventBody::new(&record.event_id, &record.key(), &record.terminal);
    enqueue(
        data,
        OutboxItem::new(
            chain,
            OutboxAction::Event {
                subscription: id.to_string(),
                event: body,
            },
            now,
        ),
        policy,
        in_flight,
    );
}

/// Whether some live subscription still waits for this turn's terminal.
fn has_live_waiter(data: &PushData, key: &TurnKey) -> bool {
    data.subscriptions
        .values()
        .any(|s| !s.revoking && s.terminal.is_none() && s.turn_key() == *key)
}

/// Repair cross-file inconsistencies after a crash between file writes.
/// Returns whether anything changed.
fn reconcile(data: &mut PushData, now: u64) -> bool {
    let mut changed = false;
    let before = data.outbox.items.len();
    let subscriptions = &data.subscriptions;
    data.outbox.items.retain(|item| match &item.action {
        OutboxAction::Revoke {
            remote_id: Some(_), ..
        } => true,
        action => action
            .subscription()
            .is_some_and(|id| subscriptions.contains_key(id)),
    });
    changed |= before != data.outbox.items.len();

    let ids: Vec<String> = data.subscriptions.keys().cloned().collect();
    for id in ids {
        let Some(sub) = data.subscriptions.get(&id).cloned() else {
            continue;
        };
        if sub.revoking {
            if !data.outbox.has_action_for(&id, "revoke") {
                let revoke = OutboxItem::new(
                    sub.chain(),
                    OutboxAction::Revoke {
                        subscription: Some(id.clone()),
                        remote_id: None,
                    },
                    now,
                );
                if sub.remote_id.is_some() {
                    // Ahead of anything newer on the chain (e.g. the
                    // replacement's register), as it was originally queued.
                    data.outbox.insert_front_of_chain(revoke);
                } else if sub.register_attempted && data.outbox.has_action_for(&id, "register") {
                    data.outbox.items.push(revoke);
                } else {
                    drop_subscription(data, &id, None);
                }
                changed = true;
            }
            continue;
        }
        if sub.remote_id.is_none() && !data.outbox.has_action_for(&id, "register") {
            data.outbox.insert_before_subscription(OutboxItem::new(
                sub.chain(),
                OutboxAction::Register {
                    subscription: id.clone(),
                },
                now,
            ));
            changed = true;
        }
        if let Some(terminal) = &sub.terminal
            && !data.outbox.has_action_for(&id, "event")
        {
            data.outbox.items.push(OutboxItem::new(
                sub.chain(),
                OutboxAction::Event {
                    subscription: id.clone(),
                    event: EventBody::new(&terminal.event_id, &sub.turn_key(), &terminal.terminal),
                },
                now,
            ));
            changed = true;
        }
    }
    changed
}

async fn persist_all(files: &PushFiles, data: &PushData) {
    if let Err(error) = files.save_subscriptions(&data.subscriptions).await {
        warn!("saving push subscriptions failed: {error:#}");
    }
    if let Err(error) = files.save_outbox(&data.outbox).await {
        warn!("saving push outbox failed: {error:#}");
    }
}

struct ValidSubscribe {
    platform: Platform,
    environment: Option<ApnsEnvironment>,
}

/// Spec §13: `agent` matches `[A-Za-z0-9._-]{1,64}`.
fn check_agent(agent: &str) -> Result<(), PushError> {
    let ok = !agent.is_empty()
        && agent.len() <= MAX_AGENT_LEN
        && agent
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(PushError::bad_request(
            "agent must match [A-Za-z0-9._-]{1,64}",
        ))
    }
}

/// Spec §13: non-empty, at most 128 UTF-8 bytes, no control characters
/// (Rust strings cannot carry unpaired surrogates).
fn check_id(name: &str, value: &str) -> Result<(), PushError> {
    if value.is_empty() || value.len() > MAX_ID_BYTES {
        return Err(PushError::bad_request(format!(
            "{name} must be 1..={MAX_ID_BYTES} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(PushError::bad_request(format!(
            "{name} must not contain control characters"
        )));
    }
    Ok(())
}

/// Validate a `push_subscribe` before anything is persisted: ids (§13),
/// target shape, grant ownership and the v2 grant signature for `aud`.
fn validate_subscribe(
    request: &SubscribeRequest,
    node_id: &str,
    host_id: &str,
    aud: &str,
    now: u64,
) -> Result<ValidSubscribe, PushError> {
    // `agent` is checked by the caller (before the support check).
    check_id("thread_id", &request.thread_id)?;
    check_id("turn_id", &request.turn_id)?;
    let platform = Platform::parse(&request.target.platform)
        .ok_or_else(|| PushError::bad_request("target.platform must be `ios` or `android`"))?;
    let environment = match (platform, request.target.apns_environment.as_deref()) {
        (Platform::Ios, Some(raw)) => Some(ApnsEnvironment::parse(raw).ok_or_else(|| {
            PushError::bad_request("target.apns_environment must be `sandbox` or `production`")
        })?),
        (Platform::Ios, None) => {
            return Err(PushError::bad_request(
                "target.apns_environment is required for ios",
            ));
        }
        (Platform::Android, None | Some("none")) => None,
        (Platform::Android, Some(_)) => {
            return Err(PushError::bad_request(
                "target.apns_environment is only valid for ios",
            ));
        }
    };
    // Opaque to the host (§5.5): only a size and base64url-alphabet check.
    let sealed = &request.target.sealed;
    let sealed_ok = !sealed.is_empty()
        && sealed.len() <= MAX_SEALED_TARGET_LEN
        && sealed
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if !sealed_ok {
        return Err(PushError::bad_request(
            "target.sealed must be unpadded base64url",
        ));
    }
    if request.grant.device_id != node_id {
        return Err(PushError::invalid_grant(
            "grant.device_id does not match the connection identity",
        ));
    }
    let claims = GrantClaims {
        aud,
        host_id,
        device_id: &request.grant.device_id,
        platform: platform.as_str(),
        environment: environment.map(ApnsEnvironment::as_str).unwrap_or("none"),
        sealed_target: sealed,
        agent: &request.agent,
        thread_id: &request.thread_id,
        turn_id: &request.turn_id,
        issued: request.grant.issued,
        expires: request.grant.expires,
        nonce: &request.grant.nonce,
    };
    verify_grant(&claims, &request.grant.signature, now)
        .map_err(|error| PushError::invalid_grant(error.to_string()))?;
    Ok(ValidSubscribe {
        platform,
        environment,
    })
}

/// Worker ids are `sub_<hex>`; anything else never goes into a URL path.
fn is_safe_remote_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn short(id: &str) -> &str {
    &id[..id.len().min(10)]
}

#[cfg(test)]
mod tests;
