//! Side-channel watcher that observes authoritative Codex turn end states
//! (design spec §6.2).
//!
//! One long-lived JSON-RPC connection per app-server (UnixDaemon / UnixProxy:
//! WebSocket over the control UDS; Websocket mode: loopback TCP). The
//! connection initializes as `codex_app_server_daemon` (whitelisted, so the
//! app-server's process-global originator is left alone) and opts out of the
//! high-frequency deltas. For every watched `(thread, turn)`:
//!
//! 1. `thread/read {includeTurns}` — a turn already `interrupted` is failed;
//!    `completed` is completed unless the thread is in `systemError` (the
//!    app-server rebuilds failed turns as completed on read).
//! 2. Active thread with the turn still running → `thread/resume` with no
//!    overrides, which subscribes this connection; wait for `turn/completed`
//!    with the matching turn id, then `thread/unsubscribe`.
//! 3. `notLoaded` threads are never cold-resumed; undecidable watches stay
//!    until the subscription expires.
//!
//! The watcher never answers server→client requests (approvals are
//! first-reply-wins across subscribers) and never reports a terminal because
//! a connection dropped; on reconnect every pending watch is re-checked.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use alleycat_codex_proto::{
    ClientInfo, InitializeCapabilities, InitializeParams, ThreadReadParams, ThreadUnsubscribeParams,
};
use async_trait::async_trait;
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tracing::{debug, info, warn};

use super::store::{FailureReason, Terminal};
use super::{TerminalReport, terminal_from_turn_status, unix_now};

pub const CODEX_AGENT: &str = "codex";
/// `clientInfo.name` on the app-server's non-originating allowlist.
pub const CLIENT_NAME: &str = "codex_app_server_daemon";

/// High-frequency notifications the watcher never needs.
pub const OPT_OUT_NOTIFICATION_METHODS: &[&str] = &[
    "item/agentMessage/delta",
    "item/reasoning/textDelta",
    "item/reasoning/summaryTextDelta",
    "item/reasoning/summaryPartAdded",
    "item/commandExecution/outputDelta",
    "item/fileChange/outputDelta",
    "item/plan/delta",
    "turn/diff/updated",
    "thread/tokenUsage/updated",
];

pub trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncIo for T {}

pub type WsStream = WebSocketStream<Box<dyn AsyncIo>>;

/// Opens a fresh WebSocket to the app-server.
#[async_trait]
pub trait CodexConnector: Send + Sync {
    async fn connect(&self) -> anyhow::Result<WsStream>;
}

/// Client-side WebSocket upgrade with limits large enough for `thread/read`
/// snapshots of long threads.
pub async fn ws_handshake(io: Box<dyn AsyncIo>, url: &str) -> anyhow::Result<WsStream> {
    let config = WebSocketConfig::default()
        .max_message_size(Some(256 << 20))
        .max_frame_size(Some(64 << 20));
    let (ws, _response) = tokio_tungstenite::client_async_with_config(url, io, Some(config))
        .await
        .map_err(|error| anyhow::anyhow!("websocket upgrade failed: {error}"))?;
    Ok(ws)
}

/// How the daemon reaches the shared app-server for side-channel watching.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexPushTarget {
    /// App-server control socket (WebSocket over UDS).
    Unix(PathBuf),
    /// `ws://host:port` listener (legacy Websocket mode).
    Tcp { host: String, port: u16 },
}

pub async fn connect_target(target: &CodexPushTarget) -> anyhow::Result<WsStream> {
    let connect = async {
        match target {
            #[cfg(unix)]
            CodexPushTarget::Unix(path) => {
                let stream = tokio::net::UnixStream::connect(path)
                    .await
                    .map_err(|error| {
                        anyhow::anyhow!("connecting to {}: {error}", path.display())
                    })?;
                ws_handshake(Box::new(stream), "ws://localhost/").await
            }
            #[cfg(not(unix))]
            CodexPushTarget::Unix(path) => Err(anyhow::anyhow!(
                "unix socket {} is not supported on this platform",
                path.display()
            )),
            CodexPushTarget::Tcp { host, port } => {
                let stream = tokio::net::TcpStream::connect((host.as_str(), *port))
                    .await
                    .map_err(|error| anyhow::anyhow!("connecting to {host}:{port}: {error}"))?;
                ws_handshake(Box::new(stream), &format!("ws://{host}:{port}/")).await
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(10), connect)
        .await
        .map_err(|_| anyhow::anyhow!("timed out connecting to codex app-server"))?
}

/// Production connector: resolves the app-server endpoint through the
/// daemon's [`crate::agents::AgentManager`] (cached endpoint first, then a
/// full `ensure_*` refresh).
pub struct AgentCodexConnector {
    agents: crate::agents::AgentManager,
}

impl AgentCodexConnector {
    pub fn new(agents: crate::agents::AgentManager) -> Self {
        Self { agents }
    }
}

#[async_trait]
impl CodexConnector for AgentCodexConnector {
    async fn connect(&self) -> anyhow::Result<WsStream> {
        if let Ok(target) = self.agents.codex_push_target(false).await {
            match connect_target(&target).await {
                Ok(ws) => return Ok(ws),
                Err(error) => debug!("cached codex endpoint unusable, refreshing: {error:#}"),
            }
        }
        let target = self.agents.codex_push_target(true).await?;
        connect_target(&target).await
    }
}

/// Timing knobs (tests shrink them).
#[derive(Debug, Clone)]
pub struct WatcherTiming {
    pub reconnect_initial: Duration,
    pub reconnect_max: Duration,
    pub rpc_timeout: Duration,
    /// Keep an idle connection this long after the last watch ends.
    pub idle_linger: Duration,
    /// When a `thread/resume` snapshot already shows the turn finished, give
    /// an in-flight `turn/completed` (exact status) this long to win.
    pub snapshot_grace: Duration,
    /// Delay before re-reading a turn that reads back as `interrupted` on a
    /// loaded, idle thread (it may be a completion still being flushed).
    pub reverify_delay: Duration,
    /// Retry a check that failed with an RPC error after this long.
    pub recheck_after_error: Duration,
    /// A connection must stay up this long before a drop resets the
    /// reconnect backoff (an app-server that accepts and immediately drops
    /// connections must not be hammered).
    pub stable_after: Duration,
}

impl Default for WatcherTiming {
    fn default() -> Self {
        Self {
            reconnect_initial: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(60),
            rpc_timeout: Duration::from_secs(30),
            idle_linger: Duration::from_secs(60),
            snapshot_grace: Duration::from_millis(500),
            reverify_delay: Duration::from_secs(1),
            recheck_after_error: Duration::from_secs(30),
            stable_after: Duration::from_secs(30),
        }
    }
}

pub enum WatchCommand {
    Track {
        thread_id: String,
        turn_id: String,
        /// Resolved after the first check: `Some` if the turn is already
        /// over, `None` if it is still running or undecidable.
        ack: Option<oneshot::Sender<Option<Terminal>>>,
    },
    Untrack {
        thread_id: String,
        turn_id: String,
    },
}

#[derive(Clone)]
pub struct CodexWatcherHandle {
    tx: mpsc::UnboundedSender<WatchCommand>,
}

impl CodexWatcherHandle {
    pub fn track(&self, thread_id: &str, turn_id: &str) {
        let _ = self.tx.send(WatchCommand::Track {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            ack: None,
        });
    }

    /// Track and wait (bounded) for the first check's verdict.
    pub async fn track_and_wait(
        &self,
        thread_id: &str,
        turn_id: &str,
        wait: Duration,
    ) -> Option<Terminal> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(WatchCommand::Track {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            ack: Some(tx),
        });
        match tokio::time::timeout(wait, rx).await {
            Ok(Ok(verdict)) => verdict,
            _ => None,
        }
    }

    pub fn untrack(&self, thread_id: &str, turn_id: &str) {
        let _ = self.tx.send(WatchCommand::Untrack {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
        });
    }
}

pub fn spawn_codex_watcher(
    connector: Arc<dyn CodexConnector>,
    reports: mpsc::UnboundedSender<TerminalReport>,
    shutdown: watch::Receiver<bool>,
    timing: WatcherTiming,
) -> (CodexWatcherHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel();
    let watcher = Watcher::new(connector, reports, timing);
    let task = tokio::spawn(watcher.run(rx, shutdown));
    (CodexWatcherHandle { tx }, task)
}

// === JSON-RPC connection ===================================================

type PendingMap = Arc<Mutex<HashMap<i64, oneshot::Sender<Result<Value, String>>>>>;

struct RpcConn {
    out: mpsc::UnboundedSender<String>,
    pending: PendingMap,
    next_id: AtomicI64,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl RpcConn {
    async fn call(&self, method: &str, params: Value, timeout: Duration) -> Result<Value, String> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending.lock().unwrap().insert(id, tx);
        // Codex frames omit the `jsonrpc` field.
        let frame = json!({ "id": id, "method": method, "params": params });
        if self.out.send(frame.to_string()).is_err() {
            self.pending.lock().unwrap().remove(&id);
            return Err("connection closed".into());
        }
        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("connection closed".into()),
            Err(_) => {
                self.pending.lock().unwrap().remove(&id);
                Err(format!("{method} timed out"))
            }
        }
    }

    fn notify(&self, method: &str) {
        let _ = self.out.send(json!({ "method": method }).to_string());
    }

    fn close(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
        self.pending.lock().unwrap().clear();
    }
}

impl Drop for RpcConn {
    fn drop(&mut self) {
        self.close();
    }
}

fn route_frame(value: Value, pending: &PendingMap, notifications: &mpsc::UnboundedSender<Value>) {
    let has_method = value.get("method").is_some();
    let id = value.get("id");
    match (id, has_method) {
        (Some(id), true) => {
            // Server → client request (approval, user input, …). Never
            // answered: other subscribers (the phone, the desktop) own it.
            let method = value
                .get("method")
                .and_then(|m| m.as_str())
                .unwrap_or_default();
            debug!(id = %id, method, "codex watcher ignoring server request");
        }
        (None, true) => {
            let _ = notifications.send(value);
        }
        (Some(id), false) => {
            let Some(id) = id.as_i64() else {
                return;
            };
            let Some(tx) = pending.lock().unwrap().remove(&id) else {
                return;
            };
            let result = match value.get("error") {
                Some(error) if !error.is_null() => Err(error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("rpc error")
                    .to_string()),
                _ => Ok(value.get("result").cloned().unwrap_or(Value::Null)),
            };
            let _ = tx.send(result);
        }
        (None, false) => {}
    }
}

fn initialize_params() -> Value {
    let params = InitializeParams {
        client_info: ClientInfo {
            name: CLIENT_NAME.to_string(),
            title: Some("alleycat push watcher".to_string()),
            version: crate::binary_version().to_string(),
        },
        capabilities: Some(InitializeCapabilities {
            experimental_api: true,
            opt_out_notification_methods: Some(
                OPT_OUT_NOTIFICATION_METHODS
                    .iter()
                    .map(|m| m.to_string())
                    .collect(),
            ),
        }),
    };
    serde_json::to_value(params).unwrap_or(Value::Null)
}

async fn open_connection(
    connector: &dyn CodexConnector,
    timing: &WatcherTiming,
) -> anyhow::Result<(Arc<RpcConn>, mpsc::UnboundedReceiver<Value>)> {
    let ws = connector.connect().await?;
    let (mut sink, mut stream) = ws.split();
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<String>();
    let (notif_tx, notif_rx) = mpsc::unbounded_channel::<Value>();
    let pending: PendingMap = Arc::new(Mutex::new(HashMap::new()));

    let writer = tokio::spawn(async move {
        while let Some(text) = out_rx.recv().await {
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
        let _ = sink.close().await;
    });
    let reader_pending = Arc::clone(&pending);
    let reader = tokio::spawn(async move {
        while let Some(message) = stream.next().await {
            let value = match message {
                Ok(Message::Text(text)) => serde_json::from_str::<Value>(text.as_str()),
                Ok(Message::Binary(bytes)) => serde_json::from_slice::<Value>(&bytes),
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => continue,
            };
            if let Ok(value) = value {
                route_frame(value, &reader_pending, &notif_tx);
            }
        }
        // Wake in-flight callers; dropping `notif_tx` tells the watcher the
        // connection is gone.
        reader_pending.lock().unwrap().clear();
    });

    let conn = Arc::new(RpcConn {
        out: out_tx,
        pending,
        next_id: AtomicI64::new(1),
        tasks: Mutex::new(vec![writer, reader]),
    });
    conn.call("initialize", initialize_params(), timing.rpc_timeout)
        .await
        .map_err(|error| anyhow::anyhow!("codex initialize failed: {error}"))?;
    conn.notify("initialized");
    Ok((conn, notif_rx))
}

// === Snapshot evaluation ===================================================

#[derive(Debug, Clone, PartialEq, Eq)]
struct TurnView {
    id: String,
    status: String,
    completed_at: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ThreadView {
    status: String,
    turns: Vec<TurnView>,
}

impl ThreadView {
    /// Parse the `thread` of a `thread/read` or `thread/resume` result.
    fn parse(result: &Value) -> Option<Self> {
        let thread = result.get("thread")?;
        let status = thread
            .get("status")
            .and_then(|s| s.get("type"))
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let turns = thread
            .get("turns")
            .and_then(Value::as_array)
            .map(|turns| {
                turns
                    .iter()
                    .filter_map(|turn| {
                        Some(TurnView {
                            id: turn.get("id")?.as_str()?.to_string(),
                            status: turn.get("status")?.as_str()?.to_string(),
                            completed_at: turn
                                .get("completedAt")
                                .and_then(super::normalize_unix_secs),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(Self { status, turns })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Verdict {
    Terminal(Terminal),
    /// Thread is running and the turn is (or may become) in progress.
    NeedsSubscription,
    /// Looks interrupted on a loaded idle thread; read again to be sure.
    Reverify,
    /// Can't tell (notLoaded without the turn, unknown status, …).
    Unknown,
}

fn evaluate(view: &ThreadView, turn_id: &str, passive_system_error: bool) -> Verdict {
    let active = view.status == "active";
    let system_error = view.status == "systemError";
    let index = view.turns.iter().position(|t| t.id == turn_id);
    let Some(index) = index else {
        return if active {
            Verdict::NeedsSubscription
        } else {
            Verdict::Unknown
        };
    };
    let turn = &view.turns[index];
    let is_last = index + 1 == view.turns.len();
    let at = turn.completed_at.unwrap_or_else(unix_now);
    match turn.status.as_str() {
        "inProgress" => {
            if active {
                Verdict::NeedsSubscription
            } else {
                Verdict::Unknown
            }
        }
        // A failed turn reads back as completed; the thread's systemError
        // flag (reset at every turn start) is what tells them apart.
        "completed" => {
            if is_last && (system_error || passive_system_error) {
                Verdict::Terminal(Terminal::failed(FailureReason::Error, at))
            } else {
                Verdict::Terminal(Terminal::completed(at))
            }
        }
        "failed" => Verdict::Terminal(Terminal::failed(FailureReason::Error, at)),
        "interrupted" => {
            if is_last && system_error {
                Verdict::Terminal(Terminal::failed(FailureReason::Error, at))
            } else if !is_last || view.status == "notLoaded" {
                Verdict::Terminal(Terminal::failed(FailureReason::Interrupted, at))
            } else {
                Verdict::Reverify
            }
        }
        _ => Verdict::Unknown,
    }
}

/// Result of one check. `subscribed` records that this check left the
/// connection subscribed to the thread (via `thread/resume`), so the watcher
/// knows to `thread/unsubscribe` once nothing needs the thread any more.
#[derive(Debug)]
enum CheckOutcome {
    Terminal {
        terminal: Terminal,
        subscribed: bool,
    },
    Waiting {
        subscribed: bool,
    },
    Unknown,
    Failed(String),
}

impl CheckOutcome {
    fn left_subscribed(&self) -> bool {
        matches!(
            self,
            Self::Terminal {
                subscribed: true,
                ..
            } | Self::Waiting { subscribed: true }
        )
    }
}

struct CheckContext {
    conn: Arc<RpcConn>,
    thread_id: String,
    turn_id: String,
    already_subscribed: bool,
    passive: Arc<Mutex<HashMap<String, bool>>>,
    timing: WatcherTiming,
}

impl CheckContext {
    fn passive_error(&self) -> bool {
        self.passive
            .lock()
            .unwrap()
            .get(&self.thread_id)
            .copied()
            .unwrap_or(false)
    }

    async fn read(&self) -> Result<ThreadView, String> {
        let params = serde_json::to_value(ThreadReadParams {
            thread_id: self.thread_id.clone(),
            include_turns: true,
        })
        .map_err(|e| e.to_string())?;
        let result = self
            .conn
            .call("thread/read", params, self.timing.rpc_timeout)
            .await?;
        ThreadView::parse(&result).ok_or_else(|| "malformed thread/read result".to_string())
    }

    async fn read_verdict(&self) -> Result<Verdict, String> {
        let view = self.read().await?;
        Ok(evaluate(&view, &self.turn_id, self.passive_error()))
    }

    /// Re-read a turn that looked interrupted; a second `Reverify` means it
    /// really was interrupted.
    async fn reverify(&self, subscribed: bool) -> Result<CheckOutcome, String> {
        tokio::time::sleep(self.timing.reverify_delay).await;
        Ok(match self.read_verdict().await? {
            Verdict::Terminal(terminal) => CheckOutcome::Terminal {
                terminal,
                subscribed,
            },
            Verdict::Reverify => CheckOutcome::Terminal {
                terminal: Terminal::failed(FailureReason::Interrupted, unix_now()),
                subscribed,
            },
            Verdict::NeedsSubscription if subscribed => CheckOutcome::Waiting { subscribed },
            Verdict::NeedsSubscription => return self.subscribe().await,
            Verdict::Unknown => CheckOutcome::Unknown,
        })
    }

    async fn subscribe(&self) -> Result<CheckOutcome, String> {
        if self.already_subscribed {
            return Ok(CheckOutcome::Waiting { subscribed: true });
        }
        // No overrides: only `threadId`. Only ever reached for active threads.
        let result = self
            .conn
            .call(
                "thread/resume",
                json!({ "threadId": self.thread_id }),
                self.timing.rpc_timeout,
            )
            .await?;
        let verdict = ThreadView::parse(&result)
            .map(|view| evaluate(&view, &self.turn_id, self.passive_error()))
            .unwrap_or(Verdict::NeedsSubscription);
        match verdict {
            Verdict::Terminal(terminal) => {
                // Let an exact `turn/completed` that raced the snapshot win.
                tokio::time::sleep(self.timing.snapshot_grace).await;
                Ok(CheckOutcome::Terminal {
                    terminal,
                    subscribed: true,
                })
            }
            Verdict::Reverify => Box::pin(self.reverify(true)).await,
            Verdict::NeedsSubscription | Verdict::Unknown => {
                Ok(CheckOutcome::Waiting { subscribed: true })
            }
        }
    }

    async fn run(self) -> CheckOutcome {
        let result = async {
            match self.read_verdict().await? {
                Verdict::Terminal(terminal) => Ok(CheckOutcome::Terminal {
                    terminal,
                    subscribed: false,
                }),
                Verdict::Reverify => self.reverify(self.already_subscribed).await,
                Verdict::NeedsSubscription => self.subscribe().await,
                Verdict::Unknown => Ok(CheckOutcome::Unknown),
            }
        }
        .await;
        result.unwrap_or_else(CheckOutcome::Failed)
    }
}

// === Watcher state machine =================================================

type WatchKey = (String, String);

#[derive(Default)]
struct Watch {
    acks: Vec<oneshot::Sender<Option<Terminal>>>,
    check_running: bool,
    recheck_requested: bool,
    next_recheck_at: Option<Instant>,
}

struct CheckResult {
    key: WatchKey,
    generation: u64,
    outcome: CheckOutcome,
}

struct Watcher {
    connector: Arc<dyn CodexConnector>,
    reports: mpsc::UnboundedSender<TerminalReport>,
    timing: WatcherTiming,
    watches: HashMap<WatchKey, Watch>,
    /// Threads this connection is subscribed to via `thread/resume`.
    subscribed: HashSet<String>,
    conn: Option<Arc<RpcConn>>,
    notifications: Option<mpsc::UnboundedReceiver<Value>>,
    generation: u64,
    reconnect_at: Instant,
    backoff: Duration,
    connected_at: Option<Instant>,
    idle_since: Option<Instant>,
    /// Passive layer: thread id → "latest turn hit a systemError".
    passive: Arc<Mutex<HashMap<String, bool>>>,
    results_tx: mpsc::UnboundedSender<CheckResult>,
    results_rx: mpsc::UnboundedReceiver<CheckResult>,
}

async fn next_notification(rx: &mut Option<mpsc::UnboundedReceiver<Value>>) -> Option<Value> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

impl Watcher {
    fn new(
        connector: Arc<dyn CodexConnector>,
        reports: mpsc::UnboundedSender<TerminalReport>,
        timing: WatcherTiming,
    ) -> Self {
        let (results_tx, results_rx) = mpsc::unbounded_channel();
        Self {
            connector,
            reports,
            backoff: timing.reconnect_initial,
            timing,
            watches: HashMap::new(),
            subscribed: HashSet::new(),
            conn: None,
            notifications: None,
            generation: 0,
            reconnect_at: Instant::now(),
            connected_at: None,
            idle_since: None,
            passive: Arc::new(Mutex::new(HashMap::new())),
            results_tx,
            results_rx,
        }
    }

    async fn run(
        mut self,
        mut commands: mpsc::UnboundedReceiver<WatchCommand>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        loop {
            if *shutdown.borrow() {
                break;
            }
            if !self.maintain_connection(&mut shutdown).await {
                break;
            }
            let wake = self.next_wake();
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                command = commands.recv() => match command {
                    Some(command) => self.handle_command(command),
                    None => break,
                },
                notification = next_notification(&mut self.notifications) => match notification {
                    Some(value) => self.handle_notification(value),
                    None => self.on_disconnect(),
                },
                Some(result) = self.results_rx.recv() => self.handle_result(result),
                _ = tokio::time::sleep_until(wake) => self.on_timer(),
            }
        }
        if let Some(conn) = self.conn.take() {
            conn.close();
        }
    }

    /// Connect when there is work, drop the connection after the idle
    /// linger. Returns false if shutdown fired while connecting.
    async fn maintain_connection(&mut self, shutdown: &mut watch::Receiver<bool>) -> bool {
        let now = Instant::now();
        if self.conn.is_none() {
            if self.watches.is_empty() || now < self.reconnect_at {
                return true;
            }
            let attempt = open_connection(self.connector.as_ref(), &self.timing);
            let result = tokio::select! {
                result = attempt => result,
                _ = shutdown.changed() => return false,
            };
            match result {
                Ok((conn, notifications)) => {
                    info!(watches = self.watches.len(), "codex push watcher connected");
                    self.conn = Some(conn);
                    self.notifications = Some(notifications);
                    self.generation += 1;
                    self.subscribed.clear();
                    self.connected_at = Some(Instant::now());
                    self.idle_since = None;
                    let keys: Vec<WatchKey> = self.watches.keys().cloned().collect();
                    for key in keys {
                        if let Some(watch) = self.watches.get_mut(&key) {
                            watch.check_running = false;
                        }
                        self.start_check(key);
                    }
                }
                Err(error) => {
                    warn!(
                        retry_in_ms = self.backoff.as_millis() as u64,
                        "codex push watcher could not connect: {error:#}"
                    );
                    self.reconnect_at = Instant::now() + self.backoff;
                    self.backoff = (self.backoff * 2).min(self.timing.reconnect_max);
                }
            }
        } else if self.watches.is_empty() {
            let idle_since = *self.idle_since.get_or_insert(now);
            if now.duration_since(idle_since) >= self.timing.idle_linger {
                debug!("codex push watcher idle; closing connection");
                self.drop_connection();
                // A deliberate close is not a failure.
                self.backoff = self.timing.reconnect_initial;
            }
        } else {
            self.idle_since = None;
        }
        true
    }

    fn next_wake(&self) -> Instant {
        let now = Instant::now();
        let mut wake = now + Duration::from_secs(3600);
        if self.conn.is_none() && !self.watches.is_empty() {
            wake = wake.min(self.reconnect_at.max(now));
        }
        if self.conn.is_some() && self.watches.is_empty() {
            let since = self.idle_since.unwrap_or(now);
            wake = wake.min(since + self.timing.idle_linger);
        }
        if self.conn.is_some() {
            for watch in self.watches.values() {
                if let Some(at) = watch.next_recheck_at {
                    wake = wake.min(at);
                }
            }
        }
        wake
    }

    fn drop_connection(&mut self) {
        if let Some(conn) = self.conn.take() {
            conn.close();
        }
        self.notifications = None;
        self.subscribed.clear();
        // Results of checks still running on the old connection are stale
        // (terminal verdicts excepted, see `handle_result`).
        self.generation += 1;
        self.connected_at = None;
        // Passive knowledge is per connection; after a reconnect the
        // thread/read status is authoritative again.
        self.passive.lock().unwrap().clear();
        self.idle_since = None;
        for watch in self.watches.values_mut() {
            watch.check_running = false;
            watch.recheck_requested = false;
            watch.next_recheck_at = None;
        }
    }

    fn on_disconnect(&mut self) {
        info!(
            watches = self.watches.len(),
            "codex push watcher lost its app-server connection; will reconnect"
        );
        let stable = self
            .connected_at
            .is_some_and(|at| at.elapsed() >= self.timing.stable_after);
        self.drop_connection();
        if stable {
            self.backoff = self.timing.reconnect_initial;
        }
        self.reconnect_at = Instant::now() + self.backoff;
        self.backoff = (self.backoff * 2).min(self.timing.reconnect_max);
    }

    fn on_timer(&mut self) {
        if self.conn.is_none() {
            // Rechecks wait for the reconnect, which re-checks everything.
            return;
        }
        let now = Instant::now();
        let due: Vec<WatchKey> = self
            .watches
            .iter()
            .filter(|(_, w)| w.next_recheck_at.is_some_and(|at| at <= now))
            .map(|(k, _)| k.clone())
            .collect();
        for key in due {
            self.start_check(key);
        }
    }

    fn start_check(&mut self, key: WatchKey) {
        if let Some(watch) = self.watches.get_mut(&key) {
            watch.next_recheck_at = None;
        }
        let Some(conn) = self.conn.clone() else {
            return;
        };
        let already_subscribed = self.subscribed.contains(&key.0);
        let Some(watch) = self.watches.get_mut(&key) else {
            return;
        };
        if watch.check_running {
            watch.recheck_requested = true;
            return;
        }
        watch.check_running = true;
        watch.recheck_requested = false;
        let context = CheckContext {
            conn,
            thread_id: key.0.clone(),
            turn_id: key.1.clone(),
            already_subscribed,
            passive: Arc::clone(&self.passive),
            timing: self.timing.clone(),
        };
        let results = self.results_tx.clone();
        let generation = self.generation;
        tokio::spawn(async move {
            let outcome = context.run().await;
            let _ = results.send(CheckResult {
                key,
                generation,
                outcome,
            });
        });
    }

    fn handle_command(&mut self, command: WatchCommand) {
        match command {
            WatchCommand::Track {
                thread_id,
                turn_id,
                ack,
            } => {
                let key = (thread_id, turn_id);
                let watch = self.watches.entry(key.clone()).or_default();
                if let Some(ack) = ack {
                    watch.acks.push(ack);
                }
                let running = watch.check_running;
                if !running {
                    self.start_check(key);
                }
            }
            WatchCommand::Untrack { thread_id, turn_id } => {
                if let Some(watch) = self.watches.remove(&(thread_id.clone(), turn_id)) {
                    for ack in watch.acks {
                        let _ = ack.send(None);
                    }
                }
                self.release_thread_if_unwatched(&thread_id);
            }
        }
    }

    fn handle_notification(&mut self, value: Value) {
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        match method {
            "turn/completed" => {
                let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
                    return;
                };
                let Some(turn) = params.get("turn") else {
                    return;
                };
                let Some(turn_id) = turn.get("id").and_then(Value::as_str) else {
                    return;
                };
                let key = (thread_id.to_string(), turn_id.to_string());
                if !self.watches.contains_key(&key) {
                    return;
                }
                let status = turn
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                if let Some(terminal) = terminal_from_turn_status(status, turn.get("completedAt")) {
                    self.resolve(key, terminal);
                }
            }
            "thread/status/changed" => {
                let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
                    return;
                };
                let kind = params
                    .get("status")
                    .and_then(|s| s.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                match kind {
                    "systemError" => {
                        self.passive
                            .lock()
                            .unwrap()
                            .insert(thread_id.to_string(), true);
                    }
                    "active" => {
                        self.passive
                            .lock()
                            .unwrap()
                            .insert(thread_id.to_string(), false);
                    }
                    _ => {}
                }
                let subscribed = self.subscribed.contains(thread_id);
                if kind != "active" || !subscribed {
                    self.recheck_thread(thread_id);
                }
            }
            "thread/closed" => {
                let Some(thread_id) = params.get("threadId").and_then(Value::as_str) else {
                    return;
                };
                self.subscribed.remove(thread_id);
                self.recheck_thread(thread_id);
            }
            _ => {}
        }
    }

    fn recheck_thread(&mut self, thread_id: &str) {
        let keys: Vec<WatchKey> = self
            .watches
            .keys()
            .filter(|(thread, _)| thread == thread_id)
            .cloned()
            .collect();
        for key in keys {
            self.start_check(key);
        }
    }

    fn handle_result(&mut self, result: CheckResult) {
        let CheckResult {
            key,
            generation,
            outcome,
        } = result;
        if outcome.left_subscribed() && generation == self.generation {
            self.subscribed.insert(key.0.clone());
        }
        if !self.watches.contains_key(&key) {
            // Untracked (or resolved) while the check ran; don't leave a
            // stray subscription behind.
            self.release_thread_if_unwatched(&key.0);
            return;
        }
        if let CheckOutcome::Terminal { terminal, .. } = outcome {
            // Authoritative whichever connection produced it; `resolve`
            // also releases the thread if we just resumed it.
            self.resolve(key, terminal);
            return;
        }
        if generation != self.generation {
            return;
        }
        let recheck_after_error = self.timing.recheck_after_error;
        let Some(watch) = self.watches.get_mut(&key) else {
            return;
        };
        watch.check_running = false;
        for ack in watch.acks.drain(..) {
            let _ = ack.send(None);
        }
        match outcome {
            CheckOutcome::Waiting { .. } | CheckOutcome::Unknown => {}
            CheckOutcome::Failed(error) => {
                debug!(thread = %key.0, turn = %key.1, "codex push check failed: {error}");
                watch.next_recheck_at = Some(Instant::now() + recheck_after_error);
                watch.recheck_requested = false;
            }
            CheckOutcome::Terminal { .. } => unreachable!("handled above"),
        }
        if self
            .watches
            .get(&key)
            .is_some_and(|watch| watch.recheck_requested)
        {
            self.start_check(key);
        }
    }

    fn resolve(&mut self, key: WatchKey, terminal: Terminal) {
        let Some(watch) = self.watches.remove(&key) else {
            return;
        };
        info!(
            thread = %key.0,
            turn = %key.1,
            kind = ?terminal.kind,
            reason = ?terminal.reason,
            "codex turn reached a terminal state"
        );
        for ack in watch.acks {
            let _ = ack.send(Some(terminal));
        }
        let _ = self.reports.send(TerminalReport {
            agent: CODEX_AGENT.to_string(),
            thread_id: key.0.clone(),
            turn_id: key.1.clone(),
            terminal,
        });
        self.release_thread_if_unwatched(&key.0);
    }

    /// `thread/unsubscribe` once no watch needs the thread any more.
    fn release_thread_if_unwatched(&mut self, thread_id: &str) {
        if self.watches.keys().any(|(thread, _)| thread == thread_id) {
            return;
        }
        if !self.subscribed.remove(thread_id) {
            return;
        }
        let Some(conn) = self.conn.clone() else {
            return;
        };
        let thread_id = thread_id.to_string();
        let timeout = self.timing.rpc_timeout;
        tokio::spawn(async move {
            let params = serde_json::to_value(ThreadUnsubscribeParams {
                thread_id: thread_id.clone(),
            })
            .unwrap_or(Value::Null);
            if let Err(error) = conn.call("thread/unsubscribe", params, timeout).await {
                debug!(thread = %thread_id, "codex thread/unsubscribe failed: {error}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn view(status: &str, turns: &[(&str, &str)]) -> ThreadView {
        ThreadView {
            status: status.to_string(),
            turns: turns
                .iter()
                .map(|(id, status)| TurnView {
                    id: id.to_string(),
                    status: status.to_string(),
                    completed_at: Some(1_790_300_000),
                })
                .collect(),
        }
    }

    #[test]
    fn evaluate_covers_the_decision_table() {
        let completed = Verdict::Terminal(Terminal::completed(1_790_300_000));
        let failed = Verdict::Terminal(Terminal::failed(FailureReason::Error, 1_790_300_000));
        let interrupted =
            Verdict::Terminal(Terminal::failed(FailureReason::Interrupted, 1_790_300_000));

        assert_eq!(
            evaluate(&view("idle", &[("t1", "completed")]), "t1", false),
            completed
        );
        // Failed turns read back as completed: systemError disambiguates.
        assert_eq!(
            evaluate(&view("systemError", &[("t1", "completed")]), "t1", false),
            failed
        );
        assert_eq!(
            evaluate(&view("idle", &[("t1", "completed")]), "t1", true),
            failed
        );
        // …but only for the latest turn.
        assert_eq!(
            evaluate(
                &view("systemError", &[("t1", "completed"), ("t2", "completed")]),
                "t1",
                true
            ),
            completed
        );
        assert_eq!(
            evaluate(&view("idle", &[("t1", "failed")]), "t1", false),
            failed
        );
        assert_eq!(
            evaluate(&view("notLoaded", &[("t1", "interrupted")]), "t1", false),
            interrupted
        );
        assert_eq!(
            evaluate(
                &view("active", &[("t1", "interrupted"), ("t2", "inProgress")]),
                "t1",
                false
            ),
            interrupted
        );
        assert_eq!(
            evaluate(&view("idle", &[("t1", "interrupted")]), "t1", false),
            Verdict::Reverify
        );
        assert_eq!(
            evaluate(&view("systemError", &[("t1", "interrupted")]), "t1", false),
            failed
        );
        assert_eq!(
            evaluate(&view("active", &[("t1", "inProgress")]), "t1", false),
            Verdict::NeedsSubscription
        );
        assert_eq!(
            evaluate(&view("active", &[]), "t1", false),
            Verdict::NeedsSubscription
        );
        assert_eq!(
            evaluate(&view("notLoaded", &[]), "t1", false),
            Verdict::Unknown
        );
        assert_eq!(
            evaluate(&view("idle", &[("t1", "weird")]), "t1", false),
            Verdict::Unknown
        );
    }

    #[test]
    fn thread_view_parses_lenient_json() {
        let result = json!({
            "thread": {
                "id": "th", "status": {"type": "active", "activeFlags": []},
                "turns": [
                    {"id": "a", "status": "completed", "completedAt": 1790300000, "items": [{"type": "whatever"}]},
                    {"id": "b", "status": "inProgress"},
                    {"status": "completed"}
                ],
                "someFutureField": true
            }
        });
        let view = ThreadView::parse(&result).unwrap();
        assert_eq!(view.status, "active");
        assert_eq!(view.turns.len(), 2);
        assert_eq!(view.turns[0].completed_at, Some(1790300000));
        assert_eq!(view.turns[1].completed_at, None);
        assert!(ThreadView::parse(&json!({"nope": 1})).is_none());
    }

    struct NeverConnects;

    #[async_trait]
    impl CodexConnector for NeverConnects {
        async fn connect(&self) -> anyhow::Result<WsStream> {
            Err(anyhow::anyhow!("app-server is down"))
        }
    }

    fn idle_watcher() -> Watcher {
        let (reports, _rx) = mpsc::unbounded_channel();
        Watcher::new(Arc::new(NeverConnects), reports, WatcherTiming::default())
    }

    #[tokio::test]
    async fn pending_recheck_does_not_wake_while_disconnected() {
        // Regression: a recheck scheduled on a dead connection used to make
        // `next_wake` return a past instant forever (busy loop).
        let mut watcher = idle_watcher();
        let now = Instant::now();
        watcher.watches.insert(
            ("th".into(), "t1".into()),
            Watch {
                next_recheck_at: Some(now - Duration::from_secs(5)),
                ..Watch::default()
            },
        );
        watcher.reconnect_at = now + Duration::from_secs(30);
        assert!(watcher.next_wake() >= watcher.reconnect_at);
        watcher.on_timer();
        watcher.start_check(("th".into(), "t1".into()));
        assert!(
            watcher
                .watches
                .values()
                .all(|w| w.next_recheck_at.is_none()),
            "start_check without a connection clears the recheck"
        );
    }

    #[tokio::test]
    async fn flapping_connections_keep_backing_off() {
        let mut watcher = idle_watcher();
        let initial = watcher.timing.reconnect_initial;
        // Dropped right after connecting: backoff grows.
        watcher.connected_at = Some(Instant::now());
        watcher.on_disconnect();
        watcher.connected_at = Some(Instant::now());
        watcher.on_disconnect();
        assert_eq!(watcher.backoff, initial * 4);
        let generation = watcher.generation;
        // A connection that stayed up resets it.
        watcher.connected_at = Some(Instant::now() - watcher.timing.stable_after);
        watcher.on_disconnect();
        assert_eq!(watcher.backoff, initial * 2);
        assert!(watcher.generation > generation, "old checks become stale");
    }

    #[test]
    fn initialize_uses_whitelisted_name_and_opt_outs() {
        let params = initialize_params();
        assert_eq!(params["clientInfo"]["name"], CLIENT_NAME);
        assert_eq!(params["capabilities"]["experimentalApi"], true);
        let opt_out = params["capabilities"]["optOutNotificationMethods"]
            .as_array()
            .unwrap();
        assert!(opt_out.iter().any(|m| m == "item/agentMessage/delta"));
        assert!(opt_out.iter().any(|m| m == "thread/tokenUsage/updated"));
        assert!(!opt_out.iter().any(|m| m == "turn/completed"));
        assert!(!opt_out.iter().any(|m| m == "thread/status/changed"));
    }
}
