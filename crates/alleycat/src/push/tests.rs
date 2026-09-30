//! Integration-style tests for the push service: a tiny local HTTP "Worker"
//! that verifies host signatures, and a fake Codex app-server speaking
//! WebSocket JSON-RPC over a Unix socket.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use iroh::{PublicKey, SecretKey, Signature};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::codex::{CodexConnector, WatcherTiming};
use super::signing::vectors::{DEVICE_SEED, HOST_ID, HOST_SEED};
use super::signing::{GrantClaims, host_request_canonical, is_lower_hex, sha256_hex};
use super::store::{FailureReason, OutboxPolicy, PushFiles, Terminal, TerminalKind};
use super::*;
use crate::config::HostConfig;
use crate::protocol::{AgentInfo, AgentWire, PushGrantWire, PushTargetWire};

const WAIT: Duration = Duration::from_secs(10);
/// Opaque sealed targets (the host never parses them).
const SEALED_A: &str = super::signing::vectors::SEALED_TARGET;
const SEALED_B: &str =
    "AQGbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-_";

async fn wait_until<F: FnMut() -> bool>(what: &str, mut condition: F) {
    let deadline = Instant::now() + WAIT;
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// === Test Worker ===========================================================

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    body: Value,
    nonce: String,
    at: Instant,
}

#[derive(Debug, Clone)]
struct Reply {
    status: u16,
    body: Value,
    headers: Vec<(String, String)>,
}

impl Reply {
    fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            body,
            headers: Vec::new(),
        }
    }

    fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }
}

type Responder = Box<dyn FnMut(&Recorded) -> Reply + Send>;

#[derive(Clone)]
struct TestWorker {
    url: String,
    requests: Arc<StdMutex<Vec<Recorded>>>,
    bad_signatures: Arc<AtomicUsize>,
    responder: Arc<StdMutex<Responder>>,
}

/// Default Worker behavior: register → `sub_<first 8 of deviceId>_<turn>`,
/// events → 202 reporting every registered subscription of the turn as sent,
/// delete → 200.
fn default_responder() -> Responder {
    let registered: Arc<StdMutex<Vec<(String, Value)>>> = Arc::default();
    Box::new(
        move |request: &Recorded| match (request.method.as_str(), request.path.as_str()) {
            ("POST", "/v2/subscriptions") => {
                let device = request.body["deviceId"].as_str().unwrap_or_default();
                let turn = request.body["turnId"].as_str().unwrap_or_default();
                let sealed = request.body["sealedTarget"].as_str().unwrap_or_default();
                let id = format!(
                    "sub_{}_{}_{}",
                    &device[..8],
                    turn,
                    &sha256_hex(sealed.as_bytes())[..6]
                );
                registered
                    .lock()
                    .unwrap()
                    .push((id.clone(), request.body.clone()));
                Reply::json(
                    201,
                    json!({"subscriptionId": id, "expiresAt": request.body["expiresAt"]}),
                )
            }
            ("POST", "/v2/events") => {
                let results: Vec<Value> = registered
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, body)| {
                        body["agent"] == request.body["agent"]
                            && body["threadId"] == request.body["threadId"]
                            && body["turnId"] == request.body["turnId"]
                    })
                    .map(|(id, _)| json!({"subscriptionId": id, "state": "sent"}))
                    .collect();
                Reply::json(
                    202,
                    json!({"eventId": request.body["eventId"], "matched": results.len(), "results": results}),
                )
            }
            ("DELETE", _) => Reply::json(200, json!({"ok": true})),
            _ => Reply::json(404, json!({"error": "not_found"})),
        },
    )
}

impl TestWorker {
    async fn start() -> Self {
        Self::start_with(default_responder()).await
    }

    async fn start_with(responder: Responder) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let worker = Self {
            url,
            requests: Arc::default(),
            bad_signatures: Arc::default(),
            responder: Arc::new(StdMutex::new(responder)),
        };
        let server = worker.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let server = server.clone();
                tokio::spawn(async move {
                    let _ = server.serve(stream).await;
                });
            }
        });
        worker
    }

    fn set_responder(&self, responder: Responder) {
        *self.responder.lock().unwrap() = responder;
    }

    async fn serve(&self, stream: tokio::net::TcpStream) -> std::io::Result<()> {
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await?;
        let mut parts = line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_string();
        let path = parts.next().unwrap_or_default().to_string();
        let mut headers = HashMap::new();
        loop {
            line.clear();
            reader.read_line(&mut line).await?;
            let trimmed = line.trim_end();
            if trimmed.is_empty() {
                break;
            }
            if let Some((name, value)) = trimmed.split_once(':') {
                headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
            }
        }
        let length: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut body = vec![0u8; length];
        reader.read_exact(&mut body).await?;

        if !signature_valid(&self.url, &method, &path, &headers, &body) {
            self.bad_signatures.fetch_add(1, Ordering::SeqCst);
        }
        let recorded = Recorded {
            method,
            path,
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
            nonce: headers
                .get("x-agentbuddy-nonce")
                .cloned()
                .unwrap_or_default(),
            at: Instant::now(),
        };
        let reply = (self.responder.lock().unwrap())(&recorded);
        self.requests.lock().unwrap().push(recorded);

        let payload = reply.body.to_string();
        let mut response = format!(
            "HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            reply.status,
            payload.len()
        );
        for (name, value) in &reply.headers {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
        response.push_str("\r\n");
        response.push_str(&payload);
        let mut stream = reader.into_inner();
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await
    }

    fn requests(&self) -> Vec<Recorded> {
        self.requests.lock().unwrap().clone()
    }

    fn count(&self, method: &str, path_prefix: &str) -> usize {
        self.requests()
            .iter()
            .filter(|r| r.method == method && r.path.starts_with(path_prefix))
            .count()
    }
}

/// Verify the spec §5.2 (v2) headers exactly as the Worker would; `origin`
/// is what the Worker computes as `new URL(request.url).origin`.
fn signature_valid(
    origin: &str,
    method: &str,
    path: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> bool {
    let (Some(host), Some(ts), Some(nonce), Some(sig)) = (
        headers.get("x-agentbuddy-host"),
        headers.get("x-agentbuddy-timestamp"),
        headers.get("x-agentbuddy-nonce"),
        headers.get("x-agentbuddy-signature"),
    ) else {
        return false;
    };
    if host != HOST_ID || !is_lower_hex(nonce, 32) || !is_lower_hex(sig, 128) {
        return false;
    }
    let Ok(ts) = ts.parse::<u64>() else {
        return false;
    };
    if ts.abs_diff(unix_now()) > 300 {
        return false;
    }
    if ts.to_string() != headers["x-agentbuddy-timestamp"] {
        return false; // decimal, no leading zeros
    }
    let canonical = host_request_canonical(origin, method, path, ts, nonce, &sha256_hex(body));
    let mut key = [0u8; 32];
    hex::decode_to_slice(host, &mut key).unwrap();
    let mut signature = [0u8; 64];
    hex::decode_to_slice(sig, &mut signature).unwrap();
    PublicKey::from_bytes(&key)
        .unwrap()
        .verify(canonical.as_bytes(), &Signature::from_bytes(&signature))
        .is_ok()
}

// === Harness ===============================================================

fn test_policy() -> OutboxPolicy {
    OutboxPolicy {
        initial_backoff: Duration::from_millis(50),
        max_backoff: Duration::from_millis(200),
        max_retry_after: Duration::from_secs(5),
        max_age: Duration::from_secs(24 * 60 * 60),
        max_items: 1000,
    }
}

fn test_timing() -> WatcherTiming {
    WatcherTiming {
        reconnect_initial: Duration::from_millis(50),
        reconnect_max: Duration::from_millis(200),
        rpc_timeout: Duration::from_secs(3),
        idle_linger: Duration::from_millis(300),
        snapshot_grace: Duration::from_millis(50),
        reverify_delay: Duration::from_millis(50),
        recheck_after_error: Duration::from_millis(200),
        stable_after: Duration::from_secs(1),
    }
}

struct Harness {
    service: PushService,
    reports: mpsc::UnboundedSender<TerminalReport>,
    config: Arc<ArcSwap<HostConfig>>,
    /// Worker origin grants are signed for.
    aud: String,
}

struct Options {
    worker_url: Option<String>,
    app_worker_url: Option<String>,
    codex: Option<Arc<dyn CodexConnector>>,
}

impl Options {
    fn worker(url: &str) -> Self {
        Self {
            worker_url: Some(url.to_string()),
            app_worker_url: None,
            codex: None,
        }
    }

    fn with_codex(mut self, connector: Arc<dyn CodexConnector>) -> Self {
        self.codex = Some(connector);
        self
    }
}

async fn start(dir: &Path, options: Options) -> Harness {
    let aud = options
        .worker_url
        .as_deref()
        .or(options.app_worker_url.as_deref())
        .and_then(|url| client::parse_worker_url(url).ok())
        .map(|url| url.origin().ascii_serialization())
        .unwrap_or_default();
    let mut config = HostConfig::default();
    config.push.worker_url = options.worker_url;
    config.push.request_timeout_secs = 2;
    let config = Arc::new(ArcSwap::from_pointee(config));
    let (reports_tx, reports_rx) = terminal_channel();
    let service = PushService::start(PushParams {
        config: Arc::clone(&config),
        secret_key: SecretKey::from_bytes(&HOST_SEED),
        files: PushFiles::in_dir(dir),
        app_worker_url: options.app_worker_url,
        reports_tx: reports_tx.clone(),
        reports_rx,
        codex: options.codex,
        policy: test_policy(),
        watcher_timing: test_timing(),
    })
    .await;
    Harness {
        service,
        reports: reports_tx,
        config,
        aud,
    }
}

impl Harness {
    async fn subscriptions(&self) -> Vec<Subscription> {
        self.service
            .core
            .state
            .lock()
            .await
            .data
            .subscriptions
            .values()
            .cloned()
            .collect()
    }

    async fn wait_state<F: FnMut(&PushData) -> bool>(&self, what: &str, mut condition: F) {
        let deadline = Instant::now() + WAIT;
        loop {
            {
                let state = self.service.core.state.lock().await;
                if condition(&state.data) {
                    return;
                }
            }
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn report(&self, agent: &str, thread: &str, turn: &str, terminal: Terminal) {
        self.reports
            .send(TerminalReport {
                agent: agent.into(),
                thread_id: thread.into(),
                turn_id: turn.into(),
                terminal,
            })
            .unwrap();
    }
}

fn device(seed: u8) -> SecretKey {
    if seed == 2 {
        SecretKey::from_bytes(&DEVICE_SEED)
    } else {
        SecretKey::from_bytes(&[seed; 32])
    }
}

struct GrantSpec<'a> {
    agent: &'a str,
    thread: &'a str,
    turn: &'a str,
    platform: &'a str,
    environment: Option<&'a str>,
    sealed: &'a str,
}

impl<'a> GrantSpec<'a> {
    fn ios(agent: &'a str, thread: &'a str, turn: &'a str) -> Self {
        Self {
            agent,
            thread,
            turn,
            platform: "ios",
            environment: Some("production"),
            sealed: SEALED_A,
        }
    }

    fn sealed(mut self, sealed: &'a str) -> Self {
        self.sealed = sealed;
        self
    }
}

fn signed_request(aud: &str, device: &SecretKey, spec: &GrantSpec<'_>) -> SubscribeRequest {
    let issued = unix_now();
    let expires = issued + 3600;
    let nonce = random_hex32();
    let device_id = device.public().to_string();
    let claims = GrantClaims {
        aud,
        host_id: HOST_ID,
        device_id: &device_id,
        platform: spec.platform,
        environment: spec.environment.unwrap_or("none"),
        sealed_target: spec.sealed,
        agent: spec.agent,
        thread_id: spec.thread,
        turn_id: spec.turn,
        issued,
        expires,
        nonce: &nonce,
    };
    let signature = hex::encode(device.sign(claims.canonical().as_bytes()).to_bytes());
    SubscribeRequest {
        agent: spec.agent.into(),
        thread_id: spec.thread.into(),
        turn_id: spec.turn.into(),
        target: PushTargetWire {
            platform: spec.platform.into(),
            apns_environment: spec.environment.map(str::to_string),
            sealed: spec.sealed.into(),
        },
        grant: PushGrantWire {
            device_id,
            issued,
            expires,
            nonce,
            signature,
        },
    }
}

async fn subscribe(
    h: &Harness,
    device: &SecretKey,
    spec: GrantSpec<'_>,
) -> Result<PushResponse, PushError> {
    h.service
        .subscribe(
            &device.public().to_string(),
            signed_request(&h.aud, device, &spec),
        )
        .await
}

fn unsubscribe_all() -> UnsubscribeRequest {
    UnsubscribeRequest {
        agent: None,
        thread_id: None,
        turn_id: None,
        all: true,
    }
}

// === Bridge + outbox tests =================================================

#[tokio::test]
async fn bridge_turn_registers_then_delivers_signed_event() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    let phone = device(2);

    let response = subscribe(&h, &phone, GrantSpec::ios("claude", "th-1", "tu-1"))
        .await
        .unwrap();
    assert!(matches!(
        response,
        PushResponse::Subscribe { terminal: None, .. }
    ));
    wait_until("register", || {
        worker.count("POST", "/v2/subscriptions") == 1
    })
    .await;
    h.wait_state("registered", |d| {
        d.subscriptions.values().all(|s| s.remote_id.is_some())
    })
    .await;

    let register = &worker.requests()[0];
    assert_eq!(register.body["deviceId"], phone.public().to_string());
    assert_eq!(register.body["platform"], "ios");
    assert_eq!(register.body["sealedTarget"], SEALED_A);
    assert!(
        register.body.get("pushToken").is_none(),
        "host never sees tokens"
    );
    let mut keys: Vec<&str> = register
        .body
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "agent",
            "apnsEnvironment",
            "deviceId",
            "expiresAt",
            "grantNonce",
            "grantSignature",
            "issuedAt",
            "platform",
            "sealedTarget",
            "threadId",
            "turnId"
        ]
    );
    assert_eq!(register.body["apnsEnvironment"], "production");
    assert_eq!(register.body["agent"], "claude");
    assert_eq!(register.body["threadId"], "th-1");
    assert_eq!(register.body["turnId"], "tu-1");
    assert!(is_lower_hex(
        register.body["grantNonce"].as_str().unwrap(),
        32
    ));
    assert!(is_lower_hex(
        register.body["grantSignature"].as_str().unwrap(),
        128
    ));
    assert!(register.body["issuedAt"].is_u64() && register.body["expiresAt"].is_u64());

    h.report("claude", "th-1", "tu-1", Terminal::completed(1_790_300_123));
    wait_until("event", || worker.count("POST", "/v2/events") == 1).await;
    h.wait_state("delivered", |d| {
        d.subscriptions.is_empty() && d.outbox.items.is_empty()
    })
    .await;

    let event = worker.requests().last().unwrap().clone();
    assert!(event.body["eventId"].as_str().unwrap().starts_with("evt_"));
    assert!(is_lower_hex(
        &event.body["eventId"].as_str().unwrap()[4..],
        32
    ));
    assert_eq!(
        event.body,
        json!({
            "eventId": event.body["eventId"],
            "agent": "claude", "threadId": "th-1", "turnId": "tu-1",
            "type": "completed", "reason": null, "occurredAt": 1_790_300_123u64
        })
    );
    assert_eq!(worker.bad_signatures.load(Ordering::SeqCst), 0);
    let status = h.service.status().await;
    assert!(status.enabled);
    assert_eq!(status.worker_host.as_deref(), Some("127.0.0.1"));
    assert!(status.last_success_at.is_some());
    assert_eq!(status.subscriptions, 0);
    assert_eq!(status.outbox_depth, 0);
}

#[tokio::test]
async fn terminal_before_subscribe_is_reported_immediately() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;

    h.report(
        "pi",
        "th-1",
        "tu-1",
        Terminal::failed(FailureReason::Interrupted, 1_790_300_000),
    );
    h.wait_state("recorded", |d| d.recent.to_vec().len() == 1)
        .await;
    let recorded_event_id = h.service.core.state.lock().await.data.recent.to_vec()[0]
        .event_id
        .clone();

    let response = subscribe(&h, &device(2), GrantSpec::ios("pi", "th-1", "tu-1"))
        .await
        .unwrap();
    assert_eq!(
        response,
        PushResponse::Subscribe {
            subscription: PushSubscriptionState::Pending,
            terminal: Some(PushTerminalWire {
                kind: PushTerminalType::Failed,
                occurred_at: 1_790_300_000
            })
        }
    );
    wait_until("event", || worker.count("POST", "/v2/events") == 1).await;
    let requests = worker.requests();
    assert_eq!(requests[0].path, "/v2/subscriptions", "register goes first");
    assert_eq!(requests[1].body["eventId"], recorded_event_id);
    assert_eq!(requests[1].body["type"], "failed");
    assert_eq!(requests[1].body["reason"], "interrupted");
}

#[tokio::test]
async fn event_waits_for_register_and_honors_retry_after() {
    let dir = tempfile::tempdir().unwrap();
    let registers = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&registers);
    let mut fallback = default_responder();
    let worker = TestWorker::start_with(Box::new(move |request: &Recorded| {
        if request.path == "/v2/subscriptions" && counter.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::json(503, json!({"error": "internal"})).with_header("Retry-After", "1");
        }
        fallback(request)
    }))
    .await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;

    subscribe(&h, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    wait_until("first register", || {
        worker.count("POST", "/v2/subscriptions") == 1
    })
    .await;
    // The turn ends while the register is backing off.
    h.report("claude", "th", "tu", Terminal::completed(1));
    wait_until("event", || worker.count("POST", "/v2/events") == 1).await;

    let requests = worker.requests();
    let paths: Vec<&str> = requests.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(
        paths,
        ["/v2/subscriptions", "/v2/subscriptions", "/v2/events"]
    );
    let gap = requests[1].at.duration_since(requests[0].at);
    assert!(
        gap >= Duration::from_millis(900),
        "Retry-After ignored: {gap:?}"
    );
    // A retry is re-signed with a fresh nonce, never replayed.
    assert_ne!(requests[0].nonce, requests[1].nonce);
    assert!(is_lower_hex(&requests[1].nonce, 32));
    h.wait_state("delivered", |d| d.subscriptions.is_empty())
        .await;
}

#[tokio::test]
async fn permanent_client_error_drops_subscription() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start_with(Box::new(|_: &Recorded| {
        Reply::json(403, json!({"error": "forbidden"}))
    }))
    .await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    subscribe(&h, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    h.wait_state("dropped", |d| {
        d.subscriptions.is_empty() && d.outbox.items.is_empty()
    })
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        worker.count("POST", "/v2/subscriptions"),
        1,
        "no retry on 4xx"
    );
    let status = h.service.status().await;
    assert!(status.last_error.unwrap().contains("forbidden"));
}

#[tokio::test]
async fn network_errors_are_retried_with_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let h = start(dir.path(), Options::worker(&url)).await;
    // Some hosts time out rather than refuse; keep attempts short.
    let mut cfg = (**h.config.load()).clone();
    cfg.push.request_timeout_secs = 1;
    h.config.store(Arc::new(cfg));
    subscribe(&h, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    h.wait_state("retries", |d| {
        d.outbox
            .items
            .first()
            .is_some_and(|item| item.attempts >= 2)
    })
    .await;
    let subs = h.subscriptions().await;
    assert_eq!(subs.len(), 1);
    assert!(subs[0].remote_id.is_none());
    assert!(h.service.status().await.last_error.is_some());
}

#[tokio::test]
async fn restart_resends_pending_event_with_same_event_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut fallback = default_responder();
    let worker = TestWorker::start_with(Box::new(move |request: &Recorded| {
        if request.path == "/v2/events" {
            Reply::json(500, json!({"error": "internal"}))
        } else {
            fallback(request)
        }
    }))
    .await;

    let first = start(dir.path(), Options::worker(&worker.url)).await;
    subscribe(&first, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    first
        .wait_state("registered", |d| {
            d.subscriptions.values().all(|s| s.remote_id.is_some())
        })
        .await;
    first.report("claude", "th", "tu", Terminal::completed(42));
    wait_until("failed event", || worker.count("POST", "/v2/events") >= 1).await;
    let event_id = worker.requests().last().unwrap().body["eventId"].clone();
    first.service.shutdown(Duration::from_secs(1)).await;
    drop(first);

    worker.set_responder(default_responder());
    let before = worker.count("POST", "/v2/events");
    let second = start(dir.path(), Options::worker(&worker.url)).await;
    wait_until("resent event", || {
        worker.count("POST", "/v2/events") > before
    })
    .await;
    let resent = worker.requests().last().unwrap().clone();
    assert_eq!(resent.path, "/v2/events");
    assert_eq!(resent.body["eventId"], event_id);
    assert_eq!(
        worker.count("POST", "/v2/subscriptions"),
        1,
        "no re-register"
    );
    second
        .wait_state("delivered", |d| d.subscriptions.is_empty())
        .await;
}

#[tokio::test]
async fn restart_resends_pending_registration() {
    let dir = tempfile::tempdir().unwrap();
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_url = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let first = start(dir.path(), Options::worker(&dead_url)).await;
    subscribe(&first, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    first.service.shutdown(Duration::from_millis(500)).await;
    drop(first);

    let worker = TestWorker::start().await;
    let second = start(dir.path(), Options::worker(&worker.url)).await;
    wait_until("register after restart", || {
        worker.count("POST", "/v2/subscriptions") == 1
    })
    .await;
    second
        .wait_state("registered", |d| {
            d.subscriptions.values().all(|s| s.remote_id.is_some())
        })
        .await;
}

#[tokio::test]
async fn unsubscribe_only_affects_requesting_device() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    let (a, b) = (device(2), device(3));
    subscribe(&h, &a, GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    subscribe(&h, &b, GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    h.wait_state("both registered", |d| {
        d.subscriptions.len() == 2 && d.subscriptions.values().all(|s| s.remote_id.is_some())
    })
    .await;
    let a_remote = h
        .subscriptions()
        .await
        .into_iter()
        .find(|s| s.device_id == a.public().to_string())
        .unwrap()
        .remote_id
        .unwrap();

    let removed = h
        .service
        .unsubscribe(&a.public().to_string(), unsubscribe_all())
        .await
        .unwrap();
    assert_eq!(removed, PushResponse::Unsubscribe { removed: 1 });
    wait_until("delete", || {
        worker.count("DELETE", "/v2/subscriptions/") == 1
    })
    .await;
    let delete = worker
        .requests()
        .into_iter()
        .find(|r| r.method == "DELETE")
        .unwrap();
    assert_eq!(delete.path, format!("/v2/subscriptions/{a_remote}"));
    h.wait_state("a gone", |d| d.subscriptions.len() == 1).await;
    let remaining = h.subscriptions().await;
    assert_eq!(remaining[0].device_id, b.public().to_string());
    assert!(!remaining[0].revoking);

    // A third device can't unsubscribe anyone else's subscriptions.
    let c = device(4);
    let removed = h
        .service
        .unsubscribe(&c.public().to_string(), unsubscribe_all())
        .await
        .unwrap();
    assert_eq!(removed, PushResponse::Unsubscribe { removed: 0 });

    h.report("claude", "th", "tu", Terminal::completed(7));
    wait_until("event", || worker.count("POST", "/v2/events") == 1).await;
    h.wait_state("b delivered", |d| d.subscriptions.is_empty())
        .await;
}

#[tokio::test]
async fn unsubscribe_filters_and_requires_a_selector() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    let a = device(2);
    subscribe(&h, &a, GrantSpec::ios("claude", "th", "tu-1"))
        .await
        .unwrap();
    subscribe(&h, &a, GrantSpec::ios("claude", "th", "tu-2"))
        .await
        .unwrap();

    let err = h
        .service
        .unsubscribe(
            &a.public().to_string(),
            UnsubscribeRequest {
                agent: None,
                thread_id: None,
                turn_id: None,
                all: false,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, error_codes::BAD_REQUEST);

    let removed = h
        .service
        .unsubscribe(
            &a.public().to_string(),
            UnsubscribeRequest {
                agent: Some("claude".into()),
                thread_id: Some("th".into()),
                turn_id: Some("tu-2".into()),
                all: false,
            },
        )
        .await
        .unwrap();
    assert_eq!(removed, PushResponse::Unsubscribe { removed: 1 });
    h.wait_state("one left", |d| d.subscriptions.len() == 1)
        .await;
    assert_eq!(h.subscriptions().await[0].turn_id, "tu-1");
}

#[tokio::test]
async fn unsubscribe_before_registration_stays_local() {
    // The Worker rejects the register before processing it (429), so it
    // never held the subscription: unsubscribing is purely local.
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start_with(Box::new(|_: &Recorded| {
        Reply::json(429, json!({"error": "rate_limited"})).with_header("Retry-After", "1")
    }))
    .await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    let a = device(2);
    subscribe(&h, &a, GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    h.wait_state("attempted", |d| {
        d.outbox.items.first().is_some_and(|i| i.attempts >= 1)
    })
    .await;
    assert!(
        h.subscriptions()
            .await
            .iter()
            .all(|s| !s.register_attempted)
    );
    h.service
        .unsubscribe(&a.public().to_string(), unsubscribe_all())
        .await
        .unwrap();
    h.wait_state("cleaned", |d| {
        d.subscriptions.is_empty() && d.outbox.items.is_empty()
    })
    .await;
    assert_eq!(worker.count("DELETE", "/v2/subscriptions/"), 0);
    assert_eq!(worker.count("POST", "/v2/subscriptions"), 1);
}

#[tokio::test]
async fn unsubscribe_after_unconfirmed_register_still_revokes_remotely() {
    // The first register reaches the Worker but no id comes back (here: a
    // 503 with Retry-After). The device unsubscribes before the retry: the
    // host must still learn the id and delete it, or other devices' events
    // for the same turn would keep notifying this one.
    let dir = tempfile::tempdir().unwrap();
    let registers = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&registers);
    let mut fallback = default_responder();
    let worker = TestWorker::start_with(Box::new(move |request: &Recorded| {
        let reply = fallback(request);
        if request.path == "/v2/subscriptions" && counter.fetch_add(1, Ordering::SeqCst) == 0 {
            return Reply::json(503, json!({"error": "internal"})).with_header("Retry-After", "1");
        }
        reply
    }))
    .await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    let a = device(2);
    subscribe(&h, &a, GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    h.wait_state("attempt recorded", |d| {
        d.subscriptions
            .values()
            .all(|s| s.register_attempted && s.remote_id.is_none())
    })
    .await;
    let removed = h
        .service
        .unsubscribe(&a.public().to_string(), unsubscribe_all())
        .await
        .unwrap();
    assert_eq!(removed, PushResponse::Unsubscribe { removed: 1 });

    wait_until("delete", || {
        worker.count("DELETE", "/v2/subscriptions/") == 1
    })
    .await;
    let requests = worker.requests();
    let register = requests
        .iter()
        .rfind(|r| r.path == "/v2/subscriptions")
        .unwrap();
    let delete = requests.iter().find(|r| r.method == "DELETE").unwrap();
    assert_eq!(worker.count("POST", "/v2/subscriptions"), 2);
    assert!(
        register.at <= delete.at,
        "re-register first to learn the id"
    );
    assert!(delete.path.starts_with("/v2/subscriptions/sub_"));
    h.wait_state("gone", |d| {
        d.subscriptions.is_empty() && d.outbox.items.is_empty()
    })
    .await;
}

#[tokio::test]
async fn resubscribe_is_idempotent_and_token_change_replaces_registration() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    let a = device(2);
    subscribe(&h, &a, GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    h.wait_state("registered", |d| {
        d.subscriptions.values().all(|s| s.remote_id.is_some())
    })
    .await;
    let again = subscribe(&h, &a, GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    assert_eq!(
        again,
        PushResponse::Subscribe {
            subscription: PushSubscriptionState::Registered,
            terminal: None
        }
    );
    assert_eq!(worker.count("POST", "/v2/subscriptions"), 1);
    let old_remote = h.subscriptions().await[0].remote_id.clone().unwrap();

    subscribe(
        &h,
        &a,
        GrantSpec::ios("claude", "th", "tu").sealed(SEALED_B),
    )
    .await
    .unwrap();
    wait_until("re-register", || {
        worker.count("POST", "/v2/subscriptions") == 2
    })
    .await;
    let requests = worker.requests();
    let delete_at = requests.iter().position(|r| r.method == "DELETE").unwrap();
    let register_at = requests
        .iter()
        .rposition(|r| r.path == "/v2/subscriptions")
        .unwrap();
    assert!(delete_at < register_at, "old registration is deleted first");
    assert_eq!(
        requests[delete_at].path,
        format!("/v2/subscriptions/{old_remote}")
    );
    assert_eq!(requests[register_at].body["sealedTarget"], SEALED_B);
    h.wait_state("replaced", |d| {
        d.subscriptions.len() == 1
            && d.subscriptions
                .values()
                .all(|s| s.sealed_target == SEALED_B && s.remote_id.is_some())
    })
    .await;
}

#[tokio::test]
async fn one_event_post_retires_every_reported_subscription() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    subscribe(&h, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    subscribe(&h, &device(3), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    h.wait_state("both registered", |d| {
        d.subscriptions.len() == 2 && d.subscriptions.values().all(|s| s.remote_id.is_some())
    })
    .await;
    h.report("claude", "th", "tu", Terminal::completed(9));
    h.wait_state("both delivered", |d| {
        d.subscriptions.is_empty() && d.outbox.items.is_empty()
    })
    .await;
    assert_eq!(worker.count("POST", "/v2/events"), 1);
}

#[tokio::test]
async fn housekeeping_expires_subscriptions() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    subscribe(&h, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap();
    {
        let mut state = h.service.core.state.lock().await;
        for sub in state.data.subscriptions.values_mut() {
            sub.expires_at = unix_now() - 1;
        }
    }
    h.service.core.housekeeping().await;
    assert!(h.subscriptions().await.is_empty());
    let persisted = PushFiles::in_dir(dir.path()).load(unix_now()).await;
    assert!(persisted.subscriptions.is_empty());
}

// === Validation / capability tests ========================================

fn agent_info(name: &str, available: bool) -> AgentInfo {
    AgentInfo {
        name: name.into(),
        display_name: name.into(),
        wire: AgentWire::Jsonl,
        available,
        presentation: None,
        capabilities: None,
    }
}

#[tokio::test]
async fn host_info_lists_only_observable_agents() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    let list = [
        agent_info("codex", true),
        agent_info("claude", true),
        agent_info("pi", false),
        agent_info("shell", true),
    ];
    let info = h.service.host_info(&list);
    assert_eq!(info.features, vec!["push.v1".to_string()]);
    let push = info.push.unwrap();
    assert!(push.enabled);
    // No codex connector (Stdio mode) → codex excluded; pi unavailable;
    // shell has no turns.
    assert_eq!(push.agents, vec!["claude".to_string()]);

    let err = subscribe(&h, &device(2), GrantSpec::ios("codex", "th", "tu"))
        .await
        .unwrap_err();
    assert_eq!(err.code, error_codes::UNSUPPORTED);
    let err = subscribe(&h, &device(2), GrantSpec::ios("shell", "th", "tu"))
        .await
        .unwrap_err();
    assert_eq!(err.code, error_codes::UNSUPPORTED);

    // Disabled agents are not offered either.
    let mut cfg = (**h.config.load()).clone();
    cfg.agents.claude.enabled = false;
    h.config.store(Arc::new(cfg));
    assert!(h.service.host_info(&list).push.unwrap().agents.is_empty());
    let err = subscribe(&h, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap_err();
    assert_eq!(err.code, error_codes::UNSUPPORTED);
}

#[tokio::test]
async fn push_disabled_without_worker_url_or_by_config() {
    let dir = tempfile::tempdir().unwrap();
    let list = [agent_info("claude", true)];

    let h = start(
        dir.path(),
        Options {
            worker_url: None,
            app_worker_url: None,
            codex: None,
        },
    )
    .await;
    let push = h.service.host_info(&list).push.unwrap();
    assert!(!push.enabled);
    assert!(push.agents.is_empty());
    let err = subscribe(&h, &device(2), GrantSpec::ios("claude", "th", "tu"))
        .await
        .unwrap_err();
    assert_eq!(err.code, error_codes::UNSUPPORTED);
    assert!(!h.service.status().await.enabled);

    // The App-provided default URL enables push…
    let worker = TestWorker::start().await;
    let dir2 = tempfile::tempdir().unwrap();
    let h = start(
        dir2.path(),
        Options {
            worker_url: None,
            app_worker_url: Some(worker.url.clone()),
            codex: None,
        },
    )
    .await;
    assert!(h.service.host_info(&list).push.unwrap().enabled);
    // …an empty config URL disables it again, as does enabled = false or a
    // non-loopback plain-http URL.
    for mutate in [
        (|c: &mut HostConfig| c.push.worker_url = Some(String::new())) as fn(&mut HostConfig),
        |c: &mut HostConfig| c.push.enabled = false,
        |c: &mut HostConfig| c.push.worker_url = Some("http://push.example".into()),
    ] {
        let mut cfg = HostConfig::default();
        mutate(&mut cfg);
        h.config.store(Arc::new(cfg));
        assert!(!h.service.host_info(&list).push.unwrap().enabled);
    }
}

#[tokio::test]
async fn subscribe_validates_grant_and_fields() {
    let dir = tempfile::tempdir().unwrap();
    let worker = TestWorker::start().await;
    let h = start(dir.path(), Options::worker(&worker.url)).await;
    let phone = device(2);
    let node = phone.public().to_string();

    // Grant signed by another device than the iroh peer.
    let other = device(3);
    let request = signed_request(&h.aud, &other, &GrantSpec::ios("claude", "th", "tu"));
    let err = h.service.subscribe(&node, request).await.unwrap_err();
    assert_eq!(err.code, error_codes::INVALID_GRANT);

    // Claimed device id matches but the signature is from someone else.
    let mut request = signed_request(&h.aud, &other, &GrantSpec::ios("claude", "th", "tu"));
    request.grant.device_id = node.clone();
    let err = h.service.subscribe(&node, request).await.unwrap_err();
    assert_eq!(err.code, error_codes::INVALID_GRANT);

    // Tampered turn id after signing.
    let mut request = signed_request(&h.aud, &phone, &GrantSpec::ios("claude", "th", "tu"));
    request.turn_id = "other".into();
    let err = h.service.subscribe(&node, request).await.unwrap_err();
    assert_eq!(err.code, error_codes::INVALID_GRANT);

    // The host swaps in another sealed target after signing.
    let mut request = signed_request(&h.aud, &phone, &GrantSpec::ios("claude", "th", "tu"));
    request.target.sealed = SEALED_B.into();
    let err = h.service.subscribe(&node, request).await.unwrap_err();
    assert_eq!(err.code, error_codes::INVALID_GRANT);

    // Grant meant for another Worker deployment.
    let request = signed_request(
        "https://other.example.test",
        &phone,
        &GrantSpec::ios("claude", "th", "tu"),
    );
    let err = h.service.subscribe(&node, request).await.unwrap_err();
    assert_eq!(err.code, error_codes::INVALID_GRANT);

    // Expired grant.
    let mut request = signed_request(&h.aud, &phone, &GrantSpec::ios("claude", "th", "tu"));
    request.grant.expires = request.grant.issued;
    let err = h.service.subscribe(&node, request).await.unwrap_err();
    assert_eq!(err.code, error_codes::INVALID_GRANT);

    let long = "x".repeat(129);
    // 43 × 3-byte characters = 129 UTF-8 bytes.
    let long_utf8 = "中".repeat(43);
    let long_agent = "a".repeat(65);
    for (spec, what) in [
        (GrantSpec::ios("bad agent!", "th", "tu"), "agent charset"),
        (GrantSpec::ios(&long_agent, "th", "tu"), "long agent"),
        (GrantSpec::ios("", "th", "tu"), "empty agent"),
        (GrantSpec::ios("claude", &long, "tu"), "long thread"),
        (
            GrantSpec::ios("claude", "th", &long_utf8),
            "long utf-8 turn",
        ),
        (GrantSpec::ios("claude", "th\u{7f}", "tu"), "DEL"),
        (GrantSpec::ios("claude", "th", ""), "empty turn"),
        (GrantSpec::ios("claude", "th\nhost=evil", "tu"), "newline"),
        (
            GrantSpec {
                environment: None,
                ..GrantSpec::ios("claude", "th", "tu")
            },
            "ios without environment",
        ),
        (
            GrantSpec {
                environment: Some("staging"),
                ..GrantSpec::ios("claude", "th", "tu")
            },
            "bad environment",
        ),
        (
            GrantSpec {
                platform: "android",
                environment: Some("production"),
                sealed: "AQG-android",
                ..GrantSpec::ios("claude", "th", "tu")
            },
            "android with apns environment",
        ),
        (
            GrantSpec {
                platform: "windows",
                ..GrantSpec::ios("claude", "th", "tu")
            },
            "bad platform",
        ),
        (
            GrantSpec::ios("claude", "th", "tu").sealed("padded+/=="),
            "sealed not base64url",
        ),
        (
            GrantSpec::ios("claude", "th", "tu").sealed(""),
            "empty sealed",
        ),
    ] {
        let err = subscribe(&h, &phone, spec).await.unwrap_err();
        assert_eq!(err.code, error_codes::BAD_REQUEST, "{what}");
    }
    assert!(h.subscriptions().await.is_empty());

    // Well-formed but unknown agent: unsupported rather than malformed.
    let err = subscribe(&h, &phone, GrantSpec::ios("my.agent-2", "th", "tu"))
        .await
        .unwrap_err();
    assert_eq!(err.code, error_codes::UNSUPPORTED);
    // Multi-byte ids within 128 bytes are fine.
    let ok = subscribe(&h, &phone, GrantSpec::ios("claude", "线程", "tu-中")).await;
    assert!(ok.is_ok(), "{ok:?}");
    h.service
        .unsubscribe(&node, unsubscribe_all())
        .await
        .unwrap();
    h.wait_state("cleared", |d| d.subscriptions.is_empty())
        .await;
    let registers_before = worker.count("POST", "/v2/subscriptions");

    // Android: no environment.
    let ok = subscribe(
        &h,
        &phone,
        GrantSpec {
            platform: "android",
            environment: None,
            sealed: "AQG-android_sealed-target",
            ..GrantSpec::ios("claude", "th", "tu")
        },
    )
    .await;
    assert!(ok.is_ok(), "{ok:?}");
    wait_until("android register", || {
        worker.count("POST", "/v2/subscriptions") > registers_before
    })
    .await;
    let register = worker
        .requests()
        .into_iter()
        .rfind(|r| r.path == "/v2/subscriptions")
        .unwrap();
    assert_eq!(register.body["platform"], "android");
    assert!(register.body.get("apnsEnvironment").is_none());
}

#[test]
fn reconcile_repairs_cross_file_gaps() {
    let sub = |id: &str, remote: Option<&str>, revoking: bool, terminal: bool| Subscription {
        id: id.into(),
        device_id: "d".into(),
        agent: "claude".into(),
        thread_id: "t".into(),
        turn_id: id.into(),
        platform: store::Platform::Ios,
        sealed_target: "AQG-x".into(),
        apns_environment: Some(store::ApnsEnvironment::Sandbox),
        issued_at: 1,
        expires_at: u64::MAX,
        grant_nonce: "n".into(),
        grant_signature: "s".into(),
        created_at: 1,
        remote_id: remote.map(str::to_string),
        register_attempted: remote.is_some(),
        revoking,
        terminal: terminal.then(|| SubscriptionTerminal {
            terminal: Terminal::completed(5),
            event_id: format!("evt_{id}"),
        }),
    };
    let mut data = PushData::default();
    for s in [
        sub("unregistered", None, false, false),
        sub("terminal", Some("sub_t"), false, true),
        sub("revoking-remote", Some("sub_r"), true, false),
        sub("revoking-local", None, true, false),
    ] {
        data.subscriptions.insert(s.id.clone(), s);
    }
    data.outbox.items.push(OutboxItem::new(
        "x".into(),
        OutboxAction::Register {
            subscription: "ghost".into(),
        },
        1,
    ));
    assert!(reconcile(&mut data, 10));
    let actions: Vec<(String, &str)> = data
        .outbox
        .items
        .iter()
        .map(|i| {
            (
                i.action.subscription().unwrap().to_string(),
                i.action.label(),
            )
        })
        .collect();
    assert!(actions.contains(&("unregistered".into(), "register")));
    assert!(actions.contains(&("terminal".into(), "event")));
    assert!(actions.contains(&("revoking-remote".into(), "revoke")));
    assert!(!actions.iter().any(|(id, _)| id == "ghost"));
    assert!(!data.subscriptions.contains_key("revoking-local"));
    let event = data
        .outbox
        .items
        .iter()
        .find_map(|i| match &i.action {
            OutboxAction::Event { event, .. } => Some(event.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(event.event_id, "evt_terminal", "event id is stable");
    assert!(!reconcile(&mut data, 11), "idempotent");
}

#[test]
fn reconcile_puts_a_missing_revoke_ahead_of_the_replacement() {
    let base = Subscription {
        id: "old".into(),
        device_id: "d".into(),
        agent: "claude".into(),
        thread_id: "t".into(),
        turn_id: "u".into(),
        platform: store::Platform::Ios,
        sealed_target: "AQG-old".into(),
        apns_environment: Some(store::ApnsEnvironment::Production),
        issued_at: 1,
        expires_at: u64::MAX,
        grant_nonce: "n1".into(),
        grant_signature: "s1".into(),
        created_at: 1,
        remote_id: Some("sub_old".into()),
        register_attempted: true,
        revoking: true,
        terminal: None,
    };
    let replacement = Subscription {
        id: "new".into(),
        sealed_target: "AQG-new".into(),
        remote_id: None,
        register_attempted: false,
        revoking: false,
        ..base.clone()
    };
    let chain = base.chain();
    let mut data = PushData::default();
    data.subscriptions.insert(base.id.clone(), base);
    data.subscriptions
        .insert(replacement.id.clone(), replacement);
    // Crash after queuing the replacement's register but before the revoke
    // of the old registration was persisted.
    data.outbox.items.push(OutboxItem::new(
        chain,
        OutboxAction::Register {
            subscription: "new".into(),
        },
        1,
    ));
    assert!(reconcile(&mut data, 2));
    let order: Vec<(&str, &str)> = data
        .outbox
        .items
        .iter()
        .map(|i| (i.action.subscription().unwrap(), i.action.label()))
        .collect();
    assert_eq!(order, [("old", "revoke"), ("new", "register")]);
}

#[cfg(unix)]
mod codex_side {
    //! Codex watcher tests against a fake app-server on a Unix socket.
    use std::path::PathBuf;

    use futures::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;

    use super::super::codex::{CodexPushTarget, WsStream, connect_target};
    use super::*;

    #[tokio::test]
    async fn codex_is_offered_when_a_watcher_exists() {
        let dir = tempfile::tempdir().unwrap();
        let worker = TestWorker::start().await;
        let fake = FakeAppServer::start(|_, _| None).await;
        let h = start(
            dir.path(),
            Options::worker(&worker.url).with_codex(fake.connector()),
        )
        .await;
        let info = h.service.host_info(&[agent_info("codex", true)]);
        assert_eq!(info.push.unwrap().agents, vec!["codex".to_string()]);
    }

    // === Fake Codex app-server =================================================

    enum FakeCommand {
        Send(Value),
        Close,
    }

    type FakeHandler = Arc<dyn Fn(&Value, usize) -> Option<Value> + Send + Sync>;

    #[derive(Clone)]
    struct FakeAppServer {
        path: PathBuf,
        _dir: Arc<tempfile::TempDir>,
        received: Arc<StdMutex<Vec<(usize, Value)>>>,
        connections: Arc<AtomicUsize>,
        control: Arc<StdMutex<Option<mpsc::UnboundedSender<FakeCommand>>>>,
    }

    struct UdsConnector(PathBuf);

    #[async_trait::async_trait]
    impl CodexConnector for UdsConnector {
        async fn connect(&self) -> anyhow::Result<WsStream> {
            connect_target(&CodexPushTarget::Unix(self.0.clone())).await
        }
    }

    impl FakeAppServer {
        /// `handler(request, connection_index)` returns the `result` for a
        /// request; `None` falls back to built-in defaults.
        async fn start<F>(handler: F) -> Self
        where
            F: Fn(&Value, usize) -> Option<Value> + Send + Sync + 'static,
        {
            let dir = tempfile::Builder::new()
                .prefix("cx")
                .tempdir_in("/tmp")
                .unwrap();
            let path = dir.path().join("s.sock");
            let listener = tokio::net::UnixListener::bind(&path).unwrap();
            let server = Self {
                path,
                _dir: Arc::new(dir),
                received: Arc::default(),
                connections: Arc::default(),
                control: Arc::default(),
            };
            let handler: FakeHandler = Arc::new(handler);
            let state = server.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    let index = state.connections.fetch_add(1, Ordering::SeqCst);
                    let (tx, rx) = mpsc::unbounded_channel();
                    *state.control.lock().unwrap() = Some(tx);
                    let state = state.clone();
                    let handler = Arc::clone(&handler);
                    tokio::spawn(async move {
                        state.serve(stream, index, rx, handler).await;
                    });
                }
            });
            server
        }

        async fn serve(
            &self,
            stream: tokio::net::UnixStream,
            index: usize,
            mut control: mpsc::UnboundedReceiver<FakeCommand>,
            handler: FakeHandler,
        ) {
            let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                return;
            };
            loop {
                tokio::select! {
                    message = ws.next() => {
                        let Some(Ok(Message::Text(text))) = message else { break };
                        let value: Value = serde_json::from_str(text.as_str()).unwrap();
                        self.received.lock().unwrap().push((index, value.clone()));
                        let (Some(id), Some(method)) = (value.get("id"), value.get("method").and_then(Value::as_str)) else {
                            continue;
                        };
                        let result = handler(&value, index).unwrap_or_else(|| default_result(method));
                        let reply = json!({"id": id, "result": result});
                        if ws.send(Message::Text(reply.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    command = control.recv() => match command {
                        Some(FakeCommand::Send(value)) => {
                            if ws.send(Message::Text(value.to_string().into())).await.is_err() {
                                break;
                            }
                        }
                        Some(FakeCommand::Close) | None => break,
                    }
                }
            }
        }

        fn connector(&self) -> Arc<dyn CodexConnector> {
            Arc::new(UdsConnector(self.path.clone()))
        }

        fn send(&self, value: Value) {
            self.control
                .lock()
                .unwrap()
                .as_ref()
                .expect("a connection")
                .send(FakeCommand::Send(value))
                .unwrap();
        }

        fn close(&self) {
            if let Some(tx) = self.control.lock().unwrap().as_ref() {
                let _ = tx.send(FakeCommand::Close);
            }
        }

        fn frames(&self, method: &str) -> Vec<(usize, Value)> {
            self.received
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, v)| v.get("method").and_then(Value::as_str) == Some(method))
                .cloned()
                .collect()
        }

        fn all_frames(&self) -> Vec<Value> {
            self.received
                .lock()
                .unwrap()
                .iter()
                .map(|(_, v)| v.clone())
                .collect()
        }
    }

    fn default_result(method: &str) -> Value {
        match method {
            "initialize" => json!({
                "userAgent": "codex_app_server_daemon/0.1.0", "codexHome": "/tmp/codex",
                "platformFamily": "unix", "platformOs": "macos"
            }),
            "thread/unsubscribe" => json!({"status": "unsubscribed"}),
            _ => json!({}),
        }
    }

    fn thread_result(status: &str, turns: &[(&str, &str)]) -> Value {
        json!({
            "thread": {
                "id": "th",
                "status": {"type": status},
                "turns": turns.iter().map(|(id, status)| json!({
                    "id": id, "status": status, "items": [], "completedAt": 1_790_300_500u64
                })).collect::<Vec<_>>()
            }
        })
    }

    fn turn_completed(thread: &str, turn: &str, status: &str) -> Value {
        json!({
            "method": "turn/completed",
            "params": {"threadId": thread, "turn": {"id": turn, "status": status, "items": [], "completedAt": 1_790_300_900u64}}
        })
    }

    fn spawn_watcher(
        fake: &FakeAppServer,
    ) -> (
        codex::CodexWatcherHandle,
        mpsc::UnboundedReceiver<TerminalReport>,
        tokio::sync::watch::Sender<bool>,
    ) {
        let (reports_tx, reports_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (handle, _task) =
            codex::spawn_codex_watcher(fake.connector(), reports_tx, shutdown_rx, test_timing());
        (handle, reports_rx, shutdown_tx)
    }

    async fn next_report(rx: &mut mpsc::UnboundedReceiver<TerminalReport>) -> TerminalReport {
        tokio::time::timeout(WAIT, rx.recv())
            .await
            .expect("report in time")
            .expect("channel open")
    }

    #[tokio::test]
    async fn codex_watcher_resumes_active_thread_and_waits_for_matching_turn() {
        let fake = FakeAppServer::start(|request, _| match request["method"].as_str()? {
            "thread/read" | "thread/resume" => Some(thread_result(
                "active",
                &[("t0", "completed"), ("t1", "inProgress")],
            )),
            _ => None,
        })
        .await;
        let (handle, mut reports, _shutdown) = spawn_watcher(&fake);

        let first = handle
            .track_and_wait("th", "t1", Duration::from_secs(5))
            .await;
        assert_eq!(first, None, "still running");
        wait_until("resume", || !fake.frames("thread/resume").is_empty()).await;

        // Handshake: whitelisted client name, experimental API, initialized.
        let init = &fake.frames("initialize")[0].1;
        assert_eq!(
            init["params"]["clientInfo"]["name"],
            "codex_app_server_daemon"
        );
        assert_eq!(init["params"]["capabilities"]["experimentalApi"], true);
        assert!(init.get("jsonrpc").is_none());
        assert_eq!(fake.frames("initialized").len(), 1);
        let read = &fake.frames("thread/read")[0].1;
        assert_eq!(
            read["params"],
            json!({"threadId": "th", "includeTurns": true})
        );
        // Resume carries no overrides.
        assert_eq!(
            fake.frames("thread/resume")[0].1["params"],
            json!({"threadId": "th"})
        );

        // An approval request reaches the watcher: it must never answer it.
        fake.send(json!({
            "id": "srv-1",
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "th", "turnId": "t1", "itemId": "i1"}
        }));
        // Another turn of the same thread finishing is not ours.
        fake.send(turn_completed("th", "t0", "completed"));
        fake.send(turn_completed("th", "t1", "failed"));

        let report = next_report(&mut reports).await;
        assert_eq!(report.agent, "codex");
        assert_eq!(report.turn_id, "t1");
        assert_eq!(report.terminal.kind, TerminalKind::Failed);
        assert_eq!(report.terminal.reason, Some(FailureReason::Error));
        assert_eq!(report.terminal.occurred_at, 1_790_300_900);

        wait_until("unsubscribe", || {
            !fake.frames("thread/unsubscribe").is_empty()
        })
        .await;
        assert_eq!(
            fake.frames("thread/unsubscribe")[0].1["params"],
            json!({"threadId": "th"})
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(reports.try_recv().is_err(), "exactly one report");
        assert!(
            !fake
                .all_frames()
                .iter()
                .any(|frame| frame.get("id") == Some(&json!("srv-1"))),
            "server request was answered"
        );
    }

    #[tokio::test]
    async fn codex_watcher_never_reports_on_disconnect_and_rechecks_after_reconnect() {
        let fake =
            FakeAppServer::start(|request, connection| match request["method"].as_str()? {
                "thread/read" if connection == 0 => {
                    Some(thread_result("active", &[("t1", "inProgress")]))
                }
                "thread/resume" => Some(thread_result("active", &[("t1", "inProgress")])),
                // After the app-server restart the failed turn reads back as
                // completed, but the thread is in systemError.
                "thread/read" => Some(thread_result("systemError", &[("t1", "completed")])),
                _ => None,
            })
            .await;
        let (handle, mut reports, _shutdown) = spawn_watcher(&fake);
        handle.track("th", "t1");
        wait_until("resume", || !fake.frames("thread/resume").is_empty()).await;

        fake.close();
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(reports.try_recv().is_err(), "disconnect is not a terminal");

        let report = next_report(&mut reports).await;
        assert!(fake.connections.load(Ordering::SeqCst) >= 2, "reconnected");
        assert_eq!(report.terminal.kind, TerminalKind::Failed);
        assert_eq!(report.terminal.reason, Some(FailureReason::Error));
    }

    #[tokio::test]
    async fn codex_watcher_reports_already_finished_turn_without_resuming() {
        let fake = FakeAppServer::start(|request, _| match request["method"].as_str()? {
            "thread/read" => Some(thread_result("notLoaded", &[("t1", "interrupted")])),
            _ => None,
        })
        .await;
        let (handle, mut reports, _shutdown) = spawn_watcher(&fake);
        let verdict = handle
            .track_and_wait("th", "t1", Duration::from_secs(5))
            .await
            .expect("already terminal");
        assert_eq!(verdict.kind, TerminalKind::Failed);
        assert_eq!(verdict.reason, Some(FailureReason::Interrupted));
        assert_eq!(next_report(&mut reports).await.turn_id, "t1");
        assert!(fake.frames("thread/resume").is_empty(), "no cold resume");
    }

    #[tokio::test]
    async fn codex_watcher_leaves_not_loaded_threads_alone_until_they_become_active() {
        let active = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&active);
        let fake = FakeAppServer::start(move |request, _| match request["method"].as_str()? {
            "thread/read" if !flag.load(Ordering::SeqCst) => Some(thread_result("notLoaded", &[])),
            "thread/read" | "thread/resume" => {
                Some(thread_result("active", &[("t1", "inProgress")]))
            }
            _ => None,
        })
        .await;
        let (handle, mut reports, _shutdown) = spawn_watcher(&fake);
        assert_eq!(
            handle
                .track_and_wait("th", "t1", Duration::from_secs(5))
                .await,
            None
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(fake.frames("thread/resume").is_empty(), "no cold resume");

        // Someone (the phone, the desktop) loads the thread and starts the turn:
        // the passive layer notices and the watcher subscribes.
        active.store(true, Ordering::SeqCst);
        fake.send(json!({"method": "thread/status/changed", "params": {"threadId": "th", "status": {"type": "active", "activeFlags": []}}}));
        wait_until("resume", || !fake.frames("thread/resume").is_empty()).await;
        fake.send(turn_completed("th", "t1", "completed"));
        let report = next_report(&mut reports).await;
        assert_eq!(report.terminal.kind, TerminalKind::Completed);
    }

    #[tokio::test]
    async fn codex_watcher_uses_system_error_to_tell_failed_from_completed() {
        let errored = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = Arc::clone(&errored);
        let fake = FakeAppServer::start(move |request, _| match request["method"].as_str()? {
            "thread/read" if flag.load(Ordering::SeqCst) => {
                // Transient read right after the error: the in-progress turn is
                // rendered interrupted while the thread sits in systemError.
                Some(thread_result("systemError", &[("t1", "interrupted")]))
            }
            "thread/read" | "thread/resume" => {
                Some(thread_result("active", &[("t1", "inProgress")]))
            }
            _ => None,
        })
        .await;
        let (handle, mut reports, _shutdown) = spawn_watcher(&fake);
        handle.track("th", "t1");
        wait_until("resume", || !fake.frames("thread/resume").is_empty()).await;
        errored.store(true, Ordering::SeqCst);
        fake.send(json!({"method": "thread/status/changed", "params": {"threadId": "th", "status": {"type": "systemError"}}}));
        let report = next_report(&mut reports).await;
        assert_eq!(report.terminal.kind, TerminalKind::Failed);
        assert_eq!(report.terminal.reason, Some(FailureReason::Error));
    }

    #[tokio::test]
    async fn codex_watcher_untrack_releases_the_thread() {
        let fake = FakeAppServer::start(|request, _| match request["method"].as_str()? {
            "thread/read" | "thread/resume" => {
                Some(thread_result("active", &[("t1", "inProgress")]))
            }
            _ => None,
        })
        .await;
        let (handle, mut reports, _shutdown) = spawn_watcher(&fake);
        handle
            .track_and_wait("th", "t1", Duration::from_secs(5))
            .await;
        handle.untrack("th", "t1");
        wait_until("unsubscribe", || {
            !fake.frames("thread/unsubscribe").is_empty()
        })
        .await;
        fake.send(turn_completed("th", "t1", "completed"));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            reports.try_recv().is_err(),
            "untracked turns are not reported"
        );
    }

    #[tokio::test]
    async fn codex_subscription_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        let worker = TestWorker::start().await;
        let fake = FakeAppServer::start(|request, _| match request["method"].as_str()? {
            "thread/read" => Some(thread_result("idle", &[("t1", "completed")])),
            _ => None,
        })
        .await;
        let h = start(
            dir.path(),
            Options::worker(&worker.url).with_codex(fake.connector()),
        )
        .await;
        let response = subscribe(&h, &device(2), GrantSpec::ios("codex", "th", "t1"))
            .await
            .unwrap();
        let PushResponse::Subscribe { terminal, .. } = response else {
            panic!("subscribe response");
        };
        assert_eq!(
            terminal,
            Some(PushTerminalWire {
                kind: PushTerminalType::Completed,
                occurred_at: 1_790_300_500
            })
        );
        wait_until("event", || worker.count("POST", "/v2/events") == 1).await;
        let event = worker.requests().last().unwrap().clone();
        assert_eq!(event.body["agent"], "codex");
        assert_eq!(event.body["type"], "completed");
        h.wait_state("delivered", |d| d.subscriptions.is_empty())
            .await;
    }

    #[tokio::test]
    async fn codex_pending_subscription_is_rewatched_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let worker = TestWorker::start().await;
        let running = FakeAppServer::start(|request, _| match request["method"].as_str()? {
            "thread/read" | "thread/resume" => {
                Some(thread_result("active", &[("t1", "inProgress")]))
            }
            _ => None,
        })
        .await;
        let first = start(
            dir.path(),
            Options::worker(&worker.url).with_codex(running.connector()),
        )
        .await;
        subscribe(&first, &device(2), GrantSpec::ios("codex", "th", "t1"))
            .await
            .unwrap();
        first
            .wait_state("registered", |d| {
                d.subscriptions.values().all(|s| s.remote_id.is_some())
            })
            .await;
        first.service.shutdown(Duration::from_millis(500)).await;
        drop(first);

        // The turn finished while the host was down.
        let finished = FakeAppServer::start(|request, _| match request["method"].as_str()? {
            "thread/read" => Some(thread_result(
                "idle",
                &[("t1", "interrupted"), ("t2", "completed")],
            )),
            _ => None,
        })
        .await;
        let second = start(
            dir.path(),
            Options::worker(&worker.url).with_codex(finished.connector()),
        )
        .await;
        wait_until("event after restart", || {
            worker.count("POST", "/v2/events") == 1
        })
        .await;
        let event = worker.requests().last().unwrap().clone();
        assert_eq!(event.body["type"], "failed");
        assert_eq!(event.body["reason"], "interrupted");
        second
            .wait_state("delivered", |d| d.subscriptions.is_empty())
            .await;
    }

    #[tokio::test]
    async fn codex_watcher_unsubscribes_when_resume_snapshot_shows_finished_turn() {
        // The turn ends between thread/read and thread/resume: no
        // turn/completed will follow, the snapshot is the verdict, and the
        // subscription the resume created must be released.
        let fake = FakeAppServer::start(|request, _| match request["method"].as_str()? {
            "thread/read" => Some(thread_result("active", &[("t1", "inProgress")])),
            "thread/resume" => Some(thread_result("idle", &[("t1", "completed")])),
            _ => None,
        })
        .await;
        let (handle, mut reports, _shutdown) = spawn_watcher(&fake);
        handle.track("th", "t1");
        let report = next_report(&mut reports).await;
        assert_eq!(report.terminal.kind, TerminalKind::Completed);
        wait_until("unsubscribe", || {
            !fake.frames("thread/unsubscribe").is_empty()
        })
        .await;
    }

    #[tokio::test]
    async fn codex_watcher_reverifies_interrupted_resume_snapshot() {
        // A resume snapshot can show a just-finished turn as interrupted
        // (thread already idle, completion not yet flushed); the follow-up
        // thread/read decides.
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&reads);
        let fake = FakeAppServer::start(move |request, _| match request["method"].as_str()? {
            "thread/read" if counter.fetch_add(1, Ordering::SeqCst) == 0 => {
                Some(thread_result("active", &[("t1", "inProgress")]))
            }
            "thread/read" => Some(thread_result("idle", &[("t1", "completed")])),
            "thread/resume" => Some(thread_result("idle", &[("t1", "interrupted")])),
            _ => None,
        })
        .await;
        let (handle, mut reports, _shutdown) = spawn_watcher(&fake);
        handle.track("th", "t1");
        let report = next_report(&mut reports).await;
        assert_eq!(report.terminal.kind, TerminalKind::Completed);
        assert!(reads.load(Ordering::SeqCst) >= 2);
    }
}

#[test]
fn mfcli_turns_are_push_eligible() {
    assert!(super::BRIDGE_PUSH_AGENTS.contains(&"mfcli"));
}
