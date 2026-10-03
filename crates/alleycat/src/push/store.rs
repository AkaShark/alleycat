//! Persistent push state: subscriptions, the signed-request outbox and the
//! recent-terminals memory. Everything here is plain data plus pure
//! scheduling logic so it can be tested without IO; [`PushFiles`] loads and
//! saves the three JSON files (0600, atomic replace).

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::fsutil::atomic_write;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
    Ios,
    Android,
    Harmony,
}

impl Platform {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "ios" => Some(Self::Ios),
            "android" => Some(Self::Android),
            "harmony" => Some(Self::Harmony),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ios => "ios",
            Self::Android => "android",
            Self::Harmony => "harmony",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ApnsEnvironment {
    Sandbox,
    Production,
}

impl ApnsEnvironment {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "sandbox" => Some(Self::Sandbox),
            "production" => Some(Self::Production),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sandbox => "sandbox",
            Self::Production => "production",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TerminalKind {
    Completed,
    Failed,
}

/// Only used to pick notification copy; `interrupted` turns are `failed`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureReason {
    Interrupted,
    Error,
}

/// An authoritative turn end state.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Terminal {
    pub kind: TerminalKind,
    #[serde(default)]
    pub reason: Option<FailureReason>,
    pub occurred_at: u64,
}

impl Terminal {
    pub fn completed(occurred_at: u64) -> Self {
        Self {
            kind: TerminalKind::Completed,
            reason: None,
            occurred_at,
        }
    }

    pub fn failed(reason: FailureReason, occurred_at: u64) -> Self {
        Self {
            kind: TerminalKind::Failed,
            reason: Some(reason),
            occurred_at,
        }
    }
}

/// `(agent, thread_id, turn_id)`.
pub type TurnKey = (String, String, String);

/// A terminal state remembered for the subscribe-after-finish race and so
/// every report of the same turn reuses one `eventId`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordedTerminal {
    pub agent: String,
    pub thread_id: String,
    pub turn_id: String,
    pub terminal: Terminal,
    pub event_id: String,
    pub recorded_at: u64,
}

impl RecordedTerminal {
    pub fn key(&self) -> TurnKey {
        (
            self.agent.clone(),
            self.thread_id.clone(),
            self.turn_id.clone(),
        )
    }
}

/// Terminal attached to a subscription, with the event id it is reported as.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubscriptionTerminal {
    pub terminal: Terminal,
    pub event_id: String,
}

/// One device's subscription to one turn, as persisted on the host.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Subscription {
    /// Host-local id (`hs_<32 hex>`); the Worker id lives in `remote_id`.
    pub id: String,
    pub device_id: String,
    pub agent: String,
    pub thread_id: String,
    pub turn_id: String,
    pub platform: Platform,
    /// Push token sealed to the Worker (spec §5.5); opaque, never logged.
    pub sealed_target: String,
    #[serde(default)]
    pub apns_environment: Option<ApnsEnvironment>,
    pub issued_at: u64,
    pub expires_at: u64,
    pub grant_nonce: String,
    pub grant_signature: String,
    pub created_at: u64,
    /// Worker `subscriptionId` once `POST /v2/subscriptions` succeeded.
    #[serde(default)]
    pub remote_id: Option<String>,
    /// A register request was sent at least once, so the Worker may hold
    /// this subscription even though no id came back (e.g. a timeout).
    #[serde(default)]
    pub register_attempted: bool,
    /// Set by `push_unsubscribe` while the Worker-side delete is pending.
    #[serde(default)]
    pub revoking: bool,
    #[serde(default)]
    pub terminal: Option<SubscriptionTerminal>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("id", &self.id)
            .field("device_id", &self.device_id)
            .field("agent", &self.agent)
            .field("thread_id", &self.thread_id)
            .field("turn_id", &self.turn_id)
            .field("platform", &self.platform)
            .field(
                "sealed_target",
                &format_args!("<{} bytes>", self.sealed_target.len()),
            )
            .field("apns_environment", &self.apns_environment)
            .field("expires_at", &self.expires_at)
            .field("remote_id", &self.remote_id)
            .field("register_attempted", &self.register_attempted)
            .field("revoking", &self.revoking)
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

impl Subscription {
    pub fn turn_key(&self) -> TurnKey {
        (
            self.agent.clone(),
            self.thread_id.clone(),
            self.turn_id.clone(),
        )
    }

    /// Outbox ordering key: everything for one `(device, agent, thread,
    /// turn)` is delivered strictly in order.
    pub fn chain(&self) -> String {
        chain_key(&self.device_id, &self.agent, &self.thread_id, &self.turn_id)
    }

    pub fn matches_turn(&self, agent: &str, thread_id: &str, turn_id: &str) -> bool {
        self.agent == agent && self.thread_id == thread_id && self.turn_id == turn_id
    }
}

pub fn chain_key(device_id: &str, agent: &str, thread_id: &str, turn_id: &str) -> String {
    format!("{device_id}\u{1f}{agent}\u{1f}{thread_id}\u{1f}{turn_id}")
}

/// `POST /v2/events` body. Field order matches the spec example; the exact
/// serialized bytes are what gets hashed into the request signature.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EventBody {
    pub event_id: String,
    pub agent: String,
    pub thread_id: String,
    pub turn_id: String,
    #[serde(rename = "type")]
    pub kind: TerminalKind,
    pub reason: Option<FailureReason>,
    pub occurred_at: u64,
}

impl EventBody {
    pub fn new(event_id: &str, key: &TurnKey, terminal: &Terminal) -> Self {
        Self {
            event_id: event_id.to_string(),
            agent: key.0.clone(),
            thread_id: key.1.clone(),
            turn_id: key.2.clone(),
            kind: terminal.kind,
            reason: terminal.reason,
            occurred_at: terminal.occurred_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutboxAction {
    /// `POST /v2/subscriptions` for a local subscription.
    Register { subscription: String },
    /// `POST /v2/events` on behalf of a local subscription.
    Event {
        subscription: String,
        event: EventBody,
    },
    /// `DELETE /v2/subscriptions/{id}`. `remote_id` is set when the local
    /// record is already gone; otherwise it is read from `subscription`.
    Revoke {
        #[serde(default)]
        subscription: Option<String>,
        #[serde(default)]
        remote_id: Option<String>,
    },
}

impl OutboxAction {
    pub fn subscription(&self) -> Option<&str> {
        match self {
            Self::Register { subscription } | Self::Event { subscription, .. } => {
                Some(subscription)
            }
            Self::Revoke { subscription, .. } => subscription.as_deref(),
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Register { .. } => "register",
            Self::Event { .. } => "event",
            Self::Revoke { .. } => "revoke",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OutboxItem {
    pub id: String,
    pub chain: String,
    pub action: OutboxAction,
    pub created_at: u64,
    #[serde(default)]
    pub attempts: u32,
    /// Unix milliseconds; 0 means "now".
    #[serde(default)]
    pub next_attempt_at_ms: u64,
    #[serde(default)]
    pub last_error: Option<String>,
}

impl OutboxItem {
    pub fn new(chain: String, action: OutboxAction, now: u64) -> Self {
        Self {
            id: format!("ob_{}", super::signing::random_hex32()),
            chain,
            action,
            created_at: now,
            attempts: 0,
            next_attempt_at_ms: 0,
            last_error: None,
        }
    }
}

/// Retry/retention knobs. Tests shrink the backoff.
#[derive(Debug, Clone)]
pub struct OutboxPolicy {
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_retry_after: Duration,
    pub max_age: Duration,
    pub max_items: usize,
}

impl Default for OutboxPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_secs(5),
            max_backoff: Duration::from_secs(600),
            max_retry_after: Duration::from_secs(3600),
            max_age: Duration::from_secs(24 * 60 * 60),
            max_items: 1000,
        }
    }
}

impl OutboxPolicy {
    /// Exponential backoff for the `attempts`-th failure (1-based), scaled by
    /// a jitter factor in `[0.8, 1.2)` derived from `jitter` in `[0, 1)`, and
    /// capped at `max_backoff`.
    pub fn backoff(&self, attempts: u32, jitter: f64) -> Duration {
        let exp = attempts.saturating_sub(1).min(20);
        let base = self
            .initial_backoff
            .saturating_mul(1u32 << exp)
            .min(self.max_backoff);
        let factor = 0.8 + 0.4 * jitter.clamp(0.0, 1.0);
        base.mul_f64(factor).min(self.max_backoff)
    }

    /// Delay before the next attempt: the backoff, but never earlier than a
    /// server-supplied `Retry-After` (itself capped at `max_retry_after`).
    pub fn retry_delay(
        &self,
        attempts: u32,
        retry_after: Option<Duration>,
        jitter: f64,
    ) -> Duration {
        let backoff = self.backoff(attempts, jitter);
        match retry_after {
            Some(retry_after) => backoff.max(retry_after.min(self.max_retry_after)),
            None => backoff,
        }
    }
}

/// Ordered queue of Worker requests with per-chain FIFO semantics.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Outbox {
    pub items: Vec<OutboxItem>,
    /// Host-wide pause after a rate limit (unix ms). Not persisted.
    #[serde(skip)]
    pub paused_until_ms: u64,
}

impl Outbox {
    /// Append an item. Returns items evicted (oldest first) to stay within
    /// `max_items`.
    pub fn push(&mut self, item: OutboxItem, max_items: usize) -> Vec<OutboxItem> {
        self.items.push(item);
        let mut evicted = Vec::new();
        while self.items.len() > max_items.max(1) {
            evicted.push(self.items.remove(0));
        }
        evicted
    }

    /// Insert `item` before every queued item of its chain.
    pub fn insert_front_of_chain(&mut self, item: OutboxItem) {
        match self.items.iter().position(|i| i.chain == item.chain) {
            Some(index) => self.items.insert(index, item),
            None => self.items.push(item),
        }
    }

    /// Insert `item` just before the first queued item that belongs to the
    /// same local subscription (or append). Used to put a missing `register`
    /// back in front of that subscription's `event` without jumping ahead of
    /// unrelated items on the same chain.
    pub fn insert_before_subscription(&mut self, item: OutboxItem) {
        let subscription = item.action.subscription().map(str::to_string);
        let index = self.items.iter().position(|i| {
            subscription.is_some() && i.action.subscription() == subscription.as_deref()
        });
        match index {
            Some(index) => self.items.insert(index, item),
            None => self.items.push(item),
        }
    }

    fn is_chain_head(&self, index: usize) -> bool {
        let chain = &self.items[index].chain;
        !self.items[..index].iter().any(|i| &i.chain == chain)
    }

    /// First chain-head item whose retry time has come, honoring the
    /// host-wide pause.
    pub fn next_due(&self, now_ms: u64) -> Option<&OutboxItem> {
        if now_ms < self.paused_until_ms {
            return None;
        }
        (0..self.items.len())
            .filter(|&i| self.is_chain_head(i))
            .map(|i| &self.items[i])
            .find(|item| item.next_attempt_at_ms <= now_ms)
    }

    /// Earliest time (unix ms) at which some chain head becomes due.
    pub fn next_wake_ms(&self) -> Option<u64> {
        (0..self.items.len())
            .filter(|&i| self.is_chain_head(i))
            .map(|i| self.items[i].next_attempt_at_ms.max(self.paused_until_ms))
            .min()
    }

    pub fn get(&self, id: &str) -> Option<&OutboxItem> {
        self.items.iter().find(|item| item.id == id)
    }

    pub fn remove(&mut self, id: &str) -> Option<OutboxItem> {
        let index = self.items.iter().position(|item| item.id == id)?;
        Some(self.items.remove(index))
    }

    /// Remove every item that belongs to local subscription `subscription`,
    /// except the one with id `keep` (typically the in-flight request).
    pub fn remove_for_subscription(&mut self, subscription: &str, keep: Option<&str>) -> usize {
        let before = self.items.len();
        self.items.retain(|item| {
            Some(item.id.as_str()) == keep || item.action.subscription() != Some(subscription)
        });
        before - self.items.len()
    }

    pub fn has_action_for(&self, subscription: &str, label: &str) -> bool {
        self.items.iter().any(|item| {
            item.action.subscription() == Some(subscription) && item.action.label() == label
        })
    }

    /// Record a failed attempt and schedule the next one.
    pub fn schedule_retry(
        &mut self,
        id: &str,
        now_ms: u64,
        delay: Duration,
        error: String,
    ) -> Option<u32> {
        let item = self.items.iter_mut().find(|item| item.id == id)?;
        item.attempts = item.attempts.saturating_add(1);
        item.next_attempt_at_ms = now_ms.saturating_add(delay.as_millis() as u64);
        item.last_error = Some(error);
        Some(item.attempts)
    }

    /// Drop items older than `max_age`, returning them.
    pub fn prune_expired(&mut self, now: u64, max_age: Duration) -> Vec<OutboxItem> {
        let max_age = max_age.as_secs();
        let (expired, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut self.items)
            .into_iter()
            .partition(|item| now.saturating_sub(item.created_at) > max_age);
        self.items = keep;
        expired
    }
}

/// How long a terminal state is remembered.
pub const RECENT_TERMINAL_TTL_SECS: u64 = 15 * 60;
const MAX_RECENT_TERMINALS: usize = 2000;

#[derive(Debug, Default, Clone)]
pub struct RecentTerminals {
    map: HashMap<TurnKey, RecordedTerminal>,
}

impl RecentTerminals {
    pub fn from_vec(items: Vec<RecordedTerminal>, now: u64) -> Self {
        let mut recent = Self::default();
        for item in items {
            recent.map.insert(item.key(), item);
        }
        recent.prune(now);
        recent
    }

    pub fn to_vec(&self) -> Vec<RecordedTerminal> {
        let mut items: Vec<_> = self.map.values().cloned().collect();
        items.sort_by(|a, b| {
            a.recorded_at
                .cmp(&b.recorded_at)
                .then(a.event_id.cmp(&b.event_id))
        });
        items
    }

    pub fn get(&self, key: &TurnKey) -> Option<&RecordedTerminal> {
        self.map.get(key)
    }

    pub fn insert(&mut self, record: RecordedTerminal) {
        self.map.insert(record.key(), record);
        while self.map.len() > MAX_RECENT_TERMINALS {
            let oldest = self
                .map
                .iter()
                .min_by_key(|(_, r)| r.recorded_at)
                .map(|(k, _)| k.clone());
            match oldest {
                Some(key) => {
                    self.map.remove(&key);
                }
                None => break,
            }
        }
    }

    /// Forget entries older than [`RECENT_TERMINAL_TTL_SECS`]. Returns
    /// whether anything was removed.
    pub fn prune(&mut self, now: u64) -> bool {
        let before = self.map.len();
        self.map
            .retain(|_, r| now.saturating_sub(r.recorded_at) <= RECENT_TERMINAL_TTL_SECS);
        before != self.map.len()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

/// All persisted push state.
#[derive(Debug, Default, Clone)]
pub struct PushData {
    pub subscriptions: BTreeMap<String, Subscription>,
    pub outbox: Outbox,
    pub recent: RecentTerminals,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct SubscriptionsFile {
    #[serde(default)]
    subscriptions: Vec<Subscription>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct OutboxFile {
    #[serde(default)]
    items: Vec<OutboxItem>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct RecentFile {
    #[serde(default)]
    terminals: Vec<RecordedTerminal>,
}

/// Locations of the three push state files.
#[derive(Debug, Clone)]
pub struct PushFiles {
    pub subscriptions: PathBuf,
    pub outbox: PathBuf,
    pub recent: PathBuf,
}

impl PushFiles {
    pub fn from_state_dir() -> anyhow::Result<Self> {
        Ok(Self {
            subscriptions: crate::paths::push_subscriptions_file()?,
            outbox: crate::paths::push_outbox_file()?,
            recent: crate::paths::push_recent_terminals_file()?,
        })
    }

    #[cfg(test)]
    pub fn in_dir(dir: &Path) -> Self {
        Self {
            subscriptions: dir.join("push-subscriptions.json"),
            outbox: dir.join("push-outbox.json"),
            recent: dir.join("push-recent-terminals.json"),
        }
    }

    /// Load all files. A missing file is empty; a corrupt file is logged,
    /// set aside as `<name>.corrupt` and treated as empty so the daemon keeps
    /// running.
    pub async fn load(&self, now: u64) -> PushData {
        let subs: SubscriptionsFile = load_json(&self.subscriptions).await;
        let outbox: OutboxFile = load_json(&self.outbox).await;
        let recent: RecentFile = load_json(&self.recent).await;
        PushData {
            subscriptions: subs
                .subscriptions
                .into_iter()
                .map(|s| (s.id.clone(), s))
                .collect(),
            outbox: Outbox {
                items: outbox.items,
                paused_until_ms: 0,
            },
            recent: RecentTerminals::from_vec(recent.terminals, now),
        }
    }

    pub async fn save_subscriptions(
        &self,
        subscriptions: &BTreeMap<String, Subscription>,
    ) -> anyhow::Result<()> {
        let file = SubscriptionsFile {
            subscriptions: subscriptions.values().cloned().collect(),
        };
        save_json(&self.subscriptions, &file).await
    }

    pub async fn save_outbox(&self, outbox: &Outbox) -> anyhow::Result<()> {
        let file = OutboxFile {
            items: outbox.items.clone(),
        };
        save_json(&self.outbox, &file).await
    }

    pub async fn save_recent(&self, recent: &RecentTerminals) -> anyhow::Result<()> {
        let file = RecentFile {
            terminals: recent.to_vec(),
        };
        save_json(&self.recent, &file).await
    }
}

async fn load_json<T: Default + serde::de::DeserializeOwned>(path: &Path) -> T {
    match tokio::fs::read(path).await {
        Ok(bytes) => match serde_json::from_slice(&bytes) {
            Ok(value) => value,
            Err(error) => {
                let mut aside = path.as_os_str().to_os_string();
                aside.push(".corrupt");
                warn!(path = %path.display(), "push state file is corrupt; starting empty: {error}");
                let _ = tokio::fs::rename(path, PathBuf::from(aside)).await;
                T::default()
            }
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => T::default(),
        Err(error) => {
            warn!(path = %path.display(), "reading push state file failed; starting empty: {error}");
            T::default()
        }
    }
}

async fn save_json<T: Serialize>(path: &Path, value: &T) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).context("serializing push state")?;
    atomic_write(path, &bytes).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(chain: &str, action: OutboxAction) -> OutboxItem {
        OutboxItem::new(chain.to_string(), action, 1000)
    }

    fn register(sub: &str) -> OutboxAction {
        OutboxAction::Register {
            subscription: sub.to_string(),
        }
    }

    fn event(sub: &str) -> OutboxAction {
        OutboxAction::Event {
            subscription: sub.to_string(),
            event: EventBody {
                event_id: "evt_1".into(),
                agent: "codex".into(),
                thread_id: "t".into(),
                turn_id: "u".into(),
                kind: TerminalKind::Completed,
                reason: None,
                occurred_at: 1,
            },
        }
    }

    #[test]
    fn event_body_matches_spec_vector_bytes() {
        let body = EventBody {
            event_id: "evt_0123456789abcdef0123456789abcdef".into(),
            agent: "codex".into(),
            thread_id: "thread-1".into(),
            turn_id: "turn-1".into(),
            kind: TerminalKind::Completed,
            reason: None,
            occurred_at: 1790300000,
        };
        assert_eq!(
            serde_json::to_string(&body).unwrap(),
            crate::push::signing::vectors::EVENT_BODY
        );
        let failed = EventBody {
            kind: TerminalKind::Failed,
            reason: Some(FailureReason::Interrupted),
            ..body
        };
        let value = serde_json::to_value(&failed).unwrap();
        assert_eq!(value["type"], "failed");
        assert_eq!(value["reason"], "interrupted");
    }

    #[test]
    fn backoff_grows_exponentially_with_jitter_and_cap() {
        let policy = OutboxPolicy::default();
        assert_eq!(policy.backoff(1, 0.5), Duration::from_secs(5));
        assert_eq!(policy.backoff(2, 0.5), Duration::from_secs(10));
        assert_eq!(policy.backoff(3, 0.5), Duration::from_secs(20));
        assert_eq!(policy.backoff(1, 0.0), Duration::from_secs(4));
        assert!(policy.backoff(1, 0.999) < Duration::from_secs(6));
        assert_eq!(policy.backoff(12, 0.5), Duration::from_secs(600));
        assert_eq!(policy.backoff(200, 0.999), Duration::from_secs(600));
    }

    #[test]
    fn retry_after_is_a_floor_not_a_ceiling() {
        let policy = OutboxPolicy::default();
        assert_eq!(
            policy.retry_delay(1, Some(Duration::from_secs(120)), 0.5),
            Duration::from_secs(120)
        );
        assert_eq!(
            policy.retry_delay(3, Some(Duration::from_secs(1)), 0.5),
            Duration::from_secs(20)
        );
        assert_eq!(
            policy.retry_delay(1, Some(Duration::from_secs(99_999)), 0.5),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn next_due_respects_chain_order_and_retry_time() {
        let mut outbox = Outbox::default();
        let a_reg = item("A", register("a"));
        let a_evt = item("A", event("a"));
        let b_reg = item("B", register("b"));
        let (a_reg_id, a_evt_id, b_reg_id) = (a_reg.id.clone(), a_evt.id.clone(), b_reg.id.clone());
        outbox.push(a_reg, 10);
        outbox.push(a_evt, 10);
        outbox.push(b_reg, 10);

        assert_eq!(outbox.next_due(5_000).unwrap().id, a_reg_id);
        // A's register backs off: B's register is next, A's event must wait.
        outbox.schedule_retry(&a_reg_id, 5_000, Duration::from_secs(10), "boom".into());
        assert_eq!(outbox.next_due(5_000).unwrap().id, b_reg_id);
        outbox.remove(&b_reg_id);
        assert!(outbox.next_due(5_000).is_none());
        assert_eq!(outbox.next_wake_ms(), Some(15_000));
        assert_eq!(outbox.next_due(15_000).unwrap().id, a_reg_id);
        outbox.remove(&a_reg_id);
        assert_eq!(outbox.next_due(15_000).unwrap().id, a_evt_id);

        let item = outbox.get(&a_evt_id).unwrap();
        assert_eq!(item.attempts, 0);
    }

    #[test]
    fn host_wide_pause_blocks_everything() {
        let mut outbox = Outbox::default();
        outbox.push(item("A", register("a")), 10);
        outbox.paused_until_ms = 9_000;
        assert!(outbox.next_due(8_999).is_none());
        assert_eq!(outbox.next_wake_ms(), Some(9_000));
        assert!(outbox.next_due(9_000).is_some());
    }

    #[test]
    fn push_evicts_oldest_beyond_cap() {
        let mut outbox = Outbox::default();
        let first = item("A", register("a"));
        let first_id = first.id.clone();
        assert!(outbox.push(first, 2).is_empty());
        assert!(outbox.push(item("B", register("b")), 2).is_empty());
        let evicted = outbox.push(item("C", register("c")), 2);
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].id, first_id);
        assert_eq!(outbox.items.len(), 2);
    }

    #[test]
    fn prune_drops_items_older_than_max_age() {
        let mut outbox = Outbox::default();
        let mut old = item("A", register("a"));
        old.created_at = 0;
        let fresh = item("B", register("b"));
        outbox.push(old, 10);
        outbox.push(fresh, 10);
        let expired = outbox.prune_expired(1000 + 10, Duration::from_secs(100));
        assert_eq!(expired.len(), 1);
        assert_eq!(outbox.items.len(), 1);
    }

    #[test]
    fn remove_for_subscription_keeps_in_flight_item() {
        let mut outbox = Outbox::default();
        let reg = item("A", register("a"));
        let reg_id = reg.id.clone();
        outbox.push(reg, 10);
        outbox.push(item("A", event("a")), 10);
        outbox.push(item("B", register("b")), 10);
        assert_eq!(outbox.remove_for_subscription("a", Some(&reg_id)), 1);
        assert!(outbox.get(&reg_id).is_some());
        assert_eq!(outbox.items.len(), 2);
    }

    #[test]
    fn insert_before_subscription_keeps_other_chain_items_first() {
        let mut outbox = Outbox::default();
        outbox.push(item("B", register("b")), 10);
        // An older subscription's revoke on the same chain must stay first.
        outbox.push(
            item(
                "A",
                OutboxAction::Revoke {
                    subscription: Some("old".into()),
                    remote_id: None,
                },
            ),
            10,
        );
        outbox.push(item("A", event("a")), 10);
        let reg = item("A", register("a"));
        let reg_id = reg.id.clone();
        outbox.insert_before_subscription(reg);
        assert_eq!(outbox.items[2].id, reg_id);
        assert_eq!(outbox.items[3].action.label(), "event");
        // No item for the subscription yet: append.
        let lone = item("C", register("c"));
        let lone_id = lone.id.clone();
        outbox.insert_before_subscription(lone);
        assert_eq!(outbox.items.last().unwrap().id, lone_id);
    }

    #[test]
    fn recent_terminals_expire_after_ttl() {
        let record = |turn: &str, at: u64| RecordedTerminal {
            agent: "claude".into(),
            thread_id: "t".into(),
            turn_id: turn.into(),
            terminal: Terminal::completed(at),
            event_id: format!("evt_{turn}"),
            recorded_at: at,
        };
        let mut recent =
            RecentTerminals::from_vec(vec![record("old", 0), record("new", 1000)], 1000);
        assert_eq!(recent.len(), 1);
        assert!(
            recent
                .get(&("claude".into(), "t".into(), "new".into()))
                .is_some()
        );
        assert!(recent.prune(1000 + RECENT_TERMINAL_TTL_SECS + 1));
        assert_eq!(recent.len(), 0);
    }

    #[tokio::test]
    async fn files_round_trip_and_survive_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let files = PushFiles::in_dir(dir.path());
        let empty = files.load(0).await;
        assert!(empty.subscriptions.is_empty() && empty.outbox.items.is_empty());

        let sub = Subscription {
            id: "hs_1".into(),
            device_id: "d".into(),
            agent: "codex".into(),
            thread_id: "t".into(),
            turn_id: "u".into(),
            platform: Platform::Android,
            sealed_target: "AQG-sealed".into(),
            apns_environment: None,
            issued_at: 1,
            expires_at: 2,
            grant_nonce: "n".into(),
            grant_signature: "s".into(),
            created_at: 1,
            remote_id: Some("sub_x".into()),
            register_attempted: true,
            revoking: false,
            terminal: Some(SubscriptionTerminal {
                terminal: Terminal::failed(FailureReason::Error, 5),
                event_id: "evt_x".into(),
            }),
        };
        let mut data = PushData::default();
        data.subscriptions.insert(sub.id.clone(), sub.clone());
        data.outbox.push(item(&sub.chain(), event("hs_1")), 10);
        data.recent.insert(RecordedTerminal {
            agent: "codex".into(),
            thread_id: "t".into(),
            turn_id: "u".into(),
            terminal: Terminal::completed(10),
            event_id: "evt_y".into(),
            recorded_at: 10,
        });
        files.save_subscriptions(&data.subscriptions).await.unwrap();
        files.save_outbox(&data.outbox).await.unwrap();
        files.save_recent(&data.recent).await.unwrap();

        let loaded = files.load(20).await;
        assert_eq!(loaded.subscriptions.get("hs_1"), Some(&sub));
        assert_eq!(loaded.outbox.items, data.outbox.items);
        assert_eq!(loaded.recent.to_vec(), data.recent.to_vec());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            for path in [&files.subscriptions, &files.outbox, &files.recent] {
                let mode = std::fs::metadata(path).unwrap().permissions().mode() & 0o777;
                assert_eq!(mode, 0o600, "{}", path.display());
            }
        }

        std::fs::write(&files.outbox, b"{not json").unwrap();
        let loaded = files.load(20).await;
        assert!(loaded.outbox.items.is_empty());
        assert_eq!(loaded.subscriptions.len(), 1);
        assert!(dir.path().join("push-outbox.json.corrupt").exists());
    }
}
