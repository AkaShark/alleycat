//! Pool of ACP agent processes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use alleycat_bridge_core::ProcessLauncher;
use anyhow::Result;
use dashmap::DashMap;
use tokio::sync::RwLock;
use tracing::{debug, info, instrument, warn};

use crate::acp_client::AcpClient;
use crate::config::AcpBridgeConfig;

/// Default maximum number of agent processes in the pool.
pub const DEFAULT_MAX_PROCESSES: usize = 4;

/// Default idle TTL for agent processes.
pub const DEFAULT_IDLE_TTL: Duration = Duration::from_secs(300);

/// Pool policy configuration.
#[derive(Debug, Clone)]
pub struct PoolPolicy {
    pub max_processes: usize,
    pub idle_ttl: Duration,
}

impl Default for PoolPolicy {
    fn default() -> Self {
        Self {
            max_processes: DEFAULT_MAX_PROCESSES,
            idle_ttl: DEFAULT_IDLE_TTL,
        }
    }
}

/// Pool entry with last access time.
struct PoolEntry {
    client: Arc<AcpClient>,
    last_access: Arc<RwLock<Instant>>,
}

/// Pool of ACP agent processes.
pub struct AcpPool {
    config: AcpBridgeConfig,
    launcher: Arc<dyn ProcessLauncher>,
    policy: PoolPolicy,
    clients: DashMap<String, PoolEntry>,
    /// ACP `initialize` request sent to every process the pool spawns.
    init_request: std::sync::RwLock<Option<serde_json::Value>>,
    /// One spawn at a time per key, so concurrent first requests for the
    /// same connection share one process instead of racing to insert.
    spawn_locks: DashMap<String, Arc<tokio::sync::Mutex<()>>>,
}

impl AcpPool {
    pub fn new(
        config: AcpBridgeConfig,
        launcher: Arc<dyn ProcessLauncher>,
        policy: PoolPolicy,
    ) -> Self {
        Self {
            config,
            launcher,
            policy,
            clients: DashMap::new(),
            init_request: std::sync::RwLock::new(None),
            spawn_locks: DashMap::new(),
        }
    }

    /// Remember the ACP `initialize` request so processes spawned later
    /// (after idle eviction or a crash) are initialized before first use.
    pub fn set_init_request(&self, request: serde_json::Value) {
        *self.init_request.write().expect("init_request poisoned") = Some(request);
    }

    /// Get or create an ACP client for the given session.
    #[instrument(skip(self), fields(session_id = %session_id))]
    pub async fn get_client(&self, session_id: &str) -> Result<Arc<AcpClient>> {
        self.get_client_in(session_id, None).await
    }

    /// Like [`get_client`](Self::get_client), but a newly spawned process
    /// starts in `cwd`.
    pub async fn get_client_in(
        &self,
        session_id: &str,
        cwd: Option<&std::path::Path>,
    ) -> Result<Arc<AcpClient>> {
        if let Some(client) = self.live_client(session_id).await {
            return Ok(client);
        }
        let spawn_lock = Arc::clone(
            self.spawn_locks
                .entry(session_id.to_string())
                .or_default()
                .value(),
        );
        let _spawning = spawn_lock.lock().await;
        // Another request may have spawned it while we waited.
        if let Some(client) = self.live_client(session_id).await {
            return Ok(client);
        }
        self.spawn_client(session_id, cwd).await
    }

    /// The pooled client for `session_id` if its process is alive; a client
    /// whose process exited is removed.
    async fn live_client(&self, session_id: &str) -> Option<Arc<AcpClient>> {
        let existing = self
            .clients
            .get(session_id)
            .map(|e| (Arc::clone(&e.client), Arc::clone(&e.last_access)));
        let (client, last_access) = existing?;
        if !client.is_closed() {
            *last_access.write().await = Instant::now();
            debug!("Reusing existing ACP client for session");
            return Some(client);
        }
        warn!("ACP agent process exited; respawning");
        self.remove_client(session_id).await;
        None
    }

    async fn spawn_client(
        &self,
        session_id: &str,
        cwd: Option<&std::path::Path>,
    ) -> Result<Arc<AcpClient>> {
        debug!("Creating new ACP client for session");

        // Check pool capacity and evict idle clients if needed
        self.evict_idle_clients().await;

        // Check capacity again after eviction
        if self.clients.len() >= self.policy.max_processes {
            warn!(
                "ACP pool is at capacity (max: {})",
                self.policy.max_processes
            );
            anyhow::bail!("ACP pool is at capacity");
        }

        // Create new client
        let client = Arc::new(AcpClient::spawn(&self.config, &self.launcher, cwd).await?);
        let init_request = self
            .init_request
            .read()
            .expect("init_request poisoned")
            .clone();
        if let Some(request) = init_request
            && let Err(err) = client.ensure_initialized(&request).await
        {
            let _ = client.kill().await;
            return Err(err);
        }
        let last_access = Arc::new(RwLock::new(Instant::now()));

        self.clients.insert(
            session_id.to_string(),
            PoolEntry {
                client: Arc::clone(&client),
                last_access,
            },
        );

        info!(
            "Created new ACP client for session (pool size: {})",
            self.clients.len()
        );
        Ok(client)
    }

    /// Remove a client from the pool, killing the underlying ACP process.
    /// The `kill()` future is `.await`-ed so the SIGKILL actually issues
    /// before we return; the previous `let _ = entry.client.kill();` was a
    /// bug that just dropped the future and left the child running.
    #[instrument(skip(self), fields(session_id = %session_id))]
    pub async fn remove_client(&self, session_id: &str) {
        if let Some((_, entry)) = self.clients.remove(session_id) {
            info!(
                "Removing ACP client from pool (pool size: {})",
                self.clients.len()
            );
            if let Err(err) = entry.client.kill().await {
                warn!(error = %err, "failed to kill ACP child while removing from pool");
            }
        }
    }

    /// Remove every client whose key starts with `prefix`.
    pub async fn remove_clients_with_prefix(&self, prefix: &str) {
        let keys: Vec<String> = self
            .clients
            .iter()
            .filter(|e| e.key().starts_with(prefix))
            .map(|e| e.key().clone())
            .collect();
        for key in keys {
            self.remove_client(&key).await;
        }
    }

    /// Evict idle clients that haven't been accessed within the TTL.
    #[instrument(skip(self))]
    async fn evict_idle_clients(&self) {
        let now = Instant::now();
        let snapshot: Vec<_> = self
            .clients
            .iter()
            .map(|e| {
                (
                    e.key().clone(),
                    Arc::clone(&e.client),
                    Arc::clone(&e.last_access),
                )
            })
            .collect();
        let mut to_remove = Vec::new();
        for (key, client, last_access) in snapshot {
            // A long `session/prompt` is not idleness: keep the process and
            // restart its idle clock.
            if client.is_busy() {
                *last_access.write().await = now;
                continue;
            }
            if now.duration_since(*last_access.read().await) > self.policy.idle_ttl {
                to_remove.push(key);
            }
        }

        if !to_remove.is_empty() {
            info!(
                "Evicting {} idle clients (TTL: {:?})",
                to_remove.len(),
                self.policy.idle_ttl
            );
            for session_id in to_remove {
                self.remove_client(&session_id).await;
            }
        }
    }

    /// Drain every pooled client, killing each child process. Called from
    /// `AcpBridge::shutdown` during daemon graceful shutdown so we don't
    /// leave orphaned `devin acp` / etc. processes behind. Synchronous
    /// in the sense that each kill is awaited before the next.
    pub async fn shutdown(&self) {
        let keys: Vec<String> = self.clients.iter().map(|e| e.key().clone()).collect();
        if keys.is_empty() {
            return;
        }
        info!(
            count = keys.len(),
            "pool shutdown: killing pooled ACP children"
        );
        for key in keys {
            self.remove_client(&key).await;
        }
    }

    /// Start background eviction task.
    #[instrument(skip(self))]
    pub fn start_eviction_task(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        info!("Starting background eviction task (interval: 60s)");
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                self.evict_idle_clients().await;
            }
        })
    }
}
