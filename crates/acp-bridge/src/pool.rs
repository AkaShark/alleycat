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
        // Reuse a live client; replace one whose process has exited.
        let existing = self
            .clients
            .get(session_id)
            .map(|e| (Arc::clone(&e.client), Arc::clone(&e.last_access)));
        if let Some((client, last_access)) = existing {
            if !client.is_closed() {
                *last_access.write().await = Instant::now();
                debug!("Reusing existing ACP client for session");
                return Ok(client);
            }
            warn!("ACP agent process exited; respawning");
            self.remove_client(session_id).await;
        }

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
        let client = Arc::new(AcpClient::spawn(&self.config, &self.launcher).await?);
        let init_request = self
            .init_request
            .read()
            .expect("init_request poisoned")
            .clone();
        if let Some(request) = init_request {
            if let Err(err) = client.ensure_initialized(&request).await {
                let _ = client.kill().await;
                return Err(err);
            }
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

    /// Evict idle clients that haven't been accessed within the TTL.
    #[instrument(skip(self))]
    async fn evict_idle_clients(&self) {
        let now = Instant::now();
        let mut to_remove = Vec::new();

        for entry in self.clients.iter() {
            let last_access = *entry.last_access.read().await;
            if now.duration_since(last_access) > self.policy.idle_ttl {
                to_remove.push(entry.key().clone());
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
