use anyhow::Context;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::fsutil::atomic_write;
use crate::paths;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct HostConfig {
    pub token: String,
    pub relay: Option<String>,
    pub agents: AgentsConfig,
    pub session: SessionConfig,
    pub push: PushConfig,
}

impl Default for HostConfig {
    fn default() -> Self {
        Self {
            token: random_token(),
            relay: None,
            agents: AgentsConfig::default(),
            session: SessionConfig::default(),
            push: PushConfig::default(),
        }
    }
}

/// `[push]` — host-reported turn completion notifications. Read on every
/// delivery attempt, so `reload` applies changes without a restart.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PushConfig {
    /// Master switch. When false the host advertises `push.enabled = false`,
    /// rejects new subscriptions and pauses (but keeps) its outbox.
    pub enabled: bool,
    /// Push Worker base URL. `None` falls back to the URL compiled into the
    /// distributing binary (`App::push_worker_url`); an empty string disables
    /// push even when the binary supplies a default.
    pub worker_url: Option<String>,
    /// Per-request HTTP timeout for Worker calls.
    pub request_timeout_secs: u64,
}

impl Default for PushConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            worker_url: None,
            request_timeout_secs: 10,
        }
    }
}

/// Knobs for the daemon-lifetime session registry. Defaults are tuned for a
/// phone client that may sit on a flaky relay path: 16 MiB of replay history
/// covers a long turn, the 10-minute idle TTL keeps a session warm across
/// short backgrounding, and the 60-second pending grace lets a reconnecting
/// client re-render approval prompts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct SessionConfig {
    pub replay_max_msgs: usize,
    pub replay_max_bytes: usize,
    pub idle_ttl_secs: u64,
    pub pending_grace_secs: u64,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            replay_max_msgs: 2048,
            replay_max_bytes: 16 << 20,
            idle_ttl_secs: 600,
            pending_grace_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AgentsConfig {
    pub codex: CodexAgentConfig,
    pub pi: PiAgentConfig,
    pub amp: AmpAgentConfig,
    pub opencode: OpencodeAgentConfig,
    pub claude: ClaudeAgentConfig,
    pub droid: DroidAgentConfig,
    pub hermes: HermesAgentConfig,
    pub devin: DevinAgentConfig,
    pub grok: GrokAgentConfig,
    pub shell: ShellAgentConfig,
}

impl AgentsConfig {
    /// `enabled` flag for a wire agent name; unknown names are disabled.
    pub fn enabled_by_name(&self, name: &str) -> bool {
        match name {
            "codex" => self.codex.enabled,
            "pi" => self.pi.enabled,
            "amp" => self.amp.enabled,
            "opencode" => self.opencode.enabled,
            "claude" => self.claude.enabled,
            "droid" => self.droid.enabled,
            "hermes" => self.hermes.enabled,
            "devin" => self.devin.enabled,
            "grok" => self.grok.enabled,
            "shell" => self.shell.enabled,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CodexAgentConfig {
    pub enabled: bool,
    pub bin: String,
    pub host: String,
    pub port: u16,
}

impl Default for CodexAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "codex".to_string(),
            host: "127.0.0.1".to_string(),
            port: 8390,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PiAgentConfig {
    pub enabled: bool,
    pub bin: String,
}

impl Default for PiAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "pi".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AmpAgentConfig {
    pub enabled: bool,
    pub bin: String,
    /// Env var that provides non-interactive Amp auth for daemon launches.
    /// Amp can also use its own local login state, but this keeps headless
    /// installs explicit.
    pub api_key_env: String,
    /// Keep the bridge default aligned for automation, while allowing
    /// policy-focused installs to opt out and rely on Amp settings/plugins.
    pub dangerously_allow_all: bool,
}

impl Default for AmpAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "amp".to_string(),
            api_key_env: "AMP_API_KEY".to_string(),
            dangerously_allow_all: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct OpencodeAgentConfig {
    pub enabled: bool,
    pub bin: String,
}

impl Default for OpencodeAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "opencode".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ClaudeAgentConfig {
    pub enabled: bool,
    pub bin: String,
    /// When true (default), spawn `claude` with `--dangerously-skip-permissions`
    /// (matches the user's local `claude` shell alias). When false, spawn with
    /// `--permission-prompt-tool stdio` and bridge each `can_use_tool`
    /// inbound control_request to a codex `requestApproval` server→client
    /// request — the connected phone client gets to approve/deny each tool
    /// call.
    pub bypass_permissions: bool,
}

impl Default for ClaudeAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "claude".to_string(),
            bypass_permissions: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct DroidAgentConfig {
    pub enabled: bool,
    pub bin: String,
    /// Env var that can provide Factory auth to `droid exec`. The CLI may
    /// also use its encrypted auth store, but this keeps daemon config
    /// explicit for headless installs.
    pub api_key_env: String,
}

impl Default for DroidAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "droid".to_string(),
            api_key_env: "FACTORY_API_KEY".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct HermesAgentConfig {
    pub enabled: bool,
    /// Hermes CLI binary used for fallback mode.
    pub bin: String,
    /// Loopback Hermes gateway API base URL.
    pub api_base: String,
}

impl Default for HermesAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "hermes".to_string(),
            api_base: "http://127.0.0.1:8642".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct DevinAgentConfig {
    pub enabled: bool,
    pub bin: String,
}

impl Default for DevinAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "devin".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GrokAgentConfig {
    pub enabled: bool,
    pub bin: String,

    /// Start a fresh agent instead of joining a leader process.
    /// Recommended default for Alleycat usage.
    pub no_leader: bool,

    /// Specific model ID to request (e.g. "grok-build").
    pub model: Option<String>,

    /// Automatically approve all tool executions.
    pub always_approve: bool,

    /// Reasoning effort level for reasoning-capable models.
    /// Common values: "low", "medium", "high", "xhigh", "max".
    pub reasoning_effort: Option<String>,
}

impl Default for GrokAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            bin: "grok".to_string(),
            no_leader: true,
            model: None,
            always_approve: false,
            reasoning_effort: Some("medium".to_string()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ShellAgentConfig {
    pub enabled: bool,
    pub shell_bin: String,
    pub default_cwd: Option<String>,
    pub allow_env_passthrough: bool,
}

impl Default for ShellAgentConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            shell_bin: std::env::var("SHELL")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| {
                    if cfg!(windows) {
                        "powershell.exe".to_string()
                    } else {
                        "/bin/zsh".to_string()
                    }
                }),
            default_cwd: None,
            allow_env_passthrough: false,
        }
    }
}

pub async fn load_or_init() -> anyhow::Result<HostConfig> {
    let path = paths::host_config_file()?;
    match fs::read_to_string(&path).await {
        Ok(raw) => toml::from_str(&raw).with_context(|| format!("parsing {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let cfg = HostConfig::default();
            save(&cfg).await?;
            Ok(cfg)
        }
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

pub async fn save(config: &HostConfig) -> anyhow::Result<()> {
    let path = paths::host_config_file()?;
    let data = toml::to_string_pretty(config).context("serializing host config")?;
    atomic_write(&path, data.as_bytes()).await?;
    Ok(())
}

pub async fn rotate_token() -> anyhow::Result<HostConfig> {
    let mut config = load_or_init().await?;
    config.token = random_token();
    save(&config).await?;
    Ok(config)
}

pub fn random_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut bytes);
    hex::encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_section_defaults_and_round_trips() {
        let config = HostConfig::default();
        assert!(config.push.enabled);
        assert_eq!(config.push.worker_url, None);
        assert_eq!(config.push.request_timeout_secs, 10);

        // Old host.toml without [push] still parses with defaults.
        let old: HostConfig = toml::from_str("token = \"abc\"\n").unwrap();
        assert_eq!(old.push, PushConfig::default());

        // rotate/save rewrite the file from the typed struct, so [push] must
        // survive a serialize → parse cycle.
        let mut custom = HostConfig::default();
        custom.push.enabled = false;
        custom.push.worker_url = Some("https://push.example".into());
        custom.push.request_timeout_secs = 3;
        let text = toml::to_string_pretty(&custom).unwrap();
        assert!(text.contains("[push]"));
        let back: HostConfig = toml::from_str(&text).unwrap();
        assert_eq!(back.push, custom.push);
    }

    #[test]
    fn random_token_is_32_bytes_hex() {
        let token = random_token();
        assert_eq!(token.len(), 64);
        let decoded = hex::decode(token).unwrap();
        assert_eq!(decoded.len(), 32);
    }

    #[test]
    fn default_config_routes_codex_to_local_app_server() {
        let config = HostConfig::default();
        assert!(config.agents.codex.enabled);
        assert_eq!(config.agents.codex.bin, "codex");
        assert_eq!(config.agents.codex.host, "127.0.0.1");
        assert_eq!(config.agents.codex.port, 8390);
        assert!(config.agents.pi.enabled);
        assert!(config.agents.opencode.enabled);
        assert!(config.agents.claude.enabled);
        assert!(config.agents.droid.enabled);
        assert!(config.agents.grok.enabled);
        assert!(config.agents.grok.no_leader);
        assert_eq!(
            config.agents.grok.reasoning_effort.as_deref(),
            Some("medium")
        );
        assert!(config.agents.shell.enabled);
    }
}
