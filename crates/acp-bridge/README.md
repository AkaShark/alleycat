# alleycat-acp-bridge

Codex app-server façade over ACP (Agent Client Protocol) agents. The
alleycat daemon wraps it per agent: `devin-bridge` (Devin), `grok-bridge`
(Grok) and `mfcli-bridge` (MyFlicker). Agent-specific launch and listing
logic stays in those crates.

## Building one

```rust
let bridge = AcpBridge::builder()
    .agent_bin("mfcli")
    .agent_args(vec!["acp".into()])
    .client_capabilities(json!({"fs": {"readTextFile": false, "writeTextFile": false}, "terminal": false}))
    .discover_models(true)
    .pool_capacity(8)
    .launcher(launcher)
    .build()
    .await?;
```

`AcpBridge::builder().from_env()` reads `ACP_BRIDGE_AGENT_BIN`,
`ACP_BRIDGE_AGENT_ARGS`, `ACP_BRIDGE_STATE_DIR`, `ACP_BRIDGE_POOL_CAPACITY`
and `ACP_BRIDGE_IDLE_TTL_SECS` (used by the standalone binary and the
conformance suite; the daemon sets everything explicitly).

## Processes

- One primary ACP process per phone connection (`<agent>:<node_id>`) and one
  secondary process (`…:aux`) for read-only calls (`session/list`, model
  discovery), so listing never waits behind a streaming prompt.
- Processes idle for 300 s are killed. A respawned or crashed-and-replaced
  process is re-sent `initialize`, and a session is restored with
  `session/resume` (or `session/load`) before its next prompt.
- Requests on one process are serialized: two turns on one phone's primary
  process run one after the other.
- `process_per_cwd(true)` (mfcli): agents that ignore the `cwd` of
  `session/new` get one process per project, started in that directory
  (key `<agent>:<node_id>@<cwd>`); requests are routed by the session's
  project, and requests that name none use `$HOME`.

## Method mapping

| Codex | ACP |
|---|---|
| `initialize` | `initialize` (`protocolVersion: 1`) |
| `thread/start` | `session/new`, then `session/set_config_option` for a requested model |
| `thread/resume` | `session/load` (history rebuilt from the replay) |
| `thread/list` | `session/list` (with the request's `cwd`) on the secondary process |
| `turn/start` | restore if needed, `session/set_config_option` for the model, then for the thinking level (checked against the new model's levels), streaming `session/prompt` |
| `turn/interrupt` | `session/cancel` |
| `model/list` | models from `configOptions[id=model]`; each model's thinking levels from its `configOptions[id=thought_level]` |

Thinking levels can differ per model (mfcli). With `discover_models(true)`
the first `model/list` switches a throwaway session through every model to
learn each one's levels, then back to the model and level it started with.
The phone has no `max` level (it drops levels it does not know), so an
agent's `max` is offered as `xhigh`, and sent back as `max`, unless the model
also has a real `xhigh`; then `max` is not offered.

Permission requests (`session/request_permission`) are approved
automatically. `turn/steer`, rollback, archive and review are not supported.
