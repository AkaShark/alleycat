//! Terminal observation for bridge agents (claude, pi, opencode, amp, droid,
//! hermes, devin, grok): every outbound frame passes through
//! `bridge-core::Session::enqueue`, so a [`SessionObserver`] on each session
//! sees `turn/completed` even while no phone is attached.

use std::sync::Arc;

use alleycat_bridge_core::SessionObserver;
use alleycat_bridge_core::session::AgentId;
use serde_json::Value;
use tokio::sync::mpsc;

use super::{TerminalReport, terminal_from_turn_status};

pub struct BridgeTerminalObserver {
    reports: mpsc::UnboundedSender<TerminalReport>,
}

impl BridgeTerminalObserver {
    /// Observer to install on every bridge session.
    pub fn shared(reports: mpsc::UnboundedSender<TerminalReport>) -> Arc<dyn SessionObserver> {
        Arc::new(Self { reports })
    }
}

impl SessionObserver for BridgeTerminalObserver {
    fn on_enqueue(&self, _node_id: &str, agent: AgentId, payload: &Value) {
        // Cheap filter first: this runs on every frame of every bridge.
        if payload.get("method").and_then(Value::as_str) != Some("turn/completed") {
            return;
        }
        if let Some(report) = parse_turn_completed(agent, payload) {
            let _ = self.reports.send(report);
        }
    }
}

/// `{"method":"turn/completed","params":{"threadId","turn":{"id","status",
/// "completedAt"?}}}` → report. `inProgress` (or anything unknown) is not a
/// terminal state and yields `None`.
pub fn parse_turn_completed(agent: &str, payload: &Value) -> Option<TerminalReport> {
    if payload.get("method")?.as_str()? != "turn/completed" {
        return None;
    }
    let params = payload.get("params")?;
    let thread_id = params.get("threadId")?.as_str()?;
    let turn = params.get("turn")?;
    let turn_id = turn.get("id")?.as_str()?;
    let terminal =
        terminal_from_turn_status(turn.get("status")?.as_str()?, turn.get("completedAt"))?;
    Some(TerminalReport {
        agent: agent.to_string(),
        thread_id: thread_id.to_string(),
        turn_id: turn_id.to_string(),
        terminal,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::push::store::{FailureReason, TerminalKind};
    use alleycat_bridge_core::session::{SessionRegistry, SessionRegistryConfig};
    use serde_json::json;
    use std::time::Duration;

    fn frame(status: &str) -> Value {
        json!({
            "jsonrpc": "2.0",
            "method": "turn/completed",
            "params": {
                "threadId": "th-1",
                "turn": {"id": "tu-1", "status": status, "items": [], "completedAt": 1790300123}
            }
        })
    }

    #[test]
    fn parses_turn_statuses() {
        let done = parse_turn_completed("claude", &frame("completed")).unwrap();
        assert_eq!(done.agent, "claude");
        assert_eq!(done.thread_id, "th-1");
        assert_eq!(done.turn_id, "tu-1");
        assert_eq!(done.terminal.kind, TerminalKind::Completed);
        assert_eq!(done.terminal.occurred_at, 1790300123);

        let failed = parse_turn_completed("pi", &frame("failed")).unwrap();
        assert_eq!(failed.terminal.kind, TerminalKind::Failed);
        assert_eq!(failed.terminal.reason, Some(FailureReason::Error));

        let interrupted = parse_turn_completed("pi", &frame("interrupted")).unwrap();
        assert_eq!(interrupted.terminal.kind, TerminalKind::Failed);
        assert_eq!(
            interrupted.terminal.reason,
            Some(FailureReason::Interrupted)
        );

        assert!(parse_turn_completed("pi", &frame("inProgress")).is_none());
        assert!(parse_turn_completed("pi", &json!({"method": "turn/started"})).is_none());
        assert!(parse_turn_completed("pi", &json!({"method": "turn/completed"})).is_none());
    }

    #[tokio::test]
    async fn observer_reports_turn_completed_from_orphaned_session() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let registry = SessionRegistry::with_observer(
            SessionRegistryConfig::default(),
            Some(BridgeTerminalObserver::shared(tx)),
        );
        let session = registry.get_or_create("phone".into(), "claude");
        // Phone detaches and the reaper drops the session; the bridge still
        // holds the Arc and keeps emitting.
        let _handle = session.install_attachment(None);
        session.drop_attachment();
        registry.tick(Duration::ZERO, Duration::ZERO);
        assert!(registry.get("phone", "claude").is_none());

        session
            .enqueue(json!({"jsonrpc": "2.0", "method": "item/agentMessage/delta", "params": {}}));
        session.enqueue(frame("completed"));

        let report = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.agent, "claude");
        assert_eq!(report.turn_id, "tu-1");
        assert!(rx.try_recv().is_err(), "only turn/completed is reported");
    }
}
