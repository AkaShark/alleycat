//! Persistent `sessionId → cwd` index for sessions the bridge has seen.
//! mfcli's own session files do not record the real cwd, so this index is
//! how resume and listing find the project of a session after a restart.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    pub cwd: String,
    pub updated_at_ms: i64,
}

pub struct CwdIndex {
    path: PathBuf,
    entries: Mutex<BTreeMap<String, IndexEntry>>,
}

impl CwdIndex {
    /// Load the index; a missing or corrupt file starts empty.
    pub fn load(path: PathBuf) -> Self {
        let entries = match std::fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text).unwrap_or_else(|err| {
                warn!(path = %path.display(), error = %err, "mfcli cwd index is corrupt; starting empty");
                BTreeMap::new()
            }),
            Err(_) => BTreeMap::new(),
        };
        Self {
            path,
            entries: Mutex::new(entries),
        }
    }

    pub fn cwd_for(&self, session_id: &str) -> Option<String> {
        self.entries
            .lock()
            .expect("index poisoned")
            .get(session_id)
            .map(|e| e.cwd.clone())
    }

    pub fn cwds(&self) -> BTreeSet<String> {
        self.entries
            .lock()
            .expect("index poisoned")
            .values()
            .map(|e| e.cwd.clone())
            .collect()
    }

    /// Record `(sessionId, cwd, updatedAtMs)` triples; relative cwds and
    /// the `/` placeholder are ignored. Writes the file (atomically) only
    /// when something changed.
    pub fn record_all(&self, items: impl IntoIterator<Item = (String, String, i64)>) {
        let snapshot = {
            let mut entries = self.entries.lock().expect("index poisoned");
            let mut changed = false;
            for (session_id, cwd, updated_at_ms) in items {
                if !cwd.starts_with('/') || cwd == "/" {
                    continue;
                }
                let entry = IndexEntry { cwd, updated_at_ms };
                if entries.get(&session_id) != Some(&entry) {
                    entries.insert(session_id, entry);
                    changed = true;
                }
            }
            if !changed {
                return;
            }
            entries.clone()
        };
        if let Err(err) = self.persist(&snapshot) {
            warn!(path = %self.path.display(), error = %err, "failed to write mfcli cwd index");
        }
    }

    fn persist(&self, snapshot: &BTreeMap<String, IndexEntry>) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = self.path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(snapshot)?)?;
        std::fs::rename(&tmp, &self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_persist_and_reload() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state/mfcli-sessions.json");
        let index = CwdIndex::load(path.clone());
        index.record_all([
            ("s1".to_string(), "/p/one".to_string(), 10),
            ("s2".to_string(), "relative".to_string(), 11),
            ("s3".to_string(), "/".to_string(), 12),
        ]);
        let reloaded = CwdIndex::load(path.clone());
        assert_eq!(reloaded.cwd_for("s1").as_deref(), Some("/p/one"));
        assert_eq!(reloaded.cwd_for("s2"), None);
        assert_eq!(reloaded.cwd_for("s3"), None);
        assert!(!path.with_extension("json.tmp").exists());
    }

    #[test]
    fn corrupt_index_starts_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("idx.json");
        std::fs::write(&path, "garbage").unwrap();
        assert!(CwdIndex::load(path).cwds().is_empty());
    }
}
