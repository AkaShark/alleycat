//! Project directories mfcli has seen, from `~/.codeflicker/data.json`
//! (`{"projects": {"/abs/path": {...}}}`). Missing or unreadable files
//! yield no directories: listing then relies on the cwd index.

use std::path::Path;

use serde_json::Value;
use tracing::warn;

pub fn project_dirs(data_json: &Path) -> Vec<String> {
    let text = match std::fs::read_to_string(data_json) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(err) => {
            warn!(path = %data_json.display(), error = %err, "cannot read mfcli data.json");
            return Vec::new();
        }
    };
    let value: Value = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(err) => {
            warn!(path = %data_json.display(), error = %err, "mfcli data.json is not valid JSON");
            return Vec::new();
        }
    };
    value
        .get("projects")
        .and_then(Value::as_object)
        .map(|projects| {
            projects
                .keys()
                .filter(|k| k.starts_with('/'))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_dirs_reads_absolute_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        std::fs::write(
            &path,
            r#"{"projects": {"/a": {}, "relative": {}, "/b/c": {}}, "recentModels": []}"#,
        )
        .unwrap();
        let mut dirs = project_dirs(&path);
        dirs.sort();
        assert_eq!(dirs, vec!["/a", "/b/c"]);
    }

    #[test]
    fn project_dirs_missing_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        assert!(project_dirs(&dir.path().join("nope.json")).is_empty());
    }

    #[test]
    fn project_dirs_corrupt_file_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("data.json");
        std::fs::write(&path, "{not json").unwrap();
        assert!(project_dirs(&path).is_empty());
        std::fs::write(&path, r#"{"projects": []}"#).unwrap();
        assert!(project_dirs(&path).is_empty());
    }
}
