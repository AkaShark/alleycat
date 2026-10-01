//! mfcli's own session files: `~/.codeflicker/projects/<slug>/<id>.jsonl`.
//!
//! mfcli's ACP `session/list` only lists the project its process started
//! in, so listing every project through ACP would cold-start one mfcli per
//! project. The bridge reads the files instead (as the Devin and Grok
//! bridges read their agents' stores).

use std::collections::BTreeSet;
use std::path::Path;
use std::time::UNIX_EPOCH;

use serde_json::Value;
use sha1::{Digest, Sha1};
use tracing::warn;

use crate::listing::AcpSession;

/// mfcli truncates long slugs to this many characters and appends a hash.
const SLUG_MAX: usize = 80;
/// mfcli titles are the first line of the first user message, cut here.
const TITLE_MAX: usize = 50;

/// Directory name mfcli uses under `projects/` for `cwd`: the path
/// lowercased, every run of non-`[a-z0-9]` characters folded into one `-`,
/// leading/trailing `-` dropped; longer than 80 characters → the first 80
/// plus `-` and the first 8 hex digits of the path's SHA-1. `/` maps to ``.
pub fn project_slug(cwd: &str) -> String {
    let mut slug = String::with_capacity(cwd.len());
    for ch in cwd.chars() {
        let c = ch.to_ascii_lowercase();
        if c.is_ascii_alphanumeric() {
            slug.push(c);
        } else if !slug.is_empty() && !slug.ends_with('-') {
            slug.push('-');
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    if slug.len() > SLUG_MAX {
        slug.truncate(SLUG_MAX);
        let digest = Sha1::digest(cwd.as_bytes());
        let hex: String = digest.iter().take(4).map(|b| format!("{b:02x}")).collect();
        slug.push('-');
        slug.push_str(&hex);
    }
    slug
}

/// Sessions mfcli stored for project `cwd` (unsorted). Files without a
/// user message (never prompted) are skipped, as are unreadable files.
pub fn sessions_in(projects_dir: &Path, cwd: &str) -> Vec<AcpSession> {
    let dir = projects_dir.join(project_slug(cwd));
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("jsonl") {
            continue;
        }
        let Some(session_id) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(err) => {
                warn!(path = %path.display(), error = %err, "cannot read mfcli session file");
                continue;
            }
        };
        let Some(title) = first_user_text(&text).map(|t| title_of(&t)) else {
            continue;
        };
        let updated_at_ms = entry
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        out.push(AcpSession {
            session_id: session_id.to_string(),
            cwd: cwd.to_string(),
            title,
            updated_at_ms,
        });
    }
    out
}

/// The project (one of `cwds`) whose directory holds session `id`.
pub fn find_session(projects_dir: &Path, cwds: &BTreeSet<String>, id: &str) -> Option<String> {
    cwds.iter()
        .find(|cwd| {
            projects_dir
                .join(project_slug(cwd))
                .join(format!("{id}.jsonl"))
                .is_file()
        })
        .cloned()
}

fn first_user_text(jsonl: &str) -> Option<String> {
    jsonl
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|v| {
            v.get("type") == Some(&Value::from("message"))
                && v.get("role") == Some(&Value::from("user"))
        })
        .and_then(|v| match v.get("content")? {
            Value::String(text) => Some(text.clone()),
            Value::Array(blocks) => Some(
                blocks
                    .iter()
                    .filter_map(|b| b.get("text").and_then(Value::as_str))
                    .collect::<Vec<_>>()
                    .join(" "),
            ),
            _ => None,
        })
}

fn title_of(text: &str) -> String {
    let line = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("");
    if line.chars().count() > TITLE_MAX {
        let cut: String = line.chars().take(TITLE_MAX).collect();
        format!("{cut}...")
    } else {
        line.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slug_matches_mfcli_for_known_directories() {
        let base = "/private/tmp/claude-501/-Users-sharker-Desktop-Project-Person-Project-AgentBuddy/c850b61f-790f-4443-bed9-560e69268953/scratchpad/acp-probe";
        assert_eq!(
            project_slug(&format!("{base}/ws")),
            "private-tmp-claude-501-users-sharker-desktop-project-person-project-agentbuddy-c-d75e1bff"
        );
        assert_eq!(
            project_slug(&format!("{base}/other")),
            "private-tmp-claude-501-users-sharker-desktop-project-person-project-agentbuddy-c-95574c09"
        );
        assert_eq!(
            project_slug(
                "/Users/sharker/Desktop/Project/Person/Project/alleycat-mfcli/crates/bridge-conformance"
            ),
            "users-sharker-desktop-project-person-project-alleycat-mfcli-crates-bridge-confor-69b89cae"
        );
        assert_eq!(
            project_slug("/Users/sharker/Downloads"),
            "users-sharker-downloads"
        );
        assert_eq!(project_slug("/"), "");
    }

    fn write_session(projects: &Path, cwd: &str, id: &str, lines: &[&str]) {
        let dir = projects.join(project_slug(cwd));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{id}.jsonl")), lines.join("\n")).unwrap();
    }

    #[test]
    fn sessions_in_reads_titles_and_skips_empty_or_broken_files() {
        let tmp = tempfile::tempdir().unwrap();
        let projects = tmp.path();
        write_session(
            projects,
            "/p",
            "s1",
            &[
                r#"{"type":"config","config":{}}"#,
                "not json",
                r#"{"type":"message","role":"user","content":"Fix the login bug\nand add a test","sessionId":"s1"}"#,
            ],
        );
        write_session(
            projects,
            "/p",
            "s2",
            &[
                r#"{"type":"message","role":"user","content":[{"type":"text","text":"Write a long description of the whole architecture please"}]}"#,
            ],
        );
        write_session(
            projects,
            "/p",
            "empty",
            &[r#"{"type":"config","config":{}}"#],
        );
        std::fs::write(projects.join(project_slug("/p")).join("notes.txt"), "x").unwrap();

        let mut sessions = sessions_in(projects, "/p");
        sessions.sort_by(|a, b| a.session_id.cmp(&b.session_id));
        let ids: Vec<_> = sessions.iter().map(|s| s.session_id.as_str()).collect();
        assert_eq!(ids, vec!["s1", "s2"]);
        assert_eq!(sessions[0].title, "Fix the login bug");
        assert_eq!(
            sessions[1].title,
            "Write a long description of the whole architecture..."
        );
        assert!(
            sessions
                .iter()
                .all(|s| s.cwd == "/p" && s.updated_at_ms > 0)
        );
        assert!(sessions_in(projects, "/missing").is_empty());
    }

    #[test]
    fn find_session_locates_the_project() {
        let tmp = tempfile::tempdir().unwrap();
        write_session(
            tmp.path(),
            "/q",
            "s9",
            &[r#"{"type":"message","role":"user","content":"hi"}"#],
        );
        let cwds = BTreeSet::from(["/p".to_string(), "/q".to_string()]);
        assert_eq!(find_session(tmp.path(), &cwds, "s9").as_deref(), Some("/q"));
        assert_eq!(find_session(tmp.path(), &cwds, "nope"), None);
    }
}
