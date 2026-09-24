//! Small filesystem helpers shared by the config writer and the push stores:
//! write-to-temp + fsync + rename, with the file restricted to the owner.

use std::path::{Path, PathBuf};

use anyhow::Context;
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// Atomically replace `target` with `contents` (mode 0600 on Unix).
pub(crate) async fn atomic_write(target: &Path, contents: &[u8]) -> anyhow::Result<()> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating {}", parent.display()))?;
    }
    let tmp = tmp_path(target);
    {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .await
            .with_context(|| format!("opening {}", tmp.display()))?;
        file.write_all(contents)
            .await
            .with_context(|| format!("writing {}", tmp.display()))?;
        file.flush().await.ok();
        file.sync_all().await.ok();
    }
    set_mode_0600(&tmp)?;
    fs::rename(&tmp, target)
        .await
        .with_context(|| format!("renaming {} -> {}", tmp.display(), target.display()))?;
    set_mode_0600(target)?;
    Ok(())
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_os_string();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(unix)]
pub(crate) fn set_mode_0600(path: &Path) -> anyhow::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("chmod 0600 {}", path.display()))
}

#[cfg(not(unix))]
pub(crate) fn set_mode_0600(_path: &Path) -> anyhow::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn atomic_write_replaces_contents_with_private_mode() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested").join("file.json");
        atomic_write(&path, b"one").await.unwrap();
        atomic_write(&path, b"two").await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        assert!(!tmp_path(&path).exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
    }
}
