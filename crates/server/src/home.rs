use anyhow::{Context, Result, ensure};
use std::path::{Component, Path, PathBuf};

// Resolve existing ancestors before creating anything, including symlinks above a new home.
fn resolved(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for part in absolute.components() {
        match part {
            Component::ParentDir => {
                normalized.pop();
            }
            Component::CurDir => {}
            part => {
                normalized.push(part);
                if normalized.exists() {
                    normalized = std::fs::canonicalize(&normalized)?;
                }
            }
        }
    }
    Ok(normalized)
}

pub(crate) fn prepare(path: &Path) -> Result<PathBuf> {
    let home = resolved(path)?;
    if let Some(user) = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE")) {
        ensure!(
            !home.starts_with(resolved(&PathBuf::from(user).join(".t3"))?),
            "The Rust migration must use a separate data directory from ~/.t3"
        );
    }
    let existed = home.exists();
    std::fs::create_dir_all(&home)?;
    #[cfg(unix)]
    if !existed {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&home, std::fs::Permissions::from_mode(0o700))?;
    }
    #[cfg(not(unix))]
    let _ = existed;
    Ok(home)
}

pub(crate) fn lock(home: &Path) -> Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(home.join("server.lock"))?;
    file.try_lock()
        .context("Another Rust server is already using this data directory")?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resolves_new_paths_before_writing() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("new/../state");
        assert_eq!(
            resolved(&path)?,
            std::fs::canonicalize(directory.path())?.join("state")
        );
        assert!(!directory.path().join("new").exists());
        Ok(())
    }
    #[cfg(unix)]
    #[test]
    fn preserves_existing_directory_permissions() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir()?;
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o755))?;
        prepare(directory.path())?;
        assert_eq!(
            std::fs::metadata(directory.path())?.permissions().mode() & 0o777,
            0o755
        );
        Ok(())
    }
    #[test]
    fn one_process_owns_a_home_at_a_time() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let first = lock(directory.path())?;
        assert!(lock(directory.path()).is_err());
        drop(first);
        lock(directory.path())?;
        Ok(())
    }
}
