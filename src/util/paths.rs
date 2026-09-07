use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use crate::error::{Result, TimevaultError};

pub fn is_safe_name(name: &str) -> bool {
    if name.is_empty() || name == "." || name == ".." {
        return false;
    }
    name.chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
}

pub fn job_lock_path(name: &str) -> Result<PathBuf> {
    if !is_safe_name(name) {
        return Err(TimevaultError::message(format!(
            "job {} name must use only letters, digits, '.', '-', '_'",
            name
        )));
    }
    Ok(PathBuf::from(format!("/var/run/timevault.{}.pid", name)))
}

pub fn ensure_base_dir(path: &Path) -> Result<()> {
    if path.exists() {
        let meta = fs::metadata(path)
            .map_err(|e| TimevaultError::message(format!("stat {}: {}", path.display(), e)))?;
        if !meta.is_dir() {
            return Err(TimevaultError::message(format!(
                "{} is not a directory",
                path.display()
            )));
        }
        if meta.uid() != 0 {
            return Err(TimevaultError::message(format!(
                "{} must be owned by root",
                path.display()
            )));
        }
        let mut perms = meta.permissions();
        perms.set_mode(0o700);
        fs::set_permissions(path, perms)
            .map_err(|e| TimevaultError::message(format!("chmod {}: {}", path.display(), e)))?;
        return Ok(());
    }
    fs::create_dir_all(path)
        .map_err(|e| TimevaultError::message(format!("create {}: {}", path.display(), e)))?;
    let meta = fs::metadata(path)
        .map_err(|e| TimevaultError::message(format!("stat {}: {}", path.display(), e)))?;
    if meta.uid() != 0 {
        return Err(TimevaultError::message(format!(
            "{} must be owned by root",
            path.display()
        )));
    }
    let mut perms = meta.permissions();
    perms.set_mode(0o700);
    fs::set_permissions(path, perms)
        .map_err(|e| TimevaultError::message(format!("chmod {}: {}", path.display(), e)))?;
    Ok(())
}

fn create_dir_0700(candidate: &Path) -> Result<()> {
    fs::create_dir_all(candidate)
        .map_err(|e| TimevaultError::message(format!("create {}: {}", candidate.display(), e)))?;
    let mut perms = fs::metadata(candidate)
        .map_err(|e| TimevaultError::message(format!("stat {}: {}", candidate.display(), e)))?
        .permissions();
    perms.set_mode(0o700);
    fs::set_permissions(candidate, perms)
        .map_err(|e| TimevaultError::message(format!("chmod {}: {}", candidate.display(), e)))?;
    Ok(())
}

pub fn create_temp_dir(base: &Path, prefix: &str) -> Result<PathBuf> {
    ensure_base_dir(base)?;
    let ts = chrono::Utc::now().format("%Y%m%d%H%M%S%3f");
    let candidate = base.join(format!("{}-{}-{}", prefix, std::process::id(), ts));
    create_dir_0700(&candidate)?;
    Ok(candidate)
}

/// A private, per-process mount point for a backup run: `<base>/<fsUuid>.<pid>.<ts>`.
/// The dot separator keeps the pid parseable even though `fsUuid` contains `-`.
/// Cleaning up leaked ones ([`parse_run_mount_pid`]) checks whether the pid is
/// still alive.
pub fn create_run_mount_dir(base: &Path, fs_uuid: &str) -> Result<PathBuf> {
    ensure_base_dir(base)?;
    let ts = chrono::Utc::now().format("%Y%m%d%H%M%S%3f");
    let candidate = base.join(format!("{}.{}.{}", fs_uuid, std::process::id(), ts));
    create_dir_0700(&candidate)?;
    Ok(candidate)
}

/// The pid embedded in a [`create_run_mount_dir`] directory name, or `None` if
/// `name` is not one (e.g. a stray directory an operator created).
pub fn parse_run_mount_pid(name: &str) -> Option<u32> {
    let mut parts = name.rsplitn(3, '.');
    let _ts = parts.next()?;
    let pid = parts.next()?;
    let _fs_uuid = parts.next()?;
    pid.parse().ok()
}

pub fn list_entries(path: &Path) -> Result<Vec<String>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(path)
        .map_err(|e| TimevaultError::message(format!("read {}: {}", path.display(), e)))?
    {
        let entry = entry
            .map_err(|e| TimevaultError::message(format!("read {}: {}", path.display(), e)))?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name == "." || name == ".." {
            continue;
        }
        out.push(name);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_mount_dir_name_round_trips_pid() {
        let base = Path::new("/run/timevault/mounts");
        let uuid = "39d0cce0-1af3-430e-b271-60947927fe0a";
        // reproduce the naming create_run_mount_dir uses
        let name = format!("{}.{}.20260907153000123", uuid, 4242);
        assert_eq!(parse_run_mount_pid(&name), Some(4242));
        assert!(base.join(&name).starts_with(base));
    }

    #[test]
    fn parse_run_mount_pid_rejects_other_names() {
        assert_eq!(
            parse_run_mount_pid("39d0cce0-1af3-430e-b271-60947927fe0a"),
            None
        );
        assert_eq!(parse_run_mount_pid("tv-1234-20260101"), None);
        assert_eq!(parse_run_mount_pid("uuid.notapid.20260101"), None);
        assert_eq!(parse_run_mount_pid("lost+found"), None);
    }
}
