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

pub fn create_temp_dir(base: &Path, prefix: &str) -> Result<PathBuf> {
    ensure_base_dir(base)?;
    let ts = chrono::Utc::now().format("%Y%m%d%H%M%S%3f");
    let candidate = base.join(format!("{}-{}-{}", prefix, std::process::id(), ts));
    fs::create_dir_all(&candidate)
        .map_err(|e| TimevaultError::message(format!("create {}: {}", candidate.display(), e)))?;
    let mut perms = fs::metadata(&candidate)
        .map_err(|e| TimevaultError::message(format!("stat {}: {}", candidate.display(), e)))?
        .permissions();
    perms.set_mode(0o700);
    fs::set_permissions(&candidate, perms)
        .map_err(|e| TimevaultError::message(format!("chmod {}: {}", candidate.display(), e)))?;
    Ok(candidate)
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
