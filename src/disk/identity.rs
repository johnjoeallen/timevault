use std::io::{Read, Write};

use serde::{Deserialize, Serialize};

use crate::disk::fs_type::FsType;
use crate::error::{DiskError, Result, TimevaultError};

pub const IDENTITY_VERSION: u32 = 1;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DiskIdentity {
    pub version: u32,
    #[serde(rename = "diskId")]
    pub disk_id: String,
    #[serde(rename = "fsUuid")]
    pub fs_uuid: String,
    #[serde(rename = "fsType", skip_serializing_if = "Option::is_none")]
    pub fs_type: Option<FsType>,
    pub created: String,
}

/// Everything Timevault keeps on a disk outside the job directories lives in
/// this directory. Releases before 1.21.0 kept the identity in a file of this
/// name and the file index in `.timevault-index`.
pub const META_DIR: &str = ".timevault";
pub const IDENTITY_FILE: &str = "identity";
const LEGACY_INDEX_DIR: &str = ".timevault-index";
const MIGRATING_IDENTITY: &str = ".timevault-identity.old";

/// Where the identity is read from: `.timevault/identity`, or the old
/// `.timevault` file on a disk that has not been migrated yet (for example one
/// mounted read-only).
pub fn identity_path(root: &std::path::Path) -> std::path::PathBuf {
    let meta = root.join(META_DIR);
    if meta.is_file() {
        meta
    } else {
        meta.join(IDENTITY_FILE)
    }
}

/// Move the metadata of an older release into `.timevault/`: the identity file
/// becomes `.timevault/identity` and `.timevault-index` becomes
/// `.timevault/index`. Safe to repeat, and safe to interrupt: a crash between
/// the two renames of the identity leaves it as `.timevault-identity.old`, which
/// the next call completes. Needs a writable disk.
pub fn migrate_layout(root: &std::path::Path) -> std::io::Result<()> {
    use std::fs;
    let meta = root.join(META_DIR);
    let parked = root.join(MIGRATING_IDENTITY);
    if meta.is_file() {
        fs::rename(&meta, &parked)?;
    }
    if parked.is_file() {
        fs::create_dir_all(&meta)?;
        let target = meta.join(IDENTITY_FILE);
        if target.exists() {
            fs::remove_file(&parked)?;
        } else {
            fs::rename(&parked, &target)?;
        }
    }
    let old_index = root.join(LEGACY_INDEX_DIR);
    let new_index = meta.join("index");
    if old_index.is_dir() && !new_index.exists() {
        fs::create_dir_all(&meta)?;
        fs::rename(&old_index, &new_index)?;
    }
    Ok(())
}

pub fn read_identity(path: &std::path::Path) -> Result<DiskIdentity> {
    let mut contents = String::new();
    std::fs::File::open(path)
        .map_err(|e| TimevaultError::message(format!("open {}: {}", path.display(), e)))?
        .read_to_string(&mut contents)
        .map_err(|e| TimevaultError::message(format!("read {}: {}", path.display(), e)))?;
    serde_yaml::from_str(&contents)
        .map_err(|e| TimevaultError::message(format!("parse {}: {}", path.display(), e)))
}

pub fn write_identity(path: &std::path::Path, identity: &DiskIdentity) -> Result<()> {
    let data = serde_yaml::to_string(identity)
        .map_err(|e| TimevaultError::message(format!("encode identity: {}", e)))?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| TimevaultError::message(format!("create {}: {}", parent.display(), e)))?;
    }
    let mut file = std::fs::File::create(path)
        .map_err(|e| TimevaultError::message(format!("create {}: {}", path.display(), e)))?;
    file.write_all(data.as_bytes())
        .map_err(|e| TimevaultError::message(format!("write {}: {}", path.display(), e)))?;
    Ok(())
}

pub fn verify_identity(identity: &DiskIdentity, disk_id: &str, fs_uuid: &str) -> Result<()> {
    if identity.version != IDENTITY_VERSION {
        return Err(DiskError::IdentityMismatch(format!(
            "version mismatch: expected {}, got {}",
            IDENTITY_VERSION, identity.version
        ))
        .into());
    }
    if identity.disk_id != disk_id {
        return Err(DiskError::IdentityMismatch(format!(
            "diskId mismatch: expected {}, got {}",
            disk_id, identity.disk_id
        ))
        .into());
    }
    if identity.fs_uuid != fs_uuid {
        return Err(DiskError::IdentityMismatch(format!(
            "fsUuid mismatch: expected {}, got {}",
            fs_uuid, identity.fs_uuid
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn identity_roundtrip() {
        let dir = TempDir::new().expect("tempdir");
        let path = identity_path(dir.path());
        let identity = DiskIdentity {
            version: IDENTITY_VERSION,
            disk_id: "disk1".to_string(),
            fs_uuid: "uuid-1".to_string(),
            fs_type: Some(crate::disk::fs_type::FsType::Ext4),
            created: "2025-01-01T00:00:00Z".to_string(),
        };
        write_identity(&path, &identity).expect("write");
        let loaded = read_identity(&path).expect("read");
        verify_identity(&loaded, "disk1", "uuid-1").expect("verify");
        assert_eq!(loaded.disk_id, "disk1");
    }

    #[test]
    fn an_old_layout_is_moved_into_the_timevault_directory() {
        let dir = TempDir::new().expect("tempdir");
        let root = dir.path();
        std::fs::write(root.join(".timevault"), "version: 1\n").unwrap();
        std::fs::create_dir(root.join(".timevault-index")).unwrap();
        std::fs::write(root.join(".timevault-index/file-index.sqlite"), "db").unwrap();

        assert_eq!(identity_path(root), root.join(".timevault"));
        migrate_layout(root).unwrap();
        migrate_layout(root).unwrap();

        assert_eq!(identity_path(root), root.join(".timevault/identity"));
        assert_eq!(
            std::fs::read_to_string(identity_path(root)).unwrap(),
            "version: 1\n"
        );
        assert!(root.join(".timevault/index/file-index.sqlite").is_file());
        assert!(!root.join(".timevault-index").exists());
        assert!(!root.join(".timevault-identity.old").exists());
    }

    #[test]
    fn an_interrupted_migration_is_completed() {
        let dir = TempDir::new().expect("tempdir");
        let root = dir.path();
        std::fs::write(root.join(".timevault-identity.old"), "kept").unwrap();

        migrate_layout(root).unwrap();

        assert_eq!(
            std::fs::read_to_string(root.join(".timevault/identity")).unwrap(),
            "kept"
        );
        assert!(!root.join(".timevault-identity.old").exists());
    }
}
