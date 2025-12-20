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

pub fn identity_path(root: &std::path::Path) -> std::path::PathBuf {
    root.join(".timevault")
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
        let path = dir.path().join(".timevault");
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
}
