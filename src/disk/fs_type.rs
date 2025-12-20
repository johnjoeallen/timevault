use std::fmt;
use std::process::Command;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::{Result, TimevaultError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FsType {
    Ext2,
    Ext3,
    Ext4,
    Xfs,
    Jfs,
    Btrfs,
    Zfs,
    F2fs,
    Other(String),
}

impl FsType {
    pub fn from_str(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "ext2" => FsType::Ext2,
            "ext3" => FsType::Ext3,
            "ext4" => FsType::Ext4,
            "xfs" => FsType::Xfs,
            "jfs" => FsType::Jfs,
            "btrfs" => FsType::Btrfs,
            "zfs" | "zfs_member" => FsType::Zfs,
            "f2fs" => FsType::F2fs,
            other => FsType::Other(other.to_string()),
        }
    }

    pub fn is_allowed(&self) -> bool {
        matches!(
            self,
            FsType::Ext2
                | FsType::Ext3
                | FsType::Ext4
                | FsType::Xfs
                | FsType::Jfs
                | FsType::Btrfs
                | FsType::Zfs
                | FsType::F2fs
        )
    }

    pub fn is_rejected(&self) -> bool {
        matches!(
            self,
            FsType::Other(name)
                if name == "vfat"
                    || name == "fat"
                    || name == "fat32"
                    || name == "exfat"
                    || name == "ntfs"
                    || name == "hfsplus"
                    || name == "hfs"
                    || name == "apfs"
                    || name == "iso9660"
                    || name == "udf"
                    || name == "msdos"
        )
    }

    pub fn as_str(&self) -> &str {
        match self {
            FsType::Ext2 => "ext2",
            FsType::Ext3 => "ext3",
            FsType::Ext4 => "ext4",
            FsType::Xfs => "xfs",
            FsType::Jfs => "jfs",
            FsType::Btrfs => "btrfs",
            FsType::Zfs => "zfs",
            FsType::F2fs => "f2fs",
            FsType::Other(name) => name,
        }
    }
}

impl fmt::Display for FsType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Serialize for FsType {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for FsType {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Ok(FsType::from_str(&value))
    }
}

pub fn detect_fs_type(device: &str) -> Result<FsType> {
    let output = Command::new("blkid")
        .arg("-o")
        .arg("value")
        .arg("-s")
        .arg("TYPE")
        .arg(device)
        .output()
        .map_err(|e| TimevaultError::message(format!("blkid {}: {}", device, e)))?;
    if !output.status.success() {
        return Err(TimevaultError::message(format!(
            "blkid {} failed with exit code {}",
            device,
            output.status.code().unwrap_or(1)
        )));
    }
    let fstype = String::from_utf8_lossy(&output.stdout).trim().to_string();
    Ok(FsType::from_str(&fstype))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_type_allowlist() {
        assert!(FsType::from_str("ext4").is_allowed());
        assert!(FsType::from_str("xfs").is_allowed());
        assert!(!FsType::from_str("vfat").is_allowed());
        assert!(FsType::from_str("vfat").is_rejected());
    }
}
