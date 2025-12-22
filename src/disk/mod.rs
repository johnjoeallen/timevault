pub mod discovery;
pub mod fs_type;
pub mod identity;

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::os::unix::fs::PermissionsExt;

use crate::config::model::BackupDiskConfig;
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::guard::MountGuard;
use crate::mount::inspect::{device_is_mounted, mountpoint_is_mounted};
use crate::mount::ops::mount_device;
use crate::types::FsUuid;
use crate::util::paths::ensure_base_dir;

pub const DEFAULT_BACKUP_MOUNT_OPTS: &str = "rw,nodev,nosuid,noexec";
pub const DEFAULT_RESTORE_MOUNT_OPTS: &str = "ro,nodev,nosuid,noexec";
pub const DISK_ADD_ALLOWED_ENTRIES: [&str; 1] = ["lost+found"];

pub fn mount_options_for_backup(disk: &BackupDiskConfig) -> String {
    disk.mount_options
        .clone()
        .unwrap_or_else(|| DEFAULT_BACKUP_MOUNT_OPTS.to_string())
}

pub fn mount_options_for_restore(_disk: &BackupDiskConfig) -> String {
    DEFAULT_RESTORE_MOUNT_OPTS.to_string()
}

pub fn device_path_for_uuid(uuid: &str) -> PathBuf {
    Path::new("/dev/disk/by-uuid").join(uuid)
}

pub fn ensure_disk_not_mounted(device: &Path) -> Result<()> {
    if device_is_mounted(device)? {
        return Err(DiskError::Other(format!(
            "device {} is already mounted",
            device.display()
        ))
        .into());
    }
    Ok(())
}

pub fn resolve_fs_uuid(fs_uuid: Option<&str>, device: Option<&str>) -> Result<FsUuid> {
    if let Some(uuid) = fs_uuid {
        return uuid
            .parse::<FsUuid>()
            .map_err(|e| TimevaultError::message(e));
    }
    if let Some(device) = device {
        let device_path = Path::new(device);
        let device_real = device_path
            .canonicalize()
            .map_err(|e| TimevaultError::message(format!("resolve {}: {}", device, e)))?;
        let entries = std::fs::read_dir("/dev/disk/by-uuid")
            .map_err(|e| TimevaultError::message(format!("read /dev/disk/by-uuid: {}", e)))?;
        for entry in entries {
            let entry = entry
                .map_err(|e| TimevaultError::message(format!("read /dev/disk/by-uuid: {}", e)))?;
            let link_path = entry.path();
            let target = link_path
                .canonicalize()
                .map_err(|e| TimevaultError::message(format!("resolve {}: {}", link_path.display(), e)))?;
            if target == device_real {
                let name = entry.file_name().to_string_lossy().to_string();
                return name
                    .parse::<FsUuid>()
                    .map_err(|e| TimevaultError::message(e));
            }
        }
        return Err(TimevaultError::message(format!(
            "no filesystem UUID found for device {}",
            device
        )));
    }
    let entries = std::fs::read_dir("/dev/disk/by-uuid")
        .map_err(|e| TimevaultError::message(format!("read /dev/disk/by-uuid: {}", e)))?;
    let mut uuids = Vec::new();
    for entry in entries {
        let entry = entry
            .map_err(|e| TimevaultError::message(format!("read /dev/disk/by-uuid: {}", e)))?;
        let name = entry.file_name().to_string_lossy().to_string();
        uuids.push(name);
    }
    if uuids.len() == 1 {
        return uuids[0]
            .parse::<FsUuid>()
            .map_err(|e| TimevaultError::message(e));
    }
    if uuids.is_empty() {
        return Err(TimevaultError::message(
            "no filesystem UUIDs found; specify --fs-uuid or --device".to_string(),
        ));
    }
    Err(TimevaultError::message(
        "multiple filesystem UUIDs found; specify --fs-uuid or --device".to_string(),
    ))
}

pub fn select_disk(
    disks: &[BackupDiskConfig],
    disk_id: Option<&str>,
) -> Result<BackupDiskConfig> {
    let connected = connected_disks_in_order(disks)
        .into_iter()
        .map(|disk| disk.fs_uuid)
        .collect::<HashSet<_>>();
    select_disk_from_connected(disks, disk_id, &connected)
}

pub fn connected_disks_in_order(disks: &[BackupDiskConfig]) -> Vec<BackupDiskConfig> {
    disks
        .iter()
        .filter(|disk| device_path_for_uuid(&disk.fs_uuid).exists())
        .cloned()
        .collect()
}

pub fn select_disk_from_connected(
    disks: &[BackupDiskConfig],
    disk_id: Option<&str>,
    connected_uuids: &HashSet<String>,
) -> Result<BackupDiskConfig> {
    if disks.is_empty() {
        return Err(DiskError::Other(
            "no backup disks enrolled; run `timevault disk enroll ...`".to_string(),
        )
        .into());
    }
    if let Some(disk_id) = disk_id {
        let disk = disks
            .iter()
            .find(|disk| disk.disk_id == disk_id)
            .ok_or_else(|| DiskError::Other(format!("disk-id {} not found in config", disk_id)))?;
        if !connected_uuids.contains(&disk.fs_uuid) {
            return Err(DiskError::Other(format!("disk-id {} not connected", disk.disk_id)).into());
        }
        return Ok(disk.clone());
    }
    let connected: Vec<BackupDiskConfig> = disks
        .iter()
        .filter(|disk| connected_uuids.contains(&disk.fs_uuid))
        .cloned()
        .collect();
    if connected.is_empty() {
        return Err(DiskError::NoDiskConnected.into());
    }
    if connected.len() > 1 {
        return Err(DiskError::MultipleDisksConnected.into());
    }
    Ok(connected[0].clone())
}

pub fn select_first_connected(
    disks: &[BackupDiskConfig],
    disk_id: Option<&str>,
) -> Result<BackupDiskConfig> {
    let connected = connected_disks_in_order(disks);
    if disks.is_empty() {
        return Err(DiskError::Other(
            "no backup disks enrolled; run `timevault disk enroll ...`".to_string(),
        )
        .into());
    }
    if let Some(disk_id) = disk_id {
        let disk = disks
            .iter()
            .find(|disk| disk.disk_id == disk_id)
            .ok_or_else(|| DiskError::Other(format!("disk-id {} not found in config", disk_id)))?;
        if !connected.iter().any(|item| item.fs_uuid == disk.fs_uuid) {
            return Err(DiskError::Other(format!("disk-id {} not connected", disk.disk_id)).into());
        }
        return Ok(disk.clone());
    }
    if connected.is_empty() {
        return Err(DiskError::NoDiskConnected.into());
    }
    Ok(connected[0].clone())
}

pub fn mount_disk_guarded(
    disk: &BackupDiskConfig,
    mount_base: &Path,
    options: &str,
) -> Result<(MountGuard, PathBuf)> {
    let device = device_path_for_uuid(&disk.fs_uuid);
    if !device.exists() {
        return Err(DiskError::Other(format!("device {} not found", device.display())).into());
    }
    ensure_disk_not_mounted(&device)?;
    ensure_base_dir(mount_base)?;
    let mountpoint = mount_base.join(&disk.fs_uuid);
    if mountpoint.exists() && !mountpoint.is_dir() {
        return Err(DiskError::Other(format!(
            "mountpoint {} exists and is not a directory",
            mountpoint.display()
        ))
        .into());
    }
    if !mountpoint.exists() {
        std::fs::create_dir_all(&mountpoint)
            .map_err(|e| TimevaultError::message(format!("create {}: {}", mountpoint.display(), e)))?;
        let mut perms = std::fs::metadata(&mountpoint)
            .map_err(|e| TimevaultError::message(format!("stat {}: {}", mountpoint.display(), e)))?
            .permissions();
        perms.set_mode(0o700);
        std::fs::set_permissions(&mountpoint, perms)
            .map_err(|e| TimevaultError::message(format!("chmod {}: {}", mountpoint.display(), e)))?;
    }
    if mountpoint_is_mounted(&mountpoint)? {
        return Err(DiskError::Other(format!(
            "mountpoint {} is already in use",
            mountpoint.display()
        ))
        .into());
    }
    mount_device(&device, &mountpoint, options)?;
    Ok((MountGuard::new(mountpoint.clone(), false), mountpoint))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_disk_with_connected_uuids() {
        let disks = vec![
            BackupDiskConfig {
                disk_id: "a".to_string(),
                fs_uuid: "uuid-a".to_string(),
                label: None,
                mount_options: None,
            },
            BackupDiskConfig {
                disk_id: "b".to_string(),
                fs_uuid: "uuid-b".to_string(),
                label: None,
                mount_options: None,
            },
        ];
        let connected = ["uuid-b".to_string()].into_iter().collect();
        let selected = select_disk_from_connected(&disks, None, &connected).unwrap();
        assert_eq!(selected.disk_id, "b");
    }
}
