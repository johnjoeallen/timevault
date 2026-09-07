pub mod discovery;
pub mod fs_type;
pub mod identity;

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::config::model::BackupDiskConfig;
use crate::error::{DiskError, Result, TimevaultError};
use crate::mount::guard::MountGuard;
use crate::mount::inspect::{device_is_mounted, find_all_device_mountpoints, find_mounts_under};
use crate::mount::ops::{mount_device, unmount_path};
use crate::types::FsUuid;
use crate::util::paths::{create_run_mount_dir, parse_run_mount_pid};

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
        return Err(
            DiskError::Other(format!("device {} is already mounted", device.display())).into(),
        );
    }
    Ok(())
}

/// The first mountpoint of the device that lies outside `mount_base` — a data
/// disk, a manual mount — which must block a backup. Mounts *under* `mount_base`
/// are other `timevault` runs and are fine.
fn backup_mount_conflict<'a>(mountpoints: &'a [PathBuf], mount_base: &Path) -> Option<&'a PathBuf> {
    mountpoints
        .iter()
        .find(|mountpoint| !mountpoint.starts_with(mount_base))
}

/// Backup runs mount each disk at their own private point under `mount_base`, so
/// the device being mounted there already (by a parallel run) is fine; only a
/// mount *outside* `mount_base` blocks us.
pub fn ensure_disk_mountable_for_backup(device: &Path, mount_base: &Path) -> Result<()> {
    let mountpoints = find_all_device_mountpoints(device)?;
    if let Some(mountpoint) = backup_mount_conflict(&mountpoints, mount_base) {
        return Err(DiskError::Other(format!(
            "device {} is mounted at {} outside {}",
            device.display(),
            mountpoint.display(),
            mount_base.display()
        ))
        .into());
    }
    Ok(())
}

/// Unmount and remove per-run mount points left behind by a run that was killed
/// hard (SIGKILL / power loss) before its cleanup could run. Safe to call at the
/// start of every backup run.
pub fn sweep_stale_run_mounts(mount_base: &Path) {
    if !mount_base.exists() {
        return;
    }
    let mounted: Vec<PathBuf> = find_mounts_under(mount_base).unwrap_or_default();
    let Ok(entries) = std::fs::read_dir(mount_base) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(pid) = name.to_str().and_then(parse_run_mount_pid) else {
            continue;
        };
        if Path::new("/proc").join(pid.to_string()).exists() {
            continue; // owner still running
        }
        if mounted.iter().any(|m| m == &path) {
            let _ = unmount_path(&path);
        }
        let _ = std::fs::remove_dir(&path);
    }
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
            let target = link_path.canonicalize().map_err(|e| {
                TimevaultError::message(format!("resolve {}: {}", link_path.display(), e))
            })?;
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
        let entry =
            entry.map_err(|e| TimevaultError::message(format!("read /dev/disk/by-uuid: {}", e)))?;
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

pub fn disk_matches_selector(disk: &BackupDiskConfig, selector: &str) -> bool {
    disk.disk_id == selector || disk.fs_uuid == selector
}

pub fn select_disk(disks: &[BackupDiskConfig], selector: Option<&str>) -> Result<BackupDiskConfig> {
    let connected = connected_disks_in_order(disks)
        .into_iter()
        .map(|disk| disk.fs_uuid)
        .collect::<HashSet<_>>();
    select_disk_from_connected(disks, selector, &connected)
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
    selector: Option<&str>,
    connected_uuids: &HashSet<String>,
) -> Result<BackupDiskConfig> {
    if disks.is_empty() {
        return Err(DiskError::Other(
            "no backup disks enrolled; run `timevault disk enroll ...`".to_string(),
        )
        .into());
    }
    if let Some(selector) = selector {
        let disk = disks
            .iter()
            .find(|disk| disk_matches_selector(disk, selector))
            .ok_or_else(|| {
                DiskError::Other(format!("disk selector {} not found in config", selector))
            })?;
        if !connected_uuids.contains(&disk.fs_uuid) {
            return Err(
                DiskError::Other(format!("disk selector {} not connected", selector)).into(),
            );
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
    selector: Option<&str>,
) -> Result<BackupDiskConfig> {
    let connected = connected_disks_in_order(disks);
    if disks.is_empty() {
        return Err(DiskError::Other(
            "no backup disks enrolled; run `timevault disk enroll ...`".to_string(),
        )
        .into());
    }
    if let Some(selector) = selector {
        let disk = disks
            .iter()
            .find(|disk| disk_matches_selector(disk, selector))
            .ok_or_else(|| {
                DiskError::Other(format!("disk selector {} not found in config", selector))
            })?;
        if !connected.iter().any(|item| item.fs_uuid == disk.fs_uuid) {
            return Err(
                DiskError::Other(format!("disk selector {} not connected", selector)).into(),
            );
        }
        return Ok(disk.clone());
    }
    if connected.is_empty() {
        return Err(DiskError::NoDiskConnected.into());
    }
    Ok(connected[0].clone())
}

/// Mount a disk for a **backup run** at a private per-process mount point, so two
/// `timevault` processes can back different jobs to the same disk at once. The
/// device already being mounted under `mount_base` (another run) is fine.
pub fn mount_disk_for_backup(
    disk: &BackupDiskConfig,
    mount_base: &Path,
    options: &str,
) -> Result<(MountGuard, PathBuf)> {
    mount_disk_at_run_dir(disk, mount_base, options, |device| {
        ensure_disk_mountable_for_backup(device, mount_base)
    })
}

/// Mount a disk at a private per-process mount point, refusing if it is already
/// mounted anywhere. Used by `disk enroll` / `disk rename`.
pub fn mount_disk_guarded(
    disk: &BackupDiskConfig,
    mount_base: &Path,
    options: &str,
) -> Result<(MountGuard, PathBuf)> {
    mount_disk_at_run_dir(disk, mount_base, options, |device| {
        ensure_disk_not_mounted(device)
    })
}

fn mount_disk_at_run_dir(
    disk: &BackupDiskConfig,
    mount_base: &Path,
    options: &str,
    precheck: impl FnOnce(&Path) -> Result<()>,
) -> Result<(MountGuard, PathBuf)> {
    let device = device_path_for_uuid(&disk.fs_uuid);
    if !device.exists() {
        return Err(DiskError::Other(format!("device {} not found", device.display())).into());
    }
    precheck(&device)?;
    let mountpoint = create_run_mount_dir(mount_base, &disk.fs_uuid)?;
    match mount_device(&device, &mountpoint, options) {
        Ok(()) => Ok((MountGuard::new(mountpoint.clone(), true), mountpoint)),
        Err(err) => {
            let _ = std::fs::remove_dir(&mountpoint);
            Err(err)
        }
    }
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
                disabled: false,
                rotated_out: false,
            },
            BackupDiskConfig {
                disk_id: "b".to_string(),
                fs_uuid: "uuid-b".to_string(),
                label: None,
                mount_options: None,
                disabled: false,
                rotated_out: false,
            },
        ];
        let connected = ["uuid-b".to_string()].into_iter().collect();
        let selected = select_disk_from_connected(&disks, None, &connected).unwrap();
        assert_eq!(selected.disk_id, "b");

        let selected = select_disk_from_connected(&disks, Some("uuid-b"), &connected).unwrap();
        assert_eq!(selected.disk_id, "b");
    }

    #[test]
    fn backup_mount_conflict_ignores_mounts_under_base() {
        let base = Path::new("/run/timevault/mounts");
        // no mounts, or only per-run mounts under the base -> no conflict
        assert!(backup_mount_conflict(&[], base).is_none());
        let under = vec![
            PathBuf::from("/run/timevault/mounts/uuid.111.20260101000000000"),
            PathBuf::from("/run/timevault/mounts/uuid.222.20260101000000001"),
        ];
        assert!(backup_mount_conflict(&under, base).is_none());
        // a mount elsewhere (data disk / manual) is a conflict
        let mut mixed = under.clone();
        mixed.push(PathBuf::from("/mnt/data"));
        assert_eq!(
            backup_mount_conflict(&mixed, base),
            Some(&PathBuf::from("/mnt/data"))
        );
    }
}
